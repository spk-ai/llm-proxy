package proxy

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	authorizationv1 "github.com/agynio/llm-proxy/.gen/go/agynio/api/authorization/v1"
	llmv1 "github.com/agynio/llm-proxy/.gen/go/agynio/api/llm/v1"
	"github.com/agynio/llm-proxy/internal/identity"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrInvalidBody           = errors.New("invalid body")
	ErrMissingModel          = errors.New("model is required")
	ErrMissingIdentity       = errors.New("identity is required")
	ErrForbidden             = errors.New("access denied")
	ErrProtocolMismatch      = errors.New("protocol mismatch")
	ErrUnsupportedAuthMethod = errors.New("unsupported auth method")
)

const (
	maxRequestBodySize int64 = 1 << 20
	responsesPath            = "/v1/responses"
	messagesPath             = "/v1/messages"
)

type ModelResolver interface {
	ResolveModel(ctx context.Context, req *llmv1.ResolveModelRequest, opts ...grpc.CallOption) (*llmv1.ResolveModelResponse, error)
}

type AuthorizationChecker interface {
	Check(ctx context.Context, req *authorizationv1.CheckRequest, opts ...grpc.CallOption) (*authorizationv1.CheckResponse, error)
}

type Handler struct {
	llmClient       ModelResolver
	authzClient     AuthorizationChecker
	meteringClient  MeteringRecorder
	sandboxResolver SandboxResolver
	client          *http.Client
}

func NewHandler(llmClient ModelResolver, authzClient AuthorizationChecker, meteringClient MeteringRecorder, sandboxResolver SandboxResolver, client *http.Client) http.Handler {
	if llmClient == nil {
		panic("llm client is required")
	}
	if authzClient == nil {
		panic("authorization client is required")
	}
	if meteringClient == nil {
		panic("metering client is required")
	}
	if sandboxResolver == nil {
		panic("sandbox resolver is required")
	}
	if client == nil {
		panic("http client is required")
	}
	return &Handler{llmClient: llmClient, authzClient: authzClient, meteringClient: meteringClient, sandboxResolver: sandboxResolver, client: client}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	expectedProtocol, ok := protocolForPath(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	resolvedIdentity, ok := identity.IdentityFromContext(r.Context())
	if !ok {
		writeProxyError(w, ErrMissingIdentity)
		return
	}
	threadID := strings.TrimSpace(r.Header.Get("x-agyn-thread-id"))

	sandbox, err := h.resolveSandboxPrincipal(r.Context(), resolvedIdentity)
	if err != nil {
		writeProxyError(w, err)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodySize)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}
	log.Printf("proxy: request body read method=%s path=%s size=%d", r.Method, r.URL.Path, len(body))

	payload, modelID, stream, err := parseRequestPayload(body)
	if err != nil {
		rawModel := extractRawModelValue(body)
		log.Printf("proxy: parse request failed err=%v raw_model=%s", err, rawModel)
		writeProxyError(w, err)
		return
	}
	log.Printf("proxy: parsed request model_id=%s stream=%t", modelID, stream)

	resolvedModel, err := h.llmClient.ResolveModel(r.Context(), &llmv1.ResolveModelRequest{ModelId: modelID})
	if err != nil {
		writeProxyError(w, err)
		return
	}

	providerConfig, err := parseProviderConfig(resolvedModel, expectedProtocol)
	if err != nil {
		writeProxyError(w, err)
		return
	}
	log.Printf(
		"proxy: resolved model model_id=%s organization_id=%s provider_organization_id=%s remote_name=%s endpoint=%s",
		modelID,
		providerConfig.organizationID,
		providerConfig.organizationID,
		providerConfig.remoteName,
		providerConfig.endpoint,
	)

	if err := h.authorizeRequest(r.Context(), resolvedIdentity, sandbox, providerConfig, modelID); err != nil {
		writeProxyError(w, err)
		return
	}

	meteringMeta := meteringMetadata{
		callID:    uuid.NewString(),
		orgID:     providerConfig.organizationID,
		modelID:   modelID,
		modelName: providerConfig.remoteName,
		threadID:  threadID,
		identity:  resolvedIdentity,
		sandbox:   sandbox,
	}

	updatedBody, err := updateRequestPayload(payload, providerConfig.remoteName, stream)
	if err != nil {
		writeProxyError(w, err)
		return
	}

	providerReq, err := buildProviderRequest(r.Context(), providerConfig.endpoint, providerConfig.token, updatedBody, stream, providerConfig.authMethod, r.Header)
	if err != nil {
		writeProxyError(w, err)
		return
	}

	if stream {
		h.streamResponse(w, r, providerReq, expectedProtocol, meteringMeta)
		return
	}

	h.forwardResponse(w, providerReq, meteringMeta)
}

func (h *Handler) forwardResponse(w http.ResponseWriter, req *http.Request, meta meteringMetadata) {
	resp, err := h.client.Do(req)
	if err != nil {
		h.recordMetering(meta, nil, meteringStatusFailed)
		writeProxyError(w, fmt.Errorf("send request: %w", err))
		return
	}
	log.Printf("proxy: upstream response status=%d", resp.StatusCode)
	defer closeResponseBody(resp.Body)

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		h.recordMetering(meta, nil, meteringStatusFailed)
		writeProxyError(w, fmt.Errorf("read response: %w", err))
		return
	}

	copyHeaders(w.Header(), resp.Header, nil)
	w.WriteHeader(resp.StatusCode)
	if _, err := w.Write(body); err != nil {
		log.Printf("proxy: forward response failed: %v", err)
	}

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		h.recordMetering(meta, nil, meteringStatusFailed)
		return
	}

	usage, err := parseUsageFromPayload(body)
	if err != nil {
		log.Printf("proxy: metering usage parse failed: %v", err)
		h.recordMetering(meta, nil, meteringStatusSuccess)
		return
	}
	h.recordMetering(meta, &usage, meteringStatusSuccess)
}

func (h *Handler) streamResponse(w http.ResponseWriter, r *http.Request, req *http.Request, protocol llmv1.Protocol, meta meteringMetadata) {
	resp, err := h.client.Do(req)
	if err != nil {
		h.recordMetering(meta, nil, meteringStatusFailed)
		writeProxyError(w, fmt.Errorf("send request: %w", err))
		return
	}
	log.Printf("proxy: upstream response status=%d", resp.StatusCode)
	defer closeResponseBody(resp.Body)

	copyHeaders(w.Header(), resp.Header, map[string]struct{}{"Content-Length": {}})
	w.WriteHeader(resp.StatusCode)
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		_, _ = io.Copy(w, resp.Body)
		h.recordMetering(meta, nil, meteringStatusFailed)
		return
	}
	usage, err := streamToClient(r.Context(), w, resp.Body, protocol)
	if err != nil {
		log.Printf("proxy: stream response failed: %v", err)
		h.recordMetering(meta, nil, meteringStatusFailed)
		return
	}
	if usage == nil {
		log.Printf("proxy: metering usage missing for streaming response")
		h.recordMetering(meta, nil, meteringStatusSuccess)
		return
	}
	h.recordMetering(meta, usage, meteringStatusSuccess)
}

func (h *Handler) authorizeRequest(ctx context.Context, resolved identity.ResolvedIdentity, sandbox sandboxPrincipal, provider providerConfig, modelID string) error {
	// A sandbox may only spend against the organization it runs in. Its owner
	// can well be a member of several, and the model decides which organization
	// the call is metered to, so without this a sandbox in one organization
	// could bill another.
	if sandbox.isSandbox() && sandbox.organizationID != provider.organizationID {
		log.Printf(
			"proxy: authorization denied sandbox_id=%s sandbox_organization_id=%s model_id=%s organization_id=%s",
			sandbox.sandboxID,
			sandbox.organizationID,
			modelID,
			provider.organizationID,
		)
		return ErrForbidden
	}

	principalID := authorizationPrincipalID(resolved, sandbox)
	user := fmt.Sprintf("identity:%s", principalID)
	object := fmt.Sprintf("model:%s", modelID)
	tuple := &authorizationv1.TupleKey{
		User:     user,
		Relation: "can_use",
		Object:   object,
	}
	log.Printf(
		"proxy: authorization check principal_identity_id=%s principal_identity_type=%s principal_workload_id=%s principal_ziti_identity_id=%s model_id=%s resolved_model_id=%s organization_id=%s provider_organization_id=%s tuple_user=%s tuple_relation=%s tuple_object=%s",
		resolved.IdentityID,
		resolved.IdentityType,
		resolved.WorkloadID,
		resolved.ZitiID,
		modelID,
		modelID,
		provider.organizationID,
		provider.organizationID,
		tuple.User,
		tuple.Relation,
		tuple.Object,
	)

	resp, err := h.authzClient.Check(ctx, &authorizationv1.CheckRequest{
		TupleKey: tuple,
	})
	if err != nil {
		log.Printf(
			"proxy: authorization check error principal_identity_id=%s principal_identity_type=%s principal_workload_id=%s principal_ziti_identity_id=%s model_id=%s organization_id=%s tuple_user=%s tuple_relation=%s tuple_object=%s err=%v",
			resolved.IdentityID,
			resolved.IdentityType,
			resolved.WorkloadID,
			resolved.ZitiID,
			modelID,
			provider.organizationID,
			tuple.User,
			tuple.Relation,
			tuple.Object,
			err,
		)
		return err
	}
	if !resp.GetAllowed() {
		log.Printf(
			"proxy: authorization denied principal_identity_id=%s principal_identity_type=%s principal_workload_id=%s principal_ziti_identity_id=%s model_id=%s organization_id=%s tuple_user=%s tuple_relation=%s tuple_object=%s",
			resolved.IdentityID,
			resolved.IdentityType,
			resolved.WorkloadID,
			resolved.ZitiID,
			modelID,
			provider.organizationID,
			tuple.User,
			tuple.Relation,
			tuple.Object,
		)
		return ErrForbidden
	}
	log.Printf(
		"proxy: authorization allowed principal_identity_id=%s principal_identity_type=%s principal_workload_id=%s principal_ziti_identity_id=%s model_id=%s organization_id=%s tuple_user=%s tuple_relation=%s tuple_object=%s",
		resolved.IdentityID,
		resolved.IdentityType,
		resolved.WorkloadID,
		resolved.ZitiID,
		modelID,
		provider.organizationID,
		tuple.User,
		tuple.Relation,
		tuple.Object,
	)
	return nil
}

// authorizationPrincipalID names the identity the model check is made against.
// A sandbox holds no model grant of its own — can_use follows organization
// membership, and a sandbox is deliberately not a member — so the check resolves
// through its record to the owner who started it. The sandbox reaches exactly
// the models its owner can already call, and nothing else about the owner
// carries over: model calls are the only operation it is granted.
func authorizationPrincipalID(resolved identity.ResolvedIdentity, sandbox sandboxPrincipal) string {
	if sandbox.isSandbox() {
		return sandbox.ownerID
	}
	return resolved.IdentityID
}

type providerConfig struct {
	endpoint       string
	token          string
	remoteName     string
	organizationID string
	authMethod     llmv1.AuthMethod
}

func protocolForPath(path string) (llmv1.Protocol, bool) {
	switch path {
	case responsesPath:
		return llmv1.Protocol_PROTOCOL_RESPONSES, true
	case messagesPath:
		return llmv1.Protocol_PROTOCOL_ANTHROPIC_MESSAGES, true
	default:
		return llmv1.Protocol_PROTOCOL_UNSPECIFIED, false
	}
}

func parseProviderConfig(resolved *llmv1.ResolveModelResponse, expectedProtocol llmv1.Protocol) (providerConfig, error) {
	endpoint := strings.TrimSpace(resolved.GetEndpoint())
	if endpoint == "" {
		return providerConfig{}, errors.New("provider endpoint is required")
	}
	remoteName := strings.TrimSpace(resolved.GetRemoteName())
	if remoteName == "" {
		return providerConfig{}, errors.New("remote model name is required")
	}
	token := strings.TrimSpace(resolved.GetToken())
	if token == "" {
		return providerConfig{}, errors.New("provider token is required")
	}
	organizationID := strings.TrimSpace(resolved.GetOrganizationId())
	if organizationID == "" {
		return providerConfig{}, errors.New("organization id is required")
	}
	if resolved.GetProtocol() != expectedProtocol {
		return providerConfig{}, fmt.Errorf("%w: expected %s got %s", ErrProtocolMismatch, expectedProtocol, resolved.GetProtocol())
	}
	authMethod := resolved.GetAuthMethod()
	switch authMethod {
	case llmv1.AuthMethod_AUTH_METHOD_BEARER, llmv1.AuthMethod_AUTH_METHOD_X_API_KEY:
		return providerConfig{
			endpoint:       endpoint,
			token:          token,
			remoteName:     remoteName,
			organizationID: organizationID,
			authMethod:     authMethod,
		}, nil
	default:
		return providerConfig{}, fmt.Errorf("%w: %s", ErrUnsupportedAuthMethod, authMethod)
	}
}

func buildProviderRequest(ctx context.Context, endpoint string, token string, body []byte, stream bool, authMethod llmv1.AuthMethod, callerHeaders http.Header) (*http.Request, error) {
	url := endpoint
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	copyProviderRequestHeaders(req.Header, callerHeaders)
	req.Header.Set("Content-Type", "application/json")
	if stream {
		req.Header.Set("Accept", "text/event-stream")
	}
	switch authMethod {
	case llmv1.AuthMethod_AUTH_METHOD_BEARER:
		req.Header.Set("Authorization", "Bearer "+token)
	case llmv1.AuthMethod_AUTH_METHOD_X_API_KEY:
		req.Header.Set("x-api-key", token)
	default:
		return nil, ErrUnsupportedAuthMethod
	}

	return req, nil
}

func copyProviderRequestHeaders(dst, src http.Header) {
	connectionHeaders := connectionHeaderTokens(src)
	for key, values := range src {
		if shouldStripProviderRequestHeader(key, connectionHeaders) {
			continue
		}
		canonical := http.CanonicalHeaderKey(key)
		for _, value := range values {
			dst.Add(canonical, value)
		}
	}
}

func connectionHeaderTokens(header http.Header) map[string]struct{} {
	tokens := make(map[string]struct{})
	for _, value := range header.Values("Connection") {
		for token := range strings.SplitSeq(value, ",") {
			canonical := http.CanonicalHeaderKey(strings.TrimSpace(token))
			if canonical != "" {
				tokens[canonical] = struct{}{}
			}
		}
	}
	return tokens
}

func shouldStripProviderRequestHeader(key string, connectionHeaders map[string]struct{}) bool {
	canonical := http.CanonicalHeaderKey(key)
	if _, ok := connectionHeaders[canonical]; ok {
		return true
	}

	lower := strings.ToLower(key)
	if strings.HasPrefix(lower, "proxy-") || strings.HasPrefix(lower, "x-agyn-") {
		return true
	}

	switch canonical {
	// Accept-Encoding is the caller's negotiation with us, not ours with the
	// provider. Forwarding it makes net/http treat compression as caller-managed
	// and stop decoding the response, so resp.Body stays gzipped: the SSE reader
	// then parses compressed bytes, usage parsing fails on '\x1f', and the client
	// receives a mangled stream it reports as disconnected. Dropping it lets the
	// Transport negotiate and decode on its own.
	case "Host", "Content-Length", "Connection", "Transfer-Encoding", "Keep-Alive", "Te", "Trailer", "Upgrade", "Authorization", "X-Api-Key", "Accept-Encoding":
		return true
	default:
		return false
	}
}

func extractRawModelValue(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(body, &payload); err != nil {
		return ""
	}
	rawModel, ok := payload["model"]
	if !ok {
		return ""
	}
	rawModel = bytes.TrimSpace(rawModel)
	if len(rawModel) == 0 {
		return ""
	}
	var modelStr string
	if err := json.Unmarshal(rawModel, &modelStr); err == nil {
		return strings.TrimSpace(modelStr)
	}
	return string(rawModel)
}

func parseRequestPayload(body []byte) (map[string]any, string, bool, error) {
	if len(body) == 0 {
		return nil, "", false, fmt.Errorf("%w: body is empty", ErrInvalidBody)
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, "", false, fmt.Errorf("%w: %v", ErrInvalidBody, err)
	}

	rawModel, ok := payload["model"]
	if !ok {
		return payload, "", false, ErrMissingModel
	}
	modelStr, ok := rawModel.(string)
	if !ok || strings.TrimSpace(modelStr) == "" {
		return payload, "", false, fmt.Errorf("%w: model must be a string", ErrInvalidBody)
	}
	modelID, err := uuid.Parse(modelStr)
	if err != nil {
		return payload, "", false, fmt.Errorf("%w: model must be a UUID", ErrInvalidBody)
	}

	stream := false
	if rawStream, ok := payload["stream"]; ok {
		value, ok := rawStream.(bool)
		if !ok {
			return payload, "", false, fmt.Errorf("%w: stream must be a boolean", ErrInvalidBody)
		}
		stream = value
	}

	return payload, modelID.String(), stream, nil
}

func updateRequestPayload(payload map[string]any, remoteName string, forceStream bool) ([]byte, error) {
	payload["model"] = remoteName
	if forceStream {
		payload["stream"] = true
	}
	updated, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidBody, err)
	}
	return updated, nil
}

func writeProxyError(w http.ResponseWriter, err error) {
	statusCode := http.StatusBadGateway
	if grpcStatus, ok := status.FromError(err); ok {
		statusCode = grpcStatusToHTTP(grpcStatus.Code())
	} else {
		switch {
		case errors.Is(err, ErrInvalidBody), errors.Is(err, ErrMissingModel), errors.Is(err, ErrProtocolMismatch), errors.Is(err, ErrUnsupportedAuthMethod):
			statusCode = http.StatusBadRequest
		case errors.Is(err, ErrMissingIdentity):
			statusCode = http.StatusUnauthorized
		case errors.Is(err, ErrForbidden), errors.Is(err, ErrSandboxUnusable):
			statusCode = http.StatusForbidden
		}
	}

	log.Printf("proxy: error status=%d err=%v", statusCode, err)

	message := err.Error()
	if statusCode >= http.StatusInternalServerError {
		message = http.StatusText(statusCode)
		if message == "" {
			message = "server error"
		}
	}

	http.Error(w, message, statusCode)
}

func grpcStatusToHTTP(code codes.Code) int {
	switch code {
	case codes.InvalidArgument, codes.OutOfRange:
		return http.StatusBadRequest
	case codes.NotFound:
		return http.StatusNotFound
	case codes.AlreadyExists, codes.Aborted:
		return http.StatusConflict
	case codes.Unauthenticated:
		return http.StatusUnauthorized
	case codes.PermissionDenied:
		return http.StatusForbidden
	case codes.FailedPrecondition:
		return http.StatusPreconditionFailed
	case codes.ResourceExhausted:
		return http.StatusTooManyRequests
	case codes.Unimplemented:
		return http.StatusNotImplemented
	case codes.Unavailable:
		return http.StatusServiceUnavailable
	case codes.DeadlineExceeded:
		return http.StatusGatewayTimeout
	default:
		return http.StatusBadGateway
	}
}

func copyHeaders(dst, src http.Header, skip map[string]struct{}) {
	for key, values := range src {
		canonical := http.CanonicalHeaderKey(key)
		if skip != nil {
			if _, ok := skip[canonical]; ok {
				continue
			}
		}
		for _, value := range values {
			dst.Add(canonical, value)
		}
	}
}

func closeResponseBody(body io.Closer) {
	if err := body.Close(); err != nil {
		log.Printf("proxy: response body close failed: %v", err)
	}
}

func streamToClient(ctx context.Context, w http.ResponseWriter, body io.Reader, protocol llmv1.Protocol) (*usageCounts, error) {
	return streamToClientObserved(ctx, w, body, protocol, nil)
}

func streamToClientObserved(ctx context.Context, w http.ResponseWriter, body io.Reader, protocol llmv1.Protocol, observe func(eventType, data string, complete bool)) (*usageCounts, error) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		return nil, errors.New("streaming unsupported")
	}
	reader := bufio.NewReader(body)
	var usage *usageCounts
	var eventType string
	var dataLines []string

	processEvent := func(complete bool) {
		if len(dataLines) == 0 {
			eventType = ""
			return
		}
		data := strings.Join(dataLines, "\n")
		if observe != nil {
			observe(eventType, data, complete)
		}
		parsed, matched, err := parseUsageFromEvent(protocol, eventType, data)
		if err != nil {
			log.Printf("proxy: metering usage parse failed: %v", err)
		} else if matched {
			usage = &parsed
		}
		eventType = ""
		dataLines = nil
	}

	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		line, err := reader.ReadString('\n')
		if len(line) > 0 {
			if _, writeErr := w.Write([]byte(line)); writeErr != nil {
				return nil, writeErr
			}
			flusher.Flush()

			trimmed := strings.TrimRight(line, "\r\n")
			if trimmed == "" {
				processEvent(true)
			} else if strings.HasPrefix(trimmed, "event:") {
				eventType = strings.TrimSpace(strings.TrimPrefix(trimmed, "event:"))
			} else if strings.HasPrefix(trimmed, "data:") {
				dataLines = append(dataLines, strings.TrimSpace(strings.TrimPrefix(trimmed, "data:")))
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				processEvent(false)
				return usage, nil
			}
			return usage, err
		}
	}
}
