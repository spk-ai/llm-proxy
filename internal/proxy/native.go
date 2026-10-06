package proxy

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	llmv1 "github.com/agynio/llm-proxy/.gen/go/agynio/api/llm/v1"
	"github.com/agynio/llm-proxy/internal/native"
	"github.com/google/uuid"
)

// NativeForwarder serves a native-mode request: the agent CLI's own request,
// with its credential replaced. The body is forwarded byte-for-byte -- the
// vendor owns the model namespace, so nothing in it is rewritten -- and parsed
// only to read the model name for the allowlist and for metering.
type NativeForwarder struct {
	client             *http.Client
	metering           MeteringRecorder
	maxRequestBodySize int64
}

func NewNativeForwarder(client *http.Client, metering MeteringRecorder, opts ...Option) *NativeForwarder {
	if client == nil {
		panic("http client is required")
	}
	if metering == nil {
		panic("metering client is required")
	}
	resolved := applyOptions(opts)
	return &NativeForwarder{client: client, metering: metering, maxRequestBodySize: resolved.maxRequestBodySize}
}

func (f *NativeForwarder) Forward(w http.ResponseWriter, r *http.Request, binding native.Binding) {
	// Forwarded byte-for-byte, so it is read whole or not at all: a body cut at
	// the limit reaches the vendor as invalid JSON, not as a size error.
	body, err := readRequestBody(r.Body, r.ContentLength, f.maxRequestBodySize)
	_ = r.Body.Close()
	if err != nil {
		var tooLarge *requestTooLargeError
		if errors.As(err, &tooLarge) {
			log.Printf("native: refused vendor=%s path=%s: %v", nativeVendorLabel(binding.Vendor), r.URL.Path, err)
			writeNativeErrorOfType(w, binding.Vendor, http.StatusRequestEntityTooLarge, "request_too_large", tooLarge.Error())
			return
		}
		writeNativeError(w, binding.Vendor, http.StatusBadRequest, "failed to read request body")
		return
	}

	// Read-only: the model name drives the allowlist and the metering label,
	// and a body that is not JSON is still forwarded as the vendor's to judge.
	modelName, stream := readNativeRequest(body)

	if !modelAllowed(modelName, binding.AllowedModels) {
		writeNativeError(w, binding.Vendor, http.StatusForbidden,
			fmt.Sprintf("model %q is not in this environment's allowed models", modelName))
		return
	}

	upstream, err := f.buildRequest(r, body, binding)
	if err != nil {
		writeNativeError(w, binding.Vendor, http.StatusBadGateway, err.Error())
		return
	}

	meta := meteringMetadata{
		callID:    uuid.NewString(),
		orgID:     binding.OrganizationID,
		modelID:   binding.SubscriptionID,
		modelName: modelName,
		threadID:  strings.TrimSpace(r.Header.Get("x-agyn-thread-id")),
		identity:  binding.Identity,
		native:    true,
		vendor:    nativeVendorLabel(binding.Vendor),
	}

	resp, err := f.client.Do(upstream)
	if err != nil {
		log.Printf("native: send failed vendor=%s model=%s request_bytes=%d: %v", meta.vendor, modelName, len(body), err)
		f.record(meta, nil, meteringStatusFailed)
		writeNativeError(w, binding.Vendor, http.StatusBadGateway, fmt.Sprintf("send request: %v", err))
		return
	}
	defer closeResponseBody(resp.Body)
	log.Printf("native: forwarded vendor=%s model=%s request_bytes=%d status=%d", meta.vendor, modelName, len(body), resp.StatusCode)

	// Vendor errors -- expired credential, rate limit, quota -- pass through
	// unchanged, including headers, so the CLI's own handling works.
	copyHeaders(w.Header(), resp.Header, map[string]struct{}{"Content-Length": {}})
	w.WriteHeader(resp.StatusCode)

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		_, _ = io.Copy(w, resp.Body)
		f.record(meta, nil, meteringStatusFailed)
		return
	}

	if stream {
		usage, err := streamToClient(r.Context(), w, resp.Body, binding.Protocol)
		if err != nil {
			log.Printf("native: stream response failed: %v", err)
			f.record(meta, nil, meteringStatusFailed)
			return
		}
		f.record(meta, usage, meteringStatusSuccess)
		return
	}

	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		f.record(meta, nil, meteringStatusFailed)
		return
	}
	if _, err := w.Write(payload); err != nil {
		log.Printf("native: forward response failed: %v", err)
		return
	}
	usage, err := parseUsageFromPayload(payload)
	if err != nil {
		log.Printf("native: metering usage parse failed: %v", err)
		f.record(meta, nil, meteringStatusSuccess)
		return
	}
	f.record(meta, &usage, meteringStatusSuccess)
}

func (f *NativeForwarder) buildRequest(r *http.Request, body []byte, binding native.Binding) (*http.Request, error) {
	// The caller's path and query both matter: Claude Code addresses
	// /v1/messages?beta=true, and dropping the query changes the request.
	target := strings.TrimSuffix(binding.UpstreamEndpoint, "/") + r.URL.RequestURI()
	upstream, err := http.NewRequestWithContext(r.Context(), r.Method, target, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	// Everything the CLI sent, minus hop-by-hop and the credential it holds.
	// anthropic-beta in particular carries the flags the request depends on.
	copyProviderRequestHeaders(upstream.Header, r.Header)
	upstream.Header.Set("Authorization", "Bearer "+binding.Token)
	if binding.AccountID != "" {
		upstream.Header.Set("chatgpt-account-id", binding.AccountID)
	}
	return upstream, nil
}

func (f *NativeForwarder) record(meta meteringMetadata, usage *usageCounts, status string) {
	records := buildUsageRecords(meta, usage, status)
	if len(records) == 0 {
		return
	}
	f.recordAsync(records)
}

// readNativeRequest reads the model name and stream flag without disturbing the
// body. A body that will not parse yields no model name, which the allowlist
// treats as unrestricted -- refusing it here would reject a request the vendor
// might accept, on a guess about a format the platform does not own.
func readNativeRequest(body []byte) (string, bool) {
	if len(body) == 0 {
		return "", false
	}
	var payload struct {
		Model  string `json:"model"`
		Stream bool   `json:"stream"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return "", false
	}
	return strings.TrimSpace(payload.Model), payload.Stream
}

// modelAllowed enforces the environment's allowlist. Empty means no
// restriction, which is the default.
func modelAllowed(modelName string, allowed []string) bool {
	if len(allowed) == 0 || modelName == "" {
		return true
	}
	for _, candidate := range allowed {
		if strings.EqualFold(strings.TrimSpace(candidate), modelName) {
			return true
		}
	}
	return false
}

func nativeVendorLabel(vendor llmv1.Vendor) string {
	switch vendor {
	case llmv1.Vendor_VENDOR_ANTHROPIC:
		return "anthropic"
	case llmv1.Vendor_VENDOR_OPENAI:
		return "openai"
	default:
		return ""
	}
}

func writeNativeError(w http.ResponseWriter, vendor llmv1.Vendor, status int, message string) {
	writeNativeErrorOfType(w, vendor, status, "permission_error", message)
}

// writeNativeErrorOfType refuses in the vendor's error format with the given
// error type, such as Anthropic's request_too_large for a 413.
func writeNativeErrorOfType(w http.ResponseWriter, vendor llmv1.Vendor, status int, errorType, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	body := map[string]any{
		"type":  "error",
		"error": map[string]any{"type": errorType, "message": "agyn: " + message},
	}
	if vendor != llmv1.Vendor_VENDOR_ANTHROPIC {
		delete(body, "type")
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		encoded = []byte(`{"error":{"type":"permission_error","message":"agyn: request refused"}}`)
	}
	if _, err := w.Write(encoded); err != nil {
		log.Printf("native: write error response: %v", err)
	}
}
