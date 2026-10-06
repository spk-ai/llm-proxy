package proxy

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	authorizationv1 "github.com/agynio/llm-proxy/.gen/go/agynio/api/authorization/v1"
	llmv1 "github.com/agynio/llm-proxy/.gen/go/agynio/api/llm/v1"
	meteringv1 "github.com/agynio/llm-proxy/.gen/go/agynio/api/metering/v1"
	"github.com/agynio/llm-proxy/internal/identity"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeLLMClient struct {
	resp    *llmv1.ResolveModelResponse
	err     error
	lastReq *llmv1.ResolveModelRequest
}

func (f *fakeLLMClient) ResolveModel(_ context.Context, req *llmv1.ResolveModelRequest, _ ...grpc.CallOption) (*llmv1.ResolveModelResponse, error) {
	f.lastReq = req
	if f.err != nil {
		return nil, f.err
	}
	return f.resp, nil
}

type fakeAuthzClient struct {
	resp    *authorizationv1.CheckResponse
	err     error
	lastReq *authorizationv1.CheckRequest
}

func (f *fakeAuthzClient) Check(_ context.Context, req *authorizationv1.CheckRequest, _ ...grpc.CallOption) (*authorizationv1.CheckResponse, error) {
	f.lastReq = req
	if f.err != nil {
		return nil, f.err
	}
	return f.resp, nil
}

type fakeMeteringClient struct{}

func (f *fakeMeteringClient) Record(_ context.Context, _ *meteringv1.RecordRequest, _ ...grpc.CallOption) (*meteringv1.RecordResponse, error) {
	return &meteringv1.RecordResponse{}, nil
}

func TestHandlerRejectsMissingIdentity(t *testing.T) {
	handler := NewHandler(&fakeLLMClient{}, &fakeAuthzClient{}, &fakeMeteringClient{}, &fakeSandboxResolver{}, http.DefaultClient)

	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"`+uuid.NewString()+`"}`))
	resp := httptest.NewRecorder()

	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusUnauthorized {
		t.Fatalf("expected status %d, got %d", http.StatusUnauthorized, resp.Code)
	}
}

func TestHandlerForwardNonStream(t *testing.T) {
	modelID := uuid.New()
	providerCalled := false
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		providerCalled = true
		if r.URL.Path != "/responses" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer provider-token" {
			t.Fatalf("unexpected auth header %q", r.Header.Get("Authorization"))
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Fatalf("unexpected content type %q", r.Header.Get("Content-Type"))
		}
		if got := r.Header.Get("Accept"); got != "" {
			t.Fatalf("unexpected accept header %q", got)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read provider body: %v", err)
		}
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Fatalf("unmarshal provider body: %v", err)
		}
		if payload["model"] != "remote-model" {
			t.Fatalf("expected model remote-model, got %v", payload["model"])
		}
		if payload["stream"] != false {
			t.Fatalf("expected stream false, got %v", payload["stream"])
		}
		w.Header().Set("X-Provider", "ok")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer provider.Close()

	llmClient := &fakeLLMClient{resp: &llmv1.ResolveModelResponse{
		Endpoint:       provider.URL + "/responses",
		Token:          "provider-token",
		RemoteName:     "remote-model",
		OrganizationId: "org-1",
		Protocol:       llmv1.Protocol_PROTOCOL_RESPONSES,
		AuthMethod:     llmv1.AuthMethod_AUTH_METHOD_BEARER,
	}}
	authzClient := &fakeAuthzClient{resp: &authorizationv1.CheckResponse{Allowed: true}}

	handler := NewHandler(llmClient, authzClient, &fakeMeteringClient{}, &fakeSandboxResolver{}, provider.Client())

	body := `{"model":"` + modelID.String() + `","stream":false}`
	req := httptest.NewRequest(http.MethodPost, "http://example.com/v1/responses", strings.NewReader(body))
	ctx := identity.WithIdentity(req.Context(), identity.ResolvedIdentity{IdentityID: "user-1", IdentityType: identity.IdentityTypeUser})
	req = req.WithContext(ctx)
	resp := httptest.NewRecorder()

	handler.ServeHTTP(resp, req)

	if !providerCalled {
		t.Fatalf("expected provider to be called")
	}
	if resp.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, resp.Code)
	}
	if strings.TrimSpace(resp.Body.String()) != `{"ok":true}` {
		t.Fatalf("unexpected response body: %s", resp.Body.String())
	}
	if resp.Header().Get("X-Provider") != "ok" {
		t.Fatalf("expected provider header")
	}
	if llmClient.lastReq.GetModelId() != modelID.String() {
		t.Fatalf("expected model id %s, got %s", modelID.String(), llmClient.lastReq.GetModelId())
	}
	if authzClient.lastReq.GetTupleKey().GetUser() != "identity:user-1" {
		t.Fatalf("unexpected authz user %q", authzClient.lastReq.GetTupleKey().GetUser())
	}
	if authzClient.lastReq.GetTupleKey().GetRelation() != "can_use" {
		t.Fatalf("unexpected authz relation %q", authzClient.lastReq.GetTupleKey().GetRelation())
	}
	if authzClient.lastReq.GetTupleKey().GetObject() != "model:"+modelID.String() {
		t.Fatalf("unexpected authz object %q", authzClient.lastReq.GetTupleKey().GetObject())
	}
}

func TestHandlerAuthorizesAgentWithIdentityPrincipal(t *testing.T) {
	modelID := uuid.New()
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer provider.Close()

	llmClient := &fakeLLMClient{resp: &llmv1.ResolveModelResponse{
		Endpoint:       provider.URL,
		Token:          "provider-token",
		RemoteName:     "remote-model",
		OrganizationId: "org-1",
		Protocol:       llmv1.Protocol_PROTOCOL_RESPONSES,
		AuthMethod:     llmv1.AuthMethod_AUTH_METHOD_BEARER,
	}}
	authzClient := &fakeAuthzClient{resp: &authorizationv1.CheckResponse{Allowed: true}}
	handler := NewHandler(llmClient, authzClient, &fakeMeteringClient{}, &fakeSandboxResolver{}, provider.Client())

	body := `{"model":"` + modelID.String() + `","stream":false}`
	req := httptest.NewRequest(http.MethodPost, "http://example.com/v1/responses", strings.NewReader(body))
	ctx := identity.WithIdentity(req.Context(), identity.ResolvedIdentity{
		IdentityID:   "agent-1",
		IdentityType: identity.IdentityTypeAgent,
		WorkloadID:   "workload-1",
		ZitiID:       "ziti-agent-1",
	})
	req = req.WithContext(ctx)
	resp := httptest.NewRecorder()

	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, resp.Code)
	}
	tuple := authzClient.lastReq.GetTupleKey()
	if tuple.GetUser() != "identity:agent-1" {
		t.Fatalf("expected agent authz user, got %q", tuple.GetUser())
	}
	if tuple.GetRelation() != "can_use" {
		t.Fatalf("unexpected authz relation %q", tuple.GetRelation())
	}
	if tuple.GetObject() != "model:"+modelID.String() {
		t.Fatalf("unexpected authz object %q", tuple.GetObject())
	}
}

func TestAuthorizationPrincipalIDUsesAgentIdentityEvenWhenWorkloadPresent(t *testing.T) {
	principalID := authorizationPrincipalID(identity.ResolvedIdentity{
		IdentityID:   "agent-1",
		IdentityType: identity.IdentityTypeAgent,
		WorkloadID:   "workload-1",
	}, sandboxPrincipal{})
	if principalID != "agent-1" {
		t.Fatalf("expected agent identity, got %q", principalID)
	}
}

func TestHandlerForwardStream(t *testing.T) {
	modelID := uuid.New()
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Accept") != "text/event-stream" {
			t.Fatalf("expected event-stream accept header")
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read provider body: %v", err)
		}
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Fatalf("unmarshal provider body: %v", err)
		}
		if payload["stream"] != true {
			t.Fatalf("expected stream true, got %v", payload["stream"])
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Content-Length", "123")
		w.Header().Set("X-Provider", "stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: hello\n\n"))
	}))
	defer provider.Close()

	llmClient := &fakeLLMClient{resp: &llmv1.ResolveModelResponse{
		Endpoint:       provider.URL + "/responses",
		Token:          "provider-token",
		RemoteName:     "remote-model",
		OrganizationId: "org-1",
		Protocol:       llmv1.Protocol_PROTOCOL_RESPONSES,
		AuthMethod:     llmv1.AuthMethod_AUTH_METHOD_BEARER,
	}}
	authzClient := &fakeAuthzClient{resp: &authorizationv1.CheckResponse{Allowed: true}}
	handler := NewHandler(llmClient, authzClient, &fakeMeteringClient{}, &fakeSandboxResolver{}, provider.Client())

	body := `{"model":"` + modelID.String() + `","stream":true}`
	req := httptest.NewRequest(http.MethodPost, "http://example.com/v1/responses", strings.NewReader(body))
	ctx := identity.WithIdentity(req.Context(), identity.ResolvedIdentity{IdentityID: "user-1", IdentityType: identity.IdentityTypeUser})
	req = req.WithContext(ctx)
	resp := httptest.NewRecorder()

	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, resp.Code)
	}
	if resp.Header().Get("Content-Length") != "" {
		t.Fatalf("expected content-length to be omitted")
	}
	if resp.Header().Get("X-Provider") != "stream" {
		t.Fatalf("expected provider header")
	}
	if strings.TrimSpace(resp.Body.String()) != "data: hello" {
		t.Fatalf("unexpected response body: %s", resp.Body.String())
	}
}

func TestHandlerForwardAnthropicMessages(t *testing.T) {
	modelID := uuid.New()
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/messages" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		if r.Host == "caller.example" {
			t.Fatalf("expected caller host not to be forwarded")
		}
		assertProviderRequestHeaderAbsent(t, r.Header, "Authorization")
		if r.Header.Get("X-Api-Key") != "provider-token" {
			t.Fatalf("unexpected x-api-key header %q", r.Header.Get("X-Api-Key"))
		}
		if r.Header.Get("Anthropic-Version") != "2023-06-01" {
			t.Fatalf("unexpected anthropic-version header %q", r.Header.Get("Anthropic-Version"))
		}
		if r.Header.Get("Anthropic-Beta") != "prompt-caching-2024-07-31" {
			t.Fatalf("unexpected anthropic-beta header %q", r.Header.Get("Anthropic-Beta"))
		}
		assertProviderRequestHeaderAbsent(t, r.Header, "X-Agyn-Thread-Id")
		assertProviderRequestHeaderAbsent(t, r.Header, "Connection")
		assertProviderRequestHeaderNotValue(t, r.Header, "Content-Length", "999")
		assertProviderRequestHeaderAbsent(t, r.Header, "Keep-Alive")
		assertProviderRequestHeaderAbsent(t, r.Header, "Te")
		assertProviderRequestHeaderAbsent(t, r.Header, "Trailer")
		assertProviderRequestHeaderAbsent(t, r.Header, "Transfer-Encoding")
		assertProviderRequestHeaderAbsent(t, r.Header, "Upgrade")
		assertProviderRequestHeaderAbsent(t, r.Header, "Proxy-Authorization")
		assertProviderRequestHeaderAbsent(t, r.Header, "X-Debug")
		if r.Header.Get("Content-Type") != "application/json" {
			t.Fatalf("unexpected content type %q", r.Header.Get("Content-Type"))
		}
		if got := r.Header.Get("Accept"); got != "" {
			t.Fatalf("unexpected accept header %q", got)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read provider body: %v", err)
		}
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Fatalf("unmarshal provider body: %v", err)
		}
		if payload["model"] != "claude-3" {
			t.Fatalf("expected model claude-3, got %v", payload["model"])
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer provider.Close()

	llmClient := &fakeLLMClient{resp: &llmv1.ResolveModelResponse{
		Endpoint:       provider.URL + "/messages",
		Token:          "provider-token",
		RemoteName:     "claude-3",
		OrganizationId: "org-1",
		Protocol:       llmv1.Protocol_PROTOCOL_ANTHROPIC_MESSAGES,
		AuthMethod:     llmv1.AuthMethod_AUTH_METHOD_X_API_KEY,
	}}
	authzClient := &fakeAuthzClient{resp: &authorizationv1.CheckResponse{Allowed: true}}

	handler := NewHandler(llmClient, authzClient, &fakeMeteringClient{}, &fakeSandboxResolver{}, provider.Client())

	body := `{"model":"` + modelID.String() + `","stream":false}`
	req := httptest.NewRequest(http.MethodPost, "http://example.com/v1/messages", strings.NewReader(body))
	req.Host = "caller.example"
	req.Header.Set("anthropic-version", "2023-06-01")
	req.Header.Set("anthropic-beta", "prompt-caching-2024-07-31")
	req.Header.Set("Authorization", "Bearer caller-token")
	req.Header.Set("x-api-key", "caller-key")
	req.Header.Set("x-agyn-thread-id", "thread-1")
	req.Header.Set("Connection", "keep-alive, x-debug")
	req.Header.Set("Content-Length", "999")
	req.Header.Set("Transfer-Encoding", "chunked")
	req.Header.Set("Keep-Alive", "timeout=5")
	req.Header.Set("TE", "trailers")
	req.Header.Set("Trailer", "Expires")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Proxy-Authorization", "Basic caller")
	req.Header.Set("x-debug", "must-not-forward")
	ctx := identity.WithIdentity(req.Context(), identity.ResolvedIdentity{IdentityID: "user-1", IdentityType: identity.IdentityTypeUser})
	req = req.WithContext(ctx)
	resp := httptest.NewRecorder()

	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, resp.Code)
	}
	if strings.TrimSpace(resp.Body.String()) != `{"ok":true}` {
		t.Fatalf("unexpected response body: %s", resp.Body.String())
	}
}

func TestHandlerForwardResponsesHeaders(t *testing.T) {
	modelID := uuid.New()
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/responses" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		if r.Host == "caller.example" {
			t.Fatalf("expected caller host not to be forwarded")
		}
		if r.Header.Get("Authorization") != "Bearer provider-token" {
			t.Fatalf("unexpected authorization header %q", r.Header.Get("Authorization"))
		}
		if r.Header.Get("Openai-Beta") != "responses=v1" {
			t.Fatalf("unexpected openai-beta header %q", r.Header.Get("Openai-Beta"))
		}
		assertProviderRequestHeaderAbsent(t, r.Header, "X-Api-Key")
		assertProviderRequestHeaderAbsent(t, r.Header, "X-Agyn-Thread-Id")
		assertProviderRequestHeaderAbsent(t, r.Header, "Connection")
		assertProviderRequestHeaderNotValue(t, r.Header, "Content-Length", "999")
		assertProviderRequestHeaderAbsent(t, r.Header, "Keep-Alive")
		assertProviderRequestHeaderAbsent(t, r.Header, "Te")
		assertProviderRequestHeaderAbsent(t, r.Header, "Trailer")
		assertProviderRequestHeaderAbsent(t, r.Header, "Transfer-Encoding")
		assertProviderRequestHeaderAbsent(t, r.Header, "Upgrade")
		assertProviderRequestHeaderAbsent(t, r.Header, "Proxy-Connection")
		assertProviderRequestHeaderAbsent(t, r.Header, "X-Debug")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer provider.Close()

	llmClient := &fakeLLMClient{resp: &llmv1.ResolveModelResponse{
		Endpoint:       provider.URL + "/responses",
		Token:          "provider-token",
		RemoteName:     "remote-model",
		OrganizationId: "org-1",
		Protocol:       llmv1.Protocol_PROTOCOL_RESPONSES,
		AuthMethod:     llmv1.AuthMethod_AUTH_METHOD_BEARER,
	}}
	authzClient := &fakeAuthzClient{resp: &authorizationv1.CheckResponse{Allowed: true}}
	handler := NewHandler(llmClient, authzClient, &fakeMeteringClient{}, &fakeSandboxResolver{}, provider.Client())

	body := `{"model":"` + modelID.String() + `","stream":false}`
	req := httptest.NewRequest(http.MethodPost, "http://example.com/v1/responses", strings.NewReader(body))
	req.Host = "caller.example"
	req.Header.Set("openai-beta", "responses=v1")
	req.Header.Set("Authorization", "Bearer caller-token")
	req.Header.Set("x-api-key", "caller-key")
	req.Header.Set("x-agyn-thread-id", "thread-1")
	req.Header.Add("Connection", "keep-alive")
	req.Header.Add("Connection", "x-debug")
	req.Header.Set("Content-Length", "999")
	req.Header.Set("Transfer-Encoding", "chunked")
	req.Header.Set("Keep-Alive", "timeout=5")
	req.Header.Set("TE", "trailers")
	req.Header.Set("Trailer", "Expires")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Proxy-Connection", "keep-alive")
	req.Header.Set("x-debug", "must-not-forward")
	ctx := identity.WithIdentity(req.Context(), identity.ResolvedIdentity{IdentityID: "user-1", IdentityType: identity.IdentityTypeUser})
	req = req.WithContext(ctx)
	resp := httptest.NewRecorder()

	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, resp.Code)
	}
	if strings.TrimSpace(resp.Body.String()) != `{"ok":true}` {
		t.Fatalf("unexpected response body: %s", resp.Body.String())
	}
}

func assertProviderRequestHeaderAbsent(t *testing.T, header http.Header, key string) {
	t.Helper()
	if values, ok := header[key]; ok {
		t.Fatalf("expected %s header to be absent, got %q", key, values)
	}
}

func assertProviderRequestHeaderNotValue(t *testing.T, header http.Header, key string, blockedValue string) {
	t.Helper()
	for _, value := range header.Values(key) {
		if value == blockedValue {
			t.Fatalf("expected %s header not to include %q, got %q", key, blockedValue, header.Values(key))
		}
	}
}

func TestHandlerMessagesWithoutAnthropicVersion(t *testing.T) {
	modelID := uuid.New()
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/messages" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "" {
			t.Fatalf("unexpected authorization header %q", r.Header.Get("Authorization"))
		}
		if r.Header.Get("X-Api-Key") != "provider-token" {
			t.Fatalf("unexpected x-api-key header %q", r.Header.Get("X-Api-Key"))
		}
		if r.Header.Get("Anthropic-Version") != "" {
			t.Fatalf("unexpected anthropic-version header %q", r.Header.Get("Anthropic-Version"))
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer provider.Close()

	llmClient := &fakeLLMClient{resp: &llmv1.ResolveModelResponse{
		Endpoint:       provider.URL + "/messages",
		Token:          "provider-token",
		RemoteName:     "claude-3",
		OrganizationId: "org-1",
		Protocol:       llmv1.Protocol_PROTOCOL_ANTHROPIC_MESSAGES,
		AuthMethod:     llmv1.AuthMethod_AUTH_METHOD_X_API_KEY,
	}}
	authzClient := &fakeAuthzClient{resp: &authorizationv1.CheckResponse{Allowed: true}}

	handler := NewHandler(llmClient, authzClient, &fakeMeteringClient{}, &fakeSandboxResolver{}, provider.Client())

	body := `{"model":"` + modelID.String() + `","stream":false}`
	req := httptest.NewRequest(http.MethodPost, "http://example.com/v1/messages", strings.NewReader(body))
	ctx := identity.WithIdentity(req.Context(), identity.ResolvedIdentity{IdentityID: "user-1", IdentityType: identity.IdentityTypeUser})
	req = req.WithContext(ctx)
	resp := httptest.NewRecorder()

	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, resp.Code)
	}
}

func TestHandlerMessagesStream(t *testing.T) {
	modelID := uuid.New()
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Accept") != "text/event-stream" {
			t.Fatalf("expected event-stream accept header")
		}
		if r.Header.Get("X-Api-Key") != "provider-token" {
			t.Fatalf("unexpected x-api-key header %q", r.Header.Get("X-Api-Key"))
		}
		if r.Header.Get("Anthropic-Version") != "2023-06-01" {
			t.Fatalf("unexpected anthropic-version header %q", r.Header.Get("Anthropic-Version"))
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read provider body: %v", err)
		}
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Fatalf("unmarshal provider body: %v", err)
		}
		if payload["stream"] != true {
			t.Fatalf("expected stream true, got %v", payload["stream"])
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Content-Length", "123")
		w.Header().Set("X-Provider", "stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: hello\n\n"))
	}))
	defer provider.Close()

	llmClient := &fakeLLMClient{resp: &llmv1.ResolveModelResponse{
		Endpoint:       provider.URL + "/messages",
		Token:          "provider-token",
		RemoteName:     "claude-3",
		OrganizationId: "org-1",
		Protocol:       llmv1.Protocol_PROTOCOL_ANTHROPIC_MESSAGES,
		AuthMethod:     llmv1.AuthMethod_AUTH_METHOD_X_API_KEY,
	}}
	authzClient := &fakeAuthzClient{resp: &authorizationv1.CheckResponse{Allowed: true}}
	handler := NewHandler(llmClient, authzClient, &fakeMeteringClient{}, &fakeSandboxResolver{}, provider.Client())

	body := `{"model":"` + modelID.String() + `","stream":true}`
	req := httptest.NewRequest(http.MethodPost, "http://example.com/v1/messages", strings.NewReader(body))
	req.Header.Set("anthropic-version", "2023-06-01")
	ctx := identity.WithIdentity(req.Context(), identity.ResolvedIdentity{IdentityID: "user-1", IdentityType: identity.IdentityTypeUser})
	req = req.WithContext(ctx)
	resp := httptest.NewRecorder()

	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, resp.Code)
	}
	if resp.Header().Get("Content-Length") != "" {
		t.Fatalf("expected content-length to be omitted")
	}
	if resp.Header().Get("X-Provider") != "stream" {
		t.Fatalf("expected provider header")
	}
	if strings.TrimSpace(resp.Body.String()) != "data: hello" {
		t.Fatalf("unexpected response body: %s", resp.Body.String())
	}
}

func TestHandlerProtocolMismatchMessagesWithResponses(t *testing.T) {
	modelID := uuid.New()
	providerCalled := false
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		providerCalled = true
		w.WriteHeader(http.StatusOK)
	}))
	defer provider.Close()

	llmClient := &fakeLLMClient{resp: &llmv1.ResolveModelResponse{
		Endpoint:       provider.URL + "/messages",
		Token:          "provider-token",
		RemoteName:     "claude-3",
		OrganizationId: "org-1",
		Protocol:       llmv1.Protocol_PROTOCOL_RESPONSES,
		AuthMethod:     llmv1.AuthMethod_AUTH_METHOD_BEARER,
	}}
	authzClient := &fakeAuthzClient{resp: &authorizationv1.CheckResponse{Allowed: true}}

	handler := NewHandler(llmClient, authzClient, &fakeMeteringClient{}, &fakeSandboxResolver{}, provider.Client())

	body := `{"model":"` + modelID.String() + `"}`
	req := httptest.NewRequest(http.MethodPost, "http://example.com/v1/messages", strings.NewReader(body))
	ctx := identity.WithIdentity(req.Context(), identity.ResolvedIdentity{IdentityID: "user-1", IdentityType: identity.IdentityTypeUser})
	req = req.WithContext(ctx)
	resp := httptest.NewRecorder()

	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, resp.Code)
	}
	if providerCalled {
		t.Fatalf("expected provider not to be called")
	}
}

func TestHandlerProtocolMismatchResponsesWithAnthropic(t *testing.T) {
	modelID := uuid.New()
	providerCalled := false
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		providerCalled = true
		w.WriteHeader(http.StatusOK)
	}))
	defer provider.Close()

	llmClient := &fakeLLMClient{resp: &llmv1.ResolveModelResponse{
		Endpoint:       provider.URL + "/responses",
		Token:          "provider-token",
		RemoteName:     "remote-model",
		OrganizationId: "org-1",
		Protocol:       llmv1.Protocol_PROTOCOL_ANTHROPIC_MESSAGES,
		AuthMethod:     llmv1.AuthMethod_AUTH_METHOD_X_API_KEY,
	}}
	authzClient := &fakeAuthzClient{resp: &authorizationv1.CheckResponse{Allowed: true}}

	handler := NewHandler(llmClient, authzClient, &fakeMeteringClient{}, &fakeSandboxResolver{}, provider.Client())

	body := `{"model":"` + modelID.String() + `"}`
	req := httptest.NewRequest(http.MethodPost, "http://example.com/v1/responses", strings.NewReader(body))
	ctx := identity.WithIdentity(req.Context(), identity.ResolvedIdentity{IdentityID: "user-1", IdentityType: identity.IdentityTypeUser})
	req = req.WithContext(ctx)
	resp := httptest.NewRecorder()

	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, resp.Code)
	}
	if providerCalled {
		t.Fatalf("expected provider not to be called")
	}
}

func TestHandlerUnsupportedAuthMethod(t *testing.T) {
	modelID := uuid.New()
	providerCalled := false
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		providerCalled = true
		w.WriteHeader(http.StatusOK)
	}))
	defer provider.Close()

	llmClient := &fakeLLMClient{resp: &llmv1.ResolveModelResponse{
		Endpoint:       provider.URL + "/responses",
		Token:          "provider-token",
		RemoteName:     "remote-model",
		OrganizationId: "org-1",
		Protocol:       llmv1.Protocol_PROTOCOL_RESPONSES,
		AuthMethod:     llmv1.AuthMethod_AUTH_METHOD_UNSPECIFIED,
	}}
	authzClient := &fakeAuthzClient{resp: &authorizationv1.CheckResponse{Allowed: true}}

	handler := NewHandler(llmClient, authzClient, &fakeMeteringClient{}, &fakeSandboxResolver{}, provider.Client())

	body := `{"model":"` + modelID.String() + `"}`
	req := httptest.NewRequest(http.MethodPost, "http://example.com/v1/responses", strings.NewReader(body))
	ctx := identity.WithIdentity(req.Context(), identity.ResolvedIdentity{IdentityID: "user-1", IdentityType: identity.IdentityTypeUser})
	req = req.WithContext(ctx)
	resp := httptest.NewRecorder()

	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, resp.Code)
	}
	if providerCalled {
		t.Fatalf("expected provider not to be called")
	}
}

func TestHandlerForbidden(t *testing.T) {
	modelID := uuid.New()
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer provider.Close()

	llmClient := &fakeLLMClient{resp: &llmv1.ResolveModelResponse{
		Endpoint:       provider.URL + "/responses",
		Token:          "provider-token",
		RemoteName:     "remote-model",
		OrganizationId: "org-1",
		Protocol:       llmv1.Protocol_PROTOCOL_RESPONSES,
		AuthMethod:     llmv1.AuthMethod_AUTH_METHOD_BEARER,
	}}
	authzClient := &fakeAuthzClient{resp: &authorizationv1.CheckResponse{Allowed: false}}
	handler := NewHandler(llmClient, authzClient, &fakeMeteringClient{}, &fakeSandboxResolver{}, provider.Client())

	body := `{"model":"` + modelID.String() + `"}`
	req := httptest.NewRequest(http.MethodPost, "http://example.com/v1/responses", strings.NewReader(body))
	ctx := identity.WithIdentity(req.Context(), identity.ResolvedIdentity{IdentityID: "user-1", IdentityType: identity.IdentityTypeUser})
	req = req.WithContext(ctx)
	resp := httptest.NewRecorder()

	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusForbidden {
		t.Fatalf("expected status %d, got %d", http.StatusForbidden, resp.Code)
	}
}

func TestHandlerInvalidBody(t *testing.T) {
	handler := NewHandler(&fakeLLMClient{}, &fakeAuthzClient{}, &fakeMeteringClient{}, &fakeSandboxResolver{}, http.DefaultClient)

	req := httptest.NewRequest(http.MethodPost, "http://example.com/v1/responses", strings.NewReader("{"))
	ctx := identity.WithIdentity(req.Context(), identity.ResolvedIdentity{IdentityID: "user-1", IdentityType: identity.IdentityTypeUser})
	req = req.WithContext(ctx)
	resp := httptest.NewRecorder()

	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, resp.Code)
	}
}

func TestHandlerRouteHandling(t *testing.T) {
	handler := NewHandler(&fakeLLMClient{}, &fakeAuthzClient{}, &fakeMeteringClient{}, &fakeSandboxResolver{}, http.DefaultClient)

	getReq := httptest.NewRequest(http.MethodGet, "http://example.com/v1/responses", nil)
	getResp := httptest.NewRecorder()
	handler.ServeHTTP(getResp, getReq)

	if getResp.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected status %d, got %d", http.StatusMethodNotAllowed, getResp.Code)
	}
	if allow := getResp.Header().Get("Allow"); allow != http.MethodPost {
		t.Fatalf("expected allow header %q, got %q", http.MethodPost, allow)
	}

	getMessagesReq := httptest.NewRequest(http.MethodGet, "http://example.com/v1/messages", nil)
	getMessagesResp := httptest.NewRecorder()
	handler.ServeHTTP(getMessagesResp, getMessagesReq)

	if getMessagesResp.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected status %d, got %d", http.StatusMethodNotAllowed, getMessagesResp.Code)
	}
	if allow := getMessagesResp.Header().Get("Allow"); allow != http.MethodPost {
		t.Fatalf("expected allow header %q, got %q", http.MethodPost, allow)
	}

	postReq := httptest.NewRequest(http.MethodPost, "http://example.com/v1/other", strings.NewReader(`{"model":"`+uuid.NewString()+`"}`))
	postResp := httptest.NewRecorder()
	handler.ServeHTTP(postResp, postReq)

	if postResp.Code != http.StatusNotFound {
		t.Fatalf("expected status %d, got %d", http.StatusNotFound, postResp.Code)
	}
}

func TestHandlerGRPCErrorMapping(t *testing.T) {
	modelID := uuid.New()
	llmClient := &fakeLLMClient{err: status.Error(codes.NotFound, "missing")}
	handler := NewHandler(llmClient, &fakeAuthzClient{}, &fakeMeteringClient{}, &fakeSandboxResolver{}, http.DefaultClient)

	body := `{"model":"` + modelID.String() + `"}`
	req := httptest.NewRequest(http.MethodPost, "http://example.com/v1/responses", strings.NewReader(body))
	ctx := identity.WithIdentity(req.Context(), identity.ResolvedIdentity{IdentityID: "user-1", IdentityType: identity.IdentityTypeUser})
	req = req.WithContext(ctx)
	resp := httptest.NewRecorder()

	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusNotFound {
		t.Fatalf("expected status %d, got %d", http.StatusNotFound, resp.Code)
	}
	if !strings.Contains(resp.Body.String(), "NotFound") {
		t.Fatalf("expected not found message, got %q", resp.Body.String())
	}

	llmClient.err = status.Error(codes.Unavailable, "down")
	req = httptest.NewRequest(http.MethodPost, "http://example.com/v1/responses", strings.NewReader(body))
	ctx = identity.WithIdentity(req.Context(), identity.ResolvedIdentity{IdentityID: "user-1", IdentityType: identity.IdentityTypeUser})
	req = req.WithContext(ctx)
	resp = httptest.NewRecorder()

	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status %d, got %d", http.StatusServiceUnavailable, resp.Code)
	}
	if strings.TrimSpace(resp.Body.String()) != http.StatusText(http.StatusServiceUnavailable) {
		t.Fatalf("expected generic error message, got %q", resp.Body.String())
	}
}

func TestHandlerProviderErrorForwardingNonStream(t *testing.T) {
	modelID := uuid.New()
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":"rate limit"}`))
	}))
	defer provider.Close()

	llmClient := &fakeLLMClient{resp: &llmv1.ResolveModelResponse{
		Endpoint:       provider.URL,
		Token:          "provider-token",
		RemoteName:     "remote-model",
		OrganizationId: "org-1",
		Protocol:       llmv1.Protocol_PROTOCOL_RESPONSES,
		AuthMethod:     llmv1.AuthMethod_AUTH_METHOD_BEARER,
	}}
	authzClient := &fakeAuthzClient{resp: &authorizationv1.CheckResponse{Allowed: true}}
	handler := NewHandler(llmClient, authzClient, &fakeMeteringClient{}, &fakeSandboxResolver{}, provider.Client())

	body := `{"model":"` + modelID.String() + `","stream":false}`
	req := httptest.NewRequest(http.MethodPost, "http://example.com/v1/responses", strings.NewReader(body))
	ctx := identity.WithIdentity(req.Context(), identity.ResolvedIdentity{IdentityID: "user-1", IdentityType: identity.IdentityTypeUser})
	req = req.WithContext(ctx)
	resp := httptest.NewRecorder()

	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusTooManyRequests {
		t.Fatalf("expected status %d, got %d", http.StatusTooManyRequests, resp.Code)
	}
	if strings.TrimSpace(resp.Body.String()) != `{"error":"rate limit"}` {
		t.Fatalf("unexpected response body: %s", resp.Body.String())
	}
}

func TestHandlerProviderErrorForwardingStream(t *testing.T) {
	modelID := uuid.New()
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("oops"))
	}))
	defer provider.Close()

	llmClient := &fakeLLMClient{resp: &llmv1.ResolveModelResponse{
		Endpoint:       provider.URL,
		Token:          "provider-token",
		RemoteName:     "remote-model",
		OrganizationId: "org-1",
		Protocol:       llmv1.Protocol_PROTOCOL_RESPONSES,
		AuthMethod:     llmv1.AuthMethod_AUTH_METHOD_BEARER,
	}}
	authzClient := &fakeAuthzClient{resp: &authorizationv1.CheckResponse{Allowed: true}}
	handler := NewHandler(llmClient, authzClient, &fakeMeteringClient{}, &fakeSandboxResolver{}, provider.Client())

	body := `{"model":"` + modelID.String() + `","stream":true}`
	req := httptest.NewRequest(http.MethodPost, "http://example.com/v1/responses", strings.NewReader(body))
	ctx := identity.WithIdentity(req.Context(), identity.ResolvedIdentity{IdentityID: "user-1", IdentityType: identity.IdentityTypeUser})
	req = req.WithContext(ctx)
	resp := httptest.NewRecorder()

	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusInternalServerError {
		t.Fatalf("expected status %d, got %d", http.StatusInternalServerError, resp.Code)
	}
	if strings.TrimSpace(resp.Body.String()) != "oops" {
		t.Fatalf("unexpected response body: %s", resp.Body.String())
	}
}

func TestHandlerBodyTooLarge(t *testing.T) {
	const limit = 64 << 10
	providerCalled := false
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		providerCalled = true
	}))
	defer provider.Close()
	handler := platformHandler(t, provider, WithMaxRequestBodySize(limit))

	oversize := strings.Repeat("a", limit+1)
	req := httptest.NewRequest(http.MethodPost, "http://example.com/v1/responses", strings.NewReader(oversize))
	ctx := identity.WithIdentity(req.Context(), identity.ResolvedIdentity{IdentityID: "user-1", IdentityType: identity.IdentityTypeUser})
	req = req.WithContext(ctx)
	resp := httptest.NewRecorder()

	handler.ServeHTTP(resp, req)

	if resp.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected status %d, got %d", http.StatusRequestEntityTooLarge, resp.Code)
	}
	if strings.TrimSpace(resp.Body.String()) != "request body exceeds the 65536-byte limit" {
		t.Fatalf("expected the limit named, got %q", resp.Body.String())
	}
	if providerCalled {
		t.Fatal("expected no provider call")
	}
}
