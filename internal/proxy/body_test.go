package proxy

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	authorizationv1 "github.com/agynio/llm-proxy/.gen/go/agynio/api/authorization/v1"
	llmv1 "github.com/agynio/llm-proxy/.gen/go/agynio/api/llm/v1"
	"github.com/agynio/llm-proxy/internal/identity"
	"github.com/google/uuid"
)

// largeMessagesBody is a valid Messages request of at least size bytes, the
// shape a long conversation with screenshots takes: one long base64 string.
func largeMessagesBody(t *testing.T, size int) []byte {
	t.Helper()
	prefix := `{"model":"claude-sonnet-5","max_tokens":7,"messages":[{"role":"user","content":[{"type":"image","source":{"type":"base64","media_type":"image/png","data":"`
	suffix := `"}}]}]}`
	fill := size - len(prefix) - len(suffix)
	if fill < 0 {
		fill = 0
	}
	body := []byte(prefix + strings.Repeat("A", fill) + suffix)
	if !json.Valid(body) {
		t.Fatal("test body is not valid JSON")
	}
	return body
}

// unsizedReader hides the length, as a chunked request does.
type unsizedReader struct{ io.Reader }

func TestDefaultMaxRequestBodySizeIsTheVendorLimit(t *testing.T) {
	if DefaultMaxRequestBodySize != 32<<20 {
		t.Fatalf("default limit = %d, want 32 MiB", DefaultMaxRequestBodySize)
	}
	if got := NewNativeForwarder(http.DefaultClient, newCapturingMeteringClient()).maxRequestBodySize; got != DefaultMaxRequestBodySize {
		t.Fatalf("native default = %d", got)
	}
}

// A body over 1 MiB reaches the vendor whole. It used to be cut at 1 MiB and
// forwarded anyway, which the vendor refused as invalid JSON.
func TestNativeForwarderForwardsLargeBodyIntact(t *testing.T) {
	body := largeMessagesBody(t, 5<<20/2)
	var got []byte
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, _ = io.ReadAll(r.Body)
		w.Write([]byte(`{"usage":{"input_tokens":1,"output_tokens":1}}`))
	}))
	defer upstream.Close()

	for _, sized := range []bool{true, false} {
		got = nil
		req := nativeRequest(t, "/v1/messages?beta=true", "")
		if sized {
			req.Body = io.NopCloser(bytes.NewReader(body))
			req.ContentLength = int64(len(body))
		} else {
			req.Body = io.NopCloser(unsizedReader{bytes.NewReader(body)})
			req.ContentLength = -1
		}
		recorder := httptest.NewRecorder()
		NewNativeForwarder(upstream.Client(), newCapturingMeteringClient()).Forward(recorder, req, nativeBinding(upstream.URL))

		if recorder.Code != http.StatusOK {
			t.Fatalf("sized=%t: status = %d, body = %s", sized, recorder.Code, recorder.Body.String())
		}
		if !bytes.Equal(got, body) {
			t.Fatalf("sized=%t: upstream received %d bytes, want the %d sent", sized, len(got), len(body))
		}
	}
}

// Over the limit the request is refused with the vendor's own 413 and never
// sent, whether or not it declared its length.
func TestNativeForwarderRefusesBodyOverLimit(t *testing.T) {
	const limit = 64 << 10
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Write([]byte(`{"usage":{"input_tokens":1,"output_tokens":1}}`))
	}))
	defer upstream.Close()
	forwarder := NewNativeForwarder(upstream.Client(), newCapturingMeteringClient(), WithMaxRequestBodySize(limit))

	atLimit := largeMessagesBody(t, limit)
	overLimit := largeMessagesBody(t, limit+1)
	cases := []struct {
		name   string
		body   []byte
		sized  bool
		status int
	}{
		{"at the limit", atLimit, true, http.StatusOK},
		{"over the limit, declared", overLimit, true, http.StatusRequestEntityTooLarge},
		{"over the limit, chunked", overLimit, false, http.StatusRequestEntityTooLarge},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			calls.Store(0)
			req := nativeRequest(t, "/v1/messages", "")
			reader := bytes.NewReader(tc.body)
			if tc.sized {
				req.Body = io.NopCloser(reader)
				req.ContentLength = int64(len(tc.body))
			} else {
				req.Body = io.NopCloser(unsizedReader{reader})
				req.ContentLength = -1
			}
			recorder := httptest.NewRecorder()
			forwarder.Forward(recorder, req, nativeBinding(upstream.URL))

			if recorder.Code != tc.status {
				t.Fatalf("status = %d, want %d; body = %s", recorder.Code, tc.status, recorder.Body.String())
			}
			if tc.status == http.StatusOK {
				if calls.Load() != 1 {
					t.Fatalf("upstream calls = %d, want 1", calls.Load())
				}
				return
			}
			if calls.Load() != 0 {
				t.Fatalf("upstream calls = %d, want none", calls.Load())
			}
			// The rest of the body is drained, so a caller still sending reads
			// the 413 instead of a reset connection.
			if reader.Len() != 0 {
				t.Fatalf("%d bytes of the refused body left unread", reader.Len())
			}
			var payload struct {
				Type  string `json:"type"`
				Error struct {
					Type    string `json:"type"`
					Message string `json:"message"`
				} `json:"error"`
			}
			if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
				t.Fatalf("expected an Anthropic-shaped error, got %s", recorder.Body.String())
			}
			if payload.Type != "error" || payload.Error.Type != "request_too_large" {
				t.Fatalf("error = %+v, want type request_too_large", payload)
			}
			if !strings.HasPrefix(payload.Error.Message, "agyn: ") || !strings.Contains(payload.Error.Message, "65536-byte limit") {
				t.Fatalf("message = %q, want the platform and the limit named", payload.Error.Message)
			}
		})
	}
}

func platformHandler(t *testing.T, provider *httptest.Server, opts ...Option) http.Handler {
	t.Helper()
	llmClient := &fakeLLMClient{resp: &llmv1.ResolveModelResponse{
		Endpoint:       provider.URL + "/v1/messages",
		Token:          "provider-token",
		RemoteName:     "remote-model",
		OrganizationId: "org-1",
		Protocol:       llmv1.Protocol_PROTOCOL_ANTHROPIC_MESSAGES,
		AuthMethod:     llmv1.AuthMethod_AUTH_METHOD_X_API_KEY,
	}}
	authzClient := &fakeAuthzClient{resp: &authorizationv1.CheckResponse{Allowed: true}}
	return NewHandler(llmClient, authzClient, &fakeMeteringClient{}, &fakeSandboxResolver{}, provider.Client(), opts...)
}

func platformRequest(body []byte) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "http://example.com/v1/messages", bytes.NewReader(body))
	req.Header.Set("anthropic-version", "2023-06-01")
	ctx := identity.WithIdentity(req.Context(), identity.ResolvedIdentity{IdentityID: "user-1", IdentityType: identity.IdentityTypeUser})
	return req.WithContext(ctx)
}

// The platform path rewrites the model name, so the conversation itself is
// compared: the image data must arrive whole.
func TestHandlerForwardsBodyOverOneMiB(t *testing.T) {
	imageData := func(body []byte) string {
		var payload struct {
			Messages []struct {
				Content []struct {
					Source struct {
						Data string `json:"data"`
					} `json:"source"`
				} `json:"content"`
			} `json:"messages"`
		}
		if err := json.Unmarshal(body, &payload); err != nil || len(payload.Messages) != 1 || len(payload.Messages[0].Content) != 1 {
			return ""
		}
		return payload.Messages[0].Content[0].Source.Data
	}
	var got []byte
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"ok":true}`))
	}))
	defer provider.Close()

	body := largeMessagesBody(t, 5<<20/2)
	body = bytes.Replace(body, []byte(`"claude-sonnet-5"`), []byte(`"`+uuid.NewString()+`"`), 1)
	resp := httptest.NewRecorder()
	platformHandler(t, provider).ServeHTTP(resp, platformRequest(body))

	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", resp.Code, resp.Body.String())
	}
	want := imageData(body)
	if len(want) < 2<<20 || imageData(got) != want {
		t.Fatalf("provider received %d bytes without the whole %d-byte image", len(got), len(want))
	}
}
