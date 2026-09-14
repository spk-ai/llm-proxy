package proxy

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	llmv1 "github.com/agynio/llm-proxy/.gen/go/agynio/api/llm/v1"
)

func TestNativeForwarderObservesAnthropicStreamErrors(t *testing.T) {
	const payload = `{"type":"error","error":{"type":"authentication_error","message":"Invalid bearer token"},"private-token":"private-token"}`
	for _, tc := range []struct {
		name, body, state, errorType, reason string
	}{
		{"complete", "event: error\ndata: " + payload + "\n\n", "complete", "authentication_error", "invalid_bearer_token"},
		{"crlf and multiline", "event: error\r\ndata: {\r\ndata: \"type\":\"error\",\"error\":{\"type\":\"authentication_error\",\"message\":\"Invalid bearer token\"}}\r\n\r\n", "complete", "authentication_error", "invalid_bearer_token"},
		{"unterminated frame", "event: error\ndata: " + payload + "\n", "incomplete", "unknown", ""},
		{"oversized", "event: error\ndata: {\"type\":\"error\",\"padding\":\"" + strings.Repeat("private-token", nativeErrorCaptureLimit) + "\"}\n\n", "oversized", "unknown", ""},
		{"malformed", "event: error\ndata: {private-token\n\n", "invalid_json", "unknown", ""},
		{"unknown values", "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"private-token\",\"message\":\"private-token\"}}\n\n", "complete", "unknown", ""},
		{"wrong envelope", "event: error\ndata: {\"type\":\"content_block_delta\",\"error\":{\"type\":\"authentication_error\"}}\n\n", "complete", "unknown", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs := captureRefusalLogs(t)
			var calls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
				w.Header().Set("X-Request-Id", "private-token")
				_, _ = io.WriteString(w, tc.body)
			}))
			defer upstream.Close()
			metering := newCapturingMeteringClient()
			forwarder := NewNativeForwarder(upstream.Client(), metering)
			binding := nativeBinding(upstream.URL)
			binding.Token = "private-token"
			binding.SubscriptionID = "22222222-2222-4222-8222-222222222222"
			recorder := httptest.NewRecorder()
			forwarder.Forward(recorder, nativeRequest(t, "/v1/messages?beta=true", `{"model":"claude-sonnet-5","stream":true}`), binding)
			if calls.Load() != 1 || recorder.Code != 200 || recorder.Body.String() != tc.body || recorder.Header().Get("X-Request-Id") != "private-token" {
				t.Fatal("stream observation changed the response or retried a request")
			}
			output := logs.snapshot()
			const marker = "native: upstream stream error "
			if strings.Count(output, marker) != 1 || strings.Contains(output, "private-token") {
				t.Fatalf("expected one credential-safe stream diagnostic; marker count=%d", strings.Count(output, marker))
			}
			_, encoded, _ := strings.Cut(output, marker)
			encoded, _, _ = strings.Cut(encoded, "\n")
			var record nativeRefusalRecord
			if err := json.Unmarshal([]byte(encoded), &record); err != nil {
				t.Fatal(err)
			}
			if record.Status != 200 || record.BodyState != tc.state || record.ErrorType != tc.errorType || record.AuthReason != tc.reason ||
				record.SubscriptionID != binding.SubscriptionID || !record.CredentialPresent || !record.AnthropicOAuthBeta || diagnosticUUID(record.CallID) == "" {
				t.Fatalf("incorrect stream error metadata: %+v", record)
			}
			select {
			case records := <-metering.records:
				if labelsOf(records, meteringKindRequest)["status"] != meteringStatusSuccess {
					t.Fatal("diagnostic-only observation changed metering semantics")
				}
			case <-time.After(time.Second):
				t.Fatal("metering not recorded")
			}
		})
	}
}

func TestNativeStreamObserverIgnoresOtherResponses(t *testing.T) {
	for _, tc := range []struct {
		name, vendor, mediaType, encoding string
		protocol                          llmv1.Protocol
	}{
		{"not SSE", "anthropic", "application/json", "", llmv1.Protocol_PROTOCOL_ANTHROPIC_MESSAGES},
		{"missing content type", "anthropic", "", "", llmv1.Protocol_PROTOCOL_ANTHROPIC_MESSAGES},
		{"encoded", "anthropic", "text/event-stream", "br", llmv1.Protocol_PROTOCOL_ANTHROPIC_MESSAGES},
		{"other vendor", "openai", "text/event-stream", "", llmv1.Protocol_PROTOCOL_ANTHROPIC_MESSAGES},
		{"other protocol", "anthropic", "text/event-stream", "", llmv1.Protocol_PROTOCOL_RESPONSES},
	} {
		t.Run(tc.name, func(t *testing.T) {
			response := &http.Response{StatusCode: 200, Header: http.Header{}}
			response.Header.Set("Content-Type", tc.mediaType)
			response.Header.Set("Content-Encoding", tc.encoding)
			if nativeStreamErrorObserver(meteringMetadata{vendor: tc.vendor}, nativeRequest(t, "/", ""), response, tc.protocol) != nil {
				t.Fatal("unexpected native event observer")
			}
		})
	}
}

type nativeDiagnosticStreamReader struct {
	chunks []string
	index  int
	before func(int)
}

func (r *nativeDiagnosticStreamReader) Read(p []byte) (int, error) {
	if r.index == len(r.chunks) {
		return 0, io.EOF
	}
	if r.before != nil {
		r.before(r.index)
	}
	n := copy(p, r.chunks[r.index])
	r.chunks[r.index] = r.chunks[r.index][n:]
	if r.chunks[r.index] == "" {
		r.index++
	}
	return n, nil
}

func TestNativeStreamObservationIsIncrementalAndBounded(t *testing.T) {
	logs := captureRefusalLogs(t)
	const prefix = "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"text\":\"private-token\"}\n\n"
	const failure = "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"private-token\"}}\n\n"
	const marker = "native: upstream stream error "
	recorder := httptest.NewRecorder()
	reader := &nativeDiagnosticStreamReader{chunks: []string{prefix, failure, strings.Repeat(failure, 10)}}
	reader.before = func(index int) {
		if index == 1 && (recorder.Body.String() != prefix || !recorder.Flushed || strings.Contains(logs.snapshot(), marker)) {
			t.Fatal("healthy frame was buffered or misclassified")
		}
		if index == 2 && (recorder.Body.String() != prefix+failure || strings.Count(logs.snapshot(), marker) != 1) {
			t.Fatal("complete error was not observed before reading the next frame")
		}
	}
	client := &http.Client{Transport: refusalRoundTripper(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(reader)}, nil
	})}
	forwarder := NewNativeForwarder(client, newCapturingMeteringClient())
	forwarder.Forward(recorder, nativeRequest(t, "/v1/messages", `{"stream":true}`), nativeBinding("https://vendor.invalid"))
	if recorder.Body.String() != prefix+strings.Repeat(failure, 11) || strings.Count(logs.snapshot(), marker) != 1 || strings.Contains(logs.snapshot(), "private-token") {
		t.Fatal("stream bytes changed, log bound exceeded or private content logged")
	}
}
