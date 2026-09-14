package proxy

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agynio/llm-proxy/internal/identity"
)

func TestNativeRefusalClassification(t *testing.T) {
	bearer := `{"error":{"type":"authentication_error","message":"Invalid bearer token"}}`
	for _, tc := range []struct {
		name, body, encoding, state, errorType, reason string
		status                                         int
		incomplete                                     bool
	}{
		{name: "bearer", body: bearer, status: 401, state: "complete", errorType: "authentication_error", reason: "invalid_bearer_token"},
		{name: "unknown authentication message", body: `{"error":{"type":"authentication_error","message":"private-token"}}`, status: 401, state: "complete", errorType: "authentication_error", reason: "unknown"},
		{name: "untrusted type and code", body: `{"error":{"type":"private-token","code":"private-token","message":"private-token"}}`, status: 403, state: "complete", errorType: "unknown", reason: "unknown"},
		{name: "api key", body: `{"error":{"type":"invalid_request_error","code":"invalid_api_key","message":"private-token"}}`, status: 401, state: "complete", errorType: "invalid_request_error", reason: "invalid_api_key"},
		{name: "rate limit", body: `{"error":{"type":"rate_limit_error","message":"private-token"}}`, status: 429, state: "complete", errorType: "rate_limit_error"},
		{name: "not an auth status", body: bearer, status: 500, state: "complete", errorType: "authentication_error"},
		{name: "malformed", body: bearer[:len(bearer)-1], status: 401, state: "invalid_json", errorType: "unknown", reason: "unknown"},
		{name: "string error", body: `{"error":"private-token"}`, status: 401, state: "invalid_json", errorType: "unknown", reason: "unknown"},
		{name: "null", body: `null`, status: 401, state: "complete", errorType: "unknown", reason: "unknown"},
		{name: "array", body: `[]`, status: 401, state: "invalid_json", errorType: "unknown", reason: "unknown"},
		{name: "empty", status: 401, state: "invalid_json", errorType: "unknown", reason: "unknown"},
		{name: "html", body: `<html>private-token</html>`, status: 502, state: "invalid_json", errorType: "unknown"},
		{name: "exact limit", body: bearer + strings.Repeat(" ", nativeErrorCaptureLimit-len(bearer)), status: 401, state: "complete", errorType: "authentication_error", reason: "invalid_bearer_token"},
		{name: "oversized", body: bearer + strings.Repeat(" ", nativeErrorCaptureLimit), status: 401, state: "oversized", errorType: "unknown", reason: "unknown"},
		{name: "incomplete", body: bearer, incomplete: true, status: 401, state: "incomplete", errorType: "unknown", reason: "unknown"},
		{name: "encoded", body: bearer, encoding: "gzip", status: 401, state: "encoded", errorType: "unknown", reason: "unknown"},
		{name: "identity encoding", body: bearer, encoding: "identity", status: 401, state: "complete", errorType: "authentication_error", reason: "invalid_bearer_token"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			capture := &nativeErrorCapture{}
			n, err := capture.Write([]byte(tc.body))
			if err != nil || n != len(tc.body) || capture.size > nativeErrorCaptureLimit {
				t.Fatalf("capture failed: n=%d, size=%d, err=%v", n, capture.size, err)
			}
			request := nativeRequest(t, "/v1/messages?private-token", `{"model":"private-token"}`)
			response := &http.Response{StatusCode: tc.status, Header: http.Header{}}
			response.Header.Set("Content-Encoding", tc.encoding)
			record := describeNativeRefusal(meteringMetadata{vendor: "anthropic"}, request, response, capture, !tc.incomplete)
			if record.Status != tc.status || record.BodyState != tc.state || record.ErrorType != tc.errorType || record.AuthReason != tc.reason {
				t.Fatalf("unexpected classification: %+v", record)
			}
			encoded, err := json.Marshal(record)
			if err != nil || bytes.Contains(encoded, []byte("private-token")) {
				t.Fatal("diagnostic serialization exposed untrusted content")
			}
		})
	}
}

func TestNativeRefusalCaptureIsBoundedAcrossWrites(t *testing.T) {
	capture := &nativeErrorCapture{}
	for range 1024 {
		if n, err := capture.Write(bytes.Repeat([]byte{'x'}, 4097)); n != 4097 || err != nil {
			t.Fatal("the capture must not short-write the response")
		}
	}
	if capture.size != nativeErrorCaptureLimit || !capture.truncated {
		t.Fatalf("capture not bounded: size=%d, truncated=%v", capture.size, capture.truncated)
	}
	if !bytes.Equal(capture.body[:], bytes.Repeat([]byte{'x'}, nativeErrorCaptureLimit)) {
		t.Fatal("captured prefix changed")
	}
	if n, err := capture.Write(nil); n != 0 || err != nil || !capture.truncated {
		t.Fatal("empty write changed truncation")
	}
}

func TestNativeRefusalIdentityAndHeaderProjection(t *testing.T) {
	const id = "A1234567-1234-4321-8123-123456789012"
	meta := meteringMetadata{
		callID: id, orgID: id, modelID: id, modelName: "private-token", threadID: "private-token", vendor: "anthropic",
		identity: identity.ResolvedIdentity{IdentityType: identity.IdentityTypeAgentInstance, IdentityID: id,
			AgentID: id, EnvironmentID: id, WorkloadID: id, ZitiID: "private-token"},
	}
	request := nativeRequest(t, "/v1/messages", `{"model":"private-token"}`)
	request.Header.Set("Authorization", "Bearer private-token")
	request.Header.Set("anthropic-beta", "private-token")
	request.Header.Add("anthropic-beta", "private-token, oauth-2025-04-20")
	response := &http.Response{StatusCode: 401, Header: http.Header{}}
	record := describeNativeRefusal(meta, request, response, &nativeErrorCapture{}, true)
	for _, value := range []string{record.CallID, record.OrganizationID, record.SubscriptionID, record.AgentID, record.AgentInstanceID, record.EnvironmentID, record.WorkloadID} {
		if value != strings.ToLower(id) {
			t.Fatalf("validated identity was not retained: %q", value)
		}
	}
	if !record.CredentialPresent || !record.AnthropicOAuthBeta {
		t.Fatal("expected credential and OAuth beta presence, not their values")
	}
	meta.callID, meta.orgID, meta.modelID, meta.vendor = "private-token", "private-token", "private-token", "private-token"
	meta.identity = identity.ResolvedIdentity{IdentityID: id, IdentityType: identity.IdentityTypeSandbox,
		AgentID: "private-token", EnvironmentID: "private-token", WorkloadID: "private-token"}
	request.Header.Set("Authorization", "Bearer ")
	record = describeNativeRefusal(meta, request, response, &nativeErrorCapture{}, true)
	if record.CredentialPresent || record.AnthropicOAuthBeta || record.Vendor != "unknown" || record.AgentInstanceID != "" {
		t.Fatalf("unsafe metadata projection: %+v", record)
	}
	encoded, _ := json.Marshal(record)
	if bytes.Contains(encoded, []byte("private-token")) || bytes.Contains(encoded, []byte("_id")) {
		t.Fatal("unvalidated metadata was logged")
	}
	for _, value := range []string{"", "00000000-0000-0000-0000-000000000000", "{" + id + "}", "urn:uuid:" + id, strings.Repeat("z", 36)} {
		if diagnosticUUID(value) != "" {
			t.Fatal("invalid/noncanonical identifier accepted")
		}
	}
}

type refusalLogBuffer struct {
	sync.Mutex
	bytes.Buffer
}

func (b *refusalLogBuffer) Write(p []byte) (int, error) {
	b.Lock()
	defer b.Unlock()
	return b.Buffer.Write(p)
}

func (b *refusalLogBuffer) snapshot() string {
	b.Lock()
	defer b.Unlock()
	return b.Buffer.String()
}

func captureRefusalLogs(t *testing.T) *refusalLogBuffer {
	t.Helper()
	previous := log.Writer()
	buffer := &refusalLogBuffer{}
	log.SetOutput(buffer)
	t.Cleanup(func() { log.SetOutput(previous) })
	return buffer
}

func refusalFromLogs(t *testing.T, logs *refusalLogBuffer) nativeRefusalRecord {
	t.Helper()
	output := logs.snapshot()
	if strings.Contains(output, "private-token") {
		t.Fatal("private content reached logs")
	}
	const prefix = "native: upstream refused "
	if strings.Count(output, prefix) != 1 {
		t.Fatalf("expected exactly one native refusal, got %q", output)
	}
	_, encoded, _ := strings.Cut(output, prefix)
	encoded, _, _ = strings.Cut(encoded, "\n")
	var record nativeRefusalRecord
	if err := json.Unmarshal([]byte(encoded), &record); err != nil {
		t.Fatalf("invalid refusal record: %v", err)
	}
	return record
}

func TestNativeForwarderRefusalDiagnosticsPreserveWireAndMetering(t *testing.T) {
	for _, tc := range []struct {
		name, body, state, encoding string
		status                      int
	}{
		{"authentication", `{"error":{"type":"authentication_error","message":"Invalid bearer token"},"private-token":"private-token"}`, "complete", "", 401},
		{"permission", `{"error":{"type":"permission_error","message":"private-token"}}`, "complete", "", 403},
		{"rate", `{"error":{"type":"rate_limit_error","message":"private-token"}}`, "complete", "", 429},
		{"large", strings.Repeat("private-token", nativeErrorCaptureLimit), "oversized", "", 500},
		{"encoded", "private-token", "encoded", "br", 502},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs := captureRefusalLogs(t)
			var calls atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				body, _ := io.ReadAll(r.Body)
				if string(body) != `{"model":"private-token","stream":true}` || r.URL.RequestURI() != "/v1/messages?beta=true" ||
					r.Header.Get("Authorization") != "Bearer private-token" || r.Header.Get("x-api-key") != "" {
					t.Error("upstream request changed or credential substitution failed")
				}
				w.Header().Set("Retry-After", "42")
				w.Header().Set("X-Request-Id", "private-token")
				if tc.encoding != "" {
					w.Header().Set("Content-Encoding", tc.encoding)
				}
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer upstream.Close()
			metering := newCapturingMeteringClient()
			forwarder := NewNativeForwarder(upstream.Client(), metering)
			binding := nativeBinding(upstream.URL)
			binding.Token = "private-token"
			binding.SubscriptionID = "22222222-2222-4222-8222-222222222222"
			request := nativeRequest(t, "/v1/messages?beta=true", `{"model":"private-token","stream":true}`)
			request.Header.Set("x-api-key", "private-token")
			recorder := httptest.NewRecorder()
			forwarder.Forward(recorder, request, binding)
			if calls.Load() != 1 || recorder.Code != tc.status || recorder.Body.String() != tc.body ||
				recorder.Header().Get("Retry-After") != "42" || recorder.Header().Get("X-Request-Id") != "private-token" ||
				recorder.Header().Get("Content-Encoding") != tc.encoding {
				t.Fatal("refusal changed status/headers/body or retried the request")
			}
			record := refusalFromLogs(t, logs)
			if record.Status != tc.status || record.BodyState != tc.state || record.SubscriptionID != binding.SubscriptionID ||
				!record.CredentialPresent || !record.AnthropicOAuthBeta || diagnosticUUID(record.CallID) == "" {
				t.Fatalf("incorrect refusal metadata: %+v", record)
			}
			select {
			case records := <-metering.records:
				labels := labelsOf(records, meteringKindRequest)
				if len(records) != 1 || records[0].GetIdempotencyKey() != record.CallID+"-request" ||
					labels["status"] != meteringStatusFailed || labels["resource_id"] != binding.SubscriptionID {
					t.Fatalf("refusal not correlated with failed metering: %v", labels)
				}
			case <-time.After(time.Second):
				t.Fatal("failed metering not recorded")
			}
		})
	}
}

type refusalRoundTripper func(*http.Request) (*http.Response, error)

func (f refusalRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type refusalRelayWriter struct {
	*httptest.ResponseRecorder
	fail bool
}

func (w *refusalRelayWriter) Write(p []byte) (int, error) {
	if w.fail {
		return 0, errors.New("private-token")
	}
	return w.ResponseRecorder.Write(p)
}

type refusalRelayReader struct {
	writer       *refusalRelayWriter
	reads        int
	fail, closed bool
}

func (r *refusalRelayReader) Read(p []byte) (int, error) {
	r.reads++
	if r.reads == 1 {
		return copy(p, `{"error":{"type":`), nil
	}
	if r.writer.Body.Len() == 0 {
		return 0, errors.New("buffered before relaying")
	}
	if r.fail {
		return 0, errors.New("private-token")
	}
	if r.reads == 2 {
		return copy(p, `"authentication_error","message":"Invalid bearer token"}}`), nil
	}
	return 0, io.EOF
}

func (r *refusalRelayReader) Close() error { r.closed = true; return nil }

func TestNativeForwarderRefusalRelayDoesNotBufferOrDrainOnFailure(t *testing.T) {
	for _, mode := range []string{"streamed", "read failure", "write failure"} {
		t.Run(mode, func(t *testing.T) {
			logs := captureRefusalLogs(t)
			writer := &refusalRelayWriter{ResponseRecorder: httptest.NewRecorder(), fail: mode == "write failure"}
			reader := &refusalRelayReader{writer: writer, fail: mode == "read failure"}
			client := &http.Client{Transport: refusalRoundTripper(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 401, Header: http.Header{}, Body: reader}, nil
			})}
			forwarder := NewNativeForwarder(client, newCapturingMeteringClient())
			forwarder.Forward(writer, nativeRequest(t, "/v1/messages", `{}`), nativeBinding("http://unused.invalid"))
			record := refusalFromLogs(t, logs)
			if !reader.closed || writer.Code != 401 {
				t.Fatal("response not closed or status changed")
			}
			if mode == "streamed" {
				if record.BodyState != "complete" || record.AuthReason != "invalid_bearer_token" || reader.reads != 3 {
					t.Fatal("response was buffered before delivery")
				}
			} else if record.BodyState != "incomplete" || record.ErrorType != "unknown" || record.AuthReason != "unknown" {
				t.Fatal("partial response was classified")
			}
			if mode == "write failure" && reader.reads != 1 {
				t.Fatal("upstream was drained after downstream failure")
			}
		})
	}
}
