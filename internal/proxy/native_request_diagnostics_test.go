package proxy

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/agynio/llm-proxy/internal/identity"
)

func TestNativeRequestDiagnosticsOptInAndWirePreservation(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		name := "disabled"
		if enabled {
			name = "enabled"
		}
		t.Run(name, func(t *testing.T) {
			logs := captureRefusalLogs(t)
			body := `{"model":"private-token","stream":false}`
			response := `{"usage":{"input_tokens":1},"private-token":"private-token"}`
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got, _ := io.ReadAll(r.Body)
				if string(got) != body || r.URL.RawQuery != "private-token" || r.Header.Get("Authorization") != "Bearer subscription-token" {
					t.Error("request changed")
				}
				w.Header().Set("Content-Type", "application/json; charset=utf-8")
				w.Header().Set("X-Private", "private-token")
				_, _ = io.WriteString(w, response)
			}))
			defer upstream.Close()
			binding := nativeBinding(upstream.URL)
			binding.Token = "subscription-token"
			f := NewNativeForwarder(upstream.Client(), newCapturingMeteringClient())
			if enabled {
				f = NewDiagnosticNativeForwarder(upstream.Client(), newCapturingMeteringClient())
			}
			w := httptest.NewRecorder()
			f.Forward(w, nativeRequest(t, "/v1/messages?private-token", body), binding)
			if w.Code != 200 || w.Body.String() != response || w.Header().Get("X-Private") != "private-token" {
				t.Fatal("response changed")
			}
			output := logs.snapshot()
			if strings.Contains(output, "private-token") || strings.Contains(output, "subscription-token") {
				t.Fatal("private data logged")
			}
			var records []map[string]any
			for _, line := range strings.Split(output, "\n") {
				_, data, ok := strings.Cut(line, "native: request metadata ")
				if !ok {
					continue
				}
				var record map[string]any
				if err := json.Unmarshal([]byte(data), &record); err != nil {
					t.Fatal(err)
				}
				records = append(records, record)
			}
			if !enabled {
				if len(records) != 0 {
					t.Fatal("diagnostics must default off")
				}
				return
			}
			if len(records) != 2 {
				t.Fatalf("wanted start and response records, got %d", len(records))
			}
			if records[0]["phase"] != "start" || records[1]["phase"] != "response" || records[0]["call_id"] != records[1]["call_id"] || records[1]["status"] != float64(200) || records[1]["response_kind"] != "json" || records[1]["endpoint"] != "anthropic_messages" || records[1]["request_stream"] != false {
				t.Fatalf("incorrect request metadata: %+v", records)
			}
		})
	}
}

func requestMetadata(t *testing.T, output string) []nativeRequestRecord {
	t.Helper()
	var records []nativeRequestRecord
	for _, line := range strings.Split(output, "\n") {
		_, data, ok := strings.Cut(line, "native: request metadata ")
		if !ok {
			continue
		}
		var record nativeRequestRecord
		if err := json.Unmarshal([]byte(data), &record); err != nil {
			t.Fatal(err)
		}
		records = append(records, record)
	}
	return records
}

func TestNativeRequestDiagnosticsMetadataProjection(t *testing.T) {
	const id = "A1234567-1234-4321-8123-123456789012"
	for _, tc := range []struct{ name, vendor, path, contentType, encoding, endpoint, kind, responseEncoding string }{
		{"SSE", "anthropic", "/v1/messages?private-token", "text/event-stream; charset=utf-8", "", "anthropic_messages", "sse", "identity"},
		{"JSON", "anthropic", "/v1/messages/count_tokens", "application/json", "identity", "anthropic_count_tokens", "json", "identity"},
		{"openai", "openai", "/backend-api/codex/responses", "text/event-stream", "", "openai_responses", "sse", "identity"},
		{"encoded", "anthropic", "/v1/messages", "text/event-stream", "private-token", "anthropic_messages", "sse", "encoded"},
		{"unknown path", "anthropic", "/private-token", "application/private-token", "", "other", "other", "identity"},
		{"unknown vendor", "private-token", "/v1/messages", "invalid;private-token", "", "other", "other", "identity"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs := captureRefusalLogs(t)
			r := nativeRequest(t, tc.path, "private-token")
			r.Header.Set("Authorization", "Bearer private-token")
			meta := meteringMetadata{callID: id, orgID: id, modelID: id, modelName: "private-token", threadID: "private-token", vendor: tc.vendor,
				identity: identity.ResolvedIdentity{IdentityType: identity.IdentityTypeAgentInstance, IdentityID: id, AgentID: id, EnvironmentID: id, WorkloadID: id}}
			resp := &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {tc.contentType}, "Content-Encoding": {tc.encoding}, "X-Private": {"private-token"}}}
			logNativeRequest(meta, r, resp, true, "response")
			output := logs.snapshot()
			if len(output) > 1500 || strings.Contains(output, "private-token") {
				t.Fatal("unbounded or private metadata")
			}
			records := requestMetadata(t, output)
			if len(records) != 1 {
				t.Fatal("missing metadata")
			}
			d := records[0]
			if d.Endpoint != tc.endpoint || d.ResponseKind != tc.kind || d.ResponseEncoding != tc.responseEncoding || d.Method != "POST" || !d.CredentialPresent || !d.RequestStream {
				t.Fatalf("wrong projection: %+v", d)
			}
			for _, value := range []string{d.CallID, d.OrganizationID, d.SubscriptionID, d.AgentID, d.AgentInstanceID, d.EnvironmentID, d.WorkloadID} {
				if value != strings.ToLower(id) {
					t.Fatal("identity lost")
				}
			}
		})
	}
	t.Run("reject identifiers", func(t *testing.T) {
		logs := captureRefusalLogs(t)
		r := nativeRequest(t, "/private-token", "")
		r.Method = "private-token"
		r.Header.Del("Authorization")
		meta := meteringMetadata{callID: "private-token", orgID: "private-token", modelID: "private-token", vendor: "private-token", identity: identity.ResolvedIdentity{IdentityType: identity.IdentityTypeSandbox, IdentityID: id, AgentID: "private-token", WorkloadID: "private-token", EnvironmentID: "private-token"}}
		logNativeRequest(meta, r, nil, false, "start")
		output := logs.snapshot()
		d := requestMetadata(t, output)[0]
		if strings.Contains(output, "private-token") || strings.Contains(output, "_id") || d.CredentialPresent || d.Method != "other" || d.Status != 0 || d.ResponseKind != "absent" {
			t.Fatal("invalid metadata retained")
		}
	})
}

func TestNativeRequestDiagnosticsTransportFailure(t *testing.T) {
	logs := captureRefusalLogs(t)
	client := &http.Client{Transport: refusalRoundTripper(func(*http.Request) (*http.Response, error) { return nil, errors.New("private-token") })}
	f := NewDiagnosticNativeForwarder(client, newCapturingMeteringClient())
	w := httptest.NewRecorder()
	f.Forward(w, nativeRequest(t, "/v1/messages?private-token", `{"stream":true}`), nativeBinding("https://vendor.invalid"))
	records := requestMetadata(t, logs.snapshot())
	if len(records) != 2 || records[1].Phase != "transport_error" || records[1].Status != 0 || records[1].ResponseKind != "absent" || records[0].CallID != records[1].CallID || w.Code != 502 || strings.Contains(logs.snapshot(), "private-token") {
		t.Fatal("transport failure metadata changed behavior or exposed data")
	}
}

func TestNativeRequestDiagnosticsObserveHeadersBeforeStream(t *testing.T) {
	logs := captureRefusalLogs(t)
	w := httptest.NewRecorder()
	const frame = "event: message_delta\ndata: {\"usage\":{\"output_tokens\":2}}\n\n"
	reader := &nativeDiagnosticStreamReader{chunks: []string{frame, frame}}
	reader.before = func(index int) {
		records := requestMetadata(t, logs.snapshot())
		if len(records) != 2 || records[1].ResponseKind != "sse" || records[1].Status != 200 || !records[1].RequestStream {
			t.Fatal("headers were not recorded before body read")
		}
		if index == 1 && (w.Body.String() != frame || !w.Flushed) {
			t.Fatal("stream buffered")
		}
	}
	client := &http.Client{Transport: refusalRoundTripper(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(reader)}, nil
	})}
	NewDiagnosticNativeForwarder(client, newCapturingMeteringClient()).Forward(w, nativeRequest(t, "/v1/messages", `{"stream":true}`), nativeBinding("https://vendor.invalid"))
	if w.Body.String() != frame+frame {
		t.Fatal("stream changed")
	}
}
