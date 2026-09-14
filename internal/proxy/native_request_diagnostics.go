package proxy

import (
	"encoding/json"
	"log"
	"mime"
	"net/http"
	"strings"

	"github.com/agynio/llm-proxy/internal/identity"
)

type nativeRequestRecord struct {
	Phase             string `json:"phase"`
	CallID            string `json:"call_id,omitempty"`
	OrganizationID    string `json:"organization_id,omitempty"`
	SubscriptionID    string `json:"subscription_id,omitempty"`
	AgentID           string `json:"agent_id,omitempty"`
	AgentInstanceID   string `json:"agent_instance_id,omitempty"`
	EnvironmentID     string `json:"environment_id,omitempty"`
	WorkloadID        string `json:"workload_id,omitempty"`
	Vendor            string `json:"vendor"`
	Method            string `json:"method"`
	Endpoint          string `json:"endpoint"`
	RequestStream     bool   `json:"request_stream"`
	CredentialPresent bool   `json:"credential_present"`
	Status            int    `json:"status"`
	ResponseKind      string `json:"response_kind"`
	ResponseEncoding  string `json:"response_encoding"`
}

func logNativeRequest(meta meteringMetadata, request *http.Request, response *http.Response, stream bool, phase string) {
	if phase != "start" && phase != "response" && phase != "transport_error" {
		return
	}
	record := nativeRequestRecord{
		Phase: phase, CallID: diagnosticUUID(meta.callID), OrganizationID: diagnosticUUID(meta.orgID),
		SubscriptionID: diagnosticUUID(meta.modelID), AgentID: diagnosticUUID(meta.identity.AgentID),
		EnvironmentID: diagnosticUUID(meta.identity.EnvironmentID), WorkloadID: diagnosticUUID(meta.identity.WorkloadID),
		Vendor: "unknown", Method: "other", Endpoint: "other", RequestStream: stream,
		CredentialPresent: strings.TrimSpace(strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer ")) != "",
		ResponseKind:      "absent", ResponseEncoding: "absent",
	}
	if meta.vendor == "anthropic" || meta.vendor == "openai" {
		record.Vendor = meta.vendor
	}
	if meta.identity.IdentityType == identity.IdentityTypeAgentInstance {
		record.AgentInstanceID = diagnosticUUID(meta.identity.IdentityID)
	}
	switch request.Method {
	case "GET", "POST", "HEAD":
		record.Method = request.Method
	}
	if request.URL != nil {
		switch {
		case record.Vendor == "anthropic" && request.URL.Path == "/v1/messages":
			record.Endpoint = "anthropic_messages"
		case record.Vendor == "anthropic" && request.URL.Path == "/v1/messages/count_tokens":
			record.Endpoint = "anthropic_count_tokens"
		case record.Vendor == "openai" && (request.URL.Path == "/v1/responses" || request.URL.Path == "/backend-api/codex/responses"):
			record.Endpoint = "openai_responses"
		}
	}
	if response != nil {
		if response.StatusCode >= 100 && response.StatusCode <= 599 {
			record.Status = response.StatusCode
		}
		record.ResponseKind = "other"
		mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
		if err == nil {
			switch mediaType {
			case "text/event-stream":
				record.ResponseKind = "sse"
			case "application/json":
				record.ResponseKind = "json"
			}
		}
		record.ResponseEncoding = "encoded"
		encoding := strings.TrimSpace(response.Header.Get("Content-Encoding"))
		if encoding == "" || strings.EqualFold(encoding, "identity") {
			record.ResponseEncoding = "identity"
		}
	}
	// All strings are fixed labels or validated UUIDs. Raw headers, URLs, model
	// names, body keys and values are deliberately excluded, even on errors.
	encoded, _ := json.Marshal(record)
	log.Printf("native: request metadata %s", encoded)
}
