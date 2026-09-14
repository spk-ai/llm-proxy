package proxy

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/agynio/llm-proxy/internal/identity"
	"github.com/google/uuid"
)

const nativeErrorCaptureLimit = 8 * 1024

// Capture only a bounded prefix while the original bytes flow to the client.
// Oversized or interrupted bodies are never classified from a partial object.
type nativeErrorCapture struct {
	body      [nativeErrorCaptureLimit]byte
	size      int
	truncated bool
}

func (c *nativeErrorCapture) Write(p []byte) (int, error) {
	n := copy(c.body[c.size:], p)
	c.size += n
	c.truncated = c.truncated || n < len(p)
	return len(p), nil
}

type nativeRefusalRecord struct {
	Status             int    `json:"status"`
	Vendor             string `json:"vendor"`
	CallID             string `json:"call_id,omitempty"`
	OrganizationID     string `json:"organization_id,omitempty"`
	SubscriptionID     string `json:"subscription_id,omitempty"`
	AgentID            string `json:"agent_id,omitempty"`
	AgentInstanceID    string `json:"agent_instance_id,omitempty"`
	EnvironmentID      string `json:"environment_id,omitempty"`
	WorkloadID         string `json:"workload_id,omitempty"`
	CredentialPresent  bool   `json:"credential_present"`
	AnthropicOAuthBeta bool   `json:"anthropic_oauth_beta"`
	BodyState          string `json:"body_state"`
	ErrorType          string `json:"error_type"`
	AuthReason         string `json:"auth_reason,omitempty"`
}

type nativeErrorDetails struct {
	Type    string `json:"type"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

func describeNativeRefusal(meta meteringMetadata, request *http.Request, response *http.Response, capture *nativeErrorCapture, complete bool) nativeRefusalRecord {
	record := nativeRefusalRecord{
		Status:            response.StatusCode,
		Vendor:            "unknown",
		CallID:            diagnosticUUID(meta.callID),
		OrganizationID:    diagnosticUUID(meta.orgID),
		SubscriptionID:    diagnosticUUID(meta.modelID),
		AgentID:           diagnosticUUID(meta.identity.AgentID),
		EnvironmentID:     diagnosticUUID(meta.identity.EnvironmentID),
		WorkloadID:        diagnosticUUID(meta.identity.WorkloadID),
		CredentialPresent: strings.TrimSpace(strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer ")) != "",
		BodyState:         "complete",
		ErrorType:         "unknown",
	}
	if meta.vendor == "anthropic" || meta.vendor == "openai" {
		record.Vendor = meta.vendor
	}
	if meta.identity.IdentityType == identity.IdentityTypeAgentInstance {
		record.AgentInstanceID = diagnosticUUID(meta.identity.IdentityID)
	}
	if record.Vendor == "anthropic" {
		for _, value := range request.Header.Values("anthropic-beta") {
			for _, flag := range strings.Split(value, ",") {
				if strings.TrimSpace(flag) == "oauth-2025-04-20" {
					record.AnthropicOAuthBeta = true
				}
			}
		}
	}
	authRefusal := response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden
	if authRefusal {
		record.AuthReason = "unknown"
	}
	switch {
	case !complete:
		record.BodyState = "incomplete"
		return record
	case capture.truncated:
		record.BodyState = "oversized"
		return record
	case response.Header.Get("Content-Encoding") != "" && !strings.EqualFold(response.Header.Get("Content-Encoding"), "identity"):
		record.BodyState = "encoded"
		return record
	}

	var payload struct {
		Error nativeErrorDetails `json:"error"`
	}
	if err := json.Unmarshal(capture.body[:capture.size], &payload); err != nil {
		record.BodyState = "invalid_json"
		return record
	}
	// Provider responses can echo credentials or prompts, even in type/code
	// fields. Only fixed labels leave this function; raw messages never do.
	switch payload.Error.Type {
	case "authentication_error", "permission_error", "rate_limit_error", "invalid_request_error",
		"not_found_error", "api_error", "overloaded_error", "request_too_large", "server_error", "insufficient_quota":
		record.ErrorType = payload.Error.Type
	}
	if authRefusal {
		record.AuthReason = nativeAuthReason(payload.Error)
	}
	return record
}

func nativeAuthReason(detail nativeErrorDetails) string {
	switch {
	case strings.EqualFold(strings.TrimSpace(detail.Message), "Invalid bearer token"):
		return "invalid_bearer_token"
	case detail.Code == "invalid_api_key":
		return "invalid_api_key"
	default:
		return "unknown"
	}
}

func diagnosticUUID(value string) string {
	if len(value) != 36 {
		return ""
	}
	id, err := uuid.Parse(value)
	if err != nil || id == uuid.Nil {
		return ""
	}
	return id.String()
}
