package proxy

import (
	"encoding/json"
	"log"
	"mime"
	"net/http"
	"strings"

	llmv1 "github.com/agynio/llm-proxy/.gen/go/agynio/api/llm/v1"
)

type nativeStreamErrorRecord struct {
	nativeRefusalRecord
	EventType string `json:"event_type"`
}

// Reuse the relay's event boundaries. Only the first declared error is observed;
// bytes, retries, metering and the downstream client's outcome remain unchanged.
func nativeStreamErrorObserver(meta meteringMetadata, request *http.Request, response *http.Response, protocol llmv1.Protocol) func(string, string, bool) {
	mediaType, _, err := mime.ParseMediaType(response.Header.Get("Content-Type"))
	encoding := response.Header.Get("Content-Encoding")
	if err != nil || mediaType != "text/event-stream" || meta.vendor != "anthropic" || protocol != llmv1.Protocol_PROTOCOL_ANTHROPIC_MESSAGES ||
		(encoding != "" && !strings.EqualFold(encoding, "identity")) {
		return nil
	}
	observed := false
	return func(eventType, data string, complete bool) {
		if observed || eventType != "error" {
			return
		}
		observed = true
		capture := &nativeErrorCapture{}
		_, _ = capture.Write([]byte(data[:min(len(data), nativeErrorCaptureLimit)]))
		capture.truncated = len(data) > nativeErrorCaptureLimit
		record := nativeStreamErrorRecord{nativeRefusalRecord: describeNativeRefusal(meta, request, response, capture, complete), EventType: "error"}
		if record.BodyState == "complete" {
			var payload struct {
				Type  string             `json:"type"`
				Error nativeErrorDetails `json:"error"`
			}
			if err := json.Unmarshal(capture.body[:capture.size], &payload); err != nil || payload.Type != "error" {
				record.ErrorType = "unknown"
			} else if record.ErrorType == "authentication_error" || record.ErrorType == "permission_error" {
				record.AuthReason = nativeAuthReason(payload.Error)
			}
		}
		encoded, _ := json.Marshal(record)
		log.Printf("native: upstream stream error %s", encoded)
	}
}
