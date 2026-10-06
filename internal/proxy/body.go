package proxy

import (
	"fmt"
	"io"
)

// DefaultMaxRequestBodySize is the largest request body the proxy accepts,
// 32 MiB. Anthropic's Messages API accepts requests up to 32 MB, and a long
// agent conversation carrying screenshots passes 1 MiB quickly. A request the
// vendor would accept must never be cut short here.
const DefaultMaxRequestBodySize int64 = 32 << 20

// Option configures a Handler or a NativeForwarder.
type Option func(*options)

type options struct {
	maxRequestBodySize int64
}

func defaultOptions() options {
	return options{maxRequestBodySize: DefaultMaxRequestBodySize}
}

func applyOptions(opts []Option) options {
	resolved := defaultOptions()
	for _, opt := range opts {
		if opt != nil {
			opt(&resolved)
		}
	}
	return resolved
}

// WithMaxRequestBodySize sets the largest request body accepted, in bytes. A
// larger request is refused with 413; it is never truncated.
func WithMaxRequestBodySize(limit int64) Option {
	if limit <= 0 {
		panic("max request body size must be positive")
	}
	return func(o *options) { o.maxRequestBodySize = limit }
}

// requestTooLargeError reports a request body over the configured limit.
type requestTooLargeError struct {
	limit int64
	// declared is the request's Content-Length, or -1 when it was not sent.
	declared int64
}

func (e *requestTooLargeError) Error() string {
	if e.declared >= 0 {
		return fmt.Sprintf("request body of %d bytes exceeds the %d-byte limit", e.declared, e.limit)
	}
	return fmt.Sprintf("request body exceeds the %d-byte limit", e.limit)
}

// maxDiscardAfterRefusal bounds how much of a refused body is read and
// discarded so the caller, which is usually still sending, receives the 413
// rather than a reset connection.
const maxDiscardAfterRefusal int64 = 32 << 20

// readRequestBody reads the whole body, or refuses it once it passes limit.
// It never returns a truncated body.
func readRequestBody(body io.Reader, declaredLength, limit int64) ([]byte, error) {
	tooLarge := &requestTooLargeError{limit: limit, declared: declaredLength}
	if declaredLength > limit {
		discard(body)
		return nil, tooLarge
	}
	data, err := io.ReadAll(io.LimitReader(body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		discard(body)
		return nil, tooLarge
	}
	return data, nil
}

func discard(body io.Reader) {
	_, _ = io.Copy(io.Discard, io.LimitReader(body, maxDiscardAfterRefusal))
}
