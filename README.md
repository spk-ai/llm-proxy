# LLM Proxy

LLM Proxy service that exposes the OpenAI Responses API, resolves models, authorizes access, and forwards requests to external providers.

Architecture: [LLM Proxy](https://github.com/agynio/architecture/blob/main/architecture/llm-proxy.md)

## Local Development

Full setup: [Local Development](https://github.com/agynio/architecture/blob/main/architecture/operations/local-development.md)

### Prepare environment

```bash
git clone https://github.com/agynio/bootstrap.git
cd bootstrap
chmod +x apply.sh
./apply.sh -y
```

See [bootstrap](https://github.com/agynio/bootstrap) for details.

### Run from sources

```bash
# Deploy once (exit when healthy)
devspace dev

# Watch mode (streams logs, re-syncs on changes)
devspace dev -w
```

### Run tests

```bash
devspace run test-e2e
```

See [E2E Testing](https://github.com/agynio/architecture/blob/main/architecture/operations/e2e-testing.md).

## Native Refusal Diagnostics

Native-mode non-2xx responses produce one `native: upstream refused` JSON log
record, correlated with metering by `call_id` (the request usage record's
idempotency key is `<call_id>-request`). It contains HTTP status, validated UUID
ownership fields, credential/OAuth-beta presence booleans and allowlisted error
categories. It does not contain credential values, arbitrary headers, URLs,
model names, prompts or raw provider messages.

The body is still relayed as received, without an additional request or retry.
At most 8 KiB is captured while relaying. Oversized, interrupted, encoded and
invalid JSON bodies remain unclassified. `auth_reason` describes a recognized
provider response, not an independently established credential failure cause.

Anthropic can also report an [error inside an HTTP 200 SSE response](https://platform.claude.com/docs/en/api/errors).
The native path now observes the first declared `event: error` frame on an
unencoded Anthropic Messages event stream and emits a separate
`native: upstream stream error` record. Its `status` remains the actual HTTP
status, with `event_type=error`; an authentication category is not relabeled as
an HTTP 401. The existing relay supplies event boundaries and still forwards
every byte immediately. The additional capture is limited to 8 KiB; oversized,
unterminated and malformed frames do not yield a recognized authentication
reason. Healthy output, other vendors/protocols and encoded streams are ignored.

This is a diagnostic-only change: it adds no request retry, changes no client
outcome and leaves existing metering semantics unchanged. The relay's existing
event buffering and success-metering policy for HTTP 200 error streams are
separate hardening work. A first-frame diagnostic and a bounded log snapshot
cannot establish that no later error occurred, and do not prove the cause of a
runtime authentication failure without a correlated live observation.

Generate the API as in the Dockerfile, then run the model-free refusal tests:

```bash
go test -race ./internal/proxy -run 'TestNative.*(Refusal|Stream)' -count=1
```
