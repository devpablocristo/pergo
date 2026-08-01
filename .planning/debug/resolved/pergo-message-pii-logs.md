---
status: resolved
trigger: "PerGo logs message recipients in plaintext; redact message PII before Pymes integrates the service."
created: 2026-08-01
updated: 2026-08-01
---

# Debug Session: PerGo message PII logs

## Symptoms

- expected: Logs contain operational identifiers and stable codes, but never recipient phone numbers, message bodies, tokens, or credentials.
- actual: The message API handler and WhatsApp adapter attach the plaintext recipient to structured log records.
- errors: No runtime error; this is an information-disclosure defect.
- timeline: Present in the current `master` implementation.
- reproduction: Submit or dispatch a WhatsApp message and inspect structured logs for the `to` attribute.

## Current Focus

- hypothesis: Confirmed. Direct attributes bypassed redaction because every process entrypoint installed an unfiltered JSON handler.
- test: Capture structured logs with unique E.164, message-body, and token markers, including a nested attribute.
- expecting: No marker reaches output while operational trace IDs remain available.
- next_action: complete
- reasoning_checkpoint: Central redaction is the safety boundary; removing known sensitive fields and provider response bodies reduces exposure before that boundary.
- tdd_checkpoint: red-green-complete

## Evidence

- Before the fix, `go test ./internal/platform/obs` failed to compile because the required safe logger and redaction value did not exist.
- `cmd/pergo/main.go` created two raw `slog.NewJSONHandler` instances.
- Message, WhatsApp, inbound, session, queue, and admin paths attached recipient identifiers directly.
- Chatwoot and Typebot errors embedded provider response bodies that callers could log through the `error` attribute.
- A date-specific legacy partition repair also made the repository suite fail on 2026-08-01; migration 030 and rolling partition maintenance repaired that independent CI blocker.

## Eliminated

- Keeping per-call redaction alone was rejected because new call sites could bypass it.
- Redacting every error entirely was rejected because stable operational error detail remains useful; provider response bodies and URL-bearing errors were removed at their sources instead.

## Resolution

- root_cause: PerGo had no process-wide logging policy, and several call sites treated recipients, message bodies, provider identities, URLs, and response bodies as ordinary diagnostic fields.
- fix: Added a single JSON logger with recursive sensitive-key redaction, routed production entrypoints through it, preserved the configured logger when adding trace context, removed known PII fields, and stopped embedding provider bodies in propagated errors.
- verification: `go test ./...`; exact CI test command with race detector, serial execution, and coverage; `go vet ./...`; `golangci-lint run`; `govulncheck ./...`; source search confirms the only raw JSON handler is inside the safe constructor.
- files_changed: `internal/platform/obs`, process entrypoints, message/provider/session/inbound log call sites, provider HTTP error handling, and rolling audit partition maintenance.
