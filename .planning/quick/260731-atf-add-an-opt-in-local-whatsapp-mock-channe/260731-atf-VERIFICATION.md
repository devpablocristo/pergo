---
quick_id: "260731-atf"
status: passed
verified: 2026-07-31
commits:
  - "1800063"
  - "c9abc54"
  - "e0fa3ac"
---

# Verification Report: Opt-in Local WhatsApp Mock Channel

## Outcome

All must-haves in `260731-atf-PLAN.md` are implemented and covered by passing automated verification. The local mock remains strictly opt-in, its dispatcher performs no provider or network I/O, and the isolated integration test proves the trace-correlated API-to-audit path.

## Must-Have Verification

### 1. Safe opt-in and disabled behavior

- `internal/config/config.go` enables the feature only when `PERGO_WHATSAPP_MOCK_ENABLED` is exactly `"true"`; unset, empty, `"false"`, `"TRUE"`, and `"1"` remain disabled.
- `cmd/pergo/main.go` registers `whatsapp_mock` only under `cfg.WhatsAppMockEnabled` and emits an explicit simulator warning.
- `DeviceHandler.Create` rejects disabled mock creation with HTTP 403.
- `DeviceHandler.RunTest` rejects existing mock connections while the feature is disabled.
- With no registered mock dispatcher, worker dispatch cannot invoke the adapter and returns a terminal missing-dispatcher error.
- Evidence: `TestWhatsAppMockEnabledIsStrictOptIn`, `TestDeviceHandler_WhatsAppMockCreateDisabled`, and `TestDeviceHandler_WhatsAppMockRunTestDisabled`.

**Status:** Passed.

### 2. Network-free dispatcher and deterministic response

- `internal/channel/whatsappmock/adapter.go` uses only context checks, SHA-256, hex encoding, and structured logging; it has no HTTP client, WhatsApp/Meta client, storage client, credential lookup, or other provider/account access.
- The adapter excludes message content from logs, respects context cancellation, and derives a stable `whatsapp-mock-<hash>` provider ID from the trace ID, falling back to the message ID.
- `POST /api/v1/messages` maps the normal API request to a `whatsapp_mock` queue payload.
- Admin `RunTest` publishes the selected connection ID, sender identity, recipient, body, channel, and the same generated trace ID to `messages.outbound`.
- Evidence: adapter unit tests, `TestCreateMessageWhatsAppMockQueueMapping`, and the enabled `RunTest` handler test.

**Status:** Passed.

### 3. Conditional Admin creation

- `PairForm` passes the feature flag into the template; `templates/pages/devices.templ` renders the `WhatsApp Mock (Local)` option and mock-specific panel only when enabled.
- The server independently rechecks the flag before persistence.
- Enabled creation generates a UUID-backed sender identity, stores no credentials, and creates a `connected`, default `whatsapp_mock` connection.
- The mock badge and synchronous `/admin/devices/create` flow are present in the generated template output.
- Evidence: `TestDeviceHandler_WhatsAppMockPairForm` and `TestDeviceHandler_WhatsAppMockCreateEnabled`.

**Status:** Passed.

### 4. Terminal registry errors and fallback

- `DispatchOrchestrator.dispatchToChannel` wraps both a nil registry and a missing channel entry with `channel.NewTerminalError`.
- `TestDispatchToChannelMissingRegistryIsTerminal` and `TestDispatchToChannelMissingEntryIsTerminal` cover the two branches explicitly.
- `TestOrchestrator_FallbackLoop/Missing_primary_dispatcher_falls_back_to_registered_channel` proves a missing primary advances immediately to the registered fallback, acknowledges the work item, and persists `sent` with `current_channel=whatsapp_mock`.

**Status:** Passed.

### 5. Isolated end-to-end flow, trace, sent state, and audit

- `cmd/pergo/whatsapp_mock_integration_test.go` refuses an empty/default/localhost NATS URL and consumes the mapped URL supplied by the package `TestMain`.
- `TestMain` owns PostgreSQL and NATS Testcontainers, uses mapped host ports, fails fatally on startup failure, runs migrations, and terminates only the containers it created.
- The test wires real auth, `TraceMiddleware`, message ingestion, default-connection resolution, JetStream publisher/consumer, worker, orchestrator, dispatcher registry, mock adapter, dispatch repository, and audit writer.
- It supplies a known `X-Trace-ID`, asserts HTTP 202 and the exact echoed response header, then queries dispatch and audit records using that same trace.
- The observed dispatch reaches `status=sent` and `current_channel=whatsapp_mock`; the outbound audit reaches `status=sent` with the deterministic mock provider response.
- The consumer has a unique name and cleanup targets only that consumer and the test workspace.

**Status:** Passed.

## Automated Checks

- `go test ./internal/config ./internal/domain ./internal/channel/whatsappmock ./internal/platform/queue -short` — passed.
- `go test ./internal/api/handler/admin -run 'TestDeviceHandler_(WhatsAppMockCreate|WhatsAppMockRunTest|WhatsAppMockPairForm)' -count=1` — passed.
- `go test ./internal/api/handler -run '^TestCreateMessageWhatsAppMockQueueMapping$' -count=1` — passed.
- `docker compose --env-file .env.example config --quiet` — passed; Docker only reported the existing obsolete `version` warning.
- `go test -tags=integration ./cmd/pergo -run '^TestWhatsAppMockEndToEnd$' -count=1 -v` — passed against test-owned PostgreSQL and NATS containers.
- `go test ./... -short` — passed.
- `git diff --check 1800063^..e0fa3ac` — passed.

## Commit and Working-Tree Scope

- The feature range `1800063^..e0fa3ac` contains the planned mock implementation, Admin/configuration work, and tests.
- `Makefile` and `context/devInfra/docker-compose.yml` are not included in any of the three feature commits.
- Both remain separate, preexisting working-tree modifications and were not changed during verification.

## Conclusion

**Verification status: PASSED.** The implementation satisfies every declared truth, artifact, and key link for quick task `260731-atf`. The test proves PerGo transport/orchestration only, intentionally not real WhatsApp Web or Meta Cloud API compatibility.
