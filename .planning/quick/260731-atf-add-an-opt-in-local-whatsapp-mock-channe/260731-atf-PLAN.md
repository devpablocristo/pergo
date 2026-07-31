---
quick_id: "260731-atf"
slug: "add-an-opt-in-local-whatsapp-mock-channel"
type: quick
status: planned
requirements: []
depends_on: []
must_haves:
  truths:
    - "whatsapp_mock is disabled by default and cannot be created or dispatched until PERGO_WHATSAPP_MOCK_ENABLED=true."
    - "When enabled, both POST /api/v1/messages and DeviceHandler.RunTest publish whatsapp_mock payloads without contacting WhatsApp, Meta, or any other external account."
    - "A real integration test uses test-owned PostgreSQL and NATS containers on ephemeral mapped ports to prove API -> resolver -> JetStream -> worker -> registry -> mock -> sent status and outbound audit persistence."
    - "The integration test sends a known X-Trace-ID through TraceMiddleware, verifies the echoed response header, and uses that exact trace to query dispatch and audit records."
    - "A nil dispatcher registry and a registry missing the requested channel both return TerminalError; a missing primary dispatcher can still fall back to a registered channel."
    - "Operators can create a connected default whatsapp_mock connection from the admin UI without credentials."
  artifacts:
    - path: "internal/channel/whatsappmock/adapter.go"
      provides: "Concurrent-safe, network-free Dispatcher returning deterministic mock provider IDs."
    - path: "internal/config/config.go"
      provides: "PERGO_WHATSAPP_MOCK_ENABLED opt-in flag with a false default."
    - path: "internal/domain/message.go"
      provides: "whatsapp_mock API channel validation."
    - path: "internal/api/handler/admin/device.go"
      provides: "Server-side gated creation of local mock connections."
    - path: "templates/pages/devices.templ"
      provides: "Conditional admin option and visual identity for the mock channel."
    - path: "cmd/pergo/whatsapp_mock_integration_test.go"
      provides: "Real PostgreSQL/NATS end-to-end proof of API ingestion through sent status and audit."
  key_links:
    - from: "internal/config/config.go"
      to: "cmd/pergo/main.go"
      via: "WhatsAppMockEnabled gates dispatcher registration and is passed to DeviceHandler."
    - from: "internal/domain/message.go"
      to: "internal/channel/whatsappmock/adapter.go"
      via: "The accepted channel value survives ingestion/JetStream and resolves through the dispatcher registry."
    - from: "templates/pages/devices.templ"
      to: "internal/api/handler/admin/device.go"
      via: "The conditional form posts channel=whatsapp_mock; the handler re-checks the feature flag before persistence."
    - from: "internal/api/middleware/trace.go"
      to: "internal/repository/dispatch.go"
      via: "The E2E-supplied X-Trace-ID is propagated through the queue and becomes the lookup key for sent status and audit correlation."
---

# Quick Plan: Add an opt-in local WhatsApp mock channel

## Objective

Provide a safe local `whatsapp_mock` channel that exercises PerGo's real API, routing, JetStream worker, dispatch status, and admin test flow without any WhatsApp/Meta account or outbound provider request.

<task>
<files>
- internal/channel/whatsappmock/adapter.go
- internal/channel/whatsappmock/adapter_test.go
- internal/config/config.go
- internal/config/config_test.go
- internal/domain/message.go
- internal/domain/message_test.go
- internal/platform/queue/orchestrator.go
- internal/platform/queue/orchestrator_test.go
- cmd/pergo/main.go
</files>
<action>
- Add `WhatsAppMockEnabled bool` loaded only from `PERGO_WHATSAPP_MOCK_ENABLED=true`; unset, empty, and other values remain disabled.
- Add `whatsapp_mock` to the public valid-channel set and validation messages so requests can use the normal ingestion contract.
- Implement a stateless `whatsappmock.Adapter` satisfying `channel.Dispatcher`. It must perform no HTTP, WhatsApp, Meta, S3, or account access; respect context cancellation; log a local simulated send without message content; and return a deterministic ID derived from the trace/message ID.
- Register the adapter in `cmd/pergo/main.go` only when the flag is enabled and emit a clear startup warning that the local simulator is active.
- Change `dispatchToChannel` to return `channel.NewTerminalError(...)` both when the registry is nil and when the requested entry is absent. Add explicit unit cases for both branches plus an orchestrator case where an unregistered primary channel advances to a registered fallback and reaches `sent`.
</action>
<verify>
<automated>
go test ./internal/config ./internal/domain ./internal/channel/whatsappmock ./internal/platform/queue -short
</automated>
</verify>
<done>
- The flag defaults to false, the adapter has no external side effects, enabled registration dispatches deterministically, both absent-registry paths are terminal, and terminal absence preserves fallback behavior without false `sent` states.
</done>
</task>

<task>
<files>
- internal/api/handler/admin/device.go
- internal/api/handler/admin/device_test.go
- templates/pages/devices.templ
- templates/pages/devices_templ.go
- .env.example
- docker-compose.yml
- docs/CONFIGURATION.md
</files>
<action>
- Introduce narrow `DeviceConnectionStore` and `DevicePublisher` interfaces containing only the repository/publisher methods used by `DeviceHandler`; keep the production repository and JetStream publisher as their concrete implementations and use deterministic in-memory fakes in handler tests.
- Pass the opt-in flag into `DeviceHandler`; render the `WhatsApp Mock (Local)` option and its credential-free explanatory panel only when enabled.
- Add a server-side `whatsapp_mock` creation branch that rejects disabled requests, stores no credentials, generates a globally unique local sender identity from the connection UUID, and creates the connection as `connected` and default for its workspace/channel.
- Make `RunTest` reject an existing mock connection when the flag is disabled. When enabled, verify with the fake publisher that it emits a `domain.QueueMessage` carrying the selected connection ID, sender identity, recipient, body, trace ID, and `Channel: "whatsapp_mock"`.
- Add a distinct mock badge and route its submit button through the existing synchronous `/admin/devices/create` flow; regenerate tracked templ output.
- Document the false-by-default variable in `.env.example` and `docs/CONFIGURATION.md`, and pass it into the application container from the root Compose file with a `${...:-false}` default.
</action>
<verify>
<automated>
templ generate
go test ./internal/api/handler/admin -run 'TestDeviceHandler_(WhatsAppMockCreate|WhatsAppMockRunTest|WhatsAppMockPairForm)' -count=1
docker compose --env-file .env.example config --quiet
</automated>
</verify>
<done>
- Disabled deployments neither display, create, nor admin-test mock connections; enabled creation and RunTest are covered without PostgreSQL, network access, or `t.Skip`.
</done>
</task>

<task>
<files>
- cmd/pergo/whatsapp_mock_integration_test.go
- internal/api/handler/message_test.go
- docs/TESTING.md
</files>
<action>
- Add handler regression coverage for API acceptance and queue mapping of `whatsapp_mock`.
- Add a build-tagged `integration` test that reuses the package's testcontainers-based `TestMain`: PostgreSQL and NATS JetStream are created for this test process, NATS is reached only through `integrationNATSURL` obtained from its random mapped host port, and the test must never connect to or fall back to `nats://localhost:4222`. Container startup failure is fatal rather than skipped.
- Against those test-owned dependencies, create an isolated workspace/API key/default mock connection and wire the real Echo message route with `TraceMiddleware` and auth, `ConnectionRepository` resolver, JetStream publisher/consumer/worker, dispatcher registry with the real mock adapter, dispatch repository, and audit writer. Use a uniquely named consumer so cleanup targets only this test's resources; delete only that consumer/workspace, while `TestMain` terminates only the containers it created.
- Generate a known trace UUID, send it as `X-Trace-ID` on the authenticated POST to `/api/v1/messages`, assert HTTP 202 and that the response `X-Trace-Id` equals the supplied value, then poll with bounded deadlines using that exact trace until `MessageDispatchRepository.GetByTraceID` reports `status=sent` and `current_channel=whatsapp_mock`, and until the correlated outbound audit row contains `status=sent` plus the deterministic mock response.
- Add local smoke and integration-test recipes that require Docker but never reuse the running development NATS; state explicitly that this proves PerGo transport/orchestration, not WhatsApp protocol compatibility.
- Run the complete short suite after template generation and confirm existing official/unofficial channel validation and dispatcher fallback tests still pass.
</action>
<verify>
<automated>
go test ./... -short
go test -tags=integration ./cmd/pergo -run '^TestWhatsAppMockEndToEnd$' -count=1 -v
</automated>
</verify>
<done>
- Unit coverage remains dependency-free, and the mandatory integration command proves the complete trace-correlated API-to-audit path using only ephemeral test-owned containers, never the active development NATS or an external messaging account.
</done>
</task>
