---
status: awaiting_human_verify
trigger: "Auditar y cerrar publicación NATS exitosa seguida de fallo al marcar queued, redelivery/duplicados entre instancias y ventana finita de deduplicación JetStream."
created: 2026-08-01T00:00:00-03:00
updated: 2026-08-01T19:18:00-03:00
---

## Current Focus

hypothesis: Confirmada: el dispatch necesita claim, lease y fencing PostgreSQL; JetStream debe quedar como optimización, no frontera de exactitud.
test: Prueba de dos orchestrators concurrentes y pruebas de lease/fencing sobre message_dispatches.
expecting: Antes del fix ambos llaman al proveedor; después sólo el dueño del claim puede hacerlo y un sending vencido pasa a uncertain sin redespacho automático.
next_action: Revisión del diff local por el integrador; no se requiere repetir una entrega real para validar la exclusión entre réplicas.
reasoning_checkpoint:
  hypothesis: Dos instancias pasan el mismo status queued antes de que alguna escriba sent.
  confirming_evidence: message_dispatches no tiene claim y sync.Map sólo vive dentro de cada proceso.
  falsification_test: Ejecutar dos DispatchOrchestrator con repositorio compartido y proveedor bloqueado.
  fix_rationale: Un claim corto en PostgreSQL serializa dueños sin mantener una transacción durante la llamada externa.
  blind_spots: Meta no ofrece deduplicación ni consulta exacta por el receipt de PerGo; un error posterior al envío debe quedar uncertain y requiere reconciliación humana.
tdd_checkpoint:
  test_file: internal/repository/dispatch_test.go e internal/platform/queue/worker_test.go
  name: TestMessageDispatchDeliveryClaimFencing y TestOrchestrator_DurableClaimPreventsCrossInstanceDuplicate
  status: green
  failure_output: Antes del fix, las pruebas no compilaban porque ClaimDelivery, ErrDispatchClaimActive, NewUncertainError e IsUncertain todavía no existían.

## Symptoms

expected: Una identidad de mensaje causa como máximo un dispatch concurrente al proveedor aunque NATS redelivere o el ledger no llegue a marcar queued.
actual: JetStream deduplica sólo dentro de su ventana y el worker usa un mapa por proceso; message_dispatches no tiene claim durable.
errors: No hay error explícito; la falla se manifiesta como entrega duplicada entre instancias.
reproduction: Publicar dos mensajes con el mismo trace_id y procesarlos en dos DispatchOrchestrator distintos antes de que alguno marque sent.
started: Riesgo presente en la implementación actual del contrato durable.

## Eliminated

## Evidence

- timestamp: 2026-08-01T00:00:00-03:00
  checked: internal/platform/queue/jetstream.go
  found: MESSAGES configura MaxAge 24h pero no una ventana de deduplicación explícita.
  implication: Nats-Msg-Id no es una frontera durable indefinida.
- timestamp: 2026-08-01T00:00:00-03:00
  checked: internal/platform/queue/orchestrator.go y migration 007/030
  found: message_dispatches sólo registra estado; el único guard concurrente es sync.Map local.
  implication: Instancias distintas pueden ejecutar el proveedor simultáneamente.
- timestamp: 2026-08-01T00:00:00-03:00
  checked: internal/api/handler/message.go
  found: NATS se publica antes de MarkQueued y un fallo deja el claim para recuperación.
  implication: Al expirar el lease puede producirse una segunda publicación legítima que debe tolerarse downstream.
- timestamp: 2026-08-01T00:20:00-03:00
  checked: internal/channel/whatsapp/waba.go, internal/channel/whatsapp/adapter.go y orchestrator.go
  found: Los errores de transporte posteriores al envío eran errores genéricos y activaban failed_transient más NAK/fallback.
  implication: Una respuesta perdida del proveedor podía duplicar el mensaje aunque el claim entre workers funcionara.

## Resolution

root_cause: El estado queued se consultaba sin CAS de propiedad; sync.Map no coordinaba réplicas y Nats-Msg-Id expiraba. Además, una respuesta de transporte perdida activaba retry/fallback aunque el proveedor pudiera haber aceptado el mensaje.
fix: Claim PostgreSQL con token, generación, lease y fencing; un sending vencido o transporte ambiguo queda uncertain sin retry/fallback. Los límites persistentes y públicos usan códigos estables sin error crudo.
verification:
  - PASS — go test -race -p 1 ./... -count=1
  - PASS — go vet ./...
  - PASS — GOFLAGS=-buildvcs=false golangci-lint run ./... (0 issues)
  - PASS — go mod tidy -diff
  - PASS — git diff --check
files_changed:
  - internal/platform/postgres/migrations/032_add_dispatch_delivery_claim.sql
  - internal/repository/dispatch.go
  - internal/repository/dispatch_test.go
  - internal/platform/queue/orchestrator.go
  - internal/platform/queue/worker_test.go
  - internal/platform/queue/jetstream.go
  - internal/platform/queue/jetstream_test.go
  - internal/channel/dispatcher.go
  - internal/channel/dispatcher_test.go
  - internal/channel/whatsapp/waba.go
  - internal/channel/whatsapp/waba_test.go
  - internal/channel/whatsapp/adapter.go
  - internal/channel/telegram/telegram.go
  - internal/channel/instagram/adapter.go
  - internal/channel/email/mautic.go
  - internal/channel/email/ses.go
  - internal/channel/email/smtp.go
  - docs/adr/0009-durable-provider-delivery-claim.md
  - docs/API.md
  - docs/architecture/04-concurrency-performance.md
  - docs/architecture/05-resilience-error-handling.md
