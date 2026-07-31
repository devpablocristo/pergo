---
quick_id: "260731-atf"
status: complete
date: 2026-07-31
description: Added a strictly opt-in, network-free WhatsApp mock channel with admin controls and isolated end-to-end verification.
---

# Quick Task 260731-atf: WhatsApp Mock Channel Summary

## Work completed

- Added `PERGO_WHATSAPP_MOCK_ENABLED`, strict false-by-default gating, public `whatsapp_mock` validation, conditional dispatcher registration, and terminal missing-dispatcher behavior with fallback coverage.
- Added a deterministic, concurrent-safe local adapter that respects cancellation and never contacts WhatsApp, Meta, storage, or another external service.
- Added credential-free admin creation, a unique UUID-derived sender identity, connected/default connection state, disabled-state enforcement, conditional UI, badge, Compose wiring, and configuration documentation.
- Added deterministic handler tests plus a build-tagged Testcontainers E2E covering authenticated HTTP ingestion, caller-supplied trace propagation, connection resolution, ephemeral JetStream, worker dispatch, sent state, and correlated outbound audit persistence.

## Commits

- `1800063` — `feat: add opt-in WhatsApp mock dispatcher`
- `c9abc54` — `feat: expose local WhatsApp mock in admin`
- `e0fa3ac` — `test: verify WhatsApp mock end to end`

## Verification

- `go test ./internal/config ./internal/domain ./internal/channel/whatsappmock ./internal/platform/queue -short`
- `go test ./internal/api/handler/admin -run 'TestDeviceHandler_(WhatsAppMockCreate|WhatsAppMockRunTest|WhatsAppMockPairForm)' -count=1`
- `docker compose --env-file .env.example config --quiet`
- `go test ./internal/api/handler -run '^TestCreateMessageWhatsAppMockQueueMapping$' -count=1`
- `go test -tags=integration ./cmd/pergo -run '^TestWhatsAppMockEndToEnd$' -count=1 -v`
- `go test ./... -short`

All commands passed. The E2E used only TestMain-owned PostgreSQL and NATS containers on ephemeral mapped ports.

## Remaining risk

- The mock validates PerGo transport and orchestration only; it intentionally does not validate WhatsApp Web or Meta Cloud API compatibility.
