---
quick_id: "260801-a1t"
status: complete
date: 2026-08-01
description: Made message ingestion durably idempotent by workspace and request key, with stable receipts and recovery proofs.
---

# Quick Task 260801-a1t: Durable Message Idempotency Summary

## Work completed

- Added migration 031 and a PostgreSQL ledger keyed by
  `(workspace_id, idempotency_key)`, with canonical payload hash, stable
  message receipt, original trace ID and renewable processing lease.
- Added the consumer-owned `MessageIdempotencyStore` port to the message
  handler and wired its PostgreSQL adapter only in `cmd/pergo`.
- Added identical accepted replay, stable `409 idempotency_key_reused`,
  concurrent `425 idempotency_in_progress`, retryable storage failures and
  backward compatibility for legacy requests without the header.
- Extended the MESSAGES JetStream duplicate window to 24 hours so a retry after
  the publish/receipt crash window reuses the original trace ID and publish
  identity.
- Added real PostgreSQL proofs for 16 concurrent claims, restart, expired lease,
  payload mismatch and handler reconstruction after a lost response.
- Documented the allowed header shape, stable receipt, workspace lifetime
  retention and the explicit boundary before provider-level delivery.

## Commits

- `3ddd172` — `docs: plan durable message idempotency`
- `583fe6a` — `feat: make message ingestion durably idempotent`
- `1708bbe` — `test: prove durable response loss recovery`

## Verification

- `go test ./... -race -count=1 -p 1 -coverprofile=/tmp/pergo-h5-coverage-final.out`
- `go vet ./...`
- `golangci-lint run`
- `govulncheck ./...`
- `git diff --check a104268..HEAD`

All commands passed against isolated PostgreSQL 16 and NATS 2.10 JetStream.
The race suite reported 24.6% aggregate statement coverage; golangci-lint
reported zero issues and govulncheck reported zero reachable vulnerabilities.

## Remaining boundary

The guarantee covers HTTP ingestion, stable acceptance and JetStream
publication. It deliberately does not claim provider-level exactly-once if an
external channel accepts a message immediately before PerGo loses its provider
result. Requests without `Idempotency-Key` retain legacy behavior.
