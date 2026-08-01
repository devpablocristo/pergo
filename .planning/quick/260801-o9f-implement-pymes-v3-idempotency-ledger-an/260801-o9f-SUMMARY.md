---
quick_id: "260801-o9f"
status: complete
---

# Summary: Durable Pymes v3 delivery contract

PerGo's production `POST /api/v1/messages` now has a durable, workspace-scoped
idempotency boundary and returns one deterministic receipt that remains stable
through dispatch and delivery webhooks.

## Implemented

1. Added migration `030_create_message_ingress_ledger.sql` and a PostgreSQL
   repository for claim, lease recovery, fencing and queued replay. Database
   transactions are always completed before JetStream publication.
2. Required `Idempotency-Key` and `X-Trace-ID` when the production ledger is
   wired. Identical replay returns `202` with the original receipt and
   timestamp; changed content or trace returns `409`; a live concurrent claim
   returns `425` with `Retry-After`.
3. Bound the deterministic receipt to `QueueMessage`, provider dispatch and
   `sent`, `delivered`, `read` and `failed` events. Provider status updates keep
   the legacy `messages.status_updated` event and also emit the canonical
   `webhooks.events` payload consumed by Pymes.
4. Scoped provider-receipt lookup and uniqueness by workspace and removed
   phone, recipient and message-body values from the touched operational logs.
5. Wired the ledger into the production composition root and documented the
   API, concurrency protocol and architecture decision.
6. Added migration `031_repair_audit_partitions.sql` because a fresh install in
   a month not hard-coded by the historical migrations produced an unattached
   audit table. The repair preserves rows and attaches current/next monthly
   partitions.
7. Added migration `032_add_dispatch_delivery_claim.sql`. Provider calls are
   serialized across replicas by a PostgreSQL token/generation lease, terminal
   completion is fenced, and an expired `sending` state becomes internal
   `uncertain` instead of being re-sent. Ambiguous transport errors stop retry
   and fallback, exposing only `failed` plus `DELIVERY_UNCERTAIN`.
8. Set the JetStream duplicate window to the complete 24-hour stream retention.
   It is a load optimization; PostgreSQL remains the correctness boundary.

## Verification

- Fresh PostgreSQL 16 migration from version 0 through 32: passed.
- Repository, inbound and queue PostgreSQL integration tests: passed.
- Full Go suite, serialized as CI does: passed.
- Exact CI test command with PostgreSQL, NATS JetStream and race detector:
  passed.
- `go vet ./...`: passed.
- `golangci-lint run ./...`: 0 issues.
- `go mod tidy -diff`, `gofmt` and `git diff --check`: clean.

No commit, push, publication or external deployment was performed.
