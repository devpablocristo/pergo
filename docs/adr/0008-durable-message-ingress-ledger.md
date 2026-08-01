# ADR 0008: Durable message ingress ledger

## Status

Accepted.

## Context

Pymes retries notification delivery after timeouts. A random HTTP response ID
and JetStream deduplication alone cannot prove whether a lost response was
accepted, cannot reject an idempotency key reused with changed content, and
cannot keep later delivery webhooks tied to the original receipt.

Holding a PostgreSQL transaction open while publishing to NATS would turn a
network timeout into a long database lock and still would not create an atomic
cross-system transaction.

## Decision

`POST /api/v1/messages` uses `message_ingress_ledger`, keyed by
`(workspace_id, idempotency_key)`.

- The stored hash covers `X-Trace-ID`, a separator, and the exact HTTP body.
- The public receipt is a deterministic UUIDv5 derived from workspace and
  idempotency key, and is persisted as the compatibility boundary.
- A short claim transaction allocates `claim_token`, `claim_generation`, and
  `claim_expires_at`.
- The transaction commits before the NATS publish.
- NATS receives the stable trace as `Nats-Msg-Id`.
- A second short compare-and-swap marks the row `queued`.
- A live concurrent claim returns `425`; an identical queued replay returns
  the stored `202`; changed content or trace returns `409`.
- An expired lease increments the generation. A stale owner cannot complete
  the row. Duplicate broker publication remains safe through the stable trace
  and the fenced PostgreSQL provider-delivery claim defined by ADR 0009;
  JetStream's finite duplicate window is an optimization only.
- `message_dispatches.receipt_id` carries the receipt through `sent`,
  `delivered`, `read`, and `failed` webhook events.

## Consequences

The HTTP hot path gains two bounded PostgreSQL operations, so latency and pool
pressure must be measured. In exchange, response loss and concurrent retries
have explicit, testable behavior without a distributed transaction. Legacy
test handlers may omit the ledger, but the production composition root must
inject it and therefore requires both identity headers.

The downstream worker also performs bounded claim/renew/complete operations.
No database transaction spans a provider call. An expired `sending` claim or
ambiguous transport result is recorded internally as `uncertain`, exposed as
`failed` plus `DELIVERY_UNCERTAIN`, and never sent automatically again.
