# ADR 0009: Durable provider delivery claim

## Status

Accepted.

## Context

The ingress ledger commits before publishing to JetStream and marks itself
queued afterward. If NATS accepts the message but the second database write
fails, lease recovery may publish the same stable trace again. JetStream
`Nats-Msg-Id` deduplication has a finite window, and a process-local map cannot
coordinate workers running in different replicas.

Checking `message_dispatches.status` before calling a provider is therefore
insufficient: two workers can both observe `queued` and send the same WhatsApp
message concurrently.

A separate ambiguity exists after the provider call. If the request was
accepted but the transport response was lost, retry or fallback can create a
second external message. Meta does not expose an exact query keyed by PerGo's
receipt, so PerGo cannot infer that outcome safely.

## Decision

Provider delivery is guarded by a PostgreSQL claim stored on
`message_dispatches`.

- A short transaction locks the dispatch row and allocates
  `delivery_claim_token`, a monotonically increasing generation and an
  expiration.
- The transaction commits before any provider call.
- Every state transition made by the worker compares the token, generation and
  unexpired lease.
- The claim is renewed with one bounded update before each fallback attempt.
- Provider calls receive a context timeout shorter than the lease.
- A live competing worker NAKs its broker message until the claim owner
  completes.
- A `queued` or `failed_transient` claim that expires can be recovered with a
  higher generation; the previous owner is fenced.
- A `sending` claim that expires becomes internal state `uncertain` and is
  never dispatched automatically.
- Transport errors after a provider send are typed `UncertainError`; they do
  not retry and do not advance fallback channels.
- Successful provider ID storage, `sent` state and claim release occur in one
  fenced database update.
- JetStream uses a 24-hour duplicate window, equal to outbound stream
  retention, as a load optimization. PostgreSQL remains the correctness
  boundary after that window expires.

Internal `sending`, `failed_transient` and `uncertain` states are not delivery
webhook event names. The public event vocabulary remains
`queued|sent|delivered|read|failed`. An uncertain result is emitted as
`failed` with the stable error code `DELIVERY_UNCERTAIN`.

No database transaction or lock is held while a provider is called.

## Consequences

Broker duplicates and concurrent replicas cannot cause simultaneous provider
calls for one trace. A worker crash or transport loss after declaring
`sending` prefers at-most-once delivery: the operator must reconcile an
internal `uncertain` row instead of PerGo guessing whether the provider
accepted it.

Automatic reconciliation remains blocked for adapters whose provider exposes
neither an idempotency key nor an exact result query. A future adapter may add
reconciliation behind that capability, but it must not silently re-send.
