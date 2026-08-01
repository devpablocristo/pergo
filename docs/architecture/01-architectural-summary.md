# 1. Architectural Summary & Principles

## PRD analysis

PerGo is a self-hosted omnichannel CPaaS gateway. The business intent is
deceptively simple: accept one standardised JSON payload, route it to any
number of heterogeneous messaging providers, and report delivery progress
back to the caller via webhooks — without per-message markup and without
surrendering data custody to a third party.

The PRD already prescribes a coherent stack (Echo + Templ/HTMX, NATS
JetStream, whatsmeow, PostgreSQL, `golang.org/x/time/rate`). The
architectural work that remains is not "choose a framework" — it is to
wire these primitives together with the **smallest amount of concurrency
machinery that survives the stated load envelope**, and to make the
hard parts (idempotency, trace propagation, backpressure, session
lifecycle) explicit rather than implicit.

## Functional shape

- **Ingestion gateway**: `POST /api/v1/messages` → validate caller-supplied
  idempotency and trace identities → acquire a fenced PostgreSQL claim →
  enqueue to NATS JetStream → persist the stable receipt → `202 Accepted`.
- **Channel workers**: JetStream consumers, one logical consumer per
  channel type, dispatching to provider SDKs (whatsmeow WebSocket,
  WABA REST, Telegram REST).
- **Connection manager**: long-lived per-device WebSocket sessions with
  in-memory registry, persistent device store in PostgreSQL.
- **Routing engine**: ordered `fallback_channels` resolved iteratively.
- **Audit engine**: every state transition written to a partitioned
  `audit_logs` table via a buffered batch writer.
- **Admin panel**: server-rendered (Echo + Templ + HTMX), QR pairing,
  connection telemetry, audit review.

## Distributed-systems challenges identified

| Challenge | Why it matters here |
|-----------|---------------------|
| **At-least-once vs. exactly-once** | JetStream `WorkQueuePolicy` guarantees at-least-once. A fenced PostgreSQL delivery claim serializes replicas by `trace_id`; an expired `sending` claim or ambiguous transport response becomes internal state `uncertain` instead of being sent again. |
| **Backpressure & queue depth** | The 1,000-message-per-session limit must be enforced *before* enqueue, not after. A naive "enqueue then check" leaks memory under burst. The ingest path must read JetStream stream info (or a local counter) and return `429` synchronously. |
| **Latency budget** | 50ms p99 ingestion leaves ~45ms for validation + broker hand-off after TLS/JSON decode. Any synchronous DB write on the hot path (auth, audit) must be removed or async. |
| **Trace-context propagation** | Trace-ID must survive three context boundaries: HTTP → NATS message headers → worker `context.Context` → SQL `pgx.Tx`. Loss at any boundary breaks the 100% trace-correlation SLO. |
| **Connection lifecycle** | WhatsApp Web sessions are stateful WebSockets. A worker crash or rolling deploy must not lose paired devices — device identity lives in PostgreSQL, the in-memory client is rebuildable. |
| **Partial failure & cascading drops** | A single provider rate-limiting must not stall the JetStream consumer for unrelated channels. Per-session rate limiters isolate backpressure to the offending session. |
| **Audit write contention** | High-throughput synchronous inserts into `audit_logs` will dominate DB write latency and violate the 50ms budget. Batched async writes with a bounded buffer are mandatory. |
| **Secret custody** | WhatsApp session credentials and channel tokens are high-value. AES-256-GCM at rest is required; key management (envelope vs. KMS) is a deployment concern, not an application one. |
| **Memory ceiling** | <512MB on 2 vCPU. Every `goroutine`-per-X and every buffered channel has a measurable footprint. Buffer sizes and worker counts must be derived from the load envelope, not picked at random. |

## Design posture

- Treat PostgreSQL plus NATS JetStream as the **ingress durability protocol**:
  PostgreSQL owns request identity, receipt, lease and fencing; JetStream owns
  queued work. No transaction spans both systems, and retries reuse the stable
  trace so at-least-once recovery does not create a second logical message.
- Treat `message_dispatches` as the **provider-delivery ownership protocol**:
  one expiring token/generation may call the provider, and every state write is
  fenced. The transaction ends before the external call.
- Treat PostgreSQL as the **system of record** for identity (workspaces,
  API keys, device sessions, audit logs) — never as a hot-path queue.
- Keep the new synchronous persistence narrowly scoped to the ingress ledger.
  Its claim and completion transactions are short and contain no provider or
  broker network call.
- Make the channel layer a **plugin boundary** (consumer-side interface)
  so unofficial protocol breakage in whatsmeow never touches the core.
