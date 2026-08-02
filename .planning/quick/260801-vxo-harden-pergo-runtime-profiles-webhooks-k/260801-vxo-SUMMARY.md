---
status: complete
task: Harden PerGo runtime profiles, webhook verification, KEK, NATS, media, and shutdown for Cloud Run
completed: 2026-08-02
---

# Summary

PerGo now runs as explicit `api`, `webhook`, `worker`, or `migrate` profiles.
Outside development/test, startup rejects the monolithic profile, automatic
migrations, missing/invalid KEK, plaintext or unscoped NATS, production R1,
in-memory media, provider mocks, and the development admin password on API.

Meta WABA POST callbacks require a tenant-specific `X-Hub-Signature-256`;
the connection is selected by the payload's unique `phone_number_id`. GET
verification accepts only a strong persisted token. Admin and seed paths now
capture the Meta app secret and reject predictable tokens.

WABA callback bodies are capped at 2 MiB before lookup/HMAC. Attachments are
never downgraded to text: when durable media is disabled or download cannot
complete, parsing returns a typed retry condition, performs zero Meta requests
in disabled mode, and the webhook responds `503 Retry-After: 300`.

JetStream supports multiple URLs, credentials files, custom CAs, TLS server
names, configurable replicas, and a durable environment/account guard.
Production media is explicitly disabled in this build and fails before network
access. Pull workers stop promptly; WhatsApp sessions and all workers are
awaited before audit closes, and concurrent late audit writes return a stable
error instead of racing a closed channel. The complete shutdown budget is eight
seconds.

Split production topology now assigns WhatsApp Web session ownership only to
the worker. API pairing, QR polling and disconnect are disabled (and hidden
from its form) until a durable coordinator exists. Existing WABA connections
can be safely backfilled or rotated with the tenant-scoped
`rotate-waba-webhook-secrets` job, reading provider secrets only from mounted
files and preserving the other encrypted Meta credentials.

Campaign start now uses a PostgreSQL outbox. Every recipient batch and its
payload hash are committed in the same transaction that changes the campaign
to `sending`; retries validate the frozen snapshot and cannot create partial
batch sets. The worker leases and republishes due batches after failures or
restart, validates broker payloads against PostgreSQL, retries the whole batch
after any recipient persistence/publication failure, and completes only after
all durable batches are confirmed. Stable per-recipient trace IDs make retries
idempotent without placing phone numbers in trace metadata.

Campaign traffic now uses a new physical `PERGO_V2_CAMPAIGNS` WorkQueue stream
and `pergo.v2.campaigns.>` subjects; rollout never updates the legacy
`CAMPAIGNS` LimitsPolicy stream in place. The v2 consumer has an explicit
two-minute AckWait, elapsed-time heartbeats, and a bounded concurrent lifecycle.
Inter-batch delay is capped at one hour and persisted in PostgreSQL: completing
one batch schedules the next without sleeping on a JetStream delivery, while
another workspace remains claimable immediately.

Campaign lifecycle mutations are now atomic and tenant-scoped. Active or
scheduled campaigns cannot be deleted, and cancellation only transitions
scheduled/sending rows, so concurrent completion cannot regress to cancelled.
CSV preview reads bounded rows incrementally instead of buffering the complete
file; request bytes, rows, columns and fields are capped. Creation strictly
decodes and revalidates recipient phones, duplicates, variables and skipped
rows instead of trusting the browser's hidden preview payload.

Campaign creation and durable start now reject disconnected connections and
WABA templates that are cross-tenant, attached to another connection, not
approved, malformed, unsupported, or missing required parameter mappings. The
exact language and body-parameter count are frozen in each durable batch and
the worker no longer assumes `pt_BR`. CSV downloads neutralize formula-leading
cells, migration 036 backfills and validates its delay constraint, and the UI
calls `completed` "Enfileirada": this state means every outbound command was
accepted by the durable queue, not that the provider delivered every message.

Remote Telegram, Instagram, WABA, Mautic, SES, and Meta template responses are
bounded to 1 MiB. Ambiguous successful responses are retried as uncertain, while
provider bodies, request URLs, credentials, and sensitive provider identifiers
never enter returned error chains.

Audit queue overflow now returns an explicit error and every overflow, failed
flush, or forced shutdown drop increments `audit_drops`. Shutdown is bounded and
cancels PostgreSQL operations. The idempotent `migrate` job maintains the
current audit partition plus six future months under an advisory lock.

## Validation

- `go test ./... -count=1`
- real temporary PostgreSQL/NATS `go test ./... -race -count=1 -p 1`
- focused media, KEK, audit, worker-stop and rotation suites with `-race`
- real WABA ingestion/status receipts and encrypted credential rotation
- real worker-profile startup and SIGINT shutdown-order smoke
- `go vet ./...`
- `go build ./cmd/pergo`
- `go run github.com/a-h/templ/cmd/templ@v0.3.1020 generate ./...`
- `docker compose config --quiet`
- `git diff --check`
- focused campaign repository/worker/admin suites against real PostgreSQL and
  NATS, including partial publish recovery, concurrent final batches, v2 stream
  isolation, active lifecycle rejection, durable cross-tenant delay, temporal
  heartbeats, bounded CSV input, canonical audience validation, exact approved
  WABA template snapshots and CSV formula neutralization
- focused campaign repository/worker suites with `-race -count=1 -p 1`
- remote-response bounds and redaction canaries for Telegram, Instagram, WABA,
  Mautic, SES, and Meta template administration, including full error chains
- audit queue overflow, flush failure, cancellation, non-cooperative shutdown,
  drain, and current-plus-six-month partition maintenance with `-race`

## Residual deployment work

- Existing WABA connections must run the documented secret rotation/backfill
  job before callbacks are enabled.
- Real production media remains disabled until the mock AWS SDK replacements
  are removed and an actual object-storage adapter is validated.
- WhatsApp Web onboarding remains disabled in split production topology until a
  durable API-to-worker pairing coordinator is designed; WABA Cloud is usable.
- NATS credentials, CA material, distinct STG/PRD accounts, and an R3 cluster
  are deployment inputs; this task created no provider resources and deployed
  nothing.
