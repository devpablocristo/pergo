---
quick_id: "260801-a1t"
status: passed
verified: 2026-08-01
commits:
  - "3ddd172"
  - "583fe6a"
  - "1708bbe"
---

# Verification Report: Durable Message Idempotency

## Outcome

Every must-have in `260801-a1t-PLAN.md` is implemented and covered by passing
automated checks. PerGo now gives Pymes a durable workspace-scoped ingestion
receipt without taking ownership of Pymes tenant data or exposing provider
credentials.

## Must-Have Verification

### 1. Workspace-scoped durable identity

- Migration 031 makes `(workspace_id, idempotency_key)` the primary key and
  constrains hash, trace, status and lease consistency.
- `MessageIdempotencyRepository.Acquire` inserts or locks the ledger row in one
  transaction and rejects a different payload hash.
- Workspace deletion cascades to the ledger; otherwise records do not silently
  expire and old keys cannot be reused.

**Status:** Passed.

### 2. Stable replay, conflict and concurrency

- The original `message_id`, `queued_at` and `trace_id` are allocated once and
  returned on every accepted replay.
- A changed payload returns HTTP 409 with `idempotency_key_reused`.
- One of 16 concurrent PostgreSQL claimants obtains the lease; every claimant
  observes the same receipt.
- A concurrent request still being accepted returns HTTP 425 and `Retry-After`.

**Status:** Passed.

### 3. Restart and response-loss recovery

- A fresh repository instance reads the original accepted receipt after a
  process restart and does not acquire another publish lease.
- `TestCreateMessageDurableResponseLossSurvivesProcessRestart` reconstructs the
  handler around the same PostgreSQL ledger after discarding the first HTTP
  response. The replay preserves receipt and trace while the ingestor remains
  at exactly one call.
- An expired processing lease can be recovered without changing receipt or
  trace.

**Status:** Passed.

### 4. Publish/receipt crash window

- The HTTP retry always reuses the trace stored in the ledger.
- JetStream sets that trace as `Nats-Msg-Id`.
- `EnsureStream` configures and tests a 24-hour duplicate window, covering the
  documented HTTP retry horizon after a publish whose receipt was not recorded.
- The documentation explicitly avoids claiming provider-level exactly-once.

**Status:** Passed.

### 5. Architecture, security and compatibility

- `MessageIdempotencyStore` is declared by the consuming handler; the concrete
  repository is constructed in `cmd/pergo`.
- The handler logs operational IDs and channel only, never payload, message
  body, destination or idempotency key.
- No dependency, container, license or public provider credential ownership
  changed.
- Legacy callers without `Idempotency-Key` remain compatible, while Pymes uses
  the durable path.

**Status:** Passed.

## Automated Checks

- Full race suite with PostgreSQL/NATS:
  `go test ./... -race -count=1 -p 1
  -coverprofile=/tmp/pergo-h5-coverage-final.out` — passed.
- `go vet ./...` — passed.
- `golangci-lint run` — passed with 0 issues.
- `govulncheck ./...` — zero reachable vulnerabilities.
- `git diff --check a104268..HEAD` — passed.
- GitHub Actions workflow dispatch `30696667106` — passed on the functional
  implementation before the additional PostgreSQL handler proof.

## Conclusion

**Verification status: PASSED.** The durable acceptance contract is ready for
review and deployment with migration 031 applied before the new runtime.
