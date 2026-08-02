---
status: complete
task: Harden PerGo runtime profiles, webhook verification, KEK, NATS, media, and shutdown for Cloud Run
---

# PerGo runtime hardening

## Goal

Make the existing PerGo runtime safe to operate as separate API, webhook, worker,
and migration workloads without deploying or publishing anything.

## Tasks

1. Add validated runtime/environment configuration and production-safe defaults.
2. Separate migration execution from normal production startup and gate each profile.
3. Require per-connection Meta webhook secrets and random verification tokens.
4. Make NATS connectivity, TLS, account isolation, and stream replicas explicit.
5. Make media disabled/fail-closed unless an allowed backend is selected.
6. Bound shutdown below Cloud Run's termination window and unblock pull consumers.
7. Add focused tests, CI policy checks, and deployment documentation.
8. Reject media webhooks before any Meta fetch when durable storage is
   unavailable; bound callback bodies and return an explicit retry response.
9. Make the worker the sole WhatsApp Web session owner in split deployments.
10. Reject trivial/public KEK fixtures and add an offline, tenant-scoped WABA
    webhook-secret rotation/backfill job.
11. Stop and await every producer before closing audit; make audit writes,
    campaign stop, and webhook stop race-safe.
12. Close the remaining main-worker stop/install race.
13. Track and await WhatsApp session producers before closing audit/NATS/DB.
14. Redact NATS connection failures so URL userinfo cannot reach logs.
15. Bound HTTP header/read/idle behavior without breaking streaming responses.
16. Pin build/runtime images by digest and run the production binary as non-root.
17. Make active campaign and terminal-webhook shutdown cancellation observable in
    regression tests.
18. Document fail-closed adoption of the NATS environment guard for legacy
    accounts and the mounted-secret permissions required by UID 65532.
19. Make campaign start atomically persist every batch in PostgreSQL before
    entering `sending`, and recover publication idempotently after partial or
    ambiguous JetStream failures.
20. Make campaign batch processing retry the whole batch after any recipient
    persistence/publication failure and mark a campaign complete only after
    every durable batch is confirmed processed.
21. Move campaigns to the versioned `PERGO_V2_CAMPAIGNS` /
    `pergo.v2.campaigns.>` protocol without mutating the legacy stream.
22. Make cancel and delete tenant-scoped atomic lifecycle transitions that
    cannot remove active ledgers or regress a concurrently completed campaign.
23. Bound campaign delay, schedule it durably between batches, add explicit
    AckWait/time-based heartbeats, and process a bounded number of workspaces
    concurrently so one delayed tenant cannot block another.
24. Stream and bound campaign CSV parsing and canonically validate the hidden
    audience payload before persistence.
25. Validate active campaign connections and exact approved WABA template
    snapshots before durable start, neutralize spreadsheet formulas in CSV
    exports, validate the delay constraint, and make enqueue-complete semantics
    explicit.
26. Bound remote provider response bodies, treat ambiguous successful responses
    as uncertain, and redact URLs, credentials, identifiers, and provider bodies
    from returned error chains.
27. Make audit drops observable and actionable, bound/cancel writer shutdown,
    and maintain an idempotent six-month future partition horizon.

## Gates

- `go test ./...`
- real PostgreSQL/NATS `go test ./... -race -count=1 -p 1`
- `go build ./cmd/pergo`
- `go vet ./...`
- worker-profile SIGINT smoke proving sessions/workers stop before audit/NATS
- production-policy tests reject insecure KEK, monolithic runtime, NATS R1, plaintext
  NATS, missing account separation, and in-memory media.
