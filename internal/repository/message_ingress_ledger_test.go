package repository

import (
	"context"
	"crypto/sha256"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestMessageIngressLedgerClaimReplayAndMismatch(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	ctx := context.Background()
	wsRepo := NewWorkspaceRepository(pool)
	ws, err := wsRepo.Create(ctx, "ingress_replay_"+uuid.New().String())
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	defer func() { _ = wsRepo.Delete(ctx, ws.ID) }()

	repo := NewMessageIngressLedgerRepository(pool)
	key := "pymes.notify." + uuid.New().String()
	traceID := "pymes.v1." + ws.ID.String() + ".notification"
	hash := sha256.Sum256([]byte("stable-payload"))
	receipt := uuid.New()

	gotReceipt, _, token, generation, replay, _, err := repo.Claim(
		ctx, ws.ID, key, hash, traceID, receipt, time.Second,
	)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if replay {
		t.Fatal("first claim must not be a replay")
	}
	if gotReceipt != receipt || token == uuid.Nil || generation != 1 {
		t.Fatalf("unexpected claim receipt=%s token=%s generation=%d", gotReceipt, token, generation)
	}

	queuedAt := time.Now().UTC().Truncate(time.Microsecond)
	if err := repo.MarkQueued(ctx, ws.ID, key, token, generation, queuedAt); err != nil {
		t.Fatalf("mark queued: %v", err)
	}

	replayReceipt, replayAt, replayToken, replayGeneration, replay, _, err := repo.Claim(
		ctx, ws.ID, key, hash, traceID, uuid.New(), time.Second,
	)
	if err != nil {
		t.Fatalf("claim replay: %v", err)
	}
	if !replay || replayReceipt != receipt || replayToken != uuid.Nil || replayGeneration != generation {
		t.Fatalf(
			"unexpected replay replay=%v receipt=%s token=%s generation=%d",
			replay,
			replayReceipt,
			replayToken,
			replayGeneration,
		)
	}
	if !replayAt.Equal(queuedAt) {
		t.Fatalf("queued_at=%s, want %s", replayAt, queuedAt)
	}

	differentHash := sha256.Sum256([]byte("different-payload"))
	_, _, _, _, _, _, err = repo.Claim(
		ctx, ws.ID, key, differentHash, traceID, receipt, time.Second,
	)
	if !errors.Is(err, ErrIngressIdempotencyKeyReused) {
		t.Fatalf("different payload error=%v, want ErrIngressIdempotencyKeyReused", err)
	}

	_, _, _, _, _, _, err = repo.Claim(
		ctx, ws.ID, key, hash, traceID+".changed", receipt, time.Second,
	)
	if !errors.Is(err, ErrIngressIdempotencyKeyReused) {
		t.Fatalf("different trace error=%v, want ErrIngressIdempotencyKeyReused", err)
	}

	_, _, _, _, _, _, err = repo.Claim(
		ctx, ws.ID, key+".changed", hash, traceID, uuid.New(), time.Second,
	)
	if !errors.Is(err, ErrIngressIdempotencyKeyReused) {
		t.Fatalf("reused trace error=%v, want ErrIngressIdempotencyKeyReused", err)
	}
}

func TestMessageIngressLedgerScopesTraceIdentityByWorkspace(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	ctx := context.Background()
	wsRepo := NewWorkspaceRepository(pool)
	firstWorkspace, err := wsRepo.Create(ctx, "ingress_trace_a_"+uuid.NewString())
	if err != nil {
		t.Fatalf("create first workspace: %v", err)
	}
	defer func() { _ = wsRepo.Delete(ctx, firstWorkspace.ID) }()
	secondWorkspace, err := wsRepo.Create(ctx, "ingress_trace_b_"+uuid.NewString())
	if err != nil {
		t.Fatalf("create second workspace: %v", err)
	}
	defer func() { _ = wsRepo.Delete(ctx, secondWorkspace.ID) }()

	repo := NewMessageIngressLedgerRepository(pool)
	traceID := "customer-selected-trace"
	hash := sha256.Sum256([]byte("same-payload"))
	for _, workspaceID := range []uuid.UUID{firstWorkspace.ID, secondWorkspace.ID} {
		if _, _, _, _, replay, _, err := repo.Claim(
			ctx,
			workspaceID,
			"same-idempotency-key",
			hash,
			traceID,
			uuid.New(),
			time.Second,
		); err != nil || replay {
			t.Fatalf("workspace %s claim replay=%v err=%v", workspaceID, replay, err)
		}
	}
}

func TestMessageIngressLedgerConcurrentClaimHasOneOwner(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	ctx := context.Background()
	wsRepo := NewWorkspaceRepository(pool)
	ws, err := wsRepo.Create(ctx, "ingress_concurrency_"+uuid.New().String())
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	defer func() { _ = wsRepo.Delete(ctx, ws.ID) }()

	repo := NewMessageIngressLedgerRepository(pool)
	key := "pymes.concurrent." + uuid.New().String()
	traceID := "pymes.v1." + ws.ID.String() + ".concurrent"
	hash := sha256.Sum256([]byte("same-payload"))
	receipt := uuid.New()

	const callers = 24
	var (
		owners atomic.Int32
		active atomic.Int32
		wg     sync.WaitGroup
	)
	errs := make(chan error, callers)
	start := make(chan struct{})
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			gotReceipt, _, token, generation, replay, _, claimErr := repo.Claim(
				ctx, ws.ID, key, hash, traceID, receipt, 2*time.Second,
			)
			if claimErr == nil {
				if replay || gotReceipt != receipt || token == uuid.Nil || generation != 1 {
					errs <- errors.New("owner received an invalid claim")
					return
				}
				owners.Add(1)
				return
			}
			if errors.Is(claimErr, ErrIngressClaimActive) {
				active.Add(1)
				return
			}
			errs <- claimErr
		}()
	}
	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("concurrent claim: %v", err)
	}
	if got := owners.Load(); got != 1 {
		t.Fatalf("owners=%d, want 1", got)
	}
	if got := active.Load(); got != callers-1 {
		t.Fatalf("active claims=%d, want %d", got, callers-1)
	}
}

func TestMessageIngressLedgerExpiredLeaseUsesFencing(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	ctx := context.Background()
	wsRepo := NewWorkspaceRepository(pool)
	ws, err := wsRepo.Create(ctx, "ingress_fencing_"+uuid.New().String())
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	defer func() { _ = wsRepo.Delete(ctx, ws.ID) }()

	repo := NewMessageIngressLedgerRepository(pool)
	key := "pymes.fencing." + uuid.New().String()
	traceID := "pymes.v1." + ws.ID.String() + ".fencing"
	hash := sha256.Sum256([]byte("same-payload"))
	receipt := uuid.New()

	_, _, oldToken, oldGeneration, _, _, err := repo.Claim(
		ctx, ws.ID, key, hash, traceID, receipt, 2*time.Second,
	)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}

	_, _, _, _, _, retryAfter, err := repo.Claim(
		ctx, ws.ID, key, hash, traceID, receipt, 2*time.Second,
	)
	if !errors.Is(err, ErrIngressClaimActive) || retryAfter <= 0 {
		t.Fatalf("active claim err=%v retry_after=%s", err, retryAfter)
	}

	if _, err := pool.Exec(ctx, `
		UPDATE message_ingress_ledger
		SET claim_expires_at = clock_timestamp() - interval '1 second'
		WHERE workspace_id = $1
		  AND idempotency_key = $2
	`, ws.ID, key); err != nil {
		t.Fatalf("expire claim: %v", err)
	}
	recoveredReceipt, _, newToken, newGeneration, replay, _, err := repo.Claim(
		ctx, ws.ID, key, hash, traceID, receipt, time.Second,
	)
	if err != nil {
		t.Fatalf("recover claim: %v", err)
	}
	if replay || recoveredReceipt != receipt || newToken == oldToken || newGeneration != oldGeneration+1 {
		t.Fatalf(
			"invalid recovered claim replay=%v receipt=%s token=%s generation=%d",
			replay,
			recoveredReceipt,
			newToken,
			newGeneration,
		)
	}

	if err := repo.MarkQueued(ctx, ws.ID, key, oldToken, oldGeneration, time.Now()); !errors.Is(err, ErrIngressClaimLost) {
		t.Fatalf("stale mark error=%v, want ErrIngressClaimLost", err)
	}
	queuedAt := time.Now().UTC()
	if err := repo.MarkQueued(ctx, ws.ID, key, newToken, newGeneration, queuedAt); err != nil {
		t.Fatalf("current mark queued: %v", err)
	}
}
