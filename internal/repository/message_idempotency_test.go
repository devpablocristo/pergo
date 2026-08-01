package repository

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestMessageIdempotencyConcurrentClaimAndReplay(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	workspaceRepo := NewWorkspaceRepository(pool)
	workspace, err := workspaceRepo.Create(
		ctx,
		"message_idempotency_"+uuid.NewString(),
	)
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	t.Cleanup(func() {
		_ = workspaceRepo.Delete(context.Background(), workspace.ID)
	})

	const callers = 16
	repository := NewMessageIdempotencyRepository(pool)
	start := make(chan struct{})
	type result struct {
		record   MessageIdempotency
		acquired bool
		err      error
	}
	results := make(chan result, callers)
	var wait sync.WaitGroup
	wait.Add(callers)
	for index := 0; index < callers; index++ {
		go func(index int) {
			defer wait.Done()
			<-start
			record, acquired, claimErr := repository.Acquire(
				ctx,
				workspace.ID,
				"pymes:notification:concurrent",
				"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				"trace-"+uuid.NewString(),
				time.Minute,
			)
			results <- result{
				record: record, acquired: acquired, err: claimErr,
			}
		}(index)
	}
	close(start)
	wait.Wait()
	close(results)

	var first MessageIdempotency
	acquiredCount := 0
	for current := range results {
		if current.err != nil {
			t.Fatalf("concurrent Acquire: %v", current.err)
		}
		if first.MessageID == uuid.Nil {
			first = current.record
		}
		if current.record.MessageID != first.MessageID ||
			current.record.QueuedAt != first.QueuedAt ||
			current.record.TraceID != first.TraceID {
			t.Fatalf(
				"claim returned another receipt: first=%+v current=%+v",
				first,
				current.record,
			)
		}
		if current.acquired {
			acquiredCount++
			first = current.record
		}
	}
	if acquiredCount != 1 {
		t.Fatalf("acquired leases = %d, want 1", acquiredCount)
	}

	accepted, err := repository.MarkAccepted(ctx, first)
	if err != nil {
		t.Fatalf("MarkAccepted: %v", err)
	}
	if !accepted.Accepted() {
		t.Fatalf("accepted record status = %q", accepted.Status)
	}

	// A new repository value models a process restart. The caller may have
	// lost the original HTTP response, but must receive the original receipt
	// without acquiring another publish lease.
	restarted := NewMessageIdempotencyRepository(pool)
	replayed, acquired, err := restarted.Acquire(
		ctx,
		workspace.ID,
		"pymes:notification:concurrent",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"another-client-trace",
		time.Minute,
	)
	if err != nil {
		t.Fatalf("Acquire after restart: %v", err)
	}
	if acquired {
		t.Fatal("accepted replay unexpectedly acquired a publish lease")
	}
	if replayed.MessageID != accepted.MessageID ||
		replayed.QueuedAt != accepted.QueuedAt ||
		replayed.TraceID != accepted.TraceID {
		t.Fatalf(
			"response-loss replay changed receipt: accepted=%+v replayed=%+v",
			accepted,
			replayed,
		)
	}

	_, _, err = restarted.Acquire(
		ctx,
		workspace.ID,
		"pymes:notification:concurrent",
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		accepted.TraceID,
		time.Minute,
	)
	if !errors.Is(err, ErrMessageIdempotencyConflict) {
		t.Fatalf("mismatched replay error = %v, want conflict", err)
	}
}

func TestMessageIdempotencyExpiredLeaseRecoversStableReceipt(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	workspaceRepo := NewWorkspaceRepository(pool)
	workspace, err := workspaceRepo.Create(
		ctx,
		"message_idempotency_lease_"+uuid.NewString(),
	)
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	t.Cleanup(func() {
		_ = workspaceRepo.Delete(context.Background(), workspace.ID)
	})

	repository := NewMessageIdempotencyRepository(pool)
	first, acquired, err := repository.Acquire(
		ctx,
		workspace.ID,
		"pymes:notification:lease",
		"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		"trace-"+uuid.NewString(),
		20*time.Millisecond,
	)
	if err != nil || !acquired {
		t.Fatalf("first Acquire acquired=%v err=%v", acquired, err)
	}
	time.Sleep(40 * time.Millisecond)

	restarted := NewMessageIdempotencyRepository(pool)
	recovered, acquired, err := restarted.Acquire(
		ctx,
		workspace.ID,
		first.IdempotencyKey,
		first.PayloadHash,
		"replacement-trace",
		time.Minute,
	)
	if err != nil || !acquired {
		t.Fatalf("recovered Acquire acquired=%v err=%v", acquired, err)
	}
	if recovered.MessageID != first.MessageID ||
		recovered.QueuedAt != first.QueuedAt ||
		recovered.TraceID != first.TraceID {
		t.Fatalf(
			"expired lease recovery changed receipt: first=%+v recovered=%+v",
			first,
			recovered,
		)
	}
}
