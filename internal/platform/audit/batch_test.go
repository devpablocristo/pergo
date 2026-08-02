package audit

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/pablojhp.pergo/internal/platform/obs"
)

func TestBatchWriterConcurrentCloseNeverSendsOnClosedChannel(t *testing.T) {
	writer := newBatchWriter(
		8192,
		1,
		100,
		func(context.Context, []Event) error { return nil },
	)

	start := make(chan struct{})
	var producers sync.WaitGroup
	for i := 0; i < 32; i++ {
		producers.Add(1)
		go func() {
			defer producers.Done()
			<-start
			for j := 0; j < 128; j++ {
				err := writer.Write(Event{
					WorkspaceID: uuid.New(),
					EventType:   "shutdown-race",
				})
				if err != nil && !errors.Is(err, ErrWriterClosed) {
					t.Errorf("Write() error = %v", err)
					return
				}
			}
		}()
	}

	close(start)
	if err := writer.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	producers.Wait()

	if err := writer.Write(Event{EventType: "after-close"}); !errors.Is(err, ErrWriterClosed) {
		t.Fatalf("Write() after Close error = %v, want ErrWriterClosed", err)
	}
	// Close is intentionally idempotent for independent shutdown paths.
	if err := writer.Close(); err != nil {
		t.Fatalf("second Close(): %v", err)
	}
}

func TestBatchWriterBufferDropReturnsErrorAndIncrementsMetric(t *testing.T) {
	flushStarted := make(chan struct{})
	releaseFlush := make(chan struct{})
	var startOnce sync.Once
	writer := newBatchWriter(
		1,
		1,
		1,
		func(context.Context, []Event) error {
			startOnce.Do(func() { close(flushStarted) })
			<-releaseFlush
			return nil
		},
	)

	if err := writer.Write(Event{EventType: "in-flight"}); err != nil {
		t.Fatalf("first Write(): %v", err)
	}
	select {
	case <-flushStarted:
	case <-time.After(time.Second):
		t.Fatal("flush did not start")
	}
	if err := writer.Write(Event{EventType: "buffered"}); err != nil {
		t.Fatalf("second Write(): %v", err)
	}

	before := obs.AuditDrops.Value()
	err := writer.Write(Event{EventType: "dropped"})
	if !errors.Is(err, ErrBufferFull) {
		t.Fatalf("third Write() error = %v, want ErrBufferFull", err)
	}
	if delta := obs.AuditDrops.Value() - before; delta != 1 {
		t.Fatalf("audit_drops delta = %d, want 1", delta)
	}

	close(releaseFlush)
	if err := writer.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
}

func TestBatchWriterFlushFailureIsCountedAndReturnedByClose(t *testing.T) {
	writer := newBatchWriter(
		4,
		1,
		2,
		func(context.Context, []Event) error {
			return errors.New("database unavailable")
		},
	)
	before := obs.AuditDrops.Value()
	for i := 0; i < 2; i++ {
		if err := writer.Write(Event{EventType: "flush-failure"}); err != nil {
			t.Fatalf("Write(%d): %v", i, err)
		}
	}

	err := writer.Close()
	if !errors.Is(err, ErrFlushFailed) {
		t.Fatalf("Close() error = %v, want ErrFlushFailed", err)
	}
	if delta := obs.AuditDrops.Value() - before; delta != 2 {
		t.Fatalf("audit_drops delta = %d, want 2", delta)
	}
}

func TestBatchWriterCloseContextCancelsFlushWithoutHanging(t *testing.T) {
	flushStarted := make(chan struct{})
	var startOnce sync.Once
	writer := newBatchWriter(
		1,
		1,
		1,
		func(ctx context.Context, _ []Event) error {
			startOnce.Do(func() { close(flushStarted) })
			<-ctx.Done()
			return ctx.Err()
		},
	)
	before := obs.AuditDrops.Value()
	if err := writer.Write(Event{EventType: "cancelled-flush"}); err != nil {
		t.Fatalf("Write(): %v", err)
	}
	select {
	case <-flushStarted:
	case <-time.After(time.Second):
		t.Fatal("flush did not start")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	startedAt := time.Now()
	err := writer.CloseContext(ctx)
	if !errors.Is(err, ErrCloseTimeout) {
		t.Fatalf("CloseContext() error = %v, want ErrCloseTimeout", err)
	}
	if elapsed := time.Since(startedAt); elapsed > 250*time.Millisecond {
		t.Fatalf("CloseContext() blocked for %s", elapsed)
	}

	select {
	case <-writer.done:
	case <-time.After(time.Second):
		t.Fatal("worker did not stop after cancellation")
	}
	if err := writer.CloseContext(context.Background()); !errors.Is(err, ErrFlushFailed) {
		t.Fatalf("CloseContext() after worker exit error = %v, want ErrFlushFailed", err)
	}
	if delta := obs.AuditDrops.Value() - before; delta != 1 {
		t.Fatalf("audit_drops delta = %d, want 1", delta)
	}
}

func TestBatchWriterCloseContextReturnsForNonCooperativeFlush(t *testing.T) {
	flushStarted := make(chan struct{})
	releaseFlush := make(chan struct{})
	writer := newBatchWriter(
		1,
		1,
		1,
		func(context.Context, []Event) error {
			close(flushStarted)
			<-releaseFlush
			return nil
		},
	)
	if err := writer.Write(Event{EventType: "blocked-flush"}); err != nil {
		t.Fatalf("Write(): %v", err)
	}
	select {
	case <-flushStarted:
	case <-time.After(time.Second):
		t.Fatal("flush did not start")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	startedAt := time.Now()
	err := writer.CloseContext(ctx)
	if !errors.Is(err, ErrCloseTimeout) {
		t.Fatalf("CloseContext() error = %v, want ErrCloseTimeout", err)
	}
	if elapsed := time.Since(startedAt); elapsed > 250*time.Millisecond {
		t.Fatalf("CloseContext() blocked for %s", elapsed)
	}

	close(releaseFlush)
	select {
	case <-writer.done:
	case <-time.After(time.Second):
		t.Fatal("worker did not exit after dependency was released")
	}
}

func TestBatchWriterCloseDrainsAcceptedEvents(t *testing.T) {
	var (
		mu      sync.Mutex
		flushed int
	)
	writer := newBatchWriter(
		8,
		1,
		3,
		func(_ context.Context, events []Event) error {
			mu.Lock()
			flushed += len(events)
			mu.Unlock()
			return nil
		},
	)
	for i := 0; i < 7; i++ {
		if err := writer.Write(Event{EventType: "drain"}); err != nil {
			t.Fatalf("Write(%d): %v", i, err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if flushed != 7 {
		t.Fatalf("flushed events = %d, want 7", flushed)
	}
}
