package audit

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pablojhp.pergo/internal/platform/obs"
)

const defaultCloseTimeout = 5 * time.Second

var (
	// ErrWriterClosed is returned when an event is submitted after shutdown has
	// started. Producers can treat it as a terminal local lifecycle condition.
	ErrWriterClosed = errors.New("audit_writer_closed")

	// ErrBufferFull reports that the bounded in-memory queue rejected and
	// dropped an audit event.
	ErrBufferFull = errors.New("audit_writer_buffer_full")

	// ErrFlushFailed reports that one or more accepted audit events could not
	// be persisted. The concrete failure is logged and the aggregate is
	// returned by Close without exposing database details to callers.
	ErrFlushFailed = errors.New("audit_writer_flush_failed")

	// ErrCloseTimeout reports that shutdown exceeded its deadline. Close
	// cancels in-flight database operations before returning this error.
	ErrCloseTimeout = errors.New("audit_writer_close_timeout")
)

// Writer is the interface for writing audit events.
type Writer interface {
	// Write sends an event to the buffered channel for batch writing.
	// If the channel is full, the event is dropped and ErrBufferFull is returned.
	Write(e Event) error

	// Close shuts down the writer, draining remaining events up to a bounded
	// internal deadline.
	Close() error
}

// NewWriter creates a new buffered batch writer that sends events to PostgreSQL
// via pgx.CopyFrom. Events are buffered in a bounded channel and flushed by
// background worker goroutines.
//
// Parameters:
//   - pool: the pgxpool.Pool for database connections
//   - bufSize: capacity of the internal event channel
//   - workers: number of background goroutines processing events
func NewWriter(pool *pgxpool.Pool, bufSize int, workers int) Writer {
	return newBatchWriter(bufSize, workers, 100, postgresFlush(pool))
}

// BatchWriter is the internal implementation of Writer that collects events
// into batches and flushes them via pgx.CopyFrom.
type BatchWriter struct {
	ch         chan Event
	flushFn    auditFlushFunc
	workerCtx  context.Context
	cancel     context.CancelFunc
	done       chan struct{}
	wg         sync.WaitGroup
	batchSize  int
	mu         sync.RWMutex
	closed     bool
	closeOnce  sync.Once
	failureMu  sync.Mutex
	failedRows int64
}

// Write sends an event to the batch writer's channel. If the channel is full,
// the event is counted as dropped and ErrBufferFull is returned.
func (w *BatchWriter) Write(e Event) error {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.closed {
		return ErrWriterClosed
	}
	select {
	case w.ch <- e:
		return nil
	default:
		obs.AuditDrops.Add(1)
		slog.Warn("audit channel full, dropping event",
			"event_type", e.EventType,
			"trace_id", e.TraceID,
		)
		return ErrBufferFull
	}
}

// Close shuts down the writer by closing the channel and waiting for all
// workers to drain remaining events. It has a bounded internal deadline so a
// stalled PostgreSQL operation cannot hang process shutdown indefinitely.
func (w *BatchWriter) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultCloseTimeout)
	defer cancel()
	return w.CloseContext(ctx)
}

// CloseContext closes the input queue and waits for workers until ctx expires.
// On timeout it cancels in-flight flushes and returns without waiting for a
// non-cooperative dependency. Repeated and concurrent calls are safe.
func (w *BatchWriter) CloseContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	w.closeOnce.Do(func() {
		// Holding the write lock waits for every in-flight sender and prevents a
		// future sender from racing with channel close.
		w.mu.Lock()
		w.closed = true
		close(w.ch)
		w.mu.Unlock()
		go func() {
			w.wg.Wait()
			close(w.done)
		}()
	})

	if err := ctx.Err(); err != nil {
		w.cancel()
		return errors.Join(ErrCloseTimeout, err, w.flushFailure())
	}
	select {
	case <-w.done:
		w.cancel()
		return w.flushFailure()
	case <-ctx.Done():
		w.cancel()
		return errors.Join(ErrCloseTimeout, ctx.Err(), w.flushFailure())
	}
}

// worker collects events into a batch slice and flushes them when the batch
// is full, when the channel is closed, or when the 50ms idle timer expires.
func (w *BatchWriter) worker() {
	defer w.wg.Done()
	batch := make([]Event, 0, w.batchSize)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-w.workerCtx.Done():
			dropped := len(batch)
			for range w.ch {
				dropped++
			}
			if dropped > 0 {
				obs.AuditDrops.Add(int64(dropped))
				slog.Error(
					"audit writer shutdown cancelled with buffered events",
					"events_dropped",
					dropped,
				)
			}
			return
		case e, ok := <-w.ch:
			if !ok {
				// Flush any remaining events after channel close
				if len(batch) > 0 {
					_ = w.flush(batch)
				}
				return
			}
			batch = append(batch, e)
			if len(batch) >= w.batchSize {
				_ = w.flush(batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				_ = w.flush(batch)
				batch = batch[:0]
			}
		}
	}
}

// flush writes a batch of events to PostgreSQL using pgx.CopyFrom.
func (w *BatchWriter) flush(events []Event) error {
	if len(events) == 0 {
		return nil
	}

	if err := w.flushFn(w.workerCtx, events); err != nil {
		obs.AuditDrops.Add(int64(len(events)))
		w.failureMu.Lock()
		w.failedRows += int64(len(events))
		w.failureMu.Unlock()
		slog.Error(
			"failed to flush audit batch",
			"error",
			err,
			"events_dropped",
			len(events),
		)
		return ErrFlushFailed
	}
	return nil
}

func (w *BatchWriter) flushFailure() error {
	w.failureMu.Lock()
	defer w.failureMu.Unlock()
	if w.failedRows == 0 {
		return nil
	}
	return fmt.Errorf("%w: %d events dropped", ErrFlushFailed, w.failedRows)
}

type auditFlushFunc func(context.Context, []Event) error

func newBatchWriter(
	bufSize int,
	workers int,
	batchSize int,
	flushFn auditFlushFunc,
) *BatchWriter {
	if bufSize < 1 {
		bufSize = 1
	}
	if workers < 1 {
		workers = 1
	}
	if batchSize < 1 {
		batchSize = 1
	}
	if flushFn == nil {
		flushFn = func(context.Context, []Event) error {
			return errors.New("audit flush is not configured")
		}
	}
	workerCtx, cancel := context.WithCancel(context.Background())
	bw := &BatchWriter{
		ch:        make(chan Event, bufSize),
		flushFn:   flushFn,
		workerCtx: workerCtx,
		cancel:    cancel,
		done:      make(chan struct{}),
		batchSize: batchSize,
	}
	bw.wg.Add(workers)
	for i := 0; i < workers; i++ {
		go bw.worker()
	}
	return bw
}

func postgresFlush(pool *pgxpool.Pool) auditFlushFunc {
	return func(ctx context.Context, events []Event) error {
		if pool == nil {
			return errors.New("audit PostgreSQL pool is not configured")
		}
		conn, err := pool.Acquire(ctx)
		if err != nil {
			return fmt.Errorf("acquire audit connection: %w", err)
		}
		defer conn.Release()

		_, err = conn.Conn().CopyFrom(
			ctx,
			pgx.Identifier{"audit_logs"},
			[]string{"id", "workspace_id", "trace_id", "event_type", "payload", "created_at"},
			pgx.CopyFromSlice(len(events), func(i int) ([]any, error) {
				e := events[i]
				return []any{uuid.New(), e.WorkspaceID, e.TraceID, e.EventType, e.Payload, e.CreatedAt}, nil
			}),
		)
		if err != nil {
			return fmt.Errorf("copy audit events: %w", err)
		}
		return nil
	}
}
