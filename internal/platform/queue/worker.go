package queue

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/pablojhp.pergo/internal/domain"
	"github.com/pablojhp.pergo/internal/platform/postgres/tenant"
)

// Worker reads messages from a JetStream consumer and delegates processing
// to the DispatchOrchestrator. The worker owns the consumer lifecycle;
// the orchestrator owns the dispatch pipeline.
type Worker struct {
	consumer     jetstream.Consumer
	cancel       context.CancelFunc
	done         chan struct{}
	orchestrator *DispatchOrchestrator
	workCtx      context.Context
	mu           sync.Mutex
	messages     jetstream.MessagesContext
	stopped      bool
	stopOnce     sync.Once

	inflight      chan struct{}
	workWG        sync.WaitGroup
	workspaceMu   sync.Mutex
	workspaceBusy map[uuid.UUID]struct{}
}

const (
	outboundWorkerConcurrency = 16
	schedulerBaseDelay        = 100 * time.Millisecond
	schedulerMaxDelay         = 5 * time.Second
)

// NewWorker starts a goroutine that reads messages from consumer and
// delegates to the orchestrator. Call Stop() to initiate shutdown.
func NewWorker(
	ctx context.Context,
	consumer jetstream.Consumer,
	orchestrator *DispatchOrchestrator,
) *Worker {
	workCtx := ctx
	pullCtx, cancel := context.WithCancel(ctx)
	w := &Worker{
		consumer:      consumer,
		cancel:        cancel,
		done:          make(chan struct{}),
		orchestrator:  orchestrator,
		workCtx:       workCtx,
		inflight:      make(chan struct{}, outboundWorkerConcurrency),
		workspaceBusy: make(map[uuid.UUID]struct{}),
	}

	go w.run(pullCtx)
	return w
}

// run is the main consumer loop. It reads messages, deserializes them,
// and delegates to the orchestrator.
func (w *Worker) run(ctx context.Context) {
	defer close(w.done)

	msgCtx, err := w.consumer.Messages()
	if err != nil {
		slog.Error("worker: failed to create messages context", "error", err)
		return
	}
	if !w.installMessagesContext(msgCtx) {
		return
	}
	defer func() {
		msgCtx.Stop()
		w.clearMessagesContext()
	}()

	slog.Info("message worker started", "consumer", w.consumer.CachedInfo().Config.Name)

	for {
		msg, err := msgCtx.Next()
		if err != nil {
			if ctx.Err() != nil {
				slog.Info("message worker stopped")
				return
			}
			slog.Error("worker: failed to get next message, recreating messages context", "error", err)
			msgCtx.Stop()
			w.clearMessagesContext()

			select {
			case <-ctx.Done():
				return
			case <-time.After(1 * time.Second):
			}

			newMsgCtx, err := w.consumer.Messages()
			if err != nil {
				slog.Error("worker: failed to recreate messages context", "error", err)
				continue
			}
			if !w.installMessagesContext(newMsgCtx) {
				return
			}
			msgCtx = newMsgCtx
			continue
		}

		w.scheduleMessage(w.workCtx, msg)
	}
}

// scheduleMessage allows bounded cross-workspace concurrency while keeping
// one provider call per workspace. Busy workspaces are returned to JetStream
// with a bounded delay so a slow tenant cannot occupy every worker slot or
// hide later tenants behind its queue.
func (w *Worker) scheduleMessage(ctx context.Context, msg jetstream.Msg) {
	var envelope struct {
		WorkspaceID uuid.UUID `json:"workspace_id"`
	}
	if err := json.Unmarshal(msg.Data(), &envelope); err != nil {
		slog.Error("worker: failed to unmarshal payload", "error", err)
		_ = msg.Ack()
		return
	}

	w.workspaceMu.Lock()
	if _, busy := w.workspaceBusy[envelope.WorkspaceID]; busy {
		w.workspaceMu.Unlock()
		w.deferScheduledMessage(msg)
		return
	}
	select {
	case w.inflight <- struct{}{}:
		w.workspaceBusy[envelope.WorkspaceID] = struct{}{}
		w.workWG.Add(1)
		w.workspaceMu.Unlock()
	default:
		w.workspaceMu.Unlock()
		w.deferScheduledMessage(msg)
		return
	}

	go func() {
		defer func() {
			w.workspaceMu.Lock()
			delete(w.workspaceBusy, envelope.WorkspaceID)
			w.workspaceMu.Unlock()
			<-w.inflight
			w.workWG.Done()
		}()
		w.processMessage(ctx, msg)
	}()
}

func (w *Worker) deferScheduledMessage(msg jetstream.Msg) {
	attempt := 0
	if metadata, err := msg.Metadata(); err == nil && metadata.NumDelivered > 0 {
		attempt = int(metadata.NumDelivered - 1)
	}
	delay := schedulerBaseDelay
	for i := 0; i < attempt && delay < schedulerMaxDelay; i++ {
		delay *= 2
	}
	if delay > schedulerMaxDelay {
		delay = schedulerMaxDelay
	}
	if err := msg.NakWithDelay(delay); err != nil {
		slog.Error("worker: failed to defer scheduled message", "error", err)
	}
}

// processMessage deserializes the JSON payload, enriches the context,
// and delegates to the orchestrator's Process method.
func (w *Worker) processMessage(ctx context.Context, msg jetstream.Msg) {
	var qMsg domain.QueueMessage
	if err := json.Unmarshal(msg.Data(), &qMsg); err != nil {
		slog.Error("worker: failed to unmarshal payload", "error", err)
		_ = msg.Ack()
		return
	}

	traceID := qMsg.TraceID
	if traceID == "" {
		if headers := msg.Headers(); headers != nil {
			traceID = headers.Get("Nats-Msg-Id")
		}
	}

	workspaceID := qMsg.WorkspaceID
	ctx = tenant.WithWorkspaceID(ctx, workspaceID)

	attempt := retryAttempt(adaptMsg(msg))

	if w.orchestrator == nil {
		slog.Warn("worker: no orchestrator configured, acking message", "trace_id", traceID)
		_ = msg.Ack()
		return
	}

	_ = w.orchestrator.Process(ctx, adaptMsg(msg), &qMsg, attempt)
}

// adaptMsg wraps a jetstream.Msg as a DispatchMessage.
func adaptMsg(msg jetstream.Msg) DispatchMessage {
	return &jetStreamMsg{msg: msg}
}

// jetStreamMsg adapts jetstream.Msg to the DispatchMessage port.
type jetStreamMsg struct {
	msg jetstream.Msg
}

func (m *jetStreamMsg) Data() []byte { return m.msg.Data() }

func (m *jetStreamMsg) Headers() map[string]string {
	h := m.msg.Headers()
	if h == nil {
		return nil
	}
	out := make(map[string]string, len(h))
	for k, vals := range h {
		if len(vals) > 0 {
			out[k] = vals[0]
		}
	}
	return out
}

func (m *jetStreamMsg) DeliveryAttempt() (int, bool) {
	metadata, err := m.msg.Metadata()
	if err != nil || metadata.NumDelivered == 0 {
		return 0, false
	}
	return int(metadata.NumDelivered - 1), true
}

func (m *jetStreamMsg) Ack() error { return m.msg.Ack() }

func (m *jetStreamMsg) NakWithDelay(delay time.Duration) error { return m.msg.NakWithDelay(delay) }

// Stop cancels the consumer context and waits for the goroutine to finish.
func (w *Worker) Stop() {
	w.stopOnce.Do(func() {
		w.cancel()
		w.mu.Lock()
		w.stopped = true
		messages := w.messages
		w.mu.Unlock()
		if messages != nil {
			messages.Stop()
		}
	})
	<-w.done
	w.workWG.Wait()
}

func (w *Worker) installMessagesContext(messages jetstream.MessagesContext) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopped {
		messages.Stop()
		return false
	}
	w.messages = messages
	return true
}

func (w *Worker) clearMessagesContext() {
	w.mu.Lock()
	w.messages = nil
	w.mu.Unlock()
}
