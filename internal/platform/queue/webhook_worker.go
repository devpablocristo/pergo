package queue

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/pablojhp.pergo/internal/repository"
	"github.com/pablojhp.pergo/internal/webhook"
)

type WebhookEvent struct {
	Event       string `json:"event"`
	TraceID     string `json:"trace_id"`
	MessageID   string `json:"message_id"`
	Channel     string `json:"channel"`
	Timestamp   string `json:"timestamp"`
	WorkspaceID string `json:"workspace_id"`
	Error       string `json:"error,omitempty"`
}

type WebhookWorker struct {
	nc               *nats.Conn
	js               jetstream.JetStream
	consumer         jetstream.Consumer
	inboundConsumer  jetstream.Consumer
	deliveryConsumer jetstream.Consumer
	dispatcher       webhook.WebhookDispatcher
	subRepo          *repository.WebhookSubscriptionRepository
	cancel           context.CancelFunc
	wg               sync.WaitGroup
	contextsMu       sync.Mutex
	messageContexts  map[string]jetstream.MessagesContext
	stopped          bool
	stopOnce         sync.Once
}

const (
	webhookAckWait           = 2 * time.Minute
	webhookHeartbeatInterval = 30 * time.Second
)

func NewWebhookWorker(ctx context.Context, nc *nats.Conn, dispatcher webhook.WebhookDispatcher, subRepo *repository.WebhookSubscriptionRepository, replicas ...int) (*WebhookWorker, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("jetstream.New: %w", err)
	}

	// Streams and consumers are created only by the migration/bootstrap job.
	stream, err := BindVersionedStream(ctx, nc, WebhookStreamName, WebhookStreamSubject, replicas...)
	if err != nil {
		return nil, fmt.Errorf("bind webhook stream: %w", err)
	}

	consumer, err := BindConsumer(ctx, stream, WebhookEventConsumerName, WebhookStreamSubject)
	if err != nil {
		return nil, fmt.Errorf("bind webhook consumer: %w", err)
	}

	inboundStream, err := BindVersionedStream(ctx, nc, InboundStreamName, InboundStreamSubject, replicas...)
	if err != nil {
		return nil, fmt.Errorf("bind inbound stream: %w", err)
	}

	inboundConsumer, err := BindConsumer(ctx, inboundStream, InboundEventConsumerName, InboundStreamSubject)
	if err != nil {
		return nil, fmt.Errorf("bind inbound webhook consumer: %w", err)
	}

	deliveryStream, err := BindVersionedStream(
		ctx,
		nc,
		WebhookDeliveryStreamName,
		WebhookDeliverySubject,
		replicas...,
	)
	if err != nil {
		return nil, fmt.Errorf("bind webhook delivery stream: %w", err)
	}

	deliveryConsumer, err := BindConsumer(
		ctx,
		deliveryStream,
		WebhookDeliveryConsumerName,
		WebhookDeliverySubject,
	)
	if err != nil {
		return nil, fmt.Errorf("create webhook delivery consumer: %w", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	w := &WebhookWorker{
		nc:               nc,
		js:               js,
		consumer:         consumer,
		inboundConsumer:  inboundConsumer,
		deliveryConsumer: deliveryConsumer,
		dispatcher:       dispatcher,
		subRepo:          subRepo,
		cancel:           cancel,
		messageContexts:  make(map[string]jetstream.MessagesContext, 3),
	}

	w.wg.Add(3)
	go w.run(ctx, w.consumer, "outbound")
	go w.run(ctx, w.inboundConsumer, "inbound")
	go w.run(ctx, w.deliveryConsumer, "delivery")
	return w, nil
}

func (w *WebhookWorker) Stop() {
	w.stopOnce.Do(func() {
		w.cancel()
		w.contextsMu.Lock()
		w.stopped = true
		contexts := make([]jetstream.MessagesContext, 0, len(w.messageContexts))
		for _, msgCtx := range w.messageContexts {
			contexts = append(contexts, msgCtx)
		}
		w.contextsMu.Unlock()
		for _, msgCtx := range contexts {
			msgCtx.Stop()
		}
	})
	w.wg.Wait()
}

func (w *WebhookWorker) SetWorkspaceRepository(repo *repository.WorkspaceRepository) {
	// Deprecated: workspace opt-in checks are now handled directly inside the WebhookDispatcher.
}

func (w *WebhookWorker) run(ctx context.Context, cons jetstream.Consumer, mode string) {
	defer w.wg.Done()
	slog.Info("webhook worker thread started", "mode", mode)

	msgCtx, err := cons.Messages()
	if err != nil {
		slog.Error("webhook worker: failed to create messages context", "error", err, "mode", mode)
		return
	}
	if !w.installMessagesContext(mode, msgCtx) {
		return
	}
	defer func() {
		msgCtx.Stop()
		w.clearMessagesContext(mode)
	}()

	for {
		msg, err := msgCtx.Next()
		if err != nil {
			if ctx.Err() != nil {
				slog.Info("webhook worker thread stopping", "mode", mode)
				return
			}
			slog.Error("webhook worker: messages context failed", "error", err, "mode", mode)
			msgCtx.Stop()
			w.clearMessagesContext(mode)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			next, nextErr := cons.Messages()
			if nextErr != nil {
				slog.Error("webhook worker: failed to recreate messages context", "error", nextErr, "mode", mode)
				continue
			}
			if !w.installMessagesContext(mode, next) {
				return
			}
			msgCtx = next
			continue
		}

		if mode == "delivery" {
			w.processDelivery(ctx, msg)
		} else {
			w.processEvent(ctx, msg, mode)
		}
	}
}

func (w *WebhookWorker) installMessagesContext(mode string, msgCtx jetstream.MessagesContext) bool {
	w.contextsMu.Lock()
	defer w.contextsMu.Unlock()
	if w.stopped {
		msgCtx.Stop()
		return false
	}
	w.messageContexts[mode] = msgCtx
	return true
}

func (w *WebhookWorker) clearMessagesContext(mode string) {
	w.contextsMu.Lock()
	delete(w.messageContexts, mode)
	w.contextsMu.Unlock()
}

func (w *WebhookWorker) processEvent(ctx context.Context, msg jetstream.Msg, mode string) {
	stopHeartbeat := maintainAckLease(ctx, msg)
	defer stopHeartbeat()

	var evt WebhookEvent
	if err := json.Unmarshal(msg.Data(), &evt); err != nil {
		slog.Error("webhook worker: failed to unmarshal event", "error", err)
		_ = msg.Ack()
		return
	}

	wsID, err := uuid.Parse(evt.WorkspaceID)
	if err != nil {
		slog.Error("webhook worker: invalid workspace ID", "error", err, "workspace_id", evt.WorkspaceID)
		_ = msg.Ack()
		return
	}

	// Look up active subscriptions for this workspace
	subs, err := w.subRepo.ListByWorkspace(ctx, wsID)
	if err != nil {
		slog.Error("webhook worker: failed to list subscriptions", "error", err, "workspace_id", wsID)
		_ = msg.NakWithDelay(5 * time.Second)
		return
	}

	publisher := NewJetStreamPublisher(w.nc)

	for _, sub := range subs {
		if !sub.Active {
			continue
		}

		if webhook.MatchesAny(sub.EventTypes, evt.Event) {
			deliveryID := deterministicWebhookDeliveryID(mode, wsID, sub.ID, evt, msg.Data())
			task := webhook.WebhookDeliveryTask{
				ID:             deliveryID,
				SubscriptionID: sub.ID,
				WorkspaceID:    wsID,
				Event:          evt.Event,
				TraceID:        evt.TraceID,
				MessageID:      evt.MessageID,
				Payload:        msg.Data(),
				Mode:           mode,
			}

			taskData, err := json.Marshal(task)
			if err != nil {
				slog.Error("webhook worker: failed to marshal delivery task", "error", err, "subscription_id", sub.ID)
				continue
			}

			subject := fmt.Sprintf("webhooks.deliveries.%s", wsID)
			err = publisher.Publish(ctx, subject, taskData, task.ID.String())
			if err != nil {
				slog.Error("webhook worker: failed to publish delivery task", "error", err, "subject", subject)
				_ = msg.NakWithDelay(5 * time.Second)
				return
			}
			slog.Info("webhook worker: fanned out task", "subject", subject, "trace_id", evt.TraceID)
		}
	}

	_ = msg.Ack()
}

func (w *WebhookWorker) processDelivery(ctx context.Context, msg jetstream.Msg) {
	stopHeartbeat := maintainAckLease(ctx, msg)
	defer stopHeartbeat()

	var task webhook.WebhookDeliveryTask
	if err := json.Unmarshal(msg.Data(), &task); err != nil {
		slog.Error("webhook worker: failed to unmarshal delivery task", "error", err)
		_ = msg.Ack()
		return
	}

	err := w.dispatcher.Dispatch(ctx, task)
	if err != nil {
		slog.Warn("webhook worker: delivery dispatch failed", "error", err, "trace_id", task.TraceID, "subscription_id", task.SubscriptionID)
		w.handleDeliveryRetry(ctx, msg, err, &task)
		return
	}

	slog.Info("webhook worker: delivered successfully", "trace_id", task.TraceID, "subscription_id", task.SubscriptionID)
	_ = msg.Ack()
}

func deterministicWebhookDeliveryID(
	mode string,
	workspaceID uuid.UUID,
	subscriptionID uuid.UUID,
	event WebhookEvent,
	payload []byte,
) uuid.UUID {
	payloadDigest := sha256.Sum256(payload)
	identity := strings.Join([]string{
		"pergo-webhook-delivery-v2",
		mode,
		workspaceID.String(),
		subscriptionID.String(),
		event.Event,
		event.TraceID,
		event.MessageID,
		fmt.Sprintf("%x", payloadDigest),
	}, "\x1f")
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(identity))
}

func maintainAckLease(ctx context.Context, msg jetstream.Msg) func() {
	_ = msg.InProgress()
	heartbeatCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(webhookHeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-ticker.C:
				if err := msg.InProgress(); err != nil {
					slog.Warn("webhook worker: failed to renew message ack lease", "error", err)
				}
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

func (w *WebhookWorker) handleDeliveryRetry(ctx context.Context, msg jetstream.Msg, err error, task *webhook.WebhookDeliveryTask) {
	numDelivered := uint64(1)
	if meta, metadataErr := msg.Metadata(); metadataErr == nil && meta != nil {
		numDelivered = meta.NumDelivered
	}

	// Check if this error is terminal or we have exhausted max retries (10)
	var httpErr *webhook.HTTPError
	isTerminalErr := false
	if errors.As(err, &httpErr) {
		// Terminal status codes: 400, 401, 403, 404
		if httpErr.StatusCode == 400 || httpErr.StatusCode == 401 || httpErr.StatusCode == 403 || httpErr.StatusCode == 404 {
			isTerminalErr = true
		}
	}
	if errors.Is(err, webhook.ErrSubscriptionNotFound) ||
		errors.Is(err, webhook.ErrSubscriptionInactive) {
		isTerminalErr = true
	}

	if isTerminalErr || numDelivered >= 10 {
		slog.Error("webhook worker: moving delivery to DLQ", "error", err, "trace_id", task.TraceID, "attempts", numDelivered, "subscription_id", task.SubscriptionID)

		failReason := err.Error()

		// Archive permanently failed event via WebhookDispatcher
		dlqCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		dlqErr := w.dispatcher.WriteToDLQ(
			dlqCtx,
			task.WorkspaceID,
			task.SubscriptionID,
			task.TraceID,
			task.MessageID,
			task.Event,
			task.Payload,
			int(numDelivered),
			failReason,
		)
		if dlqErr != nil {
			slog.Error("webhook worker: failed to write delivery to DLQ", "error", dlqErr, "trace_id", task.TraceID)
			_ = msg.NakWithDelay(5 * time.Second)
			return
		}

		_ = msg.Ack()
		return
	}

	// Calculate exponential backoff: 2^(numDelivered-1) * 1s
	delay := time.Duration(1<<(numDelivered-1)) * time.Second
	if delay > 10*time.Minute {
		delay = 10 * time.Minute
	}

	slog.Info("webhook worker: scheduling delivery retry", "trace_id", task.TraceID, "attempt", numDelivered, "backoff", delay)
	_ = msg.NakWithDelay(delay)
}
