package inbound

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/pablojhp.pergo/internal/repository"
)

type DeliveryOutboxStore interface {
	ListPendingProviderDeliveryEvents(context.Context, int) ([]repository.ProviderDeliveryOutboxEvent, error)
	MarkProviderDeliveryEventPublished(context.Context, uuid.UUID) error
}

// DeliveryReceiptRelay reconciles receipt events that were committed before a
// crash or broker outage. Stable event keys make concurrent replicas and
// response-loss retries safe through JetStream deduplication.
type DeliveryReceiptRelay struct {
	store     DeliveryOutboxStore
	publisher Publisher
	interval  time.Duration
	cancel    context.CancelFunc
	done      chan struct{}
	stopOnce  sync.Once
}

func NewDeliveryReceiptRelay(
	ctx context.Context,
	store DeliveryOutboxStore,
	publisher Publisher,
	interval time.Duration,
) *DeliveryReceiptRelay {
	if interval <= 0 {
		interval = time.Second
	}
	ctx, cancel := context.WithCancel(ctx)
	relay := &DeliveryReceiptRelay{
		store:     store,
		publisher: publisher,
		interval:  interval,
		cancel:    cancel,
		done:      make(chan struct{}),
	}
	go relay.run(ctx)
	return relay
}

func (r *DeliveryReceiptRelay) Stop() {
	r.stopOnce.Do(r.cancel)
	<-r.done
}

func (r *DeliveryReceiptRelay) run(ctx context.Context) {
	defer close(r.done)
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		r.flush(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (r *DeliveryReceiptRelay) flush(ctx context.Context) {
	if r.store == nil || r.publisher == nil {
		return
	}
	events, err := r.store.ListPendingProviderDeliveryEvents(ctx, 100)
	if err != nil {
		if ctx.Err() == nil {
			slog.Error("delivery receipt relay: list pending failed", "error", err)
		}
		return
	}
	for _, event := range events {
		if err := r.publisher.Publish(
			ctx,
			"webhooks.events",
			event.Payload,
			event.EventKey,
		); err != nil {
			if ctx.Err() == nil {
				slog.Warn(
					"delivery receipt relay: publish failed",
					"event_id",
					event.ID,
					"workspace_id",
					event.WorkspaceID,
				)
			}
			continue
		}
		if err := r.store.MarkProviderDeliveryEventPublished(ctx, event.ID); err != nil && ctx.Err() == nil {
			slog.Error(
				"delivery receipt relay: mark published failed",
				"event_id",
				event.ID,
				"workspace_id",
				event.WorkspaceID,
			)
		}
	}
}
