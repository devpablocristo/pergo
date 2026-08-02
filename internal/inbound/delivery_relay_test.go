package inbound

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/pablojhp.pergo/internal/repository"
)

func TestDeliveryReceiptRelayRetriesStableEventAfterPublishFailure(t *testing.T) {
	event := repository.ProviderDeliveryOutboxEvent{
		ID:          uuid.New(),
		WorkspaceID: uuid.New(),
		EventKey:    "trace.delivery.read",
		Payload:     []byte(`{"workspace_id":"00000000-0000-0000-0000-000000000001"}`),
	}
	store := &relayStore{event: event}
	publisher := &relayPublisher{failFirst: true, accepted: make(chan struct{})}
	relay := NewDeliveryReceiptRelay(context.Background(), store, publisher, 5*time.Millisecond)
	defer relay.Stop()

	select {
	case <-publisher.accepted:
	case <-time.After(time.Second):
		t.Fatal("relay did not retry the pending event")
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if store.markedCount() == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if store.markedCount() != 1 {
		t.Fatalf("mark count=%d, want 1", store.markedCount())
	}
	if publisher.keysCount(event.EventKey) != 2 {
		t.Fatalf("publish attempts for stable key=%d, want 2", publisher.keysCount(event.EventKey))
	}
}

type relayStore struct {
	mu     sync.Mutex
	event  repository.ProviderDeliveryOutboxEvent
	marked int
}

func (s *relayStore) ListPendingProviderDeliveryEvents(context.Context, int) ([]repository.ProviderDeliveryOutboxEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.marked != 0 {
		return nil, nil
	}
	return []repository.ProviderDeliveryOutboxEvent{s.event}, nil
}

func (s *relayStore) MarkProviderDeliveryEventPublished(context.Context, uuid.UUID) error {
	s.mu.Lock()
	s.marked++
	s.mu.Unlock()
	return nil
}

func (s *relayStore) markedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.marked
}

type relayPublisher struct {
	mu        sync.Mutex
	failFirst bool
	keys      []string
	accepted  chan struct{}
	once      sync.Once
}

func (p *relayPublisher) Publish(_ context.Context, _ string, _ []byte, traceID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.keys = append(p.keys, traceID)
	if p.failFirst {
		p.failFirst = false
		return errors.New("broker unavailable")
	}
	p.once.Do(func() { close(p.accepted) })
	return nil
}

func (p *relayPublisher) keysCount(key string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	count := 0
	for _, got := range p.keys {
		if got == key {
			count++
		}
	}
	return count
}
