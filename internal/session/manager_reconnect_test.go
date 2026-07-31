package session

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.mau.fi/whatsmeow/types"

	"github.com/pablojhp.pergo/internal/repository"
)

type reconnectStore struct {
	connections []*repository.Connection
	statuses    []string
}

func (s *reconnectStore) Create(context.Context, *repository.Connection) error { return nil }
func (s *reconnectStore) GetByID(context.Context, uuid.UUID) (*repository.Connection, error) {
	return nil, nil
}
func (s *reconnectStore) ListAll(context.Context) ([]*repository.Connection, error) {
	return s.connections, nil
}
func (s *reconnectStore) ListByWorkspace(context.Context, uuid.UUID) ([]*repository.Connection, error) {
	return nil, nil
}
func (s *reconnectStore) UpdateStatus(_ context.Context, _ uuid.UUID, status string) error {
	s.statuses = append(s.statuses, status)
	return nil
}

func TestReconnectAllRetriesEligibleDevices(t *testing.T) {
	jid := "12345:1@s.whatsapp.net"
	store := &reconnectStore{connections: []*repository.Connection{
		{ID: uuid.New(), Channel: "whatsapp", JID: &jid, Status: string(DeviceStatusDisconnected)},
		{ID: uuid.New(), Channel: "telegram", JID: &jid},
		{ID: uuid.New(), Channel: "whatsapp", JID: &jid, Status: string(DeviceStatusTerminal)},
	}}
	manager := &Manager{repo: store, registry: NewActiveSession(), wait: func(context.Context, time.Duration) bool { return true }, initialJitter: func() time.Duration { return 0 }}
	var attempts atomic.Int32
	manager.reconnect = func(context.Context, *repository.Connection) error {
		if attempts.Add(1) == 1 {
			return errors.New("temporary failure")
		}
		return nil
	}

	if err := manager.ReconnectAll(context.Background()); err != nil {
		t.Fatalf("ReconnectAll returned error: %v", err)
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("attempts = %d, want 2", got)
	}
	if len(store.statuses) != 1 || store.statuses[0] != string(DeviceStatusDisconnected) {
		t.Fatalf("statuses = %v, want one disconnected update", store.statuses)
	}
}

func TestReconnectAllStopsOnCancellation(t *testing.T) {
	jid := "12345:1@s.whatsapp.net"
	store := &reconnectStore{connections: []*repository.Connection{{ID: uuid.New(), Channel: "whatsapp", JID: &jid}}}
	ctx, cancel := context.WithCancel(context.Background())
	manager := &Manager{repo: store, registry: NewActiveSession(), initialJitter: func() time.Duration { return 0 }}
	manager.wait = func(ctx context.Context, _ time.Duration) bool {
		<-ctx.Done()
		return false
	}
	manager.reconnect = func(context.Context, *repository.Connection) error { return errors.New("offline") }

	done := make(chan error, 1)
	go func() { done <- manager.ReconnectAll(ctx) }()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ReconnectAll returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ReconnectAll did not stop after cancellation")
	}
}

func TestReconnectAllSkipsActiveSessions(t *testing.T) {
	jidValue := "12345:1@s.whatsapp.net"
	jid, err := types.ParseJID(jidValue)
	if err != nil {
		t.Fatal(err)
	}
	registry := NewActiveSession()
	registry.Add(&Session{JID: jid})
	store := &reconnectStore{connections: []*repository.Connection{{ID: uuid.New(), Channel: "whatsapp", JID: &jidValue}}}
	manager := &Manager{repo: store, registry: registry, wait: func(context.Context, time.Duration) bool { return true }, initialJitter: func() time.Duration { return 0 }}
	manager.reconnect = func(context.Context, *repository.Connection) error {
		t.Fatal("active session must not reconnect")
		return nil
	}

	if err := manager.ReconnectAll(context.Background()); err != nil {
		t.Fatal(err)
	}
}
