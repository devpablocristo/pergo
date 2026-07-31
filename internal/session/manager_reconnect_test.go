package session

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/types"
	waEvents "go.mau.fi/whatsmeow/types/events"

	whatsapp "github.com/pablojhp.pergo/internal/channel/whatsapp"
	"github.com/pablojhp.pergo/internal/repository"
)

type reconnectStore struct {
	mu          sync.Mutex
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
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statuses = append(s.statuses, status)
	return nil
}

func (s *reconnectStore) Statuses() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.statuses...)
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
	if statuses := store.Statuses(); len(statuses) != 1 || statuses[0] != string(DeviceStatusDisconnected) {
		t.Fatalf("statuses = %v, want one disconnected update", statuses)
	}
}

func TestReconnectAllLimitsConcurrentAttempts(t *testing.T) {
	connections := make([]*repository.Connection, 7)
	for i := range connections {
		jid := "54911000000" + string(rune('1'+i)) + "@s.whatsapp.net"
		connections[i] = &repository.Connection{ID: uuid.New(), Channel: "whatsapp", JID: &jid}
	}
	manager := &Manager{
		repo:          &reconnectStore{connections: connections},
		registry:      NewActiveSession(),
		wait:          func(context.Context, time.Duration) bool { return true },
		initialJitter: func() time.Duration { return 0 },
	}
	release := make(chan struct{})
	var inFlight atomic.Int32
	var maximum atomic.Int32
	manager.reconnect = func(context.Context, *repository.Connection) error {
		current := inFlight.Add(1)
		for {
			previous := maximum.Load()
			if current <= previous || maximum.CompareAndSwap(previous, current) {
				break
			}
		}
		<-release
		inFlight.Add(-1)
		return nil
	}

	done := make(chan error, 1)
	go func() { done <- manager.ReconnectAll(context.Background()) }()
	waitUntil(t, time.Second, func() bool { return maximum.Load() == maxConcurrentReconnect }, "reconnect attempts did not reach concurrency limit")
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("ReconnectAll returned error: %v", err)
	}
	if got := maximum.Load(); got != maxConcurrentReconnect {
		t.Fatalf("maximum concurrent reconnects = %d, want %d", got, maxConcurrentReconnect)
	}
}

func TestStopAllCancelsReconnectBackoff(t *testing.T) {
	jid := "12345:1@s.whatsapp.net"
	manager := &Manager{
		repo:          &reconnectStore{connections: []*repository.Connection{{ID: uuid.New(), Channel: "whatsapp", JID: &jid}}},
		registry:      NewActiveSession(),
		initialJitter: func() time.Duration { return 0 },
	}
	started := make(chan struct{})
	manager.reconnect = func(context.Context, *repository.Connection) error {
		close(started)
		return errors.New("offline")
	}
	var waits atomic.Int32
	manager.wait = func(ctx context.Context, _ time.Duration) bool {
		if waits.Add(1) == 1 { // startup jitter
			return true
		}
		<-ctx.Done()
		return false
	}

	done := make(chan error, 1)
	go func() { done <- manager.ReconnectAll(context.Background()) }()
	<-started
	manager.StopAll()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ReconnectAll returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("StopAll did not cancel reconnection backoff")
	}
	statuses := manager.repo.(*reconnectStore).Statuses()
	if len(statuses) == 0 || statuses[0] != string(DeviceStatusDisconnected) {
		t.Fatalf("permanent failure statuses = %v, want initial disconnected state", statuses)
	}
}

func TestCalcBackoffUsesJitterAndCap(t *testing.T) {
	const jitter = 0.1
	for _, attempt := range []int{0, 1, 2, 10} {
		base := defaultReconnectBackoff * time.Duration(1<<minInt(attempt, 6))
		if base > maxReconnectBackoff {
			base = maxReconnectBackoff
		}
		lower := time.Duration(float64(base) * (1 - jitter))
		upper := time.Duration(float64(base) * (1 + jitter))
		for range 20 {
			got := calcBackoff(attempt)
			if got < lower || got > upper {
				t.Fatalf("calcBackoff(%d) = %s, want within [%s, %s]", attempt, got, lower, upper)
			}
		}
	}
}

func TestReconnectDeviceCleansUpFailedClient(t *testing.T) {
	jid := "12345:1@s.whatsapp.net"
	fake := &reconnectFakeClient{connectErr: errors.New("network unavailable"), disconnected: make(chan struct{})}
	manager := &Manager{
		repo:          &reconnectStore{},
		registry:      NewActiveSession(),
		clientFactory: fakeFactory(fake),
	}
	err := manager.reconnectDevice(context.Background(), &repository.Connection{ID: uuid.New(), Channel: "whatsapp", JID: &jid})
	if err == nil {
		t.Fatal("reconnectDevice unexpectedly succeeded")
	}
	select {
	case <-fake.disconnected:
	case <-time.After(time.Second):
		t.Fatal("failed client was not disconnected")
	}
}

func TestReconnectDeviceLoggedOutBecomesTerminalWithoutRetry(t *testing.T) {
	jid := "12345:1@s.whatsapp.net"
	store := &reconnectStore{}
	fake := &reconnectFakeClient{disconnected: make(chan struct{})}
	manager := &Manager{
		repo:          store,
		registry:      NewActiveSession(),
		clientFactory: fakeFactory(fake),
	}
	if err := manager.reconnectDevice(context.Background(), &repository.Connection{ID: uuid.New(), Channel: "whatsapp", JID: &jid}); err != nil {
		t.Fatalf("reconnectDevice: %v", err)
	}
	fake.Emit(&waEvents.LoggedOut{})
	waitUntil(t, time.Second, func() bool {
		for _, status := range store.Statuses() {
			if status == string(DeviceStatusTerminal) {
				return true
			}
		}
		return false
	}, "LoggedOut did not mark connection terminal")
	select {
	case <-fake.disconnected:
	case <-time.After(time.Second):
		t.Fatal("terminal session was not disconnected")
	}
}

func waitUntil(t *testing.T, timeout time.Duration, condition func() bool, message string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal(message)
}

type reconnectFakeClient struct {
	jid          types.JID
	connectErr   error
	disconnected chan struct{}
	handler      whatsmeow.EventHandler
}

func fakeFactory(client *reconnectFakeClient) whatsapp.ClientFactory {
	return whatsapp.ClientFactoryFunc(func(whatsapp.ClientConfig) (whatsapp.Client, error) { return client, nil })
}

func (c *reconnectFakeClient) JID() types.JID       { return c.jid }
func (c *reconnectFakeClient) SetJID(jid types.JID) { c.jid = jid }
func (c *reconnectFakeClient) Run(ctx context.Context) error {
	if err := c.Connect(); err != nil {
		return err
	}
	c.Wait(ctx)
	return nil
}
func (c *reconnectFakeClient) Wait(ctx context.Context) {
	<-ctx.Done()
	c.Disconnect()
}
func (c *reconnectFakeClient) GetQRChannel(context.Context) (<-chan whatsmeow.QRChannelItem, error) {
	return nil, errors.New("QR pairing unavailable in reconnect fake")
}
func (c *reconnectFakeClient) Connect() error { return c.connectErr }
func (c *reconnectFakeClient) Disconnect() {
	select {
	case <-c.disconnected:
	default:
		close(c.disconnected)
	}
}
func (c *reconnectFakeClient) AddEventHandler(handler whatsmeow.EventHandler) uint32 {
	c.handler = handler
	return 1
}
func (c *reconnectFakeClient) Download(context.Context, whatsmeow.DownloadableMessage) ([]byte, error) {
	return nil, errors.New("media download unavailable in reconnect fake")
}
func (c *reconnectFakeClient) Emit(event interface{}) {
	if c.handler != nil {
		c.handler(event)
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
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
