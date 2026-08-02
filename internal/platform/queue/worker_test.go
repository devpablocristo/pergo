package queue

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pablojhp.pergo/internal/channel"
	"github.com/pablojhp.pergo/internal/domain"
	"github.com/pablojhp.pergo/internal/platform/audit"
	"github.com/pablojhp.pergo/internal/platform/postgres"
	"github.com/pablojhp.pergo/internal/repository"
)

func TestRetryAttemptParsing(t *testing.T) {
	tests := []struct {
		name     string
		headers  map[string]string
		expected int
	}{
		{"no headers", nil, 0},
		{"no retry header", map[string]string{}, 0},
		{"zero attempt", map[string]string{"X-Retry-Attempt": "0"}, 0},
		{"first attempt", map[string]string{"X-Retry-Attempt": "1"}, 1},
		{"third attempt", map[string]string{"X-Retry-Attempt": "3"}, 3},
		{"invalid", map[string]string{"X-Retry-Attempt": "abc"}, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &fakeDispatchMsg{headers: tt.headers}
			got := retryAttempt(msg)
			if got != tt.expected {
				t.Errorf("retryAttempt = %d, want %d", got, tt.expected)
			}
		})
	}
}

func TestRetryAttemptPrefersJetStreamMetadata(t *testing.T) {
	msg := &fakeDispatchMsg{
		headers:            map[string]string{"X-Retry-Attempt": "1"},
		deliveryAttempt:    4,
		hasDeliveryAttempt: true,
	}
	if got := retryAttempt(msg); got != 4 {
		t.Fatalf("retryAttempt = %d, want broker metadata attempt 4", got)
	}
}

func TestExponentialBackoff(t *testing.T) {
	tests := []struct {
		attempt    int
		maxBackoff time.Duration
		wantDelay  time.Duration
	}{
		{0, 60 * time.Second, 1 * time.Second},
		{1, 60 * time.Second, 2 * time.Second},
		{2, 60 * time.Second, 4 * time.Second},
		{3, 60 * time.Second, 8 * time.Second},
		{4, 60 * time.Second, 16 * time.Second},
		{5, 60 * time.Second, 32 * time.Second},
		{6, 60 * time.Second, 60 * time.Second},
		{10, 60 * time.Second, 60 * time.Second},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("attempt_%d", tt.attempt), func(t *testing.T) {
			delay := time.Duration(1<<uint(tt.attempt)) * orchDefaultBaseBackoff
			if delay > tt.maxBackoff {
				delay = tt.maxBackoff
			}
			if delay != tt.wantDelay {
				t.Errorf("backoff at attempt %d = %v, want %v", tt.attempt, delay, tt.wantDelay)
			}
		})
	}
}

func TestIsExpired(t *testing.T) {
	orchestrator := &DispatchOrchestrator{}

	tests := []struct {
		name       string
		qMsg       domain.QueueMessage
		wantExpire bool
	}{
		{
			name:       "no TTL set",
			qMsg:       domain.QueueMessage{},
			wantExpire: false,
		},
		{
			name: "TTL not expired",
			qMsg: domain.QueueMessage{
				QueuedAt:   time.Now().UTC().Add(-10 * time.Second),
				TTLSeconds: intPtr(300),
			},
			wantExpire: false,
		},
		{
			name: "TTL expired",
			qMsg: domain.QueueMessage{
				QueuedAt:   time.Now().UTC().Add(-600 * time.Second),
				TTLSeconds: intPtr(300),
			},
			wantExpire: true,
		},
		{
			name: "zero TTL ignored",
			qMsg: domain.QueueMessage{
				QueuedAt:   time.Now().UTC(),
				TTLSeconds: intPtr(0),
			},
			wantExpire: false,
		},
		{
			name: "negative TTL ignored",
			qMsg: domain.QueueMessage{
				QueuedAt:   time.Now().UTC(),
				TTLSeconds: intPtr(-1),
			},
			wantExpire: false,
		},
		{
			name: "zero queued_at with TTL",
			qMsg: domain.QueueMessage{
				TTLSeconds: intPtr(60),
			},
			wantExpire: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := orchestrator.isExpired(&tt.qMsg)
			if got != tt.wantExpire {
				t.Errorf("isExpired = %v, want %v", got, tt.wantExpire)
			}
		})
	}
}

// --- Fake adapters for orchestrator tests ---

// fakeDispatchMsg implements DispatchMessage for tests.
type fakeDispatchMsg struct {
	data     []byte
	headers  map[string]string
	acked    bool
	nacked   bool
	nakDelay time.Duration

	deliveryAttempt    int
	hasDeliveryAttempt bool
}

func (m *fakeDispatchMsg) Data() []byte               { return m.data }
func (m *fakeDispatchMsg) Headers() map[string]string { return m.headers }
func (m *fakeDispatchMsg) DeliveryAttempt() (int, bool) {
	return m.deliveryAttempt, m.hasDeliveryAttempt
}
func (m *fakeDispatchMsg) Ack() error { m.acked = true; return nil }
func (m *fakeDispatchMsg) NakWithDelay(d time.Duration) error {
	m.nacked = true
	m.nakDelay = d
	return nil
}

type fakeDispatcher struct {
	err           error
	calledCount   int
	calledWith    []string
	lastTo        string
	lastMessageID string
}

type blockingConcurrentDispatcher struct {
	calls   atomic.Int32
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (d *blockingConcurrentDispatcher) Dispatch(_ context.Context, _ *channel.MessagePayload) (string, error) {
	d.calls.Add(1)
	d.once.Do(func() { close(d.entered) })
	<-d.release
	return "provider-once", nil
}

func (m *fakeDispatcher) Dispatch(ctx context.Context, p *channel.MessagePayload) (string, error) {
	m.calledCount++
	m.calledWith = append(m.calledWith, p.Channel)
	m.lastTo = p.To
	m.lastMessageID = p.MessageID
	return "", m.err
}

type fakeQueueDepthTracker struct {
	decrements map[uuid.UUID]int
}

func (m *fakeQueueDepthTracker) Decrement(workspaceID uuid.UUID) {
	if m.decrements == nil {
		m.decrements = make(map[uuid.UUID]int)
	}
	m.decrements[workspaceID]++
}

type recordedQueuePublish struct {
	subject string
	data    []byte
	dedupID string
}

type recordingQueuePublisher struct {
	published []recordedQueuePublish
}

func (p *recordingQueuePublisher) Publish(_ context.Context, subject string, data []byte, dedupID string) error {
	p.published = append(p.published, recordedQueuePublish{
		subject: subject,
		data:    append([]byte(nil), data...),
		dedupID: dedupID,
	})
	return nil
}

type recordingAuditWriter struct {
	events []audit.Event
}

func (w *recordingAuditWriter) Write(event audit.Event) error {
	w.events = append(w.events, event)
	return nil
}

func (w *recordingAuditWriter) Close() error {
	return nil
}

func getTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	dsn := os.Getenv("PERGO_DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/pergo?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("PostgreSQL not available at %s: %v", dsn, err)
	}

	err = pool.Ping(ctx)
	if err != nil {
		pool.Close()
		t.Skipf("PostgreSQL ping failed at %s: %v", dsn, err)
	}

	_, err = postgres.NewSQLDB(pool)
	if err != nil {
		pool.Close()
		t.Fatalf("failed to initialize db: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
	})
	return pool
}

func newTestOrchestrator(dispatchers *channel.Registry, dispatchRepo *repository.MessageDispatchRepository) *DispatchOrchestrator {
	return NewDispatchOrchestrator(dispatchers, dispatchRepo, nil, nil, nil, nil, 5, 60*time.Second)
}

func TestOrchestrator_FallbackLoop(t *testing.T) {
	pool := getTestPool(t)

	ctx := context.Background()
	wsRepo := repository.NewWorkspaceRepository(pool)
	dispatchRepo := repository.NewMessageDispatchRepository(pool)

	ws, err := wsRepo.Create(ctx, "orch_test_ws_"+uuid.New().String())
	if err != nil {
		t.Fatalf("failed to create workspace: %v", err)
	}
	defer func() { _ = wsRepo.Delete(ctx, ws.ID) }()

	t.Run("Terminal error triggers fallback immediately", func(t *testing.T) {
		traceID := uuid.New().String()
		qMsg := &domain.QueueMessage{
			WorkspaceID:      ws.ID,
			TraceID:          traceID,
			To:               "+123",
			Channel:          "whatsapp",
			Body:             "test terminal",
			FallbackChannels: []string{"whatsapp_cloud", "telegram"},
		}

		msg := &fakeDispatchMsg{}

		registry := channel.NewRegistry(nil)
		disp1 := &fakeDispatcher{err: channel.NewTerminalError(errors.New("banned"))}
		disp2 := &fakeDispatcher{err: nil}
		registry.Register("whatsapp", disp1)
		registry.Register("whatsapp_cloud", disp2)

		orchestrator := newTestOrchestrator(registry, dispatchRepo)

		_ = orchestrator.Process(ctx, msg, qMsg, 0)

		if !msg.acked {
			t.Error("expected message to be acked")
		}
		if disp1.calledCount != 1 {
			t.Errorf("expected disp1 called once, got %d", disp1.calledCount)
		}
		if disp2.calledCount != 1 {
			t.Errorf("expected disp2 called once, got %d", disp2.calledCount)
		}

		d, err := dispatchRepo.GetByTraceID(ctx, qMsg.WorkspaceID, traceID)
		if err != nil {
			t.Fatalf("failed to get dispatch from DB: %v", err)
		}
		if d.Status != "sent" {
			t.Errorf("expected DB status 'sent', got %s", d.Status)
		}
		if d.CurrentChannel != "whatsapp_cloud" {
			t.Errorf("expected DB current channel 'whatsapp_cloud', got %s", d.CurrentChannel)
		}
	})

	t.Run("Missing primary dispatcher falls back to registered channel", func(t *testing.T) {
		traceID := uuid.New().String()
		qMsg := &domain.QueueMessage{
			WorkspaceID:      ws.ID,
			TraceID:          traceID,
			To:               "local-recipient",
			Channel:          "unregistered-primary",
			Body:             "fallback test",
			FallbackChannels: []string{"whatsapp_mock"},
		}
		msg := &fakeDispatchMsg{}
		fallback := &fakeDispatcher{}
		registry := channel.NewRegistry(map[string]channel.Dispatcher{
			"whatsapp_mock": fallback,
		})

		orchestrator := newTestOrchestrator(registry, dispatchRepo)
		if err := orchestrator.Process(ctx, msg, qMsg, 0); err != nil {
			t.Fatalf("process: %v", err)
		}
		if !msg.acked {
			t.Fatal("expected message to be acked")
		}
		if fallback.calledCount != 1 {
			t.Fatalf("expected fallback dispatcher once, got %d", fallback.calledCount)
		}

		dispatch, err := dispatchRepo.GetByTraceID(ctx, qMsg.WorkspaceID, traceID)
		if err != nil {
			t.Fatalf("get dispatch: %v", err)
		}
		if dispatch.Status != "sent" || dispatch.CurrentChannel != "whatsapp_mock" {
			t.Fatalf("unexpected dispatch state: status=%s channel=%s", dispatch.Status, dispatch.CurrentChannel)
		}
	})

	t.Run("Explicit retryable response triggers NAK, does not advance fallback", func(t *testing.T) {
		traceID := uuid.New().String()
		qMsg := &domain.QueueMessage{
			WorkspaceID:      ws.ID,
			TraceID:          traceID,
			To:               "+123",
			Channel:          "whatsapp",
			Body:             "test transient",
			FallbackChannels: []string{"whatsapp_cloud"},
		}

		msg := &fakeDispatchMsg{}

		registry := channel.NewRegistry(nil)
		disp1 := &fakeDispatcher{err: errors.New("provider returned rate limit")}
		registry.Register("whatsapp", disp1)

		orchestrator := newTestOrchestrator(registry, dispatchRepo)

		_ = orchestrator.Process(ctx, msg, qMsg, 0)

		if msg.acked {
			t.Error("expected message NOT to be acked")
		}
		if !msg.nacked {
			t.Error("expected message to be nacked")
		}
		if disp1.calledCount != 1 {
			t.Errorf("expected disp1 called once, got %d", disp1.calledCount)
		}

		d, err := dispatchRepo.GetByTraceID(ctx, qMsg.WorkspaceID, traceID)
		if err != nil {
			t.Fatalf("failed to get dispatch from DB: %v", err)
		}
		if d.Status != "failed_transient" {
			t.Errorf("expected DB status 'failed_transient', got %s", d.Status)
		}
		if d.ErrorMessage == nil || *d.ErrorMessage != deliveryTransientCode {
			t.Fatalf("error_message=%v, want %q", d.ErrorMessage, deliveryTransientCode)
		}
	})

	t.Run("Exhausted transient retries persist terminal failure and outbox before ACK", func(t *testing.T) {
		traceID := uuid.New().String()
		qMsg := &domain.QueueMessage{
			WorkspaceID: ws.ID,
			TraceID:     traceID,
			To:          "+123",
			Channel:     "whatsapp",
			Body:        "always transient",
		}
		registry := channel.NewRegistry(nil)
		dispatcher := &fakeDispatcher{err: errors.New("provider unavailable")}
		registry.Register("whatsapp", dispatcher)
		orchestrator := NewDispatchOrchestrator(
			registry,
			dispatchRepo,
			nil,
			nil,
			nil,
			nil,
			2,
			time.Minute,
		)

		for attempt := 0; attempt < 2; attempt++ {
			msg := &fakeDispatchMsg{}
			_ = orchestrator.Process(ctx, msg, qMsg, attempt)
			if msg.acked || !msg.nacked {
				t.Fatalf("attempt %d ack=%v nak=%v, want retry", attempt, msg.acked, msg.nacked)
			}
		}

		finalMsg := &fakeDispatchMsg{}
		_ = orchestrator.Process(ctx, finalMsg, qMsg, 2)
		if !finalMsg.acked || finalMsg.nacked {
			t.Fatalf("final delivery ack=%v nak=%v, want terminal ACK", finalMsg.acked, finalMsg.nacked)
		}
		if dispatcher.calledCount != 3 {
			t.Fatalf("provider calls=%d, want 3 bounded attempts", dispatcher.calledCount)
		}

		dispatch, err := dispatchRepo.GetByTraceID(ctx, qMsg.WorkspaceID, traceID)
		if err != nil {
			t.Fatalf("get terminal dispatch: %v", err)
		}
		if dispatch.Status != "failed" {
			t.Fatalf("status=%q, want failed", dispatch.Status)
		}
		if dispatch.ErrorMessage == nil || *dispatch.ErrorMessage != deliveryRetriesCode {
			t.Fatalf("error_message=%v, want %q", dispatch.ErrorMessage, deliveryRetriesCode)
		}

		events, err := dispatchRepo.ListPendingProviderDeliveryEvents(ctx, 100)
		if err != nil {
			t.Fatalf("list pending delivery events: %v", err)
		}
		foundTerminal := false
		for _, event := range events {
			if event.DispatchID == dispatch.ID && event.Status == "failed" {
				foundTerminal = true
				break
			}
		}
		if !foundTerminal {
			t.Fatal("terminal failed event was not durably enqueued")
		}
	})

	t.Run("Uncertain transport outcome never retries or falls back", func(t *testing.T) {
		traceID := uuid.New().String()
		qMsg := &domain.QueueMessage{
			WorkspaceID:      ws.ID,
			TraceID:          traceID,
			To:               "+123",
			Channel:          "whatsapp",
			Body:             "response lost",
			FallbackChannels: []string{"whatsapp_cloud"},
		}
		msg := &fakeDispatchMsg{}
		registry := channel.NewRegistry(nil)
		const sensitiveTransportDetail = "connection reset at https://api.example.invalid/bot-secret/send?to=+123"
		primary := &fakeDispatcher{err: channel.NewUncertainError(errors.New(sensitiveTransportDetail))}
		fallback := &fakeDispatcher{}
		registry.Register("whatsapp", primary)
		registry.Register("whatsapp_cloud", fallback)

		publisher := &recordingQueuePublisher{}
		auditWriter := &recordingAuditWriter{}
		orchestrator := NewDispatchOrchestrator(
			registry,
			dispatchRepo,
			publisher,
			nil,
			auditWriter,
			nil,
			5,
			time.Minute,
		)
		_ = orchestrator.Process(ctx, msg, qMsg, 0)

		if !msg.acked || msg.nacked {
			t.Fatalf("uncertain message ack=%v nak=%v", msg.acked, msg.nacked)
		}
		if primary.calledCount != 1 || fallback.calledCount != 0 {
			t.Fatalf("provider calls primary=%d fallback=%d", primary.calledCount, fallback.calledCount)
		}
		dispatch, err := dispatchRepo.GetByTraceID(ctx, qMsg.WorkspaceID, traceID)
		if err != nil {
			t.Fatalf("get uncertain dispatch: %v", err)
		}
		if dispatch.Status != "uncertain" {
			t.Fatalf("status=%q, want uncertain", dispatch.Status)
		}
		if dispatch.ErrorMessage == nil || *dispatch.ErrorMessage != deliveryUncertainCode {
			t.Fatalf("error_message=%v, want %q", dispatch.ErrorMessage, deliveryUncertainCode)
		}
		for _, event := range auditWriter.events {
			if string(event.Payload) == "" || !json.Valid(event.Payload) {
				t.Fatalf("invalid audit payload: %q", event.Payload)
			}
			if bytes.Contains(event.Payload, []byte(sensitiveTransportDetail)) {
				t.Fatalf("sensitive transport detail leaked to audit: %s", event.Payload)
			}
		}
		var uncertainEvent struct {
			Event     string `json:"event"`
			MessageID string `json:"message_id"`
			Error     string `json:"error"`
		}
		foundUncertainFailure := false
		for _, published := range publisher.published {
			if published.subject != "webhooks.events" {
				continue
			}
			if err := json.Unmarshal(published.data, &uncertainEvent); err != nil {
				t.Fatalf("decode delivery event: %v", err)
			}
			if uncertainEvent.Event == "sending" || uncertainEvent.Event == "failed_transient" || uncertainEvent.Event == "uncertain" {
				t.Fatalf("internal state leaked as public delivery event: %q", uncertainEvent.Event)
			}
			if uncertainEvent.Event == "failed" && uncertainEvent.Error == "DELIVERY_UNCERTAIN" {
				foundUncertainFailure = true
				if published.dedupID != traceID+".delivery.failed" {
					t.Fatalf("uncertain failure dedup_id=%q", published.dedupID)
				}
			}
		}
		if !foundUncertainFailure {
			t.Fatal("stable failed/DELIVERY_UNCERTAIN webhook event was not published")
		}

		redelivery := &fakeDispatchMsg{}
		_ = orchestrator.Process(ctx, redelivery, qMsg, 1)
		if !redelivery.acked || primary.calledCount != 1 || fallback.calledCount != 0 {
			t.Fatalf(
				"uncertain redelivery ack=%v primary=%d fallback=%d",
				redelivery.acked,
				primary.calledCount,
				fallback.calledCount,
			)
		}
	})

	t.Run("Redelivery of sent message skips dispatch", func(t *testing.T) {
		traceID := uuid.New().String()
		d, err := dispatchRepo.GetOrCreateDispatch(ctx, ws.ID, traceID, "whatsapp", nil, nil, nil)
		if err != nil {
			t.Fatalf("failed to create dispatch: %v", err)
		}
		err = dispatchRepo.UpdateDispatchStatus(ctx, d.ID, "sent", "whatsapp", 0, nil)
		if err != nil {
			t.Fatalf("failed to update status: %v", err)
		}

		qMsg := &domain.QueueMessage{
			WorkspaceID: ws.ID,
			TraceID:     traceID,
			To:          "+123",
			Channel:     "whatsapp",
			Body:        "test redelivery",
		}
		msg := &fakeDispatchMsg{}

		registry := channel.NewRegistry(nil)
		disp1 := &fakeDispatcher{err: nil}
		registry.Register("whatsapp", disp1)

		orchestrator := newTestOrchestrator(registry, dispatchRepo)

		_ = orchestrator.Process(ctx, msg, qMsg, 0)

		if !msg.acked {
			t.Error("expected message to be acked")
		}
		if disp1.calledCount != 0 {
			t.Errorf("expected dispatcher NOT to be called, got %d", disp1.calledCount)
		}
	})

	t.Run("Exhaustion of all fallback channels marks failed", func(t *testing.T) {
		traceID := uuid.New().String()
		qMsg := &domain.QueueMessage{
			WorkspaceID:      ws.ID,
			TraceID:          traceID,
			To:               "+123",
			Channel:          "whatsapp",
			Body:             "test exhaustion",
			FallbackChannels: []string{"telegram"},
		}
		msg := &fakeDispatchMsg{}

		registry := channel.NewRegistry(nil)
		disp1 := &fakeDispatcher{err: channel.NewTerminalError(errors.New("terminal 1"))}
		disp2 := &fakeDispatcher{err: channel.NewTerminalError(errors.New("terminal 2"))}
		registry.Register("whatsapp", disp1)
		registry.Register("telegram", disp2)

		orchestrator := newTestOrchestrator(registry, dispatchRepo)

		_ = orchestrator.Process(ctx, msg, qMsg, 0)

		if !msg.acked {
			t.Error("expected message to be acked (stop retries)")
		}

		d, err := dispatchRepo.GetByTraceID(ctx, qMsg.WorkspaceID, traceID)
		if err != nil {
			t.Fatalf("failed to get dispatch from DB: %v", err)
		}
		if d.Status != "failed" {
			t.Errorf("expected DB status 'failed', got %s", d.Status)
		}
		if d.ErrorMessage == nil || *d.ErrorMessage != deliveryFailedCode {
			t.Fatalf("error_message=%v, want %q", d.ErrorMessage, deliveryFailedCode)
		}
	})

	t.Run("TTL expired message is dropped", func(t *testing.T) {
		traceID := uuid.New().String()
		qMsg := &domain.QueueMessage{
			WorkspaceID: ws.ID,
			TraceID:     traceID,
			To:          "+123",
			Channel:     "whatsapp",
			Body:        "test TTL",
			QueuedAt:    time.Now().UTC().Add(-600 * time.Second),
			TTLSeconds:  intPtr(300),
		}
		msg := &fakeDispatchMsg{}

		registry := channel.NewRegistry(nil)
		disp1 := &fakeDispatcher{err: nil}
		registry.Register("whatsapp", disp1)

		orchestrator := newTestOrchestrator(registry, dispatchRepo)

		_ = orchestrator.Process(ctx, msg, qMsg, 0)

		if !msg.acked {
			t.Error("expected expired message to be acked")
		}
		if disp1.calledCount != 0 {
			t.Errorf("expected dispatcher NOT to be called for expired message, got %d", disp1.calledCount)
		}
		d, err := dispatchRepo.GetByTraceID(ctx, qMsg.WorkspaceID, traceID)
		if err != nil {
			t.Fatalf("failed to get expired dispatch from DB: %v", err)
		}
		if d.ErrorMessage == nil || *d.ErrorMessage != deliveryExpiredCode {
			t.Fatalf("error_message=%v, want %q", d.ErrorMessage, deliveryExpiredCode)
		}
	})
}

func TestOrchestrator_DurableClaimPreventsCrossInstanceDuplicate(t *testing.T) {
	pool := getTestPool(t)
	ctx := context.Background()
	wsRepo := repository.NewWorkspaceRepository(pool)
	dispatchRepo := repository.NewMessageDispatchRepository(pool)

	ws, err := wsRepo.Create(ctx, "orch_claim_"+uuid.New().String())
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	defer func() { _ = wsRepo.Delete(ctx, ws.ID) }()

	dispatcher := &blockingConcurrentDispatcher{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	registry := channel.NewRegistry(map[string]channel.Dispatcher{
		"whatsapp_mock": dispatcher,
	})
	first := newTestOrchestrator(registry, dispatchRepo)
	second := newTestOrchestrator(registry, dispatchRepo)
	qMsg := &domain.QueueMessage{
		MessageID:   uuid.New(),
		WorkspaceID: ws.ID,
		TraceID:     "cross-instance-" + uuid.New().String(),
		To:          "recipient",
		Channel:     "whatsapp_mock",
		Body:        "once",
		QueuedAt:    time.Now().UTC(),
	}
	firstMsg := &fakeDispatchMsg{}
	secondMsg := &fakeDispatchMsg{}
	firstDone := make(chan error, 1)
	secondDone := make(chan error, 1)

	go func() { firstDone <- first.Process(ctx, firstMsg, qMsg, 0) }()
	select {
	case <-dispatcher.entered:
	case <-time.After(time.Second):
		t.Fatal("first dispatch did not reach provider")
	}
	go func() { secondDone <- second.Process(ctx, secondMsg, qMsg, 0) }()

	select {
	case <-secondDone:
	case <-time.After(time.Second):
		close(dispatcher.release)
		t.Fatal("second orchestrator did not yield its duplicate claim")
	}
	if got := dispatcher.calls.Load(); got != 1 {
		close(dispatcher.release)
		t.Fatalf("provider calls=%d while first delivery is active, want 1", got)
	}
	if !secondMsg.nacked || secondMsg.acked || secondMsg.nakDelay <= 0 {
		close(dispatcher.release)
		t.Fatalf("duplicate message ack=%v nak=%v delay=%s", secondMsg.acked, secondMsg.nacked, secondMsg.nakDelay)
	}

	close(dispatcher.release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first process: %v", err)
	}
	if !firstMsg.acked {
		t.Fatal("first message was not acked")
	}

	redelivery := &fakeDispatchMsg{}
	if err := second.Process(ctx, redelivery, qMsg, 1); err != nil {
		t.Fatalf("redelivery: %v", err)
	}
	if !redelivery.acked {
		t.Fatal("sent duplicate was not acked")
	}
	if got := dispatcher.calls.Load(); got != 1 {
		t.Fatalf("provider calls after redelivery=%d, want 1", got)
	}
}

func TestOrchestratorDeliveryEventsUseIngressReceipt(t *testing.T) {
	pool := getTestPool(t)
	ctx := context.Background()
	wsRepo := repository.NewWorkspaceRepository(pool)
	dispatchRepo := repository.NewMessageDispatchRepository(pool)
	ws, err := wsRepo.Create(ctx, "orch_receipt_"+uuid.New().String())
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	defer func() { _ = wsRepo.Delete(ctx, ws.ID) }()

	type deliveryEvent struct {
		Event       string `json:"event"`
		TraceID     string `json:"trace_id"`
		MessageID   string `json:"message_id"`
		Channel     string `json:"channel"`
		WorkspaceID string `json:"workspace_id"`
	}

	t.Run("sent", func(t *testing.T) {
		traceID := "pymes.v1." + ws.ID.String() + ".sent"
		receiptID := uuid.New()
		publisher := &recordingQueuePublisher{}
		dispatcher := &fakeDispatcher{}
		registry := channel.NewRegistry(map[string]channel.Dispatcher{"whatsapp_mock": dispatcher})
		orchestrator := NewDispatchOrchestrator(
			registry,
			dispatchRepo,
			publisher,
			nil,
			nil,
			nil,
			5,
			time.Minute,
		)
		msg := &fakeDispatchMsg{}
		err := orchestrator.Process(ctx, msg, &domain.QueueMessage{
			MessageID:   receiptID,
			WorkspaceID: ws.ID,
			TraceID:     traceID,
			To:          "local-recipient",
			Channel:     "whatsapp_mock",
			Body:        "test",
			QueuedAt:    time.Now().UTC(),
		}, 0)
		if err != nil {
			t.Fatalf("process sent: %v", err)
		}
		if dispatcher.lastMessageID != receiptID.String() {
			t.Fatalf("dispatcher message_id=%q, want %s", dispatcher.lastMessageID, receiptID)
		}

		var sent *recordedQueuePublish
		for i := range publisher.published {
			var event deliveryEvent
			if publisher.published[i].subject != "webhooks.events" {
				continue
			}
			if err := json.Unmarshal(publisher.published[i].data, &event); err != nil {
				t.Fatalf("decode event: %v", err)
			}
			if event.Event == "sent" {
				sent = &publisher.published[i]
				if event.MessageID != receiptID.String() ||
					event.TraceID != traceID ||
					event.Channel != "whatsapp_mock" ||
					event.WorkspaceID != ws.ID.String() {
					t.Fatalf("unexpected sent event: %+v", event)
				}
			}
		}
		if sent == nil {
			t.Fatal("sent webhook event was not published")
		}
		if sent.dedupID != traceID+".delivery.sent" {
			t.Fatalf("sent dedup id=%q", sent.dedupID)
		}
	})

	t.Run("failed", func(t *testing.T) {
		traceID := "pymes.v1." + ws.ID.String() + ".failed"
		receiptID := uuid.New()
		publisher := &recordingQueuePublisher{}
		dispatcher := &fakeDispatcher{err: channel.NewTerminalError(errors.New("provider rejected"))}
		registry := channel.NewRegistry(map[string]channel.Dispatcher{"whatsapp_mock": dispatcher})
		orchestrator := NewDispatchOrchestrator(
			registry,
			dispatchRepo,
			publisher,
			nil,
			nil,
			nil,
			5,
			time.Minute,
		)
		msg := &fakeDispatchMsg{}
		_ = orchestrator.Process(ctx, msg, &domain.QueueMessage{
			MessageID:   receiptID,
			WorkspaceID: ws.ID,
			TraceID:     traceID,
			To:          "local-recipient",
			Channel:     "whatsapp_mock",
			Body:        "test",
			QueuedAt:    time.Now().UTC(),
		}, 0)

		var failed *recordedQueuePublish
		for i := range publisher.published {
			var event deliveryEvent
			if publisher.published[i].subject != "webhooks.events" {
				continue
			}
			if err := json.Unmarshal(publisher.published[i].data, &event); err != nil {
				t.Fatalf("decode event: %v", err)
			}
			if event.Event == "failed" {
				failed = &publisher.published[i]
				if event.MessageID != receiptID.String() || event.TraceID != traceID {
					t.Fatalf("unexpected failed event: %+v", event)
				}
			}
		}
		if failed == nil {
			t.Fatal("failed webhook event was not published")
		}
		if failed.dedupID != traceID+".delivery.failed" {
			t.Fatalf("failed dedup id=%q", failed.dedupID)
		}
	})
}

func TestOrchestrator_QueueDepthDecrement(t *testing.T) {
	tracker := &fakeQueueDepthTracker{}
	orchestrator := &DispatchOrchestrator{
		queueDepth: tracker,
	}

	wsID := uuid.New()

	// ack calls Decrement
	msg := &fakeDispatchMsg{}
	orchestrator.ack(msg, wsID)
	if tracker.decrements[wsID] != 1 {
		t.Errorf("expected 1 decrement, got %d", tracker.decrements[wsID])
	}

	// handleFailure (above max retries) calls ack → decrement
	msg2 := &fakeDispatchMsg{}
	orchestrator.maxRetries = 0
	orchestrator.handleFailure(msg2, wsID, "trace-123", 0)
	if tracker.decrements[wsID] != 2 {
		t.Errorf("expected 2 decrements after terminal failure, got %d", tracker.decrements[wsID])
	}
}

func TestOrchestrator_TelegramContactResolution(t *testing.T) {
	pool := getTestPool(t)

	ctx := context.Background()
	wsRepo := repository.NewWorkspaceRepository(pool)
	contactRepo := repository.NewContactRepository(pool)

	ws, err := wsRepo.Create(ctx, "tg_res_test_ws_"+uuid.New().String())
	if err != nil {
		t.Fatalf("failed to create workspace: %v", err)
	}
	defer func() { _ = wsRepo.Delete(ctx, ws.ID) }()

	// Upsert mapping: username "@my_user" -> "chat_98765"
	_, err = contactRepo.ResolveContact(ctx, ws.ID, "telegram", "chat_98765", "My User", "@my_user", "")
	if err != nil {
		t.Fatalf("failed to upsert contact: %v", err)
	}

	// Setup mock dispatcher
	tgDisp := &fakeDispatcher{err: nil}
	registry := channel.NewRegistry(map[string]channel.Dispatcher{
		"telegram": tgDisp,
	})

	orchestrator := NewDispatchOrchestrator(registry, nil, nil, nil, nil, contactRepo, 5, 60*time.Second)

	qMsg := &domain.QueueMessage{
		WorkspaceID: ws.ID,
		TraceID:     uuid.New().String(),
		To:          "@my_user",
		Channel:     "telegram",
		Body:        "hello world",
	}

	msg := &fakeDispatchMsg{}
	err = orchestrator.Process(ctx, msg, qMsg, 0)
	if err != nil {
		t.Fatalf("process failed: %v", err)
	}

	if tgDisp.lastTo != "chat_98765" {
		t.Errorf("expected dispatched To to be 'chat_98765', got %s", tgDisp.lastTo)
	}
}

func intPtr(i int) *int { return &i }
