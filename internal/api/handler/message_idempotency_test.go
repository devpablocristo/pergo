package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/pablojhp.pergo/internal/domain"
	"github.com/pablojhp.pergo/internal/outbound"
	"github.com/pablojhp.pergo/internal/repository"
)

type memoryIngressRow struct {
	hash       [32]byte
	traceID    string
	receiptID  uuid.UUID
	token      uuid.UUID
	generation int64
	expiresAt  time.Time
	queuedAt   time.Time
	queued     bool
}

type memoryIngressLedger struct {
	mu   sync.Mutex
	rows map[string]*memoryIngressRow
}

func newMemoryIngressLedger() *memoryIngressLedger {
	return &memoryIngressLedger{rows: make(map[string]*memoryIngressRow)}
}

func (l *memoryIngressLedger) Claim(
	_ context.Context,
	workspaceID uuid.UUID,
	idempotencyKey string,
	payloadHash [32]byte,
	traceID string,
	receiptID uuid.UUID,
	lease time.Duration,
) (uuid.UUID, time.Time, uuid.UUID, int64, bool, time.Duration, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	key := workspaceID.String() + "\x00" + idempotencyKey
	row := l.rows[key]
	if row == nil {
		row = &memoryIngressRow{
			hash:       payloadHash,
			traceID:    traceID,
			receiptID:  receiptID,
			token:      uuid.New(),
			generation: 1,
			expiresAt:  time.Now().Add(lease),
		}
		l.rows[key] = row
		return row.receiptID, time.Time{}, row.token, row.generation, false, 0, nil
	}
	if row.hash != payloadHash || row.traceID != traceID {
		return uuid.Nil, time.Time{}, uuid.Nil, 0, false, 0, repository.ErrIngressIdempotencyKeyReused
	}
	if row.queued {
		return row.receiptID, row.queuedAt, uuid.Nil, row.generation, true, 0, nil
	}
	if retryAfter := time.Until(row.expiresAt); retryAfter > 0 {
		return row.receiptID, time.Time{}, uuid.Nil, row.generation, false, retryAfter, repository.ErrIngressClaimActive
	}
	row.token = uuid.New()
	row.generation++
	row.expiresAt = time.Now().Add(lease)
	return row.receiptID, time.Time{}, row.token, row.generation, false, 0, nil
}

func (l *memoryIngressLedger) MarkQueued(
	_ context.Context,
	workspaceID uuid.UUID,
	idempotencyKey string,
	claimToken uuid.UUID,
	claimGeneration int64,
	queuedAt time.Time,
) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	key := workspaceID.String() + "\x00" + idempotencyKey
	row := l.rows[key]
	if row == nil || row.queued || row.token != claimToken || row.generation != claimGeneration {
		return repository.ErrIngressClaimLost
	}
	row.queued = true
	row.queuedAt = queuedAt.UTC()
	return nil
}

type blockingPublisher struct {
	count   atomic.Int32
	entered chan struct{}
	release chan struct{}
	once    sync.Once

	mu      sync.Mutex
	payload []byte
	traceID string
}

func newBlockingPublisher(block bool) *blockingPublisher {
	p := &blockingPublisher{}
	if block {
		p.entered = make(chan struct{})
		p.release = make(chan struct{})
	}
	return p
}

func (p *blockingPublisher) Publish(_ context.Context, _ string, data []byte, traceID string) error {
	p.count.Add(1)
	p.mu.Lock()
	p.payload = append([]byte(nil), data...)
	p.traceID = traceID
	p.mu.Unlock()
	if p.entered != nil {
		p.once.Do(func() { close(p.entered) })
		<-p.release
	}
	return nil
}

func durableMessageHandler(ledger MessageIngressLedger, publisher Publisher) *MessageHandler {
	processor := outbound.NewProcessor(nil, nil, defaultMockConnectionRepo(), publisher)
	return &MessageHandler{
		Ingestor:      processor,
		IngressLedger: ledger,
		IngressLease:  time.Second,
	}
}

func durableMessageRequest(
	t *testing.T,
	e *echo.Echo,
	workspaceID uuid.UUID,
	idempotencyKey string,
	traceID string,
	body string,
) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/messages", strings.NewReader(body))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	if idempotencyKey != "" {
		req.Header.Set("Idempotency-Key", idempotencyKey)
	}
	if traceID != "" {
		req.Header.Set("X-Trace-ID", traceID)
	}
	req = req.WithContext(testContext(traceID, workspaceID))
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	return rec
}

func decodeMessageResponse(t *testing.T, rec *httptest.ResponseRecorder) domain.CreateMessageResponse {
	t.Helper()
	var response domain.CreateMessageResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode message response: %v", err)
	}
	return response
}

func decodeErrorResponse(t *testing.T, rec *httptest.ResponseRecorder) domain.ErrorResponse {
	t.Helper()
	var response domain.ErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	return response
}

func TestDurableMessageReplayReturnsSameReceiptAndPublishesOnce(t *testing.T) {
	ledger := newMemoryIngressLedger()
	publisher := newBlockingPublisher(false)
	e := echo.New()
	durableMessageHandler(ledger, publisher).RegisterRoutes(e)

	workspaceID := uuid.New()
	key := "pymes.notify." + uuid.New().String()
	traceID := "pymes.v1." + workspaceID.String() + ".message"
	body := `{"to":"5491100000000","channel":"whatsapp","body":"recordatorio","metadata":{"pymes_org_id":"org"}}`

	first := durableMessageRequest(t, e, workspaceID, key, traceID, body)
	if first.Code != http.StatusAccepted {
		t.Fatalf("first status=%d body=%s", first.Code, first.Body.String())
	}
	second := durableMessageRequest(t, e, workspaceID, key, traceID, body)
	if second.Code != http.StatusAccepted {
		t.Fatalf("replay status=%d body=%s", second.Code, second.Body.String())
	}

	firstResponse := decodeMessageResponse(t, first)
	secondResponse := decodeMessageResponse(t, second)
	if firstResponse.MessageID != secondResponse.MessageID || firstResponse.MessageID == uuid.Nil {
		t.Fatalf("receipt changed first=%s replay=%s", firstResponse.MessageID, secondResponse.MessageID)
	}
	if !firstResponse.QueuedAt.Equal(secondResponse.QueuedAt) {
		t.Fatalf("queued_at changed first=%s replay=%s", firstResponse.QueuedAt, secondResponse.QueuedAt)
	}
	if got := publisher.count.Load(); got != 1 {
		t.Fatalf("publish count=%d, want 1", got)
	}

	publisher.mu.Lock()
	var queued domain.QueueMessage
	err := json.Unmarshal(publisher.payload, &queued)
	publishedTrace := publisher.traceID
	publisher.mu.Unlock()
	if err != nil {
		t.Fatalf("decode queue message: %v", err)
	}
	if queued.MessageID != firstResponse.MessageID || queued.TraceID != traceID || publishedTrace != traceID {
		t.Fatalf(
			"queue identity receipt=%s trace=%s nats_dedup=%s",
			queued.MessageID,
			queued.TraceID,
			publishedTrace,
		)
	}

	wantDeterministic := deterministicReceiptID(workspaceID, key)
	if firstResponse.MessageID != wantDeterministic {
		t.Fatalf("receipt=%s, want deterministic %s", firstResponse.MessageID, wantDeterministic)
	}
}

func TestDurableMessageRejectsChangedPayloadOrTrace(t *testing.T) {
	ledger := newMemoryIngressLedger()
	publisher := newBlockingPublisher(false)
	e := echo.New()
	durableMessageHandler(ledger, publisher).RegisterRoutes(e)

	workspaceID := uuid.New()
	key := "pymes.notify." + uuid.New().String()
	traceID := "pymes.v1." + workspaceID.String() + ".message"
	body := `{"to":"5491100000000","channel":"whatsapp","body":"original"}`

	first := durableMessageRequest(t, e, workspaceID, key, traceID, body)
	if first.Code != http.StatusAccepted {
		t.Fatalf("first status=%d body=%s", first.Code, first.Body.String())
	}

	changedBody := durableMessageRequest(
		t,
		e,
		workspaceID,
		key,
		traceID,
		`{"to":"5491100000000","channel":"whatsapp","body":"changed"}`,
	)
	if changedBody.Code != http.StatusConflict {
		t.Fatalf("changed body status=%d body=%s", changedBody.Code, changedBody.Body.String())
	}
	if got := decodeErrorResponse(t, changedBody).Code; got != "idempotency_key_reused" {
		t.Fatalf("changed body error code=%q", got)
	}

	changedTrace := durableMessageRequest(t, e, workspaceID, key, traceID+".other", body)
	if changedTrace.Code != http.StatusConflict {
		t.Fatalf("changed trace status=%d body=%s", changedTrace.Code, changedTrace.Body.String())
	}
	if got := decodeErrorResponse(t, changedTrace).Code; got != "idempotency_key_reused" {
		t.Fatalf("changed trace error code=%q", got)
	}
	if got := publisher.count.Load(); got != 1 {
		t.Fatalf("publish count=%d, want 1", got)
	}
}

func TestDurableMessageConcurrentReplayReturnsTooEarly(t *testing.T) {
	ledger := newMemoryIngressLedger()
	publisher := newBlockingPublisher(true)
	e := echo.New()
	durableMessageHandler(ledger, publisher).RegisterRoutes(e)

	workspaceID := uuid.New()
	key := "pymes.notify." + uuid.New().String()
	traceID := "pymes.v1." + workspaceID.String() + ".message"
	body := `{"to":"5491100000000","channel":"whatsapp","body":"concurrent"}`

	firstDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		firstDone <- durableMessageRequest(t, e, workspaceID, key, traceID, body)
	}()
	<-publisher.entered

	concurrent := durableMessageRequest(t, e, workspaceID, key, traceID, body)
	if concurrent.Code != http.StatusTooEarly {
		t.Fatalf("concurrent status=%d body=%s", concurrent.Code, concurrent.Body.String())
	}
	if got := decodeErrorResponse(t, concurrent).Code; got != "idempotency_in_progress" {
		t.Fatalf("concurrent error code=%q", got)
	}
	if concurrent.Header().Get("Retry-After") == "" {
		t.Fatal("425 response is missing Retry-After")
	}

	close(publisher.release)
	first := <-firstDone
	if first.Code != http.StatusAccepted {
		t.Fatalf("first status=%d body=%s", first.Code, first.Body.String())
	}
	if got := publisher.count.Load(); got != 1 {
		t.Fatalf("publish count=%d, want 1", got)
	}
}

func TestDurableMessageRequiresIdempotencyAndTraceHeaders(t *testing.T) {
	ledger := newMemoryIngressLedger()
	publisher := newBlockingPublisher(false)
	e := echo.New()
	durableMessageHandler(ledger, publisher).RegisterRoutes(e)
	workspaceID := uuid.New()
	body := `{"to":"5491100000000","channel":"whatsapp","body":"headers"}`

	missingKey := durableMessageRequest(t, e, workspaceID, "", "pymes.v1.org.message", body)
	if missingKey.Code != http.StatusBadRequest || decodeErrorResponse(t, missingKey).Code != "missing_idempotency_key" {
		t.Fatalf("missing key status=%d body=%s", missingKey.Code, missingKey.Body.String())
	}

	missingTrace := durableMessageRequest(t, e, workspaceID, "pymes.notify.message", "", body)
	if missingTrace.Code != http.StatusBadRequest || decodeErrorResponse(t, missingTrace).Code != "missing_trace_id" {
		t.Fatalf("missing trace status=%d body=%s", missingTrace.Code, missingTrace.Body.String())
	}
	if got := publisher.count.Load(); got != 0 {
		t.Fatalf("publish count=%d, want 0", got)
	}
}

func TestDurableMessageRejectsOversizedBodyBeforeClaim(t *testing.T) {
	ledger := newMemoryIngressLedger()
	publisher := newBlockingPublisher(false)
	e := echo.New()
	durableMessageHandler(ledger, publisher).RegisterRoutes(e)
	workspaceID := uuid.New()

	rec := durableMessageRequest(
		t,
		e,
		workspaceID,
		"pymes.notify.oversized",
		"pymes.v1.org.oversized",
		strings.Repeat("x", maxDurableMessageBodyBytes+1),
	)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized status=%d body=%s", rec.Code, rec.Body.String())
	}
	if got := decodeErrorResponse(t, rec).Code; got != "payload_too_large" {
		t.Fatalf("oversized error code=%q", got)
	}
	if got := publisher.count.Load(); got != 0 {
		t.Fatalf("publish count=%d, want 0", got)
	}
}

func TestMemoryIngressLedgerExpiredClaimFencesStaleCompletion(t *testing.T) {
	ledger := newMemoryIngressLedger()
	workspaceID := uuid.New()
	key := "key"
	traceID := "trace"
	hash := hashIngressPayload(traceID, []byte(`{"body":"x"}`))
	receipt := uuid.New()

	_, _, oldToken, oldGeneration, _, _, err := ledger.Claim(
		context.Background(), workspaceID, key, hash, traceID, receipt, time.Millisecond,
	)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	time.Sleep(3 * time.Millisecond)
	_, _, newToken, newGeneration, _, _, err := ledger.Claim(
		context.Background(), workspaceID, key, hash, traceID, receipt, time.Second,
	)
	if err != nil {
		t.Fatalf("recovered claim: %v", err)
	}
	if err := ledger.MarkQueued(
		context.Background(), workspaceID, key, oldToken, oldGeneration, time.Now(),
	); !errors.Is(err, repository.ErrIngressClaimLost) {
		t.Fatalf("stale completion error=%v", err)
	}
	if err := ledger.MarkQueued(
		context.Background(), workspaceID, key, newToken, newGeneration, time.Now(),
	); err != nil {
		t.Fatalf("current completion: %v", err)
	}
}
