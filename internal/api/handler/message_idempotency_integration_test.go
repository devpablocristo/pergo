package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v5"

	"github.com/pablojhp.pergo/internal/domain"
	"github.com/pablojhp.pergo/internal/platform/postgres"
	"github.com/pablojhp.pergo/internal/repository"
)

func TestCreateMessageDurableResponseLossSurvivesProcessRestart(
	t *testing.T,
) {
	pool := messageIdempotencyTestPool(t)
	t.Cleanup(pool.Close)
	workspaceRepository := repository.NewWorkspaceRepository(pool)
	workspace, err := workspaceRepository.Create(
		t.Context(),
		"message_handler_idempotency_"+uuid.NewString(),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = workspaceRepository.Delete(context.Background(), workspace.ID)
	})

	ingestor := &countingIngestor{}
	idempotencyKey := "pymes:response-lost:" + uuid.NewString()
	body := `{"to":"+1234567890","channel":"whatsapp","body":"Hello"}`
	firstTraceID := uuid.NewString()
	first := sendDurableMessage(
		t,
		&MessageHandler{
			Ingestor:    ingestor,
			Idempotency: repository.NewMessageIdempotencyRepository(pool),
		},
		workspace.ID,
		firstTraceID,
		idempotencyKey,
		body,
	)
	if first.Code != http.StatusAccepted {
		t.Fatalf("first response = %d: %s", first.Code, first.Body.String())
	}
	var firstReceipt domain.CreateMessageResponse
	if err = json.Unmarshal(first.Body.Bytes(), &firstReceipt); err != nil {
		t.Fatal(err)
	}

	// The first response is considered lost. A reconstructed handler and
	// repository must read the accepted receipt from PostgreSQL.
	replay := sendDurableMessage(
		t,
		&MessageHandler{
			Ingestor:    ingestor,
			Idempotency: repository.NewMessageIdempotencyRepository(pool),
		},
		workspace.ID,
		uuid.NewString(),
		idempotencyKey,
		body,
	)
	if replay.Code != http.StatusAccepted {
		t.Fatalf("replay response = %d: %s", replay.Code, replay.Body.String())
	}
	var replayReceipt domain.CreateMessageResponse
	if err = json.Unmarshal(replay.Body.Bytes(), &replayReceipt); err != nil {
		t.Fatal(err)
	}
	if replayReceipt != firstReceipt ||
		replay.Header().Get("X-Trace-Id") != firstTraceID ||
		replay.Header().Get("Idempotency-Replayed") != "true" {
		t.Fatalf(
			"durable receipt changed: first=%+v replay=%+v headers=%v",
			firstReceipt,
			replayReceipt,
			replay.Header(),
		)
	}
	if ingestor.Attempts() != 1 {
		t.Fatalf("ingestion attempts = %d, want 1", ingestor.Attempts())
	}
	record, err := repository.NewMessageIdempotencyRepository(pool).Get(
		t.Context(),
		workspace.ID,
		idempotencyKey,
	)
	if err != nil || !record.Accepted() ||
		record.MessageID != firstReceipt.MessageID {
		t.Fatalf("durable ledger record=%+v err=%v", record, err)
	}
}

func messageIdempotencyTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	databaseURL := strings.TrimSpace(os.Getenv("PERGO_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("PERGO_DATABASE_URL is not configured")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if err = pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	db, err := postgres.NewSQLDB(pool)
	if err != nil {
		pool.Close()
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if err = postgres.RunMigrations(db); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	return pool
}

func sendDurableMessage(
	t *testing.T,
	handler *MessageHandler,
	workspaceID uuid.UUID,
	traceID string,
	idempotencyKey string,
	body string,
) *httptest.ResponseRecorder {
	t.Helper()
	server := echo.New()
	handler.RegisterRoutes(server)
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/messages",
		strings.NewReader(body),
	)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", idempotencyKey)
	request = request.WithContext(testContext(traceID, workspaceID))
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	return response
}
