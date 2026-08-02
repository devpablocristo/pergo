//go:build integration

package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v5"
	"github.com/nats-io/nats.go"

	"github.com/pablojhp.pergo/internal/api/handler"
	"github.com/pablojhp.pergo/internal/api/middleware"
	"github.com/pablojhp.pergo/internal/channel"
	"github.com/pablojhp.pergo/internal/channel/whatsappmock"
	"github.com/pablojhp.pergo/internal/outbound"
	"github.com/pablojhp.pergo/internal/platform/audit"
	"github.com/pablojhp.pergo/internal/platform/crypto"
	"github.com/pablojhp.pergo/internal/platform/queue"
	"github.com/pablojhp.pergo/internal/repository"
)

func TestWhatsAppMockEndToEnd(t *testing.T) {
	if integrationDBURL == "" {
		t.Fatal("TestMain did not provide a test-owned PostgreSQL URL")
	}
	if integrationNATSURL == "" || integrationNATSURL == nats.DefaultURL || integrationNATSURL == "nats://localhost:4222" {
		t.Fatalf("refusing non-isolated NATS URL %q", integrationNATSURL)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, integrationDBURL)
	if err != nil {
		t.Fatalf("connect to test-owned PostgreSQL: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("ping test-owned PostgreSQL: %v", err)
	}
	t.Cleanup(pool.Close)

	workspaceRepo := repository.NewWorkspaceRepository(pool)
	workspace, err := workspaceRepo.Create(ctx, "whatsapp_mock_e2e_"+uuid.New().String())
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM api_keys WHERE workspace_id = $1`, workspace.ID)
		if err := workspaceRepo.Delete(context.Background(), workspace.ID); err != nil {
			t.Errorf("delete test workspace: %v", err)
		}
	})

	apiKeyRepo := repository.NewAPIKeyRepository(pool)
	_, plaintextAPIKey, err := apiKeyRepo.Create(ctx, workspace.ID, "whatsapp mock e2e")
	if err != nil {
		t.Fatalf("create API key: %v", err)
	}

	encryptor, err := crypto.NewEncryptor(make([]byte, 32))
	if err != nil {
		t.Fatalf("create credential encryptor: %v", err)
	}
	connectionRepo := repository.NewConnectionRepository(pool, encryptor)
	connectionID := uuid.New()
	connection := &repository.Connection{
		ID:             connectionID,
		WorkspaceID:    workspace.ID,
		Name:           "WhatsApp Mock E2E",
		Channel:        "whatsapp_mock",
		SenderIdentity: "whatsapp-mock:" + connectionID.String(),
		Status:         "connected",
		IsDefault:      true,
	}
	if err := connectionRepo.Create(ctx, connection); err != nil {
		t.Fatalf("create default mock connection: %v", err)
	}

	nc, err := nats.Connect(integrationNATSURL)
	if err != nil {
		t.Fatalf("connect to test-owned NATS at %q: %v", integrationNATSURL, err)
	}

	stream, err := queue.EnsureStream(ctx, nc)
	if err != nil {
		nc.Close()
		t.Fatalf("ensure outbound stream: %v", err)
	}
	consumerName := "whatsapp-mock-e2e-" + uuid.New().String()
	consumer, err := queue.EnsureConsumer(ctx, stream, consumerName)
	if err != nil {
		nc.Close()
		t.Fatalf("ensure isolated consumer: %v", err)
	}

	publisher := queue.NewJetStreamPublisher(nc)
	dispatchRepo := repository.NewMessageDispatchRepository(pool)
	auditWriter := audit.NewWriter(pool, 32, 1)
	registry := channel.NewRegistry(map[string]channel.Dispatcher{
		"whatsapp_mock": whatsappmock.NewAdapter(),
	})
	orchestrator := queue.NewDispatchOrchestrator(
		registry,
		dispatchRepo,
		nil,
		nil,
		auditWriter,
		nil,
		5,
		5*time.Second,
	)
	worker := queue.NewWorker(ctx, consumer, orchestrator)
	t.Cleanup(func() {
		stopped := make(chan struct{})
		go func() {
			worker.Stop()
			close(stopped)
		}()
		if err := stream.DeleteConsumer(context.Background(), consumerName); err != nil {
			t.Errorf("delete isolated consumer: %v", err)
		}
		select {
		case <-stopped:
		case <-time.After(5 * time.Second):
			t.Error("worker did not stop after isolated consumer deletion")
		}
		if err := auditWriter.Close(); err != nil {
			t.Errorf("close audit writer: %v", err)
		}
		nc.Close()
	})

	echoServer := echo.New()
	echoServer.Use(middleware.TraceMiddleware())
	echoServer.Use(middleware.AuthMiddleware(apiKeyRepo))
	messageHandler := &handler.MessageHandler{
		Ingestor: outbound.NewProcessor(nil, nil, connectionRepo, publisher),
	}
	messageHandler.RegisterRoutes(echoServer)

	traceID := uuid.New().String()
	requestBody := `{"to":"local-recipient","channel":"whatsapp_mock","body":"safe local end-to-end test"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/messages", strings.NewReader(requestBody))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	req.Header.Set("Authorization", "Bearer "+plaintextAPIKey)
	req.Header.Set("X-Trace-ID", traceID)
	rec := httptest.NewRecorder()

	echoServer.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("POST /api/v1/messages status = %d, want %d: %s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	if got := rec.Header().Get("X-Trace-ID"); got != traceID {
		t.Fatalf("response X-Trace-ID = %q, want %q", got, traceID)
	}

	waitForMockDispatch(t, ctx, dispatchRepo, workspace.ID, traceID)
	waitForMockAudit(t, ctx, pool, workspace.ID, traceID, expectedMockProviderID(traceID))
}

func waitForMockDispatch(
	t *testing.T,
	ctx context.Context,
	repo *repository.MessageDispatchRepository,
	workspaceID uuid.UUID,
	traceID string,
) {
	t.Helper()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()

	var lastStatus string
	for {
		dispatch, err := repo.GetByTraceID(ctx, workspaceID, traceID)
		if err == nil {
			lastStatus = fmt.Sprintf("%s/%s", dispatch.Status, dispatch.CurrentChannel)
			if dispatch.Status == "sent" && dispatch.CurrentChannel == "whatsapp_mock" {
				return
			}
		}

		select {
		case <-ctx.Done():
			t.Fatalf("dispatch %s did not reach sent/whatsapp_mock; last state %q: %v", traceID, lastStatus, ctx.Err())
		case <-ticker.C:
		}
	}
}

func waitForMockAudit(t *testing.T, ctx context.Context, pool *pgxpool.Pool, workspaceID uuid.UUID, traceID, expectedResponse string) {
	t.Helper()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()

	var lastPayload []byte
	for {
		var payload []byte
		err := pool.QueryRow(ctx,
			`SELECT payload
			 FROM audit_logs
			 WHERE workspace_id = $1 AND trace_id = $2 AND event_type = 'outbound_message'
			 ORDER BY created_at DESC
			 LIMIT 1`,
			workspaceID,
			traceID,
		).Scan(&payload)
		if err == nil {
			lastPayload = payload
			var event struct {
				Status   string `json:"status"`
				Response string `json:"response"`
			}
			if err := json.Unmarshal(payload, &event); err != nil {
				t.Fatalf("decode outbound audit payload: %v", err)
			}
			if event.Status == "sent" && event.Response == expectedResponse {
				return
			}
		}

		select {
		case <-ctx.Done():
			t.Fatalf("audit %s did not contain sent response %q; last payload %s: %v", traceID, expectedResponse, lastPayload, ctx.Err())
		case <-ticker.C:
		}
	}
}

func expectedMockProviderID(traceID string) string {
	sum := sha256.Sum256([]byte(traceID))
	return fmt.Sprintf("whatsapp-mock-%x", sum[:16])
}
