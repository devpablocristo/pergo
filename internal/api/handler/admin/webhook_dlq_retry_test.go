package admin_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/pablojhp.pergo/internal/api/handler/admin"
	"github.com/pablojhp.pergo/internal/platform/crypto"
	"github.com/pablojhp.pergo/internal/repository"
	"github.com/pablojhp.pergo/internal/webhook"
)

type recordingWebhookRetryPublisher struct {
	err      error
	subjects []string
	payloads [][]byte
	dedupIDs []string
}

func (p *recordingWebhookRetryPublisher) Publish(
	_ context.Context,
	subject string,
	data []byte,
	dedupID string,
) error {
	p.subjects = append(p.subjects, subject)
	p.payloads = append(p.payloads, append([]byte(nil), data...))
	p.dedupIDs = append(p.dedupIDs, dedupID)
	return p.err
}

func TestWebhookDLQRetryIsStableAndDeletesOnlyAfterBrokerAcceptance(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()
	ctx := context.Background()

	encryptor, err := crypto.NewEncryptor(make([]byte, 32))
	if err != nil {
		t.Fatalf("encryptor: %v", err)
	}
	workspaceRepo := repository.NewWorkspaceRepository(pool)
	workspace, err := workspaceRepo.Create(ctx, "webhook_retry_"+uuid.NewString())
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	defer func() { _ = workspaceRepo.Delete(context.Background(), workspace.ID) }()

	subscriptionRepo := repository.NewWebhookSubscriptionRepository(pool, encryptor)
	subscription, err := subscriptionRepo.Create(
		ctx,
		workspace.ID,
		"https://hooks.example.com/pergo",
		[]string{"booking.confirmed"},
		[]byte("0123456789abcdef0123456789abcdef"),
	)
	if err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	dlqRepo := repository.NewWebhookDLQRepository(pool, encryptor)
	traceID := uuid.NewString()
	if err := dlqRepo.InsertDLQ(
		ctx,
		workspace.ID,
		subscription.ID,
		traceID,
		"message-1",
		"booking.confirmed",
		[]byte(`{"event":"booking.confirmed"}`),
		subscription.URL,
		5,
		nil,
	); err != nil {
		t.Fatalf("insert DLQ: %v", err)
	}
	items, err := dlqRepo.ListDLQ(ctx, workspace.ID, 10, 0)
	if err != nil || len(items) != 1 {
		t.Fatalf("list DLQ items=%d error=%v", len(items), err)
	}
	dlqID := items[0].ID

	publisher := &recordingWebhookRetryPublisher{err: errors.New("broker unavailable")}
	handler := &admin.WebhookDLQHandler{
		Repo:      dlqRepo,
		Publisher: publisher,
	}
	invoke := func() int {
		e := echo.New()
		req := httptest.NewRequest(http.MethodPost, "/admin/webhook-dlq/"+dlqID.String()+"/retry", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		c.SetPathValues(echo.PathValues{{Name: "dlq_id", Value: dlqID.String()}})
		if err := handler.RetryDLQ(c); err != nil {
			t.Fatalf("retry handler: %v", err)
		}
		return rec.Code
	}

	if status := invoke(); status != http.StatusInternalServerError {
		t.Fatalf("failed publish status=%d", status)
	}
	if _, err := dlqRepo.GetDLQByID(ctx, dlqID); err != nil {
		t.Fatalf("DLQ deleted before broker acceptance: %v", err)
	}

	publisher.err = nil
	if status := invoke(); status != http.StatusOK {
		t.Fatalf("accepted publish status=%d", status)
	}
	if _, err := dlqRepo.GetDLQByID(ctx, dlqID); !errors.Is(err, repository.ErrWebhookDLQNotFound) {
		t.Fatalf("DLQ remained after accepted publish: %v", err)
	}
	if len(publisher.dedupIDs) != 2 || publisher.dedupIDs[0] != publisher.dedupIDs[1] {
		t.Fatalf("retry identities changed: %v", publisher.dedupIDs)
	}
	if want := "webhooks.deliveries." + workspace.ID.String(); publisher.subjects[0] != want || publisher.subjects[1] != want {
		t.Fatalf("subjects=%v, want workspace-only %q", publisher.subjects, want)
	}

	var task webhook.WebhookDeliveryTask
	if err := json.Unmarshal(publisher.payloads[0], &task); err != nil {
		t.Fatalf("decode retry task: %v", err)
	}
	expectedID := uuid.NewSHA1(
		uuid.NameSpaceURL,
		[]byte("pergo:webhook-dlq-retry:v1:"+dlqID.String()),
	)
	if task.ID != expectedID || publisher.dedupIDs[0] != expectedID.String() {
		t.Fatalf("task ID=%s dedup=%q, want %s", task.ID, publisher.dedupIDs[0], expectedID)
	}
}
