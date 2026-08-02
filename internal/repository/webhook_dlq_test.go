package repository

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/pablojhp.pergo/internal/platform/crypto"
)

func TestWebhookDLQRepository(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	ctx := context.Background()

	// 1. Setup Encryptor
	kek := make([]byte, 32)
	for i := range kek {
		kek[i] = byte(i)
	}
	enc, err := crypto.NewEncryptor(kek)
	if err != nil {
		t.Fatalf("failed to create encryptor: %v", err)
	}

	repo := NewWebhookDLQRepository(pool, enc)
	subRepo := NewWebhookSubscriptionRepository(pool, enc)

	// 2. Create two test workspaces (for isolation checks)
	wsRepo := NewWorkspaceRepository(pool)
	wsName1 := "webhook_test_ws_1_" + uuid.New().String()
	ws1, err := wsRepo.Create(ctx, wsName1)
	if err != nil {
		t.Fatalf("failed to create test workspace 1: %v", err)
	}
	defer func() { _ = wsRepo.Delete(ctx, ws1.ID) }()

	wsName2 := "webhook_test_ws_2_" + uuid.New().String()
	ws2, err := wsRepo.Create(ctx, wsName2)
	if err != nil {
		t.Fatalf("failed to create test workspace 2: %v", err)
	}
	defer func() { _ = wsRepo.Delete(ctx, ws2.ID) }()

	// --- 3. Create Webhook Subscription for WS1 ---
	testURL := "https://example.com/webhooks"
	testSecret := []byte("very-secure-webhook-secret-token")

	sub, err := subRepo.Create(ctx, ws1.ID, testURL, []string{"*"}, testSecret)
	if err != nil {
		t.Fatalf("failed to create webhook subscription: %v", err)
	}

	// --- 4. Test DLQ Operations ---
	traceID := "trace-123-abc"
	messageID := "msg-456-def"
	eventType := "failed"
	payload := []byte(`{"status":"failed","reason":"timeout"}`)
	attempts := 10
	failReason := "Gateway Timeout 504"

	// Insert into DLQ for WS1
	err = repo.InsertDLQ(ctx, ws1.ID, sub.ID, traceID, messageID, eventType, payload, testURL, attempts, &failReason)
	if err != nil {
		t.Fatalf("failed to insert into DLQ: %v", err)
	}
	var (
		rawPayload []byte
		rawURL     string
		rawReason  *string
		ciphertext []byte
	)
	if err := pool.QueryRow(ctx, `
		SELECT payload, webhook_url, failure_reason, encrypted_data
		FROM webhook_dlqs
		WHERE workspace_id = $1
	`, ws1.ID).Scan(&rawPayload, &rawURL, &rawReason, &ciphertext); err != nil {
		t.Fatalf("read raw DLQ row: %v", err)
	}
	if bytes.Contains(rawPayload, []byte("timeout")) ||
		bytes.Contains([]byte(rawURL), []byte("example.com")) ||
		(rawReason != nil && bytes.Contains([]byte(*rawReason), []byte("Gateway"))) ||
		len(ciphertext) == 0 {
		t.Fatalf(
			"DLQ plaintext at rest payload=%q url=%q reason=%v ciphertext=%d",
			rawPayload,
			rawURL,
			rawReason,
			len(ciphertext),
		)
	}

	// Check Badge Count
	count, err := repo.GetDLQBadgeCount(ctx, ws1.ID)
	if err != nil {
		t.Fatalf("failed to get DLQ badge count: %v", err)
	}
	if count != 1 {
		t.Errorf("expected DLQ count 1, got %d", count)
	}

	// WS2 badge count should be 0
	count2, err := repo.GetDLQBadgeCount(ctx, ws2.ID)
	if err != nil {
		t.Fatalf("failed to get WS2 DLQ badge count: %v", err)
	}
	if count2 != 0 {
		t.Errorf("expected WS2 DLQ count 0, got %d", count2)
	}

	// List DLQ items
	items, err := repo.ListDLQ(ctx, ws1.ID, 10, 0)
	if err != nil {
		t.Fatalf("failed to list DLQ items: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 DLQ item, got %d", len(items))
	}

	item := items[0]
	if item.TraceID != traceID || item.MessageID != messageID || item.EventType != eventType || item.SubscriptionID != sub.ID {
		t.Errorf("DLQ fields mismatch: %+v", item)
	}
	var expectedMap, actualMap map[string]interface{}
	if err := json.Unmarshal(payload, &expectedMap); err != nil {
		t.Fatalf("failed to unmarshal expected payload: %v", err)
	}
	if err := json.Unmarshal(item.Payload, &actualMap); err != nil {
		t.Fatalf("failed to unmarshal actual payload: %v", err)
	}
	for k, v := range expectedMap {
		if actualMap[k] != v {
			t.Errorf("expected payload key %s to be %v, got %v", k, v, actualMap[k])
		}
	}
	if item.FailureReason == nil || *item.FailureReason != failReason {
		t.Errorf("expected failure reason %s, got %v", failReason, item.FailureReason)
	}

	// Retrieve by ID
	fetchedItem, err := repo.GetDLQByID(ctx, item.ID)
	if err != nil {
		t.Fatalf("failed to get DLQ by ID: %v", err)
	}
	if fetchedItem.ID != item.ID {
		t.Errorf("expected item ID %s, got %s", item.ID, fetchedItem.ID)
	}

	// Delete DLQ item
	err = repo.DeleteDLQ(ctx, item.ID)
	if err != nil {
		t.Fatalf("failed to delete DLQ item: %v", err)
	}

	// Badge count should be 0 now
	count, err = repo.GetDLQBadgeCount(ctx, ws1.ID)
	if err != nil {
		t.Fatalf("failed to get DLQ count after delete: %v", err)
	}
	if count != 0 {
		t.Errorf("expected DLQ count 0 after delete, got %d", count)
	}
}

func TestWebhookDLQBackfillEncryptsAndScrubsLegacyRows(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	ctx := context.Background()
	enc, err := crypto.NewEncryptor(make([]byte, 32))
	if err != nil {
		t.Fatalf("encryptor: %v", err)
	}
	wsRepo := NewWorkspaceRepository(pool)
	ws, err := wsRepo.Create(ctx, "dlq_backfill_"+uuid.NewString())
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	defer func() { _ = wsRepo.Delete(ctx, ws.ID) }()
	subRepo := NewWebhookSubscriptionRepository(pool, enc)
	sub, err := subRepo.Create(
		ctx,
		ws.ID,
		"https://hooks.example.com/pergo",
		[]string{"*"},
		[]byte("0123456789abcdef0123456789abcdef"),
	)
	if err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	reason := "legacy-secret-reason"
	var id uuid.UUID
	if err := pool.QueryRow(ctx, `
		INSERT INTO webhook_dlqs (
			workspace_id, subscription_id, trace_id, message_id, event_type,
			payload, webhook_url, attempts, failure_reason, last_attempt_at
		)
		VALUES (
			$1, $2, 'legacy-trace', 'legacy-message', 'failed',
			'{"secret":"legacy-marker"}', 'https://legacy.example/hook',
			1, $3, now()
		)
		RETURNING id
	`, ws.ID, sub.ID, reason).Scan(&id); err != nil {
		t.Fatalf("insert legacy row: %v", err)
	}

	repo := NewWebhookDLQRepository(pool, enc)
	if err := repo.BackfillLegacyEncryption(ctx); err != nil {
		t.Fatalf("backfill: %v", err)
	}
	item, err := repo.GetDLQByID(ctx, id)
	if err != nil {
		t.Fatalf("read backfilled row: %v", err)
	}
	if !bytes.Contains(item.Payload, []byte("legacy-marker")) ||
		item.WebhookURL != "https://legacy.example/hook" ||
		item.FailureReason == nil ||
		*item.FailureReason != reason {
		t.Fatalf("backfilled logical data changed: %+v", item)
	}
	var rawText string
	if err := pool.QueryRow(ctx, `
		SELECT payload::text || webhook_url || COALESCE(failure_reason, '')
		FROM webhook_dlqs
		WHERE id = $1
	`, id).Scan(&rawText); err != nil {
		t.Fatalf("read scrubbed row: %v", err)
	}
	if strings.Contains(rawText, "legacy-marker") ||
		strings.Contains(rawText, "legacy.example") ||
		strings.Contains(rawText, reason) {
		t.Fatalf("legacy plaintext remains: %q", rawText)
	}
}
