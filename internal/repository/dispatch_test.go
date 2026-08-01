package repository

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestMessageDispatchRepository(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	ctx := context.Background()
	wsRepo := NewWorkspaceRepository(pool)
	repo := NewMessageDispatchRepository(pool)

	// Create test workspace
	ws, err := wsRepo.Create(ctx, "dispatch_test_ws_"+uuid.New().String())
	if err != nil {
		t.Fatalf("failed to create test workspace: %v", err)
	}
	defer func() {
		_ = wsRepo.Delete(ctx, ws.ID)
	}()

	traceID := uuid.New().String()
	initialChannel := "whatsapp_web"

	// 1. GetOrCreateDispatch: creation case
	d, err := repo.GetOrCreateDispatch(ctx, ws.ID, traceID, initialChannel, nil, nil, nil)
	if err != nil {
		t.Fatalf("failed to get/create dispatch: %v", err)
	}

	if d.TraceID != traceID {
		t.Errorf("expected TraceID %s, got %s", traceID, d.TraceID)
	}
	if d.WorkspaceID != ws.ID {
		t.Errorf("expected WorkspaceID %s, got %s", ws.ID, d.WorkspaceID)
	}
	if d.CurrentChannel != initialChannel {
		t.Errorf("expected CurrentChannel %s, got %s", initialChannel, d.CurrentChannel)
	}
	if d.Status != "queued" {
		t.Errorf("expected Status 'queued', got %s", d.Status)
	}
	if d.FallbackIndex != 0 {
		t.Errorf("expected FallbackIndex 0, got %d", d.FallbackIndex)
	}
	if d.ErrorMessage != nil {
		t.Errorf("expected ErrorMessage nil, got %s", *d.ErrorMessage)
	}

	// 2. GetOrCreateDispatch: retrieve existing (idempotency) case
	d2, err := repo.GetOrCreateDispatch(ctx, ws.ID, traceID, "different_channel", nil, nil, nil)
	if err != nil {
		t.Fatalf("failed to get/create existing dispatch: %v", err)
	}

	if d2.ID != d.ID {
		t.Errorf("expected ID %s, got %s", d.ID, d2.ID)
	}
	// The channel should still be initialChannel since it was already created
	if d2.CurrentChannel != initialChannel {
		t.Errorf("expected CurrentChannel to remain %s, got %s", initialChannel, d2.CurrentChannel)
	}

	// 3. UpdateDispatchStatus
	errMsg := "some transient error"
	err = repo.UpdateDispatchStatus(ctx, d.ID, "failed_transient", "telegram", 1, &errMsg)
	if err != nil {
		t.Fatalf("failed to update status: %v", err)
	}

	// 4. GetByTraceID
	d3, err := repo.GetByTraceID(ctx, traceID)
	if err != nil {
		t.Fatalf("failed to get by trace id: %v", err)
	}

	if d3.Status != "failed_transient" {
		t.Errorf("expected Status 'failed_transient', got %s", d3.Status)
	}
	if d3.CurrentChannel != "telegram" {
		t.Errorf("expected CurrentChannel 'telegram', got %s", d3.CurrentChannel)
	}
	if d3.FallbackIndex != 1 {
		t.Errorf("expected FallbackIndex 1, got %d", d3.FallbackIndex)
	}
	if d3.ErrorMessage == nil || *d3.ErrorMessage != errMsg {
		t.Errorf("expected ErrorMessage %s, got %v", errMsg, d3.ErrorMessage)
	}

	// 5. GetByTraceID: non-existent case
	_, err = repo.GetByTraceID(ctx, "non-existent-trace-id")
	if !errors.Is(err, ErrDispatchNotFound) {
		t.Errorf("expected ErrDispatchNotFound, got %v", err)
	}
}

func TestMessageDispatchProviderMessageID(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	ctx := context.Background()
	wsRepo := NewWorkspaceRepository(pool)
	repo := NewMessageDispatchRepository(pool)

	ws, err := wsRepo.Create(ctx, "dispatch_provider_test_ws_"+uuid.New().String())
	if err != nil {
		t.Fatalf("failed to create test workspace: %v", err)
	}
	defer func() {
		_ = wsRepo.Delete(ctx, ws.ID)
	}()

	traceID := uuid.New().String()
	d, err := repo.GetOrCreateDispatch(ctx, ws.ID, traceID, "whatsapp_cloud", nil, nil, nil)
	if err != nil {
		t.Fatalf("failed to create dispatch: %v", err)
	}

	providerID := "wamid.test12345"
	err = repo.UpdateProviderMessageID(ctx, d.ID, providerID)
	if err != nil {
		t.Fatalf("failed to update provider message id: %v", err)
	}

	retrieved, err := repo.GetByProviderMessageID(ctx, ws.ID, providerID)
	if err != nil {
		t.Fatalf("failed to get by provider message id: %v", err)
	}

	if retrieved.ID != d.ID {
		t.Errorf("expected ID %s, got %s", d.ID, retrieved.ID)
	}
	if retrieved.ProviderMessageID == nil || *retrieved.ProviderMessageID != providerID {
		t.Errorf("expected ProviderMessageID %s, got %v", providerID, retrieved.ProviderMessageID)
	}

	otherWS, err := wsRepo.Create(ctx, "dispatch_provider_other_"+uuid.New().String())
	if err != nil {
		t.Fatalf("create other workspace: %v", err)
	}
	defer func() { _ = wsRepo.Delete(ctx, otherWS.ID) }()
	otherDispatch, err := repo.GetOrCreateDispatch(ctx, otherWS.ID, uuid.New().String(), "whatsapp_cloud", nil, nil, nil)
	if err != nil {
		t.Fatalf("create other dispatch: %v", err)
	}
	if err := repo.UpdateProviderMessageID(ctx, otherDispatch.ID, providerID); err != nil {
		t.Fatalf("same provider ID in another workspace must be allowed: %v", err)
	}
	otherRetrieved, err := repo.GetByProviderMessageID(ctx, otherWS.ID, providerID)
	if err != nil {
		t.Fatalf("get other workspace provider ID: %v", err)
	}
	if otherRetrieved.ID != otherDispatch.ID {
		t.Fatalf("cross-tenant lookup returned dispatch %s, want %s", otherRetrieved.ID, otherDispatch.ID)
	}

	_, err = repo.GetByProviderMessageID(ctx, ws.ID, "non-existent-provider-id")
	if !errors.Is(err, ErrDispatchNotFound) {
		t.Errorf("expected ErrDispatchNotFound, got %v", err)
	}
}

func TestMessageDispatchDeliveryClaimFencing(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	ctx := context.Background()
	wsRepo := NewWorkspaceRepository(pool)
	repo := NewMessageDispatchRepository(pool)

	ws, err := wsRepo.Create(ctx, "dispatch_claim_"+uuid.New().String())
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	defer func() { _ = wsRepo.Delete(ctx, ws.ID) }()

	t.Run("only one live owner and terminal completion", func(t *testing.T) {
		dispatch, err := repo.GetOrCreateDispatch(ctx, ws.ID, uuid.New().String(), "whatsapp_mock", nil, nil, nil)
		if err != nil {
			t.Fatalf("create dispatch: %v", err)
		}

		claim, retryAfter, err := repo.ClaimDelivery(ctx, dispatch.ID, time.Second)
		if err != nil {
			t.Fatalf("first claim: %v", err)
		}
		if claim.Token == uuid.Nil || claim.Generation != 1 || retryAfter != 0 {
			t.Fatalf("unexpected claim: %+v retry_after=%s", claim, retryAfter)
		}

		_, retryAfter, err = repo.ClaimDelivery(ctx, dispatch.ID, time.Second)
		if !errors.Is(err, ErrDispatchClaimActive) || retryAfter <= 0 {
			t.Fatalf("second claim error=%v retry_after=%s", err, retryAfter)
		}

		if err := repo.UpdateClaimedDelivery(
			ctx, dispatch.ID, claim, "sending", "whatsapp_mock", 0, nil, nil, false,
		); err != nil {
			t.Fatalf("mark sending: %v", err)
		}
		providerID := "provider-" + uuid.New().String()
		if err := repo.UpdateClaimedDelivery(
			ctx, dispatch.ID, claim, "sent", "whatsapp_mock", 0, nil, &providerID, true,
		); err != nil {
			t.Fatalf("mark sent: %v", err)
		}

		_, _, err = repo.ClaimDelivery(ctx, dispatch.ID, time.Second)
		if !errors.Is(err, ErrDispatchTerminal) {
			t.Fatalf("terminal claim error=%v, want ErrDispatchTerminal", err)
		}
	})

	t.Run("expired queued claim recovers and fences stale owner", func(t *testing.T) {
		dispatch, err := repo.GetOrCreateDispatch(ctx, ws.ID, uuid.New().String(), "whatsapp_mock", nil, nil, nil)
		if err != nil {
			t.Fatalf("create dispatch: %v", err)
		}
		oldClaim, _, err := repo.ClaimDelivery(ctx, dispatch.ID, 30*time.Millisecond)
		if err != nil {
			t.Fatalf("old claim: %v", err)
		}

		time.Sleep(60 * time.Millisecond)
		newClaim, _, err := repo.ClaimDelivery(ctx, dispatch.ID, time.Second)
		if err != nil {
			t.Fatalf("recover queued claim: %v", err)
		}
		if newClaim.Token == oldClaim.Token || newClaim.Generation != oldClaim.Generation+1 {
			t.Fatalf("recovered claim=%+v, old=%+v", newClaim, oldClaim)
		}
		if err := repo.UpdateClaimedDelivery(
			ctx, dispatch.ID, oldClaim, "sent", "whatsapp_mock", 0, nil, nil, true,
		); !errors.Is(err, ErrDispatchClaimLost) {
			t.Fatalf("stale completion error=%v, want ErrDispatchClaimLost", err)
		}
	})

	t.Run("expired sending claim becomes uncertain without reacquisition", func(t *testing.T) {
		dispatch, err := repo.GetOrCreateDispatch(ctx, ws.ID, uuid.New().String(), "whatsapp_mock", nil, nil, nil)
		if err != nil {
			t.Fatalf("create dispatch: %v", err)
		}
		claim, _, err := repo.ClaimDelivery(ctx, dispatch.ID, 30*time.Millisecond)
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		if err := repo.UpdateClaimedDelivery(
			ctx, dispatch.ID, claim, "sending", "whatsapp_mock", 0, nil, nil, false,
		); err != nil {
			t.Fatalf("mark sending: %v", err)
		}

		time.Sleep(60 * time.Millisecond)
		_, _, err = repo.ClaimDelivery(ctx, dispatch.ID, time.Second)
		if !errors.Is(err, ErrDispatchDeliveryUncertain) {
			t.Fatalf("expired sending error=%v, want ErrDispatchDeliveryUncertain", err)
		}
		stored, err := repo.GetByTraceID(ctx, dispatch.TraceID)
		if err != nil {
			t.Fatalf("get uncertain dispatch: %v", err)
		}
		if stored.Status != "uncertain" {
			t.Fatalf("status=%q, want uncertain", stored.Status)
		}
		if stored.ErrorMessage == nil || *stored.ErrorMessage != "DELIVERY_UNCERTAIN" {
			t.Fatalf("error_message=%v, want DELIVERY_UNCERTAIN", stored.ErrorMessage)
		}
	})
}
