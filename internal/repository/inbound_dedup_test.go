package repository

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestInboundDeduplicate(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	repo := NewInboundDedupRepository(pool)
	ctx := context.Background()

	// Setup clean workspace
	wsRepo := NewWorkspaceRepository(pool)
	ws, err := wsRepo.Create(ctx, "dedup_test_ws_"+uuid.New().String())
	if err != nil {
		t.Fatalf("failed to create workspace: %v", err)
	}
	defer func() {
		_ = wsRepo.Delete(ctx, ws.ID)
	}()
	connectionID := uuid.New()

	t.Run("Insert and check uniqueness", func(t *testing.T) {
		providerMsgID := "msg_unique_123"
		channelName := "whatsapp"

		// First attempt should return true (unique)
		inserted, err := repo.InsertAndCheck(ctx, ws.ID, connectionID, channelName, providerMsgID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !inserted {
			t.Fatal("expected message to be unique")
		}

		// Second attempt should return false (duplicate)
		inserted, err = repo.InsertAndCheck(ctx, ws.ID, connectionID, channelName, providerMsgID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if inserted {
			t.Fatal("expected message to be classified as duplicate")
		}
	})

	t.Run("Concurrent insertions", func(t *testing.T) {
		providerMsgID := "msg_concurrent_999"
		channelName := "telegram"

		const goroutines = 10
		var wg sync.WaitGroup
		results := make(chan bool, goroutines)
		errorsChan := make(chan error, goroutines)

		for i := 0; i < goroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				inserted, err := repo.InsertAndCheck(ctx, ws.ID, connectionID, channelName, providerMsgID)
				if err != nil {
					errorsChan <- err
					return
				}
				results <- inserted
			}()
		}

		wg.Wait()
		close(results)
		close(errorsChan)

		for err := range errorsChan {
			t.Fatalf("unexpected concurrent error: %v", err)
		}

		insertedCount := 0
		for res := range results {
			if res {
				insertedCount++
			}
		}

		if insertedCount != 1 {
			t.Errorf("expected exactly 1 insertion to succeed, but got %d", insertedCount)
		}
	})
}

func TestInboundDeliveryClaimRecoveryAndStableTrace(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	ctx := context.Background()
	wsRepo := NewWorkspaceRepository(pool)
	ws, err := wsRepo.Create(ctx, "inbound_claim_"+uuid.NewString())
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	defer func() { _ = wsRepo.Delete(ctx, ws.ID) }()

	repo := NewInboundDedupRepository(pool)
	connectionID := uuid.New()
	channel := "whatsapp_cloud"
	messageID := "wamid." + uuid.NewString()

	first, replay, retryAfter, err := repo.Claim(ctx, ws.ID, connectionID, channel, messageID, time.Second)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if replay || retryAfter != 0 || first.TraceID == "" || first.Token == uuid.Nil || first.Generation != 1 {
		t.Fatalf("unexpected first claim=%+v replay=%v retry=%s", first, replay, retryAfter)
	}

	_, replay, retryAfter, err = repo.Claim(ctx, ws.ID, connectionID, channel, messageID, time.Second)
	if !errors.Is(err, ErrInboundClaimActive) || replay || retryAfter <= 0 {
		t.Fatalf("live duplicate error=%v replay=%v retry=%s", err, replay, retryAfter)
	}

	if err := repo.Release(ctx, ws.ID, connectionID, channel, messageID, first); err != nil {
		t.Fatalf("release first claim: %v", err)
	}
	recovered, replay, _, err := repo.Claim(ctx, ws.ID, connectionID, channel, messageID, time.Second)
	if err != nil {
		t.Fatalf("recover claim: %v", err)
	}
	if replay || recovered.TraceID != first.TraceID ||
		recovered.Generation != first.Generation+1 ||
		recovered.Token == first.Token {
		t.Fatalf("unexpected recovered claim=%+v first=%+v replay=%v", recovered, first, replay)
	}
	if err := repo.MarkPublished(ctx, ws.ID, connectionID, channel, messageID, first); !errors.Is(err, ErrInboundClaimLost) {
		t.Fatalf("stale completion error=%v, want ErrInboundClaimLost", err)
	}
	if err := repo.MarkPublished(ctx, ws.ID, connectionID, channel, messageID, recovered); err != nil {
		t.Fatalf("mark recovered published: %v", err)
	}

	published, replay, retryAfter, err := repo.Claim(ctx, ws.ID, connectionID, channel, messageID, time.Second)
	if err != nil {
		t.Fatalf("published replay: %v", err)
	}
	if !replay || retryAfter != 0 || published.TraceID != first.TraceID || published.Token != uuid.Nil {
		t.Fatalf("unexpected published replay=%+v replay=%v retry=%s", published, replay, retryAfter)
	}
}

func TestInboundDedupScopesProviderIdentityByConnection(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	ctx := context.Background()
	wsRepo := NewWorkspaceRepository(pool)
	ws, err := wsRepo.Create(ctx, "inbound_connections_"+uuid.NewString())
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	defer func() { _ = wsRepo.Delete(ctx, ws.ID) }()

	repo := NewInboundDedupRepository(pool)
	channel := "telegram"
	updateID := "42"
	firstConnection := uuid.New()
	secondConnection := uuid.New()

	first, replay, _, err := repo.Claim(ctx, ws.ID, firstConnection, channel, updateID, time.Second)
	if err != nil || replay {
		t.Fatalf("first bot claim=%+v replay=%v error=%v", first, replay, err)
	}
	if err := repo.MarkPublished(ctx, ws.ID, firstConnection, channel, updateID, first); err != nil {
		t.Fatalf("publish first bot: %v", err)
	}

	second, replay, _, err := repo.Claim(ctx, ws.ID, secondConnection, channel, updateID, time.Second)
	if err != nil || replay {
		t.Fatalf("second bot claim=%+v replay=%v error=%v", second, replay, err)
	}
	if second.TraceID == first.TraceID {
		t.Fatal("different provider connections shared the same dedup trace")
	}
	if err := repo.MarkPublished(ctx, ws.ID, secondConnection, channel, updateID, second); err != nil {
		t.Fatalf("publish second bot: %v", err)
	}

	replayed, replay, _, err := repo.Claim(ctx, ws.ID, firstConnection, channel, updateID, time.Second)
	if err != nil || !replay || replayed.TraceID != first.TraceID {
		t.Fatalf("first bot retry=%+v replay=%v error=%v", replayed, replay, err)
	}
}
