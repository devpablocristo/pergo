package repository_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	pergocrypto "github.com/pablojhp.pergo/internal/platform/crypto"
	"github.com/pablojhp.pergo/internal/repository"
)

func TestAPIKeyRepository_CountActive(t *testing.T) {
	pool := getTestPoolWithMigrations(t)
	defer pool.Close()

	ctx := context.Background()

	// Clean up api_keys and workspaces
	_, _ = pool.Exec(ctx, "DELETE FROM api_keys")
	_, _ = pool.Exec(ctx, "DELETE FROM workspaces")

	repo := repository.NewAPIKeyRepository(pool)
	wsRepo := repository.NewWorkspaceRepository(pool)

	// Create test workspace
	ws, err := wsRepo.Create(ctx, "apikey_test_ws_"+uuid.New().String())
	if err != nil {
		t.Fatalf("failed to create test workspace: %v", err)
	}
	defer func() {
		_ = wsRepo.Delete(ctx, ws.ID)
	}()

	// 1. Initially active keys count should be 0
	count, err := repo.CountActive(ctx, ws.ID)
	if err != nil {
		t.Fatalf("failed to count active keys: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 active keys, got %d", count)
	}

	// 2. Create active key
	key1, _, err := repo.Create(ctx, ws.ID, "Key 1")
	if err != nil {
		t.Fatalf("failed to create API key: %v", err)
	}

	count, err = repo.CountActive(ctx, ws.ID)
	if err != nil {
		t.Fatalf("failed to count active keys: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 active key, got %d", count)
	}

	// 3. Create another active key
	key2, _, err := repo.Create(ctx, ws.ID, "Key 2")
	if err != nil {
		t.Fatalf("failed to create second API key: %v", err)
	}

	count, err = repo.CountActive(ctx, ws.ID)
	if err != nil {
		t.Fatalf("failed to count active keys: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 active keys, got %d", count)
	}

	// 4. Revoke one key and check count
	err = repo.Revoke(ctx, key1.ID)
	if err != nil {
		t.Fatalf("failed to revoke API key: %v", err)
	}

	count, err = repo.CountActive(ctx, ws.ID)
	if err != nil {
		t.Fatalf("failed to count active keys: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 active key after revoking one, got %d", count)
	}

	// 5. Revoke the second key
	err = repo.Revoke(ctx, key2.ID)
	if err != nil {
		t.Fatalf("failed to revoke second API key: %v", err)
	}

	count, err = repo.CountActive(ctx, ws.ID)
	if err != nil {
		t.Fatalf("failed to count active keys: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 active keys after revoking all, got %d", count)
	}
}

func TestAPIKeyLegacyPrefixCollisionReturnsAllCandidates(t *testing.T) {
	pool := getTestPoolWithMigrations(t)
	defer pool.Close()

	ctx := context.Background()
	wsRepo := repository.NewWorkspaceRepository(pool)
	ws, err := wsRepo.Create(ctx, "apikey_collision_"+uuid.NewString())
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	defer func() { _ = wsRepo.Delete(ctx, ws.ID) }()

	firstPlaintext := "deadbeef-first-legacy-key"
	secondPlaintext := "deadbeef-second-legacy-key"
	firstHash, _ := pergocrypto.HashAPIKey(firstPlaintext)
	secondHash, _ := pergocrypto.HashAPIKey(secondPlaintext)
	for i, hash := range [][]byte{firstHash, secondHash} {
		if _, err := pool.Exec(ctx, `
			INSERT INTO api_keys (
				workspace_id, key_prefix, key_hash, name, key_id, key_version
			)
			VALUES ($1, 'deadbeef', $2, $3, $4, 1)
		`, ws.ID, hash, "legacy collision", uuid.NewString()); err != nil {
			t.Fatalf("insert legacy key %d: %v", i, err)
		}
	}

	repo := repository.NewAPIKeyRepository(pool)
	candidates, err := repo.FindActiveCandidates(ctx, secondPlaintext)
	if err != nil {
		t.Fatalf("find candidates: %v", err)
	}
	if len(candidates) != 2 {
		t.Fatalf("candidates=%d, want 2", len(candidates))
	}
	matches := 0
	for _, candidate := range candidates {
		if pergocrypto.VerifyAPIKey(secondPlaintext, candidate.KeyHash) {
			matches++
		}
	}
	if matches != 1 {
		t.Fatalf("full hash matches=%d, want 1", matches)
	}
}

func TestAPIKeyRevocationIsVisibleAcrossRepositoryInstances(t *testing.T) {
	pool := getTestPoolWithMigrations(t)
	defer pool.Close()

	ctx := context.Background()
	wsRepo := repository.NewWorkspaceRepository(pool)
	ws, err := wsRepo.Create(ctx, "apikey_revoke_replicas_"+uuid.NewString())
	if err != nil {
		t.Fatalf("create workspace: %v", err)
	}
	defer func() { _ = wsRepo.Delete(ctx, ws.ID) }()

	firstReplica := repository.NewAPIKeyRepository(pool)
	secondReplica := repository.NewAPIKeyRepository(pool)
	apiKey, plaintext, err := firstReplica.Create(ctx, ws.ID, "replicated-key")
	if err != nil {
		t.Fatalf("create key: %v", err)
	}
	if candidates, err := secondReplica.FindActiveCandidates(ctx, plaintext); err != nil || len(candidates) != 1 {
		t.Fatalf("warm second replica: %v", err)
	}
	if err := firstReplica.Revoke(ctx, apiKey.ID); err != nil {
		t.Fatalf("revoke first replica: %v", err)
	}
	if candidates, err := secondReplica.FindActiveCandidates(ctx, plaintext); err != nil || len(candidates) != 0 {
		t.Fatalf("second replica accepted revoked key: candidates=%d err=%v", len(candidates), err)
	}
}

func TestAPIKeyMutationIsWorkspaceScoped(t *testing.T) {
	pool := getTestPoolWithMigrations(t)
	defer pool.Close()

	ctx := context.Background()
	wsRepo := repository.NewWorkspaceRepository(pool)
	owner, err := wsRepo.Create(ctx, "apikey_owner_"+uuid.NewString())
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	defer func() { _ = wsRepo.Delete(ctx, owner.ID) }()
	attacker, err := wsRepo.Create(ctx, "apikey_attacker_"+uuid.NewString())
	if err != nil {
		t.Fatalf("create attacker: %v", err)
	}
	defer func() { _ = wsRepo.Delete(ctx, attacker.ID) }()

	repo := repository.NewAPIKeyRepository(pool)
	key, _, err := repo.Create(ctx, owner.ID, "owner-key")
	if err != nil {
		t.Fatalf("create key: %v", err)
	}
	if _, err := repo.GetByIDForWorkspace(ctx, attacker.ID, key.ID); !errors.Is(err, repository.ErrAPIKeyNotFound) {
		t.Fatalf("cross-workspace read error=%v", err)
	}
	if err := repo.RevokeForWorkspace(ctx, attacker.ID, key.ID); !errors.Is(err, repository.ErrAPIKeyNotFound) {
		t.Fatalf("cross-workspace revoke error=%v", err)
	}
	stored, err := repo.GetByIDForWorkspace(ctx, owner.ID, key.ID)
	if err != nil {
		t.Fatalf("owner read: %v", err)
	}
	if stored.RevokedAt != nil {
		t.Fatal("cross-workspace revoke changed the owner key")
	}
}
