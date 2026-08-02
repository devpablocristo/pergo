package repository

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"
)

func TestAdminSessionLifecycleIsDurablyRevocable(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	repository := NewAdminSessionRepository(pool)
	digest := sha256.Sum256([]byte(t.Name() + time.Now().UTC().String()))
	sessionID := hex.EncodeToString(digest[:])
	now := time.Now().UTC()
	expiresAt := now.Add(time.Hour)
	t.Cleanup(func() {
		_, _ = pool.Exec(t.Context(), `DELETE FROM admin_sessions WHERE session_id = $1`, sessionID)
	})

	if err := repository.CreateAdminSession(t.Context(), sessionID, expiresAt); err != nil {
		t.Fatalf("CreateAdminSession: %v", err)
	}
	active, err := repository.IsAdminSessionActive(t.Context(), sessionID, now)
	if err != nil || !active {
		t.Fatalf("IsAdminSessionActive before revoke = %v, %v", active, err)
	}

	if err := repository.RevokeAdminSession(t.Context(), sessionID, now.Add(time.Minute)); err != nil {
		t.Fatalf("RevokeAdminSession: %v", err)
	}
	active, err = repository.IsAdminSessionActive(t.Context(), sessionID, now.Add(2*time.Minute))
	if err != nil {
		t.Fatalf("IsAdminSessionActive after revoke: %v", err)
	}
	if active {
		t.Fatal("revoked admin session remained active")
	}
}

func TestAdminSessionExpiryIsEnforcedByDatabaseLookup(t *testing.T) {
	pool := getTestPool(t)
	defer pool.Close()

	repository := NewAdminSessionRepository(pool)
	digest := sha256.Sum256([]byte(t.Name() + time.Now().UTC().String()))
	sessionID := hex.EncodeToString(digest[:])
	now := time.Now().UTC()
	t.Cleanup(func() {
		_, _ = pool.Exec(t.Context(), `DELETE FROM admin_sessions WHERE session_id = $1`, sessionID)
	})

	if err := repository.CreateAdminSession(t.Context(), sessionID, now.Add(time.Minute)); err != nil {
		t.Fatalf("CreateAdminSession: %v", err)
	}
	active, err := repository.IsAdminSessionActive(t.Context(), sessionID, now.Add(2*time.Minute))
	if err != nil {
		t.Fatalf("IsAdminSessionActive: %v", err)
	}
	if active {
		t.Fatal("expired admin session remained active")
	}
}
