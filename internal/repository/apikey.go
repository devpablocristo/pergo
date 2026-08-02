package repository

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pablojhp.pergo/internal/platform/crypto"
)

var ErrAPIKeyNotFound = errors.New("API key not found")

// APIKey represents an API key entity.
type APIKey struct {
	ID          uuid.UUID
	WorkspaceID uuid.UUID
	KeyPrefix   string
	KeyHash     []byte
	Name        string
	RevokedAt   *time.Time
	KeyID       string
	KeyVersion  int
	CreatedAt   time.Time
}

// APIKeyRepository provides CRUD operations for API keys. Authentication reads
// are deliberately uncached so revocation is immediately visible to every API
// replica.
type APIKeyRepository struct {
	pool *pgxpool.Pool
}

// NewAPIKeyRepository creates a new APIKeyRepository.
func NewAPIKeyRepository(pool *pgxpool.Pool) *APIKeyRepository {
	return &APIKeyRepository{pool: pool}
}

// Create generates a new API key, stores the hash and prefix, and returns the API key and plaintext key.
func (r *APIKeyRepository) Create(ctx context.Context, workspaceID uuid.UUID, name string) (*APIKey, string, error) {
	// Generate random 32-byte key, hex-encoded for safe UTF-8 storage of the prefix
	keyBytes := make([]byte, 32)
	if _, err := rand.Read(keyBytes); err != nil {
		return nil, "", err
	}
	plaintext := hex.EncodeToString(keyBytes)

	hash, prefix := crypto.HashAPIKey(plaintext)

	keyID := uuid.New().String()

	var apiKey APIKey
	err := r.pool.QueryRow(ctx,
		`INSERT INTO api_keys (workspace_id, key_prefix, key_hash, name, key_id, key_version)
		 VALUES ($1, $2, $3, $4, $5, 1)
		 RETURNING id, workspace_id, key_prefix, key_hash, name, revoked_at, key_id, key_version, created_at`,
		workspaceID, prefix, hash, name, keyID,
	).Scan(&apiKey.ID, &apiKey.WorkspaceID, &apiKey.KeyPrefix, &apiKey.KeyHash,
		&apiKey.Name, &apiKey.RevokedAt, &apiKey.KeyID, &apiKey.KeyVersion, &apiKey.CreatedAt)
	if err != nil {
		return nil, "", err
	}

	return &apiKey, plaintext, nil
}

// GetByPrefix looks up a currently active API key by prefix.
func (r *APIKeyRepository) GetByPrefix(ctx context.Context, prefix string) (*APIKey, error) {
	var apiKey APIKey
	err := r.pool.QueryRow(ctx,
		`SELECT id, workspace_id, key_prefix, key_hash, name, revoked_at, key_id, key_version, created_at
		 FROM api_keys WHERE key_prefix = $1 AND revoked_at IS NULL`,
		prefix,
	).Scan(&apiKey.ID, &apiKey.WorkspaceID, &apiKey.KeyPrefix, &apiKey.KeyHash,
		&apiKey.Name, &apiKey.RevokedAt, &apiKey.KeyID, &apiKey.KeyVersion, &apiKey.CreatedAt)
	if err != nil {
		return nil, err
	}
	return &apiKey, nil
}

// FindActiveCandidates returns every active key whose stored legacy or current
// prefix can match the supplied plaintext. The caller must constant-time verify
// the complete hash; returning all candidates keeps legacy 32-bit prefix
// collisions from disabling a valid credential.
func (r *APIKeyRepository) FindActiveCandidates(
	ctx context.Context,
	plaintext string,
) ([]APIKey, error) {
	if len(plaintext) < 8 {
		return nil, nil
	}
	prefixes := []string{plaintext[:8]}
	if len(plaintext) >= crypto.APIKeyPrefixLength {
		current := plaintext[:crypto.APIKeyPrefixLength]
		if current != prefixes[0] {
			prefixes = append(prefixes, current)
		}
	}

	rows, err := r.pool.Query(ctx, `
		SELECT id, workspace_id, key_prefix, key_hash, name, revoked_at, key_id, key_version, created_at
		FROM api_keys
		WHERE key_prefix = ANY($1)
		  AND revoked_at IS NULL
	`, prefixes)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var candidates []APIKey
	for rows.Next() {
		var apiKey APIKey
		if err := rows.Scan(
			&apiKey.ID,
			&apiKey.WorkspaceID,
			&apiKey.KeyPrefix,
			&apiKey.KeyHash,
			&apiKey.Name,
			&apiKey.RevokedAt,
			&apiKey.KeyID,
			&apiKey.KeyVersion,
			&apiKey.CreatedAt,
		); err != nil {
			return nil, err
		}
		candidates = append(candidates, apiKey)
	}
	return candidates, rows.Err()
}

// Revoke marks an API key as revoked.
func (r *APIKeyRepository) Revoke(ctx context.Context, id uuid.UUID) error {
	return r.RevokeForWorkspace(ctx, uuid.Nil, id)
}

func (r *APIKeyRepository) RevokeForWorkspace(ctx context.Context, workspaceID, id uuid.UUID) error {
	tag, err := r.pool.Exec(ctx,
		`UPDATE api_keys
		 SET revoked_at = now()
		 WHERE id = $1
		   AND ($2::uuid IS NULL OR workspace_id = $2)`,
		id, nullableUUID(workspaceID),
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrAPIKeyNotFound
	}
	return nil
}

// ListByWorkspace returns all API keys for a workspace (including revoked), ordered by created_at descending.
func (r *APIKeyRepository) ListByWorkspace(ctx context.Context, workspaceID uuid.UUID) ([]APIKey, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, workspace_id, key_prefix, key_hash, name, revoked_at, key_id, key_version, created_at
		 FROM api_keys WHERE workspace_id = $1 ORDER BY created_at DESC`,
		workspaceID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var keys []APIKey
	for rows.Next() {
		var k APIKey
		if err := rows.Scan(&k.ID, &k.WorkspaceID, &k.KeyPrefix, &k.KeyHash,
			&k.Name, &k.RevokedAt, &k.KeyID, &k.KeyVersion, &k.CreatedAt); err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	return keys, rows.Err()
}

// GetByID retrieves an API key by ID.
func (r *APIKeyRepository) GetByID(ctx context.Context, id uuid.UUID) (*APIKey, error) {
	return r.GetByIDForWorkspace(ctx, uuid.Nil, id)
}

func (r *APIKeyRepository) GetByIDForWorkspace(ctx context.Context, workspaceID, id uuid.UUID) (*APIKey, error) {
	var apiKey APIKey
	err := r.pool.QueryRow(ctx,
		`SELECT id, workspace_id, key_prefix, key_hash, name, revoked_at, key_id, key_version, created_at
		 FROM api_keys
		 WHERE id = $1
		   AND ($2::uuid IS NULL OR workspace_id = $2)`,
		id, nullableUUID(workspaceID),
	).Scan(&apiKey.ID, &apiKey.WorkspaceID, &apiKey.KeyPrefix, &apiKey.KeyHash,
		&apiKey.Name, &apiKey.RevokedAt, &apiKey.KeyID, &apiKey.KeyVersion, &apiKey.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrAPIKeyNotFound
		}
		return nil, err
	}
	return &apiKey, nil
}

// CountActive returns the count of non-revoked API keys for the workspace.
func (r *APIKeyRepository) CountActive(ctx context.Context, workspaceID uuid.UUID) (int, error) {
	var count int
	err := r.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM api_keys WHERE workspace_id = $1 AND revoked_at IS NULL`,
		workspaceID,
	).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}
