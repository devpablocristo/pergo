package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrWebhookConfigNotFound = errors.New("webhook config not found")
	ErrWebhookDLQNotFound    = errors.New("webhook DLQ item not found")
)

// WebhookConfig represents a workspace's webhook configuration.
type WebhookConfig struct {
	ID          uuid.UUID
	WorkspaceID uuid.UUID
	URL         string
	Secret      []byte // Plaintext secret
	KeyID       string
	KeyVersion  int
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// WebhookDLQ represents a dead-lettered webhook payload.
type WebhookDLQ struct {
	ID             uuid.UUID
	WorkspaceID    uuid.UUID
	SubscriptionID uuid.UUID
	TraceID        string
	MessageID      string
	EventType      string
	Payload        []byte // JSON payload
	WebhookURL     string
	LastAttemptAt  time.Time
	FailureReason  *string
	Attempts       int
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type WebhookDLQRepository struct {
	pool      *pgxpool.Pool
	encryptor CredentialProvider
}

func NewWebhookDLQRepository(pool *pgxpool.Pool, encryptor CredentialProvider) *WebhookDLQRepository {
	return &WebhookDLQRepository{
		pool:      pool,
		encryptor: encryptor,
	}
}

type encryptedWebhookDLQ struct {
	Payload       []byte  `json:"payload"`
	WebhookURL    string  `json:"webhook_url"`
	FailureReason *string `json:"failure_reason,omitempty"`
}

// InsertDLQ inserts a new webhook DLQ item.
func (r *WebhookDLQRepository) InsertDLQ(ctx context.Context, workspaceID uuid.UUID, subscriptionID uuid.UUID, traceID, messageID, eventType string, payload []byte, url string, attempts int, failureReason *string) error {
	encrypted, keyID, keyVersion, err := r.encryptDLQ(payload, url, failureReason)
	if err != nil {
		return err
	}
	_, err = r.pool.Exec(ctx,
		`INSERT INTO webhook_dlqs (
			workspace_id, subscription_id, trace_id, message_id, event_type,
			payload, webhook_url, attempts, failure_reason, last_attempt_at,
			updated_at, encrypted_data, key_id, key_version
		 )
		 VALUES (
			$1, $2, $3, $4, $5,
			'{}'::jsonb, '[encrypted]', $6, NULL, now(),
			now(), $7, $8, $9
		 )`,
		workspaceID, subscriptionID, traceID, messageID, eventType, attempts,
		encrypted, keyID, keyVersion,
	)
	return err
}

// ListDLQ lists DLQ items for a workspace with pagination.
func (r *WebhookDLQRepository) ListDLQ(ctx context.Context, workspaceID uuid.UUID, limit, offset int) ([]*WebhookDLQ, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, workspace_id, subscription_id, trace_id, message_id, event_type,
		        payload, webhook_url, last_attempt_at, failure_reason, attempts,
		        created_at, updated_at, encrypted_data
		 FROM webhook_dlqs 
		 WHERE workspace_id = $1
		 ORDER BY created_at DESC
		 LIMIT $2 OFFSET $3`,
		workspaceID, limit, offset,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []*WebhookDLQ
	for rows.Next() {
		item, err := r.scanDLQ(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return items, nil
}

// GetDLQByID retrieves a specific DLQ item by ID.
func (r *WebhookDLQRepository) GetDLQByID(ctx context.Context, id uuid.UUID) (*WebhookDLQ, error) {
	var item WebhookDLQ
	var encrypted []byte
	err := r.pool.QueryRow(ctx,
		`SELECT id, workspace_id, subscription_id, trace_id, message_id, event_type,
		        payload, webhook_url, last_attempt_at, failure_reason, attempts,
		        created_at, updated_at, encrypted_data
		 FROM webhook_dlqs WHERE id = $1`,
		id,
	).Scan(
		&item.ID, &item.WorkspaceID, &item.SubscriptionID, &item.TraceID, &item.MessageID, &item.EventType,
		&item.Payload, &item.WebhookURL, &item.LastAttemptAt, &item.FailureReason,
		&item.Attempts, &item.CreatedAt, &item.UpdatedAt, &encrypted,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrWebhookDLQNotFound
		}
		return nil, err
	}
	if err := r.decryptDLQ(&item, encrypted); err != nil {
		return nil, err
	}
	return &item, nil
}

// DeleteDLQ deletes a specific DLQ item.
func (r *WebhookDLQRepository) DeleteDLQ(ctx context.Context, id uuid.UUID) error {
	_, err := r.pool.Exec(ctx,
		`DELETE FROM webhook_dlqs WHERE id = $1`,
		id,
	)
	return err
}

// GetDLQBadgeCount returns the number of unresolved DLQ items for a workspace.
func (r *WebhookDLQRepository) GetDLQBadgeCount(ctx context.Context, workspaceID uuid.UUID) (int, error) {
	var count int
	err := r.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM webhook_dlqs WHERE workspace_id = $1`,
		workspaceID,
	).Scan(&count)
	return count, err
}

// ListAllDLQ lists all DLQ items across all workspaces.
func (r *WebhookDLQRepository) ListAllDLQ(ctx context.Context, limit, offset int) ([]*WebhookDLQ, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, workspace_id, subscription_id, trace_id, message_id, event_type,
		        payload, webhook_url, last_attempt_at, failure_reason, attempts,
		        created_at, updated_at, encrypted_data
		 FROM webhook_dlqs 
		 ORDER BY created_at DESC
		 LIMIT $1 OFFSET $2`,
		limit, offset,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []*WebhookDLQ
	for rows.Next() {
		item, err := r.scanDLQ(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return items, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func (r *WebhookDLQRepository) scanDLQ(row rowScanner) (*WebhookDLQ, error) {
	var item WebhookDLQ
	var encrypted []byte
	if err := row.Scan(
		&item.ID,
		&item.WorkspaceID,
		&item.SubscriptionID,
		&item.TraceID,
		&item.MessageID,
		&item.EventType,
		&item.Payload,
		&item.WebhookURL,
		&item.LastAttemptAt,
		&item.FailureReason,
		&item.Attempts,
		&item.CreatedAt,
		&item.UpdatedAt,
		&encrypted,
	); err != nil {
		return nil, err
	}
	if err := r.decryptDLQ(&item, encrypted); err != nil {
		return nil, err
	}
	return &item, nil
}

func (r *WebhookDLQRepository) encryptDLQ(
	payload []byte,
	url string,
	failureReason *string,
) ([]byte, string, int, error) {
	if r.encryptor == nil {
		return nil, "", 0, errors.New("webhook DLQ encryptor is required")
	}
	plaintext, err := json.Marshal(encryptedWebhookDLQ{
		Payload:       payload,
		WebhookURL:    url,
		FailureReason: failureReason,
	})
	if err != nil {
		return nil, "", 0, fmt.Errorf("marshal webhook DLQ: %w", err)
	}
	ciphertext, keyID, keyVersion, err := r.encryptor.Encrypt(plaintext)
	if err != nil {
		return nil, "", 0, fmt.Errorf("encrypt webhook DLQ: %w", err)
	}
	return ciphertext, keyID, keyVersion, nil
}

func (r *WebhookDLQRepository) decryptDLQ(item *WebhookDLQ, encrypted []byte) error {
	if len(encrypted) == 0 {
		// Legacy rows remain readable only during the controlled migration job.
		return nil
	}
	if r.encryptor == nil {
		return errors.New("webhook DLQ encryptor is required")
	}
	plaintext, err := r.encryptor.Decrypt(encrypted)
	if err != nil {
		return fmt.Errorf("decrypt webhook DLQ: %w", err)
	}
	var stored encryptedWebhookDLQ
	if err := json.Unmarshal(plaintext, &stored); err != nil {
		return fmt.Errorf("decode webhook DLQ: %w", err)
	}
	item.Payload = stored.Payload
	item.WebhookURL = stored.WebhookURL
	item.FailureReason = stored.FailureReason
	return nil
}

// BackfillLegacyEncryption encrypts and scrubs rows created before migration
// 039. It is idempotent and intended for the dedicated migration job while all
// legacy writers are quiesced.
func (r *WebhookDLQRepository) BackfillLegacyEncryption(ctx context.Context) error {
	const batchSize = 100
	type legacyRow struct {
		id            uuid.UUID
		payload       []byte
		url           string
		failureReason *string
	}

	for {
		rows, err := r.pool.Query(ctx, `
			SELECT id, payload, webhook_url, failure_reason
			FROM webhook_dlqs
			WHERE encrypted_data IS NULL
			ORDER BY id
			LIMIT $1
		`, batchSize)
		if err != nil {
			return fmt.Errorf("list legacy webhook DLQ rows: %w", err)
		}
		legacy := make([]legacyRow, 0, batchSize)
		for rows.Next() {
			var item legacyRow
			if err := rows.Scan(&item.id, &item.payload, &item.url, &item.failureReason); err != nil {
				rows.Close()
				return fmt.Errorf("scan legacy webhook DLQ row: %w", err)
			}
			legacy = append(legacy, item)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("iterate legacy webhook DLQ rows: %w", err)
		}
		rows.Close()
		if len(legacy) == 0 {
			break
		}

		for _, item := range legacy {
			encrypted, keyID, keyVersion, err := r.encryptDLQ(item.payload, item.url, item.failureReason)
			if err != nil {
				return err
			}
			tag, err := r.pool.Exec(ctx, `
				UPDATE webhook_dlqs
				SET encrypted_data = $1,
				    key_id = $2,
				    key_version = $3,
				    payload = '{}'::jsonb,
				    webhook_url = '[encrypted]',
				    failure_reason = NULL,
				    updated_at = clock_timestamp()
				WHERE id = $4
				  AND encrypted_data IS NULL
			`, encrypted, keyID, keyVersion, item.id)
			if err != nil {
				return fmt.Errorf("backfill webhook DLQ row: %w", err)
			}
			if tag.RowsAffected() > 1 {
				return errors.New("webhook DLQ backfill updated multiple rows")
			}
		}
	}
	return r.RequireEncrypted(ctx)
}

// RequireEncrypted fails startup when the controlled migration left any
// plaintext DLQ rows behind.
func (r *WebhookDLQRepository) RequireEncrypted(ctx context.Context) error {
	var count int
	if err := r.pool.QueryRow(
		ctx,
		`SELECT COUNT(*) FROM webhook_dlqs WHERE encrypted_data IS NULL`,
	).Scan(&count); err != nil {
		return fmt.Errorf("validate webhook DLQ encryption: %w", err)
	}
	if count != 0 {
		return fmt.Errorf("%d legacy webhook DLQ rows remain unencrypted", count)
	}
	return nil
}
