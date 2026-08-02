package repository

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrWebhookSubscriptionNotFound = errors.New("webhook subscription not found")
)

type WebhookSubscription struct {
	ID          uuid.UUID
	WorkspaceID uuid.UUID
	URL         string
	Secret      []byte // Plaintext secret decrypted by CredentialProvider
	KeyID       string
	KeyVersion  int
	EventTypes  []string
	Active      bool
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

type WebhookSubscriptionRepository struct {
	pool      *pgxpool.Pool
	encryptor CredentialProvider
}

func NewWebhookSubscriptionRepository(pool *pgxpool.Pool, encryptor CredentialProvider) *WebhookSubscriptionRepository {
	return &WebhookSubscriptionRepository{
		pool:      pool,
		encryptor: encryptor,
	}
}

// Create inserts a new subscription
func (r *WebhookSubscriptionRepository) Create(ctx context.Context, wsID uuid.UUID, url string, eventTypes []string, secretPlaintext []byte) (*WebhookSubscription, error) {
	ciphertext, keyID, keyVersion, err := r.encryptor.Encrypt(secretPlaintext)
	if err != nil {
		return nil, err
	}

	var sub WebhookSubscription
	err = r.pool.QueryRow(ctx,
		`INSERT INTO webhook_subscriptions (workspace_id, url, secret, key_id, key_version, event_types, active, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, TRUE, now())
		 RETURNING id, workspace_id, url, key_id, key_version, event_types, active, created_at, updated_at`,
		wsID, url, ciphertext, keyID, keyVersion, eventTypes,
	).Scan(&sub.ID, &sub.WorkspaceID, &sub.URL, &sub.KeyID, &sub.KeyVersion, &sub.EventTypes, &sub.Active, &sub.CreatedAt, &sub.UpdatedAt)
	if err != nil {
		return nil, err
	}
	sub.Secret = secretPlaintext
	return &sub, nil
}

// Get retrieves a subscription by ID
func (r *WebhookSubscriptionRepository) Get(ctx context.Context, id uuid.UUID) (*WebhookSubscription, error) {
	return r.get(ctx, uuid.Nil, id)
}

// GetForWorkspace retrieves a subscription only when it belongs to the
// workspace in the authenticated route.
func (r *WebhookSubscriptionRepository) GetForWorkspace(ctx context.Context, workspaceID, id uuid.UUID) (*WebhookSubscription, error) {
	return r.get(ctx, workspaceID, id)
}

func (r *WebhookSubscriptionRepository) get(ctx context.Context, workspaceID, id uuid.UUID) (*WebhookSubscription, error) {
	var sub WebhookSubscription
	var ciphertext []byte

	err := r.pool.QueryRow(ctx, `
		SELECT id, workspace_id, url, secret, key_id, key_version, event_types, active, created_at, updated_at
		FROM webhook_subscriptions
		WHERE id = $1
		  AND ($2::uuid IS NULL OR workspace_id = $2)
	`,
		id, nullableUUID(workspaceID),
	).Scan(&sub.ID, &sub.WorkspaceID, &sub.URL, &ciphertext, &sub.KeyID, &sub.KeyVersion, &sub.EventTypes, &sub.Active, &sub.CreatedAt, &sub.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrWebhookSubscriptionNotFound
		}
		return nil, err
	}

	secret, err := r.encryptor.Decrypt(ciphertext)
	if err != nil {
		return nil, err
	}
	sub.Secret = secret
	return &sub, nil
}

// ListByWorkspace returns all subscriptions belonging to a workspace
func (r *WebhookSubscriptionRepository) ListByWorkspace(ctx context.Context, wsID uuid.UUID) ([]*WebhookSubscription, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, workspace_id, url, secret, key_id, key_version, event_types, active, created_at, updated_at
		 FROM webhook_subscriptions WHERE workspace_id = $1 ORDER BY created_at DESC`,
		wsID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var subs []*WebhookSubscription
	for rows.Next() {
		var sub WebhookSubscription
		var ciphertext []byte
		err := rows.Scan(&sub.ID, &sub.WorkspaceID, &sub.URL, &ciphertext, &sub.KeyID, &sub.KeyVersion, &sub.EventTypes, &sub.Active, &sub.CreatedAt, &sub.UpdatedAt)
		if err != nil {
			return nil, err
		}
		secret, err := r.encryptor.Decrypt(ciphertext)
		if err != nil {
			return nil, err
		}
		sub.Secret = secret
		subs = append(subs, &sub)
	}
	return subs, rows.Err()
}

// RequireSecureActive fails workload startup when a legacy active
// subscription predates the current destination or signing-secret policy.
// Runtime delivery still performs DNS and dial-time public-address validation.
func (r *WebhookSubscriptionRepository) RequireSecureActive(ctx context.Context) error {
	rows, err := r.pool.Query(ctx, `
		SELECT id, url, secret
		FROM webhook_subscriptions
		WHERE active = TRUE
		ORDER BY id
	`)
	if err != nil {
		return fmt.Errorf("list active webhook subscriptions: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			id         uuid.UUID
			rawURL     string
			ciphertext []byte
		)
		if err := rows.Scan(&id, &rawURL, &ciphertext); err != nil {
			return fmt.Errorf("scan active webhook subscription: %w", err)
		}
		secret, err := r.encryptor.Decrypt(ciphertext)
		if err != nil {
			return fmt.Errorf("decrypt active webhook subscription %s: %w", id, err)
		}
		if len(secret) < 32 {
			return fmt.Errorf("active webhook subscription %s has a signing secret shorter than 32 bytes", id)
		}
		destination, err := url.Parse(strings.TrimSpace(rawURL))
		if err != nil ||
			destination.Scheme != "https" ||
			destination.Host == "" ||
			destination.User != nil ||
			destination.Fragment != "" {
			return fmt.Errorf("active webhook subscription %s has an unsafe destination", id)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate active webhook subscriptions: %w", err)
	}
	return nil
}

// Update modifies a subscription
func (r *WebhookSubscriptionRepository) Update(ctx context.Context, id uuid.UUID, url string, eventTypes []string, active bool, secretPlaintext []byte) error {
	return r.UpdateForWorkspace(ctx, uuid.Nil, id, url, eventTypes, active, secretPlaintext)
}

// UpdateForWorkspace updates a subscription only within the requested
// workspace. A cross-workspace ID is indistinguishable from a missing row.
func (r *WebhookSubscriptionRepository) UpdateForWorkspace(ctx context.Context, workspaceID, id uuid.UUID, url string, eventTypes []string, active bool, secretPlaintext []byte) error {
	var tag pgconn.CommandTag
	var err error
	if len(secretPlaintext) > 0 {
		ciphertext, keyID, keyVersion, encryptErr := r.encryptor.Encrypt(secretPlaintext)
		if encryptErr != nil {
			return encryptErr
		}
		tag, err = r.pool.Exec(ctx,
			`UPDATE webhook_subscriptions 
			 SET url = $1, event_types = $2, active = $3, secret = $4, key_id = $5, key_version = $6, updated_at = now()
			 WHERE id = $7
			   AND ($8::uuid IS NULL OR workspace_id = $8)`,
			url, eventTypes, active, ciphertext, keyID, keyVersion, id, nullableUUID(workspaceID),
		)
	} else {
		tag, err = r.pool.Exec(ctx,
			`UPDATE webhook_subscriptions
			 SET url = $1, event_types = $2, active = $3, updated_at = now()
			 WHERE id = $4
			   AND ($5::uuid IS NULL OR workspace_id = $5)`,
			url, eventTypes, active, id, nullableUUID(workspaceID),
		)
	}
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrWebhookSubscriptionNotFound
	}
	return nil
}

// Delete removes a subscription
func (r *WebhookSubscriptionRepository) Delete(ctx context.Context, id uuid.UUID) error {
	return r.DeleteForWorkspace(ctx, uuid.Nil, id)
}

// DeleteForWorkspace deletes a subscription only within the requested
// workspace.
func (r *WebhookSubscriptionRepository) DeleteForWorkspace(ctx context.Context, workspaceID, id uuid.UUID) error {
	tag, err := r.pool.Exec(
		ctx,
		`DELETE FROM webhook_subscriptions
		 WHERE id = $1
		   AND ($2::uuid IS NULL OR workspace_id = $2)`,
		id,
		nullableUUID(workspaceID),
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrWebhookSubscriptionNotFound
	}
	return nil
}

func nullableUUID(id uuid.UUID) any {
	if id == uuid.Nil {
		return nil
	}
	return id
}
