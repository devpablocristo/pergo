package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrMessageIdempotencyConflict = errors.New("message idempotency key reused")
	ErrMessageIdempotencyLease    = errors.New("message idempotency lease lost")
)

type MessageIdempotency struct {
	WorkspaceID    uuid.UUID
	IdempotencyKey string
	PayloadHash    string
	MessageID      uuid.UUID
	TraceID        string
	Status         string
	QueuedAt       time.Time
	LeaseToken     *uuid.UUID
	LeaseExpiresAt *time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

func (record MessageIdempotency) Accepted() bool {
	return record.Status == "accepted"
}

type MessageIdempotencyRepository struct {
	pool *pgxpool.Pool
}

func NewMessageIdempotencyRepository(
	pool *pgxpool.Pool,
) *MessageIdempotencyRepository {
	return &MessageIdempotencyRepository{pool: pool}
}

func (repository *MessageIdempotencyRepository) Acquire(
	ctx context.Context,
	workspaceID uuid.UUID,
	idempotencyKey string,
	payloadHash string,
	traceID string,
	leaseFor time.Duration,
) (MessageIdempotency, bool, error) {
	if repository == nil || repository.pool == nil {
		return MessageIdempotency{}, false, errors.New(
			"message idempotency database is required",
		)
	}
	if workspaceID == uuid.Nil || idempotencyKey == "" ||
		payloadHash == "" || traceID == "" || leaseFor <= 0 {
		return MessageIdempotency{}, false, errors.New(
			"message idempotency claim is invalid",
		)
	}
	tx, err := repository.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return MessageIdempotency{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	now := time.Now().UTC()
	_, err = tx.Exec(ctx, `
		INSERT INTO message_idempotency(
			workspace_id,idempotency_key,payload_hash,message_id,trace_id,
			status,queued_at,created_at,updated_at
		)
		VALUES($1,$2,$3,$4,$5,'pending',$6,$6,$6)
		ON CONFLICT (workspace_id,idempotency_key) DO NOTHING`,
		workspaceID, idempotencyKey, payloadHash, uuid.New(), traceID, now,
	)
	if err != nil {
		return MessageIdempotency{}, false, err
	}

	record, err := scanMessageIdempotency(tx.QueryRow(ctx, `
		SELECT workspace_id,idempotency_key,payload_hash,message_id,trace_id,
		       status,queued_at,lease_token,lease_expires_at,created_at,updated_at
		FROM message_idempotency
		WHERE workspace_id=$1 AND idempotency_key=$2
		FOR UPDATE`,
		workspaceID, idempotencyKey,
	))
	if err != nil {
		return MessageIdempotency{}, false, err
	}
	if record.PayloadHash != payloadHash {
		return MessageIdempotency{}, false, ErrMessageIdempotencyConflict
	}
	if record.Accepted() {
		if err = tx.Commit(ctx); err != nil {
			return MessageIdempotency{}, false, err
		}
		return record, false, nil
	}
	if record.Status == "processing" && record.LeaseExpiresAt != nil &&
		record.LeaseExpiresAt.After(now) {
		if err = tx.Commit(ctx); err != nil {
			return MessageIdempotency{}, false, err
		}
		return record, false, nil
	}

	leaseToken := uuid.New()
	leaseExpiresAt := now.Add(leaseFor)
	record, err = scanMessageIdempotency(tx.QueryRow(ctx, `
		UPDATE message_idempotency
		SET status='processing',lease_token=$1,lease_expires_at=$2,updated_at=$3
		WHERE workspace_id=$4 AND idempotency_key=$5 AND payload_hash=$6
		RETURNING workspace_id,idempotency_key,payload_hash,message_id,trace_id,
		          status,queued_at,lease_token,lease_expires_at,created_at,updated_at`,
		leaseToken, leaseExpiresAt, now, workspaceID, idempotencyKey, payloadHash,
	))
	if err != nil {
		return MessageIdempotency{}, false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return MessageIdempotency{}, false, err
	}
	return record, true, nil
}

func (repository *MessageIdempotencyRepository) Get(
	ctx context.Context,
	workspaceID uuid.UUID,
	idempotencyKey string,
) (MessageIdempotency, error) {
	if repository == nil || repository.pool == nil {
		return MessageIdempotency{}, errors.New(
			"message idempotency database is required",
		)
	}
	return scanMessageIdempotency(repository.pool.QueryRow(ctx, `
		SELECT workspace_id,idempotency_key,payload_hash,message_id,trace_id,
		       status,queued_at,lease_token,lease_expires_at,created_at,updated_at
		FROM message_idempotency
		WHERE workspace_id=$1 AND idempotency_key=$2`,
		workspaceID, idempotencyKey,
	))
}

func (repository *MessageIdempotencyRepository) MarkAccepted(
	ctx context.Context,
	record MessageIdempotency,
) (MessageIdempotency, error) {
	if record.LeaseToken == nil {
		return MessageIdempotency{}, ErrMessageIdempotencyLease
	}
	accepted, err := scanMessageIdempotency(repository.pool.QueryRow(ctx, `
		UPDATE message_idempotency
		SET status='accepted',lease_token=NULL,lease_expires_at=NULL,updated_at=$1
		WHERE workspace_id=$2 AND idempotency_key=$3 AND payload_hash=$4
		  AND status='processing' AND lease_token=$5
		RETURNING workspace_id,idempotency_key,payload_hash,message_id,trace_id,
		          status,queued_at,lease_token,lease_expires_at,created_at,updated_at`,
		time.Now().UTC(), record.WorkspaceID, record.IdempotencyKey,
		record.PayloadHash, *record.LeaseToken,
	))
	if errors.Is(err, pgx.ErrNoRows) {
		current, getErr := repository.Get(
			ctx, record.WorkspaceID, record.IdempotencyKey,
		)
		if getErr == nil && current.PayloadHash == record.PayloadHash &&
			current.Accepted() {
			return current, nil
		}
		return MessageIdempotency{}, ErrMessageIdempotencyLease
	}
	return accepted, err
}

func (repository *MessageIdempotencyRepository) Release(
	ctx context.Context,
	record MessageIdempotency,
) error {
	if record.LeaseToken == nil {
		return nil
	}
	_, err := repository.pool.Exec(ctx, `
		UPDATE message_idempotency
		SET status='pending',lease_token=NULL,lease_expires_at=NULL,updated_at=$1
		WHERE workspace_id=$2 AND idempotency_key=$3 AND payload_hash=$4
		  AND status='processing' AND lease_token=$5`,
		time.Now().UTC(), record.WorkspaceID, record.IdempotencyKey,
		record.PayloadHash, *record.LeaseToken,
	)
	return err
}

func scanMessageIdempotency(
	row pgx.Row,
) (MessageIdempotency, error) {
	var record MessageIdempotency
	err := row.Scan(
		&record.WorkspaceID,
		&record.IdempotencyKey,
		&record.PayloadHash,
		&record.MessageID,
		&record.TraceID,
		&record.Status,
		&record.QueuedAt,
		&record.LeaseToken,
		&record.LeaseExpiresAt,
		&record.CreatedAt,
		&record.UpdatedAt,
	)
	if err != nil {
		return MessageIdempotency{}, fmt.Errorf(
			"scan message idempotency: %w",
			err,
		)
	}
	return record, nil
}
