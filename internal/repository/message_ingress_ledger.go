package repository

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	// ErrIngressIdempotencyKeyReused means a workspace reused an idempotency
	// identity with a different trace or payload.
	ErrIngressIdempotencyKeyReused = errors.New("ingress idempotency key reused")
	// ErrIngressClaimActive means another request still owns a live publish claim.
	ErrIngressClaimActive = errors.New("ingress claim is active")
	// ErrIngressClaimLost means a stale owner attempted to complete a claim after
	// its fencing token or generation had changed.
	ErrIngressClaimLost = errors.New("ingress claim was lost")
)

const defaultIngressLease = 30 * time.Second

// MessageIngressLedgerRepository owns the durable HTTP-to-JetStream handoff
// state. Database transactions end before callers publish to NATS.
type MessageIngressLedgerRepository struct {
	pool *pgxpool.Pool
}

// NewMessageIngressLedgerRepository creates a workspace-scoped ingress ledger.
func NewMessageIngressLedgerRepository(pool *pgxpool.Pool) *MessageIngressLedgerRepository {
	return &MessageIngressLedgerRepository{pool: pool}
}

// Claim creates or acquires a fenced publish claim.
//
// A queued row is returned as replay=true. An active claim returns
// ErrIngressClaimActive and a retry duration. Expired claims are recovered with
// a new token and monotonically increasing generation.
func (r *MessageIngressLedgerRepository) Claim(
	ctx context.Context,
	workspaceID uuid.UUID,
	idempotencyKey string,
	payloadHash [32]byte,
	traceID string,
	receiptID uuid.UUID,
	lease time.Duration,
) (
	storedReceipt uuid.UUID,
	queuedAt time.Time,
	claimToken uuid.UUID,
	claimGeneration int64,
	replay bool,
	retryAfter time.Duration,
	err error,
) {
	if lease <= 0 {
		lease = defaultIngressLease
	}

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return uuid.Nil, time.Time{}, uuid.Nil, 0, false, 0, fmt.Errorf("begin ingress claim: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	proposedToken := uuid.New()
	insertTag, err := tx.Exec(ctx, `
		INSERT INTO message_ingress_ledger (
			workspace_id,
			idempotency_key,
			payload_hash,
			trace_id,
			receipt_id,
			state,
			claim_token,
			claim_generation,
			claim_expires_at
		)
		VALUES ($1, $2, $3, $4, $5, 'claimed', $6, 1, clock_timestamp() + make_interval(secs => $7))
		ON CONFLICT DO NOTHING
	`, workspaceID, idempotencyKey, payloadHash[:], traceID, receiptID, proposedToken, lease.Seconds())
	if err != nil {
		return uuid.Nil, time.Time{}, uuid.Nil, 0, false, 0, fmt.Errorf("insert ingress claim: %w", err)
	}

	var (
		storedHash       []byte
		storedTrace      string
		state            string
		storedClaimToken *uuid.UUID
		claimExpiresAt   *time.Time
		storedQueuedAt   *time.Time
		retrySeconds     float64
	)
	err = tx.QueryRow(ctx, `
		SELECT
			payload_hash,
			trace_id,
			receipt_id,
			state,
			claim_token,
			claim_generation,
			claim_expires_at,
			queued_at,
			CASE
				WHEN claim_expires_at IS NULL THEN 0
				ELSE GREATEST(EXTRACT(EPOCH FROM claim_expires_at - clock_timestamp()), 0)
			END
		FROM message_ingress_ledger
		WHERE workspace_id = $1
		  AND idempotency_key = $2
		FOR UPDATE
	`, workspaceID, idempotencyKey).Scan(
		&storedHash,
		&storedTrace,
		&storedReceipt,
		&state,
		&storedClaimToken,
		&claimGeneration,
		&claimExpiresAt,
		&storedQueuedAt,
		&retrySeconds,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		// The insert can lose to the global trace_id uniqueness constraint even
		// when the idempotency key itself is new. Treat that as an identity
		// collision instead of allowing two NATS publishes with one trace.
		var conflictingKey string
		conflictErr := tx.QueryRow(ctx, `
			SELECT idempotency_key
			FROM message_ingress_ledger
			WHERE trace_id = $1
			FOR UPDATE
		`, traceID).Scan(&conflictingKey)
		if conflictErr == nil {
			return uuid.Nil, time.Time{}, uuid.Nil, 0, false, 0, ErrIngressIdempotencyKeyReused
		}
		if errors.Is(conflictErr, pgx.ErrNoRows) {
			return uuid.Nil, time.Time{}, uuid.Nil, 0, false, 0, fmt.Errorf("ingress claim disappeared")
		}
		return uuid.Nil, time.Time{}, uuid.Nil, 0, false, 0, fmt.Errorf("find conflicting ingress trace: %w", conflictErr)
	}
	if err != nil {
		return uuid.Nil, time.Time{}, uuid.Nil, 0, false, 0, fmt.Errorf("lock ingress claim: %w", err)
	}

	if storedTrace != traceID || !bytes.Equal(storedHash, payloadHash[:]) {
		return uuid.Nil, time.Time{}, uuid.Nil, 0, false, 0, ErrIngressIdempotencyKeyReused
	}

	switch state {
	case "queued":
		if storedQueuedAt == nil {
			return uuid.Nil, time.Time{}, uuid.Nil, 0, false, 0, fmt.Errorf("queued ingress row has no queued_at")
		}
		if err := tx.Commit(ctx); err != nil {
			return uuid.Nil, time.Time{}, uuid.Nil, 0, false, 0, fmt.Errorf("commit ingress replay: %w", err)
		}
		return storedReceipt, storedQueuedAt.UTC(), uuid.Nil, claimGeneration, true, 0, nil

	case "claimed":
		if storedClaimToken == nil || claimExpiresAt == nil {
			return uuid.Nil, time.Time{}, uuid.Nil, 0, false, 0, fmt.Errorf("claimed ingress row is incomplete")
		}
		if insertTag.RowsAffected() == 1 {
			if err := tx.Commit(ctx); err != nil {
				return uuid.Nil, time.Time{}, uuid.Nil, 0, false, 0, fmt.Errorf("commit new ingress claim: %w", err)
			}
			return storedReceipt, time.Time{}, *storedClaimToken, claimGeneration, false, 0, nil
		}
		if retrySeconds > 0 {
			retryAfter = time.Duration(math.Ceil(retrySeconds * float64(time.Second)))
			return storedReceipt, time.Time{}, uuid.Nil, claimGeneration, false, retryAfter, ErrIngressClaimActive
		}

		nextToken := uuid.New()
		nextGeneration := claimGeneration + 1
		tag, updateErr := tx.Exec(ctx, `
			UPDATE message_ingress_ledger
			SET claim_token = $1,
			    claim_generation = $2,
			    claim_expires_at = clock_timestamp() + make_interval(secs => $3),
			    updated_at = clock_timestamp()
			WHERE workspace_id = $4
			  AND idempotency_key = $5
			  AND state = 'claimed'
			  AND claim_generation = $6
		`, nextToken, nextGeneration, lease.Seconds(), workspaceID, idempotencyKey, claimGeneration)
		if updateErr != nil {
			return uuid.Nil, time.Time{}, uuid.Nil, 0, false, 0, fmt.Errorf("recover ingress claim: %w", updateErr)
		}
		if tag.RowsAffected() != 1 {
			return uuid.Nil, time.Time{}, uuid.Nil, 0, false, 0, ErrIngressClaimLost
		}
		if err := tx.Commit(ctx); err != nil {
			return uuid.Nil, time.Time{}, uuid.Nil, 0, false, 0, fmt.Errorf("commit recovered ingress claim: %w", err)
		}
		return storedReceipt, time.Time{}, nextToken, nextGeneration, false, 0, nil

	default:
		return uuid.Nil, time.Time{}, uuid.Nil, 0, false, 0, fmt.Errorf("unknown ingress state %q", state)
	}
}

// MarkQueued completes a claim only while its fencing token and generation are
// still current. A stale publisher can never overwrite a recovered owner.
func (r *MessageIngressLedgerRepository) MarkQueued(
	ctx context.Context,
	workspaceID uuid.UUID,
	idempotencyKey string,
	claimToken uuid.UUID,
	claimGeneration int64,
	queuedAt time.Time,
) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE message_ingress_ledger
		SET state = 'queued',
		    claim_token = NULL,
		    claim_expires_at = NULL,
		    queued_at = $1,
		    updated_at = clock_timestamp()
		WHERE workspace_id = $2
		  AND idempotency_key = $3
		  AND state = 'claimed'
		  AND claim_token = $4
		  AND claim_generation = $5
	`, queuedAt.UTC(), workspaceID, idempotencyKey, claimToken, claimGeneration)
	if err != nil {
		return fmt.Errorf("mark ingress queued: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrIngressClaimLost
	}
	return nil
}
