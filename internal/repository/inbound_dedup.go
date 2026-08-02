package repository

import (
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
	// ErrInboundClaimActive means another request owns an unexpired handoff
	// claim for the same provider message.
	ErrInboundClaimActive = errors.New("inbound delivery claim is active")
	// ErrInboundClaimLost means a stale owner attempted to mutate a claim after
	// another request recovered it.
	ErrInboundClaimLost = errors.New("inbound delivery claim was lost")
)

const defaultInboundClaimLease = 2 * time.Minute

// InboundClaim is a fenced right to hand one provider event to the durable
// message boundary. TraceID remains stable across every recovery attempt.
type InboundClaim struct {
	TraceID    string
	Token      uuid.UUID
	Generation int64
}

// InboundDedupRepository handles database-level deduplication for inbound messages.
type InboundDedupRepository struct {
	pool *pgxpool.Pool
}

// NewInboundDedupRepository creates a new InboundDedupRepository.
func NewInboundDedupRepository(pool *pgxpool.Pool) *InboundDedupRepository {
	return &InboundDedupRepository{
		pool: pool,
	}
}

// Claim atomically claims an inbound provider message for publication.
//
// A published row returns replay=true. A live claim returns
// ErrInboundClaimActive. An expired or explicitly released claim is recovered
// with a new fencing token while preserving its stable TraceID.
func (r *InboundDedupRepository) Claim(
	ctx context.Context,
	workspaceID uuid.UUID,
	connectionID uuid.UUID,
	channel string,
	providerMessageID string,
	lease time.Duration,
) (claim InboundClaim, replay bool, retryAfter time.Duration, err error) {
	if lease <= 0 {
		lease = defaultInboundClaimLease
	}

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return InboundClaim{}, false, 0, fmt.Errorf("begin inbound claim: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	proposed := InboundClaim{
		TraceID:    uuid.NewString(),
		Token:      uuid.New(),
		Generation: 1,
	}
	insertTag, err := tx.Exec(ctx, `
		INSERT INTO inbound_dedups (
			workspace_id,
			connection_id,
			channel,
			provider_message_id,
			state,
			trace_id,
			claim_token,
			claim_generation,
			claim_expires_at,
			published_at
		)
		VALUES (
			$1, $2, $3, $4, 'claimed', $5, $6, 1,
			clock_timestamp() + make_interval(secs => $7),
			NULL
		)
		ON CONFLICT (workspace_id, connection_id, channel, provider_message_id) DO NOTHING
	`, workspaceID, connectionID, channel, providerMessageID, proposed.TraceID, proposed.Token, lease.Seconds())
	if err != nil {
		return InboundClaim{}, false, 0, fmt.Errorf("insert inbound claim: %w", err)
	}

	var (
		state        string
		storedToken  *uuid.UUID
		expiresAt    *time.Time
		retrySeconds float64
	)
	err = tx.QueryRow(ctx, `
		SELECT
			state,
			trace_id,
			claim_token,
			claim_generation,
			claim_expires_at,
			CASE
				WHEN claim_expires_at IS NULL THEN 0
				ELSE GREATEST(EXTRACT(EPOCH FROM claim_expires_at - clock_timestamp()), 0)
			END
		FROM inbound_dedups
		WHERE workspace_id = $1
		  AND connection_id = $2
		  AND channel = $3
		  AND provider_message_id = $4
		FOR UPDATE
	`, workspaceID, connectionID, channel, providerMessageID).Scan(
		&state,
		&claim.TraceID,
		&storedToken,
		&claim.Generation,
		&expiresAt,
		&retrySeconds,
	)
	if err != nil {
		return InboundClaim{}, false, 0, fmt.Errorf("lock inbound claim: %w", err)
	}

	switch state {
	case "published":
		if err := tx.Commit(ctx); err != nil {
			return InboundClaim{}, false, 0, fmt.Errorf("commit inbound replay: %w", err)
		}
		return InboundClaim{TraceID: claim.TraceID, Generation: claim.Generation}, true, 0, nil
	case "claimed":
		if storedToken == nil || expiresAt == nil {
			return InboundClaim{}, false, 0, fmt.Errorf("inbound claim is incomplete")
		}
		if insertTag.RowsAffected() == 1 {
			claim.Token = *storedToken
			if err := tx.Commit(ctx); err != nil {
				return InboundClaim{}, false, 0, fmt.Errorf("commit new inbound claim: %w", err)
			}
			return claim, false, 0, nil
		}
		if retrySeconds > 0 {
			retryAfter = time.Duration(math.Ceil(retrySeconds * float64(time.Second)))
			return InboundClaim{TraceID: claim.TraceID, Generation: claim.Generation}, false, retryAfter, ErrInboundClaimActive
		}

		nextToken := uuid.New()
		nextGeneration := claim.Generation + 1
		tag, updateErr := tx.Exec(ctx, `
			UPDATE inbound_dedups
			SET claim_token = $1,
			    claim_generation = $2,
			    claim_expires_at = clock_timestamp() + make_interval(secs => $3),
			    updated_at = clock_timestamp()
			WHERE workspace_id = $4
			  AND connection_id = $5
			  AND channel = $6
			  AND provider_message_id = $7
			  AND state = 'claimed'
			  AND claim_generation = $8
		`, nextToken, nextGeneration, lease.Seconds(), workspaceID, connectionID, channel, providerMessageID, claim.Generation)
		if updateErr != nil {
			return InboundClaim{}, false, 0, fmt.Errorf("recover inbound claim: %w", updateErr)
		}
		if tag.RowsAffected() != 1 {
			return InboundClaim{}, false, 0, ErrInboundClaimLost
		}
		claim.Token = nextToken
		claim.Generation = nextGeneration
		if err := tx.Commit(ctx); err != nil {
			return InboundClaim{}, false, 0, fmt.Errorf("commit recovered inbound claim: %w", err)
		}
		return claim, false, 0, nil
	default:
		return InboundClaim{}, false, 0, fmt.Errorf("unknown inbound state %q", state)
	}
}

// MarkPublished completes the current claim after the durable broker accepted
// the event (or after the processor intentionally ignored an empty event).
func (r *InboundDedupRepository) MarkPublished(
	ctx context.Context,
	workspaceID uuid.UUID,
	connectionID uuid.UUID,
	channel string,
	providerMessageID string,
	claim InboundClaim,
) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE inbound_dedups
		SET state = 'published',
		    claim_token = NULL,
		    claim_expires_at = NULL,
		    published_at = clock_timestamp(),
		    updated_at = clock_timestamp()
		WHERE workspace_id = $1
		  AND connection_id = $2
		  AND channel = $3
		  AND provider_message_id = $4
		  AND state = 'claimed'
		  AND trace_id = $5
		  AND claim_token = $6
		  AND claim_generation = $7
	`, workspaceID, connectionID, channel, providerMessageID, claim.TraceID, claim.Token, claim.Generation)
	if err != nil {
		return fmt.Errorf("mark inbound published: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrInboundClaimLost
	}
	return nil
}

// Release expires the current claim immediately after a definite pre-accept
// failure. A retry can recover it without waiting for the crash lease.
func (r *InboundDedupRepository) Release(
	ctx context.Context,
	workspaceID uuid.UUID,
	connectionID uuid.UUID,
	channel string,
	providerMessageID string,
	claim InboundClaim,
) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE inbound_dedups
		SET claim_expires_at = clock_timestamp(),
		    updated_at = clock_timestamp()
		WHERE workspace_id = $1
		  AND connection_id = $2
		  AND channel = $3
		  AND provider_message_id = $4
		  AND state = 'claimed'
		  AND trace_id = $5
		  AND claim_token = $6
		  AND claim_generation = $7
	`, workspaceID, connectionID, channel, providerMessageID, claim.TraceID, claim.Token, claim.Generation)
	if err != nil {
		return fmt.Errorf("release inbound claim: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrInboundClaimLost
	}
	return nil
}

// InsertAndCheck atomically inserts the provider message ID.
// Returns true if the message was successfully inserted (unique), or false if it already existed.
func (r *InboundDedupRepository) InsertAndCheck(
	ctx context.Context,
	workspaceID uuid.UUID,
	connectionID uuid.UUID,
	channel string,
	providerMessageID string,
) (bool, error) {
	query := `
		INSERT INTO inbound_dedups (workspace_id, connection_id, channel, provider_message_id, created_at)
		VALUES ($1, $2, $3, $4, NOW())
		ON CONFLICT (workspace_id, connection_id, channel, provider_message_id) DO NOTHING
	`
	res, err := r.pool.Exec(ctx, query, workspaceID, connectionID, channel, providerMessageID)
	if err != nil {
		return false, fmt.Errorf("inbound dedup insert: %w", err)
	}

	return res.RowsAffected() == 1, nil
}
