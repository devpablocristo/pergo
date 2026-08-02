package repository

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/pablojhp.pergo/internal/domain"
	"github.com/pablojhp.pergo/internal/platform/messagebus"
)

var (
	ErrCampaignInvalidState         = errors.New("campaign state does not allow this operation")
	ErrCampaignBatchConflict        = errors.New("campaign batch payload conflicts with durable state")
	ErrCampaignBatchNotFound        = errors.New("campaign batch not found")
	ErrCampaignBatchPayloadMismatch = errors.New("campaign batch payload does not match durable state")
	ErrCampaignBatchLeaseLost       = errors.New("campaign batch publish lease lost")
)

// MaxCampaignBatchPayloadBytes leaves more than 100 KiB below NATS' default
// 1 MiB max_payload for subject, headers and protocol overhead. The same bound
// is enforced by migration 044 so no unpublishable durable outbox row can be
// created through another code path.
const MaxCampaignBatchPayloadBytes = messagebus.MaxPayloadBytes

// CampaignBatch is an opaque, durable outbox payload prepared before a
// campaign enters the sending state.
type CampaignBatch struct {
	BatchIndex   int
	TotalBatches int
	TraceID      string
	Payload      []byte
	DelaySeconds int
}

// ClaimedCampaignBatch is a fenced lease for one due outbox publication.
type ClaimedCampaignBatch struct {
	CampaignID      uuid.UUID
	WorkspaceID     uuid.UUID
	BatchIndex      int
	TraceID         string
	Payload         []byte
	PublishAttempts int
	LeaseToken      uuid.UUID
}

// PrepareCampaignStart atomically persists every batch and transitions the
// campaign to sending. Repeating the same request is idempotent; a different
// batch snapshot for the same campaign is rejected.
func (r *CampaignRepository) PrepareCampaignStart(
	ctx context.Context,
	campaignID uuid.UUID,
	workspaceID uuid.UUID,
	expectedUpdatedAt time.Time,
	batches []CampaignBatch,
) (domain.CampaignStatus, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var status domain.CampaignStatus
	var updatedAt time.Time
	err = tx.QueryRow(
		ctx,
		`SELECT status, updated_at
		   FROM campaigns
		  WHERE id = $1 AND workspace_id = $2
		  FOR UPDATE`,
		campaignID,
		workspaceID,
	).Scan(&status, &updatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", ErrCampaignNotFound
		}
		return "", err
	}

	switch status {
	case domain.CampaignStatusCompleted:
		return status, tx.Commit(ctx)
	case domain.CampaignStatusCancelled, domain.CampaignStatusScheduled:
		return "", ErrCampaignInvalidState
	case domain.CampaignStatusDraft, domain.CampaignStatusSending:
		// Continue below.
	default:
		return "", ErrCampaignInvalidState
	}
	if status == domain.CampaignStatusDraft && !updatedAt.Equal(expectedUpdatedAt) {
		return "", ErrCampaignBatchConflict
	}

	if len(batches) == 0 {
		var existing int
		if err := tx.QueryRow(
			ctx,
			`SELECT count(*) FROM campaign_batches WHERE campaign_id = $1`,
			campaignID,
		).Scan(&existing); err != nil {
			return "", err
		}
		if existing != 0 {
			return "", ErrCampaignBatchConflict
		}
		if _, err := tx.Exec(
			ctx,
			`UPDATE campaigns
			    SET status = $1, updated_at = now()
			  WHERE id = $2 AND workspace_id = $3`,
			domain.CampaignStatusCompleted,
			campaignID,
			workspaceID,
		); err != nil {
			return "", err
		}
		if err := tx.Commit(ctx); err != nil {
			return "", err
		}
		return domain.CampaignStatusCompleted, nil
	}

	if err := validateCampaignBatchSet(batches); err != nil {
		return "", err
	}

	for _, batch := range batches {
		payloadHash := sha256.Sum256(batch.Payload)
		if _, err := tx.Exec(
			ctx,
			`INSERT INTO campaign_batches (
				     campaign_id,
				     workspace_id,
				     batch_index,
				     total_batches,
				     trace_id,
				     payload,
				     payload_hash,
				     delay_seconds,
				     next_publish_at
				 )
				 VALUES (
				     $1, $2, $3, $4, $5, $6, $7, $8,
				     CASE WHEN $3 = 1 THEN now() ELSE 'infinity'::timestamptz END
				 )
				 ON CONFLICT (campaign_id, batch_index) DO NOTHING`,
			campaignID,
			workspaceID,
			batch.BatchIndex,
			batch.TotalBatches,
			batch.TraceID,
			batch.Payload,
			payloadHash[:],
			batch.DelaySeconds,
		); err != nil {
			return "", err
		}

		var storedTotal int
		var storedTrace string
		var storedHash []byte
		var storedDelay int
		if err := tx.QueryRow(
			ctx,
			`SELECT total_batches, trace_id, payload_hash, delay_seconds
				   FROM campaign_batches
				  WHERE campaign_id = $1 AND batch_index = $2`,
			campaignID,
			batch.BatchIndex,
		).Scan(&storedTotal, &storedTrace, &storedHash, &storedDelay); err != nil {
			return "", err
		}
		if storedTotal != batch.TotalBatches ||
			storedTrace != batch.TraceID ||
			storedDelay != batch.DelaySeconds ||
			subtle.ConstantTimeCompare(storedHash, payloadHash[:]) != 1 {
			return "", ErrCampaignBatchConflict
		}
	}

	var durableCount int
	if err := tx.QueryRow(
		ctx,
		`SELECT count(*) FROM campaign_batches WHERE campaign_id = $1`,
		campaignID,
	).Scan(&durableCount); err != nil {
		return "", err
	}
	if durableCount != len(batches) {
		return "", ErrCampaignBatchConflict
	}

	if _, err := tx.Exec(
		ctx,
		`UPDATE campaigns
		    SET status = $1, updated_at = now()
		  WHERE id = $2 AND workspace_id = $3`,
		domain.CampaignStatusSending,
		campaignID,
		workspaceID,
	); err != nil {
		return "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", err
	}
	return domain.CampaignStatusSending, nil
}

func validateCampaignBatchSet(batches []CampaignBatch) error {
	total := len(batches)
	seen := make(map[int]struct{}, total)
	for _, batch := range batches {
		if batch.BatchIndex < 1 ||
			batch.TotalBatches != total ||
			batch.BatchIndex > total ||
			batch.TraceID == "" ||
			len(batch.Payload) == 0 ||
			len(batch.Payload) > MaxCampaignBatchPayloadBytes ||
			batch.DelaySeconds < 0 ||
			batch.DelaySeconds > domain.CampaignMaxDelaySeconds {
			return ErrCampaignBatchConflict
		}
		if _, exists := seen[batch.BatchIndex]; exists {
			return ErrCampaignBatchConflict
		}
		seen[batch.BatchIndex] = struct{}{}
	}
	return nil
}

// ClaimDueCampaignBatches leases due outbox records. SKIP LOCKED allows
// multiple worker replicas without publishing one row concurrently.
func (r *CampaignRepository) ClaimDueCampaignBatches(
	ctx context.Context,
	limit int,
	leaseDuration time.Duration,
) ([]ClaimedCampaignBatch, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	leaseSeconds := int64(leaseDuration / time.Second)
	if leaseSeconds < 1 {
		leaseSeconds = 1
	}
	leaseToken := uuid.New()

	rows, err := r.pool.Query(
		ctx,
		`WITH due AS (
		     SELECT b.campaign_id, b.batch_index
		       FROM campaign_batches b
		       JOIN campaigns c
		         ON c.id = b.campaign_id
		        AND c.workspace_id = b.workspace_id
			      WHERE c.status = $1
			        AND b.processed_at IS NULL
			        AND b.next_publish_at <= now()
			        AND (b.publish_lease_until IS NULL OR b.publish_lease_until < now())
			        AND NOT EXISTS (
			            SELECT 1
			              FROM campaign_batches earlier
			             WHERE earlier.campaign_id = b.campaign_id
			               AND earlier.batch_index < b.batch_index
			               AND earlier.processed_at IS NULL
			        )
		      ORDER BY b.next_publish_at, b.campaign_id, b.batch_index
		      FOR UPDATE OF b SKIP LOCKED
		      LIMIT $2
		 )
		 UPDATE campaign_batches b
		    SET publish_lease_token = $3,
		        publish_lease_until = now() + ($4 * interval '1 second'),
		        updated_at = now()
		   FROM due
		  WHERE b.campaign_id = due.campaign_id
		    AND b.batch_index = due.batch_index
		 RETURNING b.campaign_id,
		           b.workspace_id,
		           b.batch_index,
		           b.trace_id,
		           b.payload,
		           b.publish_attempts`,
		domain.CampaignStatusSending,
		limit,
		leaseToken,
		leaseSeconds,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	claims := make([]ClaimedCampaignBatch, 0)
	for rows.Next() {
		var claim ClaimedCampaignBatch
		claim.LeaseToken = leaseToken
		if err := rows.Scan(
			&claim.CampaignID,
			&claim.WorkspaceID,
			&claim.BatchIndex,
			&claim.TraceID,
			&claim.Payload,
			&claim.PublishAttempts,
		); err != nil {
			return nil, err
		}
		claims = append(claims, claim)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return claims, nil
}

// MarkCampaignBatchPublished releases a successful publish lease and schedules
// a later recovery publication until the consumer confirms the batch.
func (r *CampaignRepository) MarkCampaignBatchPublished(
	ctx context.Context,
	claim ClaimedCampaignBatch,
	recoveryAfter time.Duration,
) error {
	recoverySeconds := int64(recoveryAfter / time.Second)
	if recoverySeconds < 1 {
		recoverySeconds = 1
	}
	tag, err := r.pool.Exec(
		ctx,
		`UPDATE campaign_batches
		    SET publish_attempts = publish_attempts + 1,
		        last_published_at = now(),
		        next_publish_at = now() + ($1 * interval '1 second'),
		        last_error = NULL,
		        publish_lease_token = NULL,
		        publish_lease_until = NULL,
		        updated_at = now()
		  WHERE campaign_id = $2
		    AND batch_index = $3
		    AND publish_lease_token = $4
		    AND processed_at IS NULL`,
		recoverySeconds,
		claim.CampaignID,
		claim.BatchIndex,
		claim.LeaseToken,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrCampaignBatchLeaseLost
	}
	return nil
}

// MarkCampaignBatchPublishFailed releases a failed publish lease with durable
// error/backoff state so another worker can recover it.
func (r *CampaignRepository) MarkCampaignBatchPublishFailed(
	ctx context.Context,
	claim ClaimedCampaignBatch,
	retryAfter time.Duration,
	lastError string,
) error {
	retrySeconds := int64(retryAfter / time.Second)
	if retrySeconds < 1 {
		retrySeconds = 1
	}
	if len(lastError) > 500 {
		lastError = lastError[:500]
	}
	tag, err := r.pool.Exec(
		ctx,
		`UPDATE campaign_batches
		    SET publish_attempts = publish_attempts + 1,
		        next_publish_at = now() + ($1 * interval '1 second'),
		        last_error = $2,
		        publish_lease_token = NULL,
		        publish_lease_until = NULL,
		        updated_at = now()
		  WHERE campaign_id = $3
		    AND batch_index = $4
		    AND publish_lease_token = $5
		    AND processed_at IS NULL`,
		retrySeconds,
		lastError,
		claim.CampaignID,
		claim.BatchIndex,
		claim.LeaseToken,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrCampaignBatchLeaseLost
	}
	return nil
}

// ValidateCampaignBatch verifies that a broker message is the exact payload
// persisted by Start. It also reports idempotent re-deliveries.
func (r *CampaignRepository) ValidateCampaignBatch(
	ctx context.Context,
	campaignID uuid.UUID,
	workspaceID uuid.UUID,
	batchIndex int,
	payload []byte,
) (bool, error) {
	var storedHash []byte
	var processed bool
	err := r.pool.QueryRow(
		ctx,
		`SELECT payload_hash, processed_at IS NOT NULL
		   FROM campaign_batches
		  WHERE campaign_id = $1
		    AND workspace_id = $2
		    AND batch_index = $3`,
		campaignID,
		workspaceID,
		batchIndex,
	).Scan(&storedHash, &processed)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, ErrCampaignBatchNotFound
		}
		return false, err
	}
	payloadHash := sha256.Sum256(payload)
	if subtle.ConstantTimeCompare(storedHash, payloadHash[:]) != 1 {
		return false, ErrCampaignBatchPayloadMismatch
	}
	return processed, nil
}

// MarkCampaignBatchProcessed confirms one batch and atomically completes the
// campaign only when every durable batch has been confirmed.
func (r *CampaignRepository) MarkCampaignBatchProcessed(
	ctx context.Context,
	campaignID uuid.UUID,
	workspaceID uuid.UUID,
	batchIndex int,
	payload []byte,
) (bool, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var status domain.CampaignStatus
	err = tx.QueryRow(
		ctx,
		`SELECT status
		   FROM campaigns
		  WHERE id = $1 AND workspace_id = $2
		  FOR UPDATE`,
		campaignID,
		workspaceID,
	).Scan(&status)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, ErrCampaignNotFound
		}
		return false, err
	}

	payloadHash := sha256.Sum256(payload)
	var delaySeconds int
	err = tx.QueryRow(
		ctx,
		`UPDATE campaign_batches
		    SET processed_at = COALESCE(processed_at, now()),
		        publish_lease_token = NULL,
		        publish_lease_until = NULL,
		        last_error = NULL,
		        updated_at = now()
		  WHERE campaign_id = $1
		    AND workspace_id = $2
		    AND batch_index = $3
		    AND payload_hash = $4
		 RETURNING delay_seconds`,
		campaignID,
		workspaceID,
		batchIndex,
		payloadHash[:],
	).Scan(&delaySeconds)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, ErrCampaignBatchPayloadMismatch
		}
		return false, err
	}

	completed := status == domain.CampaignStatusCompleted
	if status == domain.CampaignStatusSending {
		var pending bool
		if err := tx.QueryRow(
			ctx,
			`SELECT EXISTS (
			     SELECT 1
			       FROM campaign_batches
			      WHERE campaign_id = $1
			        AND processed_at IS NULL
			 )`,
			campaignID,
		).Scan(&pending); err != nil {
			return false, err
		}
		if !pending {
			tag, err := tx.Exec(
				ctx,
				`UPDATE campaigns
				    SET status = $1, updated_at = now()
				  WHERE id = $2
				    AND workspace_id = $3
				    AND status = $4`,
				domain.CampaignStatusCompleted,
				campaignID,
				workspaceID,
				domain.CampaignStatusSending,
			)
			if err != nil {
				return false, err
			}
			completed = tag.RowsAffected() == 1
		} else {
			if _, err := tx.Exec(
				ctx,
				`UPDATE campaign_batches
					    SET next_publish_at = LEAST(
					            next_publish_at,
					            now() + ($1 * interval '1 second')
					        ),
					        updated_at = now()
					  WHERE campaign_id = $2
					    AND workspace_id = $3
					    AND processed_at IS NULL
					    AND batch_index = (
					        SELECT min(next_batch.batch_index)
					          FROM campaign_batches next_batch
					         WHERE next_batch.campaign_id = $2
					           AND next_batch.workspace_id = $3
					           AND next_batch.processed_at IS NULL
					    )`,
				delaySeconds,
				campaignID,
				workspaceID,
			); err != nil {
				return false, err
			}
		}
	} else if status != domain.CampaignStatusCancelled && status != domain.CampaignStatusCompleted {
		return false, fmt.Errorf("%w: %s", ErrCampaignInvalidState, status)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return completed, nil
}
