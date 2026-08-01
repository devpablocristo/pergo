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

// ErrDispatchNotFound is returned when a message dispatch record cannot be found.
var ErrDispatchNotFound = errors.New("dispatch not found")

// ErrDispatchReceiptConflict is returned when a trace is rebound to a
// different durable ingress receipt.
var ErrDispatchReceiptConflict = errors.New("dispatch receipt conflict")

var (
	// ErrDispatchClaimActive means another worker owns an unexpired provider
	// delivery lease.
	ErrDispatchClaimActive = errors.New("dispatch claim is active")
	// ErrDispatchClaimLost means a worker attempted to mutate a delivery after
	// its token or fencing generation stopped being current.
	ErrDispatchClaimLost = errors.New("dispatch claim was lost")
	// ErrDispatchTerminal means the dispatch already reached an outcome that
	// must never call the provider again.
	ErrDispatchTerminal = errors.New("dispatch is terminal")
	// ErrDispatchDeliveryUncertain means a worker disappeared after persisting
	// "sending". Re-dispatch is intentionally blocked because the provider may
	// already have accepted the request.
	ErrDispatchDeliveryUncertain = errors.New("dispatch delivery outcome is uncertain")
)

const defaultDispatchDeliveryLease = 2 * time.Minute

// DispatchClaim is a fenced, expiring right to perform one provider delivery.
// No database transaction remains open while the provider is called.
type DispatchClaim struct {
	Token      uuid.UUID
	Generation int64
	ExpiresAt  time.Time
}

// MessageDispatch represents a row in the message_dispatches table.
type MessageDispatch struct {
	ID                uuid.UUID
	WorkspaceID       uuid.UUID
	TraceID           string
	CurrentChannel    string
	Status            string
	FallbackIndex     int
	ErrorMessage      *string
	CampaignID        *uuid.UUID
	TemplateName      *string
	VariablesJSON     map[string]string
	CreatedAt         time.Time
	UpdatedAt         time.Time
	ProviderMessageID *string
	ReceiptID         *uuid.UUID
}

// MessageDispatchRepository manages message dispatch state in the database.
type MessageDispatchRepository struct {
	pool *pgxpool.Pool
}

// NewMessageDispatchRepository creates a new MessageDispatchRepository.
func NewMessageDispatchRepository(pool *pgxpool.Pool) *MessageDispatchRepository {
	return &MessageDispatchRepository{pool: pool}
}

// GetOrCreateDispatch retrieves an existing dispatch by trace_id or inserts a new one if it doesn't exist.
func (r *MessageDispatchRepository) GetOrCreateDispatch(
	ctx context.Context,
	workspaceID uuid.UUID,
	traceID string,
	initialChannel string,
	campaignID *uuid.UUID,
	templateName *string,
	variablesJSON map[string]string,
) (*MessageDispatch, error) {
	var varsJSON []byte
	var err error
	if variablesJSON != nil {
		varsJSON, err = json.Marshal(variablesJSON)
		if err != nil {
			return nil, err
		}
	}

	var d MessageDispatch
	var varsRaw []byte
	err = r.pool.QueryRow(ctx,
		`INSERT INTO message_dispatches (workspace_id, trace_id, current_channel, status, fallback_index, campaign_id, template_name, variables_json)
		 VALUES ($1, $2, $3, 'queued', 0, $4, $5, $6)
		 ON CONFLICT (trace_id) DO UPDATE 
		 SET trace_id = EXCLUDED.trace_id -- dummy update to return existing row
		 RETURNING id, workspace_id, trace_id, current_channel, status, fallback_index, error_message, campaign_id, template_name, variables_json, created_at, updated_at, provider_message_id, receipt_id`,
		workspaceID, traceID, initialChannel, campaignID, templateName, varsJSON,
	).Scan(&d.ID, &d.WorkspaceID, &d.TraceID, &d.CurrentChannel, &d.Status, &d.FallbackIndex, &d.ErrorMessage, &d.CampaignID, &d.TemplateName, &varsRaw, &d.CreatedAt, &d.UpdatedAt, &d.ProviderMessageID, &d.ReceiptID)
	if err != nil {
		return nil, err
	}

	if len(varsRaw) > 0 {
		if err := json.Unmarshal(varsRaw, &d.VariablesJSON); err != nil {
			return nil, err
		}
	}

	return &d, nil
}

// UpdateDispatchStatus updates the state of an existing message dispatch.
func (r *MessageDispatchRepository) UpdateDispatchStatus(ctx context.Context, id uuid.UUID, status string, currentChannel string, fallbackIndex int, errMsg *string) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE message_dispatches
		 SET status = $1, current_channel = $2, fallback_index = $3, error_message = $4, updated_at = now()
		 WHERE id = $5`,
		status, currentChannel, fallbackIndex, errMsg, id,
	)
	return err
}

// ClaimDelivery serializes provider dispatch across every PerGo replica.
//
// An expired queued/failed_transient claim is safe to recover because no
// provider call was declared in progress. An expired sending claim is instead
// moved to uncertain: without provider-side idempotency, retrying it could send
// the same message twice.
func (r *MessageDispatchRepository) ClaimDelivery(
	ctx context.Context,
	id uuid.UUID,
	lease time.Duration,
) (DispatchClaim, time.Duration, error) {
	if lease <= 0 {
		lease = defaultDispatchDeliveryLease
	}

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return DispatchClaim{}, 0, fmt.Errorf("begin dispatch claim: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	var (
		status       string
		currentToken *uuid.UUID
		generation   int64
		expiresAt    *time.Time
		databaseNow  time.Time
	)
	err = tx.QueryRow(ctx, `
		SELECT
			status,
			delivery_claim_token,
			delivery_claim_generation,
			delivery_claim_expires_at,
			clock_timestamp()
		FROM message_dispatches
		WHERE id = $1
		FOR UPDATE
	`, id).Scan(&status, &currentToken, &generation, &expiresAt, &databaseNow)
	if errors.Is(err, pgx.ErrNoRows) {
		return DispatchClaim{}, 0, ErrDispatchNotFound
	}
	if err != nil {
		return DispatchClaim{}, 0, fmt.Errorf("lock dispatch claim: %w", err)
	}

	if isTerminalDispatchStatus(status) {
		return DispatchClaim{}, 0, ErrDispatchTerminal
	}
	if currentToken != nil && expiresAt != nil && expiresAt.After(databaseNow) {
		retryAfter := expiresAt.Sub(databaseNow)
		if retryAfter < time.Millisecond {
			retryAfter = time.Millisecond
		}
		return DispatchClaim{}, retryAfter, ErrDispatchClaimActive
	}

	if status == "sending" {
		const uncertainMessage = "DELIVERY_UNCERTAIN"
		if _, err := tx.Exec(ctx, `
			UPDATE message_dispatches
			SET status = 'uncertain',
			    error_message = $1,
			    delivery_claim_token = NULL,
			    delivery_claim_expires_at = NULL,
			    updated_at = clock_timestamp()
			WHERE id = $2
		`, uncertainMessage, id); err != nil {
			return DispatchClaim{}, 0, fmt.Errorf("mark expired dispatch uncertain: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return DispatchClaim{}, 0, fmt.Errorf("commit uncertain dispatch: %w", err)
		}
		return DispatchClaim{}, 0, ErrDispatchDeliveryUncertain
	}
	if status != "queued" && status != "failed_transient" {
		return DispatchClaim{}, 0, fmt.Errorf("dispatch status %q cannot be claimed", status)
	}

	claim := DispatchClaim{
		Token:      uuid.New(),
		Generation: generation + 1,
	}
	err = tx.QueryRow(ctx, `
		UPDATE message_dispatches
		SET delivery_claim_token = $1,
		    delivery_claim_generation = $2,
		    delivery_claim_expires_at = clock_timestamp() + make_interval(secs => $3),
		    updated_at = clock_timestamp()
		WHERE id = $4
		RETURNING delivery_claim_expires_at
	`, claim.Token, claim.Generation, lease.Seconds(), id).Scan(&claim.ExpiresAt)
	if err != nil {
		return DispatchClaim{}, 0, fmt.Errorf("acquire dispatch claim: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return DispatchClaim{}, 0, fmt.Errorf("commit dispatch claim: %w", err)
	}
	claim.ExpiresAt = claim.ExpiresAt.UTC()
	return claim, 0, nil
}

// UpdateClaimedDelivery changes provider delivery state only for the current
// fenced owner. release=true clears the lease atomically with the new state.
// A repeated terminal completion is accepted when a prior database response
// was lost but the exact outcome was already persisted.
func (r *MessageDispatchRepository) UpdateClaimedDelivery(
	ctx context.Context,
	id uuid.UUID,
	claim DispatchClaim,
	status string,
	currentChannel string,
	fallbackIndex int,
	errMsg *string,
	providerMessageID *string,
	release bool,
) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE message_dispatches
		SET status = $1,
		    current_channel = $2,
		    fallback_index = $3,
		    error_message = $4,
		    provider_message_id = COALESCE($5, provider_message_id),
		    delivery_claim_token = CASE WHEN $6 THEN NULL ELSE delivery_claim_token END,
		    delivery_claim_expires_at = CASE WHEN $6 THEN NULL ELSE delivery_claim_expires_at END,
		    updated_at = clock_timestamp()
		WHERE id = $7
		  AND delivery_claim_token = $8
		  AND delivery_claim_generation = $9
		  AND delivery_claim_expires_at > clock_timestamp()
	`, status, currentChannel, fallbackIndex, errMsg, providerMessageID, release, id, claim.Token, claim.Generation)
	if err != nil {
		return fmt.Errorf("update claimed dispatch: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}

	if release && isTerminalDispatchStatus(status) {
		var (
			storedStatus     string
			storedChannel    string
			storedIndex      int
			storedProviderID *string
			storedToken      *uuid.UUID
		)
		err := r.pool.QueryRow(ctx, `
			SELECT
				status,
				current_channel,
				fallback_index,
				provider_message_id,
				delivery_claim_token
			FROM message_dispatches
			WHERE id = $1
		`, id).Scan(&storedStatus, &storedChannel, &storedIndex, &storedProviderID, &storedToken)
		if err == nil &&
			storedToken == nil &&
			storedStatus == status &&
			storedChannel == currentChannel &&
			storedIndex == fallbackIndex &&
			sameOptionalString(storedProviderID, providerMessageID) {
			return nil
		}
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("verify claimed dispatch completion: %w", err)
		}
	}
	return ErrDispatchClaimLost
}

// RenewDeliveryClaim extends an active fenced claim before another provider
// attempt. It performs one bounded UPDATE and never spans the network call.
func (r *MessageDispatchRepository) RenewDeliveryClaim(
	ctx context.Context,
	id uuid.UUID,
	claim DispatchClaim,
	lease time.Duration,
) (DispatchClaim, error) {
	if lease <= 0 {
		lease = defaultDispatchDeliveryLease
	}
	err := r.pool.QueryRow(ctx, `
		UPDATE message_dispatches
		SET delivery_claim_expires_at = clock_timestamp() + make_interval(secs => $1),
		    updated_at = clock_timestamp()
		WHERE id = $2
		  AND delivery_claim_token = $3
		  AND delivery_claim_generation = $4
		  AND delivery_claim_expires_at > clock_timestamp()
		RETURNING delivery_claim_expires_at
	`, lease.Seconds(), id, claim.Token, claim.Generation).Scan(&claim.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return DispatchClaim{}, ErrDispatchClaimLost
	}
	if err != nil {
		return DispatchClaim{}, fmt.Errorf("renew dispatch claim: %w", err)
	}
	claim.ExpiresAt = claim.ExpiresAt.UTC()
	return claim, nil
}

func isTerminalDispatchStatus(status string) bool {
	switch status {
	case "sent", "delivered", "read", "failed", "uncertain":
		return true
	default:
		return false
	}
}

func sameOptionalString(stored *string, desired *string) bool {
	if desired == nil {
		return true
	}
	return stored != nil && *stored == *desired
}

// GetByTraceID retrieves a message dispatch record by trace_id.
func (r *MessageDispatchRepository) GetByTraceID(ctx context.Context, traceID string) (*MessageDispatch, error) {
	var d MessageDispatch
	var varsRaw []byte
	err := r.pool.QueryRow(ctx,
		`SELECT id, workspace_id, trace_id, current_channel, status, fallback_index, error_message, campaign_id, template_name, variables_json, created_at, updated_at, provider_message_id, receipt_id
		 FROM message_dispatches WHERE trace_id = $1`,
		traceID,
	).Scan(&d.ID, &d.WorkspaceID, &d.TraceID, &d.CurrentChannel, &d.Status, &d.FallbackIndex, &d.ErrorMessage, &d.CampaignID, &d.TemplateName, &varsRaw, &d.CreatedAt, &d.UpdatedAt, &d.ProviderMessageID, &d.ReceiptID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrDispatchNotFound
		}
		return nil, err
	}

	if len(varsRaw) > 0 {
		if err := json.Unmarshal(varsRaw, &d.VariablesJSON); err != nil {
			return nil, err
		}
	}

	return &d, nil
}

// UpdateProviderMessageID associates an external provider message ID (e.g. wamid) with a dispatch record.
func (r *MessageDispatchRepository) UpdateProviderMessageID(ctx context.Context, id uuid.UUID, providerMessageID string) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE message_dispatches 
		 SET provider_message_id = $1, updated_at = now()
		 WHERE id = $2`,
		providerMessageID, id,
	)
	return err
}

// GetByProviderMessageID retrieves a message dispatch by its workspace-scoped
// external provider message ID.
func (r *MessageDispatchRepository) GetByProviderMessageID(ctx context.Context, workspaceID uuid.UUID, providerMessageID string) (*MessageDispatch, error) {
	var d MessageDispatch
	var varsRaw []byte
	err := r.pool.QueryRow(ctx,
		`SELECT id, workspace_id, trace_id, current_channel, status, fallback_index, error_message, campaign_id, template_name, variables_json, created_at, updated_at, provider_message_id, receipt_id
		 FROM message_dispatches 
		 WHERE workspace_id = $1
		   AND provider_message_id = $2`,
		workspaceID, providerMessageID,
	).Scan(&d.ID, &d.WorkspaceID, &d.TraceID, &d.CurrentChannel, &d.Status, &d.FallbackIndex, &d.ErrorMessage, &d.CampaignID, &d.TemplateName, &varsRaw, &d.CreatedAt, &d.UpdatedAt, &d.ProviderMessageID, &d.ReceiptID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrDispatchNotFound
		}
		return nil, err
	}

	if len(varsRaw) > 0 {
		if err := json.Unmarshal(varsRaw, &d.VariablesJSON); err != nil {
			return nil, err
		}
	}
	return &d, nil
}

// BindReceipt attaches the stable ingress receipt to a dispatch. Rebinding a
// trace to another receipt is rejected.
func (r *MessageDispatchRepository) BindReceipt(ctx context.Context, id uuid.UUID, receiptID uuid.UUID) error {
	tag, err := r.pool.Exec(ctx,
		`UPDATE message_dispatches
		 SET receipt_id = $1, updated_at = now()
		 WHERE id = $2
		   AND (receipt_id IS NULL OR receipt_id = $1)`,
		receiptID, id,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrDispatchReceiptConflict
	}
	return nil
}

// UpdateStatusByProviderMessageID updates status of a message dispatch matched by its provider_message_id or id.
func (r *MessageDispatchRepository) UpdateStatusByProviderMessageID(ctx context.Context, providerMsgID string, status string) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE message_dispatches
		 SET status = $1, updated_at = now()
		 WHERE provider_message_id = $2 OR id::text = $2`,
		status, providerMsgID,
	)
	return err
}
