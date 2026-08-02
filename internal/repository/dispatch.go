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

type ProviderDeliveryOutboxEvent struct {
	ID          uuid.UUID
	WorkspaceID uuid.UUID
	DispatchID  uuid.UUID
	Status      string
	EventKey    string
	Payload     []byte
	PublishedAt *time.Time
	CreatedAt   time.Time
}

// MessageDispatchRepository manages message dispatch state in the database.
type MessageDispatchRepository struct {
	pool *pgxpool.Pool
}

// NewMessageDispatchRepository creates a new MessageDispatchRepository.
func NewMessageDispatchRepository(pool *pgxpool.Pool) *MessageDispatchRepository {
	return &MessageDispatchRepository{pool: pool}
}

// GetOrCreateDispatch retrieves an existing workspace dispatch by trace_id or
// inserts a new one if it doesn't exist.
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
		 ON CONFLICT (workspace_id, trace_id) DO UPDATE
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

// AdvanceProviderDeliveryStatus applies Meta delivery receipts monotonically.
// Duplicate and stale callbacks are successful no-ops; concurrent callbacks
// cannot regress read to delivered/sent or resurrect a failed dispatch.
func (r *MessageDispatchRepository) AdvanceProviderDeliveryStatus(
	ctx context.Context,
	id uuid.UUID,
	status string,
) (bool, error) {
	switch status {
	case "sent", "delivered", "read", "failed":
	default:
		return false, fmt.Errorf("unsupported provider delivery status %q", status)
	}

	tag, err := r.pool.Exec(ctx, `
		UPDATE message_dispatches
		SET status = $1,
		    error_message = NULL,
		    updated_at = clock_timestamp()
		WHERE id = $2
		  AND (
		        (status IN ('queued', 'sending') AND $1 IN ('sent', 'delivered', 'read', 'failed'))
		     OR (status = 'sent' AND $1 IN ('delivered', 'read', 'failed'))
		     OR (status = 'delivered' AND $1 = 'read')
		  )
	`, status, id)
	if err != nil {
		return false, fmt.Errorf("advance provider delivery status: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return true, nil
	}

	var exists bool
	if err := r.pool.QueryRow(ctx, `SELECT TRUE FROM message_dispatches WHERE id = $1`, id).Scan(&exists); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, ErrDispatchNotFound
		}
		return false, fmt.Errorf("verify provider delivery status: %w", err)
	}
	return false, nil
}

// RecordProviderDeliveryReceipt atomically advances the dispatch and creates
// its canonical webhook outbox event. Retries return the same pending event;
// stale callbacks that never represented a transition return nil.
func (r *MessageDispatchRepository) RecordProviderDeliveryReceipt(
	ctx context.Context,
	workspaceID uuid.UUID,
	id uuid.UUID,
	status string,
	eventKey string,
	payload []byte,
) (*ProviderDeliveryOutboxEvent, error) {
	if !isProviderDeliveryStatus(status) {
		return nil, fmt.Errorf("unsupported provider delivery status %q", status)
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin provider receipt: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var current string
	err = tx.QueryRow(ctx, `
		SELECT status
		FROM message_dispatches
		WHERE id = $1
		  AND workspace_id = $2
		FOR UPDATE
	`, id, workspaceID).Scan(&current)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrDispatchNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("lock provider receipt dispatch: %w", err)
	}

	if providerDeliveryTransitionAllowed(current, status) {
		if _, err := tx.Exec(ctx, `
			UPDATE message_dispatches
			SET status = $1,
			    error_message = NULL,
			    updated_at = clock_timestamp()
			WHERE id = $2
			  AND workspace_id = $3
		`, status, id, workspaceID); err != nil {
			return nil, fmt.Errorf("advance provider receipt: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO provider_delivery_outbox (
				workspace_id, dispatch_id, status, event_key, payload
			)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (dispatch_id, status) DO NOTHING
		`, workspaceID, id, status, eventKey, payload); err != nil {
			return nil, fmt.Errorf("insert provider receipt outbox: %w", err)
		}
	}

	var event ProviderDeliveryOutboxEvent
	err = tx.QueryRow(ctx, `
		SELECT id, workspace_id, dispatch_id, status, event_key, payload,
		       published_at, created_at
		FROM provider_delivery_outbox
		WHERE dispatch_id = $1
		  AND status = $2
	`, id, status).Scan(
		&event.ID,
		&event.WorkspaceID,
		&event.DispatchID,
		&event.Status,
		&event.EventKey,
		&event.Payload,
		&event.PublishedAt,
		&event.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit stale provider receipt: %w", err)
		}
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read provider receipt outbox: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit provider receipt: %w", err)
	}
	event.CreatedAt = event.CreatedAt.UTC()
	if event.PublishedAt != nil {
		published := event.PublishedAt.UTC()
		event.PublishedAt = &published
	}
	return &event, nil
}

func (r *MessageDispatchRepository) MarkProviderDeliveryEventPublished(
	ctx context.Context,
	id uuid.UUID,
) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE provider_delivery_outbox
		SET published_at = COALESCE(published_at, clock_timestamp()),
		    updated_at = clock_timestamp()
		WHERE id = $1
	`, id)
	if err != nil {
		return fmt.Errorf("mark provider delivery event published: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return ErrDispatchNotFound
	}
	return nil
}

func (r *MessageDispatchRepository) ListPendingProviderDeliveryEvents(
	ctx context.Context,
	limit int,
) ([]ProviderDeliveryOutboxEvent, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := r.pool.Query(ctx, `
		SELECT id, workspace_id, dispatch_id, status, event_key, payload,
		       published_at, created_at
		FROM provider_delivery_outbox
		WHERE published_at IS NULL
		ORDER BY created_at, id
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("list provider delivery outbox: %w", err)
	}
	defer rows.Close()

	var events []ProviderDeliveryOutboxEvent
	for rows.Next() {
		var event ProviderDeliveryOutboxEvent
		if err := rows.Scan(
			&event.ID,
			&event.WorkspaceID,
			&event.DispatchID,
			&event.Status,
			&event.EventKey,
			&event.Payload,
			&event.PublishedAt,
			&event.CreatedAt,
		); err != nil {
			return nil, err
		}
		event.CreatedAt = event.CreatedAt.UTC()
		events = append(events, event)
	}
	return events, rows.Err()
}

// EnsureDispatchWebhookEvent durably creates a non-terminal event such as
// queued. A crash before broker publication is recovered by the relay.
func (r *MessageDispatchRepository) EnsureDispatchWebhookEvent(
	ctx context.Context,
	id uuid.UUID,
	event string,
	errMsg *string,
) error {
	if event != "queued" {
		return fmt.Errorf("unsupported explicit dispatch event %q", event)
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin dispatch event: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var (
		workspaceID    uuid.UUID
		traceID        string
		receiptID      *uuid.UUID
		currentChannel string
	)
	if err := tx.QueryRow(ctx, `
		SELECT workspace_id, trace_id, receipt_id, current_channel
		FROM message_dispatches
		WHERE id = $1
		FOR UPDATE
	`, id).Scan(&workspaceID, &traceID, &receiptID, &currentChannel); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrDispatchNotFound
		}
		return fmt.Errorf("lock dispatch event: %w", err)
	}
	if err := enqueueDispatchEventTx(
		ctx,
		tx,
		id,
		workspaceID,
		traceID,
		receiptID,
		event,
		currentChannel,
		errMsg,
	); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit dispatch event: %w", err)
	}
	return nil
}

func dispatchWebhookEventForStatus(status string) string {
	switch status {
	case "sent":
		return "sent"
	case "failed", "uncertain":
		return "failed"
	default:
		return ""
	}
}

func enqueueDispatchEventTx(
	ctx context.Context,
	tx pgx.Tx,
	dispatchID uuid.UUID,
	workspaceID uuid.UUID,
	traceID string,
	receiptID *uuid.UUID,
	event string,
	channel string,
	errMsg *string,
) error {
	messageID := dispatchID
	if receiptID != nil && *receiptID != uuid.Nil {
		messageID = *receiptID
	}
	payload := struct {
		Event       string `json:"event"`
		TraceID     string `json:"trace_id"`
		MessageID   string `json:"message_id"`
		Channel     string `json:"channel"`
		Timestamp   string `json:"timestamp"`
		WorkspaceID string `json:"workspace_id"`
		Error       string `json:"error,omitempty"`
	}{
		Event:       event,
		TraceID:     traceID,
		MessageID:   messageID.String(),
		Channel:     channel,
		Timestamp:   time.Now().UTC().Format(time.RFC3339),
		WorkspaceID: workspaceID.String(),
	}
	if errMsg != nil {
		payload.Error = *errMsg
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal dispatch webhook event: %w", err)
	}
	eventKey := traceID + ".delivery." + event
	if _, err := tx.Exec(ctx, `
		INSERT INTO provider_delivery_outbox (
			workspace_id, dispatch_id, status, event_key, payload
		)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (dispatch_id, status) DO NOTHING
	`, workspaceID, dispatchID, event, eventKey, data); err != nil {
		return fmt.Errorf("insert dispatch webhook outbox: %w", err)
	}
	return nil
}

func isProviderDeliveryStatus(status string) bool {
	switch status {
	case "sent", "delivered", "read", "failed":
		return true
	default:
		return false
	}
}

func providerDeliveryTransitionAllowed(current, next string) bool {
	switch current {
	case "queued", "sending":
		return isProviderDeliveryStatus(next)
	case "sent":
		return next == "delivered" || next == "read" || next == "failed"
	case "delivered":
		return next == "read"
	default:
		return false
	}
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
		status         string
		workspaceID    uuid.UUID
		traceID        string
		currentChannel string
		receiptID      *uuid.UUID
		currentToken   *uuid.UUID
		generation     int64
		expiresAt      *time.Time
		databaseNow    time.Time
	)
	err = tx.QueryRow(ctx, `
		SELECT
			status,
			workspace_id,
			trace_id,
			current_channel,
			receipt_id,
			delivery_claim_token,
			delivery_claim_generation,
			delivery_claim_expires_at,
			clock_timestamp()
		FROM message_dispatches
		WHERE id = $1
		FOR UPDATE
	`, id).Scan(
		&status,
		&workspaceID,
		&traceID,
		&currentChannel,
		&receiptID,
		&currentToken,
		&generation,
		&expiresAt,
		&databaseNow,
	)
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
		uncertainMessage := "DELIVERY_UNCERTAIN"
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
		if err := enqueueDispatchEventTx(
			ctx,
			tx,
			id,
			workspaceID,
			traceID,
			receiptID,
			"failed",
			currentChannel,
			&uncertainMessage,
		); err != nil {
			return DispatchClaim{}, 0, err
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
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin claimed dispatch update: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var (
		workspaceID uuid.UUID
		traceID     string
		receiptID   *uuid.UUID
	)
	err = tx.QueryRow(ctx, `
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
		RETURNING workspace_id, trace_id, receipt_id
	`, status, currentChannel, fallbackIndex, errMsg, providerMessageID, release, id, claim.Token, claim.Generation).Scan(
		&workspaceID,
		&traceID,
		&receiptID,
	)
	if err == nil {
		if event := dispatchWebhookEventForStatus(status); event != "" {
			if err := enqueueDispatchEventTx(
				ctx,
				tx,
				id,
				workspaceID,
				traceID,
				receiptID,
				event,
				currentChannel,
				errMsg,
			); err != nil {
				return err
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit claimed dispatch update: %w", err)
		}
		return nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("update claimed dispatch: %w", err)
	}
	_ = tx.Rollback(ctx)

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

// GetByTraceID retrieves a message dispatch by workspace-scoped trace ID.
func (r *MessageDispatchRepository) GetByTraceID(
	ctx context.Context,
	workspaceID uuid.UUID,
	traceID string,
) (*MessageDispatch, error) {
	var d MessageDispatch
	var varsRaw []byte
	err := r.pool.QueryRow(ctx,
		`SELECT id, workspace_id, trace_id, current_channel, status, fallback_index, error_message, campaign_id, template_name, variables_json, created_at, updated_at, provider_message_id, receipt_id
		 FROM message_dispatches
		 WHERE workspace_id = $1
		   AND trace_id = $2`,
		workspaceID, traceID,
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
