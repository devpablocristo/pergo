package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pablojhp.pergo/internal/domain"
)

var (
	ErrCampaignNotFound        = errors.New("campaign not found")
	ErrCampaignDelayOutOfRange = errors.New("campaign delay is out of range")
)

type CampaignRepository struct {
	pool *pgxpool.Pool
}

func NewCampaignRepository(pool *pgxpool.Pool) *CampaignRepository {
	return &CampaignRepository{pool: pool}
}

func (r *CampaignRepository) Create(ctx context.Context, c *domain.Campaign) (*domain.Campaign, error) {
	if c.DelaySeconds < 0 || c.DelaySeconds > domain.CampaignMaxDelaySeconds {
		return nil, ErrCampaignDelayOutOfRange
	}
	recipientsJSON, err := json.Marshal(c.Recipients)
	if err != nil {
		return nil, err
	}
	skippedJSON, err := json.Marshal(c.SkippedRows)
	if err != nil {
		return nil, err
	}

	var dbCampaign domain.Campaign
	err = r.pool.QueryRow(ctx,
		`INSERT INTO campaigns (workspace_id, connection_id, name, status, batch_size, delay_seconds, template_name, template_language, channel, recipients, skipped_rows, scheduled_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		 RETURNING id, workspace_id, connection_id, name, status, batch_size, delay_seconds, template_name, template_language, channel, recipients, skipped_rows, scheduled_at, created_at, updated_at`,
		c.WorkspaceID, c.ConnectionID, c.Name, c.Status, c.BatchSize, c.DelaySeconds, c.TemplateName, c.TemplateLanguage, c.Channel, recipientsJSON, skippedJSON, c.ScheduledAt,
	).Scan(
		&dbCampaign.ID, &dbCampaign.WorkspaceID, &dbCampaign.ConnectionID, &dbCampaign.Name, &dbCampaign.Status,
		&dbCampaign.BatchSize, &dbCampaign.DelaySeconds, &dbCampaign.TemplateName, &dbCampaign.TemplateLanguage, &dbCampaign.Channel,
		&recipientsJSON, &skippedJSON, &dbCampaign.ScheduledAt, &dbCampaign.CreatedAt, &dbCampaign.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(recipientsJSON, &dbCampaign.Recipients); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(skippedJSON, &dbCampaign.SkippedRows); err != nil {
		return nil, err
	}

	return &dbCampaign, nil
}

func (r *CampaignRepository) GetByID(ctx context.Context, id uuid.UUID) (*domain.Campaign, error) {
	var c domain.Campaign
	var recipientsJSON, skippedJSON []byte

	err := r.pool.QueryRow(ctx,
		`SELECT id, workspace_id, connection_id, name, status, batch_size, delay_seconds, template_name, template_language, channel, recipients, skipped_rows, scheduled_at, created_at, updated_at
		 FROM campaigns WHERE id = $1`,
		id,
	).Scan(
		&c.ID, &c.WorkspaceID, &c.ConnectionID, &c.Name, &c.Status,
		&c.BatchSize, &c.DelaySeconds, &c.TemplateName, &c.TemplateLanguage, &c.Channel,
		&recipientsJSON, &skippedJSON, &c.ScheduledAt, &c.CreatedAt, &c.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrCampaignNotFound
		}
		return nil, err
	}

	if err := json.Unmarshal(recipientsJSON, &c.Recipients); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(skippedJSON, &c.SkippedRows); err != nil {
		return nil, err
	}

	return &c, nil
}

// CancelForWorkspace atomically transitions only active campaigns to the
// cancelled terminal state. The status predicate is evaluated while PostgreSQL
// holds the row lock, so a concurrent completion can never be regressed.
func (r *CampaignRepository) CancelForWorkspace(
	ctx context.Context,
	id uuid.UUID,
	workspaceID uuid.UUID,
) error {
	tag, err := r.pool.Exec(
		ctx,
		`UPDATE campaigns
		    SET status = $1, updated_at = now()
		  WHERE id = $2
		    AND workspace_id = $3
		    AND status IN ($4, $5)`,
		domain.CampaignStatusCancelled,
		id,
		workspaceID,
		domain.CampaignStatusScheduled,
		domain.CampaignStatusSending,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		return nil
	}

	var exists bool
	if err := r.pool.QueryRow(
		ctx,
		`SELECT EXISTS (
		     SELECT 1 FROM campaigns WHERE id = $1 AND workspace_id = $2
		 )`,
		id,
		workspaceID,
	).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return ErrCampaignNotFound
	}
	return ErrCampaignInvalidState
}

func (r *CampaignRepository) UpdateRecipients(ctx context.Context, id uuid.UUID, recipients []domain.CampaignRecipient, skipped []domain.SkippedRow) error {
	recipientsJSON, err := json.Marshal(recipients)
	if err != nil {
		return err
	}
	skippedJSON, err := json.Marshal(skipped)
	if err != nil {
		return err
	}

	tag, err := r.pool.Exec(ctx,
		`UPDATE campaigns
		    SET recipients = $1, skipped_rows = $2, updated_at = now()
		  WHERE id = $3 AND status = $4`,
		recipientsJSON, skippedJSON, id,
		domain.CampaignStatusDraft,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrCampaignInvalidState
	}
	return nil
}

func (r *CampaignRepository) ListByWorkspace(ctx context.Context, workspaceID uuid.UUID) ([]domain.Campaign, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, workspace_id, connection_id, name, status, batch_size, delay_seconds, template_name, template_language, channel, recipients, skipped_rows, scheduled_at, created_at, updated_at
		 FROM campaigns WHERE workspace_id = $1 ORDER BY created_at DESC`,
		workspaceID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var campaigns []domain.Campaign
	for rows.Next() {
		var c domain.Campaign
		var recipientsJSON, skippedJSON []byte
		err := rows.Scan(
			&c.ID, &c.WorkspaceID, &c.ConnectionID, &c.Name, &c.Status,
			&c.BatchSize, &c.DelaySeconds, &c.TemplateName, &c.TemplateLanguage, &c.Channel,
			&recipientsJSON, &skippedJSON, &c.ScheduledAt, &c.CreatedAt, &c.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(recipientsJSON, &c.Recipients); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(skippedJSON, &c.SkippedRows); err != nil {
			return nil, err
		}
		campaigns = append(campaigns, c)
	}

	return campaigns, rows.Err()
}

func (r *CampaignRepository) Delete(ctx context.Context, id, workspaceID uuid.UUID) error {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var status domain.CampaignStatus
	err = tx.QueryRow(
		ctx,
		`SELECT status
		   FROM campaigns
		  WHERE id = $1 AND workspace_id = $2
		  FOR UPDATE`,
		id,
		workspaceID,
	).Scan(&status)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrCampaignNotFound
		}
		return err
	}

	switch status {
	case domain.CampaignStatusDraft,
		domain.CampaignStatusCompleted,
		domain.CampaignStatusCancelled:
		// Draft campaigns have never produced outbound work; completed and
		// cancelled campaigns are terminal and safe to remove.
	case domain.CampaignStatusScheduled, domain.CampaignStatusSending:
		return fmt.Errorf("%w: %s", ErrCampaignInvalidState, status)
	default:
		return fmt.Errorf("%w: %s", ErrCampaignInvalidState, status)
	}

	tag, err := tx.Exec(
		ctx,
		`DELETE FROM campaigns WHERE id = $1 AND workspace_id = $2`,
		id,
		workspaceID,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrCampaignNotFound
	}
	return tx.Commit(ctx)
}
