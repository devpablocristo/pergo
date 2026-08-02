package repository

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// AdminSessionRepository persists only one-way session identifiers. The signed
// browser cookie remains the sole holder of the random nonce.
type AdminSessionRepository struct {
	pool *pgxpool.Pool
}

func NewAdminSessionRepository(pool *pgxpool.Pool) *AdminSessionRepository {
	return &AdminSessionRepository{pool: pool}
}

func (r *AdminSessionRepository) CreateAdminSession(
	ctx context.Context,
	sessionID string,
	expiresAt time.Time,
) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO admin_sessions (session_id, expires_at)
		VALUES ($1, $2)
	`, sessionID, expiresAt)
	return err
}

func (r *AdminSessionRepository) IsAdminSessionActive(
	ctx context.Context,
	sessionID string,
	now time.Time,
) (bool, error) {
	var active bool
	err := r.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			  FROM admin_sessions
			 WHERE session_id = $1
			   AND revoked_at IS NULL
			   AND expires_at > $2
		)
	`, sessionID, now).Scan(&active)
	return active, err
}

func (r *AdminSessionRepository) RevokeAdminSession(
	ctx context.Context,
	sessionID string,
	now time.Time,
) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE admin_sessions
		   SET revoked_at = COALESCE(revoked_at, $2)
		 WHERE session_id = $1
	`, sessionID, now)
	return err
}
