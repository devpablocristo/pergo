-- +goose Up
CREATE TABLE admin_sessions (
    session_id TEXT PRIMARY KEY
        CHECK (session_id ~ '^[0-9a-f]{64}$'),
    expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (expires_at > created_at)
);

CREATE INDEX admin_sessions_active_expiry_idx
    ON admin_sessions (expires_at)
    WHERE revoked_at IS NULL;

-- +goose Down
DROP TABLE IF EXISTS admin_sessions;
