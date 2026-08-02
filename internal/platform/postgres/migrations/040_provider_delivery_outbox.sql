-- +goose Up
CREATE TABLE provider_delivery_outbox (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id UUID NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    dispatch_id UUID NOT NULL REFERENCES message_dispatches(id) ON DELETE CASCADE,
    status TEXT NOT NULL CHECK (status IN ('queued', 'sent', 'delivered', 'read', 'failed')),
    event_key TEXT NOT NULL,
    payload JSONB NOT NULL,
    published_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (dispatch_id, status),
    UNIQUE (workspace_id, event_key)
);

CREATE INDEX provider_delivery_outbox_pending_idx
    ON provider_delivery_outbox (created_at, id)
    WHERE published_at IS NULL;

-- +goose Down
DROP TABLE IF EXISTS provider_delivery_outbox;
