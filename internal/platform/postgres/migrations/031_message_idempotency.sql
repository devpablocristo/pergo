-- +goose Up
CREATE TABLE message_idempotency (
    workspace_id UUID NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    idempotency_key TEXT NOT NULL,
    payload_hash CHAR(64) NOT NULL,
    message_id UUID NOT NULL,
    trace_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    queued_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_token UUID,
    lease_expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (workspace_id, idempotency_key),
    UNIQUE (workspace_id, message_id),
    CHECK (char_length(idempotency_key) BETWEEN 1 AND 255),
    CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
    CHECK (char_length(trace_id) BETWEEN 1 AND 255),
    CHECK (status IN ('pending', 'processing', 'accepted')),
    CHECK (
        (status = 'processing' AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL)
        OR
        (status <> 'processing' AND lease_token IS NULL AND lease_expires_at IS NULL)
    )
);

CREATE INDEX message_idempotency_processing_idx
    ON message_idempotency (lease_expires_at)
    WHERE status = 'processing';

-- +goose Down
DROP TABLE message_idempotency;
