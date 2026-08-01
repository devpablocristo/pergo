-- +goose Up
CREATE TABLE message_ingress_ledger (
    workspace_id UUID NOT NULL REFERENCES workspaces(id) ON DELETE CASCADE,
    idempotency_key TEXT NOT NULL CHECK (length(idempotency_key) BETWEEN 1 AND 255),
    payload_hash BYTEA NOT NULL CHECK (octet_length(payload_hash) = 32),
    trace_id TEXT NOT NULL UNIQUE CHECK (length(trace_id) BETWEEN 1 AND 255),
    receipt_id UUID NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('claimed', 'queued')),
    claim_token UUID,
    claim_generation BIGINT NOT NULL DEFAULT 1 CHECK (claim_generation > 0),
    claim_expires_at TIMESTAMPTZ,
    queued_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (workspace_id, idempotency_key),
    UNIQUE (workspace_id, receipt_id),
    CONSTRAINT message_ingress_claim_shape CHECK (
        (
            state = 'claimed'
            AND claim_token IS NOT NULL
            AND claim_expires_at IS NOT NULL
            AND queued_at IS NULL
        )
        OR
        (
            state = 'queued'
            AND claim_token IS NULL
            AND claim_expires_at IS NULL
            AND queued_at IS NOT NULL
        )
    )
);

CREATE INDEX idx_message_ingress_claim_expiry
    ON message_ingress_ledger (claim_expires_at)
    WHERE state = 'claimed';

ALTER TABLE message_dispatches
    ADD COLUMN receipt_id UUID;

CREATE UNIQUE INDEX idx_message_dispatches_workspace_receipt
    ON message_dispatches (workspace_id, receipt_id)
    WHERE receipt_id IS NOT NULL;

ALTER TABLE message_dispatches
    DROP CONSTRAINT IF EXISTS message_dispatches_provider_message_id_key;

CREATE UNIQUE INDEX idx_message_dispatches_workspace_provider_message
    ON message_dispatches (workspace_id, provider_message_id)
    WHERE provider_message_id IS NOT NULL;

-- +goose Down
DROP INDEX IF EXISTS idx_message_dispatches_workspace_provider_message;
ALTER TABLE message_dispatches
    ADD CONSTRAINT message_dispatches_provider_message_id_key
    UNIQUE (provider_message_id);
DROP INDEX IF EXISTS idx_message_dispatches_workspace_receipt;
ALTER TABLE message_dispatches
    DROP COLUMN IF EXISTS receipt_id;
DROP TABLE IF EXISTS message_ingress_ledger;
