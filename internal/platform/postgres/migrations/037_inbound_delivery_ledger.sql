-- +goose Up
ALTER TABLE inbound_dedups
    ADD COLUMN state TEXT NOT NULL DEFAULT 'published',
    ADD COLUMN trace_id TEXT NOT NULL DEFAULT gen_random_uuid()::TEXT,
    ADD COLUMN claim_token UUID,
    ADD COLUMN claim_generation BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN claim_expires_at TIMESTAMPTZ,
    ADD COLUMN published_at TIMESTAMPTZ DEFAULT clock_timestamp(),
    ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp();

ALTER TABLE inbound_dedups
    ADD CONSTRAINT inbound_dedups_state_valid
        CHECK (state IN ('claimed', 'published')),
    ADD CONSTRAINT inbound_dedups_generation_valid
        CHECK (claim_generation >= 0),
    ADD CONSTRAINT inbound_dedups_claim_shape
        CHECK (
            (
                state = 'claimed'
                AND claim_token IS NOT NULL
                AND claim_generation > 0
                AND claim_expires_at IS NOT NULL
                AND published_at IS NULL
            )
            OR
            (
                state = 'published'
                AND claim_token IS NULL
                AND claim_expires_at IS NULL
                AND published_at IS NOT NULL
            )
        ),
    ADD CONSTRAINT inbound_dedups_trace_unique UNIQUE (workspace_id, trace_id);

CREATE INDEX idx_inbound_dedups_claim_expiry
    ON inbound_dedups (claim_expires_at)
    WHERE state = 'claimed';

-- +goose Down
DROP INDEX IF EXISTS idx_inbound_dedups_claim_expiry;
ALTER TABLE inbound_dedups
    DROP CONSTRAINT IF EXISTS inbound_dedups_trace_unique,
    DROP CONSTRAINT IF EXISTS inbound_dedups_claim_shape,
    DROP CONSTRAINT IF EXISTS inbound_dedups_generation_valid,
    DROP CONSTRAINT IF EXISTS inbound_dedups_state_valid,
    DROP COLUMN IF EXISTS updated_at,
    DROP COLUMN IF EXISTS published_at,
    DROP COLUMN IF EXISTS claim_expires_at,
    DROP COLUMN IF EXISTS claim_generation,
    DROP COLUMN IF EXISTS claim_token,
    DROP COLUMN IF EXISTS trace_id,
    DROP COLUMN IF EXISTS state;
