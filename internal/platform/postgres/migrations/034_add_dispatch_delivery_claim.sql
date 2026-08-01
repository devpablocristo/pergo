-- +goose Up
ALTER TABLE message_dispatches
    ADD COLUMN delivery_claim_token UUID,
    ADD COLUMN delivery_claim_generation BIGINT NOT NULL DEFAULT 0
        CHECK (delivery_claim_generation >= 0),
    ADD COLUMN delivery_claim_expires_at TIMESTAMPTZ,
    ADD CONSTRAINT message_dispatch_delivery_claim_shape CHECK (
        (
            delivery_claim_token IS NULL
            AND delivery_claim_expires_at IS NULL
        )
        OR
        (
            delivery_claim_token IS NOT NULL
            AND delivery_claim_expires_at IS NOT NULL
        )
    );

CREATE INDEX idx_message_dispatch_delivery_claim_expiry
    ON message_dispatches (delivery_claim_expires_at)
    WHERE delivery_claim_token IS NOT NULL;

-- +goose Down
DROP INDEX IF EXISTS idx_message_dispatch_delivery_claim_expiry;
ALTER TABLE message_dispatches
    DROP CONSTRAINT IF EXISTS message_dispatch_delivery_claim_shape,
    DROP COLUMN IF EXISTS delivery_claim_expires_at,
    DROP COLUMN IF EXISTS delivery_claim_generation,
    DROP COLUMN IF EXISTS delivery_claim_token;
