-- +goose Up
ALTER TABLE campaigns
    ADD CONSTRAINT campaigns_id_workspace_unique UNIQUE (id, workspace_id);

ALTER TABLE campaigns
    ADD CONSTRAINT campaigns_delay_seconds_range
    CHECK (delay_seconds BETWEEN 0 AND 3600) NOT VALID;

UPDATE campaigns
   SET delay_seconds = GREATEST(0, LEAST(delay_seconds, 3600))
 WHERE delay_seconds < 0
    OR delay_seconds > 3600;

ALTER TABLE campaigns
    VALIDATE CONSTRAINT campaigns_delay_seconds_range;

CREATE TABLE campaign_batches (
    campaign_id UUID NOT NULL,
    workspace_id UUID NOT NULL,
    batch_index INTEGER NOT NULL,
    total_batches INTEGER NOT NULL,
    trace_id TEXT NOT NULL UNIQUE,
    payload BYTEA NOT NULL,
    payload_hash BYTEA NOT NULL,
    delay_seconds INTEGER NOT NULL DEFAULT 0,
    publish_attempts INTEGER NOT NULL DEFAULT 0,
    next_publish_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_published_at TIMESTAMPTZ,
    last_error TEXT,
    publish_lease_token UUID,
    publish_lease_until TIMESTAMPTZ,
    processed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (campaign_id, batch_index),
    CONSTRAINT campaign_batches_campaign_tenant_fk
        FOREIGN KEY (campaign_id, workspace_id)
        REFERENCES campaigns (id, workspace_id)
        ON DELETE CASCADE,
    CONSTRAINT campaign_batches_index_valid
        CHECK (batch_index > 0 AND total_batches > 0 AND batch_index <= total_batches),
    CONSTRAINT campaign_batches_payload_hash_sha256
        CHECK (octet_length(payload_hash) = 32),
    CONSTRAINT campaign_batches_publish_attempts_nonnegative
        CHECK (publish_attempts >= 0),
    CONSTRAINT campaign_batches_delay_seconds_range
        CHECK (delay_seconds BETWEEN 0 AND 3600),
    CONSTRAINT campaign_batches_publish_lease_shape
        CHECK (
            (publish_lease_token IS NULL AND publish_lease_until IS NULL)
            OR
            (publish_lease_token IS NOT NULL AND publish_lease_until IS NOT NULL)
        )
);

CREATE INDEX idx_campaign_batches_due
    ON campaign_batches (next_publish_at, campaign_id, batch_index)
    WHERE processed_at IS NULL;

CREATE INDEX idx_campaign_batches_campaign_unprocessed
    ON campaign_batches (campaign_id, batch_index)
    WHERE processed_at IS NULL;

-- +goose Down
DROP TABLE IF EXISTS campaign_batches;

ALTER TABLE campaigns
    DROP CONSTRAINT IF EXISTS campaigns_delay_seconds_range;

ALTER TABLE campaigns
    DROP CONSTRAINT IF EXISTS campaigns_id_workspace_unique;
