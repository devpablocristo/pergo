-- +goose Up
ALTER TABLE campaign_batches
    ADD CONSTRAINT campaign_batches_payload_size
    CHECK (
        octet_length(payload) BETWEEN 1 AND 921600
    ) NOT VALID;

-- Refuse rollout if a legacy durable batch can never fit through the default
-- NATS 1 MiB max_payload. Operators must cancel/rebuild that campaign using
-- the byte-aware splitter before retrying the migration.
ALTER TABLE campaign_batches
    VALIDATE CONSTRAINT campaign_batches_payload_size;

-- +goose Down
ALTER TABLE campaign_batches
    DROP CONSTRAINT IF EXISTS campaign_batches_payload_size;
