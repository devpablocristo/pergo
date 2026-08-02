-- +goose Up
ALTER TABLE connections
    ADD COLUMN IF NOT EXISTS credential_revision BIGINT;

ALTER TABLE connections
    ALTER COLUMN credential_revision TYPE BIGINT USING credential_revision::BIGINT,
    ALTER COLUMN credential_revision SET DEFAULT 0;

UPDATE connections
SET credential_revision = 0
WHERE credential_revision IS NULL;

ALTER TABLE connections
    ALTER COLUMN credential_revision SET NOT NULL;

-- +goose Down
ALTER TABLE connections
    DROP COLUMN IF EXISTS credential_revision;
