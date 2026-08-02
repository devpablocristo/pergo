-- +goose Up
ALTER TABLE webhook_dlqs
    ADD COLUMN encrypted_data BYTEA,
    ADD COLUMN key_id TEXT,
    ADD COLUMN key_version INT;

-- New application writes keep the legacy NOT NULL columns as non-sensitive
-- placeholders. Runtime migration backfills and scrubs existing rows before
-- deployed workloads are allowed to start.

-- +goose Down
-- Ciphertext cannot be restored to the legacy plaintext columns in SQL.
-- Refuse a destructive schema rollback after any row has been encrypted.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM webhook_dlqs
        WHERE encrypted_data IS NOT NULL
    ) THEN
        RAISE EXCEPTION
            'cannot downgrade webhook DLQ encryption after ciphertext exists; restore a pre-039 backup or roll forward';
    END IF;
END
$$;
-- +goose StatementEnd

ALTER TABLE webhook_dlqs
    DROP COLUMN IF EXISTS key_version,
    DROP COLUMN IF EXISTS key_id,
    DROP COLUMN IF EXISTS encrypted_data;
