-- +goose Up
ALTER TABLE inbound_dedups
    ADD COLUMN connection_id UUID;

UPDATE inbound_dedups
SET connection_id = '00000000-0000-0000-0000-000000000000'
WHERE connection_id IS NULL;

ALTER TABLE inbound_dedups
    ALTER COLUMN connection_id SET NOT NULL,
    DROP CONSTRAINT inbound_dedups_pkey,
    ADD CONSTRAINT inbound_dedups_pkey
        PRIMARY KEY (workspace_id, connection_id, channel, provider_message_id);

-- +goose Down
-- A downgrade is safe only if no provider identity is repeated across
-- connections. Refuse destructive collapse instead of silently losing rows.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM inbound_dedups
        GROUP BY workspace_id, channel, provider_message_id
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION
            'cannot downgrade inbound dedup scope: cross-connection identities exist';
    END IF;
END
$$;
-- +goose StatementEnd

ALTER TABLE inbound_dedups
    DROP CONSTRAINT inbound_dedups_pkey,
    ADD CONSTRAINT inbound_dedups_pkey
        PRIMARY KEY (workspace_id, channel, provider_message_id),
    DROP COLUMN connection_id;
