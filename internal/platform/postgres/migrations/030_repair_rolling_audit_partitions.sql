-- +goose Up
-- +goose StatementBegin
DO $$
DECLARE
    legacy RECORD;
    partition_month date;
BEGIN
    -- Migration 002 created a regular audit_logs_yYYYYmMM table before
    -- audit_logs became declaratively partitioned in migration 010. Repair
    -- every such legacy table generically instead of naming a calendar month.
    FOR legacy IN
        SELECT c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = current_schema()
          AND c.relkind = 'r'
          AND c.relispartition = false
          AND c.relname ~ '^audit_logs_y[0-9]{4}m[0-9]{2}$'
    LOOP
        partition_month := to_date(
            substr(legacy.relname, 13, 4) || substr(legacy.relname, 18, 2),
            'YYYYMM'
        );

        CREATE TEMP TABLE pergo_audit_partition_repair
        ON COMMIT DROP
        AS SELECT id, workspace_id, trace_id, event_type, payload, created_at
        FROM audit_logs
        WITH NO DATA;

        EXECUTE format(
            'INSERT INTO pergo_audit_partition_repair ' ||
            '(id, workspace_id, trace_id, event_type, payload, created_at) ' ||
            'SELECT id, workspace_id, trace_id, event_type, payload, created_at FROM %I',
            legacy.relname
        );
        EXECUTE format('DROP TABLE %I CASCADE', legacy.relname);

        PERFORM create_monthly_partition(partition_month);

        INSERT INTO audit_logs (id, workspace_id, trace_id, event_type, payload, created_at)
        SELECT id, workspace_id, trace_id, event_type, payload, created_at
        FROM pergo_audit_partition_repair;

        DROP TABLE pergo_audit_partition_repair;
    END LOOP;

    PERFORM create_monthly_partition(CURRENT_DATE);
    PERFORM create_monthly_partition((CURRENT_DATE + interval '1 month')::date);
END $$;
-- +goose StatementEnd

-- +goose Down
-- This migration repairs data-bearing tables. Reverting that repair would
-- risk data loss, so the down migration intentionally preserves the result.
SELECT 1;
