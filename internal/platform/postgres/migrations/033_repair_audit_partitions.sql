-- +goose Up
-- +goose StatementBegin
DO $$
DECLARE
    stale RECORD;
    target_date DATE;
    legacy_name TEXT;
BEGIN
    -- Migration 002 created a LIKE-table for its current month before
    -- audit_logs became a declaratively partitioned parent in migration 010.
    -- Repair every such stale table without dropping its audit data.
    FOR stale IN
        SELECT c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = current_schema()
          AND c.relkind = 'r'
          AND c.relispartition = FALSE
          AND c.relname ~ '^audit_logs_y[0-9]{4}m[0-9]{2}$'
    LOOP
        target_date := to_date(
            substring(stale.relname FROM 'audit_logs_y([0-9]{4})m')
                || substring(stale.relname FROM 'm([0-9]{2})$'),
            'YYYYMM'
        );
        legacy_name := stale.relname || '_legacy_031';

        EXECUTE format('ALTER TABLE %I RENAME TO %I', stale.relname, legacy_name);
        PERFORM create_monthly_partition(target_date);
        EXECUTE format(
            'INSERT INTO audit_logs (id, workspace_id, trace_id, event_type, payload, created_at)
             SELECT id, workspace_id, trace_id, event_type, payload, created_at
             FROM %I
             ON CONFLICT DO NOTHING',
            legacy_name
        );
        EXECUTE format('DROP TABLE %I', legacy_name);
    END LOOP;

    PERFORM create_monthly_partition(CURRENT_DATE);
    PERFORM create_monthly_partition((CURRENT_DATE + interval '1 month')::date);
END $$;
-- +goose StatementEnd

-- +goose Down
-- This repair only attaches formerly orphaned tables and preserves their rows.
-- Reverting it would reintroduce the broken partition topology.
SELECT 1;
