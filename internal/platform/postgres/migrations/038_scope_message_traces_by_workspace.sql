-- +goose Up
ALTER TABLE message_ingress_ledger
    DROP CONSTRAINT IF EXISTS message_ingress_ledger_trace_id_key;
ALTER TABLE message_ingress_ledger
    ADD CONSTRAINT message_ingress_ledger_workspace_trace_key
        UNIQUE (workspace_id, trace_id);

ALTER TABLE message_dispatches
    DROP CONSTRAINT IF EXISTS message_dispatches_trace_id_key;
DROP INDEX IF EXISTS idx_message_dispatches_trace;
ALTER TABLE message_dispatches
    ADD CONSTRAINT message_dispatches_workspace_trace_key
        UNIQUE (workspace_id, trace_id);

-- +goose Down
ALTER TABLE message_dispatches
    DROP CONSTRAINT IF EXISTS message_dispatches_workspace_trace_key;
ALTER TABLE message_dispatches
    ADD CONSTRAINT message_dispatches_trace_id_key UNIQUE (trace_id);
CREATE INDEX idx_message_dispatches_trace ON message_dispatches(trace_id);

ALTER TABLE message_ingress_ledger
    DROP CONSTRAINT IF EXISTS message_ingress_ledger_workspace_trace_key;
ALTER TABLE message_ingress_ledger
    ADD CONSTRAINT message_ingress_ledger_trace_id_key UNIQUE (trace_id);
