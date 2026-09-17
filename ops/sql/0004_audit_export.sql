-- =============================================================================
-- 0004_audit_export.sql — WO-0001 Restate audit export pipeline state.
--
-- audit_export_state: singleton cursor row (last exported (created_at,
--   invocation_id) pair); the exporter advances it only after a whole batch
--   landed in Kafka + PG, so restarts never skip or double-process records.
-- audit_events: replayable audit log, one row per Restate invocation. The
--   idempotent upsert (ON CONFLICT DO UPDATE) lives in the code — see
--   kernel/audit_export/exporter.py::upsert_audit_event — so both the
--   exporter and the Kafka replay consumer share identical semantics.
-- =============================================================================

CREATE TABLE IF NOT EXISTS audit_export_state (
    singleton            int PRIMARY KEY DEFAULT 1 CHECK (singleton = 1),
    cursor_created_at    timestamptz,
    cursor_invocation_id text NOT NULL DEFAULT '',
    updated_at           timestamptz NOT NULL DEFAULT now()
);

-- Bootstrap the singleton row; re-applying is a no-op.
INSERT INTO audit_export_state (singleton)
VALUES (1)
ON CONFLICT (singleton) DO NOTHING;

CREATE TABLE IF NOT EXISTS audit_events (
    invocation_id text PRIMARY KEY,
    service_name  text,
    handler_name  text,
    status        text,
    created_at    timestamptz,
    raw           jsonb NOT NULL,
    exported_at   timestamptz
);

CREATE INDEX IF NOT EXISTS audit_events_created_at_idx ON audit_events (created_at);
CREATE INDEX IF NOT EXISTS audit_events_service_idx ON audit_events (service_name);
