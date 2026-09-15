-- =============================================================================
-- 0002_reconcile.sql — reconciler state + compensation registry (WO-06).
-- Idempotent. Additive only (never weaken 0001 constraints).
-- =============================================================================

CREATE TABLE IF NOT EXISTS reconcile_state (
    intent_id     TEXT PRIMARY KEY REFERENCES action_intents (intent_id),
    probe_count   INTEGER NOT NULL DEFAULT 0 CHECK (probe_count >= 0),
    next_probe_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_probe_at TIMESTAMPTZ,
    last_result   TEXT CHECK (last_result IN ('APPLIED', 'NOT_APPLIED', 'UNKNOWN')),
    adapter_name  TEXT,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS compensation_registry (
    action_type      TEXT PRIMARY KEY,
    compensation_ref TEXT NOT NULL,          -- plugin function reference
    enabled          BOOLEAN NOT NULL DEFAULT TRUE,
    registered_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- index for the polling loop
CREATE INDEX IF NOT EXISTS reconcile_due_idx
    ON reconcile_state (next_probe_at)
    WHERE last_result IS NULL OR last_result = 'UNKNOWN';
