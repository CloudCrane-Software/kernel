-- =============================================================================
-- 0003_intent_params.sql — audit round 1: store intent params for the
-- reconciler's external probes (classification needs the authority pointer,
-- e.g. external_path / resource URL). Idempotent, additive.
-- =============================================================================

ALTER TABLE action_intents
    ADD COLUMN IF NOT EXISTS params JSONB NOT NULL DEFAULT '{}'::jsonb;
