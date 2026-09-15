-- =============================================================================
-- 0001_init.sql — governance kernel six-table schema + episodes/leases (WO-01).
-- PostgreSQL 16. Idempotent: safe to re-run (IF NOT EXISTS / OR REPLACE).
-- Constraints here are the INVARIANTS — weakening any of them is forbidden
-- (AGENTS.md prohibition 3); tests/test_db_invariants.py pins each one.
-- =============================================================================

-- ---------------------------------------------------------------- 1. mandates
CREATE TABLE IF NOT EXISTS mandates (
    mandate_id     TEXT PRIMARY KEY,
    human_signer   TEXT NOT NULL,
    signature      TEXT NOT NULL,
    payload_jcs    JSONB NOT NULL,
    mandate_sha256 CHAR(64) NOT NULL UNIQUE,
    cap_amount     BIGINT NOT NULL CHECK (cap_amount > 0),
    ledger_id      TEXT NOT NULL,
    expires_at     TIMESTAMPTZ NOT NULL,
    status         TEXT NOT NULL DEFAULT 'ACTIVE'
                   CHECK (status IN ('ACTIVE', 'EXPIRED', 'REVOKED')),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ------------------------------------------------------------------- 2. grants
CREATE TABLE IF NOT EXISTS grants (
    grant_id        TEXT PRIMARY KEY,
    mandate_id      TEXT NOT NULL REFERENCES mandates (mandate_id),
    parent_grant_id TEXT REFERENCES grants (grant_id),
    scope           JSONB NOT NULL,
    remaining_depth INTEGER NOT NULL CHECK (remaining_depth >= 0),
    used_calls      BIGINT NOT NULL DEFAULT 0 CHECK (used_calls >= 0),
    used_budget     BIGINT NOT NULL DEFAULT 0 CHECK (used_budget >= 0),
    expiry          TIMESTAMPTZ NOT NULL,
    status          TEXT NOT NULL DEFAULT 'ACTIVE'
                    CHECK (status IN ('ACTIVE', 'EXHAUSTED', 'EXPIRED', 'REVOKED')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- used_calls / used_budget may never DECREASE (budget is responsibility-held).
CREATE OR REPLACE FUNCTION grants_usage_monotonic() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.used_calls < OLD.used_calls OR NEW.used_budget < OLD.used_budget THEN
        RAISE EXCEPTION 'grant % usage may not decrease (calls %->%, budget %->%)',
            NEW.grant_id, OLD.used_calls, NEW.used_calls, OLD.used_budget, NEW.used_budget
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_grants_usage_monotonic ON grants;
CREATE TRIGGER trg_grants_usage_monotonic
    BEFORE UPDATE ON grants
    FOR EACH ROW EXECUTE FUNCTION grants_usage_monotonic();

-- child expiry must never extend beyond parent expiry (scope non-increasing).
CREATE OR REPLACE FUNCTION grants_expiry_within_parent() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    parent_expiry TIMESTAMPTZ;
BEGIN
    IF NEW.parent_grant_id IS NOT NULL THEN
        SELECT expiry INTO parent_expiry FROM grants WHERE grant_id = NEW.parent_grant_id;
        IF parent_expiry IS NULL THEN
            RAISE EXCEPTION 'parent grant % not found', NEW.parent_grant_id
                USING ERRCODE = 'foreign_key_violation';
        END IF;
        IF NEW.expiry > parent_expiry THEN
            RAISE EXCEPTION 'grant % expiry % exceeds parent expiry %',
                NEW.grant_id, NEW.expiry, parent_expiry
                USING ERRCODE = 'check_violation';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_grants_expiry_within_parent ON grants;
CREATE TRIGGER trg_grants_expiry_within_parent
    BEFORE INSERT OR UPDATE ON grants
    FOR EACH ROW EXECUTE FUNCTION grants_expiry_within_parent();

-- ---------------------------------------------------------- 3. action_intents
CREATE TABLE IF NOT EXISTS action_intents (
    intent_id       TEXT PRIMARY KEY,          -- == operation_id, stable across retries
    episode_id      TEXT NOT NULL,
    grant_id        TEXT NOT NULL REFERENCES grants (grant_id),
    idempotency_key TEXT NOT NULL UNIQUE,
    params_hash     CHAR(64) NOT NULL,
    fence_epoch     BIGINT NOT NULL CHECK (fence_epoch >= 0),
    state           TEXT NOT NULL DEFAULT 'PREPARED'
                    CHECK (state IN ('PREPARED', 'DISPATCHING', 'APPLIED', 'NOT_APPLIED', 'UNKNOWN')),
    attempt_count   INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    action_type     TEXT NOT NULL DEFAULT 'generic',
    registered_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- params_hash is immutable after first registration (P-4 companion).
CREATE OR REPLACE FUNCTION intents_params_hash_immutable() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.params_hash IS DISTINCT FROM OLD.params_hash THEN
        RAISE EXCEPTION 'intent % params_hash is immutable', NEW.intent_id
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_intents_params_hash_immutable ON action_intents;
CREATE TRIGGER trg_intents_params_hash_immutable
    BEFORE UPDATE ON action_intents
    FOR EACH ROW EXECUTE FUNCTION intents_params_hash_immutable();

-- state machine: forward-only; terminal states immutable; UNKNOWN resolvable
-- only to APPLIED/NOT_APPLIED (by the reconciler, never administratively to
-- a non-terminal state, never back to PREPARED/DISPATCHING).
CREATE OR REPLACE FUNCTION intents_state_one_way() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.state = OLD.state THEN
        RETURN NEW;  -- attempt_count bumps etc.
    END IF;
    IF NOT (
        (OLD.state = 'PREPARED'    AND NEW.state = 'DISPATCHING') OR
        (OLD.state = 'DISPATCHING' AND NEW.state IN ('APPLIED', 'NOT_APPLIED', 'UNKNOWN')) OR
        (OLD.state = 'UNKNOWN'     AND NEW.state IN ('APPLIED', 'NOT_APPLIED'))
    ) THEN
        RAISE EXCEPTION 'intent % illegal state transition % -> %',
            NEW.intent_id, OLD.state, NEW.state
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_intents_state_one_way ON action_intents;
CREATE TRIGGER trg_intents_state_one_way
    BEFORE UPDATE ON action_intents
    FOR EACH ROW EXECUTE FUNCTION intents_state_one_way();

-- fence epoch may never rewind.
CREATE OR REPLACE FUNCTION intents_epoch_monotonic() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.fence_epoch < OLD.fence_epoch THEN
        RAISE EXCEPTION 'intent % fence_epoch may not rewind (% -> %)',
            NEW.intent_id, OLD.fence_epoch, NEW.fence_epoch
            USING ERRCODE = 'check_violation';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_intents_epoch_monotonic ON action_intents;
CREATE TRIGGER trg_intents_epoch_monotonic
    BEFORE UPDATE ON action_intents
    FOR EACH ROW EXECUTE FUNCTION intents_epoch_monotonic();

-- ------------------------------------------------------------- 4. reservations
CREATE TABLE IF NOT EXISTS reservations (
    reservation_id TEXT PRIMARY KEY,
    intent_id      TEXT NOT NULL UNIQUE REFERENCES action_intents (intent_id),
    mandate_id     TEXT NOT NULL REFERENCES mandates (mandate_id),
    tb_transfer_id TEXT NOT NULL UNIQUE,      -- placeholder ULID until WO-05
    amount         BIGINT NOT NULL CHECK (amount > 0),
    state          TEXT NOT NULL DEFAULT 'PENDING'
                   CHECK (state IN ('PENDING', 'POSTED', 'VOIDED')),
    resolved_at    TIMESTAMPTZ,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- one-way: PENDING -> POSTED | VOIDED; terminal states immutable.
CREATE OR REPLACE FUNCTION reservations_one_way() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.state = OLD.state THEN
        RETURN NEW;
    END IF;
    IF NOT (
        (OLD.state = 'PENDING' AND NEW.state IN ('POSTED', 'VOIDED'))
    ) THEN
        RAISE EXCEPTION 'reservation % illegal state transition % -> %',
            NEW.reservation_id, OLD.state, NEW.state
            USING ERRCODE = 'check_violation';
    END IF;
    NEW.resolved_at := now();
    RETURN NEW;
END;
$$;

-- P-3: while the intent is UNKNOWN the reservation may NOT be voided —
-- the budget stays occupied until reconciliation resolves the intent.
CREATE OR REPLACE FUNCTION reservations_no_void_on_unknown() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    intent_state TEXT;
BEGIN
    IF NEW.state = 'VOIDED' AND OLD.state <> 'VOIDED' THEN
        SELECT state INTO intent_state FROM action_intents WHERE intent_id = NEW.intent_id;
        IF intent_state = 'UNKNOWN' THEN
            RAISE EXCEPTION 'reservation % may not VOID while intent % is UNKNOWN (P-3)',
                NEW.reservation_id, NEW.intent_id
                USING ERRCODE = 'check_violation';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_reservations_one_way ON reservations;
CREATE TRIGGER trg_reservations_one_way
    BEFORE UPDATE ON reservations
    FOR EACH ROW EXECUTE FUNCTION reservations_one_way();
DROP TRIGGER IF EXISTS trg_reservations_no_void_on_unknown ON reservations;
CREATE TRIGGER trg_reservations_no_void_on_unknown
    BEFORE UPDATE ON reservations
    FOR EACH ROW EXECUTE FUNCTION reservations_no_void_on_unknown();

-- -------------------------------------------------------------- 5. obligations
CREATE TABLE IF NOT EXISTS obligations (
    obligation_id TEXT PRIMARY KEY,
    intent_id     TEXT NOT NULL REFERENCES action_intents (intent_id),
    kind          TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'OPEN'
                  CHECK (status IN ('OPEN', 'ESCALATED', 'RESOLVED')),
    resolution    TEXT,
    resolved_by   TEXT,
    resolved_at   TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- at most one unresolved (OPEN or ESCALATED) obligation per intent.
CREATE UNIQUE INDEX IF NOT EXISTS obligations_one_unresolved_per_intent
    ON obligations (intent_id) WHERE status IN ('OPEN', 'ESCALATED');

-- RESOLVED requires resolution + resolved_by (audit trail of the ruling).
CREATE OR REPLACE FUNCTION obligations_resolved_requires_proof() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.status = 'RESOLVED' AND (NEW.resolution IS NULL OR NEW.resolved_by IS NULL) THEN
        RAISE EXCEPTION 'obligation % RESOLVED requires resolution and resolved_by',
            NEW.obligation_id
            USING ERRCODE = 'check_violation';
    END IF;
    IF NEW.status = 'RESOLVED' THEN
        NEW.resolved_at := now();
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_obligations_resolved_requires_proof ON obligations;
CREATE TRIGGER trg_obligations_resolved_requires_proof
    BEFORE UPDATE ON obligations
    FOR EACH ROW EXECUTE FUNCTION obligations_resolved_requires_proof();

-- ---------------------------------------------------------------- 6. decisions
-- The decision ledger: append-only, physically rejects UPDATE and DELETE.
CREATE TABLE IF NOT EXISTS decisions (
    decision_id        TEXT PRIMARY KEY,
    intent_id          TEXT NOT NULL REFERENCES action_intents (intent_id),
    policy_revision    BIGINT NOT NULL,
    registry_revision  BIGINT NOT NULL,
    mandate_sha256     CHAR(64) NOT NULL,
    grant_chain_digest TEXT NOT NULL,
    decision           TEXT NOT NULL
                       CHECK (decision IN ('ALLOW', 'DENY', 'NEED_EVIDENCE', 'DEFER')),
    reason_codes       JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION decisions_append_only() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'decisions is append-only (attempted %)', TG_OP
        USING ERRCODE = 'check_violation';
END;
$$;

DROP TRIGGER IF EXISTS trg_decisions_no_update ON decisions;
CREATE TRIGGER trg_decisions_no_update
    BEFORE UPDATE ON decisions
    FOR EACH ROW EXECUTE FUNCTION decisions_append_only();
DROP TRIGGER IF EXISTS trg_decisions_no_delete ON decisions;
CREATE TRIGGER trg_decisions_no_delete
    BEFORE DELETE ON decisions
    FOR EACH ROW EXECUTE FUNCTION decisions_append_only();

-- ------------------------------------------------------------------- episodes
CREATE TABLE IF NOT EXISTS episodes (
    episode_id      TEXT PRIMARY KEY,
    state           TEXT NOT NULL DEFAULT 'RESERVED'
                    CHECK (state IN ('RESERVED', 'RUNNING', 'VERIFYING', 'CLOSED')),
    terminal_branch TEXT
                    CHECK (terminal_branch IN ('candidate_ready', 'not_solved', 'deferred', 'expired')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT episodes_closed_requires_branch
        CHECK ((state = 'CLOSED' AND terminal_branch IS NOT NULL)
            OR (state <> 'CLOSED' AND terminal_branch IS NULL))
);

CREATE OR REPLACE FUNCTION episodes_one_way() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NOT (
        (OLD.state = 'RESERVED'  AND NEW.state = 'RUNNING') OR
        (OLD.state = 'RUNNING'   AND NEW.state IN ('VERIFYING', 'CLOSED')) OR
        (OLD.state = 'VERIFYING' AND NEW.state = 'CLOSED') OR
        (OLD.state = NEW.state)
    ) THEN
        RAISE EXCEPTION 'episode % illegal state transition % -> %',
            NEW.episode_id, OLD.state, NEW.state
            USING ERRCODE = 'check_violation';
    END IF;
    NEW.updated_at := now();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_episodes_one_way ON episodes;
CREATE TRIGGER trg_episodes_one_way
    BEFORE UPDATE ON episodes
    FOR EACH ROW EXECUTE FUNCTION episodes_one_way();

-- --------------------------------------------------------------------- leases
CREATE TABLE IF NOT EXISTS leases (
    lease_id   TEXT PRIMARY KEY,
    holder     TEXT NOT NULL,
    epoch      BIGINT NOT NULL DEFAULT 0 CHECK (epoch >= 0),
    expires_at TIMESTAMPTZ NOT NULL
);

-- Acquisition uses a single-statement upsert so the epoch increment is atomic:
--   INSERT INTO leases (lease_id, holder, epoch, expires_at)
--     VALUES ($1, $2, 1, $3)
--   ON CONFLICT (lease_id) DO UPDATE
--     SET epoch = leases.epoch + 1, holder = EXCLUDED.holder,
--         expires_at = EXCLUDED.expires_at
--   RETURNING epoch;
-- Stale executors fence themselves out by conditioning on the epoch they saw
-- (UPDATE ... WHERE fence_epoch < $seen), which then matches 0 rows (F-2).
