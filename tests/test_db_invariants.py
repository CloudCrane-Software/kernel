"""Six-table invariant tests (WO-01): P-3, P-4, F-2, append-only + extras.

All machine-checkable, straight against PG 16 (see conftest for how the
database is provisioned).
"""

from __future__ import annotations

import uuid
from typing import Any

import asyncpg
import pytest

from kernel.db import (
    Database,
    IdempotencyKeyConflict,
    InvariantViolation,
    canonical_json,
    new_ulid,
    sha256_hex,
)


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


async def _seed_mandate_and_grant(
    db: Database, *, expiry: str = "2099-01-01T00:00:00Z"
) -> tuple[str, str]:
    mandate_id = _id("mnd")
    grant_id = _id("grt")
    await db.register_mandate(
        mandate_id=mandate_id,
        human_signer="human:founder",
        signature="sig:" + uuid.uuid4().hex,
        payload={"scope": {"actions": ["*"]}, "cap": 1_000_000, "nonce": uuid.uuid4().hex},
        cap_amount=1_000_000,
        ledger_id="ledger-0",
        expires_at=expiry,
    )
    await db.register_grant(
        grant_id=grant_id,
        mandate_id=mandate_id,
        parent_grant_id=None,
        scope={"actions": ["test.*"], "resources": ["*"], "limits": {}},
        remaining_depth=3,
        expiry=expiry,
    )
    return mandate_id, grant_id


async def _seed_intent(
    db: Database,
    grant_id: str,
    *,
    fence_epoch: int = 1,
    params: dict[str, Any] | None = None,
) -> str:
    intent_id = _id("int")
    await db.register_intent(
        intent_id=intent_id,
        episode_id=_id("ep"),
        grant_id=grant_id,
        idempotency_key=f"idem-{uuid.uuid4().hex}",
        params=params or {"x": 1},
        fence_epoch=fence_epoch,
    )
    return intent_id


# --------------------------------------------------------------------- P-3
async def test_p3_unknown_blocks_void_full(fresh_db: Database) -> None:
    db = fresh_db
    mandate_id, grant_id = await _seed_mandate_and_grant(db)
    intent_id = await _seed_intent(db, grant_id)

    reservation_id = _id("res")
    await db.insert_reservation(
        reservation_id=reservation_id,
        intent_id=intent_id,
        mandate_id=mandate_id,
        amount=100,
    )
    # drive intent to UNKNOWN via the legal path
    await db.bump_attempt(intent_id)  # PREPARED -> DISPATCHING
    await db.set_intent_state(intent_id, "UNKNOWN")

    with pytest.raises(InvariantViolation, match=r"(?i)unknown"):
        await db.void_reservation(reservation_id)

    # reservation is still PENDING (budget occupied)
    async with db._pool.acquire() as conn:
        state = await conn.fetchval(
            "SELECT state FROM reservations WHERE reservation_id = $1", reservation_id
        )
    assert state == "PENDING"

    # after reconciliation resolves the intent, VOID succeeds
    await db.set_intent_state(intent_id, "NOT_APPLIED")
    await db.void_reservation(reservation_id)


# --------------------------------------------------------------------- P-4
async def test_p4_same_key_same_params_replays_first_row(db: Database) -> None:
    _mandate_id, grant_id = await _seed_mandate_and_grant(db)
    key = f"idem-{uuid.uuid4().hex}"
    params = {"action": "compute", "n": 42}

    row1, created1 = await db.register_intent(
        intent_id=_id("int"),
        episode_id=_id("ep"),
        grant_id=grant_id,
        idempotency_key=key,
        params=params,
        fence_epoch=1,
    )
    assert created1 is True

    # replay: different intent_id, SAME key, SAME params -> first row, no dup
    row2, created2 = await db.register_intent(
        intent_id=_id("int"),
        episode_id=_id("ep"),
        grant_id=grant_id,
        idempotency_key=key,
        params=params,
        fence_epoch=1,
    )
    assert created2 is False
    assert row2.intent_id == row1.intent_id

    async with db._pool.acquire() as conn:
        n = await conn.fetchval(
            "SELECT count(*) FROM action_intents WHERE idempotency_key = $1", key
        )
    assert n == 1


async def test_p4_same_key_different_params_rejected(db: Database) -> None:
    _m, grant_id = await _seed_mandate_and_grant(db)
    key = f"idem-{uuid.uuid4().hex}"
    await db.register_intent(
        intent_id=_id("int"),
        episode_id=_id("ep"),
        grant_id=grant_id,
        idempotency_key=key,
        params={"n": 1},
        fence_epoch=1,
    )
    with pytest.raises(IdempotencyKeyConflict):
        await db.register_intent(
            intent_id=_id("int"),
            episode_id=_id("ep"),
            grant_id=grant_id,
            idempotency_key=key,
            params={"n": 2},
            fence_epoch=1,
        )


async def test_p4_params_hash_immutable_after_registration(db: Database) -> None:
    _m, grant_id = await _seed_mandate_and_grant(db)
    intent_id = await _seed_intent(db, grant_id, params={"n": 1})
    async with db._pool.acquire() as conn:
        with pytest.raises(asyncpg.exceptions.PostgresError):
            await conn.execute(
                "UPDATE action_intents SET params_hash = $2 WHERE intent_id = $1",
                intent_id,
                sha256_hex(canonical_json({"n": 2})),
            )


# --------------------------------------------------------------------- F-2
async def test_f2_lease_epoch_fences_stale_holder(db: Database) -> None:
    lease_id = _id("lease")
    e1 = await db.acquire_lease(lease_id, "executor-A", 30)
    e2 = await db.acquire_lease(lease_id, "executor-B", 30)
    assert (e1, e2) == (1, 2)

    # stale holder A (saw epoch 1) cannot renew: 0 rows affected
    renewed = await db.lease_renew_if_current(lease_id, "executor-A", e1, 30)
    assert renewed is False
    # current holder B (epoch 2) renews fine
    renewed = await db.lease_renew_if_current(lease_id, "executor-B", e2, 30)
    assert renewed is True


async def test_f2_stale_epoch_conditional_update_hits_zero_rows(db: Database) -> None:
    """The pattern executors must use: any write conditioned on the epoch they
    saw. After the lease advances, the stale executor's UPDATE matches 0 rows."""
    _m, grant_id = await _seed_mandate_and_grant(db)
    intent_id = await _seed_intent(db, grant_id, fence_epoch=5)

    async with db._pool.acquire() as conn:
        # lease advances beyond the executor's snapshot
        n = await conn.execute(
            """UPDATE action_intents SET attempt_count = attempt_count + 1,
                     state = 'DISPATCHING'
                 WHERE intent_id = $1 AND state = 'PREPARED' AND fence_epoch < 6""",
            intent_id,
        )
        assert n.endswith(" 1")  # still current: one row updated

        n = await conn.execute(
            """UPDATE action_intents SET attempt_count = attempt_count + 1
                 WHERE intent_id = $1 AND fence_epoch < 6""",
            intent_id,
        )
        assert n.endswith(" 1")

        # simulate fenced executor whose WHERE clause sees an older world
        n = await conn.execute(
            "UPDATE action_intents SET attempt_count = attempt_count + 1 "
            "WHERE intent_id = $1 AND fence_epoch < 5",
            intent_id,
        )
        assert n.endswith(" 0")  # F-2: fenced out


# --------------------------------------------------------------- append-only
async def test_decisions_append_only(db: Database) -> None:
    _mandate_id, grant_id = await _seed_mandate_and_grant(db)
    intent_id = await _seed_intent(db, grant_id)
    await db.append_decision(
        decision_id=_id("dec"),
        intent_id=intent_id,
        policy_revision=1,
        registry_revision=1,
        mandate_sha256=sha256_hex("whatever"),
        grant_chain_digest="digest",
        decision="ALLOW",
        reason_codes=["ok"],
    )
    async with db._pool.acquire() as conn:
        with pytest.raises(asyncpg.exceptions.PostgresError, match=r"(?i)append-only"):
            await conn.execute("UPDATE decisions SET decision = 'DENY'")
        with pytest.raises(asyncpg.exceptions.PostgresError, match=r"(?i)append-only"):
            await conn.execute("DELETE FROM decisions")


# ------------------------------------------------------------------- extras
async def test_grant_usage_never_decreases(db: Database) -> None:
    _m, grant_id = await _seed_mandate_and_grant(db)
    async with db._pool.acquire() as conn:
        await conn.execute(
            "UPDATE grants SET used_calls = 5, used_budget = 500 WHERE grant_id = $1", grant_id
        )
        with pytest.raises(asyncpg.exceptions.PostgresError):
            await conn.execute("UPDATE grants SET used_calls = 4 WHERE grant_id = $1", grant_id)
        with pytest.raises(asyncpg.exceptions.PostgresError):
            await conn.execute("UPDATE grants SET used_budget = 499 WHERE grant_id = $1", grant_id)


async def test_grant_child_expiry_within_parent(db: Database) -> None:
    mandate_id, parent_id = await _seed_mandate_and_grant(db, expiry="2099-01-01T00:00:00Z")
    with pytest.raises(InvariantViolation):
        await db.register_grant(
            grant_id=_id("grt"),
            mandate_id=mandate_id,
            parent_grant_id=parent_id,
            scope={"actions": ["x"]},
            remaining_depth=2,
            expiry="2099-06-01T00:00:00Z",  # beyond parent
        )


async def test_obligation_single_unresolved_and_resolution_proof(db: Database) -> None:
    _mandate_id, grant_id = await _seed_mandate_and_grant(db)
    intent_id = await _seed_intent(db, grant_id)
    async with db._pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO obligations (obligation_id, intent_id, kind) VALUES ($1, $2, 'reconcile')",
            _id("obl"),
            intent_id,
        )
        with pytest.raises(asyncpg.exceptions.PostgresError):
            await conn.execute(
                "INSERT INTO obligations (obligation_id, intent_id, kind) VALUES ($1, $2, 'reconcile')",
                _id("obl"),
                intent_id,
            )
        obl_id = await conn.fetchval(
            "SELECT obligation_id FROM obligations WHERE intent_id = $1", intent_id
        )
        # RESOLVED without resolution+resolved_by is rejected
        with pytest.raises(asyncpg.exceptions.PostgresError):
            await conn.execute(
                "UPDATE obligations SET status = 'RESOLVED' WHERE obligation_id = $1", obl_id
            )
        # with proof it works
        await conn.execute(
            "UPDATE obligations SET status = 'RESOLVED', resolution = 'applied', resolved_by = 'human:founder' "
            "WHERE obligation_id = $1",
            obl_id,
        )


async def test_episode_state_machine_and_terminal_branch(db: Database) -> None:
    episode_id = _id("ep")
    async with db._pool.acquire() as conn:
        await conn.execute("INSERT INTO episodes (episode_id) VALUES ($1)", episode_id)
        # illegal: RESERVED -> VERIFYING
        with pytest.raises(asyncpg.exceptions.PostgresError):
            await conn.execute(
                "UPDATE episodes SET state = 'VERIFYING' WHERE episode_id = $1", episode_id
            )
        # legal path
        await conn.execute(
            "UPDATE episodes SET state = 'RUNNING' WHERE episode_id = $1", episode_id
        )
        # CLOSED requires terminal_branch
        with pytest.raises(asyncpg.exceptions.PostgresError):
            await conn.execute(
                "UPDATE episodes SET state = 'CLOSED' WHERE episode_id = $1", episode_id
            )
        await conn.execute(
            "UPDATE episodes SET state = 'CLOSED', terminal_branch = 'candidate_ready' WHERE episode_id = $1",
            episode_id,
        )


async def test_reservation_terminal_states_immutable(db: Database) -> None:
    mandate_id, grant_id = await _seed_mandate_and_grant(db)
    intent_id = await _seed_intent(db, grant_id)
    await db.bump_attempt(intent_id)
    await db.set_intent_state(intent_id, "APPLIED")
    reservation_id = _id("res")
    await db.insert_reservation(
        reservation_id=reservation_id,
        intent_id=intent_id,
        mandate_id=mandate_id,
        amount=10,
    )
    await db.post_reservation(reservation_id)
    with pytest.raises(InvariantViolation):
        await db.void_reservation(reservation_id)


def test_ulid_shape() -> None:
    u = new_ulid()
    assert len(u) == 26
    assert all(c in "0123456789ABCDEFGHJKMNPQRSTVWXYZ" for c in u)
    assert new_ulid() != u
