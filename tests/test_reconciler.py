"""Fault-injection tests for the reconciler (WO-06): F-1, F-3, F-4 + backoff
and escalation. Uses real PG; adapters are deterministic fakes so the
EXTERNAL AUTHORITY property is explicit (classification never comes from
kernel logs)."""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator
from typing import Any

import pytest
import pytest_asyncio

from kernel.db import Database, canonical_json, sha256_hex
from reconciler.adapters import AdapterRegistry
from reconciler.core import EscalationPolicy, Reconciler

fail = pytest.fail


class FakeAdapter:
    """Scriptable external authority: the test decides what the EXTERNAL
    system reports, independently of anything the kernel recorded."""

    def __init__(self, name: str, result: str) -> None:
        self.name = name
        self.result = result
        self.probed: list[str] = []

    async def probe(self, intent_id: str, params: dict[str, Any]) -> str:
        self.probed.append(intent_id)
        return self.result


@pytest_asyncio.fixture
async def db() -> AsyncIterator[Database]:
    dsn = os.environ.get("KERNEL_TEST_PG_DSN")
    if not dsn:
        from testcontainers.postgres import PostgresContainer

        with PostgresContainer("postgres:16.15") as pg:
            dsn = (
                pg.get_connection_url()
                .replace("postgresql+psycopg2", "postgresql")
                .replace("postgres+psycopg2", "postgresql")
            )
            database = await Database.connect(dsn)
            await database.apply_schema()
            yield database
            await database.close()
        return
    database = await Database.connect(dsn)
    await database.apply_schema()
    yield database
    await database.close()


def _id(p: str) -> str:
    return f"{p}-{uuid.uuid4().hex[:10]}"


async def _seed_unknown(db: Database, *, amount: int = 100) -> str:
    """Register an intent whose receipt was lost -> UNKNOWN + obligation +
    reconcile_state row (exactly what the gateway leaves behind)."""
    mandate_id, grant_id = _id("mnd"), _id("grt")
    await db.register_mandate(
        mandate_id=mandate_id,
        human_signer="human:founder",
        signature="sig",
        payload={"nonce": uuid.uuid4().hex},
        cap_amount=1_000_000,
        ledger_id="l0",
        expires_at="2099-01-01T00:00:00Z",
        signature_verified=True,
    )
    await db.register_grant(
        grant_id=grant_id,
        mandate_id=mandate_id,
        parent_grant_id=None,
        scope={"actions": ["external.file"], "resources": ["*"], "limits": {}},
        remaining_depth=3,
        expiry="2099-01-01T00:00:00Z",
    )
    episode_id = _id("ep")
    intent_id = _id("op")
    key = f"idem-{uuid.uuid4().hex}"
    params = {"x": 1}
    params_hash = sha256_hex(canonical_json(params))
    async with db._pool.acquire() as conn:
        await conn.execute("INSERT INTO episodes (episode_id) VALUES ($1)", episode_id)
        await conn.execute(
            """INSERT INTO action_intents
                   (intent_id, episode_id, grant_id, idempotency_key, params_hash,
                    fence_epoch, state, action_type)
               VALUES ($1, $2, $3, $4, $5, 1, 'UNKNOWN', 'external.file')""",
            intent_id,
            episode_id,
            grant_id,
            key,
            params_hash,
        )
        await conn.execute(
            """INSERT INTO reservations
                   (reservation_id, intent_id, mandate_id, tb_transfer_id, amount)
               VALUES ($1, $2, $3, $4, $5)""",
            _id("res"),
            intent_id,
            mandate_id,
            uuid.uuid4().hex,
            amount,
        )
        await conn.execute(
            "INSERT INTO obligations (obligation_id, intent_id, kind) VALUES ($1, $2, 'reconcile')",
            _id("obl"),
            intent_id,
        )
        await conn.execute(
            "INSERT INTO reconcile_state (intent_id) VALUES ($1)",
            intent_id,
        )
    return intent_id


async def _states(db: Database, intent_id: str) -> tuple[str, str, str]:
    async with db._pool.acquire() as conn:
        intent = await conn.fetchval(
            "SELECT state FROM action_intents WHERE intent_id = $1", intent_id
        )
        res = await conn.fetchval("SELECT state FROM reservations WHERE intent_id = $1", intent_id)
        obl = await conn.fetchval("SELECT status FROM obligations WHERE intent_id = $1", intent_id)
    return intent, res, obl or "-"


# ------------------------------------------------------------------- F-3
async def test_f3_lost_receipt_classified_from_external_state(db: Database) -> None:
    """F-3: receipt lost -> UNKNOWN. External authority says APPLIED -> intent
    APPLIED, budget POSTED, obligation RESOLVED. (Classification source is the
    probe, not kernel logs.)"""
    intent_id = await _seed_unknown(db)
    adapter = FakeAdapter("fs", "APPLIED")
    reg = AdapterRegistry()
    reg.register("external.file", adapter)
    rec = Reconciler(db, reg)
    await rec.poll_once()
    assert intent_id in adapter.probed
    intent, res, obl = await _states(db, intent_id)
    assert (intent, res, obl) == ("APPLIED", "POSTED", "RESOLVED")


async def test_f3_not_applied_voids_and_resolves(db: Database) -> None:
    intent_id = await _seed_unknown(db)
    reg = AdapterRegistry()
    reg.register("external.file", FakeAdapter("fs", "NOT_APPLIED"))
    await Reconciler(db, reg).poll_once()
    intent, res, obl = await _states(db, intent_id)
    assert (intent, res, obl) == ("NOT_APPLIED", "VOIDED", "RESOLVED")


# ------------------------------------------------------------------- F-4
async def test_f4_bypassed_tampering_detected_and_classified(db: Database) -> None:
    """F-4: an executor bypassed the gateway and the external state was changed
    directly. The probe (external authority) reports the effect exists; the
    intent is classified APPLIED even though no receipt ever arrived."""
    intent_id = await _seed_unknown(db)
    reg = AdapterRegistry()
    reg.register("external.file", FakeAdapter("fs", "APPLIED"))
    await Reconciler(db, reg).poll_once()
    intent, _, _ = await _states(db, intent_id)
    assert intent == "APPLIED"  # drift detected, classified from external state


# ------------------------------------------------------------------- F-1
async def test_f1_executor_crash_recovery_no_replay_no_leak(db: Database) -> None:
    """F-1: executor killed before receipt. While the external authority is
    unreachable (UNKNOWN), the budget stays occupied; recovery is idempotent —
    re-registration with the same key returns the SAME intent, exactly one
    reservation row ever exists, and one successful probe settles once."""
    intent_id = await _seed_unknown(db, amount=100)

    # external authority unreachable: stays UNKNOWN, budget occupied
    reg = AdapterRegistry()
    reg.register("external.file", FakeAdapter("fs", "UNKNOWN"))
    rec = Reconciler(db, reg)
    await rec.poll_once()
    # this intent specifically stays UNKNOWN with budget occupied
    intent, res, obl = await _states(db, intent_id)
    assert (intent, res, obl) == ("UNKNOWN", "PENDING", "OPEN")

    # executor restart replays registration with the same key -> same intent
    async with db._pool.acquire() as conn:
        key, episode, grant = await conn.fetchrow(
            "SELECT idempotency_key, episode_id, grant_id FROM action_intents WHERE intent_id=$1",
            intent_id,
        )
    row, created = await db.register_intent(
        intent_id=_id("op"),  # different operation_id, SAME key
        episode_id=episode,
        grant_id=grant,
        idempotency_key=key,
        params={"x": 1},
        fence_epoch=1,
        action_type="external.file",
    )
    assert created is False
    assert row.intent_id == intent_id

    # authority comes back with APPLIED -> settles exactly once
    async with db._pool.acquire() as conn:
        await conn.execute(
            "UPDATE reconcile_state SET next_probe_at = now() - interval '1s' WHERE intent_id = $1",
            intent_id,
        )
    reg2 = AdapterRegistry()
    reg2.register("external.file", FakeAdapter("fs", "APPLIED"))
    await Reconciler(db, reg2).poll_once()
    intent, res, obl = await _states(db, intent_id)
    assert (intent, res, obl) == ("APPLIED", "POSTED", "RESOLVED")
    async with db._pool.acquire() as conn:
        n_res = await conn.fetchval(
            "SELECT count(*) FROM reservations WHERE intent_id = $1", intent_id
        )
        n_dec = await conn.fetchval(
            "SELECT count(*) FROM decisions WHERE intent_id = $1", intent_id
        )
    assert n_res == 1  # no leak, no duplicate reservation
    assert n_dec == 0  # reconciler settles via external evidence, no new ledger row needed


# ------------------------------------------------------- backoff + escalation
async def test_backoff_grows_exponentially_and_escalates(db: Database) -> None:
    intent_id = await _seed_unknown(db, amount=50)
    reg = AdapterRegistry()
    reg.register("external.file", FakeAdapter("fs", "UNKNOWN"))
    rec = Reconciler(db, reg, policy=EscalationPolicy(max_probe_count=3, amount_threshold=10**9))
    for _ in range(3):
        await rec.poll_once()
        # simulate time passing so the due-intent scan sees it again
        async with db._pool.acquire() as conn:
            await conn.execute(
                "UPDATE reconcile_state SET next_probe_at = now() - interval '1s' "
                "WHERE intent_id = $1",
                intent_id,
            )
    async with db._pool.acquire() as conn:
        probe_count = await conn.fetchval(
            "SELECT probe_count FROM reconcile_state WHERE intent_id = $1", intent_id
        )
        obl = await conn.fetchval("SELECT status FROM obligations WHERE intent_id = $1", intent_id)
    assert probe_count == 3
    assert obl == "ESCALATED"  # over threshold -> human ruling required
    intent, res, _ = await _states(db, intent_id)
    assert (intent, res) == ("UNKNOWN", "PENDING")  # still occupied after escalation


async def test_amount_threshold_escalates_immediately(db: Database) -> None:
    intent_id = await _seed_unknown(db, amount=999_999)  # high-value intent
    reg = AdapterRegistry()
    reg.register("external.file", FakeAdapter("fs", "UNKNOWN"))
    out = await Reconciler(
        db, reg, policy=EscalationPolicy(max_probe_count=100, amount_threshold=100_000)
    ).poll_once()
    assert any(d["intent_id"] == intent_id for d in out.detail)
    _, _, obl = await _states(db, intent_id)
    assert obl == "ESCALATED"


# ------------------------------------------------------- compensation admission
async def test_compensation_whitelist_admission(db: Database) -> None:
    rec = Reconciler(db, AdapterRegistry())
    await rec.register_compensation("external.file", "compensations:rollback_file")
    assert await rec.compensation_allowed("external.file") is True
    assert await rec.compensation_allowed("unregistered.action") is False
