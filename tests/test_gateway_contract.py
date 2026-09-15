"""Gateway contract tests (WO-03): every error code at least once, plus the
concurrent same-operation race. Runs against local PG + local OPA server
(or testcontainers on CI)."""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import httpx
import pytest
import pytest_asyncio

from gateway.app import build_app
from gateway.schemas import ReceiptRequest, RegisterIntentRequest
from kernel.db import Database
from kernel.policy import PolicyClient

POLICIES_DIR = Path(__file__).resolve().parent.parent / "policies"


@pytest.fixture(scope="module")
def opa_url() -> Iterator[str]:
    import socket
    import subprocess
    import time

    if env := os.environ.get("KERNEL_TEST_OPA_URL"):
        yield env
        return
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = int(s.getsockname()[1])
    proc = subprocess.Popen(
        ["opa", "run", "--server", "--addr", f"127.0.0.1:{port}", str(POLICIES_DIR)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    base = f"http://127.0.0.1:{port}"
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        try:
            if httpx.get(f"{base}/health", timeout=1).status_code == 200:
                break
        except httpx.HTTPError:
            time.sleep(0.2)
    else:
        proc.kill()
        pytest.fail("opa did not start")
    yield base
    proc.terminate()
    proc.wait(timeout=10)


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


@pytest_asyncio.fixture
async def client(db: Database, opa_url: str) -> AsyncIterator[httpx.AsyncClient]:
    app = build_app(db, PolicyClient(opa_url))
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


def _id(p: str) -> str:
    return f"{p}-{uuid.uuid4().hex[:10]}"


async def _seed(db: Database, *, signature_verified: bool = True) -> tuple[str, str, str]:
    """Returns (mandate_id, grant_id, episode_id) with a live lease."""
    mandate_id, grant_id = _id("mnd"), _id("grt")
    await db.register_mandate(
        mandate_id=mandate_id,
        human_signer="human:founder",
        signature="sig:x",
        payload={"nonce": uuid.uuid4().hex},
        cap_amount=1_000_000,
        ledger_id="ledger-0",
        expires_at="2099-01-01T00:00:00Z",
        signature_verified=signature_verified,
    )
    await db.register_grant(
        grant_id=grant_id,
        mandate_id=mandate_id,
        parent_grant_id=None,
        scope={
            "actions": ["generic"],
            "resources": ["*"],
            "limits": {"call_limit": 100, "budget_limit": 1000},
        },
        remaining_depth=3,
        expiry="2099-01-01T00:00:00Z",
    )
    episode_id = _id("ep")
    async with db._pool.acquire() as conn:
        await conn.execute("INSERT INTO episodes (episode_id) VALUES ($1)", episode_id)
    return mandate_id, grant_id, episode_id


def _intent_req(grant_id: str, episode_id: str, **over: object) -> dict[str, object]:
    base = {
        "operation_id": _id("op"),
        "episode_id": episode_id,
        "grant_id": grant_id,
        "idempotency_key": f"idem-{uuid.uuid4().hex}",
        "params": {"n": 1},
        "action_type": "generic",
        "amount": 10,
    }
    base.update(over)
    return base


# ------------------------------------------------------------------ happy path
async def test_register_intent_allow(client: httpx.AsyncClient, db: Database) -> None:
    _, grant_id, ep = await _seed(db)
    resp = await client.post("/v1/intents", json=_intent_req(grant_id, ep))
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["created"] is True
    assert body["decision"]["outcome"] == "ALLOW"
    assert body["tb_transfer_id"]  # ULID placeholder present
    async with db._pool.acquire() as conn:
        n = await conn.fetchval(
            "SELECT count(*) FROM decisions WHERE intent_id = $1", body["intent_id"]
        )
    assert n == 1  # decision row appended


# ------------------------------------------------------------------- P-4 at API
async def test_replay_same_key_same_params(client: httpx.AsyncClient, db: Database) -> None:
    _, grant_id, ep = await _seed(db)
    req = _intent_req(grant_id, ep)
    first = (await client.post("/v1/intents", json=req)).json()
    second = await client.post("/v1/intents", json=req)
    assert second.status_code == 200
    body = second.json()
    assert body["created"] is False
    assert body["intent_id"] == first["intent_id"]


async def test_conflicting_params_422(client: httpx.AsyncClient, db: Database) -> None:
    _, grant_id, ep = await _seed(db)
    req = _intent_req(grant_id, ep)
    await client.post("/v1/intents", json=req)
    req["params"] = {"n": 999}
    resp = await client.post("/v1/intents", json=req)
    assert resp.status_code == 422
    assert resp.json()["error"] == "IDEMPOTENCY_KEY_CONFLICT"


# --------------------------------------------------------------- error contract
async def test_budget_exhausted(client: httpx.AsyncClient, db: Database) -> None:
    _, grant_id, ep = await _seed(db)
    resp = await client.post("/v1/intents", json=_intent_req(grant_id, ep, amount=995))
    assert resp.status_code == 201
    resp = await client.post("/v1/intents", json=_intent_req(grant_id, ep, amount=50))
    assert resp.status_code == 409
    assert resp.json()["error"] == "BUDGET_EXHAUSTED"


async def test_lease_fenced(client: httpx.AsyncClient, db: Database) -> None:
    _, grant_id, ep = await _seed(db)
    resp = await client.post("/v1/intents", json=_intent_req(grant_id, ep, expected_epoch=99))
    assert resp.status_code == 409
    assert resp.json()["error"] == "LEASE_FENCED"


async def test_grant_inactive(client: httpx.AsyncClient, db: Database) -> None:
    _, grant_id, ep = await _seed(db)
    async with db._pool.acquire() as conn:
        await conn.execute("UPDATE grants SET status = 'REVOKED' WHERE grant_id = $1", grant_id)
    resp = await client.post("/v1/intents", json=_intent_req(grant_id, ep))
    assert resp.status_code == 409
    assert resp.json()["error"] == "GRANT_INACTIVE"


async def test_decision_not_allow_with_reasons(client: httpx.AsyncClient, db: Database) -> None:
    _, grant_id, ep = await _seed(db, signature_verified=False)
    resp = await client.post("/v1/intents", json=_intent_req(grant_id, ep))
    assert resp.status_code == 403
    body = resp.json()
    assert body["error"] == "DECISION_NOT_ALLOW"
    assert body["decision"] == "DENY"
    assert "mandate.signature_unverified" in body["reasons"]


async def test_dependency_unavailable(db: Database) -> None:
    app = build_app(db, PolicyClient("http://127.0.0.1:1", timeout=1.0))
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        _, grant_id, ep = await _seed(db)
        resp = await c.post("/v1/intents", json=_intent_req(grant_id, ep))
    assert resp.status_code == 503
    assert resp.json()["error"] == "DEPENDENCY_UNAVAILABLE"


# ------------------------------------------------------------------- receipts
async def test_receipt_applied(client: httpx.AsyncClient, db: Database) -> None:
    _, grant_id, ep = await _seed(db)
    reg = (await client.post("/v1/intents", json=_intent_req(grant_id, ep))).json()
    resp = await client.post(
        f"/v1/intents/{reg['intent_id']}/receipt", json={"receipt": {"status": "applied"}}
    )
    assert resp.status_code == 200
    assert resp.json()["classification"] == "APPLIED"
    async with db._pool.acquire() as conn:
        state = await conn.fetchval(
            "SELECT state FROM reservations WHERE intent_id = $1", reg["intent_id"]
        )
    assert state == "POSTED"


async def test_receipt_unknown_creates_obligation(client: httpx.AsyncClient, db: Database) -> None:
    _, grant_id, ep = await _seed(db)
    reg = (await client.post("/v1/intents", json=_intent_req(grant_id, ep))).json()
    resp = await client.post(
        f"/v1/intents/{reg['intent_id']}/receipt", json={"receipt": {"junk": True}}
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["classification"] == "UNKNOWN"
    assert body["obligation_id"]
    async with db._pool.acquire() as conn:
        res_state = await conn.fetchval(
            "SELECT state FROM reservations WHERE intent_id = $1", reg["intent_id"]
        )
    assert res_state == "PENDING"  # budget stays occupied (P-3)


async def test_receipt_not_applied_voids(client: httpx.AsyncClient, db: Database) -> None:
    _, grant_id, ep = await _seed(db)
    reg = (await client.post("/v1/intents", json=_intent_req(grant_id, ep))).json()
    resp = await client.post(
        f"/v1/intents/{reg['intent_id']}/receipt",
        json={"receipt": {"status": "not_applied"}},
    )
    assert resp.json()["classification"] == "NOT_APPLIED"
    async with db._pool.acquire() as conn:
        state = await conn.fetchval(
            "SELECT state FROM reservations WHERE intent_id = $1", reg["intent_id"]
        )
    assert state == "VOIDED"


# ------------------------------------------------------------------- reconcile
async def test_reconcile_skeleton_defers_without_adapters(
    client: httpx.AsyncClient, db: Database
) -> None:
    _, grant_id, ep = await _seed(db)
    reg = (await client.post("/v1/intents", json=_intent_req(grant_id, ep))).json()
    await client.post(f"/v1/intents/{reg['intent_id']}/receipt", json={"receipt": {"junk": 1}})
    resp = await client.post("/v1/reconcile", json={"limit": 10})
    assert resp.status_code == 200
    body = resp.json()
    assert body["scanned"] >= 1
    assert body["deferred"] >= 1


# -------------------------------------------------------------- episode close
async def test_close_blocked_by_unknown(client: httpx.AsyncClient, db: Database) -> None:
    _, grant_id, ep = await _seed(db)
    reg = (await client.post("/v1/intents", json=_intent_req(grant_id, ep))).json()
    await client.post(f"/v1/intents/{reg['intent_id']}/receipt", json={"receipt": {"junk": 1}})
    async with db._pool.acquire() as conn:
        await conn.execute("UPDATE episodes SET state = 'RUNNING' WHERE episode_id = $1", ep)
    resp = await client.post(f"/v1/episodes/{ep}/close", json={"terminal_branch": "not_solved"})
    assert resp.status_code == 409
    assert resp.json()["error"] == "UNRESOLVED_UNKNOWN_EXISTS"


async def test_close_blocked_by_obligations(client: httpx.AsyncClient, db: Database) -> None:
    # obligation exists but intent resolved: still blocked until ruling
    _, grant_id, ep = await _seed(db)
    reg = (await client.post("/v1/intents", json=_intent_req(grant_id, ep))).json()
    rcpt = await client.post(
        f"/v1/intents/{reg['intent_id']}/receipt", json={"receipt": {"junk": 1}}
    )
    obl_id = rcpt.json()["obligation_id"]
    # reconcile resolves intent to NOT_APPLIED but obligation stays OPEN
    async with db._pool.acquire() as conn:
        await conn.execute(
            "UPDATE action_intents SET state = 'NOT_APPLIED' WHERE intent_id = $1",
            reg["intent_id"],
        )
        await conn.execute(
            "UPDATE reservations SET state = 'VOIDED' WHERE intent_id = $1", reg["intent_id"]
        )
        await conn.execute("UPDATE episodes SET state = 'RUNNING' WHERE episode_id = $1", ep)
    resp = await client.post(f"/v1/episodes/{ep}/close", json={"terminal_branch": "not_solved"})
    assert resp.status_code == 409
    assert resp.json()["error"] == "OPEN_OBLIGATIONS_EXIST"
    assert obl_id


async def test_close_happy_path(client: httpx.AsyncClient, db: Database) -> None:
    _, grant_id, ep = await _seed(db)
    reg = (await client.post("/v1/intents", json=_intent_req(grant_id, ep))).json()
    await client.post(
        f"/v1/intents/{reg['intent_id']}/receipt", json={"receipt": {"status": "applied"}}
    )
    async with db._pool.acquire() as conn:
        await conn.execute("UPDATE episodes SET state = 'RUNNING' WHERE episode_id = $1", ep)
    resp = await client.post(
        f"/v1/episodes/{ep}/close", json={"terminal_branch": "candidate_ready"}
    )
    assert resp.status_code == 200
    assert resp.json()["state"] == "CLOSED"
    assert resp.json()["terminal_branch"] == "candidate_ready"


# ---------------------------------------------------------- concurrency + API
async def test_concurrent_same_operation_single_registration(
    client: httpx.AsyncClient, db: Database
) -> None:
    _, grant_id, ep = await _seed(db)
    req = _intent_req(grant_id, ep)
    responses = await asyncio.gather(
        client.post("/v1/intents", json=req),
        client.post("/v1/intents", json=req),
    )
    created = [r for r in responses if r.status_code == 201]
    replayed = [r for r in responses if r.status_code == 200]
    assert len(created) == 1
    assert len(replayed) == 1
    assert created[0].json()["intent_id"] == replayed[0].json()["intent_id"]
    async with db._pool.acquire() as conn:
        n = await conn.fetchval(
            "SELECT count(*) FROM action_intents WHERE idempotency_key = $1",
            req["idempotency_key"],
        )
    assert n == 1


async def test_openapi_contains_the_four_operations(db: Database, opa_url: str) -> None:
    app = build_app(db, PolicyClient(opa_url))
    paths = app.openapi()["paths"]
    for p in (
        "/v1/intents",
        "/v1/intents/{intent_id}/receipt",
        "/v1/reconcile",
        "/v1/episodes/{episode_id}/close",
    ):
        assert p in paths


# ------------------------------------------------------- WO-05 ledger wiring
async def test_gateway_with_ledger_posts_real_transfer(db: Database, opa_url: str) -> None:
    from gateway.classifiers import default_registry
    from gateway.service import GatewayService
    from kernel.ledger import InMemoryLedger
    from kernel.policy import PolicyClient as PC

    ledger = InMemoryLedger()
    # rebuild with ledger injected (service param)

    svc = GatewayService(db, PC(opa_url), default_registry(), ledger=ledger)

    mandate_id, grant_id, ep = await _seed(db)
    await ledger.ensure_budget_account(mandate_id, cap=1000)
    req = RegisterIntentRequest(**_intent_req(grant_id, ep))  # type: ignore[arg-type]
    resp = await svc.register_intent(req)
    assert resp.created is True
    # tb_transfer_id is now the ledger transfer id (= idempotency key)
    assert resp.tb_transfer_id == req.idempotency_key
    bal = await ledger.balance(mandate_id)
    assert bal.pending == req.amount  # budget occupied at engine level

    await svc.verify_receipt(resp.intent_id, ReceiptRequest(receipt={"status": "applied"}))
    bal = await ledger.balance(mandate_id)
    assert (bal.posted, bal.pending) == (req.amount, 0)  # posted on success
