"""Episode lifecycle endpoint tests (WO-0004): POST /v1/workorders and
POST /v1/episodes/{id}/transition; plus POST /v1/episodes/{id}/close,
which joins the privileged surface in WO-107.

Pins:
- seeding goes through the executor write path (episodes row + lease fencing);
- the one-way transition map mirrors trg_episodes_one_way exactly;
- the privileged surface fails closed (401 without a configured bearer);
- idempotency contracts (201/200 on create, same-state no-op on transition).
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator

import httpx
import pytest_asyncio

from gateway.app import build_app
from gateway.schemas import ErrorBody
from kernel.db import Database
from kernel.policy import PolicyClient

_ADMIN_TOKEN = "wo0004-test-admin-token"


@pytest_asyncio.fixture
async def client(db: Database) -> AsyncIterator[httpx.AsyncClient]:
    app = build_app(db, PolicyClient("http://127.0.0.1:1"), admin_token=_ADMIN_TOKEN)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest_asyncio.fixture
async def open_client(db: Database) -> AsyncIterator[httpx.AsyncClient]:
    """No admin token configured: the privileged surface must fail closed."""
    app = build_app(db, PolicyClient("http://127.0.0.1:1"), admin_token=None)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


def _auth() -> dict[str, str]:
    return {"Authorization": f"Bearer {_ADMIN_TOKEN}"}


async def _create(client: httpx.AsyncClient, workorder_id: str) -> httpx.Response:
    return await client.post(
        "/v1/workorders",
        headers=_auth(),
        json={"workorder_id": workorder_id, "metadata": {"source": "test"}},
    )


async def _transition(
    client: httpx.AsyncClient,
    episode_id: str,
    target: str,
    branch: str | None = None,
) -> httpx.Response:
    body: dict[str, str] = {"target_state": target}
    if branch is not None:
        body["terminal_branch"] = branch
    return await client.post(f"/v1/episodes/{episode_id}/transition", headers=_auth(), json=body)


async def test_create_workorder_seeds_reserved_episode(
    client: httpx.AsyncClient, db: Database
) -> None:
    wo = _id("wo")
    resp = await _create(client, wo)
    assert resp.status_code == 201
    data = resp.json()
    assert data["episode_id"] == wo
    assert data["workorder_id"] == wo
    assert data["state"] == "RESERVED"
    assert data["created"] is True
    assert data["metadata"] == {"source": "test"}
    # the write path is the executor's: episodes row + lease fencing exist
    async with db._pool.acquire() as conn:
        state = await conn.fetchval("SELECT state FROM episodes WHERE episode_id = $1", wo)
        epoch = await conn.fetchval("SELECT epoch FROM leases WHERE lease_id = $1", f"episode:{wo}")
    assert state == "RESERVED"
    assert epoch == 1


async def test_create_workorder_idempotent_replay_returns_current_value(
    client: httpx.AsyncClient,
) -> None:
    wo = _id("wo")
    first = await _create(client, wo)
    assert first.status_code == 201
    replay = await _create(client, wo)
    assert replay.status_code == 200
    assert replay.json()["created"] is False
    assert replay.json()["state"] == "RESERVED"

    assert (await _transition(client, wo, "RUNNING")).status_code == 200
    replay2 = await _create(client, wo)
    assert replay2.status_code == 200
    assert replay2.json()["created"] is False
    assert replay2.json()["state"] == "RUNNING"  # returns the CURRENT value


async def test_full_lifecycle_to_candidate_ready(client: httpx.AsyncClient, db: Database) -> None:
    wo = _id("wo")
    assert (await _create(client, wo)).status_code == 201

    r1 = await _transition(client, wo, "RUNNING")
    assert r1.status_code == 200
    assert r1.json()["previous_state"] == "RESERVED"
    assert r1.json()["state"] == "RUNNING"
    assert r1.json()["terminal_branch"] is None

    r2 = await _transition(client, wo, "VERIFYING")
    assert r2.status_code == 200
    assert r2.json()["previous_state"] == "RUNNING"

    r3 = await _transition(client, wo, "CLOSED", branch="candidate_ready")
    assert r3.status_code == 200
    assert r3.json()["previous_state"] == "VERIFYING"
    assert r3.json()["state"] == "CLOSED"
    assert r3.json()["terminal_branch"] == "candidate_ready"

    async with db._pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT state, terminal_branch FROM episodes WHERE episode_id = $1", wo
        )
    assert row is not None
    assert row["state"] == "CLOSED"
    assert row["terminal_branch"] == "candidate_ready"


async def test_running_to_closed_direct_matches_trigger(client: httpx.AsyncClient) -> None:
    """RUNNING -> CLOSED is a legal edge of trg_episodes_one_way (shortcut)."""
    wo = _id("wo")
    await _create(client, wo)
    assert (await _transition(client, wo, "RUNNING")).status_code == 200
    r = await _transition(client, wo, "CLOSED", branch="not_solved")
    assert r.status_code == 200
    assert r.json()["state"] == "CLOSED"


async def test_same_state_transition_is_idempotent_noop(client: httpx.AsyncClient) -> None:
    wo = _id("wo")
    await _create(client, wo)
    r = await _transition(client, wo, "RESERVED")
    assert r.status_code == 200
    assert r.json()["state"] == "RESERVED"
    assert r.json()["previous_state"] == "RESERVED"


async def test_closed_is_terminal(client: httpx.AsyncClient) -> None:
    wo = _id("wo")
    await _create(client, wo)
    await _transition(client, wo, "RUNNING")
    assert (await _transition(client, wo, "CLOSED", branch="candidate_ready")).status_code == 200

    # CLOSED -> RUNNING must be refused (one-way)
    r = await _transition(client, wo, "RUNNING")
    assert r.status_code == 409
    assert r.json()["error"] == "ILLEGAL_TRANSITION"

    # same-state replay with the SAME branch is a no-op
    replay = await _transition(client, wo, "CLOSED", branch="candidate_ready")
    assert replay.status_code == 200
    assert replay.json()["state"] == "CLOSED"

    # rewriting the terminal branch is refused
    rewrite = await _transition(client, wo, "CLOSED", branch="expired")
    assert rewrite.status_code == 409


async def test_illegal_transitions_are_409(client: httpx.AsyncClient) -> None:
    wo = _id("wo")
    await _create(client, wo)
    # RESERVED may only go to RUNNING
    assert (await _transition(client, wo, "CLOSED", branch="candidate_ready")).status_code == 409
    assert (await _transition(client, wo, "VERIFYING")).status_code == 409

    await _transition(client, wo, "RUNNING")
    await _transition(client, wo, "VERIFYING")
    # backwards edges are refused
    backwards = await _transition(client, wo, "RUNNING")
    assert backwards.status_code == 409
    assert backwards.json()["error"] == "ILLEGAL_TRANSITION"
    assert (await _transition(client, wo, "RESERVED")).status_code == 409


async def test_transition_unknown_episode_404(client: httpx.AsyncClient) -> None:
    r = await _transition(client, _id("wo"), "RUNNING")
    assert r.status_code == 404
    assert r.json()["error"] == "NOT_FOUND"


async def test_transition_branch_validation_422(client: httpx.AsyncClient) -> None:
    wo = _id("wo")
    await _create(client, wo)
    # CLOSED without a branch
    r = await client.post(
        f"/v1/episodes/{wo}/transition",
        headers=_auth(),
        json={"target_state": "CLOSED"},
    )
    assert r.status_code == 422
    # branch on a non-CLOSED target
    r = await client.post(
        f"/v1/episodes/{wo}/transition",
        headers=_auth(),
        json={"target_state": "RUNNING", "terminal_branch": "candidate_ready"},
    )
    assert r.status_code == 422
    # invalid branch value
    r = await client.post(
        f"/v1/episodes/{wo}/transition",
        headers=_auth(),
        json={"target_state": "CLOSED", "terminal_branch": "whatever"},
    )
    assert r.status_code == 422
    # invalid target state value
    r = await client.post(
        f"/v1/episodes/{wo}/transition", headers=_auth(), json={"target_state": "PAUSED"}
    )
    assert r.status_code == 422


async def test_missing_token_401(client: httpx.AsyncClient) -> None:
    wo = _id("wo")
    r = await client.post("/v1/workorders", json={"workorder_id": wo})
    assert r.status_code == 401
    assert r.json()["error"] == "UNAUTHORIZED"
    r = await client.post(f"/v1/episodes/{wo}/transition", json={"target_state": "RUNNING"})
    assert r.status_code == 401
    r = await client.post(f"/v1/episodes/{wo}/close", json={"terminal_branch": "candidate_ready"})
    assert r.status_code == 401


async def test_wrong_token_401(client: httpx.AsyncClient) -> None:
    wo = _id("wo")
    r = await client.post(
        "/v1/workorders",
        headers={"Authorization": "Bearer not-the-token"},
        json={"workorder_id": wo},
    )
    assert r.status_code == 401
    # non-bearer scheme is refused too
    r = await client.post(
        "/v1/workorders",
        headers={"Authorization": f"Basic {_ADMIN_TOKEN}"},
        json={"workorder_id": wo},
    )
    assert r.status_code == 401
    r = await client.post(
        f"/v1/episodes/{wo}/close",
        headers={"Authorization": "Bearer not-the-token"},
        json={"terminal_branch": "candidate_ready"},
    )
    assert r.status_code == 401


async def test_unconfigured_token_fails_closed(open_client: httpx.AsyncClient) -> None:
    r = await open_client.post("/v1/workorders", json={"workorder_id": _id("wo")})
    assert r.status_code == 401
    assert r.json()["error"] == "UNAUTHORIZED"
    detail = r.json()["detail"]
    assert "not configured" in detail
    r = await open_client.post(
        f"/v1/episodes/{_id('wo')}/close", json={"terminal_branch": "candidate_ready"}
    )
    assert r.status_code == 401


async def test_error_body_schema_matches_gateway_contract() -> None:
    """The 401/409 bodies reuse the WO-03 ErrorBody shape (sanity pin)."""
    body = ErrorBody(error="UNAUTHORIZED", detail="x")
    assert body.model_dump() == {
        "error": "UNAUTHORIZED",
        "detail": "x",
        "decision": None,
        "reasons": [],
    }
