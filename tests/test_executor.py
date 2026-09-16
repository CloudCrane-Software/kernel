"""WO-04 integration tests — the M1 dress rehearsal.

Work order under test: "generate a hello.py and run it". Assertions per the
manual §5.4 acceptance:
  - episode walks RESERVED -> RUNNING -> VERIFYING -> CLOSED.candidate_ready
  - sandbox-produced files return THROUGH the gateway (artifact.store intent,
    receipt-settled)
  - audit events carry the full transition sequence (incl. model_cost)
  - crash (executor lost) -> recover() preserves state, asserts the fence
    invariant, resumes without replaying settled effects
"""

from __future__ import annotations

import os
import socket
import subprocess
import time
import uuid
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
import pytest_asyncio

from gateway.classifiers import default_registry
from gateway.service import GatewayService
from kernel.db import Database
from kernel.executor.episode import EpisodeExecutor, ExecutorError
from kernel.policy import PolicyClient
from kernel.runner.local import LocalSubprocessRunner

POLICIES_DIR = Path(__file__).resolve().parent.parent / "policies"


@pytest.fixture(scope="module")
def opa_url() -> Iterator[str]:
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
async def stack(
    db: Database, opa_url: str, tmp_path: Path
) -> AsyncIterator[tuple[GatewayService, EpisodeExecutor, list[dict[str, Any]]]]:
    events: list[dict[str, Any]] = []

    async def on_event(event: dict[str, Any]) -> None:
        events.append(event)

    gateway = GatewayService(db, PolicyClient(opa_url), default_registry())
    executor = EpisodeExecutor(db, gateway, runner=LocalSubprocessRunner(), on_event=on_event)
    yield gateway, executor, events


async def _seed(db: Database) -> str:
    import uuid

    mandate_id = f"mnd-{uuid.uuid4().hex[:10]}"
    grant_id = f"grt-{uuid.uuid4().hex[:10]}"
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
        scope={
            "actions": ["*"],
            "resources": ["*"],
            "limits": {"call_limit": 1000, "budget_limit": 10000},
        },
        remaining_depth=3,
        expiry="2099-01-01T00:00:00Z",
    )
    return grant_id


async def _episode_state(db: Database, episode_id: str) -> tuple[str, str | None]:
    async with db._pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT state, terminal_branch FROM episodes WHERE episode_id = $1", episode_id
        )
    return (row["state"], row["terminal_branch"]) if row else ("missing", None)


# ---------------------------------------------------------------- happy path
async def test_hello_py_work_order_full_loop(
    db: Database,
    stack: tuple[GatewayService, EpisodeExecutor, list[dict[str, Any]]],
    tmp_path: Path,
) -> None:
    gateway, executor, events = stack
    grant_id = await _seed(db)
    episode_id = f"ep-hello-{uuid.uuid4().hex[:8]}"

    await executor.start_episode(episode_id)
    assert await _episode_state(db, episode_id) == ("RESERVED", None)

    intents = await executor.run_task(
        episode_id=episode_id,
        grant_id=grant_id,
        commands=[
            "printf 'print(\"hello from the sandbox\")\\n' > hello.py",
            "python3 hello.py",
        ],
        artifacts=["hello.py"],
        workdir=tmp_path / episode_id,
    )
    assert intents == [f"model-cost-{episode_id}"]
    assert await _episode_state(db, episode_id) == ("VERIFYING", None)

    # artifacts came back THROUGH the gateway: artifact.store intent settled
    async with db._pool.acquire() as conn:
        row = await conn.fetchrow(
            """SELECT i.state AS intent_state, r.state AS res_state
               FROM action_intents i LEFT JOIN reservations r ON r.intent_id = i.intent_id
               WHERE i.intent_id = $1""",
            f"artifact-store-{episode_id}",
        )
    assert row is not None
    assert row["intent_state"] == "APPLIED"
    assert row["res_state"] == "POSTED"

    # model_cost always accounted (zero business action or not)
    async with db._pool.acquire() as conn:
        mc = await conn.fetchval(
            "SELECT count(*) FROM action_intents WHERE intent_id = $1",
            f"model-cost-{episode_id}",
        )
    assert mc == 1

    await executor.resolve_verification(episode_id, "candidate_ready")
    assert await _episode_state(db, episode_id) == ("CLOSED", "candidate_ready")

    # audit events: full transition sequence + task + artifact + closed
    kinds = [e["type"] for e in events]
    transitions = [e["state"] for e in events if e["type"] == "episode.transition"]
    assert transitions == ["RUNNING", "VERIFYING"]
    assert "episode.task_finished" in kinds
    assert "episode.artifacts_deposited" in kinds
    assert "episode.closed" in kinds
    assert (
        tmp_path / episode_id / "hello.py"
    ).read_text().strip() == 'print("hello from the sandbox")'


# ------------------------------------------------------------- crash + resume
async def test_crash_recovery_preserves_state_and_fence(
    db: Database,
    stack: tuple[GatewayService, EpisodeExecutor, list[dict[str, Any]]],
    tmp_path: Path,
) -> None:
    gateway, executor, events = stack
    grant_id = await _seed(db)
    episode_id = f"ep-crash-{uuid.uuid4().hex[:8]}"
    await executor.start_episode(episode_id)
    await executor.run_task(
        episode_id=episode_id,
        grant_id=grant_id,
        commands=["echo work > out.txt"],
        artifacts=["out.txt"],
        workdir=tmp_path / episode_id,
    )

    # executor dies; a NEW executor instance recovers (state lives in PG)
    gateway2 = GatewayService(db, PolicyClient("http://127.0.0.1:1"))  # offline ok for recover
    executor2 = EpisodeExecutor(db, gateway2, runner=LocalSubprocessRunner())
    report = await executor2.recover(episode_id)
    assert report["fence_ok"] is True
    assert report["state"] == "VERIFYING"
    assert report["lease_epoch"] >= report["max_intent_epoch"]
    assert any(i["intent_id"] == f"model-cost-{episode_id}" for i in report["intents"])

    # settled effects were NOT replayed: still exactly one model-cost intent
    async with db._pool.acquire() as conn:
        n = await conn.fetchval(
            "SELECT count(*) FROM action_intents WHERE intent_id = $1",
            f"model-cost-{episode_id}",
        )
    assert n == 1

    # resume and close through a different branch
    await executor2.resolve_verification(episode_id, "not_solved")
    assert await _episode_state(db, episode_id) == ("CLOSED", "not_solved")
    del events  # recovered event goes to executor2's own channel (none attached)


# --------------------------------------------------------- fence invariant
async def test_recover_detects_fence_violation(
    db: Database,
    stack: tuple[GatewayService, EpisodeExecutor, list[dict[str, Any]]],
    tmp_path: Path,
) -> None:
    gateway, executor, _ = stack
    grant_id = await _seed(db)
    episode_id = f"ep-fence-{uuid.uuid4().hex[:8]}"
    await executor.start_episode(episode_id)
    await executor.run_task(
        episode_id=episode_id,
        grant_id=grant_id,
        commands=["echo x > f.txt"],
        artifacts=["f.txt"],
        workdir=tmp_path / episode_id,
    )
    # sabotage: rewind the lease below the intents' epochs — recover must refuse
    async with db._pool.acquire() as conn:
        await conn.execute(
            "UPDATE leases SET epoch = 0 WHERE lease_id = $1", f"episode:{episode_id}"
        )
    with pytest.raises(ExecutorError, match="fence"):
        await executor.recover(episode_id)


async def test_terminal_branch_must_be_explicit(
    stack: tuple[GatewayService, EpisodeExecutor, list[dict[str, Any]]],
) -> None:
    _, executor, _ = stack
    with pytest.raises(ExecutorError):
        await executor.resolve_verification("ep-x", "somehow_open")
