"""WO-101 x WO-104 — episode-mode audit anchor.

The eval harness runs its task through EpisodeExecutor with metadata
`sandbox_tier: "isolated"`; scheduler routing must pick the runsc runner
and the emitted episode.task_finished audit event must carry
`runtime=runsc` (the WO-104 G3 backfill anchor). Docker/runsc is never
invoked here: the runner seat is a fake named "runsc", exercising the real
routing + emission code path.
"""

from __future__ import annotations

import os
import socket
import subprocess
import time
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest

from gateway.classifiers import default_registry as default_classifier_registry
from gateway.service import GatewayService
from kernel.db import Database
from kernel.executor.episode import EpisodeExecutor
from kernel.policy import PolicyClient
from kernel.runner.base import TaskResult, TaskSpec
from kernel.runner.runsc import RunscRunner
from kernel.runner.scheduler import default_registry, resolve_runtime

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


class FakeRunscRunner:
    """Stands in the runsc seat: never touches docker, records the spec."""

    name = "runsc"

    def __init__(self) -> None:
        self.specs: list[TaskSpec] = []

    async def run(self, spec: TaskSpec, workdir: Path) -> TaskResult:
        self.specs.append(spec)
        return TaskResult(
            exit_code=0,
            stdout="",
            stderr="",
            artifacts={},
            sandbox_id=f"runsc:{spec.episode_id}",
        )


def test_resolve_runtime_isolated_routes_to_runsc() -> None:
    assert resolve_runtime({"sandbox_tier": "isolated"}) == "runsc"
    assert resolve_runtime({}) != "runsc"  # default lane never lands on runsc


def test_default_registry_serves_runsc_runner() -> None:
    registry = default_registry()
    assert isinstance(registry["runsc"], RunscRunner)
    assert registry["runsc"].name == "runsc"


async def test_run_task_isolated_tier_emits_runtime_runsc(db: Database, opa_url: str) -> None:
    runner = FakeRunscRunner()
    events: list[dict[str, Any]] = []

    async def on_event(event: dict[str, Any]) -> None:
        events.append(event)

    gateway = GatewayService(db, PolicyClient(opa_url), default_classifier_registry())
    executor = EpisodeExecutor(
        db,
        gateway,
        runner_registry={"runsc": runner},
        on_event=on_event,
    )
    grant_id = await _seed(db)
    episode_id = f"wo101-recon-{uuid.uuid4().hex[:8]}"
    await executor.start_episode(episode_id)
    await executor.run_task(
        episode_id=episode_id,
        grant_id=grant_id,
        commands=["true"],
        artifacts=[],
        workdir=Path("/tmp"),
        metadata={"sandbox_tier": "isolated", "work_order": "WO-101"},
    )

    assert runner.specs and runner.specs[0].metadata["sandbox_tier"] == "isolated"
    finished = [e for e in events if e.get("type") == "episode.task_finished"]
    assert len(finished) == 1
    event = finished[0]
    assert event["runtime"] == "runsc", "WO-104 G3 anchor: audit event must carry runtime=runsc"
    assert event["sandbox_id"].startswith("runsc:")


async def test_run_task_default_lane_stays_off_runsc(db: Database, opa_url: str) -> None:
    runner = FakeRunscRunner()
    events: list[dict[str, Any]] = []

    async def on_event2(event: dict[str, Any]) -> None:
        events.append(event)

    gateway = GatewayService(db, PolicyClient(opa_url), default_classifier_registry())
    executor = EpisodeExecutor(
        db,
        gateway,
        runner=RunnerSeatProbe(),
        runner_registry={"runsc": runner},
        on_event=on_event2,
    )
    grant_id = await _seed(db)
    episode_id = f"wo101-recon-{uuid.uuid4().hex[:8]}"
    await executor.start_episode(episode_id)
    await executor.run_task(
        episode_id=episode_id,
        grant_id=grant_id,
        commands=["true"],
        artifacts=[],
        workdir=Path("/tmp"),
        metadata={},
    )
    assert not runner.specs, "default-lane task must never reach the runsc runner"
    event = next(e for e in events if e.get("type") == "episode.task_finished")
    assert event["runtime"] != "runsc"


class RunnerSeatProbe:
    name = "probe"

    async def run(self, spec: TaskSpec, workdir: Path) -> TaskResult:
        return TaskResult(exit_code=0, stdout="", stderr="", artifacts={})


async def _seed(db: Database) -> str:
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
