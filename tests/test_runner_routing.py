"""WO-104 scheduling-routing assertions (eval-gate suite wo104, gate G2).

Given work-order routing metadata:
  sandbox_tier="isolated" -> scheduling decision runtime == runsc
  explicit standard tier  -> default runtime (never runsc)
  no marker               -> default runtime
  unknown tier            -> default runtime (fail-safe)
  runsc unregistered      -> SchedulerError (fail-closed: an isolated-tier
                             task never silently downgrades isolation)

Pure unit tests — no docker, no PG, no OPA (the router is a pure function;
the executor seam is exercised with stub runners).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pytest

from kernel.executor.episode import EpisodeExecutor
from kernel.runner.base import Runner, TaskResult, TaskSpec
from kernel.runner.local import LocalSubprocessRunner
from kernel.runner.runsc import RunscRunner
from kernel.runner.scheduler import (
    DEFAULT_RUNTIME,
    RUNSC_RUNTIME,
    SchedulerError,
    default_registry,
    resolve_runtime,
    select_runner,
)

ISOLATED: dict[str, Any] = {"sandbox_tier": "isolated"}
STANDARD: dict[str, Any] = {"sandbox_tier": "standard"}


def test_isolated_tier_routes_to_runsc() -> None:
    assert resolve_runtime(ISOLATED) == RUNSC_RUNTIME
    runner = select_runner(ISOLATED, default_registry())
    assert runner.name == RUNSC_RUNTIME
    assert isinstance(runner, RunscRunner)


def test_standard_tier_routes_to_default_never_runsc() -> None:
    assert resolve_runtime(STANDARD) == DEFAULT_RUNTIME
    assert DEFAULT_RUNTIME != RUNSC_RUNTIME
    runner = select_runner(STANDARD, default_registry())
    assert runner.name != RUNSC_RUNTIME
    assert runner.name == DEFAULT_RUNTIME


@pytest.mark.parametrize("metadata", [{}, None, {"other": "attr"}])
def test_unmarked_task_routes_to_default(metadata: dict[str, Any] | None) -> None:
    assert resolve_runtime(metadata) == DEFAULT_RUNTIME
    runner = select_runner(metadata, default_registry())
    assert runner.name == DEFAULT_RUNTIME


def test_unknown_tier_fails_safe_to_default() -> None:
    unknown: dict[str, Any] = {"sandbox_tier": "paranoid"}
    assert resolve_runtime(unknown) == DEFAULT_RUNTIME
    runner = select_runner(unknown, default_registry())
    assert runner.name == DEFAULT_RUNTIME


def test_missing_runsc_registration_fails_closed() -> None:
    registry = {LocalSubprocessRunner.name: LocalSubprocessRunner()}
    with pytest.raises(SchedulerError):
        select_runner(ISOLATED, registry)


def test_default_lane_degrades_to_injected_default() -> None:
    fallback: Runner = LocalSubprocessRunner()
    assert select_runner({}, {}, default=fallback) is fallback


def test_task_spec_carries_routing_metadata() -> None:
    spec = TaskSpec(work_order_id="wo-104", episode_id="ep-1", commands=["true"], metadata=ISOLATED)
    assert spec.metadata == ISOLATED
    bare = TaskSpec(work_order_id="wo-104", episode_id="ep-1", commands=["true"])
    assert bare.metadata == {}


class _StubRunner:
    name = "stub"

    async def run(self, spec: TaskSpec, workdir: Path) -> TaskResult:
        raise AssertionError("stub runner must not execute in routing tests")


def test_executor_seam_selects_by_metadata() -> None:
    executor = EpisodeExecutor(
        db=cast(Any, None),
        gateway=cast(Any, None),
        runner=LocalSubprocessRunner(),
        runner_registry={RUNSC_RUNTIME: RunscRunner(), "stub": _StubRunner()},
    )
    assert executor._runner_for(ISOLATED).name == RUNSC_RUNTIME
    assert executor._runner_for(STANDARD).name == DEFAULT_RUNTIME
    assert executor._runner_for(None).name == DEFAULT_RUNTIME


def test_executor_seam_without_registry_keeps_injected_runner() -> None:
    injected = LocalSubprocessRunner()
    executor = EpisodeExecutor(
        db=cast(Any, None),
        gateway=cast(Any, None),
        runner=injected,
    )
    assert executor._runner_for(ISOLATED) is injected
