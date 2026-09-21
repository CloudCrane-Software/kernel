"""WO-104 scheduling-routing assertions (eval-gate suite wo104, gate G2).

Given work-order routing metadata:
  sandbox_tier="isolated" -> scheduling decision runtime == runsc
  explicit standard tier  -> default runtime (never runsc)
  no marker               -> default runtime
  unknown tier            -> default runtime (fail-safe)
  runsc unregistered      -> SchedulerError (fail-closed: an isolated-tier
                             task never silently downgrades isolation)

Plus the WO-108 F1 wiring gates: the production compositions (gateway
service, restate app builder) must actually PASS a sandbox registry — the
scheduler was always correct, the assembly was not.

Pure unit tests — no docker, no PG, no OPA (the router is a pure function;
the executor seam is exercised with stub runners).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pytest

from gateway.service import GatewayService
from kernel.executor.episode import EpisodeExecutor
from kernel.runner.base import Runner, TaskResult, TaskSpec
from kernel.runner.local import LocalSubprocessRunner
from kernel.runner.runsc import RunscRunner
from kernel.runner.scheduler import (
    DEFAULT_RUNTIME,
    RUNSC_RUNTIME,
    SchedulerError,
    default_registry,
    production_registry,
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


# ------------------------------------------------------------------ WO-108 F1
# Production wiring gates: the gateway service and the restate app builder
# must hand their executors a sandbox registry. Before WO-108 both assembled
# EpisodeExecutor without one, so an isolated-tier task silently degraded to
# the default runner on the live paths.


def _gateway_service(runsc_available: bool) -> GatewayService:
    return GatewayService(
        db=cast(Any, None),
        policy=cast(Any, None),
        runner_registry=production_registry(runsc_available),
    )


def test_gateway_path_isolated_routes_to_runsc() -> None:
    service = _gateway_service(runsc_available=True)
    assert service._executor.runner_registry is not None
    runner = service._executor._runner_for(ISOLATED)
    assert runner.name == RUNSC_RUNTIME
    assert isinstance(runner, RunscRunner)


def test_gateway_path_without_runsc_fails_closed_not_silent() -> None:
    service = _gateway_service(runsc_available=False)
    with pytest.raises(SchedulerError):
        service._executor._runner_for(ISOLATED)


def test_gateway_path_standard_lane_unaffected_by_registry() -> None:
    service = _gateway_service(runsc_available=False)
    assert service._executor._runner_for(STANDARD).name == DEFAULT_RUNTIME
    assert service._executor._runner_for(None).name == DEFAULT_RUNTIME
    assert service._executor._runner_for({}).name == DEFAULT_RUNTIME


def test_gateway_service_wires_production_registry_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("KERNEL_RUNSC_ENABLED", "0")  # deterministic: empty registry
    service = GatewayService(db=cast(Any, None), policy=cast(Any, None))
    assert service._executor.runner_registry == {}
    monkeypatch.setenv("KERNEL_RUNSC_ENABLED", "1")  # host asserts runsc
    wired = GatewayService(db=cast(Any, None), policy=cast(Any, None))
    registry = wired._executor.runner_registry
    assert registry is not None
    assert isinstance(registry[RUNSC_RUNTIME], RunscRunner)


def test_restate_build_service_wires_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    from kernel.executor.restate_app import build_service

    monkeypatch.setenv("KERNEL_RUNSC_ENABLED", "1")
    service = build_service(cast(Any, None), cast(Any, None))
    registry = service._executor.runner_registry
    assert registry is not None
    assert isinstance(registry[RUNSC_RUNTIME], RunscRunner)


def test_production_registry_env_overrides_binary_detection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("KERNEL_RUNSC_ENABLED", "0")
    assert production_registry() == {}
    monkeypatch.setenv("KERNEL_RUNSC_ENABLED", "1")
    assert RUNSC_RUNTIME in production_registry()
    monkeypatch.setenv("KERNEL_RUNSC_ENABLED", "yes")
    assert RUNSC_RUNTIME in production_registry()


def test_production_registry_detects_runsc_binary(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("KERNEL_RUNSC_ENABLED", raising=False)
    monkeypatch.setenv("PATH", str(tmp_path))
    # no binary -> empty registry (fail closed for the isolated tier only)
    assert production_registry() == {}
    fake = tmp_path / "runsc"
    fake.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    fake.chmod(0o755)
    assert RUNSC_RUNTIME in production_registry()
