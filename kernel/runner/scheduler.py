"""Sandbox scheduling router — runtime selection by work-order metadata (WO-104).

The gVisor strong-isolation profile: a work order carrying
`sandbox_tier: "isolated"` in its metadata is scheduled onto the runsc
runtime; everything else (explicit standard tier, unknown tier, no marker)
stays on the default lane — runsc is never the implicit default.

`sandbox_tier` rides in CreateWorkorderRequest.metadata (gateway/schemas.py:
free-form dict, snake_case convention — the only existing work-order
metadata surface) and flows into TaskSpec.metadata via
EpisodeExecutor.run_task. Resolution is a pure function, fully asserted in
CI (eval-gate suite wo104, gate G2). Selection is fail-safe by design:

- unknown/missing registration for a DEFAULT task -> injected default runner;
- missing runsc registration for an ISOLATED task -> SchedulerError
  (fail-closed: a high-risk task must never silently lose isolation).
"""

from __future__ import annotations

import os
import shutil
from collections.abc import Mapping
from typing import Any

from kernel.runner.base import Runner
from kernel.runner.local import LocalSubprocessRunner
from kernel.runner.runsc import RunscRunner

SANDBOX_TIER_KEY = "sandbox_tier"
ISOLATED_TIER = "isolated"
RUNSC_RUNTIME = "runsc"
DEFAULT_RUNTIME = LocalSubprocessRunner.name  # always-available dev fallback


class SchedulerError(Exception):
    pass


def resolve_runtime(metadata: Mapping[str, Any] | None) -> str:
    """Pure routing decision: task metadata -> runtime name (the G2 surface)."""
    tier = (metadata or {}).get(SANDBOX_TIER_KEY)
    return RUNSC_RUNTIME if tier == ISOLATED_TIER else DEFAULT_RUNTIME


def default_registry() -> dict[str, Runner]:
    """Built-in runtimes keyed by name. Production compositions extend this
    (e.g. a configured JiuwenBoxRunner injected via EpisodeExecutor)."""
    return {
        LocalSubprocessRunner.name: LocalSubprocessRunner(),
        RunscRunner.name: RunscRunner(),
    }


def select_runner(
    metadata: Mapping[str, Any] | None,
    registry: Mapping[str, Runner],
    default: Runner | None = None,
) -> Runner:
    """Pick the runner for one task. See module docstring for the failure
    posture: default tasks degrade, isolated tasks fail closed."""
    runtime = resolve_runtime(metadata)
    runner = registry.get(runtime)
    if runner is not None:
        return runner
    if runtime == RUNSC_RUNTIME:
        raise SchedulerError(
            f"runtime '{RUNSC_RUNTIME}' not registered; refusing to run an "
            "isolated-tier task without isolation"
        )
    if default is not None:
        return default
    return LocalSubprocessRunner()


def production_registry(runsc_available: bool | None = None) -> dict[str, Runner]:
    """Registry for the production compositions (gateway service, restate app).

    WO-108 F1: those compositions assembled EpisodeExecutor without a
    runner_registry, so an isolated-tier task silently degraded to the default
    runner — the WO-104 fail-closed promise was false on the live paths. This
    is the registry they pass now:

    - runsc available -> {"runsc": RunscRunner()}: isolated routes to gVisor;
    - runsc missing   -> {} (empty): default-lane tasks keep working through
      the injected default runner, while isolated-tier tasks raise
      SchedulerError in select_runner — never a silent downgrade.

    Availability resolution (first match wins):
      1. explicit `runsc_available` argument (tests / programmatic callers);
      2. KERNEL_RUNSC_ENABLED env ("1"/"true"/"yes" or "0"/"false"/"no") — a
         containerized gateway talks to a HOST docker daemon, so the runsc
         binary lives outside its own filesystem; this is the deploy-time
         assertion that the host provides the runsc runtime;
      3. the `runsc` binary on PATH (host-level compositions).
    """
    if runsc_available is None:
        env = os.environ.get("KERNEL_RUNSC_ENABLED", "").strip().lower()
        if env in {"1", "true", "yes"}:
            runsc_available = True
        elif env in {"0", "false", "no"}:
            runsc_available = False
        else:
            runsc_available = shutil.which("runsc") is not None
    return {RUNSC_RUNTIME: RunscRunner()} if runsc_available else {}
