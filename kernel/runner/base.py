"""kernel/runner — task runners (WO-04).

A Runner executes ONE bounded task inside an execution context (local
subprocess for dev/tests; JiuwenBox sandbox in production; gVisor runsc
for the isolated tier — WO-104). The executor layer owns state/budget/
evidence — runners only do work and report.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol


class RunnerError(Exception):
    pass


@dataclass(frozen=True, slots=True)
class TaskSpec:
    work_order_id: str
    episode_id: str
    commands: list[str]
    artifacts: list[str] = field(default_factory=list)  # relative paths to collect
    # WO-104: work-order routing attributes (the scheduler reads sandbox_tier
    # from here); echoed into audit trails, never part of the artifact payload
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class TaskResult:
    exit_code: int
    stdout: str
    stderr: str
    artifacts: dict[str, bytes]  # name -> content
    sandbox_id: str = ""  # ephemeral context id (destroyed on stop)


class Runner(Protocol):
    name: str

    async def run(self, spec: TaskSpec, workdir: Path) -> TaskResult: ...
