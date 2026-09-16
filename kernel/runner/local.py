"""Local subprocess runner — dev/tests and CI integration tests.

Real process isolation (separate cwd), no network restrictions: this runner
is for TRUSTED dev work orders only. Production agents run via JiuwenBoxRunner
(sandboxed, egress default-deny)."""

from __future__ import annotations

import asyncio
from pathlib import Path

from kernel.runner.base import RunnerError, TaskResult, TaskSpec


class LocalSubprocessRunner:
    name = "local-subprocess"

    async def run(self, spec: TaskSpec, workdir: Path) -> TaskResult:
        workdir.mkdir(parents=True, exist_ok=True)
        stdout_parts: list[str] = []
        stderr_parts: list[str] = []
        exit_code = 0
        for cmd in spec.commands:
            proc = await asyncio.create_subprocess_shell(
                cmd,
                cwd=str(workdir),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            out, err = await proc.communicate()
            stdout_parts.append(out.decode("utf-8", errors="replace"))
            stderr_parts.append(err.decode("utf-8", errors="replace"))
            if proc.returncode is not None and proc.returncode != 0:
                exit_code = proc.returncode
                break

        artifacts: dict[str, bytes] = {}
        for rel in spec.artifacts:
            path = workdir / rel
            if path.is_file():
                artifacts[rel] = path.read_bytes()
            else:
                raise RunnerError(f"expected artifact missing: {rel}")

        return TaskResult(
            exit_code=exit_code,
            stdout="".join(stdout_parts),
            stderr="".join(stderr_parts),
            artifacts=artifacts,
            sandbox_id=f"local:{spec.episode_id}",
        )
