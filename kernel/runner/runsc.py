"""gVisor runsc runner — strong-isolation execution profile (WO-104).

Runs each task in an ephemeral Docker container on the host's `runsc`
runtime (gVisor user-space kernel; G1: registered in docker Runtimes,
release-20260914.0 at /usr/local/bin/runsc). This is the "isolated" sandbox
tier: ONLY work orders that opt in via metadata sandbox_tier="isolated"
route here (kernel/runner/scheduler.py) — it is never the implicit default.

Contract this round (WO-104 kernel side): the code ships and is unit-tested
at the scheduling layer; execution activates when the coordinator redeploys
the gateway image (0.1.4, `up -d --no-deps gateway`) on the runsc-capable
host. Tests assert routing decisions only — they never invoke docker.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from kernel.runner.base import RunnerError, TaskResult, TaskSpec


class RunscRunner:
    name = "runsc"

    def __init__(
        self,
        image: str = "kernel-task:latest",
        cli: str = "docker",
        runtime: str = "runsc",
    ) -> None:
        self._image = image
        self._cli = cli
        self._runtime = runtime

    async def run(self, spec: TaskSpec, workdir: Path) -> TaskResult:
        workdir.mkdir(parents=True, exist_ok=True)
        # task.sh rides the bind mount; commands run sequentially with the
        # same stop-on-first-failure semantics as LocalSubprocessRunner
        # (set -e). Artifacts are collected from the mounted workdir after
        # the container exits — never through the container network.
        lines = ["set -e", *spec.commands]
        (workdir / "task.sh").write_text("\n".join(lines) + "\n")
        args = [
            self._cli,
            "run",
            "--rm",
            "--runtime",
            self._runtime,
            "-v",
            f"{workdir.resolve()}:/work",
            "-w",
            "/work",
            self._image,
            "bash",
            "task.sh",
        ]
        proc = await asyncio.create_subprocess_exec(
            *args,
            cwd=str(workdir),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, err = await proc.communicate()
        exit_code = proc.returncode if proc.returncode is not None else 1
        artifacts: dict[str, bytes] = {}
        if exit_code == 0:
            for rel in spec.artifacts:
                path = workdir / rel
                if path.is_file():
                    artifacts[rel] = path.read_bytes()
                else:
                    raise RunnerError(f"expected artifact missing: {rel}")
        return TaskResult(
            exit_code=exit_code,
            stdout=out.decode("utf-8", errors="replace"),
            stderr=err.decode("utf-8", errors="replace"),
            artifacts=artifacts,
            sandbox_id=f"runsc:{spec.episode_id}",
        )
