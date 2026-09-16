"""JiuwenBox sandbox runner — production execution context (WO-04 item 3).

Runs each task in an ephemeral JiuwenBox session (network.mode=isolated,
egress default-deny, on_stop=delete — see platform repo ops/jiuwenbox/).
Agent LLM calls go through LiteLLM (privacy proxy injects real keys; the
sandbox only ever holds placeholders). This runner shells out to the
`jiuwenbox` CLI pinned in the platform repo requirements-lock; until the
openJiuWen release ships to this host, the CLI shape below is the contract
(this file activates unchanged once the binary/image is present).
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

from kernel.runner.base import TaskResult, TaskSpec

_FALLBACK_ON_FAILURE = False  # SANDBOX mode: never fall back to bare execution


class JiuwenBoxRunner:
    name = "jiuwenbox"

    def __init__(self, config_path: Path | None = None, cli: str = "jiuwenbox") -> None:
        self._config = config_path
        self._cli = cli

    async def run(self, spec: TaskSpec, workdir: Path) -> TaskResult:
        workdir.mkdir(parents=True, exist_ok=True)
        # session = isolated, ephemeral; commands run inside; artifacts copied
        # back through the session's controlled channel (never raw network)
        script_file = workdir / "task.json"
        script_file.write_text(
            json.dumps(
                {
                    "episode_id": spec.episode_id,
                    "commands": spec.commands,
                    "artifacts": spec.artifacts,
                    "mode": "SANDBOX",
                    "fallback_on_failure": _FALLBACK_ON_FAILURE,
                }
            )
        )
        args = [
            self._cli,
            "run",
            "--config",
            str(self._config) if self._config else "default",
            "--session",
            f"ep-{spec.episode_id}",
            "--task",
            str(script_file),
        ]
        proc = await asyncio.create_subprocess_exec(
            *args,
            cwd=str(workdir),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, err = await proc.communicate()
        artifacts: dict[str, bytes] = {}
        result_file = workdir / "result.json"
        if result_file.is_file():
            payload = json.loads(result_file.read_text())
            for rel, b64 in payload.get("artifacts", {}).items():
                import base64

                artifacts[rel] = base64.b64decode(b64)
            for rel in spec.artifacts:
                artifacts.setdefault(rel, b"")
        return TaskResult(
            exit_code=proc.returncode or 0,
            stdout=out.decode("utf-8", errors="replace"),
            stderr=err.decode("utf-8", errors="replace"),
            artifacts=artifacts,
            sandbox_id=f"jiuwenbox:ep-{spec.episode_id}",
        )
