"""watcher/dispatch.py — the decoupling layer between watcher and developer.

The watcher does not develop; it drops one JSON file per accepted work order
into the dispatch volume. Any development agent watches that directory, picks
up the file and drives the rest of the lifecycle (branch -> PR -> gates ->
merge, then transition VERIFYING and close through the gateway with its own
credentials — the watcher deliberately ships no secrets in the payload).
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from kernel.watcher.models import DISPATCH_PAYLOAD_VERSION, SuiteRef


class DispatchError(RuntimeError):
    """A dispatch file could not be written."""


class DispatchWriter:
    def __init__(self, directory: Path) -> None:
        self._directory = directory

    @property
    def directory(self) -> Path:
        return self._directory

    def build_payload(
        self,
        *,
        workorder_id: str,
        episode_id: str,
        source_file: str,
        commit: str,
        title: str | None,
        dispatched_at: str,
        workorder_text: str,
        suites: list[SuiteRef],
    ) -> dict[str, Any]:
        return {
            "version": DISPATCH_PAYLOAD_VERSION,
            "workorder_id": workorder_id,
            "episode_id": episode_id,
            "source_file": source_file,
            "commit": commit,
            "title": title,
            "dispatched_at": dispatched_at,
            "workorder_text": workorder_text,
            "eval_suites": [
                {"name": s.name, "path": s.path, "text": s.text, "note": s.note} for s in suites
            ],
            "expected_consumer_flow": [
                "develop on a branch in the target repo",
                "open a PR; machine gates must pass; squash merge per ADR-0003",
                "transition the episode: POST /v1/episodes/{episode_id}/transition "
                "{'target_state': 'VERIFYING'}",
                "after eval PASS: POST .../transition {'target_state': 'CLOSED', "
                "'terminal_branch': 'candidate_ready'}",
            ],
        }

    def write(self, payload: dict[str, Any]) -> Path:
        """Atomically write <workorder_id>.json; returns the written path."""
        workorder_id = str(payload.get("workorder_id", ""))
        if not workorder_id or "/" in workorder_id:
            raise DispatchError(f"refusing to write dispatch file for id {workorder_id!r}")
        target = self._directory / f"{workorder_id}.json"
        body = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
        try:
            self._directory.mkdir(parents=True, exist_ok=True)
            tmp = target.with_name(f".{target.name}.tmp")
            tmp.write_text(body + "\n", encoding="utf-8")
            os.replace(tmp, target)
        except OSError as exc:
            raise DispatchError(f"cannot write dispatch file {target}: {exc}") from exc
        return target
