"""watcher/state.py — atomic persistence for watcher_state (state.json).

Fail-safe rules:
  * missing file  -> fresh empty state (first run)
  * corrupt file  -> StateError, never silently overwritten (the ledger is
    the only guard against re-dispatching work orders; losing it must be a
    human decision, so the watcher refuses to start over it)
  * every save is tmp-file + os.replace inside the target directory
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from kernel.watcher.models import WatcherState


class StateError(RuntimeError):
    """The state ledger exists but cannot be used (corrupt / unwritable)."""


class StateStore:
    def __init__(self, path: Path) -> None:
        self._path = path

    @property
    def path(self) -> Path:
        return self._path

    def load(self) -> WatcherState:
        if not self._path.exists():
            return WatcherState()
        try:
            raw = self._path.read_text(encoding="utf-8")
            data = json.loads(raw) if raw.strip() else {}
        except (OSError, json.JSONDecodeError) as exc:
            raise StateError(f"cannot read state ledger {self._path}: {exc}") from exc
        if not isinstance(data, dict):
            raise StateError(f"state ledger {self._path} is not a JSON object")
        try:
            return WatcherState.from_dict(data)
        except (KeyError, TypeError, ValueError) as exc:
            raise StateError(f"state ledger {self._path} has an unknown shape: {exc}") from exc

    def save(self, state: WatcherState) -> None:
        payload = json.dumps(state.to_dict(), ensure_ascii=False, indent=2, sort_keys=True)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_name(f"{self._path.name}.tmp")
        try:
            tmp.write_text(payload + "\n", encoding="utf-8")
            os.replace(tmp, self._path)
        except OSError as exc:
            raise StateError(f"cannot persist state ledger {self._path}: {exc}") from exc
