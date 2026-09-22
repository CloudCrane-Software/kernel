"""watcher/models.py — typed records shared by every watcher stage.

WatcherState is the on-disk ledger (state.json): the set of work-order files
the watcher has already seen, keyed by repo-relative path, plus what happened
to each of them. Every write goes through watcher.state.StateStore, which
persists atomically; the gateway side is idempotent, so a crash between two
steps always replays safely (at-least-once dispatch semantics).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

WorkorderStatus = Literal["baseline", "skipped", "dispatching", "dispatched", "failed"]

DISPATCH_PAYLOAD_VERSION = 1


@dataclass(frozen=True, slots=True)
class ParsedWorkorder:
    """Everything the parser can responsibly extract from one file."""

    workorder_id: str
    file_path: str
    title: str | None
    status: str | None  # raw token of the **状态** line, uppercased; None if absent
    suite_refs: tuple[str, ...]  # eval-gate suite names, deduplicated, file order


@dataclass(frozen=True, slots=True)
class SuiteRef:
    """One resolved (or unresolvable) suite reference for the dispatch file."""

    name: str
    path: str
    text: str | None
    note: str | None = None  # why text is None (not found / source not configured)


@dataclass(frozen=True, slots=True)
class CreateWorkorderResult:
    """Normalized gateway POST /v1/workorders outcome."""

    episode_id: str
    state: str
    created: bool


@dataclass(slots=True)
class WorkorderEntry:
    """One work-order file's record in the state ledger (mutable)."""

    file_path: str
    workorder_id: str
    status: WorkorderStatus
    first_seen_commit: str
    title: str | None = None
    episode_id: str | None = None
    episode_created: bool = False  # True only when the gateway seeded a fresh episode
    running: bool = False  # RESERVED -> RUNNING transition observed
    dispatch_path: str | None = None
    dispatched_at: str | None = None
    skip_reason: str | None = None
    error: str | None = None
    attempts: int = 0
    next_attempt_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "file_path": self.file_path,
            "workorder_id": self.workorder_id,
            "status": self.status,
            "first_seen_commit": self.first_seen_commit,
            "title": self.title,
            "episode_id": self.episode_id,
            "episode_created": self.episode_created,
            "running": self.running,
            "dispatch_path": self.dispatch_path,
            "dispatched_at": self.dispatched_at,
            "skip_reason": self.skip_reason,
            "error": self.error,
            "attempts": self.attempts,
            "next_attempt_at": self.next_attempt_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> WorkorderEntry:
        status = data["status"]
        if status not in ("baseline", "skipped", "dispatching", "dispatched", "failed"):
            raise ValueError(f"unknown workorder status: {status!r}")
        return cls(
            file_path=data["file_path"],
            workorder_id=data["workorder_id"],
            status=status,  # validated against the literal set above
            first_seen_commit=data["first_seen_commit"],
            title=data.get("title"),
            episode_id=data.get("episode_id"),
            episode_created=bool(data.get("episode_created", False)),
            running=bool(data.get("running", False)),
            dispatch_path=data.get("dispatch_path"),
            dispatched_at=data.get("dispatched_at"),
            skip_reason=data.get("skip_reason"),
            error=data.get("error"),
            attempts=int(data.get("attempts", 0)),
            next_attempt_at=data.get("next_attempt_at"),
        )


@dataclass(slots=True)
class WatcherState:
    """Root of state.json."""

    version: int = 1
    baseline_commit: str | None = None  # set after the first-run baseline sweep
    workorders: dict[str, WorkorderEntry] = field(default_factory=dict)  # by file path

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "baseline_commit": self.baseline_commit,
            "workorders": {k: v.to_dict() for k, v in sorted(self.workorders.items())},
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> WatcherState:
        entries = {
            path: WorkorderEntry.from_dict(raw) for path, raw in data.get("workorders", {}).items()
        }
        return cls(
            version=int(data.get("version", 1)),
            baseline_commit=data.get("baseline_commit"),
            workorders=entries,
        )


@dataclass(slots=True)
class CycleResult:
    """Outcome counters for one poll cycle."""

    files_seen: int = 0
    baselined: int = 0
    dispatched: int = 0
    skipped: int = 0
    failed: int = 0
    resumed: int = 0
