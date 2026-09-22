"""watcher/poller.py — the orchestration core: one cycle = poll + dispatch.

Cycle shape (MVP: steps 1-3 of the architecture; verifier/closer land later
against the same state ledger):

  1. fetch the mandates ref, list workorders/*.md at FETCH_HEAD
  2. diff against the state ledger:
       - first cycle -> baseline sweep (record history, dispatch nothing)
       - unseen OPEN files and interrupted "dispatching" entries -> pipeline
  3. pipeline per work order:
       parse -> seed episode (idempotent) -> RESERVED->RUNNING
       -> resolve suite texts -> write dispatch file -> mark dispatched

Every step is idempotent end to end: gateway create replays, transition is
skipped when the replay already reports RUNNING, dispatch files are rewritten
atomically. A crash anywhere replays safely on the next cycle.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from kernel.watcher.alerts import AlertSink
from kernel.watcher.config import WatcherSettings
from kernel.watcher.dispatch import DispatchWriter
from kernel.watcher.gateway import GatewayClient
from kernel.watcher.models import (
    CycleResult,
    SuiteRef,
    WatcherState,
    WorkorderEntry,
)
from kernel.watcher.parsing import (
    WorkorderFilenameError,
    is_dispatchable,
    parse_workorder,
    workorder_id_for,
)
from kernel.watcher.sources import MandatesSource, SuiteSource
from kernel.watcher.state import StateStore

logger = logging.getLogger(__name__)

TERMINAL_EPISODE_STATES = ("RUNNING", "VERIFYING", "CLOSED")
_BACKOFF_CAP = timedelta(hours=1)


def _iso(dt: datetime) -> str:
    return dt.isoformat()


class Watcher:
    def __init__(
        self,
        *,
        settings: WatcherSettings,
        mandates: MandatesSource,
        gateway: GatewayClient,
        store: StateStore,
        writer: DispatchWriter,
        alert: AlertSink,
        suites: SuiteSource | None = None,
        now_fn: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._settings = settings
        self._mandates = mandates
        self._suites = suites
        self._gateway = gateway
        self._store = store
        self._writer = writer
        self._alert = alert
        self._now = now_fn

    # ------------------------------------------------------------------ cycle
    async def run_cycle(self) -> CycleResult:
        result = CycleResult()
        commit = await self._mandates.head_commit()
        files = await self._mandates.list_workorders()
        result.files_seen = len(files)
        state = self._store.load()

        if self._settings.baseline_on_first_run and state.baseline_commit is None:
            return await self._baseline(state, files, commit, result)

        fresh = [f for f in files if f not in state.workorders]
        resume = [
            state.workorders[f]
            for f in files
            if f in state.workorders and self._due_for_retry(state.workorders[f])
        ]
        for path in fresh:
            await self._process_fresh(state, path, commit, result)
        for entry in resume:
            await self._process_resume(state, entry, commit, result)
        return result

    # --------------------------------------------------------------- baseline
    async def _baseline(
        self, state: WatcherState, files: list[str], commit: str, result: CycleResult
    ) -> CycleResult:
        for path in files:
            if path in state.workorders:
                continue
            try:
                wo_id = workorder_id_for(path)
            except WorkorderFilenameError:
                wo_id = ""
            state.workorders[path] = WorkorderEntry(
                file_path=path,
                workorder_id=wo_id,
                status="baseline",
                first_seen_commit=commit,
            )
            result.baselined += 1
        state.baseline_commit = commit
        self._store.save(state)
        logger.info(
            "baseline sweep: %d existing files recorded, nothing dispatched", result.baselined
        )
        return result

    # -------------------------------------------------------------- pipelines
    async def _process_fresh(
        self, state: WatcherState, path: str, commit: str, result: CycleResult
    ) -> None:
        try:
            wo_id = workorder_id_for(path)
        except WorkorderFilenameError as exc:
            state.workorders[path] = WorkorderEntry(
                file_path=path,
                workorder_id="",
                status="skipped",
                first_seen_commit=commit,
                skip_reason=str(exc),
            )
            self._store.save(state)
            result.skipped += 1
            logger.warning("skip %s: %s", path, exc)
            return

        entry = WorkorderEntry(
            file_path=path,
            workorder_id=wo_id,
            status="dispatching",
            first_seen_commit=commit,
        )
        state.workorders[path] = entry
        text = await self._mandates.read_workorder(path)
        parsed = parse_workorder(path, text)
        entry.title = parsed.title
        if not is_dispatchable(parsed):
            entry.status = "skipped"
            entry.skip_reason = f"status={parsed.status or 'absent'} (only OPEN dispatches)"
            self._store.save(state)
            result.skipped += 1
            logger.info("skip %s: %s", path, entry.skip_reason)
            return
        await self._pipeline(state, entry, commit, text, parsed.suite_refs, result, fresh=True)

    async def _process_resume(
        self, state: WatcherState, entry: WorkorderEntry, commit: str, result: CycleResult
    ) -> None:
        text = await self._mandates.read_workorder(entry.file_path)
        parsed = parse_workorder(entry.file_path, text)
        entry.title = parsed.title or entry.title
        await self._pipeline(state, entry, commit, text, parsed.suite_refs, result, fresh=False)

    async def _pipeline(
        self,
        state: WatcherState,
        entry: WorkorderEntry,
        commit: str,
        text: str,
        suite_refs: tuple[str, ...],
        result: CycleResult,
        *,
        fresh: bool,
    ) -> None:
        label = entry.workorder_id or entry.file_path
        try:
            # (a) seed the episode -- idempotent: replay returns current state
            created = await self._gateway.create_workorder(
                entry.workorder_id,
                metadata={
                    "source_file": entry.file_path,
                    "mandates_commit": commit,
                    "dispatched_by": "kernel-watcher",
                },
            )
            entry.episode_id = created.episode_id
            if created.created:
                entry.episode_created = True

            # (b) RESERVED -> RUNNING, unless the replay already moved on
            if created.state == "RESERVED":
                new_state = await self._gateway.transition(created.episode_id, "RUNNING")
            else:
                new_state = created.state
            entry.running = new_state in TERMINAL_EPISODE_STATES

            # (c) resolve suite texts for the dispatch payload
            suites = await self._resolve_suites(suite_refs)

            # (d) dispatch file -- written BEFORE the ledger marks success
            payload = self._writer.build_payload(
                workorder_id=entry.workorder_id,
                episode_id=created.episode_id,
                source_file=entry.file_path,
                commit=commit,
                title=entry.title,
                dispatched_at=_iso(self._now()),
                workorder_text=text,
                suites=suites,
            )
            entry.dispatch_path = str(self._writer.write(payload))
            entry.dispatched_at = _iso(self._now())
            entry.status = "dispatched"
            entry.error = None
            entry.attempts = 0
            entry.next_attempt_at = None
        except Exception as exc:
            await self._record_failure(state, entry, exc)
        else:
            if fresh:
                result.dispatched += 1
            else:
                result.resumed += 1
            logger.info(
                "dispatched %s (episode=%s running=%s file=%s)",
                label,
                entry.episode_id,
                entry.running,
                entry.dispatch_path,
            )
        self._store.save(state)

    # ----------------------------------------------------------------- helpers
    async def _resolve_suites(self, refs: tuple[str, ...]) -> list[SuiteRef]:
        out: list[SuiteRef] = []
        for name in refs:
            path = f"suites/{name}.md"
            if self._suites is None:
                out.append(
                    SuiteRef(
                        name=name, path=path, text=None, note="eval-gate source not configured"
                    )
                )
                continue
            body = await self._suites.read_suite(name)
            out.append(
                SuiteRef(
                    name=name,
                    path=path,
                    text=body,
                    note=None if body else "suite not found in eval-gate ref",
                )
            )
        return out

    async def _record_failure(
        self, state: WatcherState, entry: WorkorderEntry, exc: Exception
    ) -> None:
        label = entry.workorder_id or entry.file_path
        entry.attempts += 1
        entry.error = f"{type(exc).__name__}: {exc}"
        logger.exception("dispatch failed for %s (attempt %d)", label, entry.attempts)
        if entry.attempts >= self._settings.max_attempts:
            entry.status = "failed"
            await self._alert.send(
                "CRITICAL",
                f"watcher: work order {label} failed after {entry.attempts} attempts, "
                f"manual intervention required: {exc}",
            )
        else:
            entry.status = "dispatching"
            delay = min(
                timedelta(seconds=self._settings.backoff_base * 2 ** (entry.attempts - 1)),
                _BACKOFF_CAP,
            )
            entry.next_attempt_at = _iso(self._now() + delay)
            await self._alert.send(
                "WARNING",
                f"watcher: work order {label} attempt {entry.attempts} failed, "
                f"retrying later: {exc}",
            )

    def _due_for_retry(self, entry: WorkorderEntry) -> bool:
        if entry.status != "dispatching":
            return False
        if entry.next_attempt_at is None:
            return True
        try:
            due = datetime.fromisoformat(entry.next_attempt_at)
        except ValueError:
            return True
        return due <= self._now()
