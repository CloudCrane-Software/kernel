"""reconciler/external/injector.py — F-4 disposable drift injection runner (WO-101 G2).

Per case: baseline snapshot -> inject (disposable, test-prefixed resource)
-> poll for detection -> cleanup -> clean verification against the baseline.

Both windows are bounded and asynchronous-lag aware (T4 live findings):
  - detection is POLLED, not sampled once — pull-source systems may index
    injected state asynchronously (e.g. Langfuse ingestion is queued);
  - cleanup is VERIFIED past a settle window with re-delete on drift — a
    delete issued before the effect was indexed can be resurrected by it.

G2 = detected / total == 100% with every case cleanup-verified; anything
less fails the gate, and a failed cleanup raises (red line).
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime

from reconciler.external.model import (
    DriftFinding,
    ExternalSystemAdapter,
    InjectedResource,
    SystemSnapshot,
    diff_snapshots,
    utcnow,
)

_CLEANUP_ATTEMPTS = 3
_CLEANUP_BACKOFF_S = 0.5


class CleanupError(RuntimeError):
    """Cleanup failed after retries — leftover resources violate the red
    line; callers must stop and record [BLOCKED], never continue injecting."""


@dataclass(slots=True)
class InjectionCaseResult:
    system: str
    case_id: str
    resource: InjectedResource | None = None
    injected_at: datetime | None = None
    detected_at: datetime | None = None
    findings: list[DriftFinding] = field(default_factory=list)
    detected: bool = False
    cleaned: bool = False
    clean_verified: bool = False
    error: str | None = None


class DriftInjectionRunner:
    def __init__(
        self,
        adapters: Mapping[str, ExternalSystemAdapter],
        *,
        poll_seconds: float = 45.0,
        poll_interval: float = 3.0,
    ) -> None:
        self._adapters = adapters
        self._poll_seconds = poll_seconds
        self._poll_interval = poll_interval

    async def run_case(self, system: str, case_id: str) -> InjectionCaseResult:
        adapter = self._adapters[system]
        result = InjectionCaseResult(system=system, case_id=case_id)
        baseline = SystemSnapshot(system, utcnow(), await adapter.snapshot())
        try:
            resource = await adapter.inject(case_id)
        except Exception as exc:
            result.error = f"inject failed: {exc}"
            return result
        result.resource = resource
        result.injected_at = resource.created_at

        # detection window: poll until the handle shows up or the deadline
        deadline = utcnow().timestamp() + self._poll_seconds
        try:
            while True:
                current = SystemSnapshot(system, utcnow(), await adapter.snapshot())
                result.findings = diff_snapshots(baseline, current)
                result.detected = any(resource.handle in f.counter for f in result.findings)
                if result.detected:
                    result.detected_at = current.taken_at
                    break
                if utcnow().timestamp() >= deadline:
                    break
                await asyncio.sleep(self._poll_interval)
        except Exception as exc:
            # red line first: never leave the injected resource behind
            result.error = f"detection probe failed: {exc}"
            try:
                result.clean_verified = await self._cleanup_until_clean(adapter, resource, baseline)
                result.cleaned = True
            except Exception as clean_exc:
                result.error += f"; cleanup failed: {clean_exc}"
            return result

        try:
            result.clean_verified = await self._cleanup_until_clean(adapter, resource, baseline)
        except Exception as exc:
            result.error = f"cleanup failed: {exc}"
            return result
        result.cleaned = True
        if not result.clean_verified:
            result.error = (
                "cleanup verification timed out: residual drift after "
                f"{self._poll_seconds:.0f}s settle window"
            )
        return result

    async def run_all(self, cases: Mapping[str, str]) -> list[InjectionCaseResult]:
        return [await self.run_case(system, case_id) for system, case_id in cases.items()]

    async def _cleanup_until_clean(
        self,
        adapter: ExternalSystemAdapter,
        resource: InjectedResource,
        baseline: SystemSnapshot,
    ) -> bool:
        """Delete once, then verify the snapshot equals the baseline past a
        settle observation. Deletion itself may be queued (Langfuse: the
        delete lands tens of seconds after the 200), so do NOT re-delete
        while the effect is merely still visible — that resets the queue.
        Re-delete only after the baseline held and the effect came back
        (true resurrection)."""
        deadline = utcnow().timestamp() + self._poll_seconds
        await self._cleanup_with_retry(adapter, resource)
        while True:
            first = dict(await adapter.snapshot())
            if first == dict(baseline.counters):
                await asyncio.sleep(self._poll_interval)
                second = dict(await adapter.snapshot())
                if second == dict(baseline.counters):
                    return True
                await self._cleanup_with_retry(adapter, resource)
            if utcnow().timestamp() >= deadline:
                return dict(await adapter.snapshot()) == dict(baseline.counters)
            await asyncio.sleep(self._poll_interval)

    @staticmethod
    async def _cleanup_with_retry(
        adapter: ExternalSystemAdapter, resource: InjectedResource
    ) -> None:
        for attempt in range(_CLEANUP_ATTEMPTS):
            try:
                await adapter.cleanup(resource)
                return
            except Exception:
                if attempt == _CLEANUP_ATTEMPTS - 1:
                    raise
                await asyncio.sleep(_CLEANUP_BACKOFF_S * (attempt + 1))
