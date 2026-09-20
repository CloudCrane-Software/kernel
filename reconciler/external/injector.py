"""reconciler/external/injector.py — F-4 disposable drift injection runner (WO-101 G2).

Per case: baseline snapshot -> inject (disposable, test-prefixed resource)
-> current snapshot -> diff -> detection assertion (a finding whose counter
carries the resource handle) -> cleanup -> clean-verification (post-cleanup
snapshot == baseline, the work order's 用后清理回验). G2 = detected / total
== 100%; anything less fails the gate and the cleanup red line raises.
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
    def __init__(self, adapters: Mapping[str, ExternalSystemAdapter]) -> None:
        self._adapters = adapters

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

        current = SystemSnapshot(system, utcnow(), await adapter.snapshot())
        result.findings = diff_snapshots(baseline, current)
        result.detected = any(resource.handle in f.counter for f in result.findings)
        if result.detected:
            result.detected_at = current.taken_at

        try:
            await self._cleanup_with_retry(adapter, resource)
        except Exception as exc:
            result.error = f"cleanup failed: {exc}"
            return result
        result.cleaned = True

        after = await adapter.snapshot()
        result.clean_verified = dict(after) == dict(baseline.counters)
        return result

    async def run_all(self, cases: Mapping[str, str]) -> list[InjectionCaseResult]:
        return [await self.run_case(system, case_id) for system, case_id in cases.items()]

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
