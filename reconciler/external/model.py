"""reconciler/external/model.py — shared shapes for external reconciliation (WO-101).

Every external system presents the same three obligations to the reconciler:

  snapshot() -> read-only counters (the pull source: counts/state)
  inject()   -> create ONE disposable drift resource (test-prefixed, TTL-backed)
  cleanup()  -> remove what inject created; post-cleanup snapshot == baseline

Detection is a pure snapshot diff (baseline vs current, per counter) — the
same external-authority discipline as reconciler/core.py: drift findings come
ONLY from probing the external system, never from kernel-internal state.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol, runtime_checkable


def utcnow() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class InjectedResource:
    """A disposable drift resource created by inject(). `handle` MUST appear
    in at least one drift-finding counter (the detection precision anchor);
    `aux` carries cleanup detail (e.g. digest/token JSON)."""

    system: str
    case_id: str
    handle: str
    created_at: datetime
    aux: str = ""


@dataclass(frozen=True, slots=True)
class SystemSnapshot:
    system: str
    taken_at: datetime
    counters: Mapping[str, int]


@dataclass(frozen=True, slots=True)
class DriftFinding:
    system: str
    counter: str
    baseline: int
    current: int


@runtime_checkable
class ExternalSystemAdapter(Protocol):
    name: str

    async def snapshot(self) -> dict[str, int]: ...

    async def inject(self, case_id: str) -> InjectedResource: ...

    async def cleanup(self, resource: InjectedResource) -> None: ...


def diff_snapshots(baseline: SystemSnapshot, current: SystemSnapshot) -> list[DriftFinding]:
    """Counter-level diff. A counter added (baseline 0), removed (now 0) or
    changed is one drift finding. Counter names are case-scoped by every
    adapter, so findings point straight at the injected resource."""
    findings: list[DriftFinding] = []
    for counter in sorted(set(baseline.counters) | set(current.counters)):
        base = baseline.counters.get(counter, 0)
        cur = current.counters.get(counter, 0)
        if base != cur:
            findings.append(
                DriftFinding(system=baseline.system, counter=counter, baseline=base, current=cur)
            )
    return findings
