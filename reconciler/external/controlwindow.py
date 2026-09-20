"""reconciler/external/controlwindow.py — injection-free control window (WO-101 G3).

Observes >= 1 complete reconciliation cycle with NO injection and evaluates
every reconciliation assertion: any drift finding in the window is a false
positive. PASS iff zero — a reconciler that only ever cries when injected
but false-alarms daily cannot ship (G3 rationale).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from reconciler.external.model import (
    DriftFinding,
    ExternalSystemAdapter,
    SystemSnapshot,
    diff_snapshots,
    utcnow,
)


@dataclass(slots=True)
class ControlWindowResult:
    cycles: int
    snapshots: int
    false_positives: list[DriftFinding] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.false_positives


class ControlWindowRunner:
    def __init__(self, adapters: Mapping[str, ExternalSystemAdapter], cycles: int = 2) -> None:
        if cycles < 1:
            raise ValueError("control window requires >= 1 complete cycle")
        self._adapters = adapters
        self._cycles = cycles

    async def run(self) -> ControlWindowResult:
        previous: dict[str, SystemSnapshot] | None = None
        false_positives: list[DriftFinding] = []
        for _ in range(self._cycles + 1):
            current = {
                system: SystemSnapshot(system, utcnow(), await adapter.snapshot())
                for system, adapter in self._adapters.items()
            }
            if previous is not None:
                for system in sorted(current):
                    false_positives.extend(diff_snapshots(previous[system], current[system]))
            previous = current
        return ControlWindowResult(
            cycles=self._cycles, snapshots=self._cycles + 1, false_positives=false_positives
        )
