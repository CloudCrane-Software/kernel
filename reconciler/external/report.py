"""reconciler/external/report.py — machine eval report + G1/G2/G3 gates (WO-101).

The report is the evidence artifact: system x adapter x case matrix with
injection moment / detection moment / diff content per case, plus hard
verdicts — G2 detection rate must equal exactly 100% (zero misses) and G3
false positives must equal exactly zero.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any

from reconciler.external.controlwindow import ControlWindowResult
from reconciler.external.injector import InjectionCaseResult
from reconciler.external.model import utcnow


@dataclass(frozen=True, slots=True)
class GateResult:
    gate: str
    passed: bool
    detail: str


def g1_gate(
    system_list: Sequence[str],
    adapters: Iterable[str],
    cases: Mapping[str, Sequence[str]],
) -> GateResult:
    adapter_set = set(adapters)
    expected = set(system_list)
    missing_adapters = sorted(expected - adapter_set)
    missing_cases = sorted(s for s in system_list if not cases.get(s))
    passed = adapter_set == expected and not missing_cases and len(system_list) > 0
    detail = (
        f"adapters={len(adapter_set)}==N({len(expected)})"
        f"{'; missing adapters: ' + ','.join(missing_adapters) if missing_adapters else ''}"
        f"{'; systems without injection case: ' + ','.join(missing_cases) if missing_cases else ''}"
    )
    return GateResult("G1", passed, detail)


def g2_gate(results: Sequence[InjectionCaseResult]) -> GateResult:
    total = len(results)
    detected = sum(1 for r in results if r.detected)
    clean = all(r.cleaned and r.clean_verified for r in results)
    passed = total > 0 and detected == total and clean
    detail = f"detected/total={detected}/{total} ({(detected / total * 100):.0f}%)" + (
        "" if clean else "; cleanup/clean-verification failures present"
    )
    return GateResult("G2", passed, detail)


def g3_gate(control: ControlWindowResult) -> GateResult:
    return GateResult(
        "G3",
        control.passed,
        f"false_positives={len(control.false_positives)} over "
        f"{control.cycles} cycle(s), {control.snapshots} snapshots",
    )


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"unserializable: {type(value)!r}")


@dataclass(slots=True)
class EvalReport:
    generated_at: datetime
    suite: str
    profile: str
    system_list: list[str]
    cases: list[InjectionCaseResult]
    control_window: ControlWindowResult
    gates: list[GateResult]
    episode: dict[str, Any] | None = None
    notes: list[str] = field(default_factory=list)

    def all_passed(self) -> bool:
        return all(g.passed for g in self.gates)

    def to_json(self, indent: int | None = 2) -> str:
        return json.dumps(asdict(self), default=_json_default, indent=indent, sort_keys=True)


def build_report(
    *,
    suite: str,
    profile: str,
    system_list: Sequence[str],
    cases: Sequence[InjectionCaseResult],
    control_window: ControlWindowResult,
    adapters: Iterable[str],
    case_ids: Mapping[str, Sequence[str]],
    episode: dict[str, Any] | None = None,
    notes: Sequence[str] = (),
) -> EvalReport:
    gates = [
        g1_gate(system_list, adapters, case_ids),
        g2_gate(cases),
        g3_gate(control_window),
    ]
    return EvalReport(
        generated_at=utcnow(),
        suite=suite,
        profile=profile,
        system_list=list(system_list),
        cases=list(cases),
        control_window=control_window,
        gates=gates,
        episode=episode,
        notes=list(notes),
    )
