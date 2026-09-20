"""WO-101 gates — G1 closure parsing, G2 detection honesty, G3 zero false
positives, and the report's hard assertions. Everything runs on the
in-memory fleet: CI needs no external systems."""

from __future__ import annotations

import json

import pytest

from reconciler.external.adapters import ADAPTER_SYSTEMS, INJECTION_CASES, check_list_closure
from reconciler.external.controlwindow import ControlWindowRunner
from reconciler.external.injector import DriftInjectionRunner
from reconciler.external.model import (
    DriftFinding,
    InjectedResource,
    utcnow,
)
from reconciler.external.registry import fake_fleet
from reconciler.external.report import build_report, g2_gate, g3_gate
from reconciler.external.syslist import SystemListError, parse_system_list

_WORKORDER_SAMPLE = """
<!-- preface noise -->
| # | 系统 | 容器 |
|---|---|---|
<!-- WO101-SYSTEM-LIST BEGIN (machine-readable; closed list — adapters must cover exactly these N=6 systems, each with >=1 drift-injection case)
redis
clickhouse
minio
zot
litellm
langfuse
WO101-SYSTEM-LIST END -->
**定稿 = 6 系统**
"""


def test_parse_system_list_from_workorder_format() -> None:
    assert parse_system_list(_WORKORDER_SAMPLE) == [
        "redis",
        "clickhouse",
        "minio",
        "zot",
        "litellm",
        "langfuse",
    ]


def test_parse_system_list_fails_closed() -> None:
    with pytest.raises(SystemListError):
        parse_system_list("no markers here")
    with pytest.raises(SystemListError):
        parse_system_list("# WO101-SYSTEM-LIST BEGIN\n\n# WO101-SYSTEM-LIST END")
    with pytest.raises(SystemListError):
        parse_system_list("# WO101-SYSTEM-LIST BEGIN\nredis\nredis\n# WO101-SYSTEM-LIST END")
    with pytest.raises(SystemListError):
        parse_system_list("# WO101-SYSTEM-LIST BEGIN\nRedis-Prime!\n# WO101-SYSTEM-LIST END")


def test_g1_closure_passes_and_detects_breakage() -> None:
    ok, detail = check_list_closure(list(ADAPTER_SYSTEMS))
    assert ok, detail
    broken = [s for s in ADAPTER_SYSTEMS if s != "zot"]  # zot adapter missing
    ok2, detail2 = check_list_closure(list(ADAPTER_SYSTEMS), adapters=broken)
    assert not ok2 and "missing=['zot']" in detail2
    # list grows (e.g. TigerBeetle later) without an adapter -> closure fails
    ok3, _ = check_list_closure([*ADAPTER_SYSTEMS, "tigerbeetle"])
    assert not ok3
    # every registered system carries >=1 injection case
    assert all(INJECTION_CASES[s] for s in ADAPTER_SYSTEMS)


async def test_g2_full_fleet_hundred_percent_detection() -> None:
    fleet = fake_fleet()
    runner = DriftInjectionRunner(fleet)
    results = []
    for system in ADAPTER_SYSTEMS:
        for case_id in INJECTION_CASES[system]:
            results.append(await runner.run_case(system, case_id))
    assert len(results) == 6
    for r in results:
        assert r.detected, f"{r.system}: injection not detected"
        assert r.injected_at is not None and r.detected_at is not None
        assert r.detected_at >= r.injected_at
        assert r.findings, f"{r.system}: no diff findings recorded"
        assert r.cleaned and r.clean_verified, f"{r.system}: cleanup red line"
        assert r.error is None
    gate = g2_gate(results)
    assert gate.passed and "6/6" in gate.detail


class MuteAdapter:
    """A broken adapter: inject() creates no observable drift. G2 must catch
    the miss (检出率 100% is a hard gate — the report cannot fake detection)."""

    name = "redis"

    async def snapshot(self) -> dict[str, int]:
        return {}

    async def inject(self, case_id: str) -> InjectedResource:
        return InjectedResource(
            system=self.name, case_id=case_id, handle="nowhere", created_at=utcnow()
        )

    async def cleanup(self, resource: InjectedResource) -> None:
        return None


async def test_g2_fails_on_missed_detection() -> None:
    runner = DriftInjectionRunner({"redis": MuteAdapter()})
    result = await runner.run_case("redis", "miss-1")
    assert not result.detected
    gate = g2_gate([result])
    assert not gate.passed and "0/1" in gate.detail


class HoardingRedis:
    """DEL is a no-op: cleanup "succeeds" but the key remains — cleanup
    verification (post-cleanup snapshot == baseline) must fail the case."""

    def __init__(self) -> None:
        self.data: dict[str, str] = {}

    async def execute(self, *args: object) -> object:
        cmd = str(args[0]).upper()
        if cmd == "SET":
            self.data[str(args[1])] = str(args[2])
            return "OK"
        if cmd == "SCAN":
            import fnmatch

            matches = sorted(k for k in self.data if fnmatch.fnmatchcase(k, str(args[3])))
            return ["0", matches]
        if cmd == "DEL":
            return 0  # pretends to delete, deletes nothing
        return "OK"


async def test_g2_fails_on_cleanup_residue() -> None:
    from reconciler.external.adapters.redis_adapter import RedisAdapter

    runner = DriftInjectionRunner({"redis": RedisAdapter(HoardingRedis())})
    result = await runner.run_case("redis", "residue-1")
    assert result.detected
    assert result.cleaned and not result.clean_verified
    assert not g2_gate([result]).passed


class DriftingAdapter:
    """A fleet member whose counter changes without injection — the G3
    failure mode (false positives inside a clean control window)."""

    name = "redis"

    def __init__(self) -> None:
        self._tick = 0

    async def snapshot(self) -> dict[str, int]:
        self._tick += 1
        return {"phantom": self._tick}

    async def inject(self, case_id: str) -> InjectedResource:
        raise AssertionError("control window never injects")

    async def cleanup(self, resource: InjectedResource) -> None:
        raise AssertionError("control window never cleans up")


async def test_g3_control_window_zero_false_positives() -> None:
    control = await ControlWindowRunner(fake_fleet(), cycles=2).run()
    assert control.cycles == 2 and control.snapshots == 3
    assert control.passed
    assert g3_gate(control).passed

    noisy = await ControlWindowRunner({"redis": DriftingAdapter()}, cycles=1).run()
    assert not noisy.passed and noisy.false_positives
    assert not g3_gate(noisy).passed


async def test_build_report_hard_assertions_and_serialization() -> None:
    fleet = fake_fleet()
    runner = DriftInjectionRunner(fleet)
    cases = [await runner.run_case(s, INJECTION_CASES[s][0]) for s in ADAPTER_SYSTEMS]
    control = await ControlWindowRunner(fleet, cycles=2).run()
    report = build_report(
        suite="wo101-reconciler-adapters",
        profile="test",
        system_list=list(ADAPTER_SYSTEMS),
        cases=cases,
        control_window=control,
        adapters=ADAPTER_SYSTEMS,
        case_ids=INJECTION_CASES,
        episode={"runtime": "runsc", "episode_id": "ep-test"},
    )
    assert report.all_passed()
    gates = {g.gate: g for g in report.gates}
    assert gates["G1"].passed and gates["G2"].passed and gates["G3"].passed
    parsed = json.loads(report.to_json())
    assert parsed["episode"]["runtime"] == "runsc"
    assert len(parsed["cases"]) == 6
    matrix_row = parsed["cases"][0]
    assert {"system", "case_id", "injected_at", "detected_at", "findings"} <= set(matrix_row)

    # one missed detection must flip G2 -> the report cannot lie
    cases[3].detected = False
    failed = build_report(
        suite="wo101-reconciler-adapters",
        profile="test",
        system_list=list(ADAPTER_SYSTEMS),
        cases=cases,
        control_window=control,
        adapters=ADAPTER_SYSTEMS,
        case_ids=INJECTION_CASES,
    )
    assert not failed.all_passed()
    failed_gates = {g.gate: g.passed for g in failed.gates}
    assert failed_gates["G2"] is False and failed_gates["G1"] and failed_gates["G3"]


def test_drift_finding_shape() -> None:
    finding = DriftFinding(system="s", counter="c", baseline=0, current=2)
    assert finding.current - finding.baseline == 2
