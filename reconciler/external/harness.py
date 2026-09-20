"""reconciler/external/harness.py — WO-101 eval harness CLI.

Modes:
  check-list       G1: parse the work order's closed system list and assert
                   adapter closure (adapters == N, >=1 injection case each).
  eval             G2+G3 against the REAL systems (env-configured): one
                   drift-injection case per system + injection-free control
                   window; writes the machine report.
  sandbox-selfcheck  Same gates over the in-memory fake fleet — the payload
                   executed INSIDE the runsc sandbox (no external side
                   effects from isolation), proving the eval stack runs
                   under gVisor.
  episode          eval (live) + WO-104 carrier: EpisodeExecutor.run_task
                   with metadata sandbox_tier=isolated routes to
                   RunscRunner (one-shot env) and the episode audit event
                   carries runtime=runsc — the G3 audit anchor recorded
                   into the report.

Requires `uv run` from the kernel repo; heavy gateway/executor imports are
deferred to the episode mode.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

from reconciler.external.adapters import ADAPTER_SYSTEMS, INJECTION_CASES, check_list_closure
from reconciler.external.controlwindow import ControlWindowRunner
from reconciler.external.injector import DriftInjectionRunner
from reconciler.external.registry import adapters_from_config, config_from_env, fake_fleet
from reconciler.external.report import EvalReport, build_report
from reconciler.external.syslist import parse_system_list_file

SUITE = "wo101-reconciler-adapters"


def _write_report(report: EvalReport, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(report.to_json(), encoding="utf-8")


def _emit(report: EvalReport, report_path: Path | None) -> int:
    if report_path:
        _write_report(report, report_path)
    print(report.to_json())
    return 0 if report.all_passed() else 1


async def _cmd_check_list(system_list_path: Path) -> int:
    systems = parse_system_list_file(system_list_path)
    ok, detail = check_list_closure(systems)
    print(
        json.dumps(
            {
                "gate": "G1",
                "system_list": systems,
                "adapters": list(ADAPTER_SYSTEMS),
                "cases": {s: list(INJECTION_CASES[s]) for s in ADAPTER_SYSTEMS},
                "passed": ok,
                "detail": detail,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if ok else 1


async def _cmd_eval(
    *, report_path: Path | None, cycles: int, profile: str, system_list: list[str] | None
) -> int:
    if profile == "live":
        adapters = adapters_from_config(config_from_env(os.environ))
        systems = system_list or list(ADAPTER_SYSTEMS)
    else:
        adapters = fake_fleet()
        systems = system_list or list(ADAPTER_SYSTEMS)

    case_results = []
    runner = DriftInjectionRunner(adapters)
    for system in ADAPTER_SYSTEMS:
        for case_id in INJECTION_CASES[system]:
            case_results.append(await runner.run_case(system, case_id))
    control = await ControlWindowRunner(adapters, cycles=cycles).run()
    report = build_report(
        suite=SUITE,
        profile=profile,
        system_list=systems,
        cases=case_results,
        control_window=control,
        adapters=ADAPTER_SYSTEMS,
        case_ids=INJECTION_CASES,
        notes=(
            ["synthetic in-memory fleet — zero external side effects"] if profile != "live" else []
        ),
    )
    return _emit(report, report_path)


def _episode_env() -> dict[str, str]:
    dsn = os.environ.get("WO101_EPISODE_PG_DSN")
    opa = os.environ.get("WO101_EPISODE_OPA_URL")
    if not dsn or not opa:
        raise SystemExit(
            "episode mode requires WO101_EPISODE_PG_DSN and WO101_EPISODE_OPA_URL "
            "(one-shot scratch PG + OPA, disposed after the run)"
        )
    return {
        "dsn": dsn,
        "opa": opa,
        "image": os.environ.get("WO101_RUNSC_IMAGE", "kernel-task:latest"),
    }


async def _cmd_episode(*, report_path: Path | None, cycles: int, workdir: Path) -> int:
    import uuid
    from datetime import UTC, datetime

    env = _episode_env()
    adapters = adapters_from_config(config_from_env(os.environ))
    systems = list(ADAPTER_SYSTEMS)

    case_results = []
    runner = DriftInjectionRunner(adapters)
    for system in ADAPTER_SYSTEMS:
        for case_id in INJECTION_CASES[system]:
            case_results.append(await runner.run_case(system, case_id))
    control = await ControlWindowRunner(adapters, cycles=cycles).run()

    # -- WO-104 carrier: isolated-tier episode -> runsc runner --------------
    from gateway.classifiers import default_registry as default_classifier_registry
    from gateway.schemas import CloseEpisodeRequest
    from gateway.service import GatewayService
    from kernel.db import Database
    from kernel.executor.episode import EpisodeExecutor
    from kernel.policy import PolicyClient
    from kernel.runner.runsc import RunscRunner
    from kernel.runner.scheduler import default_registry as default_runner_registry

    db = await Database.connect(env["dsn"])
    try:
        await db.apply_schema()
        gateway = GatewayService(db, PolicyClient(env["opa"]), default_classifier_registry())
        runner_registry = dict(default_runner_registry())
        runner_registry["runsc"] = RunscRunner(image=env["image"])
        events: list[dict[str, Any]] = []

        async def on_event(event: dict[str, Any]) -> None:
            events.append(event)

        executor = EpisodeExecutor(db, gateway, runner_registry=runner_registry, on_event=on_event)

        grant_id = await _seed(db)
        episode_id = f"wo101-recon-{uuid.uuid4().hex[:8]}"
        await executor.start_episode(episode_id)
        task_dir = workdir / episode_id
        task_dir.mkdir(parents=True, exist_ok=True)
        await executor.run_task(
            episode_id=episode_id,
            grant_id=grant_id,
            commands=[
                "python -m reconciler.external.harness --mode sandbox-selfcheck "
                "--report artifacts/selfcheck-report.json"
            ],
            artifacts=["artifacts/selfcheck-report.json"],
            workdir=task_dir,
            metadata={"sandbox_tier": "isolated", "work_order": "WO-101"},
        )
        finished = next((e for e in events if e.get("type") == "episode.task_finished"), {})
        runtime = str(finished.get("runtime", ""))
        if runtime != "runsc":
            raise RuntimeError(f"WO-104 anchor broken: task_finished runtime={runtime!r}")
        await gateway.close_episode(
            episode_id, CloseEpisodeRequest(terminal_branch="candidate_ready")
        )
        row = await _episode_state(db, episode_id)
        if int(finished.get("exit_code") or 1) != 0:
            raise RuntimeError("sandbox self-check failed inside the runsc task")
        selfcheck_path = task_dir / "artifacts" / "selfcheck-report.json"
        selfcheck: dict[str, Any] | None = None
        if selfcheck_path.is_file():
            selfcheck = json.loads(selfcheck_path.read_text(encoding="utf-8"))

        episode_section = {
            "episode_id": episode_id,
            "task_finished_event": finished,
            "runtime": runtime,
            "sandbox_id": finished.get("sandbox_id", ""),
            "exit_code": finished.get("exit_code"),
            "final_state": row[0],
            "terminal_branch": row[1],
            "selfcheck_all_passed": (selfcheck or {}).get("all_passed"),
            "closed_at": datetime.now(UTC).isoformat(),
        }
    finally:
        await db.close()

    report = build_report(
        suite=SUITE,
        profile="episode-runsc",
        system_list=systems,
        cases=case_results,
        control_window=control,
        adapters=ADAPTER_SYSTEMS,
        case_ids=INJECTION_CASES,
        episode=episode_section,
    )
    return _emit(report, report_path)


async def _seed(db: Any) -> str:
    import uuid

    mandate_id = f"mnd-{uuid.uuid4().hex[:10]}"
    grant_id = f"grt-{uuid.uuid4().hex[:10]}"
    await db.register_mandate(
        mandate_id=mandate_id,
        human_signer="human:founder",
        signature="sig",
        payload={"nonce": uuid.uuid4().hex},
        cap_amount=1_000_000,
        ledger_id="l0",
        expires_at="2099-01-01T00:00:00Z",
        signature_verified=True,
    )
    await db.register_grant(
        grant_id=grant_id,
        mandate_id=mandate_id,
        parent_grant_id=None,
        scope={
            "actions": ["*"],
            "resources": ["*"],
            "limits": {"call_limit": 1000, "budget_limit": 10000},
        },
        remaining_depth=3,
        expiry="2099-01-01T00:00:00Z",
    )
    return grant_id


async def _episode_state(db: Any, episode_id: str) -> tuple[str, str | None]:
    async with db._pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT state, terminal_branch FROM episodes WHERE episode_id = $1", episode_id
        )
    return (row["state"], row["terminal_branch"]) if row else ("missing", None)


def main(argv: list[str] | None = None) -> int:
    import asyncio

    parser = argparse.ArgumentParser(prog="reconciler.external.harness")
    parser.add_argument(
        "--mode",
        required=True,
        choices=["check-list", "eval", "sandbox-selfcheck", "episode"],
    )
    parser.add_argument("--system-list", type=Path, help="work order md (check-list)")
    parser.add_argument("--report", type=Path, help="write the machine report JSON here")
    parser.add_argument("--cycles", type=int, default=2, help="control-window cycles (G3)")
    parser.add_argument(
        "--workdir", type=Path, default=Path(os.environ.get("WO101_WORKDIR", "/tmp/wo101-episode"))
    )
    args = parser.parse_args(argv)

    if args.mode == "check-list":
        if not args.system_list:
            raise SystemExit("--mode check-list requires --system-list")
        return asyncio.run(_cmd_check_list(args.system_list))
    if args.mode == "eval":
        return asyncio.run(
            _cmd_eval(report_path=args.report, cycles=args.cycles, profile="live", system_list=None)
        )
    if args.mode == "sandbox-selfcheck":
        return asyncio.run(
            _cmd_eval(
                report_path=args.report,
                cycles=args.cycles,
                profile="runsc-selfcheck",
                system_list=None,
            )
        )
    return asyncio.run(
        _cmd_episode(report_path=args.report, cycles=args.cycles, workdir=args.workdir)
    )


if __name__ == "__main__":
    sys.exit(main())
