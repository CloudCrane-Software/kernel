"""Watcher tests (batch D orchestration layer).

Unit coverage for parsing / state / dispatch / gateway client, plus poller
cycle integration over a mock gateway (httpx.MockTransport): baseline sweep,
dispatch, skip rules, retry backoff, crash-resume replay and terminal
failure alerting. No docker, no network, no PG.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest

from kernel.watcher.config import WatcherSettings
from kernel.watcher.dispatch import DispatchError, DispatchWriter
from kernel.watcher.gateway import GatewayClient, GatewayError
from kernel.watcher.models import CycleResult, WorkorderEntry
from kernel.watcher.parsing import (
    WorkorderFilenameError,
    is_dispatchable,
    parse_workorder,
    workorder_id_for,
)
from kernel.watcher.poller import Watcher
from kernel.watcher.state import StateError, StateStore

# --------------------------------------------------------------------- fakes


class FakeMandates:
    def __init__(self, files: dict[str, str], commit: str = "a" * 40) -> None:
        self.files = dict(files)
        self.commit = commit

    async def head_commit(self) -> str:
        return self.commit

    async def list_workorders(self) -> list[str]:
        return sorted(self.files)

    async def read_workorder(self, file_path: str) -> str:
        return self.files[file_path]


class FakeSuites:
    def __init__(self, suites: dict[str, str]) -> None:
        self.suites = dict(suites)

    async def read_suite(self, name: str) -> str | None:
        return self.suites.get(name)


class RecordingAlert:
    def __init__(self) -> None:
        self.sent: list[tuple[str, str]] = []

    async def send(self, severity: str, message: str) -> None:
        self.sent.append((severity, message))


class FakeGatewayBackend:
    """In-memory episode store behind httpx.MockTransport; mirrors the real
    gateway contract: create is idempotent (201/200), same-state transition
    is a 409 ILLEGAL_TRANSITION, unauthenticated writes are 401."""

    def __init__(self, *, require_auth: bool = False) -> None:
        self.states: dict[str, str] = {}
        self.calls: list[str] = []
        self.failures_remaining = 0
        self.require_auth = require_auth
        self.token: str | None = None

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.calls.append(f"{request.method} {request.url.path}")
        if self.require_auth:
            auth = request.headers.get("authorization", "")
            expected = f"Bearer {self.token}" if self.token else ""
            if not self.token or auth != expected:
                return httpx.Response(401, json={"error": "UNAUTHORIZED", "detail": "bad token"})
        if self.failures_remaining > 0:
            self.failures_remaining -= 1
            return httpx.Response(500, json={"detail": "injected failure"})
        path = request.url.path
        if path == "/v1/workorders":
            body = json.loads(request.content.decode())
            wid = str(body["workorder_id"])
            if wid in self.states:
                return httpx.Response(
                    200,
                    json={
                        "episode_id": wid,
                        "workorder_id": wid,
                        "state": self.states[wid],
                        "created": False,
                        "metadata": {},
                    },
                )
            self.states[wid] = "RESERVED"
            return httpx.Response(
                201,
                json={
                    "episode_id": wid,
                    "workorder_id": wid,
                    "state": "RESERVED",
                    "created": True,
                    "metadata": {},
                },
            )
        parts = path.split("/")
        if (
            len(parts) == 5
            and parts[1] == "v1"
            and parts[2] == "episodes"
            and parts[4] == "transition"
        ):
            episode_id = parts[3]
            target = str(json.loads(request.content.decode())["target_state"])
            if self.states.get(episode_id) == target:
                return httpx.Response(
                    409, json={"error": "ILLEGAL_TRANSITION", "detail": "one-way map"}
                )
            previous = self.states.get(episode_id, "RESERVED")
            self.states[episode_id] = target
            return httpx.Response(
                200,
                json={
                    "episode_id": episode_id,
                    "previous_state": previous,
                    "state": target,
                    "terminal_branch": None,
                },
            )
        return httpx.Response(404, json={"detail": "no such route"})

    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handler)


# -------------------------------------------------------------------- helpers

SAMPLE_OPEN = """# Work order 109 — watcher 自动化试点 — 作业工单

- **状态**: OPEN（2026-09-22 开单）
- **eval 引用**: eval-gate 仓 `suites/wo109-watcher-pilot.md`

## 任务

write the thing.
"""

SAMPLE_DONE = """# Work order 108 — 已完成的工单

- **状态**: COMPLETED 2026-09-21
"""


def make_settings(tmp_path: Path, **overrides: Any) -> WatcherSettings:
    kwargs: dict[str, Any] = {
        "mandates_dir": tmp_path / "mandates",
        "dispatch_dir": tmp_path / "dispatch",
        "state_path": tmp_path / "state" / "state.json",
        "gateway_url": "http://gateway.test",
        "gateway_token": "test-token-123",
        "poll_interval": 1.0,
        "backoff_base": 10.0,
        "max_attempts": 3,
        "alert_cmd": "",
    }
    kwargs.update(overrides)
    return WatcherSettings(**kwargs)


class Clock:
    def __init__(self, start: datetime | None = None) -> None:
        self.now = start or datetime(2026, 9, 22, 12, 0, 0, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now = self.now + timedelta(seconds=seconds)


def make_watcher(
    settings: WatcherSettings,
    mandates: FakeMandates,
    backend: FakeGatewayBackend,
    alert: RecordingAlert,
    suites: FakeSuites | None = None,
    clock: Clock | None = None,
) -> Watcher:
    gateway = GatewayClient(
        settings.gateway_url,
        settings.gateway_token.get_secret_value() if settings.gateway_token else None,
        transport=backend.transport(),
    )
    clk = clock or Clock()
    return Watcher(
        settings=settings,
        mandates=mandates,
        gateway=gateway,
        store=StateStore(settings.state_path),
        writer=DispatchWriter(settings.dispatch_dir),
        alert=alert,
        suites=suites,
        now_fn=clk,
    )


# ------------------------------------------------------------------- parsing


def test_workorder_id_convention() -> None:
    assert workorder_id_for("workorders/0105-kernel-self-rewrite.md") == (
        "wo-0105-kernel-self-rewrite"
    )
    assert workorder_id_for("workorders/0001-restate-audit-export.md") == (
        "wo-0001-restate-audit-export"
    )


@pytest.mark.parametrize(
    "name", ["notes.md", "0105-UPPER.md", "105-short.md", "0105-.md", "0105-under_score.md"]
)
def test_workorder_id_rejects_bad_names(name: str) -> None:
    with pytest.raises(WorkorderFilenameError):
        workorder_id_for(f"workorders/{name}")


def test_parse_extracts_title_status_suites() -> None:
    parsed = parse_workorder("workorders/0109-watcher-pilot.md", SAMPLE_OPEN)
    assert parsed.workorder_id == "wo-0109-watcher-pilot"
    assert parsed.status == "OPEN"
    assert parsed.title == "Work order 109 — watcher 自动化试点 — 作业工单"
    assert parsed.suite_refs == ("wo109-watcher-pilot",)


def test_parse_deduplicates_suite_refs() -> None:
    text = SAMPLE_OPEN + "again `suites/wo109-watcher-pilot.md` and `suites/wo999-x.md`\n"
    parsed = parse_workorder("workorders/0109-watcher-pilot.md", text)
    assert parsed.suite_refs == ("wo109-watcher-pilot", "wo999-x")


def test_dispatchable_only_for_open() -> None:
    assert is_dispatchable(parse_workorder("workorders/0109-x.md", SAMPLE_OPEN))
    done = parse_workorder("workorders/0108-x.md", SAMPLE_DONE)
    assert not is_dispatchable(done)
    nostatus = parse_workorder("workorders/0107-x.md", "# no status bullet\n")
    assert not is_dispatchable(nostatus)


# --------------------------------------------------------------------- state


def test_state_store_roundtrip_and_defaults(tmp_path: Path) -> None:
    store = StateStore(tmp_path / "deep" / "state.json")
    assert store.load().workorders == {}
    state = store.load()
    state.workorders["workorders/0109-x.md"] = WorkorderEntry(
        file_path="workorders/0109-x.md",
        workorder_id="wo-0109-x",
        status="dispatched",
        first_seen_commit="abc",
        episode_id="wo-0109-x",
        episode_created=True,
        running=True,
        dispatch_path="/work/dispatch/wo-0109-x.json",
        dispatched_at="2026-09-22T00:00:00+00:00",
    )
    state.baseline_commit = "abc"
    store.save(state)
    reloaded = StateStore(store.path).load()
    assert reloaded.baseline_commit == "abc"
    entry = reloaded.workorders["workorders/0109-x.md"]
    assert entry.status == "dispatched"
    assert entry.episode_created is True
    assert entry.running is True


def test_state_store_refuses_corrupt_ledger(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    path.write_text("{not json", encoding="utf-8")
    with pytest.raises(StateError):
        StateStore(path).load()


# ------------------------------------------------------------------- dispatch


def test_dispatch_writer_atomic_payload(tmp_path: Path) -> None:
    writer = DispatchWriter(tmp_path / "dispatch")
    payload = writer.build_payload(
        workorder_id="wo-0109-x",
        episode_id="wo-0109-x",
        source_file="workorders/0109-x.md",
        commit="c0ffee",
        title="t",
        dispatched_at="2026-09-22T00:00:00+00:00",
        workorder_text="body",
        suites=[],
    )
    target = writer.write(payload)
    assert target == tmp_path / "dispatch" / "wo-0109-x.json"
    data = json.loads(target.read_text(encoding="utf-8"))
    assert data["version"] == 1
    assert data["episode_id"] == "wo-0109-x"
    assert data["source_file"] == "workorders/0109-x.md"
    assert data["workorder_text"] == "body"
    assert data["eval_suites"] == []
    assert any("VERIFYING" in step for step in data["expected_consumer_flow"])
    assert not list(tmp_path.joinpath("dispatch").glob("*.tmp"))


def test_dispatch_writer_refuses_bad_id(tmp_path: Path) -> None:
    writer = DispatchWriter(tmp_path / "dispatch")
    with pytest.raises(DispatchError):
        writer.write({"workorder_id": "../escape"})


# ------------------------------------------------------------------- gateway


async def test_gateway_client_create_and_transition() -> None:
    backend = FakeGatewayBackend()
    client = GatewayClient("http://gw", "tok", transport=backend.transport())
    first = await client.create_workorder("wo-0109-x", metadata={"a": 1})
    assert first.created is True
    assert first.state == "RESERVED"
    replay = await client.create_workorder("wo-0109-x")
    assert replay.created is False
    assert replay.state == "RESERVED"
    new_state = await client.transition("wo-0109-x", "RUNNING")
    assert new_state == "RUNNING"
    replay2 = await client.create_workorder("wo-0109-x")
    assert replay2.state == "RUNNING"
    await client.aclose()


async def test_gateway_client_requires_bearer_token() -> None:
    backend = FakeGatewayBackend(require_auth=True)
    backend.token = "s3cret"
    good = GatewayClient("http://gw", "s3cret", transport=backend.transport())
    assert (await good.create_workorder("wo-1")).created is True
    await good.aclose()
    bad = GatewayClient("http://gw", None, transport=backend.transport())
    with pytest.raises(GatewayError) as excinfo:
        await bad.create_workorder("wo-1")
    assert excinfo.value.status == 401
    await bad.aclose()


async def test_gateway_client_server_error_wraps_detail() -> None:
    backend = FakeGatewayBackend()
    backend.failures_remaining = 1
    client = GatewayClient("http://gw", "tok", transport=backend.transport())
    with pytest.raises(GatewayError) as excinfo:
        await client.create_workorder("wo-1")
    assert excinfo.value.status == 500
    assert "injected failure" in excinfo.value.detail
    await client.aclose()


async def test_gateway_client_network_error() -> None:
    def raise_connect(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("no route", request=request)

    client = GatewayClient("http://gw", "tok", transport=httpx.MockTransport(raise_connect))
    with pytest.raises(GatewayError) as excinfo:
        await client.create_workorder("wo-1")
    assert excinfo.value.status == 0
    await client.aclose()


# -------------------------------------------------------------------- poller


async def test_first_cycle_is_baseline_only(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    mandates = FakeMandates(
        {
            "workorders/0105-kernel-self-rewrite.md": SAMPLE_DONE,
            "workorders/0107-gateway-close-authz-guard.md": SAMPLE_OPEN,
        }
    )
    backend = FakeGatewayBackend()
    alert = RecordingAlert()
    watcher = make_watcher(settings, mandates, backend, alert)

    result = await watcher.run_cycle()
    assert result.baselined == 2
    assert backend.calls == []  # nothing touches the gateway during baseline
    assert not list(settings.dispatch_dir.glob("*.json"))
    state = StateStore(settings.state_path).load()
    assert state.baseline_commit == mandates.commit
    assert state.workorders["workorders/0107-gateway-close-authz-guard.md"].status == ("baseline")
    # second cycle: still nothing new -> no dispatches
    result2 = await watcher.run_cycle()
    assert result2.dispatched == 0 and backend.calls == []


async def test_new_open_workorder_is_dispatched_end_to_end(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    mandates = FakeMandates({"workorders/0105-kernel-self-rewrite.md": SAMPLE_DONE})
    backend = FakeGatewayBackend()
    alert = RecordingAlert()
    suites = FakeSuites({"wo109-watcher-pilot": "# eval suite: wo109 (G1..G3)"})
    watcher = make_watcher(settings, mandates, backend, alert, suites=suites)

    await watcher.run_cycle()  # baseline
    mandates.files["workorders/0109-watcher-pilot.md"] = SAMPLE_OPEN

    result = await watcher.run_cycle()
    assert result.dispatched == 1
    assert "POST /v1/workorders" in backend.calls
    assert "POST /v1/episodes/wo-0109-watcher-pilot/transition" in backend.calls
    assert backend.states["wo-0109-watcher-pilot"] == "RUNNING"

    dispatch_file = settings.dispatch_dir / "wo-0109-watcher-pilot.json"
    data = json.loads(dispatch_file.read_text(encoding="utf-8"))
    assert data["episode_id"] == "wo-0109-watcher-pilot"
    assert data["commit"] == mandates.commit
    assert data["workorder_text"] == SAMPLE_OPEN
    assert data["eval_suites"][0]["text"].startswith("# eval suite")
    assert data["eval_suites"][0]["note"] is None

    state = StateStore(settings.state_path).load()
    entry = state.workorders["workorders/0109-watcher-pilot.md"]
    assert entry.status == "dispatched"
    assert entry.episode_created is True
    assert entry.running is True
    assert alert.sent == []


async def test_completed_workorder_is_never_dispatched(tmp_path: Path) -> None:
    settings = make_settings(tmp_path, baseline_on_first_run=False)
    mandates = FakeMandates(
        {
            "workorders/0108-done.md": SAMPLE_DONE,
            "workorders/0109-nostatus.md": "# no bullet here\n",
        }
    )
    backend = FakeGatewayBackend()
    watcher = make_watcher(settings, mandates, backend, RecordingAlert())

    result = await watcher.run_cycle()
    assert result.skipped == 2
    assert backend.calls == []
    state = StateStore(settings.state_path).load()
    assert (
        state.workorders["workorders/0108-done.md"].skip_reason
        == "status=COMPLETED (only OPEN dispatches)"
    )


async def test_unparseable_filename_is_recorded_skipped(tmp_path: Path) -> None:
    settings = make_settings(tmp_path, baseline_on_first_run=False)
    mandates = FakeMandates({"workorders/README-notes.md": "# notes\n"})
    backend = FakeGatewayBackend()
    watcher = make_watcher(settings, mandates, backend, RecordingAlert())

    result = await watcher.run_cycle()
    assert result.skipped == 1
    state = StateStore(settings.state_path).load()
    entry = state.workorders["workorders/README-notes.md"]
    assert entry.status == "skipped"
    assert entry.workorder_id == ""
    assert "does not match" in (entry.skip_reason or "")


async def test_missing_suite_source_adds_reference_note(tmp_path: Path) -> None:
    settings = make_settings(tmp_path, baseline_on_first_run=False)
    mandates = FakeMandates({"workorders/0109-watcher-pilot.md": SAMPLE_OPEN})
    watcher = make_watcher(settings, mandates, FakeGatewayBackend(), RecordingAlert(), suites=None)

    result = await watcher.run_cycle()
    assert result.dispatched == 1
    data = json.loads(
        (settings.dispatch_dir / "wo-0109-watcher-pilot.json").read_text(encoding="utf-8")
    )
    suite = data["eval_suites"][0]
    assert suite["text"] is None
    assert suite["note"] == "eval-gate source not configured"


async def test_suite_missing_in_evalgate_is_noted_not_fatal(tmp_path: Path) -> None:
    settings = make_settings(tmp_path, baseline_on_first_run=False)
    mandates = FakeMandates({"workorders/0109-watcher-pilot.md": SAMPLE_OPEN})
    suites = FakeSuites({})  # configured but empty
    watcher = make_watcher(
        settings, mandates, FakeGatewayBackend(), RecordingAlert(), suites=suites
    )

    result = await watcher.run_cycle()
    assert result.dispatched == 1
    data = json.loads(
        (settings.dispatch_dir / "wo-0109-watcher-pilot.json").read_text(encoding="utf-8")
    )
    suite = data["eval_suites"][0]
    assert suite["text"] is None
    assert suite["note"] == "suite not found in eval-gate ref"


async def test_gateway_failure_backs_off_then_succeeds(tmp_path: Path) -> None:
    settings = make_settings(tmp_path, baseline_on_first_run=False)
    mandates = FakeMandates({"workorders/0109-watcher-pilot.md": SAMPLE_OPEN})
    backend = FakeGatewayBackend()
    alert = RecordingAlert()
    clock = Clock()
    watcher = make_watcher(settings, mandates, backend, alert, clock=clock)

    backend.failures_remaining = 1
    first = await watcher.run_cycle()
    assert first.dispatched == 0 and first.failed == 0
    state = StateStore(settings.state_path).load()
    entry = state.workorders["workorders/0109-watcher-pilot.md"]
    assert entry.status == "dispatching"
    assert entry.attempts == 1
    assert entry.next_attempt_at is not None
    assert alert.sent and alert.sent[0][0] == "WARNING"

    # before the backoff elapses: no retry
    clock.advance(5)
    early = await watcher.run_cycle()
    assert early.resumed == 0
    # after the backoff elapses: resume and complete
    clock.advance(10)
    second = await watcher.run_cycle()
    assert second.resumed == 1
    assert backend.states["wo-0109-watcher-pilot"] == "RUNNING"
    state = StateStore(settings.state_path).load()
    assert state.workorders["workorders/0109-watcher-pilot.md"].status == "dispatched"


async def test_repeated_failures_escalate_to_failed_and_critical(tmp_path: Path) -> None:
    settings = make_settings(tmp_path, baseline_on_first_run=False, max_attempts=2)
    mandates = FakeMandates({"workorders/0109-watcher-pilot.md": SAMPLE_OPEN})
    backend = FakeGatewayBackend()
    backend.failures_remaining = 10_000  # gateway down for the whole test
    alert = RecordingAlert()
    clock = Clock()
    watcher = make_watcher(settings, mandates, backend, alert, clock=clock)

    await watcher.run_cycle()
    clock.advance(60)
    await watcher.run_cycle()  # attempts hits max -> failed
    state = StateStore(settings.state_path).load()
    entry = state.workorders["workorders/0109-watcher-pilot.md"]
    assert entry.status == "failed"
    assert entry.attempts == 2
    assert any(sev == "CRITICAL" and "manual intervention" in msg for sev, msg in alert.sent)
    # failed entries are terminal: no further retries
    clock.advance(600)
    again = await watcher.run_cycle()
    assert again.resumed == 0


async def test_crash_resume_skips_completed_steps(tmp_path: Path) -> None:
    """Ledger says 'episode created and running' but no dispatch file: the
    replay must not re-create, not re-transition, and must write the file."""
    settings = make_settings(tmp_path, baseline_on_first_run=False)
    mandates = FakeMandates({"workorders/0109-watcher-pilot.md": SAMPLE_OPEN})
    backend = FakeGatewayBackend()
    backend.states["wo-0109-watcher-pilot"] = "RUNNING"  # gateway already advanced
    store = StateStore(settings.state_path)
    state = store.load()
    state.workorders["workorders/0109-watcher-pilot.md"] = WorkorderEntry(
        file_path="workorders/0109-watcher-pilot.md",
        workorder_id="wo-0109-watcher-pilot",
        status="dispatching",
        first_seen_commit="b" * 40,
        title="Work order 109",
        episode_id="wo-0109-watcher-pilot",
        episode_created=True,
        running=True,
        attempts=1,
    )
    store.save(state)
    alert = RecordingAlert()
    watcher = make_watcher(settings, mandates, backend, alert)

    result = await watcher.run_cycle()
    assert result.resumed == 1
    # create replayed (idempotent 200), but NO transition call happened
    assert backend.calls.count("POST /v1/workorders") == 1
    assert not any("transition" in call for call in backend.calls)
    assert (settings.dispatch_dir / "wo-0109-watcher-pilot.json").exists()
    state = StateStore(settings.state_path).load()
    assert state.workorders["workorders/0109-watcher-pilot.md"].status == "dispatched"


def test_cycle_result_defaults() -> None:
    result = CycleResult()
    assert result.files_seen == 0
    assert result.baselined == 0
    assert result.dispatched == 0
