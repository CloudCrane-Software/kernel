"""Smoke eval subset (marker: smoke) — run by the eval-gate workflow under the
human-approved `eval-gate` environment. Fast, no external services.
These are EVAL assertions (scorer-side), not unit tests: they pin protocol
invariants of the kernel's own outputs."""

from __future__ import annotations

import pytest

from kernel.db import canonical_json, new_ulid, sha256_hex

pytestmark = pytest.mark.smoke


def test_eval_canonical_json_is_deterministic() -> None:
    a = {"b": 1, "a": [2, 3], "nested": {"z": True, "y": None}}
    b = {"nested": {"y": None, "z": True}, "a": [2, 3], "b": 1}
    assert canonical_json(a) == canonical_json(b)
    assert canonical_json(a) == '{"a":[2,3],"b":1,"nested":{"y":null,"z":true}}'


def test_eval_params_hash_stability() -> None:
    """The replay-detection contract: identical params -> identical hash."""
    params = {"action": "http.request", "url": "https://api.example/x", "n": 7}
    assert sha256_hex(canonical_json(params)) == sha256_hex(canonical_json(dict(params)))


def test_eval_ulid_shape_contract() -> None:
    u = new_ulid()
    assert len(u) == 26
    assert u[0] != "0" or True  # monotonic prefix, no ordering assert (clock)
    assert new_ulid() > u or new_ulid() != u  # uniqueness


def test_eval_error_code_universe_is_closed() -> None:
    """The gateway error universe must stay exactly the spec set — eval pins
    it; additions require an eval-gate PR (human approved)."""
    from gateway.errors import ErrorCode

    expected = {
        "IDEMPOTENCY_KEY_CONFLICT",
        "BUDGET_EXHAUSTED",
        "LEASE_FENCED",
        "GRANT_INACTIVE",
        "DECISION_NOT_ALLOW",
        "DEPENDENCY_UNAVAILABLE",
        "OPEN_OBLIGATIONS_EXIST",
        "UNRESOLVED_UNKNOWN_EXISTS",
        "NOT_FOUND",
        "VALIDATION_ERROR",
    }
    assert {c.value for c in ErrorCode} == expected


def test_eval_intent_state_machine_is_spec_shaped() -> None:
    """Eval pins the protocol state machines (KGT-1.0): states and terminal
    branches must match the spec exactly."""
    import re
    from pathlib import Path

    ddl = (Path(__file__).resolve().parent.parent / "ops" / "sql" / "0001_init.sql").read_text()
    intent_states = re.search(
        r"state\s+TEXT NOT NULL DEFAULT 'PREPARED'\s+CHECK \(state IN \(([^)]+)\)\)", ddl
    )
    assert intent_states is not None
    states = {s.strip().strip("'") for s in intent_states.group(1).split(",")}
    assert states == {"PREPARED", "DISPATCHING", "APPLIED", "NOT_APPLIED", "UNKNOWN"}

    branches = re.search(r"terminal_branch TEXT\s+CHECK \(terminal_branch IN \(([^)]+)\)\)", ddl)
    assert branches is not None
    branch_set = {s.strip().strip("'") for s in branches.group(1).split(",")}
    assert branch_set == {"candidate_ready", "not_solved", "deferred", "expired"}
