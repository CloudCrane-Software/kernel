"""Architecture guards (WO-06 item 5, manual §5.6): grep-level structural
constraints that must hold over the whole codebase.

Guard 1: no route or code path may close an UNKNOWN intent administratively.
Guard 2: ALLOW decisions must never be cached (no decision caches in the
request path).
Guard 3: no admin bypass endpoints on the gateway.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent

# modules allowed to move UNKNOWN intents to a terminal state (external
# evidence only): the receipt classifier path and the reconciler probes
UNKNOWN_WRITERS_ALLOWED = ("gateway/service.py", "reconciler/core.py", "kernel/db.py")

_FORBIDDEN_ROUTE_PATTERNS = re.compile(
    r"close.?unknown|/admin|bypass|force.?close|override.?state", re.IGNORECASE
)

# any write of an intent's state column (literal or parameterized)
_INTENT_STATE_WRITE = re.compile(r"UPDATE action_intents SET state", re.IGNORECASE)


def _py_sources() -> list[Path]:
    """Production code only — tests legitimately simulate state changes."""
    out: list[Path] = []
    for pkg in ("gateway", "kernel", "reconciler", "pricing"):
        base = REPO / pkg
        if base.exists():
            out.extend(base.rglob("*.py"))
    return out


def test_no_close_unknown_or_admin_routes() -> None:
    """No endpoint (or helper naming) may expose administrative closing of
    UNKNOWN intents or any gateway bypass."""
    for path in _py_sources():
        rel = path.relative_to(REPO).as_posix()
        text = path.read_text(encoding="utf-8")
        for m in _FORBIDDEN_ROUTE_PATTERNS.finditer(text):
            line = text[: m.start()].count("\n") + 1
            raise AssertionError(
                f"{rel}:{line}: forbidden pattern {m.group()!r} "
                "(no close-unknown / admin / bypass paths may exist)"
            )


def test_intent_state_writers_are_whitelisted() -> None:
    """ANY SQL writing action_intents.state may only exist in the
    evidence-driven paths (receipt classification, reconciler probes) —
    parameterized or literal. New writers need an explicit whitelist entry
    plus an audit trail of why they constitute external evidence."""
    for path in _py_sources():
        rel = path.relative_to(REPO).as_posix()
        text = path.read_text(encoding="utf-8")
        if not _INTENT_STATE_WRITE.search(text):
            continue
        assert rel in UNKNOWN_WRITERS_ALLOWED, (
            f"{rel} writes action_intents.state; only "
            f"{UNKNOWN_WRITERS_ALLOWED} are allowed (external-evidence paths)"
        )


def test_no_decision_result_caching() -> None:
    """The request path must not cache policy decisions: no lru/dict/TTL
    caches may wrap decision or grant reads in gateway/kernel policy code."""
    cache_patterns = re.compile(
        r"lru_cache|cachetools|@cache\b|ttl_cache|_decision_cache|decision_cache",
        re.IGNORECASE,
    )
    for name in ("gateway", "kernel"):
        base = REPO / name
        if not base.exists():
            continue
        for path in base.rglob("*.py"):
            rel = path.relative_to(REPO).as_posix()
            text = path.read_text(encoding="utf-8")
            assert not cache_patterns.search(text), (
                f"{rel}: decision/authority caching is forbidden "
                "(every decision re-queries OPA and re-reads grant state)"
            )


def test_no_hardcoded_secrets() -> None:
    secret = re.compile(r"(ghp_[A-Za-z0-9]{20,}|sk-[A-Za-z0-9]{20,}|AKIA[A-Z0-9]{16})")
    candidates = list(_py_sources())
    for pattern in ("*.sql", "*.yml", "*.yaml", "*.rego"):
        for pkg in ("ops", "policies", ".github"):
            base = REPO / pkg
            if base.exists():
                candidates.extend(base.rglob(pattern))
    for path in candidates:
        if ".venv" in path.parts:
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        assert not secret.search(text), f"{path.relative_to(REPO)}: secret-like string"
