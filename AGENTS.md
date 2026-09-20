# AGENTS.md — kernel

One sentence: the governance kernel monorepo — six-table ledger, OPA policies,
action gateway and reconciler that make every external action authorized,
budgeted, lease-fenced and evidenced.

## Directory map

```
kernel/        data access layer (asyncpg) + decision ledger domain
gateway/       action gateway — FastAPI, the only door to external effects
reconciler/    three-state reconciler for UNKNOWN intents
kernel/executor/  episode state machine (RESERVED->RUNNING->VERIFYING->CLOSED) + Restate adapter
kernel/runner/    bounded task runners (local subprocess / JiuwenBox sandbox / gVisor runsc isolation tier — WO-104 scheduler: kernel/runner/scheduler.py)
pricing/       cost engine placeholder (M3)
policies/      OPA Rego policies + tests (opa test policies/)
ops/sql/       explicit DDL migrations (applied in order, idempotent)
tests/         pytest suite (unit + testcontainers integration)
docs/adr/      architecture decision records
docs/mandate-format.md   mandate file format (WO-03)
```

## Build & test commands

```bash
uv sync                                # install pinned deps from uv.lock
uv run pytest                          # full suite (needs docker for -m integration)
uv run pytest -m "not integration"     # fast unit-only pass
uv run ruff check .
uv run ruff format --check .
uv run mypy .
uv run pre-commit run --all-files
```

## Code conventions

- Python 3.12, uv only; `uv.lock` committed and authoritative.
- ruff (lint+format), mypy strict (all modules), pytest.
- Commits: Conventional Commits, English (`feat:` `fix:` `chore:` `eval:` `docs:`).
- SQL only for DDL; Rego only for OPA policies; no ORM auto-table creation.

## Prohibitions (each rule has an executable checkpoint)

1. Never provide any path that moves an UNKNOWN intent to a terminal state
   (no close-unknown endpoint, no reconciliation shortcut).
   Checkpoint: `uv run pytest tests/test_architecture_guards.py`.
2. Never cache an ALLOW decision or skip the OPA check on the request path.
   Checkpoint: `tests/test_architecture_guards.py` + gateway contract tests.
3. Never weaken a CHECK constraint/trigger in ops/sql (only additive changes).
   Checkpoint: `uv run pytest tests/test_db_invariants.py` (P-3/P-4/F-2/append-only).
4. Never put secrets in code/config/tests.
   Checkpoint: secret-pattern scan in CI.
5. Never modify eval thresholds/suites in the same PR as product code.
   Checkpoint: `.github/workflows/guard.yml`.
6. Never push directly to main.
   Checkpoint: branch protection.

## Working rules for agents

- Read this file before doing anything in this repo.
- Work orders arrive as files; do not expand scope; stop and report on
  anything unclear or blocked.
- Every non-trivial change ships with tests that would fail without it.
- Idempotency, fencing and budget occupation are protocol invariants — when
  in doubt, fail closed (DENY/DEPENDENCY_UNAVAILABLE), never guess.
