# AGENTS.md — kernel

One sentence: the governance kernel monorepo — six-table ledger, OPA policies,
action gateway, episode executor and reconciler that make every external
action authorized, budgeted, lease-fenced, reconciled and evidenced.

## Directory map

```
kernel/                  data access layer (asyncpg) + decision ledger domain
kernel/executor/         episode state machine (RESERVED->RUNNING->VERIFYING->CLOSED) + Restate adapter (restate_app.py)
kernel/runner/           bounded task runners: local subprocess (dev/tests default), JiuwenBox sandbox (production), gVisor runsc isolation tier (WO-104) + sandbox_tier routing (scheduler.py)
kernel/audit_export/     Restate -> Kafka audit-events -> PG audit pipeline (WO-0001)
gateway/                 action gateway — FastAPI, the only door to external effects; hosts the episode lifecycle API (WO-0004, see below)
reconciler/              three-state reconciler for UNKNOWN intents
reconciler/external/     six-system external adapters (WO-101: redis/clickhouse/minio/zot/litellm/langfuse) — snapshot + drift reconciliation with disposable-resource injection
pricing/                 cost engine placeholder (M3) in this repo; the operational subscription ledger + daily reports live in the separate pricing repo (WO-102)
policies/                OPA Rego policies + tests (opa test policies/)
ops/sql/                 explicit DDL migrations (applied in order, idempotent)
ops/scripts/             evidence demo (evidence_demo.py)
deploy/                  image definitions (gateway, audit-export sidecar, kernel-task runner image for runsc episodes, images.env version pins)
tests/                   pytest suite (unit + testcontainers integration)
docs/adr/                architecture decision records (0000 process; 0003/0005 are read-only mirrors — see the headers there)
docs/mandate-format.md   mandate file format (WO-03)
```

## Episode lifecycle & work-order flow (WO-0004)

Every work order runs as an episode through the deployed gateway (bearer
admin token from the platform env — never in code, logs or transcripts):

1. open the work order: a file under `workorders/` in the mandates repo,
   merged via CNB MR (no direct pushes anywhere in this system);
2. register the episode: `POST /v1/workorders` — idempotent, seeds RESERVED;
3. drive the one-way state machine via
   `POST /v1/episodes/{episode_id}/transition` (RESERVED -> RUNNING ->
   VERIFYING; illegal moves surface as 409 ILLEGAL_TRANSITION and the
   `trg_episodes_one_way` trigger in `ops/sql/0001_init.sql` stays the final
   authority);
4. develop on a branch: PR -> machine gates (test / opa / guard / eval-smoke)
   -> `sign.yml` evidence signing -> evidence PR (skills repo) -> squash
   merge. Merges are machine-gated auto merges with no human review step
   (ADR-0003: `docs/adr/0003-auto-merge-principle.md`, read-only mirror of
   the .github repo): the system's own PRs and human PRs pass the identical
   gates — same gate, no privilege;
5. close: `POST /v1/episodes/{episode_id}/close` with
   `{"terminal_branch": "candidate_ready"}` (alternatives: not_solved,
   deferred, expired). CLOSED without a terminal branch is impossible
   (CHECK constraint).

## M2 delivery status (WO-101..105, as of 2026-09-20)

- WO-101: reconciler external adapters for the six closed-list systems —
  merged (PR#25-#27), live-verified with disposable drift injection; the
  sandbox task ran on the runsc tier (audit event runtime=runsc).
- WO-102: pricing skeleton + daily report pipeline — operational home is the
  separate pricing repo (subscriptions.yaml, reports/daily branch, cron).
- WO-103: LiteLLM weekly budget windows + LiteLLM→Langfuse success-callback
  wiring (every completion traced to Langfuse for usage governance) — platform
  repo ops/litellm/config.yaml (proxy max_budget + 7d, wo103-plans anchor,
  `success_callback: ["langfuse"]`).
- WO-104: gVisor runsc Provider + sandbox_tier routing — merged (PR#24);
  host runtime registered (release-20260914.0); an isolated-tier task with
  runsc missing fails closed (SchedulerError), never a silent downgrade.
- WO-105: kernel self-rewrite — this documentation, its doc-sync tests, and
  the fully machine-gated self-change closed loop (the loop is the
  deliverable).

Known open items: WO-106 episode evidence binding (defect, open — see the
mandates repo workorders/0106-episode-evidence-binding.md).

Deployment topology: images gateway 0.1.6, audit-export 0.2.0 and the
kernel-task runsc task image on the edge stack (platform repo compose; the
version pins are mirrored in `deploy/images.env` and bound to this line by
`tests/test_docs_sync.py`; OpenBao single-key-box — after any host restart
run the platform ops/scripts/edge-recovery.sh first).

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
- Docs stay equal to reality: a change that makes AGENTS.md/README/docs
  stale updates them in the same PR (sync gates in
  `tests/test_docs_sync.py`, WO-105).
- Unknowns are never closed administratively (manual 7.4). Any urge to have
  a human write code, pass messages or approve on the system's behalf is
  itself a defect: file a defect work order, never work around it (manual
  7.2).
- Idempotency, fencing and budget occupation are protocol invariants — when
  in doubt, fail closed (DENY/DEPENDENCY_UNAVAILABLE), never guess.
