# kernel

Governance kernel — the protocol core that binds authorization (mandate →
grant), budget (reservation), lease (fencing epoch) and evidence to every
action. One sentence: no external side effect happens untracked.

Layout: `kernel/` (six-table data layer + decision ledger), `kernel/executor/`
(episode state machine), `kernel/runner/` (bounded task runners including the
gVisor runsc isolation tier — WO-104), `kernel/audit_export/` (Restate →
Kafka → PG audit pipeline — WO-0001), `gateway/` (action gateway FastAPI:
intents plus the episode lifecycle API), `reconciler/` (three-state
reconciler; `reconciler/external/` six-system adapters — WO-101), `pricing/`
(cost engine placeholder here — the operational pricing repo is separate,
WO-102), `policies/` (OPA Rego), `ops/sql/` (DDL migrations), `tests/`,
`docs/adr/`.

## Commands

```bash
uv sync          # install pinned deps
uv run pytest    # test suite
uv run ruff check . && uv run ruff format --check .
uv run mypy .
```

See `AGENTS.md` for the map, the episode/work-order flow, the M2 delivery
status (WO-101..105) and the prohibitions (eval assets, secrets, gateway
bypass, DDL invariants). Architecture background: KGT-1.0 spec.
