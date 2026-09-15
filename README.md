# kernel

Governance kernel — the protocol core that binds authorization (mandate →
grant), budget (reservation), lease (fencing epoch) and evidence to every
action. One sentence: no external side effect happens untracked.

Layout: `kernel/` (six-table data layer + decision ledger), `gateway/` (action
gateway FastAPI), `reconciler/` (three-state reconciler), `pricing/` (cost
engine placeholder), `policies/` (OPA Rego), `ops/sql/` (DDL migrations),
`tests/`, `docs/adr/`.

## Commands

```bash
uv sync          # install pinned deps
uv run pytest    # test suite
uv run ruff check . && uv run ruff format --check .
uv run mypy .
```

See `AGENTS.md` for the map and the prohibitions (eval assets, secrets,
gateway bypass, DDL invariants). Architecture background: KGT-1.0 spec.
