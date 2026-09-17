# audit-export image — uv sync + python -m kernel.audit_export (WO-0001).
# Internal-only sidecar (no ports): polls the Restate admin API, publishes
# JSON events to Kafka audit-events, upserts PG audit_events.
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS runtime
WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-install-project
COPY kernel/ kernel/
COPY gateway/ gateway/
COPY reconciler/ reconciler/
COPY pricing/ pricing/
COPY ops/sql/ ops/sql/
RUN uv sync --frozen
CMD ["uv", "run", "python", "-m", "kernel.audit_export"]
