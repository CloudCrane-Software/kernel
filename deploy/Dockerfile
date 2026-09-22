# gateway image — uv sync + uvicorn (kernel repo, WO-03 deployment)
FROM docker:27-cli AS docker-cli

FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS runtime
COPY --from=docker-cli /usr/local/bin/docker /usr/local/bin/docker  # isolated lane: RunscRunner shells out to docker (WO-108 enablement)
WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-install-project
COPY kernel/ kernel/
COPY gateway/ gateway/
COPY reconciler/ reconciler/
COPY pricing/ pricing/
COPY ops/sql/ ops/sql/
RUN uv sync --frozen
EXPOSE 8000
CMD ["uv", "run", "uvicorn", "gateway.app:build_app_from_env", "--factory", "--host", "0.0.0.0", "--port", "8000"]
