# watcher image — resident mandates poller + episode dispatcher (batch D).
# Runtime needs git (GitCliSource fetches the mounted clones) and nothing
# else beyond the project dependencies. Entry is a plain loop, no uvicorn.
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

# GitCliSource shells out to git (fetch / ls-tree / show on mounted clones)
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-install-project
COPY kernel/ kernel/
COPY gateway/ gateway/
COPY reconciler/ reconciler/
COPY pricing/ pricing/
COPY ops/sql/ ops/sql/
RUN uv sync --frozen

# alert-notify.sh is bind-mounted at /usr/local/bin/alert-notify.sh by compose
CMD ["uv", "run", "python", "-m", "kernel.watcher.main"]
