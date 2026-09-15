"""Shared fixtures: a PG16 instance via testcontainers, or KERNEL_TEST_PG_DSN.

CI uses testcontainers (docker on the runner). Local dev may point
KERNEL_TEST_PG_DSN at any PG16 (e.g. a scratch cluster).
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator, Iterator

import pytest
import pytest_asyncio

from kernel.db import Database

_PG_IMAGE = "postgres:16.15"


def _pg_url() -> Iterator[str]:
    env_dsn = os.environ.get("KERNEL_TEST_PG_DSN")
    if env_dsn:
        yield env_dsn
        return
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer(_PG_IMAGE) as pg:
        # testcontainers returns a SQLAlchemy-style URL; asyncpg wants plain.
        raw = pg.get_connection_url()
        raw = raw.replace("postgresql+psycopg2", "postgresql").replace(
            "postgres+psycopg2", "postgresql"
        )
        yield raw


@pytest.fixture(scope="module")
def pg_dsn() -> Iterator[str]:
    yield from _pg_url()


@pytest_asyncio.fixture
async def db(pg_dsn: str) -> AsyncIterator[Database]:
    """Function-scoped: asyncpg pools bind to the creating event loop, and
    pytest-asyncio gives each test a fresh loop."""
    database = await Database.connect(pg_dsn)
    await database.apply_schema()
    yield database
    await database.close()


@pytest_asyncio.fixture
async def fresh_db(pg_dsn: str) -> AsyncIterator[Database]:
    """Alias for db (kept for tests that want to signal raw-state access)."""
    database = await Database.connect(pg_dsn)
    await database.apply_schema()
    yield database
    await database.close()
