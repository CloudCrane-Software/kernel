"""reconciler/external/adapters/clickhouse_adapter.py — ClickHouse drift adapter (WO-101 #2).

Reconciliation surface: row counts of dedicated `wo101_drift_*` MEMORY
tables (read from system.tables — pure metadata, no table scans). Injection:
CREATE one dedicated MEMORY table + INSERT rows (in-memory, dies with the
server anyway); cleanup DROPs it and the post-cleanup snapshot must equal
baseline. HTTP :8123 with basic auth — never kernel-internal logs.
"""

from __future__ import annotations

from typing import Protocol

import httpx

from reconciler.external.model import InjectedResource, utcnow

DEFAULT_TABLE_PREFIX = "wo101_drift_"


class ClickHouseQuery(Protocol):
    async def query(self, sql: str, *, data: str | None = None) -> str: ...


class HttpxClickHouse:
    def __init__(
        self,
        url: str,
        user: str = "default",
        password: str = "",
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        timeout: float = 10.0,
    ) -> None:
        self._url = url.rstrip("/")
        self._auth = (user, password)
        self._transport = transport
        self._timeout = timeout

    async def query(self, sql: str, *, data: str | None = None) -> str:
        async with httpx.AsyncClient(transport=self._transport, timeout=self._timeout) as client:
            resp = await client.post(
                f"{self._url}/", params={"query": sql}, content=data, auth=self._auth
            )
        if resp.status_code != 200:
            raise RuntimeError(f"clickhouse query failed: {resp.status_code}: {resp.text[:200]}")
        return resp.text


def _like_pattern(prefix: str) -> str:
    escaped = prefix.replace("\\", "\\\\").replace("_", r"\_").replace("%", r"\%")
    return escaped + "%"


class ClickHouseAdapter:
    name = "clickhouse"

    def __init__(
        self,
        client: ClickHouseQuery,
        *,
        table_prefix: str = DEFAULT_TABLE_PREFIX,
        database: str = "default",
    ) -> None:
        self._client = client
        self._prefix = table_prefix
        self._database = database

    async def snapshot(self) -> dict[str, int]:
        sql = (
            f"SELECT name, total_rows FROM system.tables "
            f"WHERE database = '{self._database}' AND engine = 'Memory' "
            f"AND name LIKE '{_like_pattern(self._prefix)}' FORMAT TSV"
        )
        counters: dict[str, int] = {}
        for line in (await self._client.query(sql)).splitlines():
            if not line.strip():
                continue
            name, _, rows = line.partition("\t")
            counters[f"rows:{name}"] = int(rows or 0)
        return counters

    async def inject(self, case_id: str) -> InjectedResource:
        table = f"{self._prefix}{case_id}"
        await self._client.query(
            f"CREATE TABLE {self._database}.{table} (k String, v UInt32) ENGINE = Memory"
        )
        await self._client.query(
            f"INSERT INTO {self._database}.{table} FORMAT CSV", data="alpha,1\nbeta,2\ngamma,3\n"
        )
        return InjectedResource(
            system=self.name, case_id=case_id, handle=table, created_at=utcnow()
        )

    async def cleanup(self, resource: InjectedResource) -> None:
        await self._client.query(f"DROP TABLE IF EXISTS {self._database}.{resource.handle}")
