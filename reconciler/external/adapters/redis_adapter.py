"""reconciler/external/adapters/redis_adapter.py — Redis drift adapter (WO-101 #1).

Reconciliation surface: per-key counters over SCAN of the dedicated test
prefix (db15, `wo101-drift:*` keys — the work order's closed-list surface).
Injection: one test-prefixed key with `EX 3600` TTL backstop; cleanup DELs
every test-prefixed key and the post-cleanup snapshot must equal baseline.
"""

from __future__ import annotations

from typing import Any

from reconciler.external.model import InjectedResource, utcnow
from reconciler.external.resp import RedisTransport

DEFAULT_PREFIX = "wo101-drift:"


class RedisAdapter:
    name = "redis"

    def __init__(self, client: RedisTransport, *, prefix: str = DEFAULT_PREFIX) -> None:
        self._client = client
        self._prefix = prefix

    async def snapshot(self) -> dict[str, int]:
        return {f"key:{key}": 1 for key in await self._scan_keys()}

    async def inject(self, case_id: str) -> InjectedResource:
        key = f"{self._prefix}{case_id}"
        await self._client.execute("SET", key, f"wo101-drift-payload:{case_id}", "EX", 3600)
        return InjectedResource(system=self.name, case_id=case_id, handle=key, created_at=utcnow())

    async def cleanup(self, resource: InjectedResource) -> None:
        keys = await self._scan_keys()
        if keys:
            await self._client.execute("DEL", *keys)

    async def _scan_keys(self) -> list[str]:
        keys: list[str] = []
        cursor: object = "0"
        while True:
            reply: list[Any] = await self._client.execute(
                "SCAN", cursor, "MATCH", self._prefix + "*", "COUNT", 1000
            )
            cursor = reply[0]
            keys.extend(str(k) for k in reply[1])
            if str(cursor) == "0":
                return sorted(keys)
