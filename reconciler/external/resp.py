"""reconciler/external/resp.py — minimal RESP2 client for the Redis adapter (WO-101).

Implements only what snapshot/inject/cleanup need (AUTH/SELECT/SCAN/SET/
DEL) over asyncio streams. Hand-rolled on purpose: no redis dependency
enters the kernel supply chain for one adapter, and the transport seam
(`RedisTransport`) lets CI tests inject an in-memory fake.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any, Protocol


class RedisProtocolError(RuntimeError):
    pass


class RedisTransport(Protocol):
    async def execute(self, *args: object) -> Any: ...


class RespConnection:
    """One connection; AUTH/SELECT at connect, then plain command exchange."""

    def __init__(
        self,
        host: str,
        port: int = 6379,
        *,
        password: str | None = None,
        db: int = 0,
        timeout: float = 5.0,
    ) -> None:
        self._host = host
        self._port = port
        self._password = password
        self._db = db
        self._timeout = timeout
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None

    async def connect(self) -> None:
        self._reader, self._writer = await asyncio.wait_for(
            asyncio.open_connection(self._host, self._port), self._timeout
        )
        if self._password:
            await self.execute("AUTH", self._password)
        if self._db:
            await self.execute("SELECT", self._db)

    async def execute(self, *args: object) -> Any:
        writer = self._writer
        if writer is None:
            raise RedisProtocolError("not connected")
        encoded = [a if isinstance(a, bytes) else str(a).encode("utf-8") for a in args]
        buf = bytearray(b"*%d\r\n" % len(encoded))
        for item in encoded:
            buf += b"$%d\r\n" % len(item)
            buf += item
            buf += b"\r\n"
        writer.write(bytes(buf))
        await asyncio.wait_for(writer.drain(), self._timeout)
        return await asyncio.wait_for(self._read_reply(), self._timeout)

    async def _read_reply(self) -> Any:
        assert self._reader is not None
        line = await self._reader.readline()
        if not line:
            raise RedisProtocolError("connection closed")
        prefix, body = line[:1], line[1:-2]
        if prefix == b"+":
            return body.decode("utf-8")
        if prefix == b"-":
            raise RedisProtocolError(f"redis error: {body.decode('utf-8', 'replace')}")
        if prefix == b":":
            return int(body)
        if prefix == b"$":
            length = int(body)
            if length == -1:
                return None
            payload = await self._reader.readexactly(length + 2)
            return payload[:-2].decode("utf-8")
        if prefix == b"*":
            count = int(body)
            if count == -1:
                return None
            return [await self._read_reply() for _ in range(count)]
        raise RedisProtocolError(f"bad RESP prefix: {prefix!r}")

    @property
    def connected(self) -> bool:
        return self._writer is not None and self._reader is not None

    async def aclose(self) -> None:
        if self._writer is not None:
            self._writer.close()
            with contextlib.suppress(TimeoutError, ConnectionError):
                await self._writer.wait_closed()
            self._writer = None
            self._reader = None
