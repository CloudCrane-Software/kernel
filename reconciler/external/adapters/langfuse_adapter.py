"""reconciler/external/adapters/langfuse_adapter.py — Langfuse drift adapter (WO-101 #6).

Reconciliation surface: count of traces named `wo101-drift-recon` (Public
API /api/public/traces, paginated). Injection: POST /api/public/ingestion
one trace-create item in the test namespace; cleanup DELETE
/api/public/traces/<id>; post-cleanup snapshot must equal baseline.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, Protocol

import httpx

from reconciler.external.model import InjectedResource, utcnow

DEFAULT_TRACE_NAME = "wo101-drift-recon"
_PAGE_SIZE = 100


class LangfuseClient(Protocol):
    async def ingest_trace(self, trace_id: str, name: str) -> None: ...

    async def list_trace_ids(self, name: str) -> list[str]: ...

    async def delete_trace(self, trace_id: str) -> None: ...


class HttpxLangfuse:
    def __init__(
        self,
        url: str,
        public_key: str,
        secret_key: str,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        timeout: float = 10.0,
        attempts: int = 3,
    ) -> None:
        self._url = url.rstrip("/")
        self._auth = (public_key, secret_key)
        self._transport = transport
        self._timeout = timeout
        self._attempts = attempts

    async def _client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=self._transport, timeout=self._timeout)

    async def _send(self, request: Callable[[], Awaitable[httpx.Response]]) -> httpx.Response:
        """The public API hiccups under ingestion load (observed live: a
        transient 422/503 mid-settle) — retry idempotent sends briefly."""
        import asyncio

        last_error: Exception | None = None
        for attempt in range(self._attempts):
            try:
                resp = await request()
            except httpx.HTTPError as exc:
                last_error = exc
            else:
                if resp.status_code < 500 and resp.status_code != 422:
                    return resp
                last_error = RuntimeError(f"transient status {resp.status_code}")
            if attempt < self._attempts - 1:
                await asyncio.sleep(1.0 * (attempt + 1))
        raise RuntimeError(f"langfuse request failed after {self._attempts} attempts: {last_error}")

    async def ingest_trace(self, trace_id: str, name: str) -> None:
        now = utcnow().isoformat()

        async def send() -> httpx.Response:
            client = await self._client()
            return await client.post(
                f"{self._url}/api/public/ingestion",
                json={
                    "batch": [
                        {
                            "type": "trace-create",
                            "id": trace_id,
                            "timestamp": now,
                            # v3 ingestion API: the event payload rides in `body`
                            "body": {"id": trace_id, "name": name, "timestamp": now},
                        }
                    ]
                },
                auth=self._auth,
            )

        resp = await self._send(send)
        if resp.status_code >= 300:
            raise RuntimeError(f"langfuse ingestion -> {resp.status_code}: {resp.text[:200]}")
        # 207 multi-status: per-item verdicts decide success
        try:
            body: dict[str, Any] = resp.json()
        except ValueError:
            return
        errors = [e for e in body.get("errors", []) if isinstance(e, dict)]
        if errors:
            raise RuntimeError(f"langfuse ingestion item errors: {str(errors)[:200]}")

    async def list_trace_ids(self, name: str) -> list[str]:
        ids: list[str] = []
        page = 1
        while True:

            async def send(page: int = page) -> httpx.Response:
                client = await self._client()
                return await client.get(
                    f"{self._url}/api/public/traces",
                    params={"name": name, "page": page, "limit": _PAGE_SIZE},
                    auth=self._auth,
                )

            resp = await self._send(send)
            if resp.status_code >= 300:
                raise RuntimeError(f"langfuse traces -> {resp.status_code}")
            body = resp.json()
            data = body.get("data", [])
            ids.extend(str(item.get("id")) for item in data if isinstance(item, dict))
            meta = body.get("meta") or {}
            total_pages = int(meta.get("totalPages") or 1)
            if page >= total_pages or not data:
                return ids
            page += 1

    async def delete_trace(self, trace_id: str) -> None:
        async def send() -> httpx.Response:
            client = await self._client()
            return await client.delete(f"{self._url}/api/public/traces/{trace_id}", auth=self._auth)

        resp = await self._send(send)
        if resp.status_code >= 300:
            raise RuntimeError(f"langfuse trace delete -> {resp.status_code}")


class LangfuseAdapter:
    name = "langfuse"

    def __init__(self, client: LangfuseClient, *, trace_name: str = DEFAULT_TRACE_NAME) -> None:
        self._client = client
        self._trace_name = trace_name

    async def snapshot(self) -> dict[str, int]:
        return {f"trace:{tid}": 1 for tid in await self._client.list_trace_ids(self._trace_name)}

    async def inject(self, case_id: str) -> InjectedResource:
        trace_id = f"wo101-drift-{case_id}-{utcnow().strftime('%H%M%S%f')}"
        await self._client.ingest_trace(trace_id, self._trace_name)
        return InjectedResource(
            system=self.name, case_id=case_id, handle=trace_id, created_at=utcnow()
        )

    async def cleanup(self, resource: InjectedResource) -> None:
        await self._client.delete_trace(resource.handle)
