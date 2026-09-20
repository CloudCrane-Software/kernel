"""reconciler/external/adapters/litellm_adapter.py — LiteLLM drift adapter (WO-101 #5).

Reconciliation surface: count of virtual keys whose alias carries the
`wo101-drift/` prefix (Admin API /key/list). Injection: /key/generate one
test key with `duration: 1h` (expiry backstop); cleanup /key/delete. The
token travels only in InjectedResource.aux — never logged, never in code.
"""

from __future__ import annotations

from typing import Protocol

import httpx

from reconciler.external.model import InjectedResource, utcnow

DEFAULT_ALIAS_PREFIX = "wo101-drift/"


class LiteLLMClient(Protocol):
    async def generate_key(self, alias: str, duration: str) -> str: ...

    async def list_keys(self) -> list[tuple[str, str]]: ...

    async def delete_key(self, token: str) -> None: ...


class HttpxLiteLLM:
    def __init__(
        self,
        url: str,
        admin_key: str,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        timeout: float = 10.0,
    ) -> None:
        self._url = url.rstrip("/")
        self._headers = {"Authorization": f"Bearer {admin_key}"}
        self._transport = transport
        self._timeout = timeout

    async def _post(self, path: str, payload: dict[str, object]) -> dict[str, object]:
        async with httpx.AsyncClient(transport=self._transport, timeout=self._timeout) as client:
            resp = await client.post(f"{self._url}{path}", json=payload, headers=self._headers)
        if resp.status_code >= 300:
            raise RuntimeError(f"litellm {path} -> {resp.status_code}: {resp.text[:200]}")
        result: dict[str, object] = resp.json()
        return result

    async def generate_key(self, alias: str, duration: str) -> str:
        result = await self._post("/key/generate", {"key_alias": alias, "duration": duration})
        token = result.get("key")
        if not isinstance(token, str) or not token:
            raise RuntimeError("litellm key/generate returned no key")
        return token

    async def list_keys(self) -> list[tuple[str, str]]:
        result = await self._post("/key/list", {})
        entries = result.get("keys", [])
        if not isinstance(entries, list):
            raise RuntimeError("litellm key/list returned unexpected shape")
        pairs: list[tuple[str, str]] = []
        for entry in entries:
            if isinstance(entry, dict):
                alias = str(entry.get("key_alias") or "")
                token = str(entry.get("token") or entry.get("token_hash") or "")
            else:
                alias, token = "", str(entry)
            pairs.append((alias, token))
        return pairs

    async def delete_key(self, token: str) -> None:
        await self._post("/key/delete", {"keys": [token]})


class LiteLLMAdapter:
    name = "litellm"

    def __init__(self, client: LiteLLMClient, *, alias_prefix: str = DEFAULT_ALIAS_PREFIX) -> None:
        self._client = client
        self._prefix = alias_prefix

    async def snapshot(self) -> dict[str, int]:
        counters: dict[str, int] = {}
        for alias, _token in await self._client.list_keys():
            if alias.startswith(self._prefix):
                counters[f"virtual_key:{alias}"] = 1
        return counters

    async def inject(self, case_id: str) -> InjectedResource:
        alias = f"{self._prefix}{case_id}"
        token = await self._client.generate_key(alias, "1h")
        return InjectedResource(
            system=self.name, case_id=case_id, handle=alias, created_at=utcnow(), aux=token
        )

    async def cleanup(self, resource: InjectedResource) -> None:
        if resource.aux:
            await self._client.delete_key(resource.aux)
