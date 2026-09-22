"""watcher/gateway.py — the only way the watcher touches episodes: HTTP.

All calls go to the deployed action gateway (fail-closed bearer token from
the environment; never logged). POST /v1/workorders is idempotent by design
(201 fresh / 200 replay with the current state), which is what makes the
watcher's at-least-once loop safe.
"""

from __future__ import annotations

from typing import Any

import httpx

from kernel.watcher.models import CreateWorkorderResult


class GatewayError(RuntimeError):
    def __init__(self, status: int, detail: str) -> None:
        super().__init__(f"gateway {status}: {detail}")
        self.status = status
        self.detail = detail


class GatewayClient:
    def __init__(
        self,
        base_url: str,
        token: str | None,
        timeout: float = 20.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._token = token
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            timeout=timeout,
            headers=headers,
            transport=transport,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def _post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            response = await self._client.post(path, json=payload)
        except httpx.HTTPError as exc:
            raise GatewayError(0, f"request failed: {exc}") from exc
        if response.status_code not in (200, 201):
            detail = self._error_detail(response)
            raise GatewayError(response.status_code, detail)
        try:
            data: dict[str, Any] = response.json()
        except ValueError as exc:
            raise GatewayError(response.status_code, "non-JSON response body") from exc
        return data

    @staticmethod
    def _error_detail(response: httpx.Response) -> str:
        try:
            body = response.json()
        except ValueError:
            return response.text[:200] or "(empty body)"
        if isinstance(body, dict):
            return str(body.get("detail") or body.get("error") or body)[:200]
        return str(body)[:200]

    async def create_workorder(
        self, workorder_id: str, metadata: dict[str, Any] | None = None
    ) -> CreateWorkorderResult:
        data = await self._post(
            "/v1/workorders", {"workorder_id": workorder_id, "metadata": metadata or {}}
        )
        return CreateWorkorderResult(
            episode_id=str(data["episode_id"]),
            state=str(data["state"]),
            created=bool(data.get("created", False)),
        )

    async def transition(self, episode_id: str, target_state: str) -> str:
        payload: dict[str, Any] = {"target_state": target_state}
        data = await self._post(f"/v1/episodes/{episode_id}/transition", payload)
        return str(data.get("state", target_state))
