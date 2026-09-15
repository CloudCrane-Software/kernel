"""reconciler/adapters.py — external authoritative-state probes (WO-06).

Classification MUST come from an external authority (the external system's
actual state), never from kernel's own logs. Each adapter answers one
question for an intent: did the external effect happen?
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import httpx


class ExternalStateAdapter(Protocol):
    name: str

    async def probe(self, intent_id: str, params: dict[str, Any]) -> str:
        """Return APPLIED | NOT_APPLIED | UNKNOWN. Never guess — probe."""
        ...


@dataclass(slots=True)
class LogFileExistsAdapter:
    """Example adapter 1: an external system that manifests effects as files
    (e.g. a drop directory). File exists => APPLIED; definitively absent
    (parent dir reachable but entry missing) => NOT_APPLIED; parent missing
    => UNKNOWN (cannot establish authority)."""

    name: str = "log_file_exists"

    async def probe(self, intent_id: str, params: dict[str, Any]) -> str:
        path = Path(str(params.get("external_path", "")))
        parent = path.parent
        if not parent.is_dir():
            return "UNKNOWN"
        return "APPLIED" if path.exists() else "NOT_APPLIED"


@dataclass(slots=True)
class HttpHeadAdapter:
    """Example adapter 2: external HTTP resource probe. 2xx/3xx => APPLIED,
    404 => NOT_APPLIED, other statuses / transport errors => UNKNOWN.

    url_template uses {intent_id} formatting against the intent's params.
    """

    url_template: str
    name: str = "http_head"

    async def probe(self, intent_id: str, params: dict[str, Any]) -> str:
        url = self.url_template.format(intent_id=intent_id, **params)
        try:
            async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as ac:
                resp = await ac.head(url)
        except httpx.HTTPError:
            return "UNKNOWN"
        if 200 <= resp.status_code < 400:
            return "APPLIED"
        if resp.status_code == 404:
            return "NOT_APPLIED"
        return "UNKNOWN"


class AdapterRegistry:
    def __init__(self) -> None:
        self._adapters: dict[str, ExternalStateAdapter] = {}

    def register(self, action_type: str, adapter: ExternalStateAdapter) -> None:
        self._adapters[action_type] = adapter

    def get(self, action_type: str) -> ExternalStateAdapter | None:
        return self._adapters.get(action_type)

    def register_defaults(self) -> None:
        self.register("external.file", LogFileExistsAdapter())
