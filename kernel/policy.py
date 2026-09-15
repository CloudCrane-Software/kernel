"""kernel/policy.py — OPA client for the kernel.grant policy (WO-02).

Fail-closed by construction:
- any transport/HTTP/shape error raises PolicyUnavailable (the gateway maps
  this to DEPENDENCY_UNAVAILABLE);
- results are NEVER cached — every decision re-queries OPA, and the caller
  is expected to have re-read grant/registry state beforehand.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx

_DECISION_PATH = "/v1/data/kernel/grant/decision"


class PolicyError(Exception):
    """Base policy client error."""


class PolicyUnavailable(PolicyError):
    """OPA could not be reached or answered unusably (fail closed)."""


@dataclass(frozen=True, slots=True)
class PolicyDecision:
    outcome: str  # ALLOW | DENY | NEED_EVIDENCE | DEFER
    reasons: list[str]
    decision_id: str
    registry_revision: int


class PolicyClient:
    """HTTP client to a running OPA server with the kernel.grant policy."""

    def __init__(self, base_url: str, *, timeout: float = 5.0) -> None:
        self._client = httpx.AsyncClient(base_url=base_url, timeout=timeout)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def decide(self, payload: dict[str, Any]) -> PolicyDecision:
        """Evaluate kernel.grant.decision for the given input document.

        The payload must carry decision_id (echoed back) and now (RFC3339).
        No caching — this is a live query every time.
        """
        try:
            resp = await self._client.post(_DECISION_PATH, json={"input": payload})
        except httpx.HTTPError as e:
            raise PolicyUnavailable(f"opa unreachable: {e}") from e
        if resp.status_code != 200:
            raise PolicyUnavailable(f"opa status {resp.status_code}: {resp.text[:200]}")
        try:
            result = resp.json()["result"]
            decision = PolicyDecision(
                outcome=result["outcome"],
                reasons=list(result["reasons"]),
                decision_id=result["decision_id"],
                registry_revision=int(result["registry_revision"]),
            )
        except (KeyError, TypeError, ValueError) as e:
            raise PolicyUnavailable(f"unusable decision shape: {e}") from e
        if decision.outcome not in {"ALLOW", "DENY", "NEED_EVIDENCE", "DEFER"}:
            raise PolicyUnavailable(f"unknown outcome {decision.outcome!r}")
        return decision
