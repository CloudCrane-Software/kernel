"""Integration tests for kernel.policy.PolicyClient against a real OPA server.

A local `opa run --server` subprocess is started on a free port (works both
locally and in CI — no docker needed).
"""

from __future__ import annotations

import os
import socket
import subprocess
import time
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
import pytest_asyncio

from kernel.policy import PolicyClient, PolicyUnavailable

POLICIES_DIR = Path(__file__).resolve().parent.parent / "policies"


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@pytest.fixture(scope="module")
def opa_server() -> Iterator[str]:
    if env := os.environ.get("KERNEL_TEST_OPA_URL"):
        yield env
        return
    port = _free_port()
    proc = subprocess.Popen(
        ["opa", "run", "--server", "--addr", f"127.0.0.1:{port}", str(POLICIES_DIR)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    base = f"http://127.0.0.1:{port}"
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        try:
            if httpx.get(f"{base}/health", timeout=1).status_code == 200:
                break
        except httpx.HTTPError:
            time.sleep(0.2)
    else:
        proc.kill()
        pytest.fail("opa server did not come up")
    yield base
    proc.terminate()
    proc.wait(timeout=10)


@pytest_asyncio.fixture
async def client(opa_server: str) -> AsyncIterator[PolicyClient]:
    c = PolicyClient(opa_server)
    yield c
    await c.aclose()


def _valid_payload() -> dict[str, Any]:
    return {
        "decision_id": "dec-test-1",
        "now": "2026-09-16T00:00:00Z",
        "mandate": {
            "signer_kind": "human",
            "signature_verified": True,
            "digest": "abc123",
            "registered_digest": "abc123",
            "status": "ACTIVE",
            "expires_at": "2099-01-01T00:00:00Z",
        },
        "grant": {
            "status": "ACTIVE",
            "expiry": "2098-01-01T00:00:00Z",
            "used_calls": 0,
            "call_limit": 10,
            "used_budget": 0,
            "budget_limit": 100,
            "issuer": "mandate:0001",
            "subject": "episode:0001",
            "remaining_depth": 2,
            "scope": {"actions": ["x.*"], "resources": ["*"], "limits": {}},
        },
        "grant_chain": [],
        "registry": {"fresh": True, "reachable": True, "revision": 7},
        "evidence": {"required": [], "provided": []},
    }


async def test_allow_roundtrip(client: PolicyClient) -> None:
    decision = await client.decide(_valid_payload())
    assert decision.outcome == "ALLOW"
    assert decision.decision_id == "dec-test-1"
    assert decision.registry_revision == 7


async def test_deny_roundtrip(client: PolicyClient) -> None:
    payload = _valid_payload()
    payload["grant"]["status"] = "REVOKED"
    decision = await client.decide(payload)
    assert decision.outcome == "DENY"
    assert "grant.not_active" in decision.reasons


async def test_defer_roundtrip(client: PolicyClient) -> None:
    payload = _valid_payload()
    payload["registry"]["fresh"] = False
    decision = await client.decide(payload)
    assert decision.outcome == "DEFER"


async def test_unreachable_fails_closed() -> None:
    c = PolicyClient("http://127.0.0.1:1", timeout=1.0)
    with pytest.raises(PolicyUnavailable):
        await c.decide(_valid_payload())
    await c.aclose()
