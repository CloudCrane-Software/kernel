"""Evidence chain tests (manual §6.3): sign -> persist -> verify; tampering
with the digest or the statement breaks verification.

The OpenBao Transit API is faked with httpx.MockTransport (same request/
response shapes as the real API); production goes through the same code path
against the live Bao.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import httpx
import pytest
import pytest_asyncio

from kernel.evidence import (
    EvidenceRecord,
    FileEvidenceStore,
    OpenBaoTransitSigner,
    build_statement,
    statement_bytes,
    verify,
)


def _fake_bao_client() -> httpx.AsyncClient:
    """Fake transit: signature is derived from the payload (deterministic),
    verify recomputes it. Same API shape as OpenBao transit."""

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode("utf-8"))
        is_verify = "/transit/verify/" in request.url.path
        sig = "vault:v1:" + hashlib.sha256(body["input"].encode()).hexdigest()[:32]
        if is_verify:
            return httpx.Response(200, json={"data": {"valid": body["signature"] == sig}})
        return httpx.Response(200, json={"data": {"signature": sig}})

    return httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="http://bao.local")


@pytest_asyncio.fixture
async def signer() -> AsyncIterator[OpenBaoTransitSigner]:
    client = _fake_bao_client()
    s = OpenBaoTransitSigner("http://bao.local", token="t", client=client)
    yield s
    await client.aclose()


@pytest_asyncio.fixture
async def store(tmp_path: Path) -> FileEvidenceStore:
    return FileEvidenceStore(tmp_path / "evidence")


def _statement(cap: str, artifact_digest: str) -> dict[str, Any]:
    return build_statement(
        capability=cap,
        commit_sha="abc123",
        artifacts={f"{cap}.py": artifact_digest},
        eval_digest="evald",
        threshold_version="v1",
    )


async def test_sign_persist_verify_roundtrip(
    store: FileEvidenceStore, signer: OpenBaoTransitSigner, tmp_path: Path
) -> None:
    del tmp_path
    cap = "demo-capability"
    digest = hashlib.sha256(b"artifact-bytes").hexdigest()
    statement = _statement(cap, digest)
    signature = await signer.sign(statement_bytes(statement))
    path = store.save(EvidenceRecord(capability=cap, statement=statement, signature=signature))
    assert path.exists()

    ok = await verify(store, signer, cap, artifact_name=f"{cap}.py", artifact_digest=digest)
    assert ok is True


async def test_tampered_digest_fails(
    store: FileEvidenceStore, signer: OpenBaoTransitSigner
) -> None:
    cap = "tamper-digest"
    statement = _statement(cap, hashlib.sha256(b"original").hexdigest())
    signature = await signer.sign(statement_bytes(statement))
    store.save(EvidenceRecord(capability=cap, statement=statement, signature=signature))

    ok = await verify(
        store,
        signer,
        cap,
        artifact_name=f"{cap}.py",
        artifact_digest=hashlib.sha256(b"tampered").hexdigest(),
    )
    assert ok is False


async def test_tampered_statement_fails_signature(
    store: FileEvidenceStore, signer: OpenBaoTransitSigner
) -> None:
    cap = "tamper-statement"
    digest = hashlib.sha256(b"artifact").hexdigest()
    statement = _statement(cap, digest)
    signature = await signer.sign(statement_bytes(statement))
    # attacker edits the statement after signing
    statement["predicate"]["evalDigest"] = "faked"
    store.save(EvidenceRecord(capability=cap, statement=statement, signature=signature))

    ok = await verify(store, signer, cap, artifact_name=f"{cap}.py", artifact_digest=digest)
    assert ok is False


async def test_evidence_is_immutable(
    store: FileEvidenceStore, signer: OpenBaoTransitSigner
) -> None:
    cap = "immutable-cap"
    statement = _statement(cap, "d" * 64)
    signature = await signer.sign(statement_bytes(statement))
    store.save(EvidenceRecord(capability=cap, statement=statement, signature=signature))
    from kernel.evidence import EvidenceError

    with pytest.raises(EvidenceError):
        store.save(EvidenceRecord(capability=cap, statement=statement, signature=signature))


async def test_missing_evidence_fails_closed(
    store: FileEvidenceStore, signer: OpenBaoTransitSigner
) -> None:
    ok = await verify(store, signer, "never-existed", artifact_name="x", artifact_digest="y")
    assert ok is False
