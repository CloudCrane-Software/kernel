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


# ------------------------------------------------------------------ WO-108 F11
# The persisted signature section identifies the signer (keyid = transit key
# name/version + algorithm), so third parties can verify without asking us
# which key signed. Legacy bare-string envelopes keep loading.


def test_key_id_parses_version_from_signature(signer: OpenBaoTransitSigner) -> None:
    assert signer.key_id("vault:v1:abcdef") == "eval-signer/v1"
    assert signer.key_id("vault:v12:abcdef") == "eval-signer/v12"
    assert signer.key_id("vault:xx:abcdef") == "eval-signer"  # not a version segment
    assert signer.key_id("garbage") == "eval-signer"


async def test_signed_envelope_persists_keyid_object(
    store: FileEvidenceStore, signer: OpenBaoTransitSigner
) -> None:
    cap = "keyid-cap"
    digest = "a" * 64
    statement = _statement(cap, digest)
    signature = await signer.sign(statement_bytes(statement))
    record = EvidenceRecord(
        capability=cap, statement=statement, signature=signature, keyid=signer.key_id(signature)
    )
    path = store.save(record)
    payload = json.loads(path.read_text(encoding="utf-8"))
    sig = payload["signature"]
    assert isinstance(sig, dict)
    assert sig["value"] == signature
    assert sig["keyid"] == "eval-signer/v1"
    assert sig["algorithm"] == "ed25519"
    # the object envelope round-trips through load and still verifies
    loaded = store.load(cap)
    assert loaded.keyid == "eval-signer/v1"
    assert loaded.signature == signature
    assert await verify(store, signer, cap, artifact_name=f"{cap}.py", artifact_digest=digest)


async def test_load_accepts_legacy_bare_string_signature(
    store: FileEvidenceStore, signer: OpenBaoTransitSigner
) -> None:
    cap = "legacy-cap"
    statement = _statement(cap, "b" * 64)
    legacy_sig = "vault:v1:legacy"
    store._path(cap).write_text(
        json.dumps({"statement": statement, "signature": legacy_sig}), encoding="utf-8"
    )
    record = store.load(cap)
    assert record.signature == legacy_sig
    assert record.keyid == ""
    assert record.algorithm == "ed25519"
    # verify() still processes the loaded record (signature mismatch -> False,
    # but the code path — not the envelope shape — decides)
    ok = await verify(store, signer, cap, artifact_name=f"{cap}.py", artifact_digest="b" * 64)
    assert ok is False


async def test_publish_output_shape_carries_keyid_section(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, signer: OpenBaoTransitSigner
) -> None:
    """kernel.evidence_publish prints the F11 object envelope (the exact JSON
    sign.yml commits to the skills repo evidence/ dir)."""
    import argparse
    import contextlib
    import io

    from kernel import evidence_publish

    cap_dir = tmp_path / "cap"
    cap_dir.mkdir()
    (cap_dir / "spec.md").write_text("spec", encoding="utf-8")

    def fake_signer(base_url: str, *, token: str = "", **kw: object) -> OpenBaoTransitSigner:
        del base_url, token, kw
        return signer

    monkeypatch.setattr(evidence_publish, "OpenBaoTransitSigner", fake_signer)
    monkeypatch.setenv("BAO_ADDR", "http://bao.local")
    monkeypatch.setenv("BAO_TOKEN", "t")
    monkeypatch.setenv("EVIDENCE_ARTIFACTS_DIR", str(cap_dir))
    args = argparse.Namespace(
        capability="shape-cap", eval_digest="e", threshold_version="v1", commit="c" * 40
    )
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        rc = await evidence_publish._run(args)
    assert rc == 0
    out = json.loads(buf.getvalue())
    sig = out["signature"]
    assert isinstance(sig, dict)
    assert sig["keyid"] == "eval-signer/v1"
    assert sig["algorithm"] == "ed25519"
    assert sig["value"].startswith("vault:v1:")
    # the statement inside is exactly what was signed (digest manifest present)
    assert out["statement"]["subject"][0]["name"].endswith("spec.md")
