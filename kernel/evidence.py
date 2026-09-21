"""kernel/evidence.py — evidence chain (manual §6.3, cosign/in-toto style).

Flow: eval green -> build a digest manifest -> wrap it in an in-toto Statement
(predicate: evaluator=eval-signer, threshold version, pass time) -> sign the
statement bytes with the OpenBao Transit ed25519 key `eval-signer` (the key
never leaves OpenBao) -> persist statement+signature as the capability's
evidence (WO-108 F11: the persisted signature section is an object carrying
keyid + algorithm, so third parties can verify against the published public
key). Reuse of a capability requires verify() to pass on the artifact
digest — memory or claims are never authority.

The private key never exists as a file; evidence is never signed by the
evaluatee (eval-signer serves the eval flow only).
"""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

STATEMENT_TYPE = "https://in-toto.io/Statement/v1"
PREDICATE_TYPE = "https://cloudcrane.software/eval/v1"


class EvidenceError(Exception):
    pass


def _is_key_version(segment: str) -> bool:
    return segment.startswith("v") and segment[1:].isdigit()


def build_statement(
    *,
    capability: str,
    commit_sha: str,
    artifacts: dict[str, str],
    eval_digest: str,
    threshold_version: str,
) -> dict[str, Any]:
    """in-toto statement with a custom eval predicate."""
    now = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    return {
        "_type": STATEMENT_TYPE,
        "subject": [
            {"name": name, "digest": {"sha256": digest}}
            for name, digest in sorted(artifacts.items())
        ],
        "predicateType": PREDICATE_TYPE,
        "predicate": {
            "capability": capability,
            "commit": commit_sha,
            "evaluator": "eval-signer",
            "evalDigest": eval_digest,
            "thresholdVersion": threshold_version,
            "passedAt": now,
        },
    }


def statement_bytes(statement: dict[str, Any]) -> bytes:
    return json.dumps(statement, sort_keys=True, separators=(",", ":")).encode("utf-8")


class OpenBaoTransitSigner:
    """Signs/verifies via OpenBao Transit — the ed25519 key never leaves Bao."""

    def __init__(
        self,
        base_url: str,
        *,
        key_name: str = "eval-signer",
        token: str = "",
        client: httpx.AsyncClient | None = None,
        timeout: float = 10.0,
    ) -> None:
        self._client = client or httpx.AsyncClient(
            base_url=base_url, timeout=timeout, headers={"X-Vault-Token": token}
        )
        self._owns_client = client is None
        self._key_name = key_name

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def sign(self, payload: bytes) -> str:
        body = {"input": base64.b64encode(payload).decode("ascii")}
        resp = await self._client.post(f"/v1/transit/sign/{self._key_name}", json=body)
        if resp.status_code != 200:
            raise EvidenceError(f"bao sign failed: {resp.status_code} {resp.text[:200]}")
        try:
            return str(resp.json()["data"]["signature"])
        except (KeyError, TypeError) as e:
            raise EvidenceError(f"unusable sign response: {e}") from e

    def key_id(self, signature: str) -> str:
        """WO-108 F11: signer identity for the evidence envelope — the transit
        key name plus the key version embedded in the signature payload
        (OpenBao transit signatures carry it as ``vault:vN:<digest>``), e.g.
        ``eval-signer/v1``. Falls back to the bare key name when the payload
        shape is unrecognized. Third parties use this (with the published
        public key) to tell WHICH key version signed an evidence record."""
        parts = signature.split(":", 2)
        version = parts[1] if len(parts) == 3 and _is_key_version(parts[1]) else ""
        return f"{self._key_name}/{version}" if version else self._key_name

    async def verify(self, payload: bytes, signature: str) -> bool:
        body = {
            "input": base64.b64encode(payload).decode("ascii"),
            "signature": signature,
        }
        resp = await self._client.post(f"/v1/transit/verify/{self._key_name}", json=body)
        if resp.status_code != 200:
            return False
        try:
            return bool(resp.json()["data"]["valid"])
        except (KeyError, TypeError):
            return False


@dataclass(frozen=True, slots=True)
class EvidenceRecord:
    capability: str
    statement: dict[str, Any]
    signature: str
    # WO-108 F11: signer identity (OpenBao transit key name/version, e.g.
    # "eval-signer/v1") and signature algorithm — persisted with the record
    # so third parties can verify without asking us which key signed.
    keyid: str = ""
    algorithm: str = "ed25519"


def signature_section(record: EvidenceRecord) -> dict[str, Any] | str:
    """The persisted envelope's signature section (WO-108 F11).

    Records that know their signer carry an object::

        {"value": "<vault:vN:...>", "keyid": "eval-signer/v1", "algorithm": "ed25519"}

    legacy records (bare "vault:vN:..." string) keep their historical shape —
    load() accepts both, already-published evidence never breaks.
    """
    if record.keyid:
        return {"value": record.signature, "keyid": record.keyid, "algorithm": record.algorithm}
    return record.signature


class FileEvidenceStore:
    """Evidence persistence (the skills repo's evidence/ dir, or a local dir)."""

    def __init__(self, root: Path) -> None:
        self._root = root
        self._root.mkdir(parents=True, exist_ok=True)

    def _path(self, capability: str) -> Path:
        safe = capability.replace("/", "__")
        return self._root / f"{safe}.evidence.json"

    def save(self, record: EvidenceRecord) -> Path:
        path = self._path(record.capability)
        if path.exists():
            raise EvidenceError(f"evidence for {record.capability} already exists (immutable)")
        payload = {
            "statement": record.statement,
            "signature": signature_section(record),
        }
        path.write_text(json.dumps(payload, sort_keys=True, indent=2), encoding="utf-8")
        return path

    def load(self, capability: str) -> EvidenceRecord:
        path = self._path(capability)
        if not path.exists():
            raise EvidenceError(f"no evidence for {capability}")
        data = json.loads(path.read_text(encoding="utf-8"))
        sig = data["signature"]
        if isinstance(sig, dict):  # WO-108 F11 object envelope
            return EvidenceRecord(
                capability=capability,
                statement=data["statement"],
                signature=str(sig.get("value", "")),
                keyid=str(sig.get("keyid", "")),
                algorithm=str(sig.get("algorithm", "ed25519")),
            )
        return EvidenceRecord(  # legacy bare-string envelope
            capability=capability,
            statement=data["statement"],
            signature=str(sig),
        )


async def verify(
    store: FileEvidenceStore,
    signer: OpenBaoTransitSigner,
    capability: str,
    *,
    artifact_name: str,
    artifact_digest: str,
) -> bool:
    """Reuse gate: the artifact digest must match a signed statement whose
    predicate carries evaluator=eval-signer, and the signature must verify."""
    try:
        record = store.load(capability)
    except EvidenceError:
        return False
    subject = record.statement.get("subject", [])
    digest_ok = any(
        s.get("name") == artifact_name and s.get("digest", {}).get("sha256") == artifact_digest
        for s in subject
    )
    if not digest_ok:
        return False
    predicate = record.statement.get("predicate", {})
    if predicate.get("evaluator") != "eval-signer":
        return False
    return await signer.verify(statement_bytes(record.statement), record.signature)
