"""CLI: build + sign an evidence statement and print it for the skills repo.

Usage (CI sign.yml):
    python -m kernel.evidence_publish --capability NAME --eval-digest SHA \
        --threshold-version V --commit SHA
Prints the signed evidence JSON to stdout (the workflow commits it to the
skills repo evidence/ directory). The signature comes from OpenBao transit;
the private key never exists as a file.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from typing import Any

from kernel.evidence import EvidenceRecord, OpenBaoTransitSigner, build_statement, statement_bytes


async def _run(args: argparse.Namespace) -> int:
    # digest manifest for the capability's spec+eval implementation files:
    # in CI the checkout IS the capability workspace
    artifacts: dict[str, str] = {}
    for path in sorted(_capability_files(args.capability)):
        artifacts[str(path)] = hashlib.sha256(path.read_bytes()).hexdigest()
    if not artifacts:
        print(f"no artifact files found for {args.capability}", file=sys.stderr)
        return 2

    statement = build_statement(
        capability=args.capability,
        commit_sha=args.commit,
        artifacts=artifacts,
        eval_digest=args.eval_digest,
        threshold_version=args.threshold_version,
    )
    signer = OpenBaoTransitSigner(_bao_addr(), token=_bao_token())
    try:
        signature = await signer.sign(statement_bytes(statement))
    finally:
        await signer.aclose()
    record = EvidenceRecord(capability=args.capability, statement=statement, signature=signature)
    print(json.dumps({"statement": record.statement, "signature": record.signature}, indent=2))
    return 0


def _capability_files(capability: str) -> list[Any]:
    """Artifact set for the digest manifest: everything under
    $EVIDENCE_ARTIFACTS_DIR (the capability workspace in CI), falling back to
    skills/<capability>/. Evidence files themselves are excluded."""
    import os
    from pathlib import Path

    base = Path(os.environ.get("EVIDENCE_ARTIFACTS_DIR", "")) or Path("skills") / capability
    if base.is_dir():
        return [
            p
            for p in base.rglob("*")
            if p.is_file() and "evidence" not in p.parts and ".git" not in p.parts
        ]
    return []


def _bao_addr() -> str:
    import os

    addr = os.environ.get("BAO_ADDR")
    if not addr:
        raise SystemExit("BAO_ADDR env is required")
    return addr


def _bao_token() -> str:
    import os

    token = os.environ.get("BAO_TOKEN")
    if not token:
        raise SystemExit("BAO_TOKEN env is required")
    return token


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--capability", required=True)
    parser.add_argument("--eval-digest", required=True)
    parser.add_argument("--threshold-version", required=True)
    parser.add_argument("--commit", required=True)
    args = parser.parse_args()
    import asyncio

    raise SystemExit(asyncio.run(_run(args)))


if __name__ == "__main__":
    main()
