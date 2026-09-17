"""Live evidence-chain demo (manual 6.3): sign -> persist -> verify ->
digest-tamper caught -> statement-tamper caught, against the REAL OpenBao
transit key `eval-signer` (the private key never leaves Bao).

Usage:
    KERNEL_BAO_TOKEN=<token> KERNEL_BAO_URL=https://bao.<DOMAIN> \
      uv run python ops/scripts/evidence_demo.py [--out DIR]
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path

from kernel.evidence import (
    EvidenceRecord,
    FileEvidenceStore,
    OpenBaoTransitSigner,
    build_statement,
    statement_bytes,
    verify,
)


async def main(out_dir: Path) -> int:
    cap = "demo-hello-capability"
    artifact = hashlib.sha256(b"print('hello governance')\n").hexdigest()
    stmt = build_statement(
        capability=cap,
        commit_sha="demo0000",
        artifacts={"hello.py": artifact},
        eval_digest=hashlib.sha256(b"eval:pass:5/5").hexdigest(),
        threshold_version="kernel.yaml@v1",
    )
    signer = OpenBaoTransitSigner(
        os.environ["KERNEL_BAO_URL"], token=os.environ["KERNEL_BAO_TOKEN"]
    )
    try:
        sig = await signer.sign(statement_bytes(stmt))
        print("1) signed with eval-signer:", sig[:28], "...")
        store = FileEvidenceStore(out_dir)
        store.save(EvidenceRecord(capability=cap, statement=stmt, signature=sig))
        print("2) evidence persisted:", store._path(cap))  # noqa: SLF001
        ok = await verify(store, signer, cap, artifact_name="hello.py", artifact_digest=artifact)
        print("3) verify(artifact) ->", ok)
        bad = await verify(
            store,
            signer,
            cap,
            artifact_name="hello.py",
            artifact_digest=hashlib.sha256(b"tampered").hexdigest(),
        )
        print("4) tampered digest ->", bad)
        rec = store.load(cap)
        rec.statement["predicate"]["evalDigest"] = "faked-after-signing"
        tstore = FileEvidenceStore(out_dir.parent / f"{out_dir.name}-tampered")
        tpath = tstore._path(cap)  # noqa: SLF001
        tpath.parent.mkdir(parents=True, exist_ok=True)
        tpath.write_text(json.dumps({"statement": rec.statement, "signature": sig}))
        bad2 = await verify(tstore, signer, cap, artifact_name="hello.py", artifact_digest=artifact)
        print("5) tampered statement ->", bad2)
        passed = ok and not bad and not bad2
        print("DEMO:", "PASS" if passed else "FAIL")
        return 0 if passed else 1
    finally:
        await signer.aclose()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=Path("/tmp/evidence-demo"))
    args = parser.parse_args()
    raise SystemExit(asyncio.run(main(args.out)))
