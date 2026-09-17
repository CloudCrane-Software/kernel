"""``python -m kernel.audit_export`` — env-driven service entrypoint (WO-0001).

Environment:

- ``RESTATE_ADMIN_URL``     — Restate admin API (default ``http://127.0.0.1:9070``)
- ``KAFKA_BOOTSTRAP``       — Kafka bootstrap servers (default ``localhost:9092``)
- ``KERNEL_PG_DSN``         — Postgres DSN (required; same DB as the kernel)
- ``AUDIT_POLL_INTERVAL``   — exporter poll interval (default ``10s``; accepts
                              ``500ms``/``0.5``/``10s``/``1m``)
- ``AUDIT_EXPORT_MODE``     — ``export`` (default) run the poll loop;
                              ``replay`` rebuild ``audit_events`` from the
                              topic forever; ``replay-wait`` drain the topic
                              once, then exit (fresh-DB rebuilds).
- ``AUDIT_EVENTS_TOPIC``    — topic name (default ``audit-events``)
- ``AUDIT_BATCH_LIMIT``     — max records per poll (default ``500``)

Schema: ops/sql migrations (incl. 0004_audit_export.sql) are applied
idempotently at startup, so the sidecar can be pointed at a fresh database
without a manual migration step.

SIGTERM/SIGINT cancel the run loop cleanly (container stop).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
import sys
from typing import NoReturn

from kernel.audit_export.consumer import AuditConsumer, KafkaAuditEventSource
from kernel.audit_export.exporter import (
    AUDIT_EVENTS_TOPIC_DEFAULT,
    AuditExporter,
    KafkaAuditEventPublisher,
)
from kernel.audit_export.restate_client import RestateAdminClient
from kernel.db import Database

logger = logging.getLogger("kernel.audit_export")

_DEFAULT_INTERVAL = "10s"
_UNIT_SECONDS = {"ms": 0.001, "s": 1.0, "m": 60.0, "h": 3600.0}


def _parse_seconds(raw: str) -> float:
    """Parse ``10s`` / ``500ms`` / ``0.5`` / ``1m`` into seconds."""
    text = raw.strip().lower()
    for unit, factor in _UNIT_SECONDS.items():
        if text.endswith(unit):
            return float(text[: -len(unit)]) * factor
    return float(text)


async def _run_exporter(db: Database, args: dict[str, str]) -> None:
    restate = RestateAdminClient(args["restate_admin_url"], timeout=10.0)
    try:
        exporter = AuditExporter(
            db,
            restate,
            KafkaAuditEventPublisher(args["kafka_bootstrap"], args["topic"]),
            poll_interval=_parse_seconds(args["poll_interval"]),
            batch_limit=int(args["batch_limit"]),
        )
        await exporter.run()
    finally:
        await restate.aclose()


async def _run_consumer(db: Database, args: dict[str, str], *, once: bool) -> None:
    consumer = AuditConsumer(
        db,
        KafkaAuditEventSource(args["kafka_bootstrap"], args["topic"]),
        batch_limit=int(args["batch_limit"]),
    )
    if once:
        await consumer.replay_all()
    else:
        await consumer.run()


async def amain() -> NoReturn:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    dsn = os.environ.get("KERNEL_PG_DSN")
    if not dsn:
        raise SystemExit("KERNEL_PG_DSN is required")
    args = {
        "restate_admin_url": os.environ.get("RESTATE_ADMIN_URL", "http://127.0.0.1:9070"),
        "kafka_bootstrap": os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"),
        "poll_interval": os.environ.get("AUDIT_POLL_INTERVAL", _DEFAULT_INTERVAL),
        "topic": os.environ.get("AUDIT_EVENTS_TOPIC", AUDIT_EVENTS_TOPIC_DEFAULT),
        "batch_limit": os.environ.get("AUDIT_BATCH_LIMIT", "500"),
    }
    mode = os.environ.get("AUDIT_EXPORT_MODE", "export")
    if mode not in ("export", "replay", "replay-wait"):
        raise SystemExit(f"unknown AUDIT_EXPORT_MODE {mode!r}")

    db = await Database.connect(dsn)
    await db.apply_schema()  # idempotent; bootstraps audit_export_state/audit_events

    task: asyncio.Task[None]
    if mode == "export":
        task = asyncio.create_task(_run_exporter(db, args))
    else:
        task = asyncio.create_task(_run_consumer(db, args, once=mode == "replay-wait"))

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, task.cancel)
    try:
        await task
    except asyncio.CancelledError:
        logger.info("audit-export (%s) stopped cleanly", mode)
    finally:
        await db.close()
    sys.exit(0)


def main() -> None:
    # signal path depends on tty coverage
    with contextlib.suppress(KeyboardInterrupt):  # pragma: no cover
        asyncio.run(amain())


if __name__ == "__main__":
    main()
