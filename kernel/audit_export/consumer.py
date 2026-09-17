"""AuditConsumer — replayable consumer of the ``audit-events`` topic (WO-0001).

Rebuilds the PG ``audit_events`` table from the Kafka log, idempotently:

- starts at the **earliest** offset (``auto_offset_reset=earliest``), so the
  whole audit history can be replayed onto a fresh database;
- applies events through the same :func:`upsert_audit_event` the exporter
  uses — a replay rebuilds byte-identical rows, including ``exported_at``
  (it is read from the event payload, never from wall-clock at apply time);
- commits offsets only after the batch was applied, so a crash mid-batch
  re-reads (and harmlessly re-upserts) the same events.

The Kafka wiring (aiokafka) is isolated behind :class:`KafkaAuditEventSource`;
tests inject an in-memory source and never touch a broker.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Protocol

from kernel.audit_export.exporter import upsert_audit_event
from kernel.db import Database

logger = logging.getLogger(__name__)


class AuditEventSource(Protocol):
    """Where audit events come from (Kafka in prod, fake in tests)."""

    async def start(self) -> None: ...

    async def getmany(self, *, timeout_ms: int, max_records: int) -> list[dict[str, Any]]: ...

    async def commit(self) -> None: ...

    async def stop(self) -> None: ...


class KafkaAuditEventSource:
    """aiokafka-backed source; the only Kafka-aware production piece.

    ``group_id`` offsets make resume-after-restart cheap, and
    ``auto_offset_reset="earliest"`` makes a fresh group (or a wiped log
    with ``__consumer_offsets`` lost) fall back to a full replay instead of
    silently skipping history.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str = "audit-events",
        *,
        group_id: str = "audit-export-replay",
    ) -> None:
        from aiokafka import AIOKafkaConsumer  # lazy: keeps unit imports broker-free

        self._consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            client_id="audit-consumer",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda raw: raw.decode("utf-8"),
        )

    async def start(self) -> None:
        await self._consumer.start()

    async def getmany(self, *, timeout_ms: int, max_records: int) -> list[dict[str, Any]]:
        batch = await self._consumer.getmany(timeout_ms=timeout_ms, max_records=max_records)
        events: list[dict[str, Any]] = []
        for _tp, messages in batch.items():
            for message in messages:
                value = message.value
                events.append(json.loads(value if isinstance(value, str) else value.decode()))
        return events

    async def commit(self) -> None:
        await self._consumer.commit()

    async def stop(self) -> None:
        await self._consumer.stop()


class AuditConsumer:
    """Replay ``audit-events`` into PG ``audit_events`` (idempotent upserts)."""

    def __init__(
        self,
        db: Database,
        source: AuditEventSource,
        *,
        poll_timeout_ms: int = 1000,
        batch_limit: int = 500,
    ) -> None:
        self._db = db
        self._source = source
        self._poll_timeout_ms = poll_timeout_ms
        self._batch_limit = batch_limit

    async def poll_once(self) -> int:
        """Apply one batch of events; returns the number applied."""
        events = await self._source.getmany(
            timeout_ms=self._poll_timeout_ms, max_records=self._batch_limit
        )
        for event in events:
            await upsert_audit_event(self._db, event)
        if events:
            await self._source.commit()
        logger.info("audit replay: %d events applied", len(events))
        return len(events)

    async def replay_all(self, *, idle_rounds: int = 3) -> int:
        """Drain the topic until idle; returns the total number applied.

        Used by one-shot rebuilds (AUDIT_EXPORT_MODE=replay-wait): the
        consumer is considered caught up after ``idle_rounds`` consecutive
        empty polls.
        """
        applied_total = 0
        idle = 0
        while idle < idle_rounds:
            applied = await self.poll_once()
            applied_total += applied
            idle = 0 if applied else idle + 1
        return applied_total

    async def run(self) -> None:
        """Consume forever until cancelled (SIGTERM/SIGINT at the entrypoint)."""
        await self._source.start()
        try:
            while True:
                try:
                    await self.poll_once()
                except Exception:
                    logger.exception("audit replay poll failed; retrying next interval")
                await asyncio.sleep(0.1)
        finally:
            await self._source.stop()
