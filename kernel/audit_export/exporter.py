"""AuditExporter — poll Restate invocations, publish to Kafka + PG (WO-01).

State machine:

- cursor persisted in PG (``audit_export_state``): last ``(created_at, id)``
  exported; survives restarts, so re-running never re-processes old records
  (and even if it did, ``audit_events`` upserts are idempotent and Kafka
  events are keyed by invocation id).
- each poll fetches invocations strictly after the cursor, publishes one
  JSON event per invocation to the ``audit-events`` topic, upserts the PG
  ``audit_events`` row, then advances the cursor only after the whole batch
  succeeded (fail -> retry the batch; every step is idempotent).
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, Protocol

from kernel.audit_export.restate_client import RestateAdminClient, parse_ts
from kernel.db import Database

logger = logging.getLogger(__name__)

AUDIT_EVENTS_TOPIC_DEFAULT = "audit-events"
EVENT_TYPE = "restate.invocation"

_CURSOR_SINGLETON = 1


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"not JSON serializable: {type(value).__name__}")


def dumps(event: dict[str, Any]) -> str:
    """Canonical serialization for audit events (ISO datetimes)."""
    return json.dumps(event, default=_json_default, separators=(",", ":"), ensure_ascii=False)


def _now_utc() -> datetime:
    return datetime.now(tz=UTC)


class AuditEventPublisher(Protocol):
    """Where audit events go (Kafka in prod, fake in tests)."""

    async def start(self) -> None: ...

    async def publish(self, key: str, event: dict[str, Any]) -> None: ...

    async def stop(self) -> None: ...


class KafkaAuditEventPublisher:
    """aiokafka-backed publisher; the only Kafka-aware production piece."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str = AUDIT_EVENTS_TOPIC_DEFAULT,
        *,
        client_id: str = "audit-exporter",
    ) -> None:
        from aiokafka import AIOKafkaProducer  # lazy: keeps unit imports broker-free

        self._topic = topic
        self._producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            client_id=client_id,
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: dumps(v).encode("utf-8"),
        )

    async def start(self) -> None:
        await self._producer.start()

    async def publish(self, key: str, event: dict[str, Any]) -> None:
        await self._producer.send_and_wait(self._topic, key=key, value=event)

    async def stop(self) -> None:
        await self._producer.stop()


def build_event(invocation: dict[str, Any], *, exported_at: datetime) -> dict[str, Any]:
    """Wire shape of one audit event: type + timestamp + the raw record."""
    return {
        "event_type": EVENT_TYPE,
        "exported_at": exported_at.isoformat(),
        "invocation": invocation,
    }


def _as_dt(value: Any) -> datetime | None:
    return parse_ts(value)


async def upsert_audit_event(db: Database, event: dict[str, Any]) -> None:
    """Idempotently write one audit event into ``audit_events``.

    The same function backs the exporter and the replay consumer, so a
    replay rebuilds byte-identical rows (``exported_at`` comes from the
    event, not from wall-clock at apply time).
    """
    invocation = dict(event.get("invocation") or {})
    invocation_id = invocation.get("id")
    if not invocation_id or not isinstance(invocation_id, str):
        raise ValueError(f"audit event without invocation id: {event!r:.200}")
    exported_at = event.get("exported_at")
    async with db.pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO audit_events
                (invocation_id, service_name, handler_name, status,
                 created_at, raw, exported_at)
            VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
            ON CONFLICT (invocation_id) DO UPDATE SET
                service_name = EXCLUDED.service_name,
                handler_name = EXCLUDED.handler_name,
                status = EXCLUDED.status,
                created_at = EXCLUDED.created_at,
                raw = EXCLUDED.raw,
                exported_at = EXCLUDED.exported_at
            """,
            invocation_id,
            invocation.get("target_service_name"),
            invocation.get("target_handler_name"),
            invocation.get("status"),
            _as_dt(invocation.get("created_at")),
            dumps(invocation),
            _as_dt(exported_at),
        )


class AuditExporter:
    """Poll-loop service: Restate -> Kafka ``audit-events`` + PG ``audit_events``."""

    def __init__(
        self,
        db: Database,
        restate: RestateAdminClient,
        publisher: AuditEventPublisher,
        *,
        poll_interval: float = 10.0,
        batch_limit: int = 500,
        exported_at_clock: Callable[[], datetime] = _now_utc,
    ) -> None:
        self._db = db
        self._restate = restate
        self._publisher = publisher
        self._poll_interval = poll_interval
        self._batch_limit = batch_limit
        self._clock = exported_at_clock

    # ------------------------------------------------------------- cursor
    async def load_cursor(self) -> tuple[datetime | None, str]:
        async with self._db.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT cursor_created_at, cursor_invocation_id "
                "FROM audit_export_state WHERE singleton = $1",
                _CURSOR_SINGLETON,
            )
        if row is None or row["cursor_created_at"] is None:
            return None, ""
        return row["cursor_created_at"], row["cursor_invocation_id"]

    async def save_cursor(self, created_at: datetime, invocation_id: str) -> None:
        async with self._db.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO audit_export_state
                    (singleton, cursor_created_at, cursor_invocation_id, updated_at)
                VALUES ($1, $2, $3, now())
                ON CONFLICT (singleton) DO UPDATE SET
                    cursor_created_at = EXCLUDED.cursor_created_at,
                    cursor_invocation_id = EXCLUDED.cursor_invocation_id,
                    updated_at = now()
                """,
                _CURSOR_SINGLETON,
                created_at,
                invocation_id,
            )

    # ------------------------------------------------------------- polling
    async def poll_once(self) -> int:
        """One poll cycle; returns the number of records exported."""
        cursor_ts, cursor_id = await self.load_cursor()
        rows = await self._restate.fetch_invocations(
            cursor_ts,
            after_id=cursor_id,
            limit=self._batch_limit,
        )
        for row in rows:
            invocation_id = str(row["id"])
            event = build_event(row, exported_at=self._clock())
            await self._publisher.publish(invocation_id, event)
            await upsert_audit_event(self._db, event)
        if rows:
            last = rows[-1]
            last_ts = _as_dt(last.get("created_at")) or _as_dt(last.get("modified_at"))
            if last_ts is not None:
                await self.save_cursor(last_ts, str(last["id"]))
        logger.info("audit export: %d records (cursor=%s)", len(rows), cursor_ts)
        return len(rows)

    async def run(self) -> None:
        """Poll forever until cancelled (SIGTERM/SIGINT at the entrypoint)."""
        await self._publisher.start()
        try:
            while True:
                try:
                    await self.poll_once()
                except Exception:
                    logger.exception("audit export poll failed; retrying next interval")
                await asyncio.sleep(self._poll_interval)
        finally:
            await self._publisher.stop()
