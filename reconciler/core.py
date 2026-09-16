"""reconciler/core.py — level-based three-state reconciliation loop (WO-06).

Drift classification comes ONLY from external authoritative probes:
  APPLIED      -> settle intent, POST reservation, evaluate compensation
  NOT_APPLIED  -> settle intent, VOID reservation
  UNKNOWN      -> exponential backoff (probe_count+1); over threshold the
                  obligation escalates for a human ruling

UNKNOWN is never closed here by assumption or timeout — only a probe (or a
later external receipt) can resolve it.
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from reconciler.adapters import AdapterRegistry

# audit event callback (production: Kafka producer to topic audit-events)
EventCallback = Callable[[dict[str, Any]], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class EscalationPolicy:
    max_probe_count: int = 8
    amount_threshold: int = 10_000  # minor units
    max_age_hours: int = 48


@dataclass(slots=True)
class ReconcileOutcome:
    scanned: int = 0
    resolved_applied: int = 0
    resolved_not_applied: int = 0
    still_unknown: int = 0
    escalated: int = 0
    no_adapter: int = 0
    detail: list[dict[str, Any]] = field(default_factory=list)


@dataclass(slots=True)
class Reconciler:
    db: Any  # kernel.db.Database
    adapters: AdapterRegistry
    policy: EscalationPolicy = field(default_factory=EscalationPolicy)
    on_event: EventCallback | None = None

    async def _emit(self, event: dict[str, Any]) -> None:
        if self.on_event is not None:
            await self.on_event(event)

    async def poll_once(self, limit: int = 50) -> ReconcileOutcome:
        out = ReconcileOutcome()
        async with self.db._pool.acquire() as conn:
            due = await conn.fetch(
                """SELECT i.intent_id, i.action_type, i.params::text AS params_text,
                          r.probe_count, r.next_probe_at
                   FROM action_intents i
                   JOIN reconcile_state r ON r.intent_id = i.intent_id
                   WHERE i.state = 'UNKNOWN' AND r.next_probe_at <= now()
                   ORDER BY r.next_probe_at
                   LIMIT $1""",
                limit,
            )
        out.scanned = len(due)

        for row in due:
            adapter = self.adapters.get(row["action_type"])
            if adapter is None:
                out.no_adapter += 1
                continue
            import json as _json

            result = await adapter.probe(row["intent_id"], _json.loads(row["params_text"]))
            if result == "APPLIED":
                await self._settle(row["intent_id"], "APPLIED", adapter.name)
                out.resolved_applied += 1
            elif result == "NOT_APPLIED":
                await self._settle(row["intent_id"], "NOT_APPLIED", adapter.name)
                out.resolved_not_applied += 1
            else:
                escalated = await self._backoff(row, adapter.name)
                out.still_unknown += 1
                if escalated:
                    out.escalated += 1
            out.detail.append({"intent_id": row["intent_id"], "result": result})
        return out

    async def _settle(self, intent_id: str, state: str, adapter_name: str) -> None:
        async with self.db._pool.acquire() as conn, conn.transaction():
            await conn.execute(
                "UPDATE action_intents SET state = $2 WHERE intent_id = $1",
                intent_id,
                state,
            )
            if state == "APPLIED":
                await conn.execute(
                    "UPDATE reservations SET state = 'POSTED' WHERE intent_id = $1",
                    intent_id,
                )
            else:
                await conn.execute(
                    "UPDATE reservations SET state = 'VOIDED' WHERE intent_id = $1",
                    intent_id,
                )
            await conn.execute(
                """UPDATE reconcile_state
                          SET last_result = $2, last_probe_at = now(),
                              adapter_name = $3, updated_at = now()
                        WHERE intent_id = $1""",
                intent_id,
                state,
                adapter_name,
            )
            await conn.execute(
                """UPDATE obligations
                          SET status = 'RESOLVED',
                              resolution = $2, resolved_by = $3, resolved_at = now()
                        WHERE intent_id = $1 AND status IN ('OPEN', 'ESCALATED')""",
                intent_id,
                f"reconciler:{adapter_name}:{state.lower()}",
                f"reconciler:{adapter_name}",
            )
        await self._emit(
            {
                "type": "reconcile.settled",
                "intent_id": intent_id,
                "state": state,
                "adapter": adapter_name,
            }
        )

    async def _backoff(self, row: Any, adapter_name: str) -> bool:
        probe_count = int(row["probe_count"]) + 1
        backoff_s = min(2**probe_count, 3600)  # exponential, capped at 1h
        escalated = False
        async with self.db._pool.acquire() as conn, conn.transaction():
            await conn.execute(
                """UPDATE reconcile_state
                          SET probe_count = $2,
                              next_probe_at = now() + make_interval(secs => $3),
                              last_probe_at = now(), last_result = 'UNKNOWN',
                              adapter_name = $4, updated_at = now()
                        WHERE intent_id = $1""",
                row["intent_id"],
                probe_count,
                backoff_s,
                adapter_name,
            )
            amount = await conn.fetchval(
                "SELECT amount FROM reservations WHERE intent_id = $1", row["intent_id"]
            )
            age_hours = (datetime.now(UTC) - row["next_probe_at"]).total_seconds() / 3600
            if (
                probe_count >= self.policy.max_probe_count
                or (amount or 0) >= self.policy.amount_threshold
                or age_hours >= self.policy.max_age_hours
            ):
                await conn.execute(
                    """UPDATE obligations SET status = 'ESCALATED'
                            WHERE intent_id = $1 AND status = 'OPEN'""",
                    row["intent_id"],
                )
                escalated = True
        if escalated:
            await self._emit(
                {
                    "type": "reconcile.escalated",
                    "intent_id": row["intent_id"],
                    "probe_count": probe_count,
                }
            )
        return escalated

    # ------------------------------------------------------------- admission
    async def register_compensation(self, action_type: str, ref: str) -> None:
        """Compensation admission: only whitelisted action types may register.
        Compensation execution itself must be registered as a NEW intent via
        the gateway (never executed inline)."""
        async with self.db._pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO compensation_registry (action_type, compensation_ref)
                   VALUES ($1, $2)
                   ON CONFLICT (action_type) DO UPDATE
                       SET compensation_ref = EXCLUDED.compensation_ref""",
                action_type,
                ref,
            )

    async def compensation_allowed(self, action_type: str) -> bool:
        async with self.db._pool.acquire() as conn:
            return (
                await conn.fetchval(
                    """SELECT enabled FROM compensation_registry
                        WHERE action_type = $1""",
                    action_type,
                )
            ) or False


def audit_event_to_json(event: dict[str, Any]) -> str:
    return json.dumps(event, sort_keys=True, separators=(",", ":"))
