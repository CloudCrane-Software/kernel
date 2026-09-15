"""kernel/db.py — asyncpg data access layer for the six-table governance ledger.

Every invariant enforced by ops/sql/0001_init.sql triggers surfaces here as
:class:`InvariantViolation` (fail closed); idempotency conflicts surface as
:class:`IdempotencyKeyConflict` (HTTP 422 at the gateway layer).
"""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import asyncpg

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "ops" / "sql" / "0001_init.sql"

_CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
_ULID_ERR = "ulid must be a 26-char crockford string"


class KernelError(Exception):
    """Base class for kernel data-layer errors."""


class IdempotencyKeyConflict(KernelError):
    """Same idempotency key replayed with different params (P-4)."""


class InvariantViolation(KernelError):
    """A database invariant (CHECK/trigger/unique) rejected the operation."""


def canonical_json(payload: dict[str, Any] | list[Any]) -> str:
    """RFC 8785-flavored canonical JSON: sorted keys, no whitespace.

    Not a full JCS implementation (no number normalization beyond json's
    default repr); sufficient for stable hashing of our own payloads.
    """
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_hex(data: str | bytes) -> str:
    if isinstance(data, str):
        data = data.encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def _as_dt(v: str | datetime) -> datetime:
    """Timestamptz params must be datetime objects for asyncpg."""
    if isinstance(v, str):
        return datetime.fromisoformat(v.replace("Z", "+00:00"))
    return v


def new_ulid() -> str:
    """26-char ULID: 48-bit ms timestamp + 80-bit randomness (Crockford base32)."""
    ms = int(time.time() * 1000) & ((1 << 48) - 1)
    rand = secrets.randbits(80)
    value = (ms << 80) | rand
    chars: list[str] = []
    for _ in range(26):
        chars.append(_CROCKFORD[value & 31])
        value >>= 5
    return "".join(reversed(chars))


@dataclass(frozen=True, slots=True)
class MandateRow:
    mandate_id: str
    human_signer: str
    signature: str
    payload_jcs: str
    mandate_sha256: str
    cap_amount: int
    ledger_id: str
    expires_at: str
    status: str


@dataclass(frozen=True, slots=True)
class GrantRow:
    grant_id: str
    mandate_id: str
    parent_grant_id: str | None
    scope: dict[str, Any]
    remaining_depth: int
    used_calls: int
    used_budget: int
    expiry: str
    status: str


@dataclass(frozen=True, slots=True)
class IntentRow:
    intent_id: str
    episode_id: str
    grant_id: str
    idempotency_key: str
    params_hash: str
    fence_epoch: int
    state: str
    attempt_count: int
    action_type: str


@dataclass(frozen=True, slots=True)
class ReservationRow:
    reservation_id: str
    intent_id: str
    mandate_id: str
    tb_transfer_id: str
    amount: int
    state: str


@dataclass(frozen=True, slots=True)
class DecisionRow:
    decision_id: str
    intent_id: str
    policy_revision: int
    registry_revision: int
    mandate_sha256: str
    grant_chain_digest: str
    decision: str
    reason_codes: list[str]


_INVARIANT_SQLSTATES = frozenset({"23505", "23514", "23503"})


def _wrap_pg_error(e: asyncpg.PostgresError) -> KernelError:
    if e.sqlstate in _INVARIANT_SQLSTATES:
        return InvariantViolation(str(e))
    return KernelError(str(e))


class Database:
    """Thin, explicit DAL. No ORM, no auto-table creation, no result caching."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    # ------------------------------------------------------------ lifecycle
    @classmethod
    async def connect(cls, dsn: str | None = None) -> Database:
        dsn = dsn or os.environ.get("KERNEL_PG_DSN")
        if not dsn:
            raise KernelError("no DSN: pass dsn or set KERNEL_PG_DSN")
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=8)
        return cls(pool)

    async def close(self) -> None:
        await self._pool.close()

    async def apply_schema(self, sql: str | None = None) -> None:
        """Apply ops/sql/0001_init.sql (idempotent by construction)."""
        sql = sql or SCHEMA_PATH.read_text(encoding="utf-8")
        async with self._pool.acquire() as conn:
            await conn.execute(sql)

    @staticmethod
    def _intent_row(r: asyncpg.Record) -> IntentRow:
        return IntentRow(
            intent_id=r["intent_id"],
            episode_id=r["episode_id"],
            grant_id=r["grant_id"],
            idempotency_key=r["idempotency_key"],
            params_hash=r["params_hash"],
            fence_epoch=r["fence_epoch"],
            state=r["state"],
            attempt_count=r["attempt_count"],
            action_type=r["action_type"],
        )

    # ------------------------------------------------------------- mandates
    async def register_mandate(
        self,
        *,
        mandate_id: str,
        human_signer: str,
        signature: str,
        payload: dict[str, Any],
        cap_amount: int,
        ledger_id: str,
        expires_at: str,
        signature_verified: bool = False,
    ) -> MandateRow:
        payload_jcs = canonical_json(payload)
        q = """
            INSERT INTO mandates
                (mandate_id, human_signer, signature, signature_verified, payload_jcs,
                 mandate_sha256, cap_amount, ledger_id, expires_at)
            VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8, $9::timestamptz)
            ON CONFLICT (mandate_id) DO NOTHING
            RETURNING mandate_id, human_signer, signature, payload_jcs::text,
                      mandate_sha256, cap_amount, ledger_id, expires_at::text, status
        """
        async with self._pool.acquire() as conn:
            try:
                row = await conn.fetchrow(
                    q,
                    mandate_id,
                    human_signer,
                    signature,
                    signature_verified,
                    payload_jcs,
                    sha256_hex(payload_jcs),
                    cap_amount,
                    ledger_id,
                    _as_dt(expires_at),
                )
                if row is None:
                    row = await conn.fetchrow(
                        """SELECT mandate_id, human_signer, signature, payload_jcs::text,
                                  mandate_sha256, cap_amount, ledger_id, expires_at::text, status
                           FROM mandates WHERE mandate_id = $1""",
                        mandate_id,
                    )
                assert row is not None
                return MandateRow(
                    mandate_id=row["mandate_id"],
                    human_signer=row["human_signer"],
                    signature=row["signature"],
                    payload_jcs=row["payload_jcs"],
                    mandate_sha256=row["mandate_sha256"],
                    cap_amount=row["cap_amount"],
                    ledger_id=row["ledger_id"],
                    expires_at=row["expires_at"],
                    status=row["status"],
                )
            except asyncpg.PostgresError as e:
                raise _wrap_pg_error(e) from e

    # --------------------------------------------------------------- grants
    async def register_grant(
        self,
        *,
        grant_id: str,
        mandate_id: str,
        parent_grant_id: str | None,
        scope: dict[str, Any],
        remaining_depth: int,
        expiry: str,
    ) -> GrantRow:
        q = """
            INSERT INTO grants
                (grant_id, mandate_id, parent_grant_id, scope, remaining_depth, expiry)
            VALUES ($1, $2, $3, $4::jsonb, $5, $6::timestamptz)
            RETURNING grant_id, mandate_id, parent_grant_id, scope::text,
                      remaining_depth, used_calls, used_budget, expiry::text, status
        """
        async with self._pool.acquire() as conn:
            try:
                r = await conn.fetchrow(
                    q,
                    grant_id,
                    mandate_id,
                    parent_grant_id,
                    canonical_json(scope),
                    remaining_depth,
                    _as_dt(expiry),
                )
                assert r is not None
                return GrantRow(
                    grant_id=r["grant_id"],
                    mandate_id=r["mandate_id"],
                    parent_grant_id=r["parent_grant_id"],
                    scope=json.loads(r["scope"]),
                    remaining_depth=r["remaining_depth"],
                    used_calls=r["used_calls"],
                    used_budget=r["used_budget"],
                    expiry=r["expiry"],
                    status=r["status"],
                )
            except asyncpg.PostgresError as e:
                raise _wrap_pg_error(e) from e

    # ------------------------------------------------- intents (idempotent)
    async def register_intent(
        self,
        *,
        intent_id: str,
        episode_id: str,
        grant_id: str,
        idempotency_key: str,
        params: dict[str, Any],
        fence_epoch: int,
        action_type: str = "generic",
    ) -> tuple[IntentRow, bool]:
        """Register an intent idempotently.

        Returns (row, created). Replays with identical params return the first
        row; replays with different params raise IdempotencyKeyConflict (P-4).
        """
        params_hash = sha256_hex(canonical_json(params))
        q = """
            INSERT INTO action_intents
                (intent_id, episode_id, grant_id, idempotency_key, params_hash,
                 fence_epoch, action_type)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (idempotency_key) DO NOTHING
            RETURNING intent_id, episode_id, grant_id, idempotency_key, params_hash,
                      fence_epoch, state, attempt_count, action_type
        """
        async with self._pool.acquire() as conn:
            try:
                r = await conn.fetchrow(
                    q,
                    intent_id,
                    episode_id,
                    grant_id,
                    idempotency_key,
                    params_hash,
                    fence_epoch,
                    action_type,
                )
                if r is not None:
                    return self._intent_row(r), True
                existing = await conn.fetchrow(
                    """SELECT intent_id, episode_id, grant_id, idempotency_key,
                              params_hash, fence_epoch, state, attempt_count, action_type
                       FROM action_intents WHERE idempotency_key = $1""",
                    idempotency_key,
                )
                assert existing is not None
                row = self._intent_row(existing)
                if row.params_hash != params_hash:
                    raise IdempotencyKeyConflict(
                        f"idempotency key {idempotency_key!r} replayed with different params"
                    )
                return row, False
            except asyncpg.PostgresError as e:
                raise _wrap_pg_error(e) from e

    async def bump_attempt(self, intent_id: str) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                "UPDATE action_intents SET attempt_count = attempt_count + 1, "
                "state = 'DISPATCHING' WHERE intent_id = $1 AND state = 'PREPARED'",
                intent_id,
            )

    async def set_intent_state(self, intent_id: str, new_state: str) -> None:
        async with self._pool.acquire() as conn:
            try:
                await conn.execute(
                    "UPDATE action_intents SET state = $2 WHERE intent_id = $1",
                    intent_id,
                    new_state,
                )
            except asyncpg.PostgresError as e:
                raise _wrap_pg_error(e) from e

    async def get_intent(self, intent_id: str) -> IntentRow | None:
        async with self._pool.acquire() as conn:
            r = await conn.fetchrow(
                """SELECT intent_id, episode_id, grant_id, idempotency_key, params_hash,
                          fence_epoch, state, attempt_count, action_type
                   FROM action_intents WHERE intent_id = $1""",
                intent_id,
            )
            return self._intent_row(r) if r else None

    # --------------------------------------------------------- reservations
    async def insert_reservation(
        self,
        *,
        reservation_id: str,
        intent_id: str,
        mandate_id: str,
        amount: int,
        tb_transfer_id: str | None = None,
    ) -> ReservationRow:
        tb_transfer_id = tb_transfer_id or new_ulid()
        q = """
            INSERT INTO reservations
                (reservation_id, intent_id, mandate_id, tb_transfer_id, amount)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING reservation_id, intent_id, mandate_id, tb_transfer_id, amount, state
        """
        async with self._pool.acquire() as conn:
            try:
                r = await conn.fetchrow(
                    q, reservation_id, intent_id, mandate_id, tb_transfer_id, amount
                )
                assert r is not None
                return ReservationRow(
                    reservation_id=r["reservation_id"],
                    intent_id=r["intent_id"],
                    mandate_id=r["mandate_id"],
                    tb_transfer_id=r["tb_transfer_id"],
                    amount=r["amount"],
                    state=r["state"],
                )
            except asyncpg.PostgresError as e:
                raise _wrap_pg_error(e) from e

    async def post_reservation(self, reservation_id: str) -> None:
        async with self._pool.acquire() as conn:
            try:
                await conn.execute(
                    "UPDATE reservations SET state = 'POSTED' WHERE reservation_id = $1",
                    reservation_id,
                )
            except asyncpg.PostgresError as e:
                raise _wrap_pg_error(e) from e

    async def void_reservation(self, reservation_id: str) -> None:
        async with self._pool.acquire() as conn:
            try:
                await conn.execute(
                    "UPDATE reservations SET state = 'VOIDED' WHERE reservation_id = $1",
                    reservation_id,
                )
            except asyncpg.PostgresError as e:
                raise _wrap_pg_error(e) from e

    # -------------------------------------------------------------- decisions
    async def append_decision(
        self,
        *,
        decision_id: str,
        intent_id: str,
        policy_revision: int,
        registry_revision: int,
        mandate_sha256: str,
        grant_chain_digest: str,
        decision: str,
        reason_codes: list[str] | None = None,
    ) -> DecisionRow:
        q = """
            INSERT INTO decisions
                (decision_id, intent_id, policy_revision, registry_revision,
                 mandate_sha256, grant_chain_digest, decision, reason_codes)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
            RETURNING decision_id, intent_id, policy_revision, registry_revision,
                      mandate_sha256, grant_chain_digest, decision, reason_codes::text
        """
        async with self._pool.acquire() as conn:
            try:
                r = await conn.fetchrow(
                    q,
                    decision_id,
                    intent_id,
                    policy_revision,
                    registry_revision,
                    mandate_sha256,
                    grant_chain_digest,
                    decision,
                    canonical_json(reason_codes or []),
                )
                assert r is not None
                return DecisionRow(
                    decision_id=r["decision_id"],
                    intent_id=r["intent_id"],
                    policy_revision=r["policy_revision"],
                    registry_revision=r["registry_revision"],
                    mandate_sha256=r["mandate_sha256"],
                    grant_chain_digest=r["grant_chain_digest"],
                    decision=r["decision"],
                    reason_codes=json.loads(r["reason_codes"]),
                )
            except asyncpg.PostgresError as e:
                raise _wrap_pg_error(e) from e

    # ----------------------------------------------------------------- leases
    async def acquire_lease(self, lease_id: str, holder: str, ttl_seconds: int) -> int:
        """Atomically acquire/renew; returns the new epoch (single statement)."""
        q = """
            INSERT INTO leases (lease_id, holder, epoch, expires_at)
            VALUES ($1, $2, 1, now() + make_interval(secs => $3))
            ON CONFLICT (lease_id) DO UPDATE
                SET epoch = leases.epoch + 1,
                    holder = EXCLUDED.holder,
                    expires_at = EXCLUDED.expires_at
            RETURNING epoch
        """
        async with self._pool.acquire() as conn:
            r = await conn.fetchval(q, lease_id, holder, ttl_seconds)
            return int(r)

    async def lease_renew_if_current(
        self, lease_id: str, holder: str, seen_epoch: int, ttl_seconds: int
    ) -> bool:
        """F-2 fencing: a stale epoch holder renews 0 rows."""
        q = """
            UPDATE leases
               SET expires_at = now() + make_interval(secs => $4)
             WHERE lease_id = $1 AND holder = $2 AND epoch = $3
            RETURNING 1
        """
        async with self._pool.acquire() as conn:
            r = await conn.fetchval(q, lease_id, holder, seen_epoch, ttl_seconds)
            return r is not None

    # ---------------------------------------------------------------- episodes
    async def episode_transition(
        self, episode_id: str, new_state: str, terminal_branch: str | None = None
    ) -> None:
        async with self._pool.acquire() as conn:
            try:
                await conn.execute(
                    "UPDATE episodes SET state = $2, terminal_branch = $3 WHERE episode_id = $1",
                    episode_id,
                    new_state,
                    terminal_branch,
                )
            except asyncpg.PostgresError as e:
                raise _wrap_pg_error(e) from e
