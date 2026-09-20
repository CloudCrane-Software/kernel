"""gateway/service.py — the gateway operations (WO-03 + WO-0004).

Cross-cutting rules (manual §5.3):
- every request appends a decisions row (append-only ledger);
- grant/registry state is re-read before every decision — nothing cached;
- PG or OPA unavailable -> DEPENDENCY_UNAVAILABLE (fail closed);
- UNKNOWN is never a terminal state here: no endpoint closes it.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from gateway.classifiers import ClassifierRegistry
from gateway.errors import ErrorCode, GatewayError
from gateway.schemas import (
    CloseEpisodeRequest,
    CloseEpisodeResponse,
    CreateWorkorderRequest,
    CreateWorkorderResponse,
    DecisionInfo,
    ReceiptRequest,
    ReceiptResponse,
    ReconcileRequest,
    ReconcileResponse,
    RegisterIntentRequest,
    RegisterIntentResponse,
    TransitionEpisodeRequest,
    TransitionEpisodeResponse,
)
from kernel.db import (
    Database,
    IdempotencyKeyConflict,
    InvariantViolation,
    canonical_json,
    new_ulid,
    sha256_hex,
)
from kernel.executor.episode import EpisodeExecutor
from kernel.policy import PolicyClient, PolicyUnavailable

_MAX_CHAIN = 10
_GRANT_INACTIVE_REASONS = {"grant.not_active", "grant.expired"}
_BUDGET_REASONS = {"grant.budget_limit_exceeded"}

# WO-0004: forward edges only — exactly the legal set of trg_episodes_one_way
# (ops/sql/0001_init.sql), which stays the final authority. Same-state
# requests are handled as idempotent replays (no-op), matching the trigger.
_EPISODE_FORWARD: set[tuple[str, str]] = {
    ("RESERVED", "RUNNING"),
    ("RUNNING", "VERIFYING"),
    ("RUNNING", "CLOSED"),
    ("VERIFYING", "CLOSED"),
}


class IllegalEpisodeTransition(Exception):
    """WO-0004: requested episode transition is outside the one-way map.

    Maps to HTTP 409 at the routing layer. Deliberately NOT a member of
    gateway.errors.ErrorCode: that enum is pinned closed by the smoke eval
    (tests/test_smoke_eval.py) and guard.yml forbids mixing eval assets with
    product code in one PR."""


def _now_rfc3339() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


class GatewayService:
    def __init__(
        self,
        db: Database,
        policy: PolicyClient,
        classifiers: ClassifierRegistry | None = None,
        ledger: Any | None = None,
        admin_token: str | None = None,
    ) -> None:
        self.db = db
        self.policy = policy
        self.classifiers = classifiers or ClassifierRegistry()
        # WO-0004 privileged surface: bearer token for the admin/transition
        # endpoints. None = fail closed (every admin request is 401).
        # Compared constant-time at the routing layer.
        self.admin_token = admin_token
        # WO-0004: episode seeding goes through the executor's write path —
        # INSERT + lease fencing, identical to in-process execution; there is
        # deliberately no second way to create episodes.
        self._executor = EpisodeExecutor(db=db, gateway=self)
        # optional budget ledger (WO-05): when present, reservations carry a
        # real engine transfer id instead of the ULID placeholder
        self.ledger = ledger
        # external-state probe adapters, registered by the reconciler (WO-06)
        self.probe_adapters: dict[str, Any] = {}

    # ------------------------------------------------------ POST /v1/intents
    async def register_intent(self, req: RegisterIntentRequest) -> RegisterIntentResponse:
        async with self.db._pool.acquire() as conn:
            try:
                async with conn.transaction():
                    result = await self._register_intent_tx(conn, req)
            except IdempotencyKeyConflict as e:
                raise GatewayError(ErrorCode.IDEMPOTENCY_KEY_CONFLICT, str(e)) from e
            except InvariantViolation as e:
                raise GatewayError(ErrorCode.VALIDATION_ERROR, str(e)) from e
        if isinstance(result, GatewayError):
            # audit rows were committed; surface the failure now
            raise result
        return result

    async def _register_intent_tx(
        self, conn: Any, req: RegisterIntentRequest
    ) -> RegisterIntentResponse | GatewayError:
        # 1. lease epoch snapshot (auto-create on first use)
        lease_id = f"episode:{req.episode_id}"
        epoch = await conn.fetchval("SELECT epoch FROM leases WHERE lease_id = $1", lease_id)
        if epoch is None:
            epoch = await conn.fetchval(
                """INSERT INTO leases (lease_id, holder, epoch, expires_at)
                   VALUES ($1, 'gateway', 1, now() + interval '1 hour')
                   ON CONFLICT (lease_id) DO UPDATE
                       SET epoch = leases.epoch + 1 RETURNING epoch""",
                lease_id,
            )
        epoch = int(epoch)
        if req.expected_epoch is not None and req.expected_epoch != epoch:
            raise GatewayError(
                ErrorCode.LEASE_FENCED,
                f"lease epoch moved to {epoch}, caller saw {req.expected_epoch}",
            )

        # 2. fresh grant + chain + mandate reads (no caching)
        grant = await conn.fetchrow(
            """SELECT grant_id, mandate_id, parent_grant_id, scope::text, status,
                      expiry::text, used_calls, used_budget, remaining_depth
               FROM grants WHERE grant_id = $1""",
            req.grant_id,
        )
        if grant is None:
            raise GatewayError(ErrorCode.NOT_FOUND, f"grant {req.grant_id} not found")
        chain: list[dict[str, Any]] = []
        parent_id: str | None = grant["parent_grant_id"]
        hops = 0
        while parent_id is not None and hops < _MAX_CHAIN:
            parent = await conn.fetchrow(
                """SELECT grant_id, parent_grant_id, scope::text, status,
                          expiry::text, remaining_depth
                   FROM grants WHERE grant_id = $1""",
                parent_id,
            )
            if parent is None:
                break
            chain.append(
                {
                    "status": parent["status"],
                    "expiry": parent["expiry"],
                    "remaining_depth": parent["remaining_depth"],
                    "scope": json.loads(parent["scope"]),
                }
            )
            parent_id = parent["parent_grant_id"]
            hops += 1

        mandate = await conn.fetchrow(
            """SELECT mandate_id, payload_jcs::text, mandate_sha256, signature_verified,
                      status, expires_at::text, human_signer, cap_amount
               FROM mandates WHERE mandate_id = $1""",
            grant["mandate_id"],
        )
        if mandate is None:
            raise GatewayError(ErrorCode.NOT_FOUND, "mandate for grant not found")

        scope = json.loads(grant["scope"])
        decision_id = f"dec-{new_ulid()}"
        policy_input: dict[str, Any] = {
            "decision_id": decision_id,
            "now": _now_rfc3339(),
            "mandate": {
                "signer_kind": "human",
                "signature_verified": bool(mandate["signature_verified"]),
                "digest": sha256_hex(canonical_json(json.loads(mandate["payload_jcs"]))),
                "registered_digest": mandate["mandate_sha256"],
                "status": mandate["status"],
                "expires_at": mandate["expires_at"],
            },
            "grant": {
                "status": grant["status"],
                "expiry": grant["expiry"],
                "used_calls": grant["used_calls"],
                "call_limit": int(scope.get("limits", {}).get("call_limit", 2**62)),
                "used_budget": grant["used_budget"],
                "budget_limit": int(scope.get("limits", {}).get("budget_limit", 2**62)),
                "issuer": f"mandate:{grant['mandate_id']}",
                "subject": f"episode:{req.episode_id}",
                "remaining_depth": grant["remaining_depth"],
                "scope": scope,
            },
            "grant_chain": chain,
            "registry": {"fresh": True, "reachable": True, "revision": 1},
            "evidence": {"required": [], "provided": []},
        }

        # params_hash computed once, reused for insert and conflict detection
        params_hash = sha256_hex(canonical_json(req.params))

        # 3. OPA decision (inside the transaction, per protocol)
        try:
            decision = await self.policy.decide(policy_input)
        except PolicyUnavailable as e:
            raise GatewayError(ErrorCode.DEPENDENCY_UNAVAILABLE, f"opa: {e}") from e

        if decision.outcome != "ALLOW":
            # audit the rejection: the intent is registered PREPARED (never
            # dispatched) so the append-only decision row has a referent
            inserted = await conn.execute(
                """INSERT INTO action_intents
                       (intent_id, episode_id, grant_id, idempotency_key, params_hash,
                        fence_epoch, action_type, params)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
                   ON CONFLICT (idempotency_key) DO NOTHING""",
                req.operation_id,
                req.episode_id,
                req.grant_id,
                req.idempotency_key,
                params_hash,
                epoch,
                req.action_type,
                canonical_json(req.params),
            )
            if not inserted.endswith(" 1"):
                # same key replayed under a different operation_id
                raise IdempotencyKeyConflict(
                    f"idempotency key {req.idempotency_key!r} already registered"
                )
            await conn.execute(
                """INSERT INTO decisions (decision_id, intent_id, policy_revision,
                       registry_revision, mandate_sha256, grant_chain_digest,
                       decision, reason_codes)
                   VALUES ($1, $2, 1, $3, $4, $5, $6, $7::jsonb)""",
                decision_id,
                req.operation_id,
                decision.registry_revision,
                mandate["mandate_sha256"],
                sha256_hex(canonical_json([g["grant_id"] for g in chain] + [req.grant_id])),
                decision.outcome,
                json.dumps(decision.reasons),
            )
            reasons = set(decision.reasons)
            if _GRANT_INACTIVE_REASONS & reasons:
                return GatewayError(
                    ErrorCode.GRANT_INACTIVE,
                    f"decision {decision.outcome}",
                    decision=decision.outcome,
                    reasons=decision.reasons,
                )
            if _BUDGET_REASONS & reasons:
                return GatewayError(
                    ErrorCode.BUDGET_EXHAUSTED,
                    f"decision {decision.outcome}",
                    decision=decision.outcome,
                    reasons=decision.reasons,
                )
            return GatewayError(
                ErrorCode.DECISION_NOT_ALLOW,
                f"decision {decision.outcome}",
                decision=decision.outcome,
                reasons=decision.reasons,
            )

        # 4. idempotent intent insert (P-4)
        row = await conn.fetchrow(
            """INSERT INTO action_intents
                   (intent_id, episode_id, grant_id, idempotency_key, params_hash,
                    fence_epoch, action_type, params)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
               ON CONFLICT (idempotency_key) DO NOTHING
               RETURNING intent_id, state, fence_epoch""",
            req.operation_id,
            req.episode_id,
            req.grant_id,
            req.idempotency_key,
            params_hash,
            epoch,
            req.action_type,
            canonical_json(req.params),
        )
        if row is None:
            # replay — return the first registration, do not reserve twice
            existing = await conn.fetchrow(
                """SELECT i.intent_id, i.state, i.fence_epoch, i.params_hash,
                          r.reservation_id, r.tb_transfer_id, d.decision_id, d.decision,
                          d.reason_codes::text, d.registry_revision
                   FROM action_intents i
                   LEFT JOIN reservations r ON r.intent_id = i.intent_id
                   LEFT JOIN LATERAL (
                       SELECT decision_id, decision, reason_codes, registry_revision
                       FROM decisions WHERE intent_id = i.intent_id
                       ORDER BY created_at DESC LIMIT 1
                   ) d ON true
                   WHERE i.idempotency_key = $1""",
                req.idempotency_key,
            )
            if existing is None:
                raise GatewayError(ErrorCode.VALIDATION_ERROR, "idempotency anomaly")
            if existing["params_hash"] != params_hash:
                raise IdempotencyKeyConflict(
                    f"idempotency key {req.idempotency_key!r} replayed with different params"
                )
            return RegisterIntentResponse(
                intent_id=existing["intent_id"],
                created=False,
                state=existing["state"],
                fence_epoch=existing["fence_epoch"],
                reservation_id=existing["reservation_id"] or "",
                tb_transfer_id=existing["tb_transfer_id"] or "",
                decision=DecisionInfo(
                    decision_id=existing["decision_id"] or decision_id,
                    outcome=existing["decision"] or decision.outcome,
                    reasons=json.loads(existing["reason_codes"] or "[]"),
                    registry_revision=existing["registry_revision"] or 0,
                ),
            )

        # 5. budget reservation (engine-grade check in SQL, atomic with insert)
        budget_limit = int(scope.get("limits", {}).get("budget_limit", 2**62))
        updated = await conn.execute(
            """UPDATE grants SET used_budget = used_budget + $2
               WHERE grant_id = $1 AND used_budget + $2 <= $3""",
            req.grant_id,
            req.amount,
            budget_limit,
        )
        if not updated.endswith(" 1"):
            await conn.execute(
                """INSERT INTO decisions (decision_id, intent_id, policy_revision,
                       registry_revision, mandate_sha256, grant_chain_digest,
                       decision, reason_codes)
                   VALUES ($1, $2, 1, $3, $4, $5, 'DENY', $6::jsonb)""",
                decision_id,
                req.operation_id,
                decision.registry_revision,
                mandate["mandate_sha256"],
                sha256_hex(canonical_json([g["grant_id"] for g in chain] + [req.grant_id])),
                json.dumps(["grant.budget_limit_exceeded"]),
            )
            return GatewayError(ErrorCode.BUDGET_EXHAUSTED, "grant budget window exhausted")

        # 6. reservation row: engine transfer id when the ledger is wired,
        # ULID placeholder before TigerBeetle lands (WO-05 staged adoption)
        reservation_id = f"res-{new_ulid()}"
        if self.ledger is not None:
            await self.ledger.ensure_budget_account(grant["mandate_id"], mandate["cap_amount"])
            tb_transfer_id = await self.ledger.create_pending_transfer(
                grant["mandate_id"], req.amount, req.idempotency_key
            )
        else:
            tb_transfer_id = new_ulid()
        await conn.execute(
            """INSERT INTO reservations
                   (reservation_id, intent_id, mandate_id, tb_transfer_id, amount)
               VALUES ($1, $2, $3, $4, $5)""",
            reservation_id,
            req.operation_id,
            grant["mandate_id"],
            tb_transfer_id,
            req.amount,
        )

        # 7. append ALLOW decision
        await conn.execute(
            """INSERT INTO decisions (decision_id, intent_id, policy_revision,
                   registry_revision, mandate_sha256, grant_chain_digest,
                   decision, reason_codes)
               VALUES ($1, $2, 1, $3, $4, $5, 'ALLOW', '[]'::jsonb)""",
            decision_id,
            req.operation_id,
            decision.registry_revision,
            mandate["mandate_sha256"],
            sha256_hex(canonical_json([g["grant_id"] for g in chain] + [req.grant_id])),
        )

        return RegisterIntentResponse(
            intent_id=req.operation_id,
            created=True,
            state=row["state"],
            fence_epoch=epoch,
            reservation_id=reservation_id,
            tb_transfer_id=tb_transfer_id,
            decision=DecisionInfo(
                decision_id=decision_id,
                outcome=decision.outcome,
                reasons=[],
                registry_revision=decision.registry_revision,
            ),
        )

    # ------------------------------------------- POST /v1/intents/{id}/receipt
    async def verify_receipt(self, intent_id: str, req: ReceiptRequest) -> ReceiptResponse:
        async with self.db._pool.acquire() as conn:
            try:
                async with conn.transaction():
                    intent = await conn.fetchrow(
                        """SELECT intent_id, state, action_type FROM action_intents
                           WHERE intent_id = $1""",
                        intent_id,
                    )
                    if intent is None:
                        raise GatewayError(ErrorCode.NOT_FOUND, f"intent {intent_id} not found")

                    classification = self.classifiers.classify(intent["action_type"], req.receipt)

                    if classification == "UNKNOWN":
                        # evidence insufficient -> UNKNOWN (never guess) + obligation
                        if intent["state"] == "PREPARED":
                            await conn.execute(
                                "UPDATE action_intents SET state = 'DISPATCHING' "
                                "WHERE intent_id = $1 AND state = 'PREPARED'",
                                intent_id,
                            )
                        await conn.execute(
                            "UPDATE action_intents SET state = 'UNKNOWN' "
                            "WHERE intent_id = $1 AND state = 'DISPATCHING'",
                            intent_id,
                        )
                        obligation_id = f"obl-{new_ulid()}"
                        await conn.execute(
                            "INSERT INTO obligations (obligation_id, intent_id, kind) "
                            "VALUES ($1, $2, 'reconcile')",
                            obligation_id,
                            intent_id,
                        )
                        # enroll the intent in the reconciliation loop (same tx):
                        # without this row the poller would never pick it up
                        await conn.execute(
                            "INSERT INTO reconcile_state (intent_id) VALUES ($1) "
                            "ON CONFLICT (intent_id) DO NOTHING",
                            intent_id,
                        )
                        return ReceiptResponse(
                            intent_id=intent_id,
                            classification="UNKNOWN",
                            obligation_id=obligation_id,
                        )

                    if classification == "APPLIED":
                        await self._settle(conn, intent_id, "APPLIED")
                    else:
                        await self._settle(conn, intent_id, "NOT_APPLIED")
                    return ReceiptResponse(intent_id=intent_id, classification=classification)
            except InvariantViolation as e:
                raise GatewayError(ErrorCode.VALIDATION_ERROR, str(e)) from e

    async def _settle(self, conn: Any, intent_id: str, state: str) -> None:
        # settle the engine transfer alongside the DB reservation (WO-05)
        if self.ledger is not None:
            tb_id = await conn.fetchval(
                "SELECT tb_transfer_id FROM reservations WHERE intent_id = $1", intent_id
            )
            if tb_id:
                if state == "APPLIED":
                    await self.ledger.post_transfer(tb_id)
                else:
                    await self.ledger.void_transfer(tb_id)
        if (
            await conn.fetchval("SELECT state FROM action_intents WHERE intent_id = $1", intent_id)
            == "PREPARED"
        ):
            await conn.execute(
                "UPDATE action_intents SET state = 'DISPATCHING' "
                "WHERE intent_id = $1 AND state = 'PREPARED'",
                intent_id,
            )
        await conn.execute(
            "UPDATE action_intents SET state = $2 WHERE intent_id = $1",
            intent_id,
            state,
        )
        if state == "APPLIED":
            await conn.execute(
                "UPDATE reservations SET state = 'POSTED' WHERE intent_id = $1", intent_id
            )
        else:
            await conn.execute(
                "UPDATE reservations SET state = 'VOIDED' WHERE intent_id = $1", intent_id
            )

    # --------------------------------------------------- POST /v1/reconcile
    async def reconcile(self, req: ReconcileRequest) -> ReconcileResponse:
        """Scheduling skeleton: scan UNKNOWN intents; adapters are plugins
        (WO-06 registers the real external probes). No adapter -> deferred."""
        async with self.db._pool.acquire() as conn:
            unknowns = await conn.fetch(
                "SELECT intent_id, action_type FROM action_intents "
                "WHERE state = 'UNKNOWN' LIMIT $1",
                req.limit,
            )
        resolved = still_unknown = deferred = 0
        for row in unknowns:
            adapter = self.probe_adapters.get(row["action_type"])
            if adapter is None:
                deferred += 1
                continue
            # adapter contract (WO-06): await adapter.probe(intent_id) -> str
            classification = await adapter.probe(row["intent_id"])
            if classification == "APPLIED":
                async with self.db._pool.acquire() as conn, conn.transaction():
                    await self._settle(conn, row["intent_id"], "APPLIED")
                resolved += 1
            elif classification == "NOT_APPLIED":
                async with self.db._pool.acquire() as conn, conn.transaction():
                    await self._settle(conn, row["intent_id"], "NOT_APPLIED")
                resolved += 1
            else:
                still_unknown += 1
        return ReconcileResponse(
            scanned=len(unknowns),
            resolved=resolved,
            still_unknown=still_unknown,
            deferred=deferred,
        )

    # ------------------------------------------ POST /v1/episodes/{id}/close
    async def close_episode(
        self, episode_id: str, req: CloseEpisodeRequest
    ) -> CloseEpisodeResponse:
        async with self.db._pool.acquire() as conn:
            episode = await conn.fetchrow(
                "SELECT episode_id, state, terminal_branch FROM episodes WHERE episode_id = $1",
                episode_id,
            )
            if episode is None:
                raise GatewayError(ErrorCode.NOT_FOUND, f"episode {episode_id} not found")

            # WO-107 follow-up (K3 cold-review #3): CLOSED is terminal. Replaying
            # the same branch is an idempotent no-op -- no UPDATE at all, so
            # trg_episodes_one_way never bumps updated_at (J7 finding). A
            # different branch is a conflict, never a rewrite of a terminal
            # episode.
            if episode["state"] == "CLOSED":
                if episode["terminal_branch"] == req.terminal_branch:
                    return CloseEpisodeResponse(
                        episode_id=episode_id,
                        state="CLOSED",
                        terminal_branch=episode["terminal_branch"],
                    )
                raise IllegalEpisodeTransition(
                    f"episode {episode_id}: already CLOSED (terminal_branch="
                    f"{episode['terminal_branch']}); refusing rewrite to "
                    f"{req.terminal_branch}"
                )

            unknown = await conn.fetchval(
                "SELECT count(*) FROM action_intents WHERE episode_id = $1 AND state = 'UNKNOWN'",
                episode_id,
            )
            if unknown:
                raise GatewayError(
                    ErrorCode.UNRESOLVED_UNKNOWN_EXISTS,
                    f"{unknown} UNKNOWN intents remain (reconcile first)",
                )
            open_obl = await conn.fetchval(
                """SELECT count(*) FROM obligations o
                   JOIN action_intents i ON i.intent_id = o.intent_id
                   WHERE i.episode_id = $1 AND o.status IN ('OPEN', 'ESCALATED')""",
                episode_id,
            )
            if open_obl:
                raise GatewayError(
                    ErrorCode.OPEN_OBLIGATIONS_EXIST,
                    f"{open_obl} unresolved obligations remain",
                )
            pending = await conn.fetchval(
                """SELECT count(*) FROM reservations r
                   JOIN action_intents i ON i.intent_id = r.intent_id
                   WHERE i.episode_id = $1 AND r.state = 'PENDING'""",
                episode_id,
            )
            if pending:
                raise GatewayError(
                    ErrorCode.UNRESOLVED_UNKNOWN_EXISTS,
                    f"{pending} reservations still PENDING",
                )

            try:
                async with conn.transaction():
                    await conn.execute(
                        "UPDATE episodes SET state = 'CLOSED', terminal_branch = $2 "
                        "WHERE episode_id = $1",
                        episode_id,
                        req.terminal_branch,
                    )
            except Exception as e:
                raise GatewayError(
                    ErrorCode.VALIDATION_ERROR, f"illegal episode transition: {e}"
                ) from e
        return CloseEpisodeResponse(
            episode_id=episode_id, state="CLOSED", terminal_branch=req.terminal_branch
        )

    # -------------------------------------------------- POST /v1/workorders
    async def create_workorder(self, req: CreateWorkorderRequest) -> CreateWorkorderResponse:
        """Seed a RESERVED episode for a work order (WO-0004).

        The write path is EpisodeExecutor.start_episode — the same
        INSERT + lease fencing the executor uses in-process. Idempotent:
        an existing id returns its CURRENT state with created=False
        (201 only on first creation, mirroring POST /v1/intents)."""
        async with self.db._pool.acquire() as conn:
            existing = await conn.fetchval(
                "SELECT state FROM episodes WHERE episode_id = $1", req.workorder_id
            )
        if existing is not None:
            return CreateWorkorderResponse(
                episode_id=req.workorder_id,
                workorder_id=req.workorder_id,
                state=existing,
                created=False,
                metadata=req.metadata,
            )
        await self._executor.start_episode(req.workorder_id)
        return CreateWorkorderResponse(
            episode_id=req.workorder_id,
            workorder_id=req.workorder_id,
            state="RESERVED",
            created=True,
            metadata=req.metadata,
        )

    # ------------------------------------------- POST /v1/episodes/{id}/transition
    async def transition_episode(
        self, episode_id: str, req: TransitionEpisodeRequest
    ) -> TransitionEpisodeResponse:
        """One-way episode state transition (WO-0004).

        The pre-check mirrors trg_episodes_one_way so the contract is a 409
        instead of a raw PG exception; the trigger remains the final
        authority (the write goes through kernel.db.episode_transition).
        CLOSED requires (and pins) its terminal_branch."""
        async with self.db._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT state, terminal_branch FROM episodes WHERE episode_id = $1", episode_id
            )
        if row is None:
            raise GatewayError(ErrorCode.NOT_FOUND, f"episode {episode_id} not found")
        current, current_branch = row["state"], row["terminal_branch"]

        if req.target_state == "CLOSED" and req.terminal_branch is None:
            raise GatewayError(
                ErrorCode.VALIDATION_ERROR,
                "terminal_branch is required when target_state is CLOSED",
            )
        if req.target_state != "CLOSED" and req.terminal_branch is not None:
            raise GatewayError(
                ErrorCode.VALIDATION_ERROR,
                "terminal_branch is only allowed when target_state is CLOSED",
            )

        if req.target_state == current:
            # idempotent replay (trigger allows same-state); a CLOSED episode
            # may only replay with its own branch — rewriting history is 409
            if current == "CLOSED" and req.terminal_branch != current_branch:
                raise IllegalEpisodeTransition(
                    f"episode {episode_id} already CLOSED with terminal_branch={current_branch!r}"
                )
            return TransitionEpisodeResponse(
                episode_id=episode_id,
                previous_state=current,
                state=current,
                terminal_branch=current_branch,
            )

        if (current, req.target_state) not in _EPISODE_FORWARD:
            raise IllegalEpisodeTransition(
                f"episode {episode_id}: illegal one-way transition {current} -> {req.target_state}"
            )
        try:
            await self.db.episode_transition(episode_id, req.target_state, req.terminal_branch)
        except InvariantViolation as e:
            raise IllegalEpisodeTransition(str(e)) from e
        return TransitionEpisodeResponse(
            episode_id=episode_id,
            previous_state=current,
            state=req.target_state,
            terminal_branch=req.terminal_branch,
        )
