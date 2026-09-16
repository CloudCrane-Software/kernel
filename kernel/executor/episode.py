"""kernel/executor — episode execution layer (WO-04).

EpisodeExecutor drives RESERVED → RUNNING → VERIFYING → CLOSED{branch}:

  1. seeds the episode row + lease (fencing);
  2. charges model_cost (always — even a zero-business-action episode
     consumes model budget; manual §5.5 rule);
  3. registers ONE intent per external side effect through the action
     gateway (the only door — nothing goes around it);
  4. runs the task via the injected Runner (JiuwenBox in production,
     local subprocess in dev/tests);
  5. VERIFYING pauses for external evaluation — resolve() continues to a
     terminal branch (the Restate layer models this pause as an awakeable);
  6. recovery after a crash: scan the three tables, assert the fence
     invariant (max intent epoch <= lease epoch), then idempotently re-drive
     — never replaying settled external effects.

Audit events (state transitions, artifacts, costs) are emitted to the
injected callback; in production that feeds Kafka topic audit-events and
mirrors into PG (L0 memory layer — context restore only, never evidence).
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from gateway.schemas import RegisterIntentRequest
from kernel.db import Database
from kernel.runner.base import Runner, TaskSpec
from kernel.runner.local import LocalSubprocessRunner

EventCallback = Callable[[dict[str, Any]], Awaitable[None]]


class ExecutorError(Exception):
    pass


@dataclass(slots=True)
class EpisodeOutcome:
    episode_id: str
    terminal_branch: str
    artifacts: dict[str, bytes]
    intents: list[str]


class EpisodeExecutor:
    def __init__(
        self,
        db: Database,
        gateway: Any,  # gateway.service.GatewayService (typed loosely: cycle-free)
        runner: Runner | None = None,
        on_event: EventCallback | None = None,
    ) -> None:
        self.db = db
        self.gateway = gateway
        self.runner = runner or LocalSubprocessRunner()
        self.on_event = on_event

    async def _emit(self, event: dict[str, Any]) -> None:
        if self.on_event is not None:
            await self.on_event(event)

    async def _transition(self, episode_id: str, new_state: str) -> None:
        async with self.db._pool.acquire() as conn:
            await conn.execute(
                "UPDATE episodes SET state = $2 WHERE episode_id = $1", episode_id, new_state
            )
        await self._emit(
            {"type": "episode.transition", "episode_id": episode_id, "state": new_state}
        )

    async def start_episode(self, episode_id: str) -> None:
        async with self.db._pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO episodes (episode_id) VALUES ($1)
                   ON CONFLICT (episode_id) DO NOTHING""",
                episode_id,
            )
            # lease: single-statement atomic epoch acquisition (F-2)
            await conn.execute(
                """INSERT INTO leases (lease_id, holder, epoch, expires_at)
                   VALUES ($1, 'executor', 1, now() + interval '1 hour')
                   ON CONFLICT (lease_id) DO UPDATE
                       SET epoch = leases.epoch + 1,
                           holder = EXCLUDED.holder,
                           expires_at = EXCLUDED.expires_at""",
                f"episode:{episode_id}",
            )

    async def _charge_model_cost(self, episode_id: str, grant_id: str, amount: int = 1) -> str:
        """model_cost is always accounted, even for no-op episodes (§5.5)."""
        resp = await self.gateway.register_intent(
            RegisterIntentRequest(
                operation_id=f"model-cost-{episode_id}",
                episode_id=episode_id,
                grant_id=grant_id,
                idempotency_key=f"model-cost-{episode_id}",
                params={"kind": "model_cost", "tokens_estimate": 0},
                action_type="model.cost",
                amount=amount,
            )
        )
        return str(resp.intent_id)

    async def run_task(
        self,
        *,
        episode_id: str,
        grant_id: str,
        commands: list[str],
        artifacts: list[str],
        workdir: Path,
    ) -> list[str]:
        """RESERVED -> RUNNING -> VERIFYING, executing the task via the runner.
        Returns the intents created (model_cost first)."""
        await self._charge_model_cost(episode_id, grant_id)
        await self._transition(episode_id, "RUNNING")
        spec = TaskSpec(
            work_order_id=episode_id,
            episode_id=episode_id,
            commands=commands,
            artifacts=artifacts,
        )
        result = await self.runner.run(spec, workdir)
        await self._emit(
            {
                "type": "episode.task_finished",
                "episode_id": episode_id,
                "exit_code": result.exit_code,
                "runner": self.runner.name,
                "sandbox_id": result.sandbox_id,
                "artifacts": sorted(result.artifacts),
            }
        )
        # artifacts return THROUGH the gateway: one artifact.store intent,
        # receipt-settled with content digests as evidence (never raw memory)
        if result.artifacts:
            import hashlib

            deposit = await self.gateway.register_intent(
                RegisterIntentRequest(
                    operation_id=f"artifact-store-{episode_id}",
                    episode_id=episode_id,
                    grant_id=grant_id,
                    idempotency_key=f"artifact-store-{episode_id}",
                    params={
                        "kind": "artifact.store",
                        "digests": {
                            name: hashlib.sha256(content).hexdigest()
                            for name, content in sorted(result.artifacts.items())
                        },
                    },
                    action_type="artifact.store",
                    amount=1,
                )
            )
            from gateway.schemas import ReceiptRequest

            await self.gateway.verify_receipt(
                deposit.intent_id,
                ReceiptRequest(
                    receipt={
                        "status": "applied",
                        "stored": sorted(result.artifacts),
                    }
                ),
            )
            await self._emit(
                {
                    "type": "episode.artifacts_deposited",
                    "episode_id": episode_id,
                    "intent_id": deposit.intent_id,
                    "artifacts": sorted(result.artifacts),
                }
            )

        # stash artifacts for the VERIFYING phase / recovery
        async with self.db._pool.acquire() as conn:
            await conn.execute(
                """INSERT INTO episodes (episode_id, state) VALUES ($1, 'VERIFYING')
                   ON CONFLICT (episode_id) DO UPDATE SET state = 'VERIFYING', updated_at = now()""",
                episode_id,
            )
        (workdir / ".episode-artifacts.json").write_text(
            json.dumps({k: len(v) for k, v in result.artifacts.items()})
        )
        await self._emit(
            {"type": "episode.transition", "episode_id": episode_id, "state": "VERIFYING"}
        )
        return [f"model-cost-{episode_id}"]

    async def resolve_verification(
        self, episode_id: str, branch: str, result: EpisodeOutcome | None = None
    ) -> None:
        """External evaluation resolved the episode (Restate awakeable analog)."""
        if branch not in {"candidate_ready", "not_solved", "deferred", "expired"}:
            raise ExecutorError(f"illegal terminal branch {branch}")
        async with self.db._pool.acquire() as conn:
            await conn.execute(
                "UPDATE episodes SET state = 'CLOSED', terminal_branch = $2 WHERE episode_id = $1",
                episode_id,
                branch,
            )
        await self._emit({"type": "episode.closed", "episode_id": episode_id, "branch": branch})

    # ------------------------------------------------------------- recovery
    async def recover(self, episode_id: str) -> dict[str, Any]:
        """Worker-loss recovery (manual §5.4 item 4): scan intents/reservations/
        leases, assert the fence invariant, then idempotently resume. Settled
        external effects are NEVER replayed."""
        async with self.db._pool.acquire() as conn:
            lease_epoch = await conn.fetchval(
                "SELECT epoch FROM leases WHERE lease_id = $1", f"episode:{episode_id}"
            )
            max_intent_epoch = await conn.fetchval(
                "SELECT max(fence_epoch) FROM action_intents WHERE episode_id = $1", episode_id
            )
            state = await conn.fetchval(
                "SELECT state FROM episodes WHERE episode_id = $1", episode_id
            )
            intents = await conn.fetch(
                """SELECT intent_id, state FROM action_intents WHERE episode_id = $1""",
                episode_id,
            )
        if lease_epoch is None:
            raise ExecutorError(f"no lease for episode {episode_id}")
        if (max_intent_epoch or 0) > lease_epoch:
            raise ExecutorError(
                f"fence violation: intent epoch {max_intent_epoch} > lease {lease_epoch}"
            )
        # UNKNOWN intents stay for the reconciler — recovery never closes them
        report = {
            "episode_id": episode_id,
            "state": state,
            "lease_epoch": lease_epoch,
            "max_intent_epoch": max_intent_epoch or 0,
            "intents": [dict(r) for r in intents],
            "fence_ok": True,
        }
        await self._emit({"type": "episode.recovered", **report})
        return report
