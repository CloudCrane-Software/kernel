"""Restate adapter (WO-04 item 2) — thin wiring, import-guarded.

The framework-agnostic core lives in kernel/executor/episode.py. This module
exposes the same operations as Restate Virtual Objects (WorkOrderObject /
EpisodeObject) when restate-sdk is installed: episode state transitions are
durable in PG regardless; Restate adds invocation journaling, awakeables
(waiting for human eval approval resolves via HTTP), and retries.

Activate with: RESTATE_URL=http://restate:8080 uvicorn kernel.executor.restate_app:app
"""

from __future__ import annotations

from typing import Any

from gateway.classifiers import default_registry
from gateway.service import GatewayService
from kernel.db import Database
from kernel.executor.episode import EpisodeExecutor
from kernel.policy import PolicyClient
from kernel.runner.scheduler import production_registry


def build_service(db: Database, policy: PolicyClient) -> GatewayService:
    # WO-108 F1: GatewayService wires the sandbox-tier registry itself
    # (kernel/runner/scheduler.py production_registry) — the gateway-side
    # executor never silently downgrades an isolated-tier task.
    return GatewayService(db, policy, default_registry())


try:  # pragma: no cover - activated only with restate-sdk installed
    from restate.app import App
    from restate.service import Service

    from kernel.runner.jiuwenbox import JiuwenBoxRunner

    episode_object = Service("EpisodeObject")

    @episode_object.handler()
    async def start(ctx: Any, episode_id: str, grant_id: str) -> dict[str, Any]:
        db = await Database.connect()
        policy = PolicyClient(str(ctx.request_state.get("opa_url", "http://opa:8181")))
        # WO-108 F1: the executor carries the sandbox-tier registry — an
        # isolated-tier task routes to runsc, or fails closed (SchedulerError)
        # when this environment has no runsc runtime. Never a silent downgrade.
        executor = EpisodeExecutor(
            db,
            build_service(db, policy),
            runner=JiuwenBoxRunner(),
            runner_registry=production_registry(),
        )
        await executor.start_episode(episode_id)
        return {"started": episode_id}

    @episode_object.handler()
    async def verify(ctx: Any, episode_id: str, branch: str) -> dict[str, Any]:
        db = await Database.connect()
        policy = PolicyClient(str(ctx.request_state.get("opa_url", "http://opa:8181")))
        executor = EpisodeExecutor(
            db, build_service(db, policy), runner_registry=production_registry()
        )
        await executor.resolve_verification(episode_id, branch)
        return {"closed": episode_id, "branch": branch}

    app = App(services=[episode_object])

except ImportError:  # restate-sdk not installed in this environment
    app = None
