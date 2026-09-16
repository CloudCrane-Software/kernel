"""gateway/app.py — FastAPI application factory (WO-03).

Four operations, all fail-closed:
  POST /v1/intents                      register an action intent
  POST /v1/intents/{id}/receipt         verify external-effect receipt
  POST /v1/reconcile                    reconciliation scheduler (skeleton)
  POST /v1/episodes/{id}/close          close an episode (explicit branch)

There is deliberately NO endpoint that closes an UNKNOWN intent.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import asyncpg
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

from gateway.classifiers import default_registry
from gateway.errors import ErrorCode, GatewayError
from gateway.schemas import (
    CloseEpisodeRequest,
    CloseEpisodeResponse,
    ErrorBody,
    ReceiptRequest,
    ReceiptResponse,
    ReconcileRequest,
    ReconcileResponse,
    RegisterIntentRequest,
    RegisterIntentResponse,
)
from gateway.service import GatewayService
from kernel.db import Database
from kernel.policy import PolicyClient


def _make_app(service: GatewayService) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        yield
        await service.policy.aclose()

    app = FastAPI(
        title="governance action gateway",
        version="0.1.0",
        description="The only door for external side effects.",
        lifespan=lifespan,
    )

    @app.exception_handler(GatewayError)
    async def gateway_error_handler(_: Request, exc: GatewayError) -> JSONResponse:
        body = ErrorBody(
            error=str(exc.code),
            detail=exc.detail,
            decision=exc.decision,
            reasons=exc.reasons,
        )
        return JSONResponse(status_code=exc.http_status, content=body.model_dump())

    @app.exception_handler(asyncpg.PostgresError)
    async def pg_unavailable_handler(_: Request, exc: Exception) -> JSONResponse:
        # any PG dependency failure maps to 503 fail-closed (manual 5.3)
        body = ErrorBody(error=str(ErrorCode.DEPENDENCY_UNAVAILABLE), detail=f"pg: {exc}")
        return JSONResponse(status_code=503, content=body.model_dump())

    @app.post("/v1/intents", response_model=RegisterIntentResponse, status_code=201)
    async def register_intent(
        req: RegisterIntentRequest, response: Response
    ) -> RegisterIntentResponse:
        result = await service.register_intent(req)
        if not result.created:
            response.status_code = 200  # idempotent replay of first registration
        return result

    @app.post("/v1/intents/{intent_id}/receipt", response_model=ReceiptResponse)
    async def verify_receipt(intent_id: str, req: ReceiptRequest) -> ReceiptResponse:
        return await service.verify_receipt(intent_id, req)

    @app.post("/v1/reconcile", response_model=ReconcileResponse)
    async def reconcile(req: ReconcileRequest) -> ReconcileResponse:
        return await service.reconcile(req)

    @app.post("/v1/episodes/{episode_id}/close", response_model=CloseEpisodeResponse)
    async def close_episode(episode_id: str, req: CloseEpisodeRequest) -> CloseEpisodeResponse:
        return await service.close_episode(episode_id, req)

    return app


def build_app(db: Database, policy: PolicyClient, ledger: Any | None = None) -> FastAPI:
    service = GatewayService(db, policy, default_registry(), ledger=ledger)
    return _make_app(service)


def build_app_from_env() -> FastAPI:
    """Production entrypoint.

    Env: KERNEL_PG_DSN, OPA_URL, and optionally KERNEL_TB_ADDRESSES
    (plus KERNEL_TB_CLUSTER_ID, default 0). When the TigerBeetle address is
    set, the gateway is wired with the ENGINE ledger so the mandate cap is
    enforced at engine level (debits_must_not_exceed_credits) — the engine
    is the only line of defense; application checks are advisory.
    """
    dsn = os.environ.get("KERNEL_PG_DSN")
    opa_url = os.environ.get("OPA_URL", "http://opa:8181")
    if not dsn:
        raise RuntimeError("KERNEL_PG_DSN is required")
    db = asyncio.run(Database.connect(dsn))
    ledger = None
    if tb := os.environ.get("KERNEL_TB_ADDRESSES"):
        from kernel.ledger import TigerBeetleLedger

        ledger = TigerBeetleLedger(
            cluster_id=int(os.environ.get("KERNEL_TB_CLUSTER_ID", "0")), addresses=tb
        )
    return build_app(db, PolicyClient(opa_url), ledger=ledger)
