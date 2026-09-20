"""gateway/app.py — FastAPI application factory (WO-03 + WO-0004).

Operations, all fail-closed:
  POST /v1/intents                      register an action intent
  POST /v1/intents/{id}/receipt         verify external-effect receipt
  POST /v1/reconcile                    reconciliation scheduler (skeleton)
  POST /v1/episodes/{id}/close          close an episode (explicit branch)
  POST /v1/workorders             seed a RESERVED episode (WO-0004)
  POST /v1/episodes/{id}/transition     one-way episode transition (WO-0004)

There is deliberately NO endpoint that closes an UNKNOWN intent.
"""

from __future__ import annotations

import os
import secrets
from collections.abc import AsyncIterator, Callable
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
    CreateWorkorderRequest,
    CreateWorkorderResponse,
    ErrorBody,
    ReceiptRequest,
    ReceiptResponse,
    ReconcileRequest,
    ReconcileResponse,
    RegisterIntentRequest,
    RegisterIntentResponse,
    TransitionEpisodeRequest,
    TransitionEpisodeResponse,
)
from gateway.service import GatewayService, IllegalEpisodeTransition
from kernel.db import Database
from kernel.policy import PolicyClient


def _unauthorized(detail: str) -> JSONResponse:
    """WO-0004: 401 without touching gateway.errors.ErrorCode (that enum is
    pinned closed by the smoke eval; see gateway/service.py)."""
    body = ErrorBody(error="UNAUTHORIZED", detail=detail)
    return JSONResponse(status_code=401, content=body.model_dump())


def _admin_guard(request: Request, service: GatewayService) -> JSONResponse | None:
    """Bearer check for the WO-0004 privileged surface. Fail-closed: when no
    token is configured, every admin request is 401."""
    expected = service.admin_token
    if expected is None:
        return _unauthorized("admin auth not configured (KERNEL_ADMIN_TOKEN unset)")
    supplied = request.headers.get("authorization", "")
    if supplied.startswith("Bearer ") and secrets.compare_digest(
        supplied[len("Bearer ") :], expected
    ):
        return None
    return _unauthorized("missing or invalid bearer token")


def _register_routes(app: FastAPI, service_of: Callable[[], GatewayService]) -> None:
    pass

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok", "service": "action-gateway"}

    @app.post("/v1/intents", response_model=RegisterIntentResponse, status_code=201)
    async def register_intent(
        req: RegisterIntentRequest, response: Response
    ) -> RegisterIntentResponse:
        result = await service_of().register_intent(req)
        if not result.created:
            response.status_code = 200  # idempotent replay of first registration
        return result

    @app.post("/v1/intents/{intent_id}/receipt", response_model=ReceiptResponse)
    async def verify_receipt(intent_id: str, req: ReceiptRequest) -> ReceiptResponse:
        return await service_of().verify_receipt(intent_id, req)

    @app.post("/v1/reconcile", response_model=ReconcileResponse)
    async def reconcile(req: ReconcileRequest) -> ReconcileResponse:
        return await service_of().reconcile(req)

    @app.post("/v1/episodes/{episode_id}/close", response_model=CloseEpisodeResponse)
    async def close_episode(episode_id: str, req: CloseEpisodeRequest) -> CloseEpisodeResponse:
        return await service_of().close_episode(episode_id, req)

    @app.post("/v1/workorders", response_model=CreateWorkorderResponse, status_code=201)
    async def create_workorder(
        request: Request, req: CreateWorkorderRequest, response: Response
    ) -> CreateWorkorderResponse | JSONResponse:
        if (denied := _admin_guard(request, service_of())) is not None:
            return denied
        result = await service_of().create_workorder(req)
        if not result.created:
            response.status_code = 200  # idempotent replay: return the current value
        return result

    @app.post("/v1/episodes/{episode_id}/transition", response_model=TransitionEpisodeResponse)
    async def transition_episode(
        request: Request, episode_id: str, req: TransitionEpisodeRequest
    ) -> TransitionEpisodeResponse | JSONResponse:
        if (denied := _admin_guard(request, service_of())) is not None:
            return denied
        return await service_of().transition_episode(episode_id, req)


def _register_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(IllegalEpisodeTransition)
    async def illegal_transition_handler(_: Request, exc: IllegalEpisodeTransition) -> JSONResponse:
        # WO-0004: outside the one-way map (trg_episodes_one_way) -> 409
        body = ErrorBody(error="ILLEGAL_TRANSITION", detail=str(exc))
        return JSONResponse(status_code=409, content=body.model_dump())

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


def _make_app(service_of: Callable[[], GatewayService]) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        yield
        await service_of().policy.aclose()

    app = FastAPI(
        title="governance action gateway",
        version="0.1.0",
        description="The only door for external side effects.",
        lifespan=lifespan,
    )
    _register_error_handlers(app)
    _register_routes(app, service_of)
    return app


def build_app(
    db: Database,
    policy: PolicyClient,
    ledger: Any | None = None,
    admin_token: str | None = None,
) -> FastAPI:
    service = GatewayService(db, policy, default_registry(), ledger=ledger, admin_token=admin_token)
    return _make_app(lambda: service)


def build_app_from_env() -> FastAPI:
    """Production entrypoint (sync factory; dependencies connect lazily in lifespan).

    Env: KERNEL_PG_DSN, OPA_URL, and optionally KERNEL_TB_ADDRESSES
    (plus KERNEL_TB_CLUSTER_ID, default 0) and KERNEL_ADMIN_TOKEN (WO-0004
    bearer token for the admin/transition surface; unset = fail closed).
    When the TigerBeetle address is
    set, the gateway is wired with the ENGINE ledger so the mandate cap is
    enforced at engine level (debits_must_not_exceed_credits) — the engine
    is the only line of defense; application checks are advisory.
    """

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        dsn = os.environ.get("KERNEL_PG_DSN")
        opa_url = os.environ.get("OPA_URL", "http://opa:8181")
        if not dsn:
            raise RuntimeError("KERNEL_PG_DSN is required")
        db = await Database.connect(dsn)
        ledger = None
        if tb := os.environ.get("KERNEL_TB_ADDRESSES"):
            from kernel.ledger import TigerBeetleLedger

            ledger = TigerBeetleLedger(
                cluster_id=int(os.environ.get("KERNEL_TB_CLUSTER_ID", "0")), addresses=tb
            )
        app.state.service = GatewayService(
            db,
            PolicyClient(opa_url),
            default_registry(),
            ledger=ledger,
            admin_token=os.environ.get("KERNEL_ADMIN_TOKEN"),
        )
        yield
        await app.state.service.policy.aclose()

    app = FastAPI(
        title="governance action gateway",
        version="0.1.0",
        description="The only door for external side effects.",
        lifespan=lifespan,
    )
    _register_error_handlers(app)
    _register_routes(app, lambda: app.state.service)
    return app
