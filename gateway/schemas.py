"""gateway/schemas.py — request/response models (WO-03)."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class RegisterIntentRequest(BaseModel):
    operation_id: str = Field(min_length=1, max_length=128)
    episode_id: str = Field(min_length=1, max_length=128)
    grant_id: str = Field(min_length=1, max_length=128)
    idempotency_key: str = Field(min_length=1, max_length=256)
    params: dict[str, Any]
    action_type: str = "generic"
    amount: int = Field(gt=0, description="reserved budget in minor units")
    expected_epoch: int | None = Field(
        default=None, description="caller's lease snapshot; mismatch = LEASE_FENCED"
    )


class DecisionInfo(BaseModel):
    decision_id: str
    outcome: str
    reasons: list[str]
    registry_revision: int


class RegisterIntentResponse(BaseModel):
    intent_id: str
    created: bool
    state: str
    fence_epoch: int
    reservation_id: str
    tb_transfer_id: str
    decision: DecisionInfo


class ReceiptRequest(BaseModel):
    receipt: dict[str, Any]


class ReceiptResponse(BaseModel):
    intent_id: str
    classification: str  # APPLIED | NOT_APPLIED | UNKNOWN
    obligation_id: str | None = None


class ReconcileRequest(BaseModel):
    limit: int = Field(default=50, ge=1, le=500)


class ReconcileResponse(BaseModel):
    scanned: int
    resolved: int
    still_unknown: int
    deferred: int


class CloseEpisodeRequest(BaseModel):
    terminal_branch: str = Field(pattern="^(candidate_ready|not_solved|deferred|expired)$")


class CloseEpisodeResponse(BaseModel):
    episode_id: str
    state: str = "CLOSED"
    terminal_branch: str


class ErrorBody(BaseModel):
    error: str
    detail: str
    decision: str | None = None
    reasons: list[str] = []
