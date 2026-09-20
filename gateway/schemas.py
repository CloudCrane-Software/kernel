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


class CreateWorkorderRequest(BaseModel):
    """WO-0004: seed a RESERVED episode for a work order (episode_id =
    workorder_id). metadata is echoed, not persisted (episodes table has no
    metadata column; the audit trail lives in episodes/decisions)."""

    workorder_id: str = Field(min_length=1, max_length=128)
    metadata: dict[str, Any] = Field(default_factory=dict)


class CreateWorkorderResponse(BaseModel):
    episode_id: str
    workorder_id: str
    state: str
    created: bool
    metadata: dict[str, Any]


class TransitionEpisodeRequest(BaseModel):
    """WO-0004: one-way transition target. The legal edge set mirrors
    trg_episodes_one_way (ops/sql/0001_init.sql), which stays the final
    authority; violations surface as 409 ILLEGAL_TRANSITION."""

    target_state: str = Field(pattern="^(RESERVED|RUNNING|VERIFYING|CLOSED)$")
    terminal_branch: str | None = Field(
        default=None,
        pattern="^(candidate_ready|not_solved|deferred|expired)$",
        description="required when target_state is CLOSED",
    )


class TransitionEpisodeResponse(BaseModel):
    episode_id: str
    previous_state: str
    state: str
    terminal_branch: str | None = None


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
