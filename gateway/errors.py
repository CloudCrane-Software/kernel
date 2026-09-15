"""gateway/errors.py — gateway error codes (WO-03 contract)."""

from __future__ import annotations

from enum import StrEnum


class ErrorCode(StrEnum):
    IDEMPOTENCY_KEY_CONFLICT = "IDEMPOTENCY_KEY_CONFLICT"
    BUDGET_EXHAUSTED = "BUDGET_EXHAUSTED"
    LEASE_FENCED = "LEASE_FENCED"
    GRANT_INACTIVE = "GRANT_INACTIVE"
    DECISION_NOT_ALLOW = "DECISION_NOT_ALLOW"
    DEPENDENCY_UNAVAILABLE = "DEPENDENCY_UNAVAILABLE"
    OPEN_OBLIGATIONS_EXIST = "OPEN_OBLIGATIONS_EXIST"
    UNRESOLVED_UNKNOWN_EXISTS = "UNRESOLVED_UNKNOWN_EXISTS"
    NOT_FOUND = "NOT_FOUND"
    VALIDATION_ERROR = "VALIDATION_ERROR"


_STATUS: dict[ErrorCode, int] = {
    ErrorCode.IDEMPOTENCY_KEY_CONFLICT: 422,
    ErrorCode.BUDGET_EXHAUSTED: 409,
    ErrorCode.LEASE_FENCED: 409,
    ErrorCode.GRANT_INACTIVE: 409,
    ErrorCode.DECISION_NOT_ALLOW: 403,
    ErrorCode.DEPENDENCY_UNAVAILABLE: 503,
    ErrorCode.OPEN_OBLIGATIONS_EXIST: 409,
    ErrorCode.UNRESOLVED_UNKNOWN_EXISTS: 409,
    ErrorCode.NOT_FOUND: 404,
    ErrorCode.VALIDATION_ERROR: 422,
}


class GatewayError(Exception):
    def __init__(
        self,
        code: ErrorCode,
        detail: str,
        *,
        decision: str | None = None,
        reasons: list[str] | None = None,
    ) -> None:
        super().__init__(f"{code}: {detail}")
        self.code = code
        self.detail = detail
        self.decision = decision
        self.reasons = reasons or []

    @property
    def http_status(self) -> int:
        return _STATUS[self.code]
