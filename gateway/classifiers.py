"""gateway/classifiers.py — receipt classifier plugins (WO-03 endpoint 2).

The gateway never GUESSES: evidence insufficient → UNKNOWN. Classifiers are
registered per action_type; the default classifier handles "generic".
"""

from __future__ import annotations

from typing import Any, Protocol

from kernel.db import new_ulid


class ReceiptClassifier(Protocol):
    def classify(self, receipt: dict[str, Any]) -> str:
        """Return APPLIED | NOT_APPLIED | UNKNOWN. Never guess."""
        ...


class GenericClassifier:
    """Default: a receipt is conclusive only when it carries an explicit
    external status. Anything else (missing/ambiguous fields) is UNKNOWN."""

    def classify(self, receipt: dict[str, Any]) -> str:
        status = receipt.get("status")
        if status == "applied":
            return "APPLIED"
        if status == "not_applied":
            return "NOT_APPLIED"
        return "UNKNOWN"


class HttpHeadClassifier:
    """For http.* actions: conclusive when the receipt embeds a probed status
    code from the external system; 2xx/3xx = APPLIED, 4xx/5xx = NOT_APPLIED,
    anything else UNKNOWN."""

    def classify(self, receipt: dict[str, Any]) -> str:
        code = receipt.get("probed_status_code")
        if isinstance(code, int):
            if 200 <= code < 400:
                return "APPLIED"
            if 400 <= code < 600:
                return "NOT_APPLIED"
        return "UNKNOWN"


class ClassifierRegistry:
    def __init__(self) -> None:
        self._classifiers: dict[str, ReceiptClassifier] = {}

    def register(self, action_type: str, classifier: ReceiptClassifier) -> None:
        self._classifiers[action_type] = classifier

    def classify(self, action_type: str, receipt: dict[str, Any]) -> str:
        classifier = self._classifiers.get(action_type, GenericClassifier())
        result = classifier.classify(receipt)
        if result not in {"APPLIED", "NOT_APPLIED", "UNKNOWN"}:
            # a buggy plugin must fail closed, never guess
            return "UNKNOWN"
        return result


def default_registry() -> ClassifierRegistry:
    registry = ClassifierRegistry()
    registry.register("generic", GenericClassifier())
    registry.register("http.request", HttpHeadClassifier())
    return registry


def new_obligation_id() -> str:
    return f"obl-{new_ulid()}"
