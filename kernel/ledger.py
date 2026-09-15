"""kernel/ledger.py — the budget ledger (WO-05).

Account model (per mandate):
  Budget account   — one per mandate, flags: debits_must_not_exceed_credits
                     (engine-grade invariant: posted + pending <= cap)
  Operator account — income side of transfers
  Waste account    — window-end sweep of residuals (M3, pricing/ CLI)

Transfer lifecycle: register_intent creates a PENDING transfer (timeout=0);
receipt APPLIED posts it; NOT_APPLIED voids it; while the intent is UNKNOWN
the pending transfer PERSISTS (budget stays occupied — P-3 at ledger level).
Parent/child work orders share the mandate's Budget account: deriving a child
grant never creates new allowance.

Two implementations:
  InMemoryLedger    — semantics-faithful fake for tests and local dev
  TigerBeetleLedger — the real engine (official python client); import is
                      lazy so the dependency stays optional until deployment
"""

from __future__ import annotations

import hashlib
import threading
from dataclasses import dataclass
from typing import Any, Protocol

ACCOUNT_BUDGET = "budget"
ACCOUNT_OPERATOR = "operator"
ACCOUNT_WASTE = "waste"


class LedgerError(Exception):
    pass


class BudgetExhausted(LedgerError):
    """Engine-grade rejection: posted + pending would exceed the cap."""


@dataclass(frozen=True, slots=True)
class BudgetBalance:
    mandate_id: str
    cap: int
    posted: int
    pending: int

    @property
    def remaining(self) -> int:
        return self.cap - self.posted - self.pending


class Ledger(Protocol):
    async def ensure_budget_account(self, mandate_id: str, cap: int) -> None: ...

    async def create_pending_transfer(
        self, mandate_id: str, amount: int, idempotency_key: str
    ) -> str: ...

    async def post_transfer(self, transfer_id: str) -> None: ...

    async def void_transfer(self, transfer_id: str) -> None: ...

    async def balance(self, mandate_id: str) -> BudgetBalance: ...


def _deterministic_u128(source: str) -> int:
    return int.from_bytes(hashlib.sha256(source.encode()).digest()[:16], "big")


@dataclass
class _MemAccount:
    cap: int = 0
    posted: int = 0
    pending: int = 0


class InMemoryLedger:
    """Same invariants the TigerBeetle engine enforces (debits must not
    exceed credits), single-writer via a lock — for tests and local dev."""

    def __init__(self) -> None:
        self._accounts: dict[str, _MemAccount] = {}
        self._transfers: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()

    async def ensure_budget_account(self, mandate_id: str, cap: int) -> None:
        with self._lock:
            acc = self._accounts.setdefault(mandate_id, _MemAccount())
            acc.cap = max(acc.cap, cap)

    async def create_pending_transfer(
        self, mandate_id: str, amount: int, idempotency_key: str
    ) -> str:
        with self._lock:
            acc = self._accounts.get(mandate_id)
            if acc is None:
                raise LedgerError(f"budget account for {mandate_id} not created")
            existing = self._transfers.get(idempotency_key)
            if existing is not None:
                if existing["amount"] != amount:
                    raise BudgetExhausted("idempotency key reused with different amount")
                return idempotency_key  # idempotent replay
            # engine-grade invariant: debits (posted + pending) <= credits (cap)
            if acc.posted + acc.pending + amount > acc.cap:
                raise BudgetExhausted(
                    f"mandate {mandate_id}: posted+pending "
                    f"{acc.posted + acc.pending} + {amount} > cap {acc.cap}"
                )
            acc.pending += amount
            self._transfers[idempotency_key] = {
                "mandate_id": mandate_id,
                "amount": amount,
                "state": "PENDING",
            }
            return idempotency_key

    async def post_transfer(self, transfer_id: str) -> None:
        with self._lock:
            t = self._transfers.get(transfer_id)
            if t is None:
                raise LedgerError(f"transfer {transfer_id} not found")
            if t["state"] != "PENDING":
                return  # terminal states are immutable; post is idempotent no-op
            acc = self._accounts[t["mandate_id"]]
            acc.pending -= t["amount"]
            acc.posted += t["amount"]
            t["state"] = "POSTED"

    async def void_transfer(self, transfer_id: str) -> None:
        with self._lock:
            t = self._transfers.get(transfer_id)
            if t is None:
                raise LedgerError(f"transfer {transfer_id} not found")
            if t["state"] != "PENDING":
                return
            acc = self._accounts[t["mandate_id"]]
            acc.pending -= t["amount"]
            t["state"] = "VOIDED"

    async def balance(self, mandate_id: str) -> BudgetBalance:
        with self._lock:
            acc = self._accounts.get(mandate_id)
            if acc is None:
                raise LedgerError(f"budget account for {mandate_id} not found")
            return BudgetBalance(
                mandate_id=mandate_id, cap=acc.cap, posted=acc.posted, pending=acc.pending
            )


class TigerBeetleLedger:
    """Real engine wiring. Requires `pip install tigerbeetle` and a running
    cluster (platform compose service `tigerbeetle`). All amounts are minor
    units mapped to u128; transfer ids derive deterministically from the
    idempotency key so retries are naturally idempotent at engine level."""

    def __init__(self, cluster_id: int = 0, addresses: str = "127.0.0.1:3000") -> None:
        try:
            from tigerbeetle import Client
        except ImportError as e:  # pragma: no cover - optional dependency
            raise LedgerError(
                "tigerbeetle client not installed (optional dep until deployment)"
            ) from e
        self._client = Client(cluster_id=cluster_id, addresses=addresses)

    def _budget_account_id(self, mandate_id: str) -> int:
        return _deterministic_u128(f"{mandate_id}:{ACCOUNT_BUDGET}")

    def _operator_account_id(self) -> int:
        return _deterministic_u128(ACCOUNT_OPERATOR)

    def _waste_account_id(self) -> int:
        return _deterministic_u128(ACCOUNT_WASTE)

    def _transfer_id(self, idempotency_key: str) -> int:
        return _deterministic_u128(f"transfer:{idempotency_key}")

    async def ensure_budget_account(self, mandate_id: str, cap: int) -> None:
        account = {
            "id": self._budget_account_id(mandate_id),
            "ledger": 1,
            "code": 1,
            "flags": 0x0002,  # debits_must_not_exceed_credits
            "credits": cap,
            "debits": 0,
            "user_data_128": 0,
            "user_data_64": 0,
            "user_data_32": 0,
        }
        # create_accounts is idempotent-safe at engine level per release docs;
        # account-exists results are treated as success
        results = self._client.create_accounts([account])
        for r in results:
            if r.get("result") not in ("ok", "exists"):
                raise LedgerError(f"account create rejected: {r}")

    async def create_pending_transfer(
        self, mandate_id: str, amount: int, idempotency_key: str
    ) -> str:
        transfer = {
            "id": self._transfer_id(idempotency_key),
            "debit_account_id": self._budget_account_id(mandate_id),
            "credit_account_id": self._operator_account_id(),
            "amount": amount,
            "user_data_128": 0,
            "user_data_64": 0,
            "user_data_32": 0,
            "timeout": 0,  # pending forever until post/void
            "flags": 0x0001,  # pending
        }
        results = self._client.create_transfers([transfer])
        for r in results:
            res = r.get("result")
            if res in ("ok", "exists"):
                continue
            if res == "exceeds_credits":
                raise BudgetExhausted(f"mandate {mandate_id} cap exceeded")
            raise LedgerError(f"transfer rejected: {r}")
        return str(transfer["id"])

    async def post_transfer(self, transfer_id: str) -> None:
        tid = int(transfer_id)
        results = self._client.commit_transfers(
            [{"id": tid, "flags": 0x0004}]  # post_pending_transfer
        )
        for r in results:
            if r.get("result") not in ("ok", "exists", "transfer_posted"):
                raise LedgerError(f"post rejected: {r}")

    async def void_transfer(self, transfer_id: str) -> None:
        tid = int(transfer_id)
        results = self._client.commit_transfers(
            [{"id": tid, "flags": 0x0008}]  # void_pending_transfer
        )
        for r in results:
            if r.get("result") not in ("ok", "exists", "transfer_voided", "transfer_expired"):
                raise LedgerError(f"void rejected: {r}")

    async def balance(self, mandate_id: str) -> BudgetBalance:
        import asyncio

        accounts = await asyncio.to_thread(
            self._client.lookup_accounts, [self._budget_account_id(mandate_id)]
        )
        for a in accounts:
            posted = int(a["debits_posted"])
            pending = int(a["debits_pending"])
            return BudgetBalance(
                mandate_id=mandate_id,
                cap=int(a["credits"]),
                posted=posted,
                pending=pending,
            )
        raise LedgerError(f"budget account for {mandate_id} not found")


def window_end_sweep(ledger: Ledger, mandate_id: str, *, to: str = ACCOUNT_WASTE) -> Any:
    """Window-end residual sweep (M3 pricing CLI entrypoint): moves remaining
    allowance to the Waste account so the next window starts clean. The
    concrete transfer is executed by the engine ledger; placeholder until
    pricing/ lands."""
    raise NotImplementedError("window sweep ships with pricing/ (M3)")
