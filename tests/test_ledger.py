"""Ledger invariant tests (WO-05) — engine-grade semantics on InMemoryLedger;
the TigerBeetle engine enforces the same invariants natively.

Invariants (manual §5.5):
  - posted + pending over cap -> BUDGET_EXHAUSTED (engine is the ONLY line
    of defense; app-layer checks are advisory)
  - UNKNOWN keeps the pending transfer alive (budget occupied)
  - parent/child work orders share the mandate budget account (no new quota)
  - model_cost is always accounted (even zero-business-action episodes) —
    covered by the gateway always reserving amount >= 1
"""

from __future__ import annotations

import pytest

from kernel.ledger import BudgetExhausted, InMemoryLedger, LedgerError


@pytest.fixture
def ledger() -> InMemoryLedger:
    return InMemoryLedger()


async def test_cap_rejects_posted_plus_pending(ledger: InMemoryLedger) -> None:
    await ledger.ensure_budget_account("mnd-1", cap=1000)
    t1 = await ledger.create_pending_transfer("mnd-1", 600, "k1")
    t2 = await ledger.create_pending_transfer("mnd-1", 300, "k2")
    assert t1 and t2
    # 600 pending + 400 new > 1000 cap -> engine-grade rejection
    with pytest.raises(BudgetExhausted):
        await ledger.create_pending_transfer("mnd-1", 400, "k3")
    # posting the first consumes budget; pending of second still occupies
    await ledger.post_transfer(t1)
    bal = await ledger.balance("mnd-1")
    assert (bal.posted, bal.pending) == (600, 300)
    assert bal.remaining == 100


async def test_unknown_keeps_pending_occupied(ledger: InMemoryLedger) -> None:
    await ledger.ensure_budget_account("mnd-2", cap=500)
    t = await ledger.create_pending_transfer("mnd-2", 500, "k1")
    # UNKNOWN: no post, no void — the transfer stays pending, cap fully used
    bal = await ledger.balance("mnd-2")
    assert bal.pending == 500
    with pytest.raises(BudgetExhausted):
        await ledger.create_pending_transfer("mnd-2", 1, "k2")
    # after reconciliation voids it, budget frees
    await ledger.void_transfer(t)
    assert (await ledger.balance("mnd-2")).pending == 0
    await ledger.create_pending_transfer("mnd-2", 500, "k3")


async def test_parent_child_share_budget_account(ledger: InMemoryLedger) -> None:
    """Child work orders draw from the SAME mandate budget account — deriving
    a child grant never mints new allowance."""
    await ledger.ensure_budget_account("mnd-3", cap=100)
    parent_transfer = await ledger.create_pending_transfer("mnd-3", 60, "parent")
    child_transfer = await ledger.create_pending_transfer("mnd-3", 30, "child")
    await ledger.post_transfer(parent_transfer)
    bal = await ledger.balance("mnd-3")
    # both draw the same account: 60 posted + 30 pending
    assert (bal.posted, bal.pending) == (60, 30)
    with pytest.raises(BudgetExhausted):
        await ledger.create_pending_transfer("mnd-3", 11, "child2")
    await ledger.post_transfer(child_transfer)


async def test_idempotent_transfer_replay(ledger: InMemoryLedger) -> None:
    await ledger.ensure_budget_account("mnd-4", cap=100)
    t = await ledger.create_pending_transfer("mnd-4", 40, "same-key")
    t2 = await ledger.create_pending_transfer("mnd-4", 40, "same-key")
    assert t == t2  # same id, single occupation
    assert (await ledger.balance("mnd-4")).pending == 40
    with pytest.raises(BudgetExhausted):
        await ledger.create_pending_transfer("mnd-4", 99, "other")


async def test_terminal_transfers_immutable(ledger: InMemoryLedger) -> None:
    await ledger.ensure_budget_account("mnd-5", cap=100)
    t = await ledger.create_pending_transfer("mnd-5", 10, "k")
    await ledger.post_transfer(t)
    await ledger.void_transfer(t)  # no-op: POSTED is terminal
    assert (await ledger.balance("mnd-5")).posted == 10
    assert (await ledger.balance("mnd-5")).pending == 0


async def test_missing_account_fails_closed(ledger: InMemoryLedger) -> None:
    with pytest.raises(LedgerError):
        await ledger.create_pending_transfer("never-created", 1, "k")
