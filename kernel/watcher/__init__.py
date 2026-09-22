"""kernel.watcher — work-order watcher (batch D orchestration layer).

Polls the mandates repo for new work-order files, seeds an episode for each
one through the action gateway (POST /v1/workorders, idempotent), drives the
RESERVED -> RUNNING transition and drops a dispatch file for the external
development agent. The watcher itself never develops: it is the trigger and
the state ledger, deliberately decoupled from any specific agent runtime.

MVP scope (this release):
  1. poller      — fetch mandates origin main, diff against the local state
  2. dispatcher  — seed episode + RESERVED -> RUNNING via the gateway
  3. dispatch    — atomic JSON file per work order into the dispatch volume

Left as interfaces for later iterations (verified against the same models):
  4. verifier    — run the suite eval criteria on VERIFYING episodes
  5. closer      — evidence section + mandates close-out MR
"""

from __future__ import annotations

__version__ = "0.1.0"
