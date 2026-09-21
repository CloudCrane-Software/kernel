# ADR 0003: auto-merge principle (owner decision, 2026-09-16)

> **Mirror (WO-108 F2).** Authoritative home: the `.github` (dotgithub) repo,
> `docs/adr/0003-auto-merge-principle.md` — per ADR-0005 §3, governance ADRs
> are "pointer, not mirror": this kernel copy is a READ-ONLY SNAPSHOT kept so
> in-repo references (AGENTS.md, `.github/workflows/eval-gate.yml`) resolve
> without leaving the repo. Changes are made in the authoritative repo and
> re-synced here; never edit this file directly.

## Context
ADR 0001's per-PR human approval gate (merge-gate + eval-gate environment
reviewers) buried the sole human under approval clicks. The owner has
explicitly changed the operating principle: NO per-PR human approvals — CI
green means merge, automatically.

## Decision
- merge-gate workflow and `human-approval` required checks are REMOVED from
  all repos; eval-gate environments carry no required reviewers.
- Quality gates are exclusively machine checks: CI (kernel: test/opa/guard,
  platform: validate), strict up-to-date branches, linear history, no force
  pushes, admins included. Product/eval separation stays enforced by
  guard.yml.
- Human role narrows to: mandate signing, UNKNOWN rulings, eval threshold
  changes (eval-gate repo), post-hoc review of what merged.

## Consequences
- Agents merge autonomously once checks pass.
- Protection against bad merges = the test/guard suites themselves.
  Strengthening those (not adding clicks) is the safety lever.
- ADR 0001's mechanism is superseded; its analysis of why
  required_approving_review_count=1 is impossible for a single-member
  account remains valid background.
