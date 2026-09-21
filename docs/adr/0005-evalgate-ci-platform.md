# ADR 0005: eval-gate CI runs on GitHub Actions; CNB repos hold the spec of record

> **Mirror (WO-108 F2).** Authoritative home: the platform repo,
> `docs/adr/0005-evalgate-ci-platform.md` — this kernel copy is a READ-ONLY
> SNAPSHOT kept so in-repo references (`.github/workflows/guard.yml`) resolve
> without leaving the repo (single-authority principle per ADR-0005 §3).
> Changes are made in the authoritative repo and re-synced here; never edit
> this file directly.

Date: 2026-09-20 (owner default ratified in the 2026-09-20 owner session)
Status: Accepted
Work order: mandates `workorders/0003-evalgate-ci-platform-adr.md` (M2 启动计划终稿 §2.3 [EVALGATE-CI])

## Context

The eval-gate gate (machine gate for work-order acceptance) was specified in
the CNB org (Cloudbird-Software), but investigation during M2 reconnaissance
(2026-09-19, 侦察核查结论 EVALGATE-CI evidence 1–4) established where it
actually executes:

- The gate actually executes as GitHub Actions workflow **359083273**
  ("eval-gate (smoke)", file `.github/workflows/eval-gate.yml`) in the
  `CloudCrane-Software/kernel` repo. Verified 2026-09-20 via
  `GET /repos/CloudCrane-Software/kernel/actions/workflows/359083273`:
  `state: active`; recent runs (run #29–#31, 2026-09-17) all `success`.
  Its triggers are `push` / `pull_request` / `workflow_call` only (no
  `schedule` — time-based evals such as suite restate-audit-export E4 need
  an external scheduler; see mandates incident 2026-09-19 §4).
- The four CNB private repos (`eval-gate`, `mandates`, `pricing`, `legacy`)
  have **zero build records** (`GET /{repo}/-/build/logs` → `total: 0` for
  all four, re-verified 2026-09-20) and none contains a `.cnb.yml` pipeline.
  They hold specifications and work-order governance artifacts, not builds.
- The `platform` repo's own CI ("validate", workflow 358937891) runs
  repo-level checks (shellcheck, compose config, image pinning, secret scan,
  Caddyfile validate); the eval-gate smoke suite runs in the kernel repo, not
  here.
- This repo's `docs/adr/` previously held only ADR 0002 (CNB org naming) and
  ADR 0004 (edge colocation) — the split of ADR authority across repos was
  undocumented.

## Decision

1. Record the fact base: the eval-gate gate executes on **GitHub Actions**
   (workflow 359083273, kernel repo); the CNB repos are **specification and
   governance asset repos** with no CI execution.
2. Adopt the following authority statement (owner-ratified default, written
   here verbatim as the binding wording):

   > **规格权威 = CNB eval-gate 仓 `suites/` 与 `thresholds/`；执行记录 = GitHub Actions run URL；两者冲突时以 GitHub 实际执行结果为准。**

   (Translation: the spec authority is the CNB eval-gate repo's `suites/`
   and `thresholds/`; the execution record is the GitHub Actions run URL;
   on conflict, the actual GitHub execution result prevails.)
3. **ADR repo relationship — pointer, not mirror** (chosen per WO-0003
   requirement 2, with reason): ADR-0001 (`0001-required-approvals`) and
   ADR-0003 (`0003-auto-merge-principle`) are governance-process ADRs whose
   authoritative home is the **.github (dotgithub) repo** `docs/adr/`
   (GitHub main = 39c80da). This platform repo `docs/adr/` holds
   **platform-scoped ADRs** (0002 CNB org naming, 0004 edge colocation, this
   ADR 0005, ADR 0006 key escrow) and does **not** mirror governance ADRs;
   documents refer to them by number plus repo qualifier, e.g. "ADR-0003
   (.github 仓 docs/adr/)". Reason: mirroring would create two writable
   copies of one decision (dual authority) that will drift; a pointer keeps
   exactly one authoritative text per domain while keeping references
   resolvable. The 5 cross-references in the manual/M1 ledger already carry
   the repo qualifier (batch A DOC-FIX, 2026-09-20).
4. Migration to CNB (option b: add `.cnb.yml` pipelines and produce ≥1 build
   record) is explicitly **not** taken.

## Consequences

- Work-order acceptance evidence cites GitHub Actions run URLs as the
  execution record; suite/threshold changes are proposed against the CNB
  eval-gate repo (spec of record) and only take effect on execution when the
  kernel workflow consumes them.
- `platform` ADR numbering is continuous across repos (0001/0003 in the
  .github repo; 0002/0004/0005/0006 here) — look up by the repo qualifier.
- M1 未决 #3 ("eval-gate 跑在 GitHub") can be closed by the M1 ledger batch
  after this work order closes: the fact is now documented here as the
  authoritative record.
- If the org later wants CI on CNB, that is a new ADR (option b above),
  including a decision about which executor's results prevail during any
  transition window.

## Verification (machine-checkable, WO-0003 acceptance)

```
ls docs/adr/ | grep 0005                                   → this file
git log origin/main --oneline | grep -i 0005               → the merge commit
git ls-remote <github origin> refs/heads/main
  == git ls-remote <cnb mirror> refs/heads/main            → mirrors in sync
curl -H "Authorization: token $GH" \
  https://api.github.com/repos/CloudCrane-Software/kernel/actions/workflows/359083273
  → "state":"active"
for repo in eval-gate mandates pricing legacy; do
  curl -H "Authorization: Bearer $CNB" \
    "https://api.cnb.cool/Cloudbird-Software/$repo/-/build/logs" → total: 0
done
```
