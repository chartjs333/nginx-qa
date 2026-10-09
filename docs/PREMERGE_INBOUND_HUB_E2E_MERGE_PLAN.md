# Inbound Hub ↔ nginx-qa pre-merge plan

## Decision

IE2E-001 through IE2E-006 completed their managed gates, and the accepted
qualification record is `b9b44072f576d06b0e571b86a4b6746d96f65a01` (tree
`f0a53e9d25f115a0bd1e0fcf9e91d0c56dd3e2eb`). This document is a merge plan,
not merge, deployment, restart, promotion, or permission to mutate `main`,
18025, 18026, or any runtime state.

The current candidate is **NO-GO for an immediate PR merge**. The live managed
orchestrator on 18026 required three code-only recovery fixes that are not in
the candidate Git graph and are not advertised by the canonical GitHub
`origin`. Protected local service/fix repositories retain the objects and local
refs, but that is not durable repository publication. The fixes must be
published, integrated without rewriting their ancestry, and the resulting
exact head must be requalified before the repository owner opens the final PR.

The IE2E-007 record commit is a documentation-only child of the qualified
commit. Its immutable SHA is intentionally bound externally by the accepted
IE2E-007 result because a commit cannot contain its own ID. Before any later
integration, require the remote `agent/ie2e-07-merge-plan` head to equal that
accepted result SHA and require its diff from `b9b44072...` to contain only the
IE2E-007 plan and evidence index.

## Candidate identity and topology

| Item | Exact value |
|---|---|
| Repository | `https://github.com/chartjs333/nginx-qa.git` |
| Pinned target main | `70524c8aba60e6f8b9ebc3b3374f5992be520fc1` |
| Active sprint manifest source | `a5dec1213a0d4530928ad7e9763a4d0d044348db` |
| Accepted prior inbound qualification | `02d16fdf2e2220a0dfd9854721e89da73d7fe545` |
| Manifest/evidence and implementation join | `89573ff445cef784ec91d43b7d12be17c6fb23ee` |
| Canonical integration checkpoint | `integration/inbound-coordinator-e2e-20261007@2abb11c0a869ab924934d7d471741aed036e63da` |
| Accepted Play-boundary proof | `a1bf2426abe3aa1b56c891d9674e0ad8c61b9211` |
| Accepted qualification record | `agent/ie2e-06-qualification@b9b44072f576d06b0e571b86a4b6746d96f65a01` |
| Merge-plan branch | `agent/ie2e-07-merge-plan` |

The real ancestry join `89573ff...` has parents
`556a0442b0e4666f6456cd94f773de51decf21c9` (manifest/evidence lineage) and
`02d16fdf2e2220a0dfd9854721e89da73d7fe545` (qualified inbound implementation
lineage). `2abb11c...` is its child and an ancestor of `b9b44072...`. Preserve
that ancestry: do not squash, rebase, or reconstruct it with ad-hoc cherry-picks.

The canonical integration branch alone is not a final PR head because it
precedes IE2E-003 through IE2E-006. The eventual source must descend from the
accepted IE2E-007 head, incorporate the runtime-fix stack below, and carry new
qualification evidence for the exact combined head.

At `b9b44072...`, `origin/main...candidate` is 0 behind and 110 commits ahead,
with 190 changed files, 147,787 insertions, and 5,886 deletions. This is a broad
cumulative repository update, not an inbound-only patch. Review and CI must
cover the complete diff.

### Excluded parallel lineage

`origin/codex/premerge-inbound-hub-e2e-sprint` has advanced to
`631391115e8e6e79f50a13e00880e9f6459b2a3a`, and the `*-r2` branches descend
from a parallel integration checkpoint `1a4d80b294352e73b41f998493827a819f9819c4`.
Neither is an ancestor of the qualified candidate. The active sprint used the
manifest at `a5dec121...`; do not silently add `6313911...`, merge the parallel
R2 branches, or claim their current tips were qualified. Any desired content
from that lineage requires an explicit diff decision and requalification.

The historical manifest also uses the movable source ref
`refs/heads/codex/premerge-inbound-hub-e2e-sprint` with no pinned
`expected_source_commit`; that ref now resolves to the unqualified R2 tip
`6313911...`. Never replay this sprint from the movable ref. Any future reuse
must pin a reviewed immutable commit/ref in a separate manifest change.

## Required runtime-fix prerequisite

The running 18026 service is at `569e0bfe4af1596f3992a2c5b0566aca2f35c2bd`.
Its clean local fix series is:

1. `bb5095ab76cef6c8921d69a45dbc066b01187798` — expose the validated managed
   workspace block from phone-specific identity and publish its response schema.
2. `9fe9d3b2e11ba35491df180f702474ade21c4cab` — recover publication when a
   managed branch is reused and has advanced by an ordinary fast-forward.
3. `569e0bfe4af1596f3992a2c5b0566aca2f35c2bd` — schedule a repeated
   `any_parent` successor atomically during review acceptance.

The series is linear from `6b4b4cc4877eff46725f86614bafd3229fa3d9b8`,
touches nine files, and totals 1,176 insertions and 137 deletions. No head on
the canonical GitHub `origin` advertises any commit in the series; local
service/fix repositories retain them. The candidate lacks the managed identity response schema,
still has the pre-recovery reused-branch logic, and its
`_prospective_join_plan()` only plans `all_parents`. A read-only application
check of the complete `6b4b4cc...569e0bfe` patch against `b9b44072...` passed,
but this is not a substitute for a reviewed merge and tests.

The nine-file range is limited to the managed contract, `main.py`,
`nginx_qa/git_provider.py`, `nginx_qa/managed_continuity.py`, the new managed
identity response schema, and the four focused contract/continuity/workspace
test modules.

Required handling:

1. Publish the exact linear fix series to a protected remote branch after
   verifying the local clean heads, object IDs, parent chain, author/committer
   metadata, and recorded signature status. The exact commits are currently
   unsigned, so do not imply a signature exists or rewrite them merely to add
   one; apply repository signature policy explicitly. Do not copy files
   manually and do not force-push.
2. From the accepted IE2E-007 head, create a new final-candidate branch and
   perform an ancestry-preserving non-fast-forward merge of the fix tip
   `569e0bfe...`. This retains all three exact commits. Abort on conflicts or
   unexpected files; do not resolve by reset, squash, or unreviewed rewrite.
3. Review the full combined diff. Confirm that the merge adds only the nine-file
   fix-series delta on top of the accepted candidate and that no local config,
   secret, runtime, SQLite, registry, log, prompt, PID, or workspace file is
   tracked.
4. Re-run the IE2E-006 qualification suites on the exact combined head, plus
   focused regressions for managed workspace identity, reused-branch
   publication recovery, and repeated `any_parent` review transition. Repeat a
   fresh disposable managed flow proving those three behaviors together.
   At minimum, the focused command must include
   `tests.test_managed_continuity`,
   `tests.test_managed_continuity_import_recovery`,
   `tests.test_managed_workspaces`, and `tests.test_sprint_type_contract`, plus
   OpenAPI/schema validation.
5. Commit a new exact-head qualification record and obtain both API and
   evidence approvals. Only that new approved head may become the PR source.

Merging `b9b44072...` or the IE2E-007 documentation head without this decision
would leave `main` unable to reproduce recovery behavior required by the sprint
that produced the evidence.

## Accepted managed graph evidence

Every accepted result below had two non-deduplicated managed approvals.

| Node | Assignment and outcome | Accepted commit | Result key | Reviews |
|---|---|---|---|---|
| IE2E-001 completed baseline | `assignment-2a84f7d0389181f8a41926c3`, DONE | `556a0442b0e4666f6456cd94f773de51decf21c9` | `result-f8e86ca4091c497fc7f4a22b91d6b9644c5a67a75df065822860d5ae28d80a13` | `review-8e1626170cf1965e0e28274e`, `review-d993fab16c8293c86473abc5` |
| IE2E-002 integration | `assignment-0c98f585c0338bf0089bdc1e`, DONE | `2abb11c0a869ab924934d7d471741aed036e63da` | `result-3cf9b6feacab8114a339a9f473e706d0b83473c1293c92514e9d1c4b4602d3bb` | `review-97b2d2f091383c23f58b3720`, `review-340db7332e7075134505ebec` |
| IE2E-003 final staging | `assignment-aa4d660cbb4ad8b3cc44b9ff`, DONE | `172f0c23e985c337f180cc84de1f1a2ec70d503e` | `result-bdcee768468df0fcdefb5ffd188d94be5e8bd1ea5494cee0651470aae9a26e62` | `review-dfa5dfd1c1e15cc52dd4c699`, `review-ed66b1bc840187e941a401c6` |
| IE2E-004 final inbound flow | `assignment-a47b8da98f92c09203cdf89c`, DONE | `74fe1016d2eacd6e73e141c262f5ef2b2a323f1b` | `result-c40d9838116cf318b9328974a5dd5d6e701d3346bb7e1ad86836d9665950d399` | `review-553738e560eca74817cd3a70`, `review-1c7851c4689784f560fdb74a` |
| IE2E-005 final Play rework | `assignment-f7bb9d6e3330b206a37d0e23`, DONE | `a1bf2426abe3aa1b56c891d9674e0ad8c61b9211` | `result-cf577e452c971e77c9f01bf513a74bd942c982aba242f9a62d2057a12f67515b` | `review-d2b249470eff6eebff0ca6d2`, `review-fe56a514f20c43caf374b1c7` |
| IE2E-006 qualification | `assignment-7ed1b11e1efb53eb4e2d5e19`, PASS | `b9b44072f576d06b0e571b86a4b6746d96f65a01` | `result-c0b105fa5ebc6f6a3d29b5d034a9b5267f9b4c4dd8bdfbadb8a28e2ad6b149a4` | `review-a189f94e8b3ae42001b1e3b3`, `review-737f70a17a55a98363e164a5` |

Historical/rework records remain auditable but are not final evidence. In
particular, `0f3c9d3...` was rejected by the API reviewer because it used the
legacy `/sprints` projection as managed inventory. The authoritative Play proof
is `a1bf2426...` and uses `/api/v1/execution-catalog`.

## Qualification and E2E proof

Counts are deliberately non-additive:

- affected regression and legacy compatibility: 118/118 passed;
- managed continuity/import/Git/scope-secret/workspaces: 212/212 passed;
- isolated actor-import rerun: 57/57 passed (overlaps the 118 selection);
- 330 unique managed/legacy selected tests and 387 executions including rerun;
- inbound contract and proposal API: 43/43 passed;
- Pending Sprints UI: 14/14 passed;
- focused Telegram pending compatibility: 16/16 passed;
- Python compilation passed;
- all three staging profiles used by this flow passed, 3/3.

All recorded runs had zero failures, errors, and skips except the separately
disclosed generic staging-example diagnostic below.

Primary committed evidence:

- `docs/evidence/premerge-inbound-hub-e2e-v1/ie2e-001-completed-sprint-baseline.json`
- `docs/evidence/premerge-inbound-hub-e2e-v1/ie2e-004-inbound-hub-e2e-resume.json`
- `docs/evidence/premerge-inbound-hub-e2e-v1/ie2e-005-play-boundary-rework.json`
- `docs/evidence/premerge-inbound-hub-e2e-v1/ie2e-006-qualification.json`

The fresh loopback staging profile at 18031 used isolated `D:/nq-e2e-18031`
and `C:/nq-e2e-18031` roots, with child ports 18500-18599. `/`, `/git-config`,
`/openapi.json`, `/api/v1/execution-catalog`, and managed observability returned
HTTP 200. The authoritative catalog contained zero sprints immediately before
Play and exactly one disposable active sprint after one bodyless proposal
`/start`. Create, read, status, preview, comment, and reject did not activate.
No direct start-from-git request was made.

The qualification also verified idempotency, source provenance, stable
auth/error/correlation behavior, Pending UI and Telegram compatibility, and
that source metadata does not leak into the executor as an email/folder
adapter. Seven evidence artifacts were checked by ten high-signal secret rules
(70 checks, zero hits), with a separate structured sensitive-key scan.

## PR and operator-only merge gate

The following is a plan for a named repository operator. IE2E-007 does not
authorize these actions.

1. Wait for the IE2E-007 result, both managed reviews, and terminal sprint state
   `completed`. Record the operator, change ticket, review owners, rollback
   owner, and merge window.
2. Complete and requalify the runtime-fix prerequisite above. Pin the resulting
   final candidate SHA and tree in a replacement qualification record.
3. Fetch `origin` and resolve remote heads. Require `origin/main` still equals
   `70524c8...`. If it moved, stop; build a fresh integration descendant and
   rerun qualification rather than rebasing or force-pushing accepted evidence.
4. Require the final candidate to contain `89573ff...`, `2abb11c...`,
   `a1bf2426...`, the accepted IE2E-007 record, and all three runtime-fix commits
   as ancestors. Require a clean worktree and exact remote head.
5. Run `git diff --check`, parse every JSON artifact, resolve every referenced
   object, rerun CI/qualification, and perform a full-repository secret/SAST/SCA
   gate if repository policy requires it. The committed 70-check evidence scan
   is not a substitute for broader policy tooling.
6. Review `origin/main...final-candidate` as a whole. Confirm no `.env`, DPAPI
   blob, registry content, database/WAL/SHM, queue, runtime, prompt, log, PID, or
   workspace artifact is present.
7. Open a protected PR from the final candidate to `main`; do not push directly
   to `main`. Require repository-owner, API, and evidence approvals plus green
   branch protection checks.
8. Immediately before merge, re-fetch and compare immutable SHAs again. Create
   a normal ancestry-preserving, non-squash merge commit even if a fast-forward
   is possible. Record that merge SHA for the rollback procedure. Never squash,
   rebase, fast-forward, or force-push this evidence chain into `main`.
9. After source merge, verify that the merged tree equals the approved candidate
   tree and CI remains green. Do not restart or deploy 18025/18026, migrate
   state, clean staging, or change any route as part of this source merge.

## Risks and explicit limits

- The PR is a 110-plus-commit cumulative stack over the pinned `main`, so scope
  and dependency review must cover far more than the inbound proposal files.
- The full generic `tests.test_staging_isolation` run is 16/18 on this host:
  `test_environment_example_normalizes_to_the_frozen_staging_config` and
  `test_mutable_roots_are_dedicated_state_base_children` raise
  `ValueError: managed path contains a filesystem alias`. This pre-existing
  generic `.env.staging.example` alias does not affect any of the three profiles
  used here, all of which pass, or the fresh E2E. Keep it visible as follow-up.
- Child range 18500-18599 is also named by an older inactive 18029 profile.
  There were no listeners or process collision; reserve 18600+ next time.
- Disposable 18030/18031 processes and state are test evidence, not production
  assets. Any cleanup must use a supported operator lifecycle action, never
  direct file or SQLite deletion.
- Managed non-activation evidence comes from `/api/v1/execution-catalog`, not
  the legacy `/sprints` projection.
- Play/Start was proved only on isolated loopback with tunnel, webhook, and
  public exposure disabled. This plan authorizes no production exposure.
- No code scan can prove absence of every secret class. Preserve protected
  credential stores and run organization-required scanners before merge.

## Rollback plan

Before merge, any SHA, ancestry, diff, CI, review, or policy mismatch means
abort; no rollback is needed because `main` has not changed.

After a source merge but before any separately approved deployment:

1. Freeze further merges and record the old main SHA, candidate SHA, merge SHA,
   tree, PR, reviews, and failing evidence.
2. Create a normal reviewed revert PR for the merge commit. Do not reset,
   rewrite, force-push, or manually remove individual files.
3. Run the same qualification and security gates on the revert head, merge it
   through branch protection, and verify the restored main tree.
4. Preserve both candidate and failure evidence for diagnosis.

Deployment rollback is deliberately out of scope. A future deployment requires
its own cold backup, drain, configuration, health, and lossless state plan. Do
not copy disposable staging roots or managed SQLite into live. Once any
post-cutover write exists, blind snapshot restore is forbidden without a
separately reviewed reconciliation plan.

## Stale-review maintenance issue

Open a follow-up issue titled:

> Managed observability reports superseded sibling review as active/current after REJECT

Evidence is the completed sprint
`msv1-73283eef635f07783771ac13564a74ec55bd191a97a7095bf7cb5b5412e8b6d3`
at revision 46. `review-ac278d22d5bcf84267deb398` belongs to rejected result
`result-17557e1a742ee66f1170b4a1723e2d06f1a237b80d7517dfd91ab9d3620154ee`
from `assignment-1855d5fb357db908a1823954@291e9e053bd0fe39d69357b3df5ac5fa25f484cd`.
Its sibling `review-0fcad4623491609877fef3b8` rejected the result; rework
`assignment-2409927cf5f23eb92e2d2ba9@8ce4519941f3aeab2dfcc6e600679b076bca16d9`
was accepted and downstream qualification passed at `02d16fdf...`. The stale
review has no live queue reference or attention gate and is excluded from
accepted evidence.

Root cause: `ManagedContinuityRuntime._submit_review_locked()` settles the
deciding review and creates rework on REJECT but does not terminalize sibling
reviews for the same result. Observability then treats a raw pending/active
review status as current even though `_binding_is_live()` correctly denies
execution authority.

Minimum supported fix:

- atomically mark sibling reviews for the rejected result `superseded`;
- retain `superseded_by`, replacement result/assignment IDs, and timestamp;
- safely settle undelivered outbox records;
- compute observability currentness from a live/open journal binding rather
  than raw review status alone;
- provide a supported historical reconciliation path, never a manual state or
  SQLite edit.

Regression tests must cover sibling terminalization, absence of phone binding
and current projection, retained audit history, exactly one rework assignment,
stable late/replayed sibling decisions, restart idempotency, and no active
review in completed sprint observability.

## Final boundary

The completed output of IE2E-007 is this plan and its machine-readable evidence
index. `main` remains unchanged. No PR merge, deployment, service restart,
start-from-git, sprint import, runtime/state edit, staging cleanup, or result
outside the managed IE2E-007 flow is authorized by this document.
