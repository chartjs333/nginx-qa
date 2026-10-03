# Universal Managed Sprint Engine release candidate

## Decision

The software qualification result is **PASS** for runtime source
`1504f6fe27fe1ebc117afbc3b56f349978c29556` (tree
`c994b07dc2d194326bd3bf7c0469578cfcc78171`). Live promotion is **not
authorized by this document**. It remains a separate, named operator action
after both UMSE-009 reviewers approve and the sequential graph reaches its
terminal `completed` state.

The release-record commit is a documentation-only child of the qualified
runtime SHA. Reviewers must prove that it changes only this report, its JSON
evidence index, and the qualified-environment record. The executable/runtime
source remains exactly `1504f6fe...`.

Machine-readable evidence is in
`docs/universal-managed-sprint-release-evidence.json`. The qualified Windows
package set is in `docs/universal-managed-sprint-qualified-environment.txt`.

## Candidate identity

| Item | Value |
|---|---|
| Repository | `https://github.com/chartjs333/nginx-qa.git` |
| Live source baseline | `agent/groups-cycles-graph-ui@3c42efc1701a0297d3563d7d31e66e532b072394` |
| Qualified runtime source | `1504f6fe27fe1ebc117afbc3b56f349978c29556` |
| Runtime tree | `c994b07dc2d194326bd3bf7c0469578cfcc78171` |
| Release-record branch | `agent/umse-09-release-qualification` |
| Diff from live source | 38 commits; 65 files; 77,801 insertions; 19 deletions |
| Runtime/executable files in that diff | 13 |
| Release-record scope | Documentation/evidence only |

Remote heads for the staging, legacy, and release branches were independently
confirmed at `1504f6fe...` before this record was created. PRs #3 through #9
are open, non-draft, and `CLEAN`; they form the accepted stack. The legacy and
release qualification branches intentionally began as no-delta branches.

## Accepted graph chain

| Node | Accepted SHA | PR | Architecture review | Evidence review |
|---|---|---|---|---|
| architecture-contract | `274385c9ad5adf57826483e91c6ec3673c53a9f5` | #3 | `8e39b356-d46c-4ece-a4c6-39a199233f2d` | `b708a334-06e7-418f-bfd3-45630c018968` |
| workspace-core | `8d1260a880fbfd63d50fc6f004db3582dc9e3a4c` | #4 | `a316afc0-bd16-4d02-942d-bfa8d32d5bbb` | `b8b23ead-475b-4cb3-92e5-4cde687803a4` |
| sprint-type-dispatch | `643b669d5a4d7fce5d136e636d42a499abcea526` | #5 | `31a49ee2-392e-4cda-9081-291d2f876355` | `e6cb776d-dbc3-4fd4-96d4-11ba5e11cc76` |
| transactional-import | `ce23c90ca8ffdc36b41f7dbcf727d6fdc5f36107` | #6 | `0e67be0a-6854-4f00-a773-24be98b49925` | `a3e16951-ce1a-48be-8c3d-90dc0daf8bec` |
| concurrent-runtime | `e213cbaa1392de44dcc8cb1bda7c7574e1fe147d` | #7 | `e3a74a82-53f9-443d-9175-f0e2e9c34889` | `10072e9d-ab11-4ee7-9437-017c58182016` |
| continuity-runtime | `ec593fe552aa6a7f56fa550801830fdbb84c8ee0` | #8 | `f03453b9-0f35-41c8-90e1-722a554cc59b` | `7e80253d-e6a3-47e0-bee6-b2560318976b` |
| staging-qualification | `1504f6fe27fe1ebc117afbc3b56f349978c29556` | #9 | `6ee65e5c-1ec0-42a4-a427-8d007236801c` | `77aa9867-dea5-4e29-9edb-508e80adf9ae` |
| legacy-regression | `1504f6fe27fe1ebc117afbc3b56f349978c29556` | no delta | `e8cc42c1-3f4c-4633-a588-43aef5248b97` | `313625cb-6345-4709-8289-d3352b680b4f` |

The graph records exactly eight rejected visits. Each actionable finding was
closed by a later commit and then approved twice. The closed findings covered
prospective transition-token validity, crash-atomic repository binding,
single-active import fencing, Windows EOL reproducibility, v1 migration
compatibility, per-process v2 strictness, parallel rework-limit settlement,
continuity fingerprints/recovery, and staging live-read/cleanup/resume/path
boundaries.

## Acceptance evidence

The final clean serialized run executed all 587 collected tests plus 260
subtests in 2,020.49 seconds with zero failures and zero skips. Complete shard
accounting is 201 legacy/UI/Telegram/queue/type + 81 continuity + 69 process
supervisor + 62 staging + 33 branch/Git/safety + 20 workspaces + 10 port leases,
108 managed import, and 3 pytest-only Windows launcher, totaling 587.

The legacy comparison uses `8d1260a...` as the canonical pre-dispatch baseline.
All 50 historical actor-import tests remain; none were removed, and seven
dispatch tests were added. An absent `sprint_type` remains absent in persisted
and exported representations and follows the legacy path; explicit
`legacy_v1` follows the same path while retaining its explicit provenance.

The exact-source staging artifact is:

```text
C:\nginx-qa-staging-state\umse-007\evidence\umse-007-20261003T195221287Z-1504f6fe.json
SHA-256 842f964f419e6ba7ede563e00ba035f9e0c5be1afd313b9eb06abcd8024b9941
```

It records `status=passed`, cold HTTP 201, idempotent replay, and a clean host
restart from PID 51704 to PID 37176. Four child process/workspace/Windows Job
identities survived unchanged on unique ports 18100–18103. Authenticated cleanup
left four `STOPPED` processes, four released leases, and no staging listener.
All twelve host/child log files have recorded hashes and contain no
error/traceback/exception/critical/warning lines.

The external live snapshots and the current read-only recheck agree: listener
PID 47304, start time `2026-10-02T10:01:50.6897592Z`, HTTP 200, branch
`agent/groups-cycles-graph-ui`, commit `3c42efc...`, and a clean worktree. No
live stop, restart, deployment, or state read was performed by qualification.

## Security and migration notes

- High-confidence credential-shape scanning found no candidate value outside
  tests and no value in the staging evidence/log set. Five GitHub-token-shaped
  values are deliberate negative fixtures in two test modules.
- Managed credentials are references, not persisted literals. Redaction occurs
  before durable state, logs, or evidence.
- Git roots, remotes, branch ownership, dirty/diverged worktrees, path
  canonicalization, reparse points, ADS, UNC/device names, process birth
  identity, Windows Jobs, and loopback health are fail-closed.
- Existing legacy records are not eagerly migrated. Managed runtime v1 records
  retain their frozen semantics when upgraded; unmarked v2 records remain
  strict per process.
- `FILE-MANIFEST.json` is the protected original-spec snapshot, not an RC
  manifest. Its intentionally unchanged `staging-isolation.md` entry predates
  downstream qualification edits. RC identity is the exact Git tree plus the
  staging manifest Git-blob hash and evidence hash in the JSON evidence index.
- Runtime requirements contain ranges. The qualified staging environment was
  Python 3.12.1 with a 36-package sorted freeze hash of
  `56865f9826e33ae3440c9a6421fbd2bee2f7a07a078d1aa36c7de59dd691f689`.

## Operator-only promotion plan

The following is a runbook for a named operator; it is not permission for the
sprint executor to run it.

### 1. Authorize and pin

1. Require both UMSE-009 `APPROVE` decisions and terminal graph state
   `completed`.
2. Record the named operator, change ticket, maintenance window, rollback owner,
   and user-visible outage plan.
3. Resolve the full remote SHA of
   `refs/heads/agent/umse-09-release-qualification`; require it to equal the
   final SHA recorded by the accepted UMSE-009 result.
4. Verify that its parent chain contains `1504f6fe...`, that
   `3c42efc...` is an ancestor, and that the diff from `1504f6fe...` is limited
   to the three release-record files.
5. Recheck the evidence JSON hash, exact Git tree, PR states, live PID/start
   identity, port 8025 health, and a clean live worktree. Abort on any mismatch.

### 2. Prepare without touching live state

1. Reserve a backup destination outside `D:\nginx-qa`, verify free space, and
   test archive creation and restore on disposable data.
2. Download/build a Windows Python 3.12.1 wheelhouse from the candidate
   `requirements.txt`, constrained by the qualified-environment file. Record
   every wheel hash and perform import/compile smoke tests in a disposable venv.
3. Keep Telegram credentials only in the existing protected `.env`/secret
   store. Never put them in the release checkout, evidence, command history, or
   backup manifest.
4. Do not copy any staging `.env`, `.venv`, SQLite database, runtime state,
   queues, pending files, prompts, logs, PID/lease files, workspaces, or evidence
   into live.
5. Prefer a dark first deployment: leave `NGINX_QA_MANAGED_ROOT` unset so
   legacy starts without creating managed state. Enable managed mode later in a
   separately approved window with dedicated absolute roots outside the source
   checkout.

### 3. Cold backup and source cutover

1. Enter maintenance and prevent new mutating requests.
2. Stop only the live nginx-qa process under the operator's normal service
   control. Confirm PID 47304 and all descendants exited and port 8025 is free.
3. Before changing tracked files, make a cold, checksummed backup of the entire
   live source/state directory and the external prompt archive. It must include
   `.env`, legacy registries/history, agents, pending sprints, attachments,
   evidence/screenshot indexes, `runtime_state` queues/schedules/settings, and
   any configured managed SQLite/root. Store credentials encrypted and access
   controlled. Record archive hash and successfully restore it to a disposable
   path.
4. Preserve the old branch pointer. Fetch the exact approved SHA, verify a
   clean worktree, and switch the live checkout to that SHA in detached mode.
   Do not use force push, `reset --hard`, or clean/remove commands.
5. Replace the runtime venv only with the prebuilt, hash-verified environment;
   retain the old venv intact for rollback. Use an offline wheelhouse and the
   recorded constraints so `run.bat` cannot resolve newer packages during the
   window.
6. Start nginx-qa under the normal operator service control. Do not enable a
   second tunnel/webhook instance or bind 8026.

### 4. Validate and decide

Within the rollback window, require all of the following before reopening
writes:

- exactly one expected owner of port 8025 with a new recorded PID/start time;
- HTTP 200 and `text/html` on `/` plus successful legacy read-only UI/history;
- exact approved Git SHA and clean worktree;
- restored queue/schedule/pending counts equal the cold snapshot;
- no unexpected managed migration, recovery, Coordinator, duplicate lease, or
  process-owner event;
- Telegram webhook/tunnel identity remains the operator-approved one;
- a bounded legacy smoke import on disposable data preserves absent
  `sprint_type` semantics;
- logs contain no startup traceback, invariant failure, secret value, or
  repeated restart.

Only the named operator may declare promotion complete. Preserve the backup,
old venv, evidence, and change log for the retention period.

## Rollback plan

Trigger rollback on failed health, runtime/state invariant failure, unexpected
migration, legacy semantic change, queue/pending count drift, port/process/Job
ownership mismatch, webhook/tunnel duplication, secret exposure, or repeated
process restart.

1. Close writes and stop the candidate process; prove port 8025 and candidate
   descendants are gone.
2. Archive any post-promotion managed root read-only for diagnosis. Do not feed
   managed records to the legacy importer and do not rewrite them as legacy.
3. Switch the source back to the preserved
   `agent/groups-cycles-graph-ui@3c42efc...` pointer without reset/clean.
4. Restore the entire cold pre-promotion state snapshot as one consistency unit,
   including external prompts and the prior venv/config. Do not mix individual
   files from before and after promotion.
5. Start the old service through normal operator control and verify its exact
   SHA/branch, single port owner, new recorded PID/start time, HTTP 200,
   queue/pending counts, webhook identity, and legacy smoke behavior.
6. Keep writes closed and escalate if either source or state verification fails.
   Record the rollback archive hash and incident evidence.

## Known non-blocking debt

- The historical leading-underscore `_test_legacy...` helper was deliberately
  retired when queue-graph semantics changed. It is not collected and fails
  identically on baseline and candidate; the active replacement passes.
- The original spec `FILE-MANIFEST.json` is not a current RC manifest, as noted
  above.
- PRs are intentionally still stacked and unmerged. Final merge/tag strategy is
  an operator/repository-owner decision after the terminal graph gate.

There is no known P0/P1 software blocker. The unresolved hard gate is external:
explicit operator authorization with a verified cold backup and rollback drill.
