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
| Release-record review | PR #10; exact accepted head is bound by the UMSE-009 transition |
| Qualified runtime diff from live source | 38 commits; 65 files; 77,801 insertions; 19 deletions |
| Runtime/executable files in that diff | 13 |
| Release-record scope | Documentation/evidence only |

Remote heads for the staging and legacy branches were independently confirmed
at `1504f6fe...`. The release branch is now a documentation-only descendant of
that runtime and is reviewed in PR #10. An accepted UMSE-009 result must bind
`from_commit=1504f6fe...` and `git_commit` to the exact final remote release
head; its diff from the qualified runtime must contain exactly the three
release-record files and zero runtime or protected-file changes. PRs #3 through
#10 are open, non-draft, and `CLEAN` at the time of this record.

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

The graph records nine rework cycles: eight implementation/staging findings
closed by later commits and two approvals, plus one UMSE-009 release-provenance
rework. The ninth occurred because a PASS result named runtime `1504f6fe...`
after the release branch had advanced to documentation commit `8ba0be7c...`.
This hardened documentation descendant is not accepted until it is submitted
at its exact remote head and approved by both UMSE-009 reviewers. The earlier
closed findings covered prospective transition-token validity, crash-atomic
repository binding, single-active import fencing, Windows EOL reproducibility,
v1 migration compatibility, per-process v2 strictness, parallel rework-limit
settlement, continuity fingerprints/recovery, and staging
live-read/cleanup/resume/path boundaries.

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
- `gitleaks`, `trufflehog`, and `pip-audit` were unavailable in the qualification
  environment. The credential-shape scan is targeted evidence only: no formal
  secret-scanning, SAST, or SCA claim is made. Promotion requires exact-candidate
  reports from approved tools or explicit named security-risk acceptance.
- Managed credentials are references, not persisted literals. Redaction occurs
  before durable state, logs, or evidence.
- The application has no in-app caller authentication on mutating managed
  routes, including `POST /api/v1/projects/{project_id}/sprints/start-from-git`
  and `POST /api/v1/sprints/{sprint_id}/repair`. Production exposure is
  **NO-GO** until a tested authenticated gateway or ACL (for example mTLS or
  Cloudflare Access) is enforced. Loopback binding alone is not authorization.
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
  Promotion requires a fully resolved offline wheelhouse with every wheel hash
  recorded. If its package set differs from that freeze, rerun the full suite
  and exact-source staging qualification before promotion.

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
   final SHA recorded by the accepted UMSE-009 result and PR #10 head.
4. Verify that its parent chain contains `1504f6fe...`, that
   `3c42efc...` is an ancestor, and that the diff from `1504f6fe...` is limited
   to the three release-record files.
5. Recheck the evidence JSON hash, exact Git tree, PR states, live PID/start
   identity, port 8025 health, and a clean live worktree. Abort on any mismatch.
6. Obtain explicit production service/runbook approval and approve the stable
   route-cutover and authenticated-gateway configuration before the window.

### 2. Prepare without touching live state

1. Prepare a fresh checkout such as
   `D:\nginx-qa-release\umse-<short-sha>`, detached at the exact accepted
   release SHA. Do not change the existing live checkout or venv.
2. Reserve a backup destination outside both checkouts, verify free space, and
   test archive creation and restore on disposable data.
3. Build a fresh Windows Python 3.12.1 venv offline from the approved,
   hash-verified wheelhouse. Record every wheel hash and run import/compile
   smoke tests. Requalify the exact package set if it differs from the recorded
   36-package freeze.
4. Keep Telegram credentials only in the protected secret store. Never put
   them in the release checkout, evidence, command history, or backup manifest.
5. Do not copy any staging `.env`, `.venv`, SQLite database, runtime state,
   queues, pending files, prompts, logs, PID/lease files, workspaces, or evidence
   into production.
6. Define an approved production service with a unique instance identity,
   isolated absolute state/runtime/prompt/managed roots, loopback candidate port
   `18025` (or another approved non-live port), and a separately approved child
   range such as `18100-18199`. Protected roots must cover old live, development,
   and staging paths.
7. Do not use the current `run.bat` for a side-by-side launch: it binds 8025,
   may resolve dependencies online, and can start a tunnel. Keep Telegram and
   tunnel startup disabled until deliberately approved.
8. Prefer a dark legacy-compatible start with `NGINX_QA_MANAGED_ROOT` unset.
   Enable managed mode only in a separately approved window with its isolated
   production roots and authenticated control plane.

### 3. Freeze, back up, and start green

1. Enter maintenance, close mutating ingress at the authenticated gateway, and
   prove that writes are frozen.
2. Stop only the old live service through its real supervisor. Confirm its
   recorded PID and descendants exited and port 8025 is free. Never run old and
   green instances concurrently against the same mutable state.
3. Make a cold, checksummed backup of the complete live consistency unit. It
   includes protected configuration/secret references, legacy registries and
   history, agents, pending sprints, attachments, evidence/screenshot indexes,
   `runtime_state` queues/schedules/settings, external prompts, and any managed
   SQLite/root. Preserve ACLs, encrypt credentials, record the archive hash, and
   successfully restore the archive to a disposable path.
4. Restore only the approved live mutable artifacts into the isolated green
   production roots. Never restore staging artifacts and never modify the old
   checkout, old venv, or old service definition.
5. Launch the exact detached SHA under the approved service manager with the
   fresh venv, for example `<venv>\Scripts\python.exe -B -m uvicorn main:app
   --host 127.0.0.1 --port 18025`. Keep the candidate loopback-only and dark.

### 4. Validate and cut over

Before reopening writes, require all of the following:

- exactly one expected green owner of the approved internal port with a newly
  recorded PID/start time and no owner of the old live port;
- HTTP 200 and `text/html` on `/` through the authenticated gateway plus
  successful legacy read-only UI/history;
- exact accepted Git SHA, expected tree, clean fresh checkout, approved service
  definition, and hash-verified environment;
- restored queue/schedule/pending counts equal the cold snapshot;
- no unexpected managed migration, recovery, Coordinator, duplicate lease, or
  process-owner event;
- a bounded legacy smoke import on disposable data preserves absent
  `sprint_type` semantics;
- if managed mode is approved, a new isolated managed test project starts and
  is drained through the authenticated supervisor, leaving no owned process,
  Job, listener, recovery task, or durable lease;
- logs contain no startup traceback, invariant failure, secret value, or
  repeated restart.

Only after those checks may the named operator move the stable route from the
old origin to green, verify the public URL/webhook/client identity, observe the
approved smoke window, and unfreeze writes. Keep the old checkout, venv, service
definition, backup, evidence, and change log intact for rollback and retention.

### 5. Explicit NO-GO conditions

Promotion is **NO-GO** if any of these is absent or mismatched:

- both UMSE-009 approvals and terminal graph state;
- exact accepted SHA, PR #10 head, three-file docs-only diff, or evidence hashes;
- tested authenticated gateway/ACL or controlled stable-route cutover;
- approved production service definition and runbook;
- hash-verified qualified wheelhouse, or full requalification of a changed set;
- rehearsed consistent backup/restore and named rollback owner;
- rehearsed authenticated drain of all managed processes and leases;
- isolated ports, roots, credentials, and production instance identity;
- approved formal security reports or named risk acceptance for unavailable
  secret/SAST/SCA scanners; or
- expected unchanged live identity at the start of the operator window.

## Rollback plan

Trigger rollback on failed health, runtime/state invariant failure, unexpected
migration, legacy semantic change, queue/pending count drift, port/process/Job
ownership mismatch, webhook/tunnel duplication, secret exposure, or repeated
process restart.

1. Close writes and route traffic away from green. If managed mode has never
   been activated, stop green through normal service control, prove its internal
   port and descendants are gone, restore the cold snapshot, start the untouched
   old service, validate it, and route back.
2. After any managed activation, keep green available behind the authenticated
   control plane and use the authenticated supervisor path to drain every
   process in `PREPARED`, `STARTING`, `HEALTHY`, or `STOPPING` state. Prove no
   owned PID, Windows Job, child listener, recovery task, reserved/bound port, or
   durable lease remains before stopping the green host.
3. Archive the drained managed roots read-only for diagnosis. Do not feed their
   records to the legacy importer and do not rewrite them as legacy.
4. Restore the entire cold pre-promotion state snapshot as one consistency unit.
   Never mix individual files from before and after promotion.
5. Start the untouched old checkout/venv/service definition at
   `agent/groups-cycles-graph-ui@3c42efc...` through normal operator control.
   Verify exact SHA/branch, single expected port owner, new recorded PID/start
   time, HTTP 200, queue/pending counts, webhook identity, and legacy behavior,
   then move the stable route back.
6. If complete managed drain cannot be proved, abort the legacy rollback and
   recover the exact candidate/config under incident control. Do not force-kill,
   manually release ownership records, reset/clean, or mix state merely to make
   rollback appear complete. Record all archive hashes and incident evidence.

## Known non-blocking debt

- The historical leading-underscore `_test_legacy...` helper was deliberately
  retired when queue-graph semantics changed. It is not collected and fails
  identically on baseline and candidate; the active replacement passes.
- The original spec `FILE-MANIFEST.json` is not a current RC manifest, as noted
  above.
- PRs are intentionally still stacked and unmerged. Final merge/tag strategy is
  an operator/repository-owner decision after the terminal graph gate.
- `gitleaks`, `trufflehog`, and `pip-audit` were unavailable, so formal security
  reports remain an operator gate; the performed targeted scan is not SAST/SCA.
- The release-record commits are unsigned (`git signature status N`); the exact
  accepted SHA, tree, remote advertisement, PR #10 head, and evidence hashes
  must be recorded together.

There is no known P0/P1 software blocker. All unresolved hard gates are external
operator controls enumerated in the NO-GO list; this qualification grants no
permission to deploy, restart live, migrate state, or change routes.
