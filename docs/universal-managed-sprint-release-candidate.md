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
| Release-record PR | `#10` |
| Final release-record identity | Exact `git_commit` accepted by UMSE-009; it must equal the branch and PR #10 heads |
| Diff from live source | 38 commits; 65 files; 77,801 insertions; 19 deletions |
| Runtime/executable files in that diff | 13 |
| Release-record scope | Documentation/evidence only |

Remote heads for the staging, legacy, and release branches were independently
confirmed at `1504f6fe...` before this record was created. PRs #3 through #9
are open, non-draft, and `CLEAN`; they form the accepted stack. The legacy and
release qualification branches intentionally began as no-delta branches. PR
#10 contains only this release record. Because a Git object cannot embed its
own object ID, the final immutable release-record SHA is bound externally by
the accepted UMSE-009 result; the branch head and `refs/pull/10/head` must equal
that SHA at both review and promotion time.

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

The graph records eight rejected implementation visits. Each implementation
finding was closed by a later commit and then approved twice. A ninth rejection
occurred in UMSE-009 because the release branch advanced after the first result;
a tenth rejected the first hardened record because its environment, drain, and
post-write rollback evidence remained incomplete. This release-record rework
addresses both, while closure is bound externally by the two final reviews. The
closed implementation findings covered
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
- Runtime requirements contain ranges. The qualified staging runtime venv was
  Python 3.12.1 with a 36-package sorted freeze hash of
  `56865f9826e33ae3440c9a6421fbd2bee2f7a07a078d1aa36c7de59dd691f689`.
  It did not contain pytest. The final serialized suite used the separate
  `C:\Python312\python.exe` test-runner environment with pytest 9.0.0; the
  release evidence records both environments rather than conflating them.
- The credential scan was a documented high-confidence pattern scan, not a
  formal SAST, SCA, gitleaks, or trufflehog run; those tools were unavailable.
- Managed `start-from-git` and repair operations do not provide application-
  level caller authentication. Public managed activation is therefore a
  **NO-GO** unless a reviewed gateway enforces authentication and authorization
  before requests reach the service.

## Operator-only promotion plan

The following is a runbook for a named operator; it is not permission for the
sprint executor to run it.

### 1. Authorize and pin

1. Require both UMSE-009 `APPROVE` decisions and terminal graph state
   `completed`.
2. Record the named operator, change ticket, maintenance window, rollback owner,
   and user-visible outage plan.
3. Approve this production service/runbook explicitly. Resolve the full remote
   SHA of `refs/heads/agent/umse-09-release-qualification`; require it, the PR
   #10 head, and the final SHA recorded by the accepted UMSE-009 result to be
   identical.
4. Verify that the final release-record SHA descends from runtime SHA
   `1504f6fe...`, that `3c42efc...` is an ancestor, and that the diff from
   `1504f6fe...` is limited to the three release-record files. Executable,
   schema, requirements, test, and protected-orchestration diffs must be empty.
5. Recheck the evidence JSON hash, exact runtime and release-record trees, PR
   states, live PID/start identity, port 8025 health, and a clean live worktree.
   Abort on any mismatch.

### 2. Prepare without touching live state

1. Create a fresh production checkout such as
   `D:\nginx-qa-release\umse-<accepted-sha>` at the exact accepted SHA. Keep
   `D:\nginx-qa` and its current venv untouched. Do not deploy by changing the
   live checkout in place.
2. Reserve a backup destination outside both checkouts, verify free space, and
   test archive creation and restore on disposable data.
3. Treat `universal-managed-sprint-qualified-environment.txt` as a version
   inventory, not as a hash lock. Build two complete Windows Python 3.12.1
   offline artifacts. The first is a pristine runtime wheelhouse containing
   every one of its 36 package lines, including the `jsonschema[format]`
   dependencies used by `FormatChecker`; produce an approved requirements file
   with a SHA-256 hash for every wheel and install it into the release venv with
   `--require-hashes --no-index`. The second is a separately hash-locked test
   harness containing those exact runtime packages plus pytest and all test/dev
   dependencies; run the full suite from that harness. Launch the exact release
   venv for black-box/E2E, import/compile, health, and schema-format probes and
   prove its 36-package freeze is exact. Do not install pytest into or otherwise
   mutate the pristine release venv. A `requirements.txt` install constrained
   by the inventory is insufficient because constraints do not install
   unrequested extras.
4. Keep Telegram credentials only in the existing protected `.env`/secret
   store. Never put them in the release checkout, evidence, command history, or
   backup manifest.
5. Do not copy any staging `.env`, `.venv`, SQLite database, runtime state,
   queues, pending files, prompts, logs, PID/lease files, workspaces, or evidence
   into live.
6. Provision separate production roots for runtime, prompt archive, managed
   state, and backups. For managed mode, approve the complete frozen
   environment before launch: loopback host `127.0.0.1`, an internal HTTP port
   other than 8025/8026 (for example 18025), child ports 18100-18199, the fresh
   checkout as service root, pairwise-disjoint absolute roots, a unique
   production instance ID, protected roots covering old live/dev/staging, and
   Telegram/tunnel disabled unless separately configured.

   A reviewed production configuration must set every managed value explicitly;
   for example, with operator-approved free ports and roots:

   ```text
   NGINX_QA_HTTP_HOST=127.0.0.1
   NGINX_QA_HTTP_PORT=18025
   NGINX_QA_SERVICE_ROOT=D:/nginx-qa-release/umse-<accepted-sha>
   NGINX_QA_PROTECTED_ROOTS=["D:/nginx-qa","D:/nginx-qa-staging","D:/nginx-qa-umse","D:/Prompt"]
   NGINX_QA_RUNTIME_ROOT=C:/nginx-qa-prod-state/umse-v1/runtime
   NGINX_QA_PROMPT_ROOT=C:/nginx-qa-prod-state/umse-v1/prompt
   NGINX_QA_MANAGED_ROOT=C:/nginx-qa-prod-state/umse-v1/managed
   NGINX_QA_GIT_FETCH_TIMEOUT_SECONDS=120
   NGINX_QA_CHILD_PORT_RANGE=18100-18199
   NGINX_QA_INSTANCE_ID=universal-managed-sprint-engine-production-v1
   NGINX_QA_DISABLE_TELEGRAM=1
   NGINX_QA_DISABLE_TUNNEL=1
   ```

   The operator must prove the selected service and child ports are free and do
   not overlap any live, staging, or other managed instance before launch.
7. Require a stable gateway route and authenticated access control such as
   Cloudflare Access, mTLS, or an equivalent IP/user ACL. If it cannot protect
   every managed mutating endpoint, leave `NGINX_QA_MANAGED_ROOT` unset and
   declare managed activation **NO-GO**. Record the exact reviewed gateway
   configuration/hash, allowed principals and routes, the operator-owned cutover
   and route-rollback commands, and a negative test proving an unauthenticated
   managed mutation is denied before it reaches nginx-qa.
8. This RC does not ship a reviewed production drain endpoint or CLI. Managed
   activation remains **NO-GO** until a separate operator drain utility and
   command runbook are implemented, security-reviewed, tested, and requalified.
   That utility must use the frozen config and authenticated ownership checks;
   ad-hoc database edits, internal Python snippets, force-kill, and manual lease
   release are forbidden.

### 3. Cold backup and blue start

1. Enter maintenance and prevent new mutating requests.
2. Stop only the live nginx-qa process under the operator's normal service
   control. Capture its current PID, birth time, executable, command line, owning
   service, Git identity, and listener before the stop and compare them with the
   approved change record; historical PID 47304 is evidence, not future
   authority. Confirm the captured process and descendants exited and port 8025
   is free.
3. Record the old source as an immutable branch/SHA reference; do not mix `.git`
   or tracked source into a mutable-data restore. Create separate, checksummed
   artifacts with explicit destinations for: (a) the allowlisted legacy JSON,
   JSONL, queue, pending, attachment, screenshot, evidence, and `runtime_state`
   data; (b) the external prompt archive; (c) `.env` and other configuration or
   secrets under restrictive ACLs; and (d) the prior venv. If managed production
   state already exists, archive its whole SQLite/WAL/SHM and managed roots as a
   separate read-only unit. Record every archive hash and prove restoration to
   disposable destinations before continuing.
4. Copy only the cold legacy mutable-state consistency set into the fresh blue
   checkout. Keep the original source, state, venv, config, and branch pointer
   untouched for rollback. Never copy staging state or feed managed SQLite to
   the legacy importer.
5. Start the exact accepted SHA and hash-verified venv under a dedicated
   named operator service identity on loopback. Approve and record that service
   manager's exact start, stop, status, and log commands before the window. Its
   process command must resolve to the qualified interpreter and exact checkout;
   for example:

   ```powershell
   & $ReleasePython -B -m uvicorn main:app --host 127.0.0.1 --port 18025
   ```

   Do not use `run.bat`: it binds the legacy port and may resolve ranged
   dependencies. Do not enable a second tunnel or Telegram webhook.
6. With blue still unreachable from the public route, verify listener/service
   ownership, exact SHA, config snapshot, GET `/` = 200, startup logs, read-only
   legacy project/sprint/queue/pending counts, schema format enforcement, and a
   managed and legacy smoke using a new isolated test project/tenant and
   dedicated roots, followed by authenticated cleanup. Prove the test processes
   are STOPPED, their leases and ports are released, their test data is removed
   or archived under the approved retention rule, absent `sprint_type` semantics
   are preserved, and every pre-smoke production count is unchanged.

### 4. Route cutover, validate, and decide

Keep writes frozen. Change only the stable gateway origin from the old service
to blue port 18025, preserving the public URL or explicitly updating approved
clients/webhook configuration. Require all of the following before reopening
writes:

- exactly one expected owner of blue port 18025 with a recorded PID/start time,
  and no unexpected owner of 8025/8026 or any child port;
- HTTP 200 and `text/html` on `/` plus successful legacy read-only UI/history;
- exact approved Git SHA and clean worktree;
- restored queue/schedule/pending counts equal the cold snapshot;
- no unexpected managed migration, recovery, Coordinator, duplicate lease, or
  process-owner event;
- Telegram webhook/tunnel identity remains the operator-approved one;
- read-only legacy UI/history/export probes agree with the pre-cutover snapshot;
- logs contain no startup traceback, invariant failure, secret value, or
  repeated restart.

Do not enable public managed writes until the gateway authorization test and a
full managed drain/rollback rehearsal both pass. A legacy-only canary must keep
`NGINX_QA_MANAGED_ROOT` unset.

Only the named operator may declare promotion complete. Preserve the backup,
old venv, evidence, and change log for the retention period.

The cold-snapshot rollback below is valid only before the first accepted
post-cutover write. Once writes reopen, blindly restoring that snapshot is
**NO-GO** because it would discard new queue/history/pending changes. A later
rollback requires a separately reviewed, lossless state-reconciliation plan and
proof; otherwise keep or restart the exact candidate and escalate.

## Rollback plan

Trigger rollback on failed health, runtime/state invariant failure, unexpected
migration, legacy semantic change, queue/pending count drift, port/process/Job
ownership mismatch, webhook/tunnel duplication, secret exposure, or repeated
process restart.

1. Freeze and route away writes while keeping the exact blue service/config
   available for an authenticated drain.
2. If managed activation ever occurred, invoke the separately approved drain
   utility to enumerate every durable PREPARED, STARTING, HEALTHY, or STOPPING
   record and execute the authenticated managed supervisor stop path for each
   one. Prove every owned process/Windows Job is gone and every lease/listener
   is released. If that utility was never approved/rehearsed, activation should
   never have proceeded and rollback is **NO-GO**. If any identity or stop check
   fails, abort rollback and keep or restart the exact candidate/config; do not
   improvise an internal call, force-kill a process, or manually release a lease.
3. Only after the drain proof, stop the blue host service gracefully and prove
   its loopback listener is gone. Host shutdown alone is not a child drain.
4. Archive and hash the blue mutable and managed roots read-only for diagnosis.
   Never feed managed records to the legacy importer or rewrite them as legacy.
5. Only if no post-snapshot write was accepted, restore the cold legacy data,
   prompt, config/secret, and venv artifacts to their explicit destinations in
   the untouched old root as one consistency unit. Do not restore `.git`, tracked
   source, or a managed archive into the legacy importer, and do not mix files
   from different snapshots. If any post-snapshot write exists, this step is
   forbidden until a named lossless reconciliation is reviewed and proven.
6. Start the preserved `agent/groups-cycles-graph-ui@3c42efc...` service through
   normal operator control and verify its exact SHA/branch, single port-8025
   owner, new PID/start time, HTTP 200, queue/pending counts, webhook identity,
   and legacy smoke behavior before routing traffic back.
7. Keep writes closed and escalate if source, state, drain, or health verification
   fails. Record the rollback archive hash and incident evidence.

## Known non-blocking debt

- The historical leading-underscore `_test_legacy...` helper was deliberately
  retired when queue-graph semantics changed. It is not collected and fails
  identically on baseline and candidate; the active replacement passes.
- The original spec `FILE-MANIFEST.json` is not a current RC manifest, as noted
  above.
- PRs are intentionally still stacked and unmerged. Final merge/tag strategy is
  an operator/repository-owner decision after the terminal graph gate.
- The 36-package environment record is not a per-wheel hash lock; production
  promotion requires a newly generated, approved, and requalified hashed
  wheelhouse.
- Formal SAST/SCA and automated secret scanners were unavailable. Promotion
  evidence must record their later result or a named risk acceptance.
- The production service definition, authenticated gateway policy, and this
  runbook require named operator/security approval.
- Managed production activation remains NO-GO until a reviewed, executable
  drain utility and exact command runbook exist and pass a full rehearsal.

There is no known P0/P1 software blocker. The unresolved hard gates are external:
explicit operator authorization, authenticated routing, a hash-locked and
requalified environment, and a verified cold backup/drain/rollback drill.
