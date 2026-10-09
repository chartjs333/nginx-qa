# Versioned scope: isolated preparation and future maintenance plan

Status: implementation preparation only. **No deployment, live migration,
restart, amendment, ACK, handoff, import or graph advance is authorized by this
document.** Execute a maintenance window only after the user approves its exact
release SHA and the final operator checklist.

## 1. Identities and release boundary

| Item | Exact identity |
| --- | --- |
| nginx-qa source base | `d28726bbcbb552dd21f8cda1042f39f2ee2a21ec` |
| Requirements repository/commit | `chartjs333/delta`, `e72e6f88abf81037a2afec408f8d023980607d66` |
| Development checkout | `D:\nginx-qa-scope-v2` |
| Development branch | `codex/scope-decisions-graph-ui` |
| Only possible future service target | `D:\nginx-qa-release\umse-9375675`, port `18025` |
| Protected, excluded service | `D:\nginx-qa`, port `8025` |

Pin the exact final reviewed release commit from the delivery report. Never
deploy a moving branch tip. Do not apply the original v1 runbook's old base
`9375675…`, old Delta scope payload, historical PIDs or one-shot launch guard to
this release. The v1 runbook remains useful as a backup inventory reference,
not as a substitute for these migration rules.

Delta revision 74 is compatibility input, **not** a current-state CAS value.
The retained copy observed on 2026-10-05 has 28 assignments / 18 transition
reviews and assignment `89e70d7d-7b35-43ff-b5c2-cfe96f8526c6`; deployment must
rediscover all of these values. See `SCOPE_V2_COMPATIBILITY_EVIDENCE.json`.

## 2. Read-only preflight before any outage

1. Record both listeners' PID, process start time, executable, parent chain and
   actual working directory. Recheck immediately before stopping anything;
   PIDs are observations, not constants. The 18025 listener must be proven to
   belong to the exact release root. The independently identified 8025 tree
   must remain excluded from every operation.
2. Read `git rev-parse HEAD`, `git status --porcelain=v1` and the actual startup
   configuration in the release root. Preserve its existing modified `run.bat`
   and untracked `QUICKSTART.md` byte-for-byte, with SHA-256 and ACLs. Stop for
   unexpected source changes; do not reset, clean or overwrite them.
3. Compare the installed virtual environment's full `pip freeze --all` against
   the approved dependency pins. This feature requires no dependency upgrade.
   Do not execute a batch launcher that can silently run `pip --upgrade` or
   contact an index. Preserve the venv and verify it again after installation.
4. Under the actual launch identity, verify existing DPAPI `CurrentUser`
   ciphertexts and restricted ACLs, six distinct values of the approved length,
   and successful rereading. Print names and booleans only. Do not regenerate
   existing credentials or create user/system-global environment variables.
   The server receives all six values only in its process environment; role
   clients read their own value at request time. Existing agent processes do
   not inherit a later shell's environment. Admin credentials never go to
   executor prompts or evidence.
5. Read state using `GET /api/v1/projects/9000/state.json` and the authenticated
   `GET /api/v1/projects/9000/sprints/{actual_sprint_id}/scope-amendments/preflight`.
   Repeat the revision read to detect intervening execution. Record sprint,
   assignment, node, phase, occurrence, revision, queues, assignments, results,
   reviews, scope revisions and ACK identities. These observations are not a
   consistent rollback backup. Never use common `whoami`, `/work` or queue GET
   as a supposedly read-only check.
6. Budget disk space for append-only execution checkpoints and their backups.
   This release retains full saved snapshots; it does not prune history or
   promise bounded storage. Check current state size, expected event volume and
   free space before maintenance, and monitor growth after an approved rollout.
   Long-running production storage/performance qualification is not included in
   the isolated functional checks. Do not delete old checkpoints as a workaround.

## 3. Launch gate must be upgraded, not bypassed

The currently installed local `Delta-Operator.ps1 -Action Launch` was built
for the prior rollout: it pins an old commit and refuses persisted
`scope_control`. It is **not a valid v2 launcher**. Direct `run.bat` execution
to bypass that refusal is forbidden.

Before authorizing maintenance, prepare and review a small replacement launch
wrapper outside the service repository, without embedded credentials. It must:

- require the exact approved release SHA, release root, port 18025, frozen state
  SHA-256, hashes of preserved `run.bat`/`QUICKSTART.md`, and dependency pins;
- run the new offline prestart validator against the exact state selected for
  launch and reject unsupported/corrupt scope history;
- load existing DPAPI values under the verified identity, validate ACLs and
  distinctness, set them only on the intended child process, and avoid printing
  command environments, credentials or response headers;
- retain the approved operational settings and disable dependency downloads,
  automatic tunnel/webhook registration, import/recovery and unrequested
  managed-runtime startup; bind to the verified port only;
- start the approved child hidden, record PID/start-time/root, and prevent
  duplicate/restart-wrapper races. Do not use a broad image-name process kill.

Review this wrapper as part of the maintenance release checklist. No installed
launch wrapper was modified by this development task. Browser pairing can use
the separately prepared operator-authorized pairing wrapper; pairing is not
scope approval and must not disclose the administrative token to the browser.

## 4. Freeze all writers and take one coherent backup

After separate outage approval, quiesce every client able to write 18025:
sequential executors, role clients, operator sessions, pollers, scheduled jobs,
Telegram/tunnel ingress and any other integration identified in preflight.
Stop the **18025** auto-restart launcher/supervisor before its server child, then
verify no descendant can restart it and the port stays closed. A client with a
retry loop is still a writer even while the server is down. Verify the 8025
listener and start time are unchanged. If the entire writer set cannot be
accounted for, do not migrate.

Create a fresh timestamped restricted backup outside both repositories. Copy
the complete configured instance state and journal inventory, not just
`port_git_map.json`: agents, sprint/pending registries, conversation log,
runtime queue/scheduler state, presence files, prompt archives/latest pointers,
all explicitly configured external state paths, launcher/configuration,
dependency pins and Git identity. Inventory all present **and absent** paths
and file ACLs. Resolve each path first; any unexplained external writer/path is
a stop condition. Do not include the DPAPI vault, decrypted environment or
credential-bearing `.env` in a normal evidence archive. Credential-bearing
operational configuration, if required for recovery, belongs in a separately
restricted encrypted operator backup, never Git or prompts.

Create a manifest of relative source path, type, byte count, SHA-256, absence
flags and source HEAD. Rehash every copied file and compare source bytes while
still stopped. Test readability of every canonical JSON and restored copy in
an isolated directory. Record the manifest's absolute path and SHA-256. Keep
the backup immutable. The development observation under `runtime_state` is
explicitly **not** this backup.

## 5. Copy-out offline migration, with no semantic change

The v2 runtime reads v1 records without converting them on GET/startup.
Additional revisions on existing v1 state require this explicit envelope
migration. It appends no amendment/ACK, changes no execution revision, and
preserves the complete old amendment, receipt, binding and ACK bytes as JSON
values. `assignment_bindings`/`acknowledgements` remain current lookup caches;
`binding_history` and `acknowledgement_history` retain immutable versions.

After migration, new revisions must use the persisted human workflow. The raw
Git amendment route rejects new schema-2 applications with
`SCOPE_HUMAN_DECISION_REQUIRED`; supplying invented decision IDs is not an
approval path. Exact historical v1 receipts remain replayable. New reviewer
assignments receive the latest explicitly approved reviewer instruction while
retaining the separate, hash-bound `reviewed_source_scope_context` of the exact
result being reviewed. Existing reviewer assignments/decisions are not
retroactively relabelled.

Run the exact approved code and pinned interpreter against the **verified
backup copy**, with a new output path:

```powershell
# Replace these placeholders only from the verified maintenance manifest.
$PinnedPython = 'D:\nginx-qa-release\umse-9375675\.venv\Scripts\python.exe'
$FrozenCopy = '<verified-backup>\port_git_map.json'
$MigratedCopy = '<isolated-stage>\port_git_map.v2.json'
$FrozenHash = '<sha256-of-the-frozen-copy>'
& $PinnedPython -m nginx_qa.scope_control_prestart --state-file $FrozenCopy --project 9000 --require-scope-control present
if ($LASTEXITCODE -ne 0) { throw 'Pre-migration validation failed' }
& $PinnedPython -m nginx_qa.scope_control_migrate --state-file $FrozenCopy --output $MigratedCopy --expected-sha256 $FrozenHash --project 9000 --writers-stopped
if ($LASTEXITCODE -ne 0) { throw 'Migration failed; input remains unchanged' }
& $PinnedPython -m nginx_qa.scope_control_prestart --state-file $MigratedCopy --project 9000 --require-scope-control present
if ($LASTEXITCODE -ne 0) { throw 'Post-migration validation failed' }
```

Run these commands with working directory set to the staged approved checkout,
never an old code directory. `--writers-stopped` is an operator assertion, not
automatic proof of quiescence. The tool refuses an existing output path,
in-place migration and source hash drift. Recheck source bytes after copying.
Diff the two JSON documents: only the governed scope envelope/lineage fields
may change. Verify sprint/assignment/revision, all original ledger entries,
queues, result/review counts, transitions and graph topology are identical.

## 6. Install while stopped; start only after all gates pass

Pin and verify the approved commit object before modifying the service tree.
While stopped, install exactly that tree with the reviewed local launcher and
QUICKSTART retained. Recheck tracked source hashes against the approved commit
and the declared local-file exceptions. Preserve the existing pinned venv.
Atomically replace only the approved canonical state file with the validated
migrated output; keep the exact frozen original and full backup. Do not import
a sprint, clear a queue, replay events, patch a graph pointer or edit runtime
JSON manually. No scope amendment is applied by migration.

Re-run offline compatibility validation and dependency/file-hash gates from
the new tree. Start via the reviewed v2-capable wrapper from section 3, not the
old wrapper or an unguarded batch file. Keep external writers frozen during
post-start read-only checks.

## 7. Verify and release the maintenance freeze

Verify the new 18025 process root, PID/start time, exact SHA, one listener,
credential preflight, healthy HTTP and absence of migration/start errors.
Compare live read-only state and all backup invariants from section 2. The
preflight must report `versioned_v2`, effective revision unchanged, no offline
migration required; the original assignment and ACK must still match exactly.
Read the new `/observability` and `/scope-requests` endpoints and Graph page:
reading must not change queues, execution revision, assignment or ACK.
Existing historical data without a saved old checkpoint must be displayed as
unavailable, not invented from today's state. Confirm the 8025 listener/root/
start time and on-disk hashes remain unchanged.

Record the new SHA, dependency comparison, backup manifest/hash, observed
sprint/assignment/execution revision, assignment/review counts and preflight.
Only then request release of the maintenance freeze. **Do not apply the Delta
scope change, send ACK, handoff or claim another task as a deployment test.**
Those are ordinary later workflow operations requiring their own authorization.

## 8. Rollback boundary

Before migration installation, restoring the old **code plus its matching
frozen v1 state** is permitted only under an approved rollback. If any startup
writes occurred, first freeze all writers again, retain the failed state/logs,
and compare against the backup; never assume that a read-only UI session proves
no background writes. Restore the full consistent inventory, not a mix of
old config and new queues. Use a reviewed launch gate that understands the old
v1 state; the existing pre-amendment-only launcher still cannot be bypassed.

After a v2 envelope was installed but before any newer scope/workflow/execution
record exists, a rollback to v1 is **not** an in-place schema downgrade. It
requires all writers stopped, proof that no accepted post-backup work exists,
and restoration of the complete matching pre-migration backup plus exact old
code. Preserve the discarded v2 copy for audit. If the proof fails, stop and
retain v2-compatible code/state; do not erase accepted events.

After any new request/decision, amendment, ACK or execution event has been
accepted, never start d28726b or any v1-only runtime on v2 state. A rollback that
would erase those facts needs a new explicit recovery decision. The safe
default is a v2-compatible forward fix or separately reviewed export/recovery
that retains the complete lineage. There is no automatic destructive down-
migration or graph rewind in this release.
