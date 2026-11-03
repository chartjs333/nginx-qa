# Preparing a managed instance

Telegram launcher intake now publishes its immutable generated manifest topic,
then calls `LauncherOperations.prepare`. This invokes the existing
`TransactionalSprintImporter.prepare`, not another runtime or task scheduler.
An operation ID is a server-generated logical instance ID. Canonical project ID,
derived sprint ID, occurrence/assignment IDs and the existing workspace manager
provide separate state, workspace, result and eventual queue/log namespaces.
The same SQLite control store arbitrates all launcher and older managed starts.

`prepare` stops after verified workspace creation and before resource activation,
assignment publication or queue insertion. Its atomic control commit records
`PREPARED`, immutable launcher ownership, desired activation mode, prepared
artifacts and a no-work receipt. Manual instances display `Waiting`; autonomous
instances display `Prepared` until the subsequent authorized launch step.
No per-instance nginx-qa server is necessary to display this metadata.

Parking releases the short activation fence lease. The long project reservation
remains held. Same-delivery replay returns the same receipt, and startup retry
reconciliation skips intentionally parked imports. `activate_prepared` requires
prior preparation, reacquires a fresh fence and rechecks workspace cleanliness,
branch publication ownership and the current shared resource snapshot. It uses
the original pinned source even if the source branch advances while waiting.
The generated topic must resolve to the exact published commit and raw manifest
digest before import registration. Old start and retry entry points cannot use
a guessed launcher request key to activate a parked instance.

Default `start(project, request)` behavior remains unchanged for existing callers
without a launcher reservation. Existing control records need no backfill; the
additional `PREPARED` contract and evidence requirements apply to new parked
records. An older server binary must not operate the new shared database after
these records exist: upgrade the control-plane readers together.

`cancel_prepared` is an explicit stop operation for a parked instance only. It
validates the no-work runtime and shared resource indexes in one transaction,
records `PREPARATION_STOPPED` in the original import history, and retires only
that operation's project reservation. It never removes workspaces, Git objects,
logs or prior evidence, never marks a graph DONE, and refuses activated instances.
Active-instance stop and executor readiness belong to the launch-mode adapter.
Failures retain their reservation for recovery and project status reports the
actual failed import; a failed import is not silently retried as a new run.

Operator limits:

| Setting | Default | Enforced range and behavior |
| --- | --- | --- |
| `NGINX_QA_LAUNCHER_PREPARED_LIMIT` | 100 | 1–100 held pending/prepared/running reservations across projects in the shared store; checked under its writer transaction |
| `NGINX_QA_LAUNCHER_PREPARED_TTL_SECONDS` | 86400 | 300–604800; deadline frozen when reserved, checked before preparation/start; expiry retains history and reservation until explicit cancellation |

Untrusted JSON cannot override these settings. An expired Waiting instance is
not automatically executed or recursively deleted. No process is allocated by
preparation, so its process and memory consumption is limited to the bounded
import request and Git operations. Execution uses the existing supervisor's
platform-specific process/time/memory controls; this step does not claim an OS
sandbox or memory enforcement for an external agent client. Launcher v1 rejects
uploaded process command definitions. Executor supervision and live readiness
are separate launch gates.

Verification uses disposable repositories and runtime roots, actual Git
publication, actual workspace preparation and the production shared control
store. Cases cover no-work snapshots, repeated and concurrent preparation,
crashes before/after parking, a fresh activation fence, source movement,
changed-topic rejection, expired instances, quota denial, dirty workspaces,
two independent projects, scoped failure and sequential runs after cancellation.
The real implementation sprint and other local servers are not test fixtures.
