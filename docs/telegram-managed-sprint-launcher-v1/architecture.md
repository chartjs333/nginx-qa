# Telegram Managed Sprint Launcher v1: architecture contract

Status: design and transport schema, before runtime implementation.
Baseline: nginx-qa main after PR16, 2482beb33ea55d8e5f8565cda9bd546df9c6bc32.

The user amendment takes precedence over the original specification: one active
Managed Sprint per canonical project. One new-launcher pending/prepared/running
operation also reserves that project's slot. Different existing projects retain
independent execution; aliases are not a way to run another sprint of this project.
Parallel task nodes inside a single managed graph remain supported.

## Existing components and exact extension points

| Concern | Existing implementation | Reuse and missing behavior |
| --- | --- | --- |
| Telegram JSON | main.py actor_payload_from_telegram_update, telegram_document_payload, _telegram_actor_import_for_mode | Reuse Bot API/download/chunk helpers. New authenticated dispatch must precede download. Existing JSON path creates pending, not a full managed launch. |
| Telegram trust | ensure_telegram_sender_allowed, TELEGRAM_WEBHOOK_SECRET | Current allowlists and webhook secret are optional. New launcher requires configured secret plus nonempty user/chat/project grants, before parsing or fetching documents. Existing legacy behavior is preserved. |
| Prompt output | render_sequential_launch_prompt, get_sequential_sprint_prompt, materialize_sequential_launch_prompt | Existing output is an executor launch prompt. Add a separately named sprint-authoring prompt built from installed schemas/contracts, not a copy of the executor prompt. |
| Pending/proposal | create_inbound_pending_proposal_file, claim_managed_pending_proposal_file, settle_managed_pending_proposal_file | Reuse immutable intent/fingerprint/recovery conventions; no new assignment scheduler. Producer permissions exclude start. |
| Managed dispatch | sprint_types.resolve_sprint_type, managed_graph_semantic_issues; managed_import.managed_schema_errors, parse_strict_json_object | Validate inner manifest with these actual contracts. New envelope is opt-in; no reinterpretation of legacy JSON. |
| Canonical Git start | main.execute_managed_project_sprint_start, TransactionalSprintImporter.start/_start_transaction | Public managed import is Git-based and pinned. Never feed a managed manifest into the legacy actor importer. |
| Prepare/activate | TransactionalSprintImporter._preflight/_prepare/_planned_runtime_resources; ManagedImportStore.activate | Today a successful preparation immediately activates. Introduce a durable parked preparation boundary in this importer; do not call activate and then undo queues. |
| Single active project | ManagedImportStore project control and managed_single_active_sprint partial unique index | Preserve the index and add a common reservation guard used by both new and old starts. Locks isolated to another DB cannot enforce this invariant. |
| Workspaces/resources | workspace_manager, branch_leases, port_leases, process_supervisor | Retain ownership/fencing, non-reparse paths, leases and restart proof. Existing process limits are not a security sandbox for arbitrary commands. |
| Execution/reviews | managed_continuity, current per-role whoami, managed result/review schemas | Existing agent clients poll/claim, commit, submit and review. Running a server is not evidence that a model client is connected. Do not introduce an LLM provider or another scheduling loop. |
| UI/scope | scope_api, project state/projection APIs, existing Web UI | Add an authorized instance view and allowlisted gateway routes. Do not expose general legacy administration endpoints through a new public tunnel. |
| Tunnel | cloudflared_quick_tunnel.ps1, cloudflared_public_base_url | Reuse one operator-owned tunnel targeting a narrow gateway. Never automatically install or create a tunnel per instance. |
| Isolation | run_staging.ps1 and staging_host_app.py | Isolated control server with external mutable roots, loopback listener, separate venv and protected live roots. Never copy live state as fixtures. |

Existing Telegram pending links use pending_token in the URL. They are NOT the new
login mechanism. A user's Telegram ID is NOT authenticated just because it appears
in JSON. Empty allowlists are NOT an opt-in grant for the launcher.

## Versioned transport and dispatch

The new schema is schemas/telegram-managed-sprint-launcher-v1.schema.json. The
only top-level fields are kind, schema_version, project_id, autonomous_mode,
manifest. The kind is telegram_managed_sprint_v1, version is 1, project_id is the
canonical 9000–9999 phone from the server registry, and autonomous_mode is a JSON
boolean. The manifest references the installed managed-workspace-v1 schema.
No credentials, operator policy, arbitrary state roots, upstream URL, raw shell
command, source-update permission, or client-selected instance ID are envelope fields.

Dispatch must distinguish the explicit launcher namespace from unrelated legacy
extension fields. Exact supported kind selects the new schema; an unknown version
in the telegram_managed_sprint_* namespace is rejected. Other pre-existing JSON
formats go through their original dispatch. A plain managed manifest without this
envelope retains MANAGED_GIT_START_REQUIRED on the old Telegram route.
A missing flag in legacy JSON preserves old behavior. A missing flag in an
explicit new envelope is invalid. true, false and absent are not interchangeable.

Limits for the new transport: at most 1 MiB UTF-8 input; at most 64 nesting levels,
256 graph nodes and 1024 checksum entries. Bound structural traversal and reject
depth/count excess before recursive schema validation. Reject duplicate keys,
nonfinite numbers, invalid UTF-8, unsupported versions and unknown envelope fields.
Code fences are accepted only through a bounded explicit text adapter; the inner
document remains strict JSON. Text/caption/document precedence is deterministic;
ambiguity is rejected. Reject edited messages and channel posts for v1 launcher
mutations; ordinary private messages and owned callback queries are supported.
Declared document size is only an early bound; actual streamed bytes are bounded.

The envelope is not an authorization token. Validation order:
1. Feature enabled and authenticated Telegram webhook/user/chat.
2. Resolve an already registered canonical project and authorized repository.
3. Parse bounded JSON; validate envelope, inner schema, graph, role/branch/path
   policies and explicit process/secret policy.
4. Read/pin source Git context and verify file checksums, source/ref expectations
   and canonical repository match; no execution workspace is created yet.
5. Atomically claim deduplication and the project reservation, rechecking grants.
6. Provision immutable Git manifest, then invoke the existing importer preparation.
7. Park or activate according to the persisted authorized mode.

No mutation of main, project registry identity or runtime roots is inferred from
the uploaded task. Process declarations are denied by default in launcher v1;
an operator must explicitly allow a fixed executable/template and narrowly scoped
secret references. A filename or a model-authored command is not such a grant.

## Authoring prompt

/sprint_prompt takes a registered project/repository selection and task text.
Use installed schema bytes, contract text/version and digests, graph rules, role
rules, two independent reviewers, Git/source context and security/testing limits.
Escape the user task as data. Include bounded verified source-file/hash entries
so an external model need not invent SHA values. Canonical repository/project
selection is server-owned. Prompt generation is authorized and read-only except
optional bounded prompt archival in this instance's own prompt root.

Provide text using Telegram-safe chunking or a UTF-8 document. Do not send private
source content, local machine paths or credentials to the model. The user chooses
and contacts the external model; a built-in model API/client is outside scope.
Every exported prompt declares its contract digest; intake revalidates against the
currently installed contracts and returns actionable schema-drift errors.
A deterministic generated example is not proof of a real external-model round trip.

## Git provision boundary

Managed start reads an immutable manifest from an allowed remote/ref and separately
pins the source workspace commit. files hashes refer to source Git blob bytes, not
the working tree or the manifest commit. Existing import dispatch/provenance,
source validation, registry identity and exact-head checks remain unchanged.

A valid Telegram upload needs a small trusted provision adapter around the current
Git provider: create a manifest commit on a server-named, dedicated topic ref,
record raw blob digest and intended parent/ref in an immutable intent, and publish
only with operator-granted manifest_publish capability. Never overwrite main,
force-push another owner, accept a caller transport URL, or pretend an uncommitted
local file is remotely fetchable. Reconcile an ambiguous publication by reading
the intended ref and verifying parent/blob/digest; do not create repeated commits.
The adapter has no graph scheduling responsibility.

If publication is not permitted/configured, return PUBLICATION_REQUIRED/BLOCKED
with the saved operation and exact next action; do not report an execution instance
as prepared or active. Fake/local remote repositories are sufficient for automated
adapter tests, but not evidence of a real GitHub publication. Later release PR
publication is a separate capability and must not merge automatically.

## One control store and preparation boundary

Persist launcher ingress metadata, notification intents, reservation ownership and
mode alongside the existing ManagedImportStore transaction boundary. Small new
tables are adapters, not an alternate engine. A UNIQUE reservation on canonical
project_id covers pending/provisioning/prepared/running operations, with owner
operation/attempt ID, user, payload digest and generation. All managed starts,
including pre-existing start-from-git and pending-start APIs, consult this guard.
The operation owning a reservation may advance it; another operation returns
PROJECT_BUSY. An exact same-message retry returns its existing instance/status.

Store update deduplication by bot identity + update_id, with sender/chat/project
and canonical payload fingerprint verified on replay. A changed payload with the
same key returns IDEMPOTENCY_CONFLICT. A newly sent identical document is a new
intent and is subject to the project slot; do not merge unrelated requests by
content hash alone. Authorization is rechecked before returning saved data.

Extend the existing importer with prepare-only and activate-prepared public
operations; default start remains compatible. Park AFTER verified workspace
preparation and BEFORE assignment/outbox/process activation. Persist PREPARED plus
desired activation mode explicitly in the existing attempt contract, with versioned
schema/invariant updates and migration/read compatibility. Never represent Waiting
by an active graph followed by deletion of assignments.

A long-lived manual reservation is distinct from the short activation fencing
lease. Release the short lease when parking; on activation acquire a new fence
for the same immutable attempt and verify source, workspaces, branch ownership
and current resource snapshot again. Preserve original provenance and prepared
workspace ownership. Allocate execution child processes only at activation.
Recovery must skip intentionally parked manual attempts. Auto attempts resume the
same intent; legacy attempts with no launcher metadata retain legacy semantics.

Waiting can display UI data without a child nginx-qa process. The existing engine
provides per-run workspace/state/queue/result ownership; do not copy the entire
service or assign a separate control database per sprint. All outputs are inside
verified owned roots. Never select Madoev EEG roots/ports, follow junctions or
delete unproven ownership. Completion releases the project reservation only after
the engine's durable terminal/cleanup proof, preserving history and evidence.

Quotas are operator limits for total prepared instances across projects, prepared
TTL, process count/memory/time and bounded notification/event storage. If a native
limit cannot be enforced, report that capability instead of claiming enforcement.
Cancellation uses the existing supervisor/leases and preserves result/log history.
Queued external data may be discarded only under explicit retention policy; no
automatic recursive workspace deletion is part of v1.

## Lifecycle projection

These are launcher/API display states, not a second graph scheduler.

| Display | Authoritative evidence | Agent/task activation |
| --- | --- | --- |
| Created | Authorized validated operation and project reservation, before preparation | none |
| Prepared | Immutable Git provenance and importer parked PREPARED receipt | none |
| Waiting | Prepared with manual mode, waiting for authorized explicit Start | none |
| Running | Existing engine ACTIVATE committed; executor adapter accepted/healthy | existing assignments |
| Completed | Existing managed terminal DONE with durable results/reviews | none new |
| Failed | Durable nonrecoverable preparation/runtime failure | no duplicate restart |
| Stopped | Explicit cancel/stop with owned process death and lease proof | none new |

A transient external blocker is separately reported as blocked_reason and retryable
while retaining the last proved lifecycle state; it is not fake Completed.
Legacy start result status=active remains unchanged. For autonomous readiness,
a configured adapter for the existing agent-client loop must acknowledge the
execution request; missing credentials/client yields EXECUTOR_UNAVAILABLE with a
recoverable receipt. Do not install an unapproved model client or mark a plain
HTTP server as a working agent. At-most-once logical activation relies on durable
receipts; the transport is at-least-once, not magically exactly-once delivery.

## Authority matrix

New launcher access is deny-by-default and requires explicit server policy.
Policy is loaded from operator-controlled configuration outside uploaded JSON.
Bot token, webhook secret and Git credentials remain secret references.

| Action | Required grant and checks |
| --- | --- |
| prompt | authenticated allowed user/chat + project read |
| import/prepare | user/chat/project prepare + allowed repository, safe process/secret policy |
| manifest provision | distinct manifest_publish for this project/ref prefix |
| auto start | persisted autonomous policy + current start + executor capability |
| manual Start | current start grant on exact instance; recheck on callback/POST |
| status/UI | current read on exact project/instance, including completed history |
| cancel | explicit stop grant and owned instance |
| source push/PR/deploy | separately authorized capability, never inferred from auto |
| merge | never automatic |

Producer bearer remains create/read/preview only and is never silently promoted
to an operator. The new gateway must not use the legacy localhost bypass as proof
of Telegram identity. The existing loopback-only operator API remains a trusted
local administration boundary and is never forwarded publicly by the new gateway.
Separate reviewer identities submit their own exact result-bound approvals; an
implementer or coordinator cannot satisfy both slots.

## HTTP and Telegram surfaces to add

These are design contracts, not implemented endpoints at this stage.

| Method/path | Behavior |
| --- | --- |
| Telegram /sprint_prompt | authorized installed-contract authoring prompt |
| Existing Telegram webhook adapter | authenticated command/upload/callback dispatch, legacy fallthrough |
| GET /api/v1/launcher/instances/{instance_id} | scoped redacted status, no arbitrary upstream |
| POST /api/v1/launcher/instances/{instance_id}/start | current operator grant, idempotent prepared activation |
| POST /api/v1/launcher/instances/{instance_id}/stop | scoped owned cancellation |
| POST /api/v1/launcher/auth/telegram | verified raw Mini App initData + bounded server launch challenge -> single-use exchange |
| POST /api/v1/launcher/auth/exchange | atomic one-use identity/instance/challenge binding -> scoped session |
| POST /api/v1/launcher/auth/logout | revoke current session |
| GET /launcher/instances/{instance_id} | authorized instance UI; no unprotected general admin proxy |

Prompt preview/status and preparation return no credentials or filesystem roots.
Safe error envelope: detail.error, message, correlation_id, retryable and optional
field; no raw invalid value, traceback, request body or secret. Codes include
LAUNCHER_DISABLED, AUTH_REQUIRED, ACCESS_DENIED, INVALID_JSON, PAYLOAD_TOO_LARGE,
SCHEMA_INVALID, GRAPH_INVALID, GIT_CONTEXT_INVALID, PROJECT_BUSY,
IDEMPOTENCY_CONFLICT, PUBLICATION_REQUIRED, EXECUTOR_UNAVAILABLE,
PREPARATION_FAILED, INSTANCE_NOT_FOUND, TOKEN_EXPIRED and TOKEN_REPLAY.
Reuse existing managed errors where they already express the failure; wrapper
errors must retain the correlation ID and truthful phase.

Three primary buttons: Start, Web UI, Status. callback_data is an opaque handle
within Telegram's 64-byte limit; it is not authority. Validate user/chat/action/
instance ownership on every callback, answer promptly, then perform bounded work.
Notify Waiting/Running/Completed/Failed/Stopped through durable deduplicated
notification intents. Retry notification delivery without retrying activation.

## Telegram login and gateway threat model

Use a Telegram Mini App button in a private bot chat. The URL contains only a
non-secret opaque instance handle, never a persistent token. Unsupported/group/
business modes instruct the user to open private chat rather than exposing origin.
Verify raw initData according to the official Telegram HMAC algorithm using the
correct bot, reject duplicate fields, compare signatures in constant time, parse
authenticated user, and enforce auth_date/future-skew/freshness limits.
initDataUnsafe, query user_id and browser-supplied headers are untrusted.

Use a server-generated launch challenge and per-session authenticated initData
digest record. Default single-use exchange token TTL is 60s, maximum 300s;
cryptographically random, hash-only storage, user+instance+challenge binding.
Consume on POST atomically. Replay, expiry, other identity/instance, parallel
exchange and revoked grants fail closed. Reload uses the established session;
reopening a fresh Telegram launch has a new validated challenge. Replayed signed
initData cannot mint unlimited sessions. No token consumption or task start on GET.

Session cookies are Secure, HttpOnly, SameSite, bounded TTL and server-side scoped.
Cookie Path alone is not authorization. Check current ACL on reads, writes, files,
SSE and WebSockets; enforce CSRF/origin on mutations and WS handshake, revoke on
logout/operator change. Rate-limit auth and callbacks. Set no-store and restrictive
Referrer-Policy, redact secrets/token bodies from access/application logs.
A public tunnel URL grants no access. Strip spoofed identity/forwarding headers.

Use one existing operator-owned Cloudflare tunnel to the narrow gateway. Gateway
upstreams come only from server-owned registry/lease evidence, on loopback, with
allowlisted routes. Reject arbitrary upstream URL/port/host/path traversal and
open redirects. Unauthenticated API/admin/health/file/WS routes cannot fall through
to the legacy app. Test failures/reconnect without stopping unrelated engine work.
Bot token is available only to the trusted auth adapter, never to task agents.

## Acceptance, stages and release

acceptance-matrix.json preserves the original 16 scenarios with scenario 08
replaced by a same-project concurrency rejection test. Automated tests use temporary
roots and local Git remotes/fake external transports. Only the existing engine
handles assignment/result/review/rework/recovery.

Implementation order: architecture -> prompt -> intake -> preparation -> activation
-> buttons -> gateway -> QA -> release docs. Dependency order deliberately puts
safe preparation before launch modes. Every task result requires two independent
reviews of its exact commit and bounded rework. Required outputs: code/contracts,
architecture/threat model, automated evidence, reviewer reports, setup/rollback,
Telegram guide, real external demonstration and explicitly authorized PR.

Real external-AI output, actual mobile Mini App login through an authorized tunnel
and a real PR URL must remain NOT_RUN/BLOCKED until obtained. Fixtures cannot prove
those facts. Missing bot/domain/tunnel/executor/publication configuration is an
operator setup dependency, not license to borrow live credentials or weaken gates.
Feature is disabled by default; enable only for explicit allowed users/projects.
Rollback disables new dispatch and new starts while preserving existing running
instances, parked receipts and evidence. Never auto-merge main or delete history.
