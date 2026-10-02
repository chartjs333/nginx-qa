# Universal Managed Sprint Engine — architecture contract v1

Status: **frozen contract for UMSE implementation nodes**

Contract version: `1`

Source specification: `agent/universal-managed-sprint-engine-spec@f30b98d04dcc196f6478e5286a7eeded2a271448`

Implementation base: `agent/groups-cycles-graph-ui@3c42efc1701a0297d3563d7d31e66e532b072394`

This document fixes the compatibility and data boundaries used by the later
workspace, dispatch, import, concurrency, continuity, and qualification nodes.
It does not connect a new runtime to `main.py` and does not authorize live
deployment.

## 1. Frozen dispatch contract

`sprint_type` is an optional, top-level JSON field. It is independent of
`execution.mode`; neither value may be inferred from the other.

| Source JSON | Effective type | Pipeline | Serialization |
|---|---|---|---|
| field absent | `legacy_v1` | exact existing legacy pipeline | keep field absent |
| `"legacy_v1"` | `legacy_v1` | exact existing legacy pipeline | preserve explicit field |
| `"managed_workspace_v1"` | `managed_workspace_v1` | managed substrate | preserve explicit field |
| any other value, including `null` | none | none | reject with `SPRINT_TYPE_UNSUPPORTED` |

The absent and explicit-legacy cases must call the same existing functions,
with the same ordering, defaults, operational responses, queue/archive side
effects, and error behavior. Their only permitted difference is source
provenance: an explicitly supplied `legacy_v1` key remains present in the raw
stored/exported payload, while an absent key remains absent. New managed
validation must never be applied to either payload. In particular, this
contract does not strengthen the existing graph validator for a legacy import.

The pure resolver is `nginx_qa.sprint_types.resolve_sprint_type`. It returns
both the declared value and the effective value so readers can interpret an old
record as legacy without writing `sprint_type` back into that record.

### Required ingress ordering

Every ingress must resolve the type before its first project-state mutation:

1. direct agents/actors import: before `actor_import_options` is allowed to
   reach `project_actor_mutation_transaction`;
2. Telegram/pending staging: before `stage_project_sprint_file` writes a pending
   record;
3. pending start/retry: inspect the already stored `import_payload` before
   `update_pending_sprint_activation_file(..., action="claim")` changes status,
   attempt count, lease, or timestamps;
4. declared sequential graph import: before reviewer bootstrap or first-node
   enqueue;
5. start-from-git: after reading the manifest from its pinned Git commit, but
   before project runtime preparation, activation, workspace creation, lease
   allocation, agent replacement, or enqueue.

A mirror fetch needed to read a Git manifest may update only the isolated
repository cache. It may not alter current project state, and an unsupported
manifest may not create a workspace or assignment.

Repository-cache operations are serialized by one fetch lock per
`mirror_storage_key` (therefore per canonical remote), even when multiple
registry aliases resolve to that mirror. Fetch has a configured process timeout and captures redacted
stdout/stderr as attempt evidence. The bare mirror is never used as a working
tree; concurrent readers address immutable object IDs, and a later fetch may
move remote-tracking refs but may never change the pinned `source_commit` of an
existing sprint or assignment.

Before project-control storage is allowed, start-from-Git CAS-binds
`(canonical_project_id, idempotency_key)` to the first resolved repository and
request fingerprint. Its authoritative receipt is canonical JSON at
`<managed_root>/repository-bindings/<binding_sha256>.json`. A same-directory
fsynced temporary file is published with an atomic no-clobber link before any
mirror creation or fetch; once published, later failures never delete it. A
cross-process request lock serializes discovery/creation. The Git blob behind
`refs/nginx-qa/request-bindings/<binding_sha256>` in exactly one managed bare
mirror is secondary evidence and the migration source for legacy caches. A
present root receipt is always authoritative: malformed, mismatched, duplicate,
or redirected evidence fails closed and never falls back to the mutable
registry alias or to a conflicting Git ref. This root-scoped receipt preserves
the first repository even if the process stops before a mirror exists.

## 2. Versioned models

The dependency-free, import-side-effect-free value objects live in
`nginx_qa/sprint_types.py`:

- `SprintType`: the two public values;
- `SprintPipeline`: internal dispatch target, deliberately separate from
  `execution.mode`;
- `SprintTypeSelection`: declared/effective/pipeline tuple with
  absence-preserving serialization;
- `StartSprintFromGitRequest`: request fields for the Git-start API;
- `SprintProvenance`: the immutable five-part sprint identity;
- `SprintImportPhase`: `VALIDATE`, `PREPARE`, `ACTIVATE`;
- `SprintTypeUnsupported`: transport-neutral error with the stable code
  `SPRINT_TYPE_UNSUPPORTED`.

These models are contracts, not services. They perform no filesystem, Git,
network, queue, process, or persistence operation.

The durable schema fixes the remaining managed records and ownership keys:

| Model | Stable identity / required provenance | Owner |
|---|---|---|
| repository mirror | canonical `repository_id` + normalized remote | Git provider |
| Git assignment | repository, pinned source commit, source ref, assigned branch, existing-branch policy | workspace manager |
| managed workspace | sprint/node/assignment IDs + expected root + actual verified Git root/dir/remote | workspace manager |
| branch lease | canonical mirror key + branch + assignment + read/write mode; alias retained as provenance | branch lease service |
| port lease | network namespace + host + port + assignment/process owner; instance retained as provenance | port lease service |
| process record | process + assignment + workspace + runtime root + PID/group/log/lease references | process supervisor |
| import attempt | attempt + idempotency key + pinned provenance + phase + preflight | transactional importer |
| assignment result | `assignment_id + outcome + result_commit` | continuity runtime |
| node occurrence / transition token | derived occurrence/token IDs + source/target graph revisions | continuity runtime |
| transition entry | result key + monotonic journal state + produced token/occurrence/outbox IDs | continuity runtime |
| integration attempt | ordered parent tokens/results + target revision + immutable merge evidence | workspace manager |
| integration workspace artifact | deterministic integration/signature ID + project/sprint/repository/root/lifecycle proof | workspace manager |
| repair record | sprint + from/to graph revision + idempotency key + future-only patch | Coordinator |
| project activation control | canonical project ID + start idempotency index + fenced activation lease | transactional importer |

Process state is exactly `PREPARED → STARTING → HEALTHY → STOPPING → STOPPED`,
with `FAILED` as the failure sink. Transition journal state advances along
`RESULT_RECEIVED → RESULT_VALIDATED → REVIEWS_PENDING → REVIEWS_ACCEPTED →
TRANSITION_COMMITTED`, and advances once more to `NEXT_ASSIGNMENT_ENQUEUED`
only when its token is actually consumed into an occurrence whose assignment
was durably queued. A terminal token and every available token awaiting a
deterministic scheduler or join step remain correctly settled at
`TRANSITION_COMMITTED`. Durable command/environment snapshots contain redacted
values and credential references, never secrets.

Other durable lifecycles are monotonic: assignment `prepared → active →
reviews_pending → completed` (with `blocked`/`failed` terminal exits before
completion); review assignment `prepared → active → decided`; branch lease
`active → released`; port lease `reserved → bound → released` (or reserved
directly to released); and outbox `pending → delivered`. Replaying the current
state is idempotent. Completed/failed/blocked assignments and decided reviews
never reactivate; a rework is a new assignment. Branch ownership releases only
after the assignment is settled and every owned process is confirmed stopped;
port release additionally requires confirmed process-group exit.
Every process-configured assignment has one linear attempt chain: exactly one
`restart_attempt=0` root, at most one child per failed attempt, contiguous
attempt numbers, and at most one live process. Every historical process keeps
reciprocal assignment/workspace/port ownership even after it stops or fails.

Every graph revision stores a complete validated managed-manifest snapshot and
the SHA-256 of its canonical compact UTF-8 JSON bytes. Revision 1 is the source
manifest; a repair materializes a new full snapshot while retaining all older
snapshots. Durable `workflow` is a typed execution projection containing its
graph revision, execution mode, immutable node occurrences, and consumable
transition tokens. Recovery therefore never depends on
reapplying a repair patch or refetching a mutable Git ref.

Canonical JSON in this contract means `json.dumps(value, sort_keys=True,
ensure_ascii=False, allow_nan=False, separators=(",", ":"))`, followed by
strict UTF-8 encoding, with no Unicode normalization. JSON parsing rejects
duplicate object keys and non-finite numbers. This exact v1 encoding is used
for graph-definition, lease-snapshot, result-request, review-request, and
repair-request fingerprints; `nginx_qa.sprint_types.canonical_json_bytes`
provides the dependency-free reference implementation.

Graph revisions are unique and contiguous from 1. Exactly one stored snapshot
matches top-level `graph_revision`; a non-null workflow carries that same
revision, and every assignment references an existing snapshot. Repair commits
the new snapshot, revision number, workflow projection, and repair record in
one transaction. Older snapshots and every revision used by a completed
assignment are immutable.
Schema validation and semantic graph preflight are rerun for every stored
snapshot, not only revision 1. They recheck start/transition/coordinator
targets, Windows-safe case-folded identity namespaces, reachability and a
terminal path from every reachable task, and exact `join_parent_order`
coverage. A repair may not remove the target of an available token.
For every token already available when repair commits, the source node must
also remain an eligible inbound parent of that target in the candidate graph;
eligibility includes the implicit reserved-outcome edges to the current
Coordinator node. Durable validation applies the same rule to every currently
available token and the current graph, preventing a moved Coordinator route
from stranding accepted `STOP` or `NEED_DECISION` work.
Before repair commits, the candidate is also checked against every unsettled
assignment. A `prepared` or `active` assignment must retain a reachable target
for every frozen allowed outcome; a `reviews_pending` assignment need retain
only its already immutable recorded outcome. Each outcome is resolved from the
assignment's creation-revision node, with creation-revision Coordinator routes
taking precedence for `STOP` and `NEED_DECISION`. The target must exist in the
candidate; if it remains a task, the source node must still be an eligible
explicit or implicit predecessor. A terminal target is allowed because the
result transaction can settle it atomically. The durable checker applies this
rule to live assignments and the current graph. It intentionally does not
infer that an old-revision rework assignment existed during every intervening
repair: rework retains its occurrence revision but may be created later.

`repository_id` is a logical project-registry alias. Its canonical remote is
resolved from that registry using the remote-address subset of the existing
`normalize_project_git_address` rules and never from a caller-supplied checkout
path. Existing nginx-qa `repository_key` remains the canonical remote identity.
The distinct filesystem-safe `mirror_storage_key` is `mirror-` plus lowercase
SHA-256 of the canonical remote's UTF-8 bytes. The mirror and write workspace
paths are deterministic:

```text
<managed_root>/repositories/<mirror_storage_key>.git
<managed_root>/projects/<project_path_segment>/sprints/<sprint_path_segment>/nodes/<node_path_segment>/<assignment_id>/
```

Logical project, sprint, and node IDs remain unchanged in durable state. To
preserve their schema limits without exceeding Git for Windows path limits, an
ID longer than 48 UTF-16 code units uses `<kind>-` plus the first 24 lowercase
hex characters of SHA-256 over its exact UTF-8 bytes as its physical segment.
Shorter IDs remain literal. Runtime invariants derive and verify the same
segment mapping.

Durable state requires `repository_key == canonical_remote`, recomputes the
mirror key from that value, and requires every workspace remote and branch
lease to point back to the same canonical identity. Agreement only between two
mutable child records is insufficient.

Each path segment originating outside the service is an opaque validated ID,
not a raw path. Safety comparisons use resolved, normalized absolute paths (and
case-folded comparison on Windows), reject symlink/junction escape, and compare
whole paths rather than string prefixes.
V1 rejects control/Windows-forbidden characters, colon/ADS or drive spellings,
leading/trailing space aliases, spaces immediately before extension dots,
trailing-dot and DOS 8.3 (`~`) aliases, and reserved device basenames (`CON`,
`NUL`, `CONIN$`, `CONOUT$`, `COM1`…`COM9`, `LPT1`…`LPT9`, including the
Windows-reserved superscript spellings such as `COM¹` and `LPT³`) in every
materialized path segment. Configured
absolute paths use only canonical DOS drive-root syntax; UNC, `\\?\` extended,
and `\\.\` device namespaces are rejected before containment comparison. IDs,
paths, and branch/ref components must also be unique under Windows case-folding;
`Build` and `build` cannot coexist. The reference predicate is
`windows_path_segment_valid`.

The `project_id` used in provenance is the canonical registry project phone/ID
after route aliases are resolved, never the raw `{project_id}` path token. Thus
all aliases of one project produce the same managed sprint identity.

The request `ref` pins the **manifest commit** stored in
`SprintProvenance.commit`. The manifest's `git.source_ref` separately pins the
**workspace source commit** used to create initial workspaces. If
`expected_source_commit` is non-null, resolution must equal it. If it is null,
v1 requires the workspace source commit to equal the manifest commit;
otherwise preflight returns `SOURCE_COMMIT_REQUIRED`. This preserves the
frozen five-part sprint identity while still allowing a distinct workspace
commit when that commit is embedded immutably in the manifest blob.

The managed manifest uses top-level `schema_version: 1`. This field is the
managed **manifest** schema version; it is unrelated to existing
`schema_version` values in nginx-qa runtime storage files.

## 3. Machine-readable schemas

The JSON Schemas have intentionally different scopes:

- `schemas/sprint-dispatch-v1.schema.json` validates only the optional dispatch
  field and permits all existing payload members;
- `schemas/managed-workspace-sprint-v1.schema.json` applies only after dispatch
  selected `managed_workspace_v1` and fixes managed schema version 1, Git
  policy, execution mode, and the node collection;
- `schemas/start-sprint-from-git-v1.schema.json` and
  `schemas/start-sprint-from-git-response-v1.schema.json` freeze the Git-start
  request/response envelopes, while `schemas/repair-sprint-v1.schema.json`
  and `schemas/repair-sprint-response-v1.schema.json` freeze future-only repair;
- `schemas/managed-assignment-result-v1.schema.json`, its response schema, and
  `schemas/managed-review-decision-v1.schema.json` plus its response schema
  freeze durable handoff;
- `schemas/managed-project-control-v1.schema.json` freezes the project-scoped
  activation lease and start-idempotency lookup that exists before a sprint ID;
- `schemas/managed-api-error-v1.schema.json` freezes the direct API error
  envelope (Telegram retains its existing acknowledgement wrapper);
- `schemas/sprint-preflight-report-v1.schema.json` freezes normalized validation
  evidence;
- `schemas/managed-runtime-config-v1.schema.json` maps the environment-backed
  runtime configuration into typed values;
- `schemas/managed-runtime-state-v1.schema.json` preserves the original
  durable-state contract, while `schemas/managed-runtime-state-v2.schema.json`
  keeps the same sprint, workspace, lease, workflow, integration, recovery,
  and repair model and additionally requires each process record to carry its
  OS birth token, durable startup deadline, and terminal reason.

The top-level envelope and explicitly open record objects accept additive
metadata. Safety-critical nested policy objects are closed. A new required
meaning, a changed meaning, or a new safety-critical nested field requires a
new managed schema version, not an in-place reinterpretation of v1.

New activations persist runtime-state version 2. On supervisor startup, a
version-1 state may be migrated to version 2 only when every process is still
`PREPARED` and none of the three version-2 process fields is already present;
the state document and normalized process rows are updated in one SQLite
transaction. A version-1 state containing a started or terminal process, or a
partial/foreign safety-field shape, fails closed instead of being
reinterpreted.

Minimal managed envelope:

```json
{
  "schema_version": 1,
  "sprint_type": "managed_workspace_v1",
  "git_address": "https://github.com/owner/repository.git",
  "git": {
    "source_ref": "refs/heads/main",
    "expected_source_commit": null,
    "assigned_branch": "agent/example",
    "existing_branch_policy": "resume"
  },
  "execution": {
    "mode": "sequential",
    "start_node": "build",
    "required_approvals": 2,
    "max_rework_cycles": 5,
    "reviewers": [
      {"id": "reviewer-a", "name": "Reviewer A", "phone": "2891", "git_branch": "review/a"},
      {"id": "reviewer-b", "name": "Reviewer B", "phone": "2892", "git_branch": "review/b"}
    ]
  },
  "nodes": [
    {
      "id": "build",
      "agent": {"id": "builder", "name": "Builder", "phone": "2861"},
      "tasks": [{"task_id": "BUILD-1", "queue": "worker-all", "message": "Build"}],
      "workspace": {
        "access": "write",
        "process": {
          "command": ["python", "service.py"],
          "cwd": ".",
          "environment": {"APP_MODE": "test"},
          "health_path": "/health",
          "restart_policy": "never",
          "resource_limits": {}
        }
      },
      "transitions": {"DONE": "completed"}
    },
    {
      "id": "continuity",
      "agent": {"id": "coordinator", "name": "Coordinator", "phone": "2860"},
      "tasks": [{"task_id": "COORD-1", "queue": "consultant-all", "message": "Recover"}],
      "workspace": {"access": "read"},
      "activation_policy": "any_parent",
      "transitions": {"RESUME": "build", "BLOCKED_EXTERNAL": "completed"}
    },
    {
      "id": "completed",
      "type": "terminal",
      "status": "DONE",
      "message": "Completed"
    }
  ],
  "coordinator": {
    "node_id": "continuity",
    "routes": {"STOP": "continuity", "NEED_DECISION": "continuity"}
  },
  "files": [
    {
      "path": "service.py",
      "sha256": "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
    },
    {
      "path": "orchestration/README.md",
      "sha256": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
    }
  ]
}
```

`files` contains every source-bound artifact whose content the sprint depends
on, but excludes the manifest itself: the manifest's exact blob hash is already
`manifest_sha256` in sprint provenance. Paths are unique safe relative Git-tree
paths; hashes are lowercase SHA-256 over exact blob bytes at the pinned
workspace source commit (or a repair revision's explicit artifact source
commit).
Before every future assignment is made active, the same file set is rechecked
at that assignment's `initial_head_commit`; the revision artifact source must
be an ancestor and every declared blob must match byte-for-byte. Otherwise the
assignment remains prepared/blocked with `ASSIGNMENT_ARTIFACT_MISMATCH` and is
routed to the Coordinator. A repair-source check alone never authorizes
execution from a different tree.

`workspace.process.cwd` is either `.` or a canonical relative POSIX path with
no empty, dot, dot-dot, backslash, control, or absolute segment. Before process
start, its resolved real path must remain under the already verified workspace
root; symlink or junction escape is `WORKSPACE_ROOT_MISMATCH`.

`workspace_policy` is an optional assertion of immutable safety defaults:
parent repositories are rejected and dirty/diverged workspaces route to the
Coordinator. It cannot relax those defaults. `process_policy`, when omitted,
normalizes to host `127.0.0.1`, health path `/health`, restart policy `never`,
maximum 0 restart attempts, 5-second backoff, and finite effective limits of
3600 wall-clock seconds, 2 GiB memory, 100 percent CPU, and 16 processes. A node
process must supply command/cwd/environment;
its optional health path, restart policy, and resource limits replace the
corresponding top-level defaults as whole values; restart-attempt and backoff
scalars do likewise; resource-limit members overlay the finite defaults rather
than replacing them with an unbounded empty object. Host is never
node-overridable. If the effective policy becomes `on_failure` and no cap was
specified, its cap normalizes to 3; an effective `on_failure` policy requires
1–20 attempts, while `never` requires a zero cap and cannot create a restart
record.

Commands are direct argv execution with no shell. A child inherits only a
platform-minimal, documented non-secret baseline (`PATH`, temp, locale, and on
Windows `SystemRoot`/`WINDIR`/`COMSPEC`/`PATHEXT`), then explicit manifest
environment. Repository, Telegram, tunnel, API, and parent credential variables
are never inherited. The supervisor injects reserved
`NGINX_QA_MANAGED_HOST`, `NGINX_QA_MANAGED_PORT`,
`NGINX_QA_MANAGED_PROCESS_ID`, `NGINX_QA_MANAGED_RUNTIME_ROOT`, and
`NGINX_QA_MANAGED_LAUNCH_NONCE` after allocating ownership; manifest attempts
to set that prefix are rejected. Semantic preflight also rejects NUL in argv or
environment values, empty environment names, names containing `=` or NUL, and
names that collide under case-insensitive Windows comparison. Unsupported
resource enforcement is
`RESOURCE_LIMIT_UNSUPPORTED`, not a silent unbounded launch. V1 trusts the
configured repository code as executable and is not an OS security sandbox.
Health paths must be ASCII HTTP origin-form paths; non-ASCII, spaces,
backslashes, and malformed percent escapes fail semantic preflight, while a
space may be supplied as its canonical `%20` encoding.

The wall-clock resource budget begins at the authenticated OS process creation
time, including any time spent suspended behind the launch gate. The durable
startup deadline is capped by both that creation-time budget and the earlier
launch-intent budget; recovery may shorten a legacy deadline but never refresh
or extend either budget.

Each explicit environment value is either a non-secret string or the closed
object `{"secret_ref": "provider:path"}`. A credential-shaped variable name
(`*_PASSWORD`, `*_TOKEN`, `*_SECRET`, `*_API_KEY`, private-key/auth/cookie
variants) requires a secret reference; a literal with a recognizable bearer,
private-key, provider-token, or JWT shape is rejected even under an innocuous
name. Secret references use the same qualified provider grammar as repository
credential references, and their path is scanned too; prefixing a raw token
with `vault:` does not convert it into a reference. The same literal scan
applies to command arguments. Resolution happens only in process memory
immediately before launch. Durable manifest/process
snapshots retain the reference, never its value, and the closed process record
does not admit a parallel `environment_raw` or equivalent escape hatch.

Process ownership persists a random launch nonce, OS process creation time,
normalized executable/cwd, and Windows Job Object (or equivalent held process
handle) identity. Reconciliation or termination requires all available
components to match and rejects the live service PID/ancestry. A reused PID or
identity mismatch becomes `ORPHAN_PROCESS`; it is never signalled by PID alone.
On Windows, recovered resume rechecks the birth token, executable, and Job
membership through the same pinned process handle used by `NtResumeProcess`;
termination retains one authenticated Job handle from identity/scope validation
through `TerminateJobObject`, so a recycled PID or Job name cannot retarget the
OS side effect.
The durable process record also equals the creation revision's effective
command, explicit environment, resolved in-workspace cwd, health path, restart
policy/backoff/cap, and finite resource limits. Stopped and failed records keep
the same reciprocal ownership proof as live records.

`FAILED` remains a sink for one immutable process-attempt record. A permitted
restart creates a new `PREPARED` process record linked by
`restart_of_process_id` with an incremented attempt number; it never changes a
failed record back to `STARTING`. The old process group must be confirmed dead
before a new port lease is bound. Exhaustion routes to the Coordinator.

Top-level `git.assigned_branch` and `existing_branch_policy` are the defaults
for every write node. A node either omits `workspace.git` and inherits both, or
supplies a complete override containing both values. Read nodes acquire no
write-branch lease. The policies are exact:

- `create`: both local mirror refs and fetched remote refs must be absent, then
  create the branch at the pinned source commit;
- `resume`: create at the pinned source if absent; if present, resume only when
  the pinned source is an ancestor of the existing head, otherwise return
  `BRANCH_DIVERGED` to the Coordinator;
- `reject_if_exists`: any existing local or remote ref is
  `BRANCH_ALREADY_EXISTS`; otherwise create at the pinned source;
- `require_exact_head`: the branch must exist and equal
  `expected_branch_head`; absence or mismatch is `BRANCH_DIVERGED` and never a
  reset.

Source refs are validated with `git check-ref-format` semantics and must be
full `refs/heads/*` or `refs/tags/*` refs that peel to a commit. Assigned
branches are validated with `git check-ref-format --branch` semantics. Schema
regexes are only an early character/length filter. No policy permits reset,
clean, forced checkout, forced update, or force push.
On the Windows target, each ref component additionally passes the same safe
segment/device-name test, and branch lease keys use a case-folded branch name.

Credential references are resolved from the trusted project repository
registry by `repository_id`; neither credentials nor their references are read
from the sprint manifest. Managed production repositories must use a configured
remote transport and may not use the current checkout or a `local:` project
path. Unit tests may inject an isolated local Git provider explicitly.
The durable reference is null or a provider-qualified identifier using one of
`env:`, `keyring:`, `secret-manager:`, `vault:`, or `windows-credential:`;
recognizable bearer, private-key, provider-token, or JWT literals are rejected
even when prefixed to resemble a reference.
If optional manifest `git_address` is present, it is only a provenance
assertion: normalization must equal the registry's canonical remote or
preflight returns `REPOSITORY_IDENTITY_MISMATCH`. It is never used as a fetch
target or credential source. The assertion is re-normalized and compared for
every stored graph revision, including repaired snapshots; recomputing a graph
digest cannot make a different repository assertion valid.

Execution mode remains orthogonal to sprint type:

- `sequential` requires one `start_node` and ACTIVATE creates exactly one
  initial assignment;
- `parallel` requires ordered, unique `start_nodes` and ACTIVATE creates one
  initial assignment for each, atomically and in declared order;
- durable state therefore records `active_assignment_ids` and
  `allowed_outcomes_by_assignment`, never a single global current node;
- a committed accepted transition emits one durable token bound to its source
  occurrence, source graph revision, result key, and target. While available,
  `target_graph_revision` is null. Consumption atomically binds it to the
  current revision and to the created target occurrence; a terminal token binds
  the then-current target revision but is not consumed by an occurrence. A
  non-entry target defaults to
  `activation_policy=all_parents`; one available token from every declared
  inbound parent is consumed atomically to create an occurrence. `any_parent`
  consumes the globally lowest available `(source generation, result key)`
  token and intentionally creates one occurrence per later token;
- an available deterministic token for an `any_parent` target, or for an
  `all_parents` target with exactly one inbound parent, together with its exact
  `TRANSITION_COMMITTED` journal entry, is durable scheduler work. Startup must
  resume that stage and atomically consume the token, create the occurrence and
  assignment, and enqueue it. It is not a stranded branch merely because no
  assignment is active between those two transactions;
- for a multi-parent node, `join_parent_order` is required and must list every
  inbound parent node exactly once. `all_parents` selects the lowest unconsumed
  `(source generation, result key)` token for each parent in that order. Tokens
  are consumed once, so cycles cannot reuse an earlier generation's result;
- occurrence generation is one plus the greatest prior generation for that
  node. Its ID is `occ-` plus v1 canonical-JSON SHA-256 over sprint ID, graph
  revision, node ID, generation, and ordered trigger-token IDs. Token IDs are
  `token-` plus the canonical tuple hash of sprint ID, source occurrence ID,
  result key, and target node. Creation and token consumption are one durable
  transaction, so replay cannot duplicate an occurrence;
- occurrence generations are unique and contiguous per node across graph
  revisions. The relational checker derives inbound parents from the creating
  revision and requires `any_parent` to consume exactly one such token and
  `all_parents` to consume exactly one token per parent in
  `join_parent_order`;
- the sprint reaches a terminal state only when no nonterminal assignment is
  active/eligible, no nonterminal token remains available, and every live
  branch has a durable `status=terminal` token. Terminal nodes do not create a
  synthetic occurrence or assignment.

Terminal node statuses are exactly `DONE`, `BLOCKED_EXTERNAL`, and `FAILED`.
When all live branches settle, runtime status is `failed` if any terminal token
targets `FAILED`, otherwise `blocked` if any targets `BLOCKED_EXTERNAL`, and
otherwise `completed` only when every terminal token targets `DONE`. Every
activated `failed` or `blocked` state requires a bound Coordinator failure
context even when its terminal token already selects that status. A completed
`BLOCK_EXTERNAL` recovery is terminal evidence only when it references that
same context and graph revision, targets the context's failed import attempt or
assignment, and its `parameters.reason_code` exactly equals the context's
stable `reason_code`. For an assignment target, its produced records must also
include a blocker observation bound to that assignment. A pending or failed
recovery is never terminal evidence. A pre-ACTIVATE import failure is the sole
case that may have no workflow or terminal token, but its context must still be
bound to the failed import attempt.

Each assignment result still receives two independent reviews. Parallelism
changes the number of active assignments, not review requirements or branch,
workspace, port, and process ownership.

Managed agent IDs and logical phones are globally unique across task agents,
the Coordinator, and both reviewers; no identity or phone can occupy two roles.
Every task assignment's effective allowed outcomes are its declared transition
keys plus engine-reserved `STOP` and `NEED_DECISION`. Both coordinator routes
must target `coordinator.node_id` and take precedence over node transitions;
preflight rejects any collision that maps a reserved outcome elsewhere.

An assignment permanently records the graph revision that created its
occurrence; a hot repair never rebinds a prepared, active, or reviews-pending
assignment. Its result is validated against that revision's outcomes and
transition target. A rework remains in the same occurrence and therefore keeps
that revision; assignments for newly activated occurrences use the then-current
revision. Before repair, every still-possible frozen outcome is validated
prospectively against the candidate as described above. An available token
retains its source revision and immutable target node ID across repair; repair
preflight rejects removal of any such target. When the token is consumed, its
`target_graph_revision` and the new occurrence are bound to the new current
snapshot, so a repair can change future-node content without changing accepted
history. Token lifecycle is resolved against the current target while it is
available and against `target_graph_revision` after settlement. Consequently,
if repair changes that future target from task to terminal, the scheduler must
atomically settle the token as `terminal` at the repaired revision; leaving it
`available` would be a liveness hole. An available token is valid only while
its immutable source remains an explicit transition predecessor of the current
target or an implicit predecessor of the current Coordinator target.

Initial entry assignments use `source_kind=sprint_source` and the pinned
workspace source commit. A normal successor uses `accepted_result` and the
accepted parent result commit; a rework uses `rework_result` and the rejected
result commit linked by its rework record. For a join, identical accepted
parent commits use that commit. Distinct commits with the default
`join_strategy=require_same_commit` produce `JOIN_SOURCE_DIVERGED` and route to
the Coordinator. `join_strategy=merge_no_ff` may instead create one recorded
non-fast-forward integration commit from parent result keys in declared inbound
edge order; conflict is `JOIN_MERGE_CONFLICT`, preserves the integration
workspace, and routes to the Coordinator. The integration record makes replay
reuse the same commit rather than creating a second merge. Integration attempts
are top-level durable records, created before the merge and independent of an
assignment. `PREPARED`, `CONFLICT`, and `FAILED` therefore remain recoverable
even when no target assignment exists. Only a `COMMITTED` record may be linked
from an assignment by `integration_id`; it binds the target occurrence,
assignment, ordered trigger tokens, parent result/commit tuples, and integration
commit. `integration_id` is `integration-` plus the canonical-JSON SHA-256 of
sprint ID, target graph revision, target node ID, and ordered trigger-token
IDs; that signature is unique. Before Git runs, the record also freezes author
and committer names/emails/timestamps and the exact commit message, so replay
of a write-completed but not-yet-committed attempt reproduces the same object
ID rather than making a second merge.

Join selection is itself durable and deterministic. The target must be an
`all_parents` task, its `join_parent_order` must be an exact permutation of all
inbound parents, and the runtime selects the lowest `(source occurrence
generation, result_key, token_id)` available token for each parent in that
declared order. An integration is valid only for `merge_no_ff`, at least two
different accepted parent commits, that exact token list, and parent tuples
derived from those tokens and immutable result receipts. A complete
same-commit token set is consumed into its occurrence/assignment/outbox in one
transaction and may not remain durably available. A complete divergent
`require_same_commit` set atomically creates exactly one Coordinator context
with `JOIN_SOURCE_DIVERGED`; that context is the durable failure record even
though no integration workspace exists. A complete divergent `merge_no_ff`
set atomically creates exactly one deterministic integration/artifact pair, so
ready tokens cannot be stranded without either record.

For `PREPARED` and for an unresolved `CONFLICT` or `FAILED`, every trigger token
remains `available` with null target revision and consumer, and the integration
has no target occurrence or assignment. A `CONFLICT`/`FAILED` integration
resolved by an exact completed join `APPLY_REPAIR` remains immutable historical
evidence; its original trigger set may later be entirely consumed by one
rechecked successor occurrence. `COMMITTED` atomically consumes every token
into one reciprocal target occurrence at the stored graph revision and binds
the first assignment of that occurrence to the integration commit and ordered
parent result keys. No mixed or partially consumed set is valid, including for
historically resolved evidence.

Durable state has a required top-level `integration_workspaces` collection
separate from assignment `workspaces`. For the canonical integration-signature
object
`{"sprint_id": sprint_id, "target_graph_revision": target_graph_revision,
"target_node_id": target_node_id, "trigger_token_ids": ordered_trigger_token_ids}`,
let `d` be its v1 canonical-JSON SHA-256. The paired IDs are exactly
`integration-<d>` and `integration-workspace-<d>`. Every integration has
exactly one artifact whose `integration_id` and `workspace_artifact_id` are
those paired values, and the integration's `workspace_artifact_id` points back
to it. Duplicate pairs, shared artifacts, and orphan records are invalid.

An integration workspace artifact contains exactly the durable ownership and
verification facts needed to recover the merge:
`workspace_artifact_id`, `integration_id`, `project_id`, `sprint_id`,
`repository_id`, `repository_remote`, `mirror_storage_key`, `expected_root`,
`actual_git_toplevel`, `actual_git_dir`, `base_commit`, `head_commit`,
`artifact_status`, `working_tree_state`, `created_at`, `verified_at`, and
`released_at`. Project and sprint IDs equal the containing runtime identity;
repository ID, canonical remote, and recomputed mirror key equal the containing
repository record. `base_commit` is the first ordered parent's commit. The
expected root is exactly
`<managed_root>/integration-workspaces/<workspace_artifact_id>/`, and a live or
preserved artifact requires its resolved `actual_git_toplevel` to equal that
root, its Git directory to belong to that checkout, and its repository remote
to match the canonical remote. Active and preserved roots are unique under the
same resolved, Windows-case-folded comparison used for assignment workspaces.

The integration and artifact lifecycles map as follows:

| Integration status | Artifact status | Head / working tree | Error |
|---|---|---|---|
| `PREPARED` | `active` | `head_commit=base_commit`; `clean` or `merging` | null |
| `CONFLICT` | `preserved` | `head_commit=base_commit`; `conflicted` | non-empty normalized error |
| `FAILED` | `preserved` | `head_commit=base_commit`; `clean`, `merging`, or `conflicted` | non-empty normalized error |
| `COMMITTED` | `active` or `released` | `head_commit=integration_commit`; active is `clean`, released is `unavailable` | null |

`artifact_status=released` is valid exactly when `released_at` is non-null and
`working_tree_state=unavailable`; active or preserved artifacts have
`released_at=null` and are never unavailable. A committed active artifact may
be released in a later monotonic cleanup transaction, but a prepared,
conflicted, or failed artifact must remain available for recovery.

Every `CONFLICT` or `FAILED` integration has exactly one matching Coordinator
context and therefore, by reciprocal outbox ownership, exactly one Coordinator
enqueue. `PREPARED` and `COMMITTED` integrations have none. The context repeats
the exact join signature, binds its normalized error byte-for-byte to the
integration, uses `JOIN_MERGE_CONFLICT` only for `CONFLICT` and
`JOIN_INTEGRATION_FAILED` only for `FAILED`, and stores the canonical artifact
projection (`workspace_artifact_id`, `integration_id`, expected root, base/head
commits, artifact/tree statuses, and verification timestamp). Join contexts
have no assignment branch/commit, process, port, or reviewer evidence. Missing,
duplicate, or substituted context/evidence is corruption.

While no ordinary assignment is runnable, `status=active` may have an empty
`active_assignment_ids` and outcome map only with explicit durable work. That
witness is either the exact `TRANSITION_COMMITTED` scheduler stage described
above, or a valid current-revision join stage: a `PREPARED` integration, a
`CONFLICT`/`FAILED` integration with its exact context, or a
`require_same_commit` divergence context. Removing the applicable witness makes
the state invalid. Available tokens keep such a sprint non-terminal.

Before any Git mutation, the workspace manager acquires the deterministic
integration/artifact lock, verifies or creates the canonical root, and commits
the paired integration and artifact records atomically with the frozen parent
order, author/committer identities and timestamps, and commit message. Exact
replay reacquires that same lock and reuses the same root and records:
`PREPARED` resumes there, `COMMITTED` returns the stored integration commit,
and `CONFLICT` or `FAILED` returns/routes the stored evidence while preserving
the checkout. Replay never creates a second root or merge commit and never
silently cleans, resets, releases, or replaces a preserved artifact.

A missing or duplicate partner, wrong deterministic ID/root, missing active or
preserved directory, repository/base/head mismatch, or verification failure is
durable integration-workspace corruption. It must stop replay and route the
stored evidence for explicit recovery; it must not be repaired by recreating a
checkout, recomputing a merge, or accepting the current filesystem contents.

`source_commit` records that logical source. `initial_head_commit` records the
actual checkout HEAD after applying branch policy: it equals source for create
and read workspaces, but may be a source-descendant existing head for resume or
the exact required head. Result `from_commit` binds to `initial_head_commit`.

Request validation is strict: `repository_id` matches
`[A-Za-z0-9][A-Za-z0-9._-]{0,95}`, `ref` is a full `refs/heads/*` or
`refs/tags/*` name, and `idempotency_key` is 1–200 characters. `manifest_path`
is a relative POSIX Git-tree path of at most 1024 characters; it is rejected if
it is absolute, contains a backslash, NUL, empty segment, `.` segment, or `..`
segment, or if normalized spelling differs from the submitted spelling.
Credentials are never request members.

The schema validates the envelope; managed preflight additionally applies the
graph rules below.

## 4. API contracts

### 4.1 Existing imports

Existing endpoints and their response bodies remain stable. Dispatch is an
internal prefix step. For missing/`legacy_v1`, control passes to the current
legacy importer with the original payload object and no synthesized keys.

The existing direct, Telegram, pending, and declared-graph ingresses do not
have immutable Git provenance and therefore do not activate an explicitly
managed payload. They return `MANAGED_GIT_START_REQUIRED` before staging,
claiming, archive/history writes, agent replacement, or enqueue, directing the
caller to `start-from-git`. A previously stored managed pending payload is not
claimed. This is a managed dispatch result, not a legacy fallback.

For an unsupported type, the stable detail is:

```json
{
  "detail": {
    "error": "SPRINT_TYPE_UNSUPPORTED",
    "field": "sprint_type",
    "supported": ["legacy_v1", "managed_workspace_v1"],
    "correlation_id": "request-id"
  }
}
```

Direct/core HTTP adapters return `400 Bad Request` using FastAPI's `detail`
envelope; they must return before mutation and must not echo an arbitrary
invalid value into logs or durable evidence. Existing Telegram webhook routes
retain HTTP `200` acknowledgement semantics and put `status_code=400` plus the
same error detail inside their existing `ok=false` response envelope.

### 4.2 Start a managed sprint from Git

```http
POST /api/v1/projects/{project_id}/sprints/start-from-git
Content-Type: application/json
```

```json
{
  "repository_id": "main",
  "ref": "refs/heads/sprint-definition",
  "manifest_path": "orchestration/sprint.json",
  "idempotency_key": "caller-generated-key"
}
```

This endpoint is managed-only. After reading the pinned manifest, an absent or
explicit `legacy_v1` type returns `400 MANAGED_SPRINT_TYPE_REQUIRED` without
delegating to legacy import. Existing direct, pending, and Telegram endpoints
remain the supported legacy ingress. An unknown value returns
`SPRINT_TYPE_UNSUPPORTED`.

Successful first activation returns `201 Created`:

```json
{
  "sprint_id": "msv1-5f55f42e54a6ffccd983edbe38830972dc3b8e57ad82323cdb41f3008faa37f6",
  "status": "active",
  "phase": "ACTIVATE",
  "deduplicated": false,
  "execution_mode": "sequential",
  "identity": {
    "project_id": "project-id",
    "repository_id": "main",
    "commit": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "manifest_path": "orchestration/sprint.json",
    "manifest_sha256": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
  },
  "workspace_source_commit": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "initial_assignment_ids": ["assignment-id"]
}
```

The immutable identity is the ordered tuple:

```text
project_id + repository_id + commit + manifest_path + manifest_sha256
```

The server validates all components first, encodes that ordered tuple as a
compact UTF-8 JSON array (no ASCII escaping and separators `,`/`:`), hashes the
bytes with SHA-256, and uses `msv1-<lowercase hex digest>` as the stable sprint
ID. It stores the five components as well as the derived ID. The ref is
provenance metadata but the resolved commit is identity. Reuse of an
`idempotency_key` with the same request fingerprint returns the original result
with `200 OK` and `deduplicated=true`; reuse with a different fingerprint is
`409 IDEMPOTENCY_KEY_CONFLICT`.

`commit` is the full lowercase object ID returned by Git. `manifest_sha256` is
SHA-256 over the exact manifest blob bytes from that commit, before JSON parsing;
it is not the Git blob SHA-1 used by the sprint specification's
`FILE-MANIFEST.json`. Identity strings are hashed exactly as validated, with no
Unicode normalization. The idempotency request fingerprint is SHA-256 over the
same canonical JSON-array encoding of `[project_id, repository_id, ref,
manifest_path]`; the idempotency key itself is the lookup key and is not an
array component.

The lookup scope is `(canonical_project_id, idempotency_key)`, persisted in
the project-scoped control record before any sprint ID exists. Once the pinned
manifest dispatches to `managed_workspace_v1`, VALIDATE first writes a fenced
intent containing the request fingerprint, attempt ID, phase/status, and the
frozen repository binding, with null sprint provenance and workspace source.
It then fills the five pinned identity fields atomically, followed by the
separately resolved immutable workspace source commit, before PREPARE. A crash
before intent creation resumes from the authoritative root receipt and may
materialize its frozen repository mirror without consulting the current
registry alias. After intent creation it resumes only from the repository
binding embedded in that durable intent. If that later bound mirror is missing,
recovery fails closed instead of fetching another repository. An in-progress
exact replay resumes or observes the same attempt; a successful exact replay
reconstructs a one-entry provider from the durable repository binding,
reconciles any missing publication, and returns its stored response; a changed
request conflicts before activation.
The activation lease carries a
monotonic fencing token from a durable high-water counter so an expired owner
cannot commit after a successor or reuse a token after restart.
Every live start record retains both its immutable `created_fencing_token` and
the current positive `fencing_token` that authorizes mutation after any resume.
A record whose creation fence predates an already successful activation is
durably failed as `ATTEMPT_SUPERSEDED` before it can renew its lease. At the
final ACTIVATE boundary the current fence owner atomically fails every other
nonterminal attempt with a lower current fence, validates that no other active
runtime exists, and only then publishes the new success. A `SUCCEEDED` record's
sprint ID and stored response resolve exactly to `active_sprint_id`; that
pointer remains present through the sprint's valid terminal state until a later
fenced activation atomically replaces it. Among historical successful start
records, it therefore names the success with the greatest fencing token, not
merely any previously successful sprint. A lease, record, counter, runtime
status, and active pointer that do not name a consistent fenced owner are
durable corruption.
SQLite enforces at most one `active` runtime per project with a partial unique
index. Initialization fails closed when legacy duplicates prevent installing
that index; it never marks such a store initialized without the constraint.
Startup restoration reads active rows and their complete project-control
documents in one database snapshot, revalidates both schemas and all relational
invariants, and refuses to restore any inconsistent state.
A failed record retains its normalized error and evidence; an exact caller
replay returns that stored failure with its stored HTTP status without
resolving a newer ref. It is immutable. `RETRY_IMPORT` is an explicit
Coordinator recovery action that creates a new fenced start record with a new
internal recovery idempotency key and attempt ID. That record points to the
failed predecessor through `recovery_of_attempt_id`, increments
`recovery_generation`, and copies the same request fingerprint, pinned identity,
workspace source commit, and sprint ID; it never resolves the mutable ref again.
Each failed recovery may have a further generation. The original caller key
continues to replay its original failure, while a new caller attempt uses a new
caller idempotency key.

Idempotency keys and attempt IDs are unique within a project. A new key that
resolves to an already durable sprint ID never replaces or reactivates it:
active/completed state returns `SPRINT_ALREADY_EXISTS`; preparing/failed state
returns `SPRINT_RECOVERY_REQUIRED` for explicit Coordinator recovery. The pure
`managed_project_control_invariant_issues` checker enforces key/attempt/token
uniqueness, pinned-identity hashes, stored response equality, lease ownership,
and the active-sprint pointer before every project-control commit.

Failures use a normalized body and create no assignment:

```json
{
  "detail": {
    "error": "SPRINT_PREFLIGHT_FAILED",
    "phase": "VALIDATE",
    "correlation_id": "attempt-id",
    "issues": [
      {
        "code": "MANIFEST_CHECKSUM_MISMATCH",
        "path": "files[0]",
        "message": "Manifest file checksum does not match the pinned commit"
      }
    ]
  }
}
```

### 4.3 Future-node repair

```http
POST /api/v1/sprints/{sprint_id}/repair
```

The request contains `expected_revision`, `idempotency_key`, and an additive
replacement for future nodes/prompts/profiles/reviewer metadata/coordinator
routing/checksum metadata. The implementation must reject any patch addressing
a completed node, accepted result or review, assignment identity, or completed
source/result commit with `REPAIR_IMMUTABLE_HISTORY`. A successful repair
increments the graph revision, preserves prior revisions, and returns:

```json
{
  "sprint_id": "msv1-5f55f42e54a6ffccd983edbe38830972dc3b8e57ad82323cdb41f3008faa37f6",
  "from_revision": 3,
  "graph_revision": 4,
  "repair_source_commit": "dddddddddddddddddddddddddddddddddddddddd",
  "deduplicated": false
}
```

Patch application is deterministic against the complete snapshot at
`expected_revision`: `future_nodes` upserts full node definitions by node ID;
`remove_future_node_ids` removes only future IDs; `prompts` maps node IDs to
task-ID/message replacements; `profiles` maps future agent/reviewer IDs to
profile text; `reviewer_metadata`, `coordinator_routing`, and
`checksum_metadata` replace their complete corresponding collections/objects.
Overlapping fields with conflicting values are `REPAIR_PATCH_CONFLICT`, not a
precedence rule. The service materializes a full candidate manifest, runs the
entire managed schema/graph/checksum/immutability preflight, then stores that
snapshot and its canonical digest as the next revision. That preflight also
evaluates the candidate against every outcome still possible for a live pinned
assignment; a missing target is
`REPAIR_LIVE_ASSIGNMENT_TARGET_INVALID`, and a retained task target that no
longer admits the pinned source is
`REPAIR_LIVE_ASSIGNMENT_TARGET_INELIGIBLE`.
`repair_source_commit` is a required full object ID in the canonical mirror;
all replacement checksum metadata is verified from that exact commit, which is
stored as the revision's `artifact_source_commit`. It cannot alter provenance
or commits of completed/active assignments.

Repair idempotency is scoped to `(sprint_id, idempotency_key)`. Its request
fingerprint is the v1 canonical-JSON SHA-256 of `{"sprint_id": sprint_id,
"expected_revision": expected_revision, "repair_source_commit":
repair_source_commit, "patch": patch}`. Exact replay
returns the stored revisions with `200 OK` and `deduplicated=true`; reuse with
a different revision or patch is `409 IDEMPOTENCY_KEY_CONFLICT`. The revision
check and idempotency insert are one transaction.

The Coordinator identity must be distinct from both reviewer identities and
cannot submit a review. It may create an immutable-generation import retry,
retry an existing handoff idempotently, route a node to rework, continue the
same node through a new assignment, apply the future-only repair above, record
recovery, or declare an external block. It
cannot skip reviews, rewrite accepted history, impersonate a reviewer, edit a
repository outside the assignment lease, or perform destructive/forced Git.
Every such action creates an immutable `recoveryRecord` before its outbox side
effect.
Recovery idempotency is scoped to `(sprint_id, coordinator_id,
idempotency_key)`. Its canonical request hash binds context ID, graph revision,
action, the exactly one target (import attempt, assignment, or join signature),
and redacted typed parameters. The record
retains produced repair/rework/assignment/outbox IDs plus response or normalized
error/evidence; exact replay resumes/returns it and changed reuse is
`RECOVERY_CONFLICT`.
Precisely, the hash is the canonical-JSON SHA-256 of
`{"action": action, "assignment_id": assignment_id, "context_id": context_id,
"graph_revision": graph_revision, "import_attempt_id": import_attempt_id,
"join_target": join_target, "parameters": parameters}`. `join_target` is null
for non-join recovery and otherwise repeats target revision/node, ordered token
IDs, and nullable integration ID. A completed record freezes a response containing
the same recovery ID, action, `RECOVERY_COMPLETED` status, and the exact ordered
`produced_record_ids`; every produced ID must resolve to a durable record.
The action-specific parameter shapes are frozen in the runtime schema:
`RETRY_IMPORT` binds failed attempt plus recovery key, `RETRY_HANDOFF` a result
key, `CONTINUE_NODE` a node and source commit, `APPLY_REPAIR` the complete repair
request, `ROUTE_REWORK` a rejected result plus feedback, and `BLOCK_EXTERNAL` a
stable reason plus operator action.
Completed effects are also relationally bound: retry-import creates one next
generation with unchanged pinned provenance; retry-handoff names its receipt
and outbox effect; continue-node creates exactly one matching assignment and
enqueue; route-rework binds its rejected result, feedback, replacement, and
enqueue; apply-repair binds the full request to one repair; and external block
binds the Coordinator context reason and, for assignment failures, its blocker
observation. Completed action receipts reject unrelated record kinds or extra
pre-existing IDs: retry-import contains exactly its new attempt; continue and
rework contain exactly their typed records and enqueue effects; a non-join
repair contains exactly its repair record; assignment external-block contains
exactly its blocker observation; and import external-block contains no
fabricated effect. A pending or failed recovery has no produced IDs and is
never terminal evidence. A pending apply-repair record may temporarily coexist
with its matching durable repair during crash recovery and must resume from
that effect. A terminal `failed` record may not coexist with that published
repair: once the idempotent effect exists, replay must resume/complete rather
than return a contradictory failure.

The retry-import generation's idempotency key is exactly
`parameters.recovery_idempotency_key`; it cannot reuse or silently substitute a
caller key. A completed retry-handoff contains exactly its immutable result key
plus every result-bound review/transition outbox event and no unrelated ID.
Join failures may target only `APPLY_REPAIR`: the preserved evidence and
available tokens are then re-evaluated against the next graph revision. This
narrow repair must preserve the target's `all_parents` policy, inbound-parent
membership and order, and `join_parent_order`; it may change `join_strategy`.
This compatibility check applies as soon as a repair matching the join
recovery request is durable, including while the recovery receipt is still
`pending`; publishing an incompatible graph cannot be deferred until receipt
completion.
The completed recovery receipt freezes the first durable recheck boundary. Its
exact produced-ID set is:

- repair, deterministic integration attempt, and integration-workspace
  artifact when distinct commits are rechecked with `merge_no_ff`;
- repair, new divergence context, and Coordinator enqueue when distinct commits
  are rechecked with `require_same_commit`; or
- repair, target occurrence, its first assignment, and assignment enqueue when
  all selected commits are already equal.

That receipt does not change when the integration later commits or the tokens
are consumed. The original failure integration/context remains immutable
historical evidence and may then reference the same tokens in their settled
state. `BLOCK_EXTERNAL` is limited to a failed import attempt or assignment. V1
does not define a destructive token-settlement effect for a join, so allowing a
join external block would create an impossible terminal state with live
available tokens.

### 4.4 HTTP/error mapping

| Condition | HTTP | Stable error |
|---|---:|---|
| malformed JSON/request/path/ref | 400 | `INVALID_MANAGED_SPRINT_REQUEST` |
| managed payload on an old ingress | 400 | `MANAGED_GIT_START_REQUIRED` |
| fetched manifest is absent/explicit legacy | 400 | `MANAGED_SPRINT_TYPE_REQUIRED` |
| unsupported `sprint_type` or manifest schema | 400 | `SPRINT_TYPE_UNSUPPORTED` / `SPRINT_SCHEMA_UNSUPPORTED` |
| repository alias or pinned ref/object absent | 404 | `REPOSITORY_NOT_FOUND` / `SOURCE_REF_NOT_FOUND` |
| same idempotency key with a different request | 409 | `IDEMPOTENCY_KEY_CONFLICT` |
| another project activation owns the lease | 409 | `PROJECT_ACTIVATION_IN_PROGRESS` |
| a different key resolves to an existing sprint | 409 | `SPRINT_ALREADY_EXISTS` / `SPRINT_RECOVERY_REQUIRED` |
| managed preflight has one or more issues | 409 | `SPRINT_PREFLIGHT_FAILED` |
| prepare failed before activation | 500 | `SPRINT_PREPARE_FAILED` |
| activation/journal commit failed | 500 | `SPRINT_ACTIVATE_FAILED` |
| repair revision is stale | 409 | `GRAPH_REVISION_CONFLICT` |
| repair targets immutable history | 409 | `REPAIR_IMMUTABLE_HISTORY` |
| repair strands a live assignment outcome | 409 | `REPAIR_LIVE_ASSIGNMENT_TARGET_INVALID` / `REPAIR_LIVE_ASSIGNMENT_TARGET_INELIGIBLE` |
| repair patch contains overlapping conflicting values | 409 | `REPAIR_PATCH_CONFLICT` |
| result differs from accepted result request | 409 | `RESULT_CONFLICT` |
| result commit missing or not the verified branch/workspace HEAD | 409 | `RESULT_COMMIT_NOT_FOUND` / `RESULT_HEAD_MISMATCH` |
| result commit does not descend from assignment base | 409 | `RESULT_NOT_DESCENDANT` |
| reviewer reuses its assignment for a different request | 409 | `REVIEW_CONFLICT` |
| Coordinator reuses a recovery key for a different action | 409 | `RECOVERY_CONFLICT` |

Every error includes a request/attempt correlation ID but no credential,
secret, raw environment, or arbitrary invalid value. A `5xx` failure retains
the old active sprint and exposes recovery evidence; it does not report success
for a partially committed activation.

### 4.5 Durable assignment result and review

Managed assignments keep the existing current-assignment endpoint shape:

```http
POST /api/v1/projects/{project_id}/agents/{agent_phone}/whoami
```

The result request is frozen by
`schemas/managed-assignment-result-v1.schema.json`. `status` is the graph
outcome, `from_commit` must equal the assignment's recorded
`initial_head_commit`,
`git_commit` is the durable `result_commit`, and `git_branch` must equal the
leased assigned branch. The result identity is canonical SHA-256 over
`[assignment_id, outcome, result_commit]`, exposed as `result-<digest>`.
For a read-only assignment `git_branch` is JSON `null` and `git_commit` equals
the pinned source commit. The v1 canonical-JSON hash of the complete validated
request is stored separately as its replay fingerprint.
The raw request body is not retained. The durable receipt stores only the
validated fields, the request fingerprint, and a deterministic redacted result
summary; reviewer prompts receive that redacted summary. Redaction runs before
persistence/logging and replaces credential-shaped values and configured secret
tokens without preserving their original length.

Before `RESULT_VALIDATED`, the service verifies the result commit exists in
the canonical mirror/workspace, equals the verified workspace and leased-branch
HEAD, and descends from `initial_head_commit` (equality is required for a
read-only assignment). It rechecks repository/branch/workspace ownership and a
clean tree. Missing, mismatched, rewound/unrelated, or dirty results return
`RESULT_COMMIT_NOT_FOUND`, `RESULT_HEAD_MISMATCH`,
`RESULT_NOT_DESCENDANT`, or `WORKSPACE_DIRTY` and do not begin review.

The first valid receipt returns HTTP `200` with
`status=REVIEWS_PENDING`. An exact replay returns HTTP `200` with the stored
receipt, `status=ALREADY_ACCEPTED`, and `deduplicated=true`, even if a crash
occurred before review/next-assignment enqueue. Reusing the assignment/outcome
with a different result commit is `409 RESULT_CONFLICT`; accepted result fields
are immutable.

Reviewers post to their own current-assignment endpoint using
`schemas/managed-review-decision-v1.schema.json`. To preserve the existing
endpoint shape, the client sends only its current `assignment_id`, `status`,
and optional `feedback`; source assignment, result key, commit, and outcome are
server-owned immutable metadata on that review assignment and are copied into
the durable review record. Client-supplied binding fields are rejected. Only
`APPROVE` and `REJECT` are valid;
`REJECT` requires non-empty actionable feedback. Reviewer identity comes from
the authenticated/current logical reviewer, not request JSON. One reviewer has
one immutable decision per result key and cannot satisfy both approval slots.
The first approval returns `REVIEW_ACCEPTED`; the first rejection returns
`REWORK_ENQUEUED`. An exact canonical-request replay returns
`ALREADY_ACCEPTED` with `deduplicated=true`; a changed request is
`REVIEW_CONFLICT`.
The review request fingerprint is the canonical-JSON SHA-256 of exactly
`{"assignment_id": review_assignment_id, "feedback": feedback,
"status": status}`. Omitted approval feedback normalizes to the empty string;
no server-owned source/result field is part of this client-request hash. The
durable review assignment, decision record, and response must agree exactly on
source assignment, result key/commit/outcome, reviewer identity/index,
decision, normalized feedback, and fingerprint.

Two approvals advance the happy-path journal. A rejection leaves the original
result receipt and review immutable, creates a separate `reworkRecord`, and
atomically prepares a new assignment for the same source node with the feedback.
It never rewrites the journal into `REVIEWS_ACCEPTED`. Instead the journal
remains at `REVIEWS_PENDING`, receives `disposition=reworked` and a durable
`rework_id`; it is a non-advancing sink. Recovery advances `open` entries
through review and `accepted` entries through transition/outbox delivery.

`rework_cycle` is scoped to one node-occurrence lineage: its first assignment
is 0 and each rejected-result replacement increments it atomically. If creating
the next assignment would exceed that graph revision's
`execution.max_rework_cycles`, no assignment/outbox is created; the occurrence
and sprint become blocked with `REWORK_LIMIT_EXCEEDED` and Coordinator evidence.
Rework does not consume a transition token and does not create a second
occurrence: the replacement assignment is appended to the same occurrence's
ordered `assignment_ids` lineage.

Every new assignment, review assignment, Coordinator handoff, or rework enqueue
writes an outbox record in the same durable transaction. Keys are exactly
`enqueue:assignment:<sprint_id>:<assignment_id>`,
`enqueue:review:<sprint_id>:<result_key>:<reviewer_index>`, and
`enqueue:coordinator:<sprint_id>:<context_id>`. The durable queue has a unique
index on that same key and an insert-or-return-existing operation that also
compares the canonical payload; a mismatched reuse is corruption, never a
second queue item. Startup/retry scans incomplete journal stages and pending
outbox records, completes the next monotonic stage, and marks an enqueue
delivered only after storing the queue's immutable receipt ID. A crash after
queue insertion but before outbox delivery therefore returns the same queue
receipt and resumes the same transition instead of creating another assignment.
The relation is reciprocal: every durable assignment, review assignment, and
Coordinator context has exactly one correctly typed enqueue record, and every
such outbox record resolves back to exactly one owner.

At `TRANSITION_COMMITTED`, a journal entry records its one produced token.
`NEXT_ASSIGNMENT_ENQUEUED` additionally records the target occurrence and
outbox event IDs. At an `all_parents` join, every contributing token's journal
is advanced to that final stage in the same transaction that creates the shared
occurrence; a waiting token remains at `TRANSITION_COMMITTED`. A terminal token
also remains there because no assignment is enqueued.

## 5. Serialization and archive rules

`import_payload` remains an exact deep copy of the accepted source object. The
optional summary/record field follows this matrix:

| Declared value | Stored payload | Sprint record/summary | Download/export | UI badge |
|---|---|---|---|---|
| absent | no injected key | omit key | omit key | none |
| `legacy_v1` | preserve key | preserve key | preserve key | `legacy_v1` |
| `managed_workspace_v1` | preserve key | preserve key | preserve key | `managed_workspace_v1` |
| unsupported | no record | no record | none | none |

Reading a missing field produces effective `legacy_v1` in memory only. It does
not rewrite runtime state, pending state, sprint history, or an archive. The
existing sprint-record boolean named `legacy` describes historical archive
origin and must not be overloaded as a sprint-type discriminator.

Managed records additionally retain, without recomputation:

- manifest schema version and declared sprint type;
- all five `SprintProvenance` components;
- requested ref and canonical repository identity;
- preflight result and contract version;
- source commit for every assignment and accepted result commit for completed
  assignments.

Secrets and credential material are references only and are redacted from
payloads, responses, commands, logs, and evidence.

## 6. Migration policy

There is no eager migration.

1. Existing sprint, pending, queue, agent, and history files remain readable at
   their current storage schema versions.
2. A missing `sprint_type` always reads as effective legacy and remains absent
   on the next write unless a user explicitly submitted it.
3. Existing endpoints, archive shapes, and UI behavior do not require a new
   field for legacy records.
4. Managed-only durable state is stored separately or under additive optional
   members; readers must ignore unknown additive members.
5. Rollback may leave managed records archived/read-only, but must not rewrite
   them as legacy or feed them to the legacy importer.

## 7. Runtime configuration and isolation

The normalized config is defined by
`schemas/managed-runtime-config-v1.schema.json` and is sourced from:

```text
NGINX_QA_HTTP_HOST
NGINX_QA_HTTP_PORT
NGINX_QA_SERVICE_ROOT
NGINX_QA_PROTECTED_ROOTS
NGINX_QA_RUNTIME_ROOT
NGINX_QA_PROMPT_ROOT
NGINX_QA_MANAGED_ROOT
NGINX_QA_GIT_FETCH_TIMEOUT_SECONDS
NGINX_QA_CHILD_PORT_RANGE
NGINX_QA_INSTANCE_ID
NGINX_QA_DISABLE_TELEGRAM
NGINX_QA_DISABLE_TUNNEL
```

Normalization also derives four mandatory absolute sibling roots from the
configured runtime root: `process_runtime_root=<runtime_root>/processes`,
`log_root=<runtime_root>/logs`, `pid_root=<runtime_root>/pids`, and
`lease_root=<runtime_root>/leases`. They are not extra environment inputs and
may not be redirected independently. Every child process receives a distinct
directory below `process_runtime_root`.

ACTIVATE freezes the complete normalized config object in the durable sprint;
all later validation, recovery, and cleanup use that snapshot. A service
restart must compare its newly normalized environment with the frozen object
and route any drift through explicit recovery rather than rebasing an existing
sprint onto new roots, ports, or instance identity. The pure
`managed_runtime_config_invariant_issues` checker validates the snapshot before
the relational state checker uses it.

For process ID `p`, durable paths are exactly
`<process_runtime_root>/<p>`, `<log_root>/<p>.stdout.log`, and
`<log_root>/<p>.stderr.log` after normalized Windows-safe comparison. The
resolved executable is an absolute path outside every protected root, and cwd
is the resolved manifest-relative path inside the owned assignment workspace.
Each port lease repeats the frozen instance ID, loopback host, and a port in
the frozen child range. One lease may belong to only one immutable process
attempt; restarts acquire a new lease rather than reusing historical ownership.

The legacy path keeps its current defaults (`0.0.0.0`, port `8025`, relative
`runtime_state`) and does not require any new variable. A managed/staging launch
must provide every normalized value explicitly. The sprint qualification
defaults are host `127.0.0.1`, HTTP port `18025`, child range `18100-18199`, and
instance ID `universal-managed-sprint-engine-staging`, with a 120-second Git
fetch timeout and Telegram/tunnel disabled.

For this sprint the qualification `service_root` is exactly
`D:\nginx-qa-staging\universal-managed-sprint-engine`; `D:\nginx-qa` and its
runtime/prompt roots are mandatory protected roots. Staging uses its own
`.env.staging`, `.venv`, project registry, queues, pending records, prompt
archive, tunnel/webhook files, logs, PIDs, leases, and managed workspaces. The
launcher/qualification nodes capture live PID, port health, and protected-file
metadata before and after tests and permit only read-only live health checks.

Config preflight requires `child_port_start <= child_port_end`, disallows HTTP
port membership in the child range, disallows live ports `8025`/`8026`, and
isolates every configured/derived root from the live install. `prompt_root`,
`managed_root`, and `runtime_root` are pairwise disjoint; the four derived
runtime roots are intentional, distinct, non-overlapping children of
`runtime_root`.
No managed service may silently fall back to a live/legacy root.

## 8. Managed preflight contract

Preflight is deterministic for a pinned commit and a snapshot of durable lease
state. It returns a value; it does not activate the sprint. Checks run in this
order so cheap, non-mutating failures win:

1. dispatch type and managed manifest `schema_version`;
2. canonical repository identity, resolved immutable manifest and workspace
   source commits, manifest path/SHA-256, and declared file checksums;
3. start node, transition targets, at least one reachable terminal, and no
   reachable dead end;
4. uniqueness of task, agent, logical phone, node, assignment, and reviewer
   identities where each namespace requires uniqueness;
5. two independent reviewers, coordinator routes, allowed outcomes, and rework
   limits; the Coordinator task node must use `activation_policy=any_parent` so
   each reserved `STOP`/`NEED_DECISION` route creates its own occurrence;
6. source ref/expected commit and existing-branch policy;
7. exclusive `(mirror_storage_key, branch)` write ownership across all aliases
   of a remote while permitting concurrent pinned readers;
8. canonical managed root, candidate workspace root, runtime root, and forbidden
   root/parent/install/live paths;
9. child port range validity and absence of live/staging port overlap;
10. credential/secret absence and use of credential references;
11. construction of a complete activation plan with non-empty workflow,
    initial assignment set, and per-assignment allowed outcomes.

The preflight report embeds the normalized lease snapshot as well as its hash.
Branch and port arrays are sorted by `lease_id`, process owners by `process_id`,
then the complete snapshot is encoded with v1 canonical JSON. The hash alone is
never accepted as evidence without the matching embedded snapshot.
V1 uses one shared host network namespace, so exclusivity is
`(network_namespace_id="host", host, port)` across service instances; an
`instance_id` never weakens that key. ACTIVATE atomically compares the snapshot
hash and acquires all branch/port/process leases with shared compare-and-swap.
A stale snapshot restarts VALIDATE with no partial ownership or returns
`LEASE_SNAPSHOT_STALE`; the per-project activation lease alone is insufficient
for cross-project resources.

Stable issue codes include:

```text
SPRINT_TYPE_UNSUPPORTED
MANAGED_SPRINT_TYPE_REQUIRED
MANAGED_GIT_START_REQUIRED
SPRINT_SCHEMA_UNSUPPORTED
REPOSITORY_IDENTITY_MISMATCH
SOURCE_COMMIT_MISMATCH
SOURCE_COMMIT_REQUIRED
SOURCE_REF_INVALID
MANIFEST_CHECKSUM_MISMATCH
ASSIGNMENT_ARTIFACT_MISMATCH
GRAPH_START_NODE_UNKNOWN
GRAPH_TRANSITION_TARGET_UNKNOWN
GRAPH_TERMINAL_UNREACHABLE
JOIN_SOURCE_DIVERGED
JOIN_MERGE_CONFLICT
DUPLICATE_TASK_ID
DUPLICATE_AGENT_ID
DUPLICATE_NODE_ID
REVIEWER_CONFIGURATION_INVALID
COORDINATOR_ROUTE_MISSING
REWORK_LIMIT_EXCEEDED
BRANCH_ALREADY_LEASED
BRANCH_ALREADY_EXISTS
BRANCH_DIVERGED
LEASE_SNAPSHOT_STALE
WORKSPACE_ROOT_FORBIDDEN
WORKSPACE_ROOT_MISMATCH
WORKSPACE_DIRTY
RUNTIME_ROOT_NOT_ISOLATED
PORT_RANGE_INVALID
PORT_LEASE_CONFLICT
LIVE_PORT_CONFLICT
ORPHAN_PROCESS
RESOURCE_LIMIT_UNSUPPORTED
RESERVED_ENVIRONMENT_KEY
PATH_SEGMENT_UNSAFE
SECRET_MATERIAL_FORBIDDEN
ACTIVATION_INVARIANT_FAILED
```

Later nodes may add more specific codes but may not change the meaning of these
codes or turn a failed preflight into a partial activation.

`ALREADY_ACCEPTED` is the stable idempotent result status (not a preflight
issue). Dirty workspaces are preserved byte-for-byte and routed to the
Coordinator; no clean/reset is attempted. Orphan PID/process records are
reconciled before any new assignment. A port lease is released only after
confirmed process-group exit, and crash/failed-process logs remain immutable
evidence.

## 9. Transaction and mutation boundary

```text
VALIDATE -> PREPARE -> ACTIVATE
```

- **VALIDATE** resolves type, pins provenance, parses schemas, and computes the
  preflight report. Before managed dispatch it first publishes the isolated
  root request-binding receipt, then may create/update only its frozen bare
  mirror and secondary Git-ref receipt needed to read a Git manifest. After
  managed dispatch it may additionally create/update the fenced project-control
  start intent and normalized terminal validation evidence, but it may not
  publish runtime state, assignments, workspaces, leases, processes, queues, or
  agents.
- **PREPARE** may create unreferenced managed artifacts under the configured
  managed root. They remain unreachable from current project state and are
  deleted or retained as failed evidence according to policy.
- **ACTIVATE** is one durable commit that swaps the active sprint, agents,
  execution state, leases, and complete initial assignment set, then makes the
  corresponding queue items recoverably enqueueable through the outbox. Every
  `reserved` initial port is first acquired as a process-local exclusive OS
  socket; SQLite rollback releases only the sockets acquired by that failed
  transaction. That process-local rollback runs before the SQLite writer lock
  is released and uses a registry-wide revision plus a reservation generation
  fence, so a stale token cannot remove a durable, reacquired, or handed-off
  successor holder.
  Before serving traffic, startup attempts every committed
  assignment independently. It reacquires that assignment's durable
  `reserved` ports when possible; a conflicting endpoint leaves only that
  assignment fail-closed, and the background monitor then records durable
  recovery evidence without delaying HTTP readiness or suppressing recovery
  of unrelated assignments.
  Restart allocation serializes candidate selection with the same SQLite write
  transaction: under `BEGIN IMMEDIATE` it inserts the unique lease row before
  binding the provisional process-local reservation socket, then commits the
  lease and restart record together and marks the socket durable; either both
  resources survive or both are rolled back. A child handoff gap therefore
  never makes a still-live durable endpoint available to another service
  instance.

Service-owned branch refs are published only after the SQLite commit. The
branch and an immutable `refs/nginx-qa/publications/*` receipt are created in
one Git ref transaction. Without that receipt, recovery accepts only the exact
frozen initial head and rechecks the frozen existing-branch policy; with the
receipt, later compare-and-swap worker advances are legitimate. Startup
reconciles missing receipts/refs, so completion never depends on the client
retrying the original idempotency key.

A durable sprint record whose status is `preparing` has `workflow=null` and no
assignments, reviews, integrations, integration workspaces, leases, processes,
journal entries, outbox entries, repairs, or Coordinator/recovery records.
PREPARE filesystem artifacts are named only in the import attempt until the
ACTIVATE transaction publishes them. A failure before ACTIVATE may therefore
retain a failed import attempt and Coordinator evidence without any successful
activation receipt; an activated workflow has exactly one successful ACTIVATE
receipt and exact initial-assignment response. Prepared artifact IDs are
Windows-safe opaque path segments. On successful ACTIVATE they equal, in entry
order, the workspace IDs owned by the exact initial assignment list; while an
attempt is still in PREPARE (or failed before ACTIVATE), those IDs deliberately
need not resolve through the still-unpublished active workspace collection.

Before ACTIVATE, current agents, queues, execution state, active sprint,
reviewer decisions, and assignment ownership are unchanged. After a crash, the
journal must either expose the old active sprint or complete the same activation
idempotently. For a `managed_workspace_v1` activation, this state is forbidden
at every durable boundary:

```text
managed sprint_type AND status=active AND
(workflow is null/empty OR
 (exactly one of active_assignment_ids / allowed_outcomes_by_assignment is empty) OR
 (both are empty AND no valid current-revision scheduler/join witness))
```

Outside those narrow scheduler and join transaction/recovery boundaries, both
active IDs and the outcome map are non-empty. At a boundary both are empty
together; they may not disagree. A scheduler witness is a deterministic
available token for `any_parent` or single-inbound `all_parents` with its exact
`TRANSITION_COMMITTED` journal stage. Historical source assignments remain
durable, and a join witness is recomputed from the complete deterministic
available-token set, strategy, integration/artifact pair, and any required
Coordinator context.

The pure relational checker
`nginx_qa.sprint_types.managed_activation_invariant_issues` additionally
requires the active ID set to equal the active/reviews-pending assignment set
and outcome-map keys; those records must agree with their occurrence, creation
revision, effective outcomes/agent, workspace and write-branch lease ownership.
The checker recomputes occurrence/token/result identities, reciprocal token
consumption, source/integration provenance, journal effects, global branch/port
ownership, assignment and integration-workspace roots, full process
configuration/restart chains, graph/repair history, review/rework/recovery
fingerprints and effects, reciprocal outbox ownership, repository provenance,
and the successful ACTIVATE receipt. It also validates every stored graph's
reachability, Windows-safe case-folded paths/branches, inherited process
policy, and repair materialization. For completed/failed/blocked sprints it
also rejects live
assignments/occurrences, active leases/processes, available tokens, or pending
outbox work and requires terminal-token/status mapping plus bound failure or
completed external-block evidence. Standard JSON Schema cannot express these
cross-record equalities; ACTIVATE and every recovery write run both schema and
relational checks.

This invariant must not be retrofitted onto a legacy `queue_graph` execution;
existing legacy states may legitimately have no managed workflow/current node.

## 10. Current legacy integration seams

The compatibility implementation node must keep the absent/legacy call chain
intact around these named seams in `main.py`:

- `sequential_graph_import_definition` and `actor_import_options` — current
  validation/normalization;
- `stage_project_sprint_file` — first pending-store write;
- `update_pending_sprint_activation_file` — pending claim mutation;
- `project_actor_mutation_transaction` — agent/project mutation;
- `import_project_actors_data` — queue activation and archive orchestration;
- `record_project_sprint_import_file` and `download_project_sprint` — archive
  persistence/export;
- direct, Telegram, and pending-start route adapters.

Managed code must be placed behind a separate dispatch branch and must not make
these legacy functions stricter as a side effect.

## 11. Protected invariants and ownership

The following are frozen across downstream nodes:

- public sprint-type strings and absence semantics;
- `SPRINT_TYPE_UNSUPPORTED` pre-mutation behavior;
- separation of sprint type from execution mode;
- five-part provenance identity;
- absence-preserving serialization and no eager migration;
- VALIDATE/PREPARE/ACTIVATE barrier;
- no destructive Git recovery and no force push;
- no live checkout, process, port, `.venv`, runtime, queue, pending, agent, or
  prompt mutation;
- completed assignment/result/review/source/result commit immutability;
- operator-only promotion.

Workspace, import, concurrency, and continuity owners may implement behind this
contract and add versioned fields. Changing a frozen item requires Coordinator
routing and a new architecture review; silently changing v1 is not allowed.

## 12. Contract acceptance evidence

The architecture node must prove without starting a server that:

- missing and explicit legacy resolve to the same pipeline;
- missing remains absent when serialized;
- managed remains explicit and chooses the managed pipeline;
- unknown and non-string values yield the stable error without mutating input;
- changing `execution.mode` cannot affect type dispatch;
- schemas are valid JSON and enumerate only the two v1 values;
- the managed schema is never a validator for the legacy path;
- provenance component ordering is stable.

Runtime, workspace, Git, process, queue, and staging behavior is implemented and
qualified only by later graph nodes.
