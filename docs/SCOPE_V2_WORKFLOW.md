# Scope revisions, human decisions and execution UI

This is the versioned feature contract, not permission to change Delta. The
development task performs no production installation or workflow operation.
See `SCOPE_V2_DEPLOYMENT.md` for the separate maintenance/migration boundary.

## Operator experience

Open **Выполнение спринта** from QA Queue Control, or `/execution`. Select a
project, sprint and Live or a saved timestamp/revision. Graph, visits, current
assignments, Pending decisions, exact review gates and timeline share the same
saved records. Polling uses a cursor and deduplicates event identities. A failed
read displays a stale/disconnected notice; reconnect cannot switch a selected
historical checkpoint back to Live. A past time without a stored checkpoint is
unavailable, never reconstructed from the current pointer. Archived sprints
remain inspectable through their stored final snapshots.

The execution status and operator-attention status are independent. An approved
scope waiting for ACK is not a graph transition, completion or Formal GO.
Unknown metadata is shown as unknown; an unvisited branch is not 'skipped'.
Review counters count only decisions applicable to the exact source result.

Read-only viewing requires no claim or ACK. Decision buttons require an operator
session. For a local DPAPI deployment, authenticate once by matching the public
pairing code displayed by this browser with the local credentialed helper:

```powershell
# FUTURE authorized deployment only; no token is entered or printed.
.\scripts\Authorize-ScopeBrowser.ps1 -PairingCode '<code displayed in this browser>' `
  -BaseUri 'http://127.0.0.1:18025/' `
  -SecretDirectory "$env:LOCALAPPDATA\nginx-qa\secrets\delta-18025"
```

The code alone grants no access. The helper must read the admin DPAPI credential
and authorize that exact browser's HttpOnly cookie. The administrative token
never enters the browser, a URL, a prompt or Git. After this one-time login, the
normal workflow needs no scripts, JSON, hash assembly or credential copying.
Sessions expire after one hour and on server restart; POSTs require same-origin
and CSRF checks. An authenticated operator may also use the administrative HTTP
header in a non-browser client. Deployment is single-writer/single-worker; a
multiworker session backend is not supplied by this feature.

For each pending request:

1. Read the reason, dependency, source reference, requested instructions and
   retained restrictions. The executor supplied this structured request.
2. Choose Approve, Reject or Edit boundaries. For approval/edit, the server
   validates the exact proposed content and shows the resulting diff for the
   current assignment **and every affected future node/reviewer**.
3. Confirm that exact validated decision. Editing again invalidates confirmation.
   A revision/content change requires a fresh validation; a concurrent losing
   operator receives HTTP 409. The accepted decision and rejected stale attempt
   remain in the audit. An exact retry returns the original receipt.
4. Approved content and its decision become durable together. The UI shows
   applied / awaiting exact ACK. A failed application publishes neither a
   decision nor a partial scope. Reject records the refusal without changing
   scope, prior ACK or execution.

Previously given human authorization is represented by explicit provenance and
the distinct **Применить ранее выданное разрешение** action, not a fabricated
Approve event. Source material or an executor assertion cannot authorize itself.
Recording prior authorization requires the operator's existing credential,
unchanged original proposal, exact validation and provenance. Editing those
boundaries needs its own explicit decision.

## Executor protocol and authority

The executor uses its **current role's** credential. In a sequential executor,
switch roles by rereading that role's DPAPI file per request; do not assume that a
new shell's environment reached an already-running agent. The generic helper
`scripts/Invoke-ScopeRoleRequest.ps1` supports these narrow role endpoints without
ever returning the credential. Existing installed helpers are not overwritten
by development; their old path allowlist must not be assumed to include new APIs.

Managed child processes do not inherit scope-control tokens. Their generic
`env:` secret resolver also refuses the whole `NGINX_QA_SCOPE_CONTROL_*`
namespace, including references under an innocent destination alias. Such a
manifest fails closed; use the role-request helper for the current role instead.
This is a credential-delivery boundary, not an operating-system sandbox against
arbitrary code running under the vault owner's Windows account.

Read-only role context:

`GET /api/v1/projects/{project}/assignments/{assignment}/scope-request-context`

This returns the exact sprint, immutable assignment identity, current execution
and scope revision, request endpoint and schema name. It does not claim work.
An executor sends `POST .../sprints/{sprint}/scope-requests` with the contract in
`schemas/scope-change-request-v1.schema.json`. The request contains semantic
instructions, retained restrictions, existing target node/reviewer IDs, reason,
dependencies, an idempotency key and a full Git commit/path source reference.
The server resolves and records the blob SHA-256 using the project's trusted
repository mapping. The executor need not calculate it. An optional supplied
`sha256` is checked as an additional expected-value constraint, never trusted.
It never downloads or executes code from a submitted source path.

Example structure (values are examples, not a Delta amendment):

```json
{
  "assignment_id": "<current assignment>",
  "expected_execution_revision": 12,
  "expected_scope_revision": 1,
  "idempotency_key": "request-proof-boundary-2",
  "reason": "A prerequisite needs the already identified wider proof boundary",
  "dependencies": ["A required reference lemma"],
  "proposal": {
    "instructions": "Complete the approved reference proof and its tests.",
    "retained_restrictions": ["No production integration", "No review or qualification waiver"],
    "node_ids": ["coordinator", "proof"],
    "reviewer_ids": ["review-one", "review-two"]
  },
  "source": {
    "repository_key": "github.com/example/project",
    "commit": "<full 40-character commit>",
    "path": "orchestration/scope/request.json"
  }
}
```

Scope requests are not graph outcomes. Never substitute `NEED_SCOPE_CHANGE` for
a normal handoff/outcome unless an independent runtime contract defines it.
Ordinary prerequisites, files, helper lemmas and reviewer cycles inside approved
boundaries do not need another human request.

After application, every governed assignment—including fresh reviewer tasks—
must obtain `GET .../assignments/{id}/effective-scope`, read the `effective`
instruction, then `POST .../effective-scope/ack` with:

```json
{"schema_version": 1, "scope_context": "<the complete returned object, not a string>"}
```

The illustrative placeholder above must be replaced by the exact JSON object.
`effective` is authoritative; `issued` is immutable history and cannot override
it. The independently verifiable `effective_core` hash excludes transport text.
ACK binds role, assignment, visit, revision, source and effective-content hash.
The old ACK never authorizes a newer context. Normal result/review/handoff
submissions carry that exact `scope_context` and the same role header. Admin
tokens cannot be used as role credentials. Scope requests do not convey
administrative authority, and UI browsing never sends an ACK on anyone's behalf.

For Delta, this includes coordinator 2750, formal/linkage roles 2753/2754 and
reviewers 2791/2792 when their assignments are governed. Role-specific credentials
are read at request time; they are not copied into generated prompts. Normal
review requirements and result/commit bindings are unchanged.

## Durable contract

The legacy adapter commits request/decision/scope/audit in the atomic canonical
config under the existing cross-process config lock. Managed runtimes use their
existing sprint exclusion and SQLite transaction. Execution and scope revisions
are distinct CAS inputs. The scope-workflow event sequence does not itself move
the execution pointer. Exact-context ACK updates only acknowledgement/audit data;
normal execution must still satisfy every review and other gate.

V2 has append-only amendments, assignment `binding_history` and
`acknowledgement_history`. Current lookup caches may advance; their historical
entries cannot be replaced. Previously issued assignment/result/review IDs and
instruction snapshots remain unchanged. Snapshot checkpoints are written only
with mutations, never with Graph/Timeline/GET reads.

An existing v1 governed sprint must undergo the explicit offline, copy-out
migration before a new revision. GET/startup does not silently migrate it. The
old one-shot API remains readable/replayable for compatibility; a new v2 raw
amendment cannot bypass the authenticated human-decision workflow.

Supported observations cover legacy conditional graphs, sequential queue graphs,
structured parallel group tasks and managed workspace execution. Parallel task
identity is the real queue item delivered by normal execution, not a fabricated
historical assignment. Unstructured old messages without a durable task identity
cannot safely be retroactively amended; the API fails closed instead of guessing.
No compatibility claim treats missing state as completed or approved.
