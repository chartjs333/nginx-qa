# Inbound Pending Proposal API v1

Status: normative nginx-qa contract initially frozen by NQII-001. Backend,
managed-Git bridging, UI, neutral status integration, and qualification are
implemented by later sprint nodes. NQII-005 adds the closed status/read
projection without changing the existing operator and Telegram surfaces.

## 1. Scope and compatibility boundary

This API adds a producer-neutral intake contract to the existing project-scoped
Pending Sprints persistence and view. It does not create a second proposal
store or a second UI.

The following rules are normative:

- `create`, `read`, `preview`, `reject`, `comment`, and
  `request-regeneration` never claim, import, enqueue, activate, or call
  managed `start-from-git`;
- `POST .../start` remains the only activation boundary;
- `source_type` is provenance for display only. nginx-qa does not fetch email,
  folders, Telegram, Slack, Drive, or any other external source;
- existing Telegram JSON-to-pending behavior, `update_id` deduplication,
  webhook acknowledgement, deep links, and pending-sprint UI access remain
  compatible;
- existing pending records without v1 proposal fields remain readable through
  the existing local/deep-link operator surfaces;
- no source-system credential, authorization header, cookie, raw header set,
  credential-bearing URL, or attachment credential is accepted in proposal
  `source`, `summary`, or action-comment metadata, or persisted, logged, or
  returned from those fields.

The version is frozen by both the `/api/v1` URI and `schema_version: 1` in
successful mutation request/response documents and the status response. The
FastAPI-compatible error envelope has its separately versioned machine schema
and does not add a top-level `schema_version`. Producer request and status
response objects are closed. Changing a field's meaning, removing a field, or
adding a required field needs a new API/schema version.

Machine-readable contracts:

- `schemas/inbound-pending-proposal-create-v1.schema.json`;
- `schemas/inbound-pending-proposal-v1.schema.json`;
- `schemas/inbound-pending-proposal-response-v1.schema.json`;
- `schemas/inbound-pending-proposal-status-response-v1.schema.json`;
- `schemas/inbound-pending-proposal-action-v1.schema.json`;
- `schemas/inbound-api-error-v1.schema.json`;
- `schemas/inbound-producer-registry-v1.schema.json`.

## 2. HTTP surface

The v1 implementation extends the existing resource rather than replacing it.

| Method | Route | Meaning | Activates? |
| --- | --- | --- | --- |
| `POST` | `/api/v1/projects/{project_id}/pending-sprints` | Create a producer proposal | No |
| `GET` | `/api/v1/projects/{project_id}/pending-sprints` | Existing collection, additively enriched | No |
| `GET` | `/api/v1/projects/{project_id}/pending-sprints/{pending_sprint_id}` | Existing detail/read | No |
| `GET` | `/api/v1/projects/{project_id}/pending-sprints/{pending_sprint_id}/status` | Closed producer-neutral lifecycle snapshot | No |
| `POST` | `/api/v1/projects/{project_id}/pending-sprints/{pending_sprint_id}/preview` | Validate and record proposal-local preflight metadata | No |
| `POST` | `/api/v1/projects/{project_id}/pending-sprints/{pending_sprint_id}/reject` | Reject proposal metadata | No |
| `POST` | `/api/v1/projects/{project_id}/pending-sprints/{pending_sprint_id}/comments` | Append an operator/producer comment | No |
| `POST` | `/api/v1/projects/{project_id}/pending-sprints/{pending_sprint_id}/regenerate-request` | Set the regeneration-request marker | No |
| `POST` | `/api/v1/projects/{project_id}/pending-sprints/{pending_sprint_id}/start` | Existing explicit Play/Start gate | **Yes** |

`project_id` in the path is authoritative. For producer-authenticated calls,
the server authenticates the bearer credential before project lookup, resolves
the authenticated request to the canonical project, then authorizes that
principal for the canonical project and action before idempotency lookup. A
producer body cannot override the project.
`pending_sprint_id` is the nginx-qa record ID used by existing Pending Sprints
routes. `proposal_id` is the producer's stable project-scoped business ID and is
stored separately.

The closed `inbound-pending-proposal-response-v1` envelope applies only to the
create, preview, reject, comment, and regeneration-request mutations. The
closed `inbound-pending-proposal-status-response-v1` envelope applies only to
the bearer-only status route. The existing collection/detail GET envelopes and
existing `/start` response retain all current keys. Each existing
`pending_sprint` summary is additively enriched with one nested `proposal`
member: a v1 proposal resource for producer-created records, or `null` for
legacy/Telegram records. The existing string-valued `pending_sprint.source`,
`status`, `import_payload`, JSON preview, deep-link token, and Start behavior
are not replaced or reinterpreted. Legacy records are projected at read time
and are never rewritten merely to add `proposal: null`.

The collection retains its current selection and ordering behavior. A producer
uses the `pending_sprint_id` returned by Create for status polling. The owned
detail record remains readable after `started`, `rejected`, or `failed` even
when the proposal is no longer selected by the collection view; the closed
status record remains readable under the same terminal-state rule.

## 3. Create request

Producer requests use `Content-Type: application/json` and the create schema.
The maximum accepted body is 1 MiB of UTF-8 JSON. The first accepted request
returns `201 Created`; an exact replay returns `200 OK`.

A newly accepted proposal has `revision=0`, `proposal_status=created`,
`activation_state=not_started`, validation `not_run` with `checked_at=null` and
no issues, no comments, `regenerate_requested=false`, and
`started_sprint_id=null`. The server generates `pending_sprint_id`, UTC
`created_at`/`updated_at`, and `submitted_by` from the authenticated producer;
none is accepted from the create body.

Managed-Git example:

```json
{
  "schema_version": 1,
  "proposal_id": "hub-thread-2026-10-08-17",
  "idempotency_key": "create:hub-thread-2026-10-08-17:v1",
  "source": {
    "source_type": "email",
    "conversation_id": "conversation-17",
    "thread_id": "thread-17",
    "message_id": "message-42",
    "sender_label": "Release coordinator",
    "title": "Inbound coordinator sprint",
    "observed_at": "2026-10-08T08:30:00Z"
  },
  "summary": "Review and start the pinned managed sprint after validation.",
  "candidate": {
    "kind": "managed_git",
    "request": {
      "repository_id": "main",
      "ref": "refs/heads/inbound-sprint",
      "manifest_path": "orchestration/sprints/inbound/sequential-sprint.json",
      "idempotency_key": "activate:hub-thread-2026-10-08-17:v1"
    }
  }
}
```

The two idempotency keys have deliberately different lifecycles. The top-level
key deduplicates proposal creation. `candidate.request` is exactly the strict
four-member managed start request; its key is used only by Play/Start.

Direct legacy JSON uses:

```json
{
  "schema_version": 1,
  "proposal_id": "api-proposal-18",
  "idempotency_key": "create:api-proposal-18:v1",
  "source": {"source_type": "api", "message_id": "request-18"},
  "summary": "Legacy sprint JSON supplied by an authenticated producer.",
  "candidate": {
    "kind": "legacy_json",
    "payload": {"sprint_type": "legacy_v1", "actors": []}
  }
}
```

An absent `sprint_type` in a `legacy_json` payload preserves current legacy
semantics. An explicit value must be `legacy_v1`. A direct payload declaring
`managed_workspace_v1`, or an unknown sprint type, is rejected before proposal
storage. Managed proposals must use `managed_git` so immutable Git provenance
can be established by the existing start-from-git contract.

Create applies the existing legacy actor-import semantic validation without
executing the import. The pending summary records the derived assignment mode,
agent count, and task count; malformed actor/task structures are rejected
before storage.

### 3.1 Source metadata

`source` is intentionally closed and provider-neutral. Its allowed fields are
`source_type`, `conversation_id`, `thread_id`, `message_id`, `sender_label`,
`title`, and `observed_at`. The human-readable proposal summary is a separate
bounded field.

`source_type=email`, `folder`, or `telegram` never instructs nginx-qa to contact
that source. Authentication identity is not taken from `source`: the server
derives `submitted_by.producer_id` from the authenticated principal. Arbitrary
metadata bags are excluded from v1 so secret-shaped fields cannot cross the
intake boundary unnoticed.

Every source, summary, and action-comment string is display text, not opaque
transport data. The server rejects control characters and recognized
credential forms before persistence: authorization-scheme prefixes (`Bearer`,
`Basic`), URI user-info, PEM private-key markers, and configured secret/token
signatures. Rejection is value-free and uses `PROPOSAL_REQUEST_INVALID`.
Producers remain responsible for never submitting source secrets; server-side
redaction is not permission to send them. Runtime tests must scan the serialized
pending record, response, and captured logs with canary credentials before
accepting the implementation.

## 4. Proposal resource and lifecycle

Mutation responses contain `schema_version`, `correlation_id`, the required
`deduplicated` flag, and a proposal resource. In existing GET responses that
same resource is nested as `pending_sprint.proposal`. It uses
`source_metadata`, specifically to avoid colliding with the legacy string field
`pending_sprint.source`. A resource exposes only the contract model, never the
complete durable pending record or project access token.

The public `proposal_status` values are:

- `created`: accepted, with validation not yet complete;
- `ready`: validated and eligible for, or currently inside, an explicit Start;
- `started`: Start completed and `started_sprint_id` is present;
- `rejected`: rejected without activation;
- `failed`: validation or activation failed.

Transient activation is separate: `activation_state` is `not_started`,
`starting`, `started`, or `failed`. This avoids changing the meaning of the
legacy pending record's existing `status` values. Validation is independently
reported as `not_run`, `valid`, or `invalid` with sanitized issues.

Comments are append-only and carry a server-derived actor identity and
timestamp. `regenerate_requested` is a marker; it does not call or embed an
external adapter. Every metadata mutation increments `revision` exactly once.
Read and idempotent replay do not increment it.

The explicit Preview route uses the action schema with `action=preview`. It is
the only non-activation operation that may change validation state. A successful
Preview moves `created` or validation-failed `failed` to `ready`; an invalid
result sets `proposal_status=failed`, `validation.status=invalid`, and leaves
`activation_state=not_started`. It increments revision once. A replay does not.

Lifecycle constraints are:

| Proposal status | Validation | Activation | Start ID | Allowed next public actions |
| --- | --- | --- | --- | --- |
| `created` | `not_run` | `not_started` | `null` | preview, comment, regenerate, reject |
| `ready` | `valid` | `not_started` | `null` | preview, comment, regenerate, reject, start |
| `ready` | `valid` | `starting` | `null` | comment only; identical Start recovery may continue |
| `started` | `valid` | `started` | non-null | comment only |
| `rejected` | any prior result | `not_started` | `null` | comment, regenerate |
| `failed` | `invalid` | `not_started` | `null` | preview, comment, regenerate, reject |
| `failed` | `valid` | `failed` | `null` | start retry (`legacy_json` only), comment, regenerate, reject |

An ambiguous managed outcome remains `ready/starting` and is reconciled using
the stored managed request and activation idempotency key; it is not first
published as `failed`. A confirmed managed failure is immutable under that
caller key and follows the Coordinator recovery rule in section 8. Only a
`legacy_json` candidate may reclaim `failed/failed` through the existing leased
Start path, moving back through `ready/starting`; a `managed_git` candidate may
not.
Reject is forbidden while `starting`, after `started`, or after `rejected`.

### 4.1 Neutral status/read projection

`GET /api/v1/projects/{project_id}/pending-sprints/{pending_sprint_id}/status`
is the stable machine-to-machine read surface for an external producer such as
Inbound Hub. It returns the closed
`inbound-pending-proposal-status-response-v1` envelope:

```json
{
  "schema_version": 1,
  "correlation_id": "corr-hub-status-17",
  "status": {
    "schema_version": 1,
    "proposal_id": "hub-thread-2026-10-08-17",
    "pending_sprint_id": "pending-0123456789abcdef0123456789abcdef",
    "project_id": "9000",
    "revision": 4,
    "proposal_status": "started",
    "activation_state": "started",
    "source_metadata": {
      "source_type": "email",
      "conversation_id": "conversation-17",
      "message_id": "message-42"
    },
    "summary": "Review and start the pinned managed sprint after validation.",
    "validation": {
      "status": "valid",
      "checked_at": "2026-10-08T08:32:00Z",
      "issues": []
    },
    "regenerate_requested": false,
    "created_at": "2026-10-08T08:31:00Z",
    "updated_at": "2026-10-08T08:35:00Z",
    "started_sprint_id": "msv1-0123456789abcdef"
  }
}
```

The `status` object contains exactly `schema_version`, `proposal_id`,
`pending_sprint_id`, `project_id`, `revision`, `proposal_status`,
`activation_state`, `source_metadata`, `summary`, `validation`,
`regenerate_requested`, `created_at`, `updated_at`, and `started_sprint_id`.
It deliberately excludes the candidate, comments, producer identity, complete
durable pending record, import payload, activation attempt details, credentials,
and Telegram-specific fields. Its five lifecycle states and their validation,
activation, and Start-ID constraints are the same as the table above.

The status route is read-only. It does not increment `revision`, update a
timestamp, write a pending record, validate or fetch a candidate, emit a
notification, or invoke either legacy import or managed start-from-git. It
reads terminal proposals directly by `pending_sprint_id`; collection omission
after `started` or `rejected` does not make the status resource disappear.

The response is a level-triggered snapshot, not an event feed and not a
delivery acknowledgement. A Hub records the highest observed `revision` and
the last observed `(proposal_status, activation_state)` for each
`(project_id, pending_sprint_id)`. Repeating the same revision is a polling or
delivery retry and must not duplicate a notification. A later revision with an
unchanged lifecycle pair may reflect metadata-only work and need not generate
a lifecycle notification; a changed pair is eligible for a new notification.
This contract does not guarantee observation of every short-lived intermediate
state. A future transition-history or push-delivery API would require its own
versioned contract.

The Hub may relay an observed status to Telegram or any other user-facing
channel, but nginx-qa neither calls such a channel from this route nor requires
Telegram to be configured. Telegram is optional notification transport, not a
machine-to-machine dependency.

## 5. Idempotency and concurrency

### 5.1 Create binding

The durable lookup scope is the tuple:

```text
(canonical_project_id, authenticated_producer_id, idempotency_key)
```

The request fingerprint is SHA-256 over compact UTF-8 JSON for this ordered
array:

```text
[
  schema_version,
  canonical_project_id,
  proposal_id,
  source,
  summary,
  candidate
]
```

The body is parsed with the existing `parse_strict_json_object` semantics:
strict UTF-8, a top-level object, no duplicate object keys, no non-finite
numbers, and no invalid Unicode scalar. Schema validation follows parsing.
Fingerprint bytes use the existing `canonical_json_bytes` semantics: object
keys sorted recursively, no Unicode normalization, no ASCII escaping, compact
`,`/`:` separators, and no non-finite values. Authentication, the top-level
idempotency key, and transport correlation ID are not fingerprint members.

A secondary binding
`(canonical_project_id, authenticated_producer_id, proposal_id)` points to the
same fingerprint and pending record. It prevents a producer from duplicating a
business proposal by changing only the idempotency key.

Under the existing pending-sprints file lock, both bindings and the pending
record are checked/created atomically:

- a new binding creates exactly one record and returns `201` with
  `deduplicated=false`;
- an exact replay of an already-bound key returns that record's current
  resource with `200` and `deduplicated=true`, with no write, event,
  notification, or runtime mutation;
- an exact secondary `proposal_id` replay under a new key returns the same
  resource and may add only the missing key-to-record alias atomically; it does
  not change proposal revision/state or emit an event/notification;
- reuse of a key with another fingerprint returns `409
  PROPOSAL_IDEMPOTENCY_CONFLICT`;
- reuse of a `proposal_id` with another fingerprint returns `409
  PROPOSAL_ID_CONFLICT`;
- concurrent equal requests have one creator and replay the same
  `pending_sprint_id` to every loser.

Replay never resurrects or rewinds a rejected, failed, or started proposal.
Every replay uses a fresh response envelope with the current request's
correlation ID and `deduplicated=true`; transport correlation is never stored
as part of the semantic result.

### 5.2 Non-activation action binding

The action schema is used for preview, reject, comment, and
request-regeneration. Its maximum accepted body is 64 KiB of UTF-8 JSON. Route
suffixes map to action values exactly as follows:
`preview -> preview`, `reject -> reject`, `comments -> comment`, and
`regenerate-request -> request_regeneration`. The durable lookup scope is:

```text
(canonical_project_id, pending_sprint_id, authenticated_actor_id, idempotency_key)
```

The action fingerprint is the canonical ordered array of schema version,
canonical project, pending ID, action, expected revision, and comment; an
absent comment is encoded as JSON `null`. The body action must match its route.
`comment`, `reject`, and `request_regeneration` require `comment` as the text,
reason, or regeneration instruction respectively; `preview` forbids it.
Their single-write effects are fixed: `comment` appends one comment; `reject`
appends its reason and moves the proposal to `rejected`;
`request_regeneration` appends its instruction and sets
`regenerate_requested=true`; and `preview` replaces the validation result and
applies the lifecycle transition in section 4. Server-derived actor and time
fields are added to every appended comment.
Every successful first action and exact replay returns `200 OK` using the
closed mutation-response schema.

After authentication and authorization, the server checks an existing action
binding before checking `expected_revision`. Thus an exact replay returns the
stored semantic proposal snapshot in a fresh transport envelope with the
current request's correlation ID and `deduplicated=true`; it does not repeat
the mutation even though the first call advanced the proposal revision. The
first successful action has `deduplicated=false`. A changed replay conflicts.
Only a new binding is compared with the current revision; a stale
`expected_revision` returns `409 PROPOSAL_REVISION_CONFLICT` without mutation.
Binding, revision check, proposal mutation, revision increment, and semantic
result snapshot are one atomic file-lock operation.

No non-activation action may call `update_pending_sprint_activation_file`, the
legacy importer, queue mutation, or managed start-from-git.

## 6. Authentication and authorization

The producer API requires a protected machine credential even on loopback. The
normative application input is `Authorization: Bearer <credential>`. A gateway
may authenticate upstream traffic, but it must translate that identity to this
same bearer contract over its protected hop; nginx-qa does not trust an
alternate identity header in v1.

The new bearer-only status route always uses this producer boundary, including
on loopback. It never falls back to localhost operator access,
`X-Pending-Sprints-Token`, the Telegram webhook secret, a query parameter, or a
cookie. Existing collection/detail routes retain their local/deep-link
compatibility behavior and are not the neutral Hub status contract.

`NGINX_QA_INBOUND_PRODUCER_REGISTRY` names an absolute JSON file whose existing
path components are not symlinks, junctions, or reparse points and that
validates against `schemas/inbound-producer-registry-v1.schema.json`.
The file stores only lowercase SHA-256 token verifiers, producer IDs, canonical
project IDs, and allowed actions; it never stores plaintext bearer tokens. The
launcher supplies the file from protected configuration with owner/SYSTEM-only
write access. If the variable is absent, producer-authenticated operations are
disabled with `503 PROPOSAL_SERVICE_UNAVAILABLE`, while existing local and
deep-link Pending Sprints access remains available. If the variable is set,
startup fails closed for a missing/invalid file, duplicate `producer_id`,
duplicate `token_sha256`, unknown project, or unsafe ACL. Producer IDs are
case-sensitive.

Bearer tokens are unpadded base64url ASCII encodings of at least 32 random
bytes (at least 43 characters). The server hashes the exact presented ASCII
bytes and uses constant-time digest comparison against `token_sha256`. The
allowed action vocabulary deliberately omits `start`.

Exactly one `Authorization` header is accepted. Its scheme is
case-insensitive `Bearer`, followed by one ASCII token; multiple headers,
comma-joined values, padding, control/whitespace inside the token, query
parameters, and cookies are rejected. When the registry is configured,
missing or malformed authentication and an unknown token all produce the same
`401` response. When it is not configured, producer operations return the
documented `503` before credential or project lookup.

The server derives an opaque `producer_id` plus allowed project/action scopes.
Missing or invalid authentication is `401 PROPOSAL_AUTH_REQUIRED` with
`WWW-Authenticate: Bearer`. A valid principal lacking the canonical project or
action scope receives `403 PROPOSAL_FORBIDDEN`. Authentication comparisons are
constant-time where bearer material is handled.

The credential is configuration, never a request field. It must not be stored
in pending JSON, source metadata, responses, errors, or logs. In particular it
is distinct from and must not reuse:

- `X-Pending-Sprints-Token` deep-link/UI capabilities;
- the Telegram webhook secret;
- Scope Control admin or role credentials;
- repository credentials.

Producer credentials do not authorize `/start`. Existing localhost and
project-scoped deep-link UI behavior remains an independent compatibility
surface. Operator actions require their existing operator capability or an
explicit per-project action scope. A producer principal has exactly the
project/action scopes listed in its registry entry and receives no implicit
grants.

Bearer ownership is mandatory. For a bearer-authenticated request, a
producer-created proposal is visible or mutable only when
`proposal.submitted_by.producer_id` equals the authenticated `producer_id`.
Collection reads filter out proposals owned by other producers and all
legacy/Telegram records. Detail and action requests for a non-owned
`pending_sprint_id` return `404 PROPOSAL_NOT_FOUND` after project/action
authorization, never `403`, so ownership or existence is not disclosed.
Granting `read`, `preview`, `comment`, `reject`, or `request_regeneration` does
not waive this ownership check. Existing loopback and
`X-Pending-Sprints-Token` operator surfaces retain their current project-wide
visibility.

The status route requires the existing `read` action scope. Authentication is
performed before canonical project lookup; project/action denial is `403
PROPOSAL_FORBIDDEN`, while a missing or non-owned proposal is the same `404
PROPOSAL_NOT_FOUND` used for an unknown ID. Thus the status projection does not
disclose cross-producer or cross-project existence.

For action idempotency and comment attribution, actor IDs are server-derived:
Bearer calls use `producer:<producer_id>`, the existing project deep-link
capability uses `pending-token:<canonical_project_id>`, and current loopback
operator access uses `local-operator`. Raw capability/token material is never
part of an actor ID. The project deep-link capability, which already authorizes
project Start, authorizes read, preview, comment, reject, and regeneration for
that project; loopback access does the same under its existing local policy.
This extension does not change Start authorization and does not grant Start to
a producer bearer.

## 7. Correlation and normalized errors

The caller may supply `X-Correlation-ID` matching
`[A-Za-z0-9][A-Za-z0-9._:-]{0,127}`. Otherwise the server generates one. Every
request to a new proposal mutation route, every bearer-authenticated
collection/detail read, and every status read returns it in both the
`X-Correlation-ID` response header and JSON body. Mutation successes and status
successes carry it in their respective closed response envelopes; existing
bearer collection/detail GETs add a top-level `correlation_id` without removing
current keys. An invalid supplied value produces a normalized `400` using a new
server-generated correlation ID; the invalid value is not echoed or logged.
Existing local/deep-link GET and `/start` body/error shapes remain outside this
normalized producer contract.

Errors use the FastAPI-compatible envelope:

```json
{
  "detail": {
    "error": "PROPOSAL_IDEMPOTENCY_CONFLICT",
    "message": "The idempotency key is already bound to another request.",
    "correlation_id": "corr-7d45f0d1",
    "retryable": false
  }
}
```

Stable status/code families are:

| HTTP | Error | Meaning |
| --- | --- | --- |
| `400` | `PROPOSAL_REQUEST_INVALID` | Invalid JSON/schema/route-action/correlation or semantic candidate |
| `401` | `PROPOSAL_AUTH_REQUIRED` | Missing or invalid producer authentication |
| `403` | `PROPOSAL_FORBIDDEN` | Principal lacks project/action authorization |
| `404` | `PROPOSAL_PROJECT_NOT_FOUND` / `PROPOSAL_NOT_FOUND` | Authorized resource does not exist |
| `409` | `PROPOSAL_IDEMPOTENCY_CONFLICT` | Idempotency key is bound to different semantics |
| `409` | `PROPOSAL_ID_CONFLICT` | Producer proposal ID is bound to different semantics |
| `409` | `PROPOSAL_REVISION_CONFLICT` / `PROPOSAL_STATE_CONFLICT` | Stale revision or invalid lifecycle action |
| `413` | `PROPOSAL_REQUEST_TOO_LARGE` | Request exceeds the byte limit |
| `500` | `PROPOSAL_STORAGE_FAILED` | Sanitized non-retryable storage failure |
| `503` | `PROPOSAL_SERVICE_UNAVAILABLE` | Sanitized retryable dependency/service failure |

`retryable` is `true` only for `PROPOSAL_SERVICE_UNAVAILABLE`; it is `false`
for every other v1 proposal error. A caller may issue a new corrected or
re-authorized request where appropriate, but replaying the same failed request
is not advertised as safe unless `retryable=true`.

Validation messages identify a field or stable issue code, but never copy an
arbitrary invalid value. Authentication failures do not disclose whether a
project or proposal exists.

## 8. Managed-Git activation handoff

A managed candidate stores `candidate.request`, which validates directly
against `schemas/start-sprint-from-git-v1.schema.json` and therefore contains
exactly `repository_id`, `ref`, `manifest_path`, and activation
`idempotency_key`. Schema validation is followed by the existing procedural
`git_ref_format_valid` and `relative_git_path_valid` checks; credentials are
never candidate members.

Proposal Preview is advisory, proposal-local validation and does not invoke
start-from-git. `POST .../start` retains its existing bodyless request
contract; `expected_revision` and the non-activation action schema do not
apply. For a v1 managed proposal, Play validates operator authorization and
acquires the pending-sprints file lock for a state compare-and-set. Only
`ready/not_started` may be claimed.
The claim freezes the candidate fingerprint and stored activation request,
writes a durable fenced `activation_attempt_id`, keeps
`proposal_status=ready`, changes `activation_state=starting`, and increments
proposal revision in the same atomic write before invoking start-from-git. It
then passes the frozen `candidate.request` object
unchanged to the existing project-scoped operation. The managed operation's
own `(canonical_project_id, idempotency_key)` binding remains authoritative.

A concurrent Start while that claim is live returns `409
PROPOSAL_STATE_CONFLICT` and cannot create another activation binding or key.
Completion or failure is conditional on the current `activation_attempt_id`
and candidate fingerprint, not on an unchanged proposal revision, because a
comment may advance revision while `starting`. Reject, Preview, and
regeneration cannot mutate a claimed proposal. After a fenced claim expires,
recovery may create a new physical attempt ID, but it must reuse the frozen
candidate request and activation idempotency key; a stale attempt cannot
complete the proposal.

"Exactly once" means one logical activation binding, not one physical HTTP or
function call. If managed activation succeeds but the proposal completion write
or response is lost, recovery must replay the identical stored request with the
identical activation key, accept the managed operation's deduplicated result,
and finish the same proposal. It must never generate a replacement activation
key or create a second logical activation.

A confirmed managed `FAILED` start record is different from an ambiguous or
lost outcome. It retains its normalized error, and exact replay of the caller
key returns that stored failure with its original HTTP status. Ordinary
proposal Start must leave the proposal at `failed/failed`; it cannot reclaim
the record or invent a new key.
Recovery, when appropriate, is only the explicit managed Coordinator
`RETRY_IMPORT` action defined by the universal managed sprint contract, with a
new internal recovery key and attempt linked to the immutable failed record.
The proposal API never initiates `RETRY_IMPORT`. If an independently authorized
Coordinator recovery later succeeds, bridge reconciliation may advance the
proposal directly from `failed` to `started` in one revision; otherwise it
remains failed. Existing legacy-JSON Start recovery remains unchanged.

The authoritative managed preflight runs inside start-from-git and may create
its durable request/repository intent after the proposal enters `starting`.
Therefore Preview is not presented as equivalent to authoritative preflight.
If managed preflight fails, start-from-git creates no assignment, assignment
workspace, lease, or active runtime; the bridge records a sanitized
activation-failed proposal state. Repository cache/intents owned by the managed
contract retain their existing recovery semantics.

## 9. Required implementation and regression evidence

Later implementation nodes must prove:

1. create, exact replay, changed replay, secondary proposal-ID conflict, and
   concurrent equal creates;
2. cross-project and cross-principal isolation;
3. missing/invalid/forbidden authentication causes byte-for-byte no proposal or
   execution mutation;
4. safe source fields round-trip while unknown/secret-shaped fields are
   rejected and credentials never appear in persistence/logs/responses;
5. read, preview, reject, comment, regeneration request, and their replays do
   not change agents, queues, sprint history, managed control, assignment
   workspaces, leases, or active execution;
6. direct managed JSON is rejected before storage;
7. managed Play has one logical activation; a crash after managed success but
   before proposal completion replays the identical request/key and converges
   on the same sprint;
8. authoritative managed-preflight failure produces no assignment, assignment
   workspace, lease, or active runtime; exact caller replay preserves the
   failure and only explicit Coordinator `RETRY_IMPORT` may recover it;
9. mixed v1 and legacy/Telegram records preserve the existing GET envelopes,
   legacy `source` string and `import_payload`, using only additive
   `proposal: null` for legacy records;
10. existing Pending Sprints UI, deep links, legacy Start, Telegram staging and
    Telegram `update_id` deduplication remain green;
11. the closed status response validates in `created`, `ready`, `started`,
    `rejected`, validation-failed, and activation-failed states, remains readable
    after collection omission, and a status read is byte-for-byte non-mutating;
12. missing/invalid/forbidden status authentication, cross-project and
    cross-producer reads, correlation handling, and terminal `404` behavior use
    the normalized producer contract; and
13. status polling works with Telegram disabled and does not send a message or
    call any external adapter.

NQII-001 intentionally changes no backend route, runtime state, UI, Telegram
adapter, or activation behavior. NQII-005 adds the versioned status/read
contract while keeping its implementation transport-neutral and Telegram
optional.
