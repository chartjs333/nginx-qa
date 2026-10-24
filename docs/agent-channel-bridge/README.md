# Agent Channel Bridge: nginx-qa implementation

Draft software integration. No production activation or hardware acceptance is implied.

## Scope and pinned sources

- nginx-qa base/PR target: `spec/agent-channel-bridge-v1`, `3ac994996e5ffd5fc2e79229622dd6a8f0b15267`.
  Its only differences from main `627b478e29f5f7254d279363790f46e92c114d8d` are the three Spec Kit documents.
- Inbound Hub PR #2: `work/channel-bridge-completion`, `d31f6bd54f70a7e7e2e4295b43424b81dc272155`.
- Inbound Hub PR #1: `work/musician-booking-completion`, `d518e2f1411da188699da47565f2f564da1fd95b`.
  Its `integrations/nginx_qa.py` creates/reads pending proposals only. Its requirements matrix and
  VALIDATION distinguish actual pending-API integration from active-sprint integration.
- Reviewed branch inventory and open nginx PRs #2–#10; none implements this Bridge. Historical
  managed changes are already in the source ancestry. No unfinished local changes existed.

No Hub source, main, source branch, protected historical PR, pending proposal API, Play/Start
boundary, queue content or live database was changed. Development uses a new branch and temporary
SQLite databases. The implementation supports active **managed execution, reviewer and coordinator assignments**,
resolved through existing binding/liveness rules. A reviewer still submits a normal review after
acknowledging evidence; a coordinator still submits a normal recovery decision. Legacy
queues/identity retain their existing interfaces; channel access is opt-in to the managed runtime.
No legacy assignment is silently reinterpreted as a managed assignment.

## Architecture and graph behavior

`ChannelBridge` resolves the exact `(project, sprint, assignment, agent)` through the existing
validated managed runtime store. `agent_id` is the graph's logical ID, not the phone. The existing
phone-bound `X-Nginx-QA-Scope-Token` is mandatory even for assignments without scope amendments.
Project IDs are exact durable managed IDs; no alias guessing or caller-supplied role is accepted.

Three additive tables in the same managed SQLite database hold request outbox/mailbox, immutable
event inbox and audit receipts. They are created only when the bridge is configured. Frozen
runtime JSON schemas, assignment statuses, review schemas and graph definitions are unchanged.
Request admission, cancellation/acknowledgement and ordinary result submission use the same
existing sprint lock. Writes commit before HTTP acknowledgement. Network calls hold neither
SQLite write transactions nor the sprint lock, allowing Hub introspection to call nginx safely.

An agent may submit many requests (`blocking: false`) and continue working. `blocking: true`
creates a **WAITING_FOR_EXTERNAL_INPUT** dependency of that exact assignment. Result submission
returns 409 while any blocking request is unacknowledged. Independent assignments can finish.
A response or terminal failure makes the dependency **EXTERNAL_INPUT_AVAILABLE**. The agent reads
its mailbox/artifact, handles the outcome, then POSTs `ack`; the dependency becomes **RESOLVED**.
Acknowledgement never marks an assignment complete, approves a review or starts a successor.
The normal result and two-review lifecycle still runs. Timeouts/cancellation also require explicit
acknowledgement, so silence cannot be mistaken for a successful measurement.

Mailbox/status are the persistent agent receive queue: no transient message is injected into a
worker queue where it could be claimed by another assignment. Agents poll their scoped mailbox
and follow the integration example below. Responses are labelled `untrusted_external_data`;
content is never concatenated into instructions, parsed as executable code or used to approve a gate.

The worker reconciles at startup and every second. Network failures back off to at most 60 seconds;
request deadlines are mandatory and at most 24 hours. At most 100 unacknowledged requests per
assignment are admitted; acknowledge terminal nonblocking requests to free quota. Dispatch and
callbacks are at-least-once. Gateway/provider adapters MUST deduplicate by request ID. Duplicate
request keys with changed content fail 409. Duplicate event IDs with changed bodies fail 409.
Out-of-order nonterminal events cannot regress terminal state. Unknown, mismatched, late or
inactive-assignment events never release a dependency; valid late receipts remain auditable.

## Authorization and research profiles

`neurogeneticist`, `clinician` and `bioinformatician` profiles describe specializations and requested
capabilities only. Profile selection grants nothing. Effective access is the intersection of:

1. nginx project operation policy and explicit agent/channel entitlement (including expiry/revoke);
2. exact live managed assignment;
3. Hub channel/project/agent capabilities and consent;
4. verified researcher identity/session, device and signed plugin activation;
5. scoped action grant, plugin version/hash, expiry/revocation and required assurance.

Hub remains the issuer/verifier of sessions, challenges and grants through its existing mobile
API and trusted Ed25519 IdP/plugin keys. nginx agents receive **opaque grant IDs only**; no login
proof, PIN, biometric, smart-card secret or IdP signing key crosses the agent API. A grant is
provided by the authorized researcher through their Hub workflow, not minted from an agent profile.
Smart-card authentication is a signed challenge response once per session; no card-presence polling.
Hub owns user switching, logout, idle/max lifetime, action-bound step-up, replay protection and
plugin consent. nginx's `required_assurance_levels` can raise requirements per operation; it never
lowers a Hub channel requirement. `max_auth_age_seconds` bounds authenticated session freshness.

The 1.1 `ScopedAssignmentClient` consumes the existing introspection response's permitted channel
operations on **every** Gateway authorization, including dispatch, retry and observation. The
original Hub client checked only `active`; using it with this adapter is rejected at construction.
Live requests require local permission for both the live operation and asynchronous fallback.
The Gateway checks owner-controlled presence; absent/stale presence is Offline. Instrument
operations are restricted to `request_data`/`request_observation`. No generic command field,
plugin execution payload, arbitrary URL, raw binary or physical actuation operation exists.

## HTTP API

Let `A=/api/v1/projects/{project_id}/sprints/{sprint_id}/agents/{agent_id}`.
Every agent operation requires the role-token header. Reads/actions require `assignment_id` query
parameter. IDs and bodies must exactly match the path; callers cannot override routing identity.

| Method/path | Behavior |
|---|---|
| `GET A/channels` | Current Hub capabilities intersected with nginx policy; availability, plugin metadata, AAL; discovery is not a grant |
| `POST A/channel-requests` | Strict request body, durable enqueue, HTTP 202; unchanged replay returns original resource |
| `GET A/channel-requests` | Assignment mailbox, 100 per page, `after`/`next_cursor` |
| `GET /api/v1/projects/{project_id}/sprints/{sprint_id}/channel-requests/{request_id}` | State + latest 100 immutable event receipts; `agent_id` and `assignment_id` required |
| `POST A/channel-requests/{request_id}/cancel` | Durable local cancellation and remote tombstone delivery; `cancel_pending` exposes incomplete propagation |
| `POST A/channel-requests/{request_id}/ack` | Acknowledge terminal response/failure; cannot acknowledge pending request |
| `GET A/channel-requests/{request_id}/artifact` | Re-authorized bounded immutable artifact, SHA-256 verified, attachment/nosniff/no-store |
| `POST /api/v1/integrations/inbound-hub/agent-introspection` | Exact Binding body; separately scoped Bearer credential; active + allowed channels/operations/profile/AAL |
| `POST /api/v1/integrations/inbound-hub/channel-events` | Signed raw event body; durable dedupe and provenance correlation |
| `GET /api/v1/projects/{project_id}/sprints/{sprint_id}/channel-requests` | Existing admin-token authorization; operator status, worker health and pagination |

Schemas are in [schemas](schemas). `scripts/export_channel_schemas.py --check` verifies drift.
New routes are also enumerated in FastAPI OpenAPI. Error envelopes use `detail.error` and static
value-free codes (403 authority, 409 lifecycle/conflict, 413 size, 422 schema, 429 quota, 503 disabled/unavailable).
No capability mutates Pending Sprints or invokes Play/Start.

## Contract compatibility: v1 versus explicit 1.1 adapter

The existing Hub PR #2 HTTP app exposes only `POST /v1/channel-requests` for agents. Its Python
Gateway additionally offers discover/authorize/cancel/status/artifact operations, but these are
not a service API. It rejects v1 envelopes whose `created_at` is older than 60 seconds **before**
idempotency lookup, so an unchanged durable retry cannot survive a long outage.

This repo supplies an **opt-in companion**, `channel_hub_adapter.py`, mounted at
`POST /v1/nginx-qa-bridge` beside the unmodified Hub app. The original v1 route and models remain
unchanged. Body: `{schema_version: "1.1", action, payload}`. Supported actions: `discover`,
`submit`, `validate`, `status`, `cancel`, `artifact`. Every call is HMAC signed. The adapter delegates
to the real Gateway rather than substituting an authorization simulator.

`submit` first durably records the original envelope and its canonical fingerprint in a separate
Hub extension receipt table. On first successful delivery it supplies Gateway v1 with a fresh
transport creation time; all original provenance stays in the receipt. Recovery compares every
field except this transport timestamp against the persisted Gateway request. It never changes
request ID, idempotency key, scope, payload, deadline or experiment. Tombstones fence submit/cancel
races, including cancellation before delivery. Cancellation cannot reverse already delivered
messages, uploaded observations or an accepted call; the returned state exposes that limitation.

Callbacks retain **actual v1** field names and floating-point Unix seconds, and
`X-Timestamp` / `X-Signature = HMAC-SHA256(key, timestamp + '.' + exact_body)` with a 60-second
transport window. Old events may be re-signed for retry; persisted event IDs prevent replay.
`source_metadata` binds project/sprint/assignment/agent/experiment/checkpoint/sample; immutable
artifact references use `artifact://sha256/{digest}`. nginx checks reference/digest correspondence,
then fetches bounded chunks through the adapter and verifies content SHA-256 before returning bytes.
No caller-supplied URL is fetched. TLS endpoints never follow redirects or inherit proxy credentials.

The companion is a **new versioned integration dependency**, not an API claimed to exist in PR #2.
Deploying nginx alone against the original Hub entry point is insufficient and fails closed.
No original Hub files were edited. Removing the companion leaves original v1 behavior intact.

## Configuration and isolated use

Install nginx requirements. Set `NGINX_QA_CHANNEL_BRIDGE_CONFIG` to a protected JSON file conforming
to `config-v1.schema.json`, and `NGINX_QA_CHANNEL_TRANSPORT_KEY` to base64 of 32+ random bytes.
`NGINX_QA_MANAGED_ROOT` and existing managed configuration remain necessary. Default is disabled.
The config contains `hub_url`, `introspection_token_sha256`, `introspection_projects` and project
agent policies. Tokens are environment secrets; config contains only the SHA-256 of the separately
issued 32+-character introspection credential. Configure existing per-phone role tokens. Config
policy is reread on authorization; endpoint/key changes require an authorized service restart.
Use dedicated secrets for this integration, separate from pending proposal credentials.

```json
{
  "schema_version": 1,
  "hub_url": "https://hub.example.invalid",
  "introspection_token_sha256": "<sha256 of separately issued credential>",
  "introspection_projects": ["project-id"],
  "projects": {
    "project-id": {
      "operations": ["send_message", "request_data", "request_observation", "request_live_session"],
      "required_assurance_levels": {"request_observation": 2},
      "max_auth_age_seconds": 300,
      "agents": {
        "builder": {
          "profile": "neurogeneticist", "expires_at": 1800000000.0,
          "channels": {"microscope": ["request_data", "request_observation"]}
        }
      }
    }
  }
}
```

For the Hub companion, install the pinned PR #2 package in a dedicated environment, make this repo
importable, and use `python -m nginx_qa.channel_hub_service` **instead of** the original Hub launcher.
This composes the original Gateway/mobile app/adapters with the 1.1 endpoint and scoped introspector.
It accepts the original `BRIDGE_CONFIG`, `BRIDGE_TRANSPORT_KEY`, `BRIDGE_ASSIGNMENT_TOKEN`; configure
`assignment_endpoint` and `callback_endpoint` to the nginx routes above. Trust registries,
artifacts, database, consent, approved plugin catalog and trusted IdP remain operator-owned.
`live_session_endpoints` optionally maps human channel IDs to configured HTTPS endpoints, token
**environment references**, and `media: audio|video`. Such providers must honor Idempotency-Key;
only 200/202/204 count as accepted. Missing/unanswered provider follows Hub's bounded async fallback.
No provider or IdP credential is included in this repository.

The companion's human adapter exposes `request_data`/`request_observation` in the original durable
mobile inbox. This addresses another actual PR #2 gap: its default MobileInboxAdapter accepts only
`send_message`. The device path uses the original read-only HTTPS GET adapter. Video **session
initiation** is supported by the provider contract; raw video artifact upload remains unsupported
by Hub's current media validator (text/JSON/PNG/JPEG/WAV only).

## Agent workflow

1. Obtain active identity using the existing managed identity API; retain exact IDs and role token.
2. Discover `GET A/channels?assignment_id=...`. Ask the researcher through Hub's mobile workflow
   for any required grant. Do not ask for their PIN or authenticator proof.
3. POST a schema-valid request with a stable request/idempotency ID, Unix `created_at`, mandatory
   deadline, optional sample/experiment/checkpoint and `blocking` dependency choice.
4. Continue independent work; poll the assignment mailbox. For `response_received`, inspect the
   provenance and fetch the immutable artifact if needed. Treat its content as untrusted evidence.
5. Handle response, rejection, timeout or cancellation; POST `ack`. Submit results through the
   existing identity/result API. Existing reviewers still decide whether the graph advances.

## Operational limits

Single-node SQLite durability and restart recovery are tested. Distributed authorization/storage,
Windows runtime qualification, real TLS/IdP deployment, browser/mobile platforms, real card reader,
physical read-only instrument calibration, real voice/video provider, load/soak, retention/backup
operations and independent security review require separate acceptance. A local ASGI integration
and fixture public keys are not hardware or production evidence. Do not mark production-ready.
