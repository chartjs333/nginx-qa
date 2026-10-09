# nginx-qa Inbound Integration v1

Status: implementation specification for nginx-qa side only.

## Goal

Keep nginx-qa responsible for sprint intake, review, activation and execution, while external systems such as Inbound Hub remain independent services.

nginx-qa does NOT read email, folders, Slack, Drive or other external sources in this sprint.

## Responsibilities of nginx-qa

1. Expose a stable versioned API for creating a pending sprint proposal.
2. Reuse the existing project-scoped pending-sprints persistence and UI.
3. Store source metadata supplied by an external producer, without interpreting source-specific credentials.
4. Show proposal details and JSON preview in UI.
5. Support operator actions: Preview, Play/Start, Reject, Comment/Regenerate request.
6. Keep Play/Start as the explicit activation gate.
7. Support managed start-from-git proposals in addition to existing direct pending JSON where applicable.
8. Expose a closed, reduced, read-only status snapshot suitable for external
   consumers and optional Telegram notifications.
9. Preserve legacy Telegram pending-sprint behavior.

## External producer contract

An external producer such as Inbound Hub may submit:
- project identity;
- proposal_id / idempotency_key;
- source_type (email, telegram, folder, api, other);
- source conversation/thread/message identity;
- human-readable summary;
- candidate sprint JSON or Git ref + manifest_path;
- optional metadata for UI display.

Duplicate submission with the same idempotency binding must not create a second pending sprint.

## API boundary

The implementation must provide a versioned create/read/update action surface for pending proposals. Exact route names may reuse or extend existing pending-sprints routes, but existing Telegram behavior must remain compatible.

The normative v1 routes, schemas, idempotency bindings, source metadata,
authentication, error, correlation, and activation boundaries are frozen in
[Inbound Pending Proposal API v1](INBOUND_PROPOSAL_API_V1.md). NQII-001 is a
contract-only change; backend, managed-Git bridge, UI, Telegram status, and
qualification remain owned by their subsequent sprint nodes.

The neutral notification-read boundary is the bearer-only endpoint:

```text
GET /api/v1/projects/{project_id}/pending-sprints/{pending_sprint_id}/status
```

Its closed
`schemas/inbound-pending-proposal-status-response-v1.schema.json` response
contains lifecycle/source display data only. It excludes candidate content,
comments, producer identity, durable pending-record internals, and Telegram
fields. It uses the producer `read` scope and owner isolation, returns terminal
states, follows the normalized error/correlation contract, and never activates
or mutates a proposal. Existing collection/detail endpoints remain compatible
operator and Telegram surfaces rather than becoming the Hub status contract.

Machine-to-machine API concerns:
- authentication boundary;
- stable versioned request/response schema;
- idempotency;
- normalized errors;
- correlation IDs;
- no source-system secrets in stored proposal metadata.
- level-triggered status polling with revision-based Hub deduplication.

## UI

Extend the existing Pending Sprints view instead of creating a parallel proposal system.

The UI should show:
- project;
- source type;
- source sender/title summary when provided;
- proposal status;
- candidate JSON or Git manifest reference;
- validation/preflight status;
- operator comments;
- Preview;
- Play/Start;
- Reject;
- Regenerate-request marker.

For managed Git proposals, Play must call the existing managed start-from-git path after validation.

## Telegram

Telegram remains a human-facing observability/notification channel. Existing Telegram pending JSON import remains supported.

nginx-qa may provide status information that an external Hub can relay to Telegram, but Telegram is not required for machine-to-machine integration.
The Hub, not nginx-qa, owns notification delivery, retry, and channel-specific
formatting. Repeated reads of the same proposal revision must not create a
duplicate notification. nginx-qa does not require a Telegram bot, webhook, or
chat to serve proposal status, and the status route does not send Telegram
messages or call an external adapter.

## Safety

- Preview/read/reject/comment must not activate a sprint.
- Status reads must not change proposal revision, pending storage, active
  execution, queues, workspaces, or leases.
- Play/Start is the activation boundary.
- Invalid managed proposals fail before mutation.
- Existing pending-sprint and Telegram regressions must remain green.
- No inbound email/folder adapters are implemented in nginx-qa in this sprint.
- No autonomous correspondence/contact-policy implementation is included here.

## Acceptance

PASS requires:
1. external API can idempotently create a pending proposal;
2. existing Telegram pending flow still works;
3. proposal appears in existing Pending Sprints UI;
4. source metadata is visible but source secrets are not persisted;
5. Preview does not mutate active execution;
6. Reject does not activate;
7. Play activates exactly once;
8. managed Git proposal can invoke start-from-git;
9. duplicate API submission does not duplicate pending sprint;
10. a bearer-authenticated Hub can read closed status snapshots for
    created/ready/started/rejected/failed proposals, including terminal records;
11. repeated status reads are non-mutating and support Hub-side deduplication;
12. status polling works without Telegram configuration or outbound adapters;
13. regression coverage passes.
