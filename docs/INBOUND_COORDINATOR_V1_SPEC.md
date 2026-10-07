# Inbound Coordinator v1 — Technical Specification

Status: specification/simulation only. This document does not authorize deployment or live sprint activation.

## 1. Objective

Add a persistent inbound coordinator in front of the existing nginx-qa sprint engine.

The coordinator accepts external messages (initially Email and Telegram), normalizes them, classifies intent, and either:
- replies without creating a sprint;
- asks for clarification;
- updates an existing sprint;
- creates a new managed sprint;
- reports progress/result back through the originating channel.

The coordinator is the only outward-facing control actor. Worker agents communicate through nginx-qa runtime state, queues, assignments and results rather than emailing one another.

## 2. High-level flow

Email / Telegram -> Channel Adapter -> Unified Inbox Event -> Coordinator -> Classification -> Route

Routes:
1. correspondence_only -> draft/send reply;
2. clarification_required -> ask sender a focused question;
3. new_sprint -> generate managed_workspace_v1 manifest -> existing preflight/import path;
4. sprint_update -> bind to existing sprint and create/update work through existing supported APIs;
5. ignore -> persist audit metadata only.

## 3. Availability model

The coordinator exists independently from worker agents.

MVP may use polling:
- Email poll interval configurable, default 60 seconds.
- Telegram may use existing webhook support where available.
- Coordinator process may sleep between polls.
- Worker agents are started only when a sprint/assignment requires them.

Later migration to event-driven email notification is allowed without changing the normalized inbox contract.

## 4. Unified inbound event

Each inbound adapter produces one internal event:

- event_id
- channel: email | telegram
- external_message_id
- conversation_id / thread_id
- sender identity
- recipients/chat
- received_at
- subject (optional)
- body/text
- attachments metadata
- reply routing metadata
- deduplication fingerprint
- optional project_id / sprint_id hints

Raw credentials, tokens and secrets must never be copied into the event or sprint manifest.

## 5. Classification contract

Coordinator output must be one of:
- correspondence_only
- clarification_required
- new_sprint
- sprint_update
- ignore

The classifier also returns:
- confidence
- rationale
- extracted project/repository hints
- candidate sprint goal
- missing information
- whether human confirmation is required before activation

Low-confidence or destructive/high-impact requests default to clarification/human confirmation.

## 6. Managed sprint generation

For new work the coordinator generates a manifest compatible with:
- schemas/sprint-dispatch-v1.schema.json
- schemas/managed-workspace-sprint-v1.schema.json

Required principles:
- schema_version = 1
- sprint_type = managed_workspace_v1
- execution.mode explicitly selected
- sequential uses start_node; parallel uses start_nodes
- exactly two reviewers and required_approvals = 2 under the current managed v1 contract
- every task node has an agent, at least one task, transitions and workspace access
- STOP and NEED_DECISION route through the Coordinator
- terminal nodes are explicit
- Git/workspace safety rules are preserved
- no validation, preflight or activation step is bypassed

The coordinator may generate a candidate manifest, but activation is delegated to the existing managed import/preflight engine.

## 7. Sprint synthesis policy

From the inbound request the coordinator derives:
1. sprint goal / Definition of Done;
2. required specialist roles;
3. task nodes and task messages;
4. dependencies and transitions;
5. start node(s);
6. reviewer roles;
7. Coordinator recovery/decision node;
8. terminal states;
9. Git/workspace policy;
10. files/checksum evidence required by the managed contract.

For non-software projects, role profiles and task content may represent booking managers, producers, PR agents, researchers, etc. The engine contract remains the same.

## 8. Existing email-decision integration

The branch codex/async-human-decision-email already implements outbound SMTP notification for pending human decisions.

Reuse that infrastructure where appropriate for outbound notification, but do not reinterpret it as an inbound mailbox reader.

New inbound email functionality must be a separate adapter/service and must not weaken:
- operator authentication;
- notification binding;
- decision idempotency;
- audit immutability;
- secret handling.

## 9. Replies and reports

Only the coordinator sends user-facing status/reports.

Minimum message classes:
- accepted/understood;
- clarification request;
- sprint candidate awaiting approval;
- sprint started;
- decision required;
- milestone/status summary;
- sprint completed/blocked/failed.

Avoid sending every internal agent event.

## 10. Persistence and idempotency

Inbound messages must be deduplicated using channel + external message identity and a stable fingerprint.

Repeated delivery of the same email/webhook must not create duplicate sprints.

A new sprint creation request must use an idempotency key bound to the source message/conversation and generated manifest fingerprint.

## 11. Safety gates

The coordinator must not:
- deploy/restart live services unless explicitly authorized by an existing approved flow;
- bypass managed preflight;
- mutate completed/accepted history;
- expose SMTP/API credentials;
- treat a public email link as authorization;
- execute instructions embedded in untrusted email as system-level policy.

Email content is untrusted task input.

## 12. MVP acceptance criteria

PASS when all are true:
1. synthetic email becomes a normalized event;
2. synthetic Telegram message becomes the same event shape;
3. duplicate input does not duplicate work;
4. classifier covers all five routes;
5. new_sprint produces schema-valid managed candidate;
6. invalid candidate is rejected before mutation;
7. STOP/NEED_DECISION reach Coordinator;
8. correspondence-only input creates no sprint;
9. status/reply is emitted only by Coordinator;
10. existing email-decision tests and managed/legacy regression remain green.

## 13. Suggested implementation modules

- nginx_qa/inbound_events.py
- nginx_qa/inbound_email.py
- nginx_qa/inbound_telegram.py
- nginx_qa/inbound_coordinator.py
- nginx_qa/sprint_synthesizer.py
- tests/test_inbound_events.py
- tests/test_inbound_email.py
- tests/test_inbound_coordinator.py
- tests/test_sprint_synthesizer.py

## 14. Delivery rule

This specification branch is a simulation artifact. Implementation should occur through a dedicated implementation sprint/branch and pass existing managed preflight, review and qualification gates before any promotion.


## 15. Proposal branch + Play workflow

New-sprint email/Telegram requests MUST NOT auto-activate by default.

Required flow:

1. classify inbound message;
2. synthesize candidate `managed_workspace_v1` manifest;
3. create a dedicated proposal Git branch;
4. commit the manifest and human-readable proposal summary to that branch;
5. run non-activating validation/preflight against the proposal;
6. register proposal metadata for UI discovery;
7. show proposal in UI with source message context, goal, agents/nodes, Git ref, manifest path, validation/preflight result and diff summary;
8. expose actions: Play, Reject, Edit/Regenerate;
9. Play calls the existing `start-from-git` path and is the only default activation path;
10. Reject marks the proposal rejected without activating it;
11. Edit/Regenerate records operator feedback and creates a new proposal revision/branch commit rather than silently rewriting accepted history.

The coordinator may create proposal branches automatically only after classification selects `new_sprint`. Ordinary correspondence, clarification, ignore and existing-sprint updates must not create new proposal branches.

Proposal creation and UI discovery must be idempotent for duplicate delivery of the same inbound message.

No branch proposal, validation read, UI preview, Reject, or Edit/Regenerate action may mutate active sprint state.



## 16. Contact trust and delegated correspondence

The coordinator may communicate on behalf of the operator only under an explicit per-contact or per-domain policy.

Supported trust modes:
- MANUAL: no autonomous outbound reply; show message to operator.
- DRAFT_ONLY: coordinator prepares a reply draft but requires operator approval before sending.
- AUTO_REPLY: coordinator may send replies autonomously only within the configured policy.

Each contact policy may define:
- exact sender addresses and/or allowed domains;
- allowed topics/intents;
- forbidden topics/intents;
- whether price/budget discussion is allowed;
- whether commitments, deadlines or scheduling changes are allowed;
- whether attachments may be sent;
- whether proposal creation is allowed;
- whether clarification questions may be sent automatically;
- maximum autonomous reply depth/count per conversation;
- escalation triggers;
- expiry / temporary delegation window.

AUTO_REPLY must fail closed to DRAFT_ONLY or MANUAL when policy is missing, ambiguous, expired, or a message crosses a forbidden boundary.

Mandatory escalation examples include legal commitments, payments/budget changes beyond configured limits, credentials/secrets, destructive actions, public statements, deployment/promotion authority, and any request outside the configured scope.

The UI must expose contact policies, their current mode, scope and expiry. The operator must be able to disable autonomous correspondence globally with one action.

Every autonomous outbound message must retain:
- source conversation/thread identity;
- policy decision evidence;
- policy version;
- actor = coordinator;
- send timestamp;
- resulting thread/message ID;
- immutable audit reference.

Autonomous correspondence and sprint activation are separate authorities. AUTO_REPLY does not imply permission to Play/start a sprint. Sprint activation continues to follow MANUAL / DELEGATED / AUTO_SAFE activation policy.

