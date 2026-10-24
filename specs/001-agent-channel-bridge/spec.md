# Feature Spec: Active Sprint Agent Channels
# Shared protocol — Agent Channel Bridge v1
Status: proposed; no production activation.
Repositories: chartjs333/nginx-qa and chartjs333/inbound-hub, branches spec/agent-channel-bridge-v1.

## Contract
Keep existing inbound sprint proposal and explicit Play/Start untouched. This protocol operates ONLY on already-authorized active sprint agents.
Channel: channel_id, type human|device|service, capabilities, owner, project scope, availability, policy. Agents may use multiple channels.
Request envelope: schema_version=1, request_id, idempotency_key, correlation_id, project_id, sprint_id, assignment_id, agent_id, channel_id, operation, payload, created_at, deadline?, experiment_id?, checkpoint_id?.
Event envelope: schema_version=1, event_id, request_id, correlation_id, channel_id, event_type, occurred_at, received_at, content_ref?, sha256?, source_metadata.
Operations: send_message, request_data, request_observation, request_live_session. Event types: accepted, rejected, queued, delivered, human_unavailable, response_received, failed, expired, cancelled.
No arbitrary raw binaries inside messages; reference immutable artifacts.

## Rules
Manual Online/Offline state belongs to human, not agent. Offline -> never initiate voice/video call, but may queue/send asynchronous messages. Online -> call may be attempted; unanswered uses limited retries and async fallback. Unknown/stale presence treated as Offline.
QR identifies registered checkpoint context only; QR grants no access or device control. Cameras and voice are consent-based. Physical instrument actuation is OUT OF SCOPE for MVP, with explicit separate authorization and safety review before future support.
Authenticate transport in both directions, authorize project/agent/channel/action, protect against replay, cross-project access, malicious payloads, duplicate delivery and prompt injection. Ensure traceability for sample, instrument, operator and experiment.
Durable outbox/inbox, explicit delivery status and idempotent retry. Waiting for human must not block independent agents. 

## User stories
1. Active AI Scientist sends a request through Hub to an authorized human, device or service channel; reply returns to same sprint assignment.
2. Researcher manually toggles Online/Offline. Live calls are impossible when Offline; messages are allowed.
3. Researcher scans QR and contributes observations via mobile voice, photo, video reference or instrument file with provenance.
4. Graph continues unrelated work while request awaits human input. Existing reviewers and sprint activation cannot be bypassed.
## Acceptance
Idempotent request, persisted events, correlated replies, enforced project isolation, outage recovery, no accidental new sprint, no direct instrument actuation.
