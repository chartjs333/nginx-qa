# Plan: nginx-qa
## Boundary
Existing Pending Sprints API remains unchanged. Add runtime communication interface to active-sprint agent surface. Proposed routes (not current API):
POST /api/v1/projects/{project_id}/sprints/{sprint_id}/agents/{agent_id}/channel-requests
GET /api/v1/projects/{project_id}/sprints/{sprint_id}/channel-requests/{request_id}
POST /api/v1/integrations/inbound-hub/channel-events
## Architecture
Validate agent's active assignment and channel authorization; create durable outbox request with idempotency and lifecycle; dispatch to Hub; authenticate signed Hub events; durable inbox, dedupe, persist data references and feed reply to agent's queue. Represent dependency WAITING_FOR_EXTERNAL_INPUT; never change Play/Start or review gates. Use typed versioned JSON schemas and explicit errors.
## Validation
Unit/contract tests, unauthorized project, replay, offline no-call, QR spoofing, duplicate replies, restart, timeouts, media limits, prompt injection, terminal sprint, cross-repo simulator E2E.
