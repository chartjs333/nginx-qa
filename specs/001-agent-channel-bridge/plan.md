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


## Agent capability authorization and mobile runtime contract
Define agent capability policy in nginx-qa, and negotiate signed, versioned plugin manifests from Inbound Hub (plugin_id/version, device class, runtime server|mobile, OS compatibility, permissions, allowed operations, artifact integrity). Runtime access is least-privilege: effective grant = agent role entitlement ∩ researcher entitlement ∩ lab/project/device policy ∩ explicit action approval.
Proposed authorization API contract (not implemented): request_grant(agent_id, user_id, device_id, plugin_id, actions, sprint_id, challenge_method), approve_challenge(challenge_id, proof), validate_grant(grant_id, action, device_id), revoke_grant(grant_id). No reusable PIN stored or transmitted to agents; PIN is verified in platform-secured authentication flow, or external one-time code is validated with trusted verifier. Challenges are short-lived, rate-limited and bound to session/device/action, with audit and replay prevention. Support revoke and auto-expiry when checkpoint/mobile session ends.
Support mobile plugin discovery and consented installation/activation: iOS must obey platform code-signing/distribution limits (prefer bundled approved functionality, web/server adapters); Android may use vetted signed dynamic features through approved distribution. Neither QR nor Hub arbitrary URL is executable.


## Configurable session assurance
Extend request/grant policy with `required_assurance_level`, session context with `session_assurance_level`, `auth_method`, `auth_time`, `expires_at` and device binding. On each privileged action compare effective assurance and freshness; if insufficient, request a new verified challenge. Card reader authentication happens once at session entry, not continuously. Authentication must use signed cryptographic challenge-response and trusted verifier, not bare RFID UID. Audit challenge and grant, not PIN or biometric data.
