# Agent Channel Bridge: independent HTTP services

This correction is based on Draft PR #13, not main. nginx-qa and Inbound Hub own
separate state and communicate only through the documented HTTP/HTTPS surface.
No Hub package is imported into nginx, no Hub database or artifact directory is
opened by nginx, and no repository layout is part of runtime configuration.

## Compatibility conclusion

**Full compatibility with stock Inbound Hub PR #2 is blocked.** The nginx 1.1
client requires `POST /v1/nginx-qa-bridge`. Stock Hub commit
`d31f6bd54f70a7e7e2e4295b43424b81dc272155` exposes only
`POST /v1/channel-requests` for service intake. Its mobile API uses researcher
session cookies and is not a service API. nginx must not impersonate a researcher
or read the Hub's database to fill these gaps.

The original PR #13 companion implemented 1.1 by importing Hub internals and
accessing its SQLite tables. It violated the independent-service boundary and is
retired. `python -m nginx_qa.channel_hub_service` now exits with a migration error
before starting anything. The module `channel_hub_adapter` now contains the
existing HTTP client, with stricter receipt/response validation. The public
`nginx_qa.channel_bridge.HubClient` import remains compatible.

No automatic downgrade to v1 is provided. Stock v1 rejects envelopes older than
60 seconds before deduplication, lacks the required service endpoints, and its
active-only introspector does not consume operation/channel entitlement fields.
A fallback would weaken authorization or incorrectly claim recovery support.

## Versioned remote 1.1 contract

Configure only `hub_url` in the existing protected nginx bridge policy and the
nginx-owned `NGINX_QA_CHANNEL_TRANSPORT_KEY`. HTTPS is required for this client;
redirects, environment proxies and payload-selected destinations are disabled.
The request is canonical JSON:

```json
{"schema_version":"1.1","action":"submit","payload":{"schema_version":1}}
```

`payload` above is abbreviated: submit/validate/status/cancel use the full
`CreateRequest.wire()` v1 envelope, without nginx's `blocking` field. Requests
carry `X-Timestamp` and `X-Signature`, HMAC-SHA256 over timestamp + `.` + exact
body bytes. Responses must be JSON objects with `schema_version: "1.1"` and:

| Action | Payload / required result |
| --- | --- |
| discover | Four-field project/sprint/assignment/agent binding; `channels` array with channel/project IDs and capabilities |
| submit | Full v1 envelope; `request_id` must equal the submitted request ID |
| validate | Full envelope; `authorized` must be boolean true; false denies |
| status | Full envelope; one documented request status, or `unknown` |
| cancel | Full envelope; `status: cancelled`, `remote_effects_reversible: false` |
| artifact | `{request, digest, offset}`; `{sha256, size, offset, data}` with bounded base64 chunks; nginx additionally checks content hash and scope |

The missing remote implementation must preserve immutable request bytes and
idempotency across lost HTTP acknowledgements and delays beyond 60 seconds;
authorize each action and dispatch against current agent/channel/project,
researcher/session/plugin/device/assurance policy; fence cancel-before-submit;
and expose only request-bound artifacts. These are **requirements**, not claims
that stock Hub already implements them. No server-side 1.1 replacement is
implemented inside nginx.

A 404/405/501 returns `CHANNEL_HUB_CONTRACT_UNAVAILABLE` (502), not a permissive
fallback. Invalid 2xx receipts remain retryable transport errors, so nginx's
outbox is not falsely marked sent. Authentication/authorization denials remain
terminal; cancellation intent remains durable until acknowledged.

## nginx API and asynchronous state

Existing PR #13 routes are unchanged. Let
`A=/api/v1/projects/{project_id}/sprints/{sprint_id}/agents/{agent_id}`:

| Route | Purpose |
| --- | --- |
| `GET A/channels` | Remote capabilities intersected with local policy |
| `POST A/channel-requests` | Admit to nginx-owned durable outbox |
| `GET A/channel-requests` | Exact assignment mailbox |
| `GET /api/v1/projects/{project_id}/sprints/{sprint_id}/channel-requests/{request_id}` | Status/events with exact agent/assignment binding |
| `POST A/channel-requests/{request_id}/{ack,cancel}` | Resolve local dependency or cancel |
| `GET A/channel-requests/{request_id}/artifact` | Authorized HTTP artifact retrieval and integrity check |
| `POST /api/v1/integrations/inbound-hub/agent-introspection` | Scoped Bearer-token introspection |
| `POST /api/v1/integrations/inbound-hub/channel-events` | Bounded HMAC-signed callback; duplicates handled by event ID and fingerprint |

Callbacks preserve project, sprint, assignment, agent, correlation, experiment,
checkpoint and sample provenance. Late/duplicate/out-of-order deliveries do not
resurrect terminal requests or bypass review gates. Local outbox/inbox tables
remain inside nginx's own managed database. This is not shared service storage.

Scientist profiles describe requested capabilities and do not grant permissions.
Physical device actuation remains excluded. Secrets/PIN/biometric/card material
are not accepted in the agent payload or callback metadata.

## Test boundary

`tests/test_channel_http_contract.py`: malformed receipts, headers/signatures,
redirects, bounds, retired composition and no internal Hub imports.

`integration_tests/test_channel_bridge.py`: the real nginx runtime/API over HTTPS
against a **separate contract-simulator process**, including retries, restart,
callback outbox recovery, concurrency, origin isolation and existing review gates.
These tests do not qualify a real remote Hub 1.1 implementation.

`integration_tests/test_hub_public_http.py`: a wheel built from unchanged Hub PR #2
runs through its public executable in a separate process. The controller provisions
only that process's private config/DB/artifact paths; none are supplied to nginx.
All exercised service/browser interactions use HTTP/HTTPS, including the actual
Hub callbacks and Ed25519-signed test researcher login. A loopback TLS proxy and
private test CA provide HTTPS; no TLS verification bypass is used.

The suite explicitly detects missing 1.1/freshness/operation-scope features. A
passing incompatibility-detection test is not a passed compatibility gate.

```sh
python scripts/run_channel_bridge_checks.py --hub-wheel /artifacts/inbound_agent_channel_bridge-0.1.0-py3-none-any.whl
```

See root `VALIDATION.md` for exact build/test commands and results. No path audit,
full regression, live service access, merge or deployment is part of this work.
