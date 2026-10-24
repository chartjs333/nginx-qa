# Requirement and architecture matrix

## Pre-implementation findings and implementation sequence

| Existing surface | Reuse / gap / compatibility decision |
|---|---|
| `ManagedContinuityRuntime.scope_snapshot`, validated `ManagedImportStore.runtime_state` | Exact project/sprint/assignment/agent lookup without publishing, claiming, starting or recovering tasks |
| `verify_role_token`, existing per-phone role secrets | Require existing role credential for every agent API; never use unverified request role/profile |
| Managed sprint lock, SQLite result journal and review lifecycle | Admit dependency under same lock; result guard checks persisted unacknowledged dependencies; no new assignment-state enum |
| Managed worker queues and `active_assignment_ids` | Independent assignments remain schedulable; persistent scoped mailbox instead of ambiguous worker-queue injection |
| Scope acknowledgements/review receipts | Unchanged; channel ack acknowledges only external evidence, never a scope or review decision |
| Inbound proposal Bearer registry/API | Separate principal and routes; reuse protected-path/ACL helpers only; Pending/Play boundary untouched |
| Existing Hub v1 envelope | Use actual float Unix timestamps, payload allowlist and HMAC headers; strict local additive blocking flag is stripped before transport |
| Hub Python registry/grant/cancel/artifact APIs | Missing service endpoints; supply explicit version 1.1 opt-in composition adapter in nginx repo |
| Hub 60-second creation freshness | Original v1 cannot recover long-outage delivery; 1.1 preserves original receipt and assigns first-delivery transport timestamp |
| Hub original introspector checks only active identity | 1.1 scoped introspector also checks returned channel/operation policy on every invocation |
| Hub default human adapter only handles send_message | Composition human adapter permits authorized data/observation requests in existing mobile inbox; bounded live provider contract |
| Branches/PRs/history | Source is main + spec documents. Reuse ancestry, no cherry-picks from unrelated unfinished branches, no merges or source rewrites |

Implementation sequence: typed contracts → durable transport/mailbox → exact identity/policy →
Hub adapter/composition → graph guard → signed callbacks/artifacts → regression/security tests → docs/Draft PR.

## Spec Kit and user requirements

| Requirement | Software implementation/evidence | External or remaining gate |
|---|---|---|
| Inventory existing APIs | Architecture table above; pinned PR #1/#2 code, requirement matrices and VALIDATION read | None for inventory |
| Freeze schemas | Generated request/event/binding/config JSON Schemas, actual v1 callback and explicit 1.1 extension | Cross-repo maintainer acceptance of optional adapter |
| Active-agent request/status API | Scoped role-token routes; persisted managed-state lookup; negative binding tests | Managed execution/reviewer/coordinator bindings supported; legacy runtime remains unchanged |
| Outbox/status/retry/timeout/cancel | SQLite + restart worker, bounded retry/deadline, cancellation tombstones, at-least-once dedupe | Production operational sizing/retention/backup acceptance |
| Inbox/callback | HMAC freshness, strict parsing/limits, correlation, immutable event IDs, out-of-order terminal fencing | Independent penetration review |
| Route answer to source context | Assignment mailbox; exact experiment/checkpoint/sample, origin node/occurrence/revision/workspace/commit and Hub instrument/operator provenance | Real domain/hardware provenance calibration |
| Nonblocking graph and external waits | Multiple requests; independent-agent completion; blocked result until terminal input explicitly acknowledged; two existing reviews still required | Windows native qualification of existing managed runtime |
| Data/image/file/observation | Content-addressed bounded artifacts, integrity check and scoped download; Hub validates text/JSON/PNG/JPEG/WAV | Raw video upload unsupported by pinned Hub; live video session initiation supported |
| Voice/video | Owner presence, consent, bounded retry/fallback; optional idempotent HTTPS provider adapter with configured media | Real provider/browser/phone acceptance |
| NQ-10 scientist roles/intersection | Three descriptive profiles, explicit project/agent/channel policy; Hub researcher/device/plugin/session intersection | Real operator policy and catalogs |
| NQ-11 step-up/grants | Reuse signed IdP/mobile challenge and grant flow; no agent credential/proof forwarding; grant IDs only | Production IdP/card reader provisioning |
| NQ-12 per-invocation grant checks | Actual Gateway revalidation plus live nginx policy, revocation before dispatch/download tests | Distributed-state acceptance |
| NQ-13 plugin discovery/activation | Signed manifest trust, version/hash invalidation, compatible app version and consent tested using actual Hub | Native installation attestation remains absent in Hub; physical Android/iOS acceptance |
| NQ-14 configurable assurance | Per-operation local minimum and auth freshness, intersected with Hub channel AAL | Assurance policy certification is external |
| NQ-15 session lifetime/switch/step-up | Smart-card cryptographic proof fixture; UID/PIN rejection; session expiry/revoke/switch; same-session step-up | Genuine card/reader/biometric hardware acceptance |
| Instrument safety | Device operations read/observe only; actual read-only HTTP GET adapter tested | Physical actuation explicitly not implemented |
| Pending Sprints / managed regressions | Targeted existing suites, full-suite attempt and unchanged-base diagnostic disclosed in VALIDATION | Any skipped/failed platform gate is not counted as passed |
| Isolation | New work branch; temporary databases; unchanged Hub checkout, source and main; no deployment/start/restart of working instances | No production readiness claim |

## Test coverage mapping

| User test | Automated evidence in `integration_tests/test_channel_bridge.py` |
|---|---|
| Full request/reply | real_roundtrip_wait_ack_and_review_gates; read_only_http_observation_roundtrip |
| Async/crash recovery | restart_outage_beyond_v1_freshness_and_lost_ack; callback_outage_and_hub_restart |
| Several agents | two_agents_cannot_read_each_others_research; parallel_assignment_can_finish_while_other_waits |
| Several channels | concurrent_dedupe_multiple_channels (32 concurrent duplicate submissions) |
| External waits | roundtrip; cancel_timeout; full_graph_completes_only_after_existing_two_reviews; active reviewer/coordinator context tests |
| Replay/forgery | duplicate_forged_stale_out_of_order_and_scope_callbacks; duplicate JSON/limits tests |
| Project/sprint/data isolation | parameterized binding_and_introspection_isolation; two_agents; artifact ownership |
| Revoke/session end | grant/session/expiry/plugin/local entitlement/key/workstation revocation; idle/max expiration; revoked artifact access |
| Graph/queue/review compatibility | independent assignment completion; normal two-review terminal flow; existing regression suites |
| Pending/managed regression | existing proposal, pending UI, queue, contract, scope suites; baseline/platform comparison |

ASGI clients execute the real server handlers and storage/authorization. Humans, external provider
responses and hardware measurements are fixtures. Managed continuity's historical fixture retains
its schema-bound Windows paths and uses a disposable native Git-provider injection on Linux; result
Git verification is the existing fixture verifier. These tests establish communication/state-machine
integration, **not** native Windows Git/process or physical device qualification. No contract
simulator substitutes for the Hub implementation.
