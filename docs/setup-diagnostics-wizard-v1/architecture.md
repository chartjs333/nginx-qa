# Setup and Diagnostics Wizard v1 — architecture and inventory

Status: **SDW-001 architecture deliverable; proposed SDW contracts, not implemented features or release acceptance.** Baseline inspected: `89f7f09b318a3d9d78ceb2d8c92b6e730e644626`. No provider, Telegram, tunnel or worker endpoint was probed during this inventory. Runtime credential values and configuration files were not read. Configuration names below are source-code evidence only.

The wizard belongs to the existing private nginx-qa Web UI. It first helps an operator discover an existing executor, test its configured connection, and run one genuine small Managed Sprint in an explicitly authorized test project. It then diagnoses Git, credentials, Telegram, tunnel/webhook ownership and recovery. The same UI remains available for later diagnostics. The deterministic path works without an LLM. An optional model explains sanitized facts and proposes catalog actions; it never grants permission, runs tools, changes status or schedules assignments.

## 1. Verified inventory and reuse seams

Line references refer to the baseline above, not future implementations.

| Capability | Actual source and contract | Reuse / boundary |
| --- | --- | --- |
| Private UI and operator authentication | `nginx_qa/scope_api.py:28` `register_scope_api`; `:52` operator closure, exported as `host.scope_api_operator` at `:69`; session routes `:105`, `:127`, `:145`; `/execution` at `:687`; `nginx_qa/static/execution.js:104,277` | Reuse private session pairing, admin-token verification, Origin and CSRF semantics. New wizard reads also require authentication and an explicit current project/action policy; existing observability reads are not sufficient authorization. GETs using the existing operator closure must send CSRF too. Do not invent a Telegram user/chat for a browser operator. |
| Canonical project and Git registry | `main.py:1893` `read_git_config_file`; `:4389` `project_registry_from_config`; `:6453` `project_for_group_api`; `:36491` `managed_repository_registry_for_project` | Existing canonical phone and configured `RepositorySpec` remain authoritative. Never call `resolve_or_create_project` to diagnose. New bounded registry reader must distinguish missing from malformed configuration: the old JSON reader returns `{}` for both. Expose selected safe metadata, not entire registry entries. |
| Native start and preparation | `main.py:36707` `execute_managed_project_sprint_start`, POST start-from-git `:36822`; `nginx_qa/managed_import.py:7195` `prepare`, `:7201` `activate_prepared`, `:7209` `start` | These stay the only execution path. Source/ref/manifest checks, ownership and schema preflight remain mandatory. Wizard planning does not call any of these methods. |
| Shared project slot | `nginx_qa/managed_import.py:2739` `assert_launcher_slot_available`; `nginx_qa/telegram_launcher_store.py:98` `reserve`; existing managed single-active index | One reservation/active sprint per canonical project across old and new ingress. Reuse and, where necessary, generalize this common guard for a private wizard owner. No separate execution DB, aliases or shadow scheduler. |
| Existing executor bridge | `nginx_qa/telegram_launcher_execution.py:66` `ExistingClientHTTPAdapter`, `:45` `descriptor`, `:54` `validate_receipt`, `:120` `executor_for`; `docs/telegram-managed-sprint-launcher-v1/launch-modes.md:23-47` | Protocol `nginx-qa-managed-client-v1`; authenticated explicit-IP loopback HTTP; redirects/proxies disabled; 10-second call timeout and 32-KiB response cap. READY, ACCEPTED and STOPPED have exact descriptor/session bindings. This is a client-side bridge, not a bundled executor. |
| Executor launch/cancel | Same module `:133` `launch_sync`, `:184` `stop_sync`; `nginx_qa/managed_cancellation.py` `begin_cancel`, `finalize_cancel`, `assert_no_live_resources` | Preserve request lock, current permission rechecks, original session, stop fence and native process death/lease proof. Do not equate READY with work, ACCEPTED with completion or STOPPED acknowledgement with cleanup. |
| Actual worker implementation | Tracked `main.py`, `nginx_qa`, `scripts`, `tools` contain no server implementing `/managed-client/prepare`, `/start`, `/stop`; `tests/test_telegram_launcher_launch_modes.py:24` contains `FakeExistingClient` | **Gap:** no bundled compatible background agent service was found. The actual Architect identity request proves this interactive client can obtain its assignment; it does not establish an unattended HTTP service. A previously installed external client may exist, but availability is UNKNOWN until an authorized configured-endpoint probe and real task evidence. Never promote the fixture to production. |
| Existing hosted-model transport | `main.py:3416` `call_gemini_clone_model`, `:3514` `call_gemini_models_api`, `:3567` `build_gemini_models_response`, `:3591` `build_agent_clone_preview`; routes `:39557`, `:39569` | Gemini integration exists for profile cloning/model enumeration. It is not a safe advisor adapter: clone-only output shape, raw key input/query, long blocking calls, unbounded response reads and provider error bodies. Factor a bounded, sanitized provider transport with injected trusted credentials; preserve legacy behavior. Do not feed diagnostics into clone-preview or expose its credential input as the wizard's setup form. No other model transport was found in the inspected runtime. Actual configured Gemini availability is UNKNOWN. |
| Secret separation | `nginx_qa/control_secrets.py:8,19,29`; `main.py:36602` child resolver and `:36667` Git resolver | Protected scope/launcher control namespaces and raw Telegram names are denied to child/Git aliases. `trusted_control_resolver` supports only `env:NGINX_QA_LAUNCHER_CONTROL_*`. Other providers are not automatically supported. Keep new advisor control credentials inside this boundary; never broaden the child resolver. |
| Observability and cheap reads | `nginx_qa/execution_observability.py:328` `project_execution`, `:315` exact-result review gates; `nginx_qa/execution_read_cache.py:86` `ExecutionReadGate`, `:33` historical cache; `scope_api.py:159,633` | Reuse detached projection semantics and cancellation-safe admission. Cache is derived data, never authority. Do not expose full scope/task/log payloads to the advisor. |
| Dangerous apparent getters | `ManagedImportStore.project_control` `managed_import.py:858-906` may reconcile and write; store readers call `_ensure_initialized` `:563`; `LauncherOperations.get` `telegram_launcher_store.py:181` calls `_refresh` `:145-178`, which can UPDATE and retire a slot | Passive wizard GET must NOT call these unconditionally. Add an explicitly read-only snapshot interface on the already initialized store, using one read transaction and existing pure validation/projection. Missing/uninitialized/migration-needed storage returns UNKNOWN/BLOCKED, not initialization or repair. No raw SQL from browser/model. |
| Git provenance | `nginx_qa/git_provider.py:910` `resolve_repository`, `:1602` `ensure_mirror`, `:1946` `create_launcher_manifest_commit`, `:1991` `publish_launcher_manifest`, `:2034` `resolve_commit`, `:2063` `read_blob` | Provider/mirror creation, fetch and publication have effects; not passive GET work. Passive facts use already owned immutable objects/read-only checkout inspection, with optional Git locks disabled. Explicit connection checks may use a bounded configured transport; read access never proves push permission. No push-to-test-rights operation. |
| Tunnel/webhook ownership | `nginx_qa/launcher_origin.py:12` `owned_origin`; `launcher_tunnel.py:33` `read_receipt`, `:68` `request_stop`, `:89` `webhook_registration`, `:123` `monitor` | Origin freshness already checks a 15-second heartbeat and owned initialization/upstream. Keep the underlying reason when receipt is absent/stale/foreign; `None` alone cannot explain it. `webhook_registration` calls setWebhook and must never be used as a diagnostic probe. No PID scan/kill, download or automatic tunnel launch. |
| Public/private separation | `nginx_qa/launcher_host.py:151-200` serves two sockets under one private lifespan; `launcher_gateway.py:74` creates a separate app, routes `:231-263` | Wizard routes/assets/API exist only on the private app. No public gateway forwarding, fallback, wildcard proxy or new lifespan. Test all methods and path variants on the public app. |

Safe configuration names found in code: `NGINX_QA_LAUNCHER_EXECUTOR_URL`, `NGINX_QA_LAUNCHER_EXECUTOR_SECRET_REF`, `NGINX_QA_TELEGRAM_LAUNCHER_ENABLED`, `NGINX_QA_TELEGRAM_LAUNCHER_POLICY_JSON`, `NGINX_QA_LAUNCHER_BOT_SECRET_REF`, `NGINX_QA_LAUNCHER_WEBHOOK_SECRET_REF`, `NGINX_QA_LAUNCHER_ORIGIN_STATE`, `NGINX_QA_LAUNCHER_PUBLIC_ORIGIN`, `NGINX_QA_GATEWAY_PORT`, `NGINX_QA_INSTANCE_ID`, `GEMINI_API_KEY`. A name is not evidence that a value exists, is valid or grants a capability. The legacy Gemini key is not automatically protected against child alias lookup; the advisor must require an explicitly configured protected control reference, without silently copying/migrating the key.

## 2. Components and private API

Proposed implementation units (SDW-002 onward):

| Unit | Responsibility / existing seam |
| --- | --- |
| `nginx_qa/setup_diagnostics.py` | Pure check normalization, freshness, dependencies and capability/action projection; injected bounded read adapters. No construction of main/runtime on demand. |
| `nginx_qa/setup_diagnostics_api.py` | `register_setup_diagnostics_api(host)` after `register_scope_api`; authenticated private endpoints below. Inject existing lifespan-owned store, registry reader and operator callable. |
| `ManagedImportStore.diagnostic_snapshot` | New read-only transaction over current control/runtime/resource/launcher rows; reuse pure invariants. No schema initialization, migration, outbox delivery or retirement. Existing writer APIs unchanged. |
| `nginx_qa/setup_operations.py` | Server-owned action catalog, typed plan/apply and small durable receipts in the existing managed store. No task queue or graph transitions. |
| `nginx_qa/setup_worker_adapter.py` | Inspect preconfigured candidate IDs, bounded capability probe and binding selection; wrap the existing execution protocol. No network scanning or new model loop. |
| `nginx_qa/setup_advisor.py` | Minimal fact projection, configured provider adapter, strict response validation and explanation only. No access to operation apply callable. |
| `nginx_qa/static/setup.{html,js,css}` | Russian private UI linked from `/execution`; reuse session flow, textContent rendering, CSP and current request-generation handling. |

Proposed routes, all gated by feature configuration, private listener, existing operator authentication and current server-owned project/action grants:

| Method/path | Contract and effects |
| --- | --- |
| GET `/setup` and `/setup/assets/{name}` | Fixed assets only; no inline model HTML. Private shell; protected data remains API-only. |
| GET `/api/v1/setup/projects` | Only registered canonical projects allowed for the authenticated operator. No creation or scan. |
| GET `/api/v1/projects/{project_id}/setup/snapshot` | Passive `DiagnosticSnapshot`; no network, new directories, DB writes, child starts, Git fetch or mutation. Optional instance selection is a server-verified opaque ID. |
| POST `.../setup/plans` | Strict `PlanRequest` -> `ActionPlan`; resolve targets and current preconditions, show effects. Store only bounded redacted plan metadata, no execution. |
| POST `.../setup/plans/{plan_id}/apply` | Strict `ApplyRequest` -> existing receipt or `OperationReceipt`; current authorization and fencing required. |
| GET `.../setup/operations/{operation_id}` | Read-only status projection; never retries effects or advances the engine. |
| POST `.../setup/advice` | `AdviceRequest` -> validated `AdviceResponse`; explicit Ask, current advise grant and configured budget. No apply; model requests are not triggered by render/refresh. |

No generic shell, URL, SQL, filesystem edit, environment dump, secret-value or process-ID argument exists. New operator policy is a deny-by-default wizard policy, separate from Telegram ACL. Existing operator pairing establishes actor authentication, not a project grant. Recheck policy generation on every read and action; invalid/missing policy disables the relevant capability. A browser session must be tied to current authorization generation so logout/revocation prevents reuse; do not assume the existing in-memory authenticated flag supplies revocation by itself.

## 3. Versioned data contracts

The normative structural draft is [contracts.schema.json](contracts.schema.json), Draft 2020-12 with separate `$defs` entrypoints. It is an architecture artifact, not an installed runtime validator yet. SDW-002/004/005 must install and enforce the corresponding versioned schemas before exposing their APIs. All closed objects reject unknown fields, duplicate keys, non-finite numbers, excess depth and oversized bodies before schema traversal.

`DiagnosticSnapshot` carries schema/version, snapshot digest, canonical project, optional exact instance, generated time, source code/config versions, authority fingerprint, checks and available action IDs. Each check has unique `check_id`, one of `PASS/WARN/FAIL/UNKNOWN/NOT_CONFIGURED/BLOCKED`, scope (`passive/connection/execution`), observation/expiry/freshness, source adapter/version, safe typed evidence, error code, capability IDs and next-action IDs. Missing observation uses null timestamps and UNOBSERVED; it is not timestamp zero or success. Recompute freshness from the clock and server TTL, never from caller/model claims. Expired PASS is historical evidence, not current readiness.

| Status | Meaning |
| --- | --- |
| PASS | This exact bounded check has positive evidence, for its declared scope and freshness only. |
| WARN | Observed degradation/nonblocking limitation; not unverified success. |
| FAIL | A deterministic assertion was tested and failed, e.g. invalid protocol or dirty owned checkout. |
| UNKNOWN | No trustworthy observation, stale source, timeout, DNS/network failure or missing evidence. |
| NOT_CONFIGURED | The required approved adapter/reference/configuration is absent. |
| BLOCKED | Current authority, foreign ownership, busy project or required prerequisite prevents the operation. |

Errors distinguish `TIMEOUT`, `DNS_ERROR`, `CONNECTION_ERROR`, `AUTH_REJECTED`, `ACCESS_DENIED`, `NOT_CONFIGURED`, `PROTOCOL_INVALID`, `PROBE_UNSUPPORTED`, `SOURCE_STALE`, `PROJECT_BUSY`, `OWNERSHIP_MISMATCH`, `STORE_UNAVAILABLE`, `STATE_CORRUPT` and `UNSUPPORTED_OPERATION`. HTTP 401/403 is authentication/authorization rejection; it never proves expiration. Report explicit provider expiry only if an authoritative, validated provider signal establishes it. Corrupt state fails closed, with a safe error code; it is not repaired by reading.

Evidence is an allowlist of check-specific fact IDs and bounded scalar values, never arbitrary log text. Raw exception/provider bodies, paths, credential locators/values, repository content and request headers are excluded. UI may show an approved loopback endpoint display, provider label, client identity and protocol; advisor projection drops endpoint, canonical project/instance, repo and user identifiers. `evidence_kind=CODE/CONFIG_METADATA/LIVE_PROBE/MANAGED_RESULT/MOCK/NONE` prevents a fixture or source inventory from proving live execution. Every capability points to its supporting check IDs.

`snapshot_id` is SHA-256 of canonical JSON excluding itself. `authority_fingerprint` is a separate server-derived digest of canonical project, host ownership generation, registry/repository/source binding, control/reservation revision, policy generation, selected adapter binding and applicable live target identity. A snapshot hash alone never authorizes a mutation. Apply reloads these authoritative inputs under the owning transaction/lock.

Initial server-owned check catalog (not arbitrary IDs submitted by a client):

| Check IDs | Scope / source | Dependent capability |
| --- | --- | --- |
| `project.identity`, `project.slot` | passive; bounded canonical registry and shared control/reservation snapshot | project selection; a busy slot blocks a new run, not reading history |
| `engine.runtime`, `engine.resources` | passive; validated current state and shared process/port/branch ownership indexes | distinguish active, settled and unknown resource state; never signal or release anything |
| `engine.delivery`, `engine.result_reviews`, `engine.recovery` | passive; owned queue/outbox counts, exact-result review gates, normalized pending recovery IDs/codes | explain waiting for delivery/result/reviews/operator without claiming failure or advancing graph |
| `worker.configuration`, `worker.binding` | passive; approved candidate and selected binding/version metadata | probe availability; absence differs from unobserved connection |
| `worker.connection`, `worker.protocol` | connection; bounded authenticated configured-endpoint probe and strict capabilities, if supported | connection/capability evidence only; not READY or execution proof |
| `selftest.execution` | execution; real native sprint/result/independent review/terminal evidence | live worker proof; MOCK cannot satisfy the product capability |
| `git.source`, `git.publication_authority` | passive; registered/pinned owned Git facts and current explicit grant provenance | source readiness and declared publication permission; neither proves remote write access |
| `git.connection` | connection; explicit bounded configured repository/ref query | observed read/ref access only; no push or permanent fetch required |
| `secrets.references`, `provider.configuration` | passive; trusted existence metadata for allowed references and adapter IDs | do not resolve secret values just to render readiness |
| `provider.connection` | connection; explicit bounded provider metadata probe | authenticated connectivity, not inference quality |
| `advisor.response` | connection; actual bounded Ask response accepted by strict advisor validator | installed advisor roundtrip only; no engine/task success implication |
| `telegram.configuration` | passive; bot/webhook reference metadata and policy shape | explicit bot probe availability |
| `telegram.identity`, `telegram.webhook` | connection; explicit getMe/getWebhookInfo facts and owned-origin comparison | bot identity/current webhook ownership, never automatic setWebhook |
| `tunnel.ownership`, `tunnel.heartbeat` | passive; bounded owned receipt with initialization/upstream and existing 15-second heartbeat rule | current owned-origin evidence only; no proof of mobile/browser reachability |

The minimum worker milestone still requires a valid pinned test source and publication authority before self-test. Those prerequisites are checked as part of its plan; placing the full Git/Telegram/tunnel diagnostic screens later never waives them. Pending delivery/reviews are normal waiting facts when their records are consistent, not automatically FAIL. Conflicting ownership/corrupt state is a blocker; no observations are reconstructed from guessed queue counts.

## 4. Causal workflow, budgets and probes

The wizard UI stage is a projection: `select_project -> discover_worker -> probe_worker -> review_self_test -> observe_self_test -> inspect_integrations`. Return visits can open any diagnostic; missing prerequisites disable only dependent actions. Keep setup stage, check status, operation receipt status and engine lifecycle separate. There is no wizard-specific sprint state machine. Manual Waiting, Running, pending reviews, stop pending and terminal evidence continue to come from the existing engine.

Discovery reads only approved candidate metadata. The existing execution protocol has **no read-only capabilities endpoint**; do not call `/prepare` with invented sprint/assignment IDs to fake a probe. A narrowly versioned optional authenticated `GET /managed-client/capabilities` extension can advertise stable client ID, execution protocol, supported actions, durability/reviewer-isolation declarations and adapter version. Its strict response is `WorkerCapabilities` in the schema. These declarations are connection evidence, not proved behavioral guarantees. A 404 means PROBE_UNSUPPORTED; the original execution protocol is not retroactively changed. READY may be requested only for a real pinned run descriptor at an authorized launch boundary. A bare HTTP 200 proves neither protocol compatibility nor useful work.

Endpoint selection comes only from operator-owned candidate IDs, not browser/model URLs. Executor origin remains explicit HTTP loopback IP and port, no path/userinfo/query/redirect/proxy. Provider probes use an installed adapter's fixed HTTPS origin, explicit destination policy and address validation at connection; no generic URL fetch. Protect DNS resolution and connection from rebinding; disallow redirects and unexpected private/link-local/metadata destinations except the explicitly configured loopback executor. No full-network/port/process scan.

Initial limits: 64 checks/snapshot, 64 KiB snapshot/plan response, 16 KiB action input, 8 KiB advice output, 4 KiB user question, depth 16; one passive capture at a time per app, two total external probe/advice operations and one per project; bounded admission with 429 instead of an unbounded queue. Passive cache is at most 32 projects/2 MiB, default 5 seconds and keyed by authorization/source versions; validate authorization on each hit. Cache is never used for apply preconditions. External probes: 10-second total wall deadline, at most 32 KiB response and no automatic mutation retries. Advisor: at most 30-second total deadline, one inference per click/idempotency key, configured input/output and daily call caps. No claim of monetary token accounting if the existing provider cannot report/enforce it; disable beyond configured conservative call caps instead.

Blocking library socket timeouts alone do not provide total deadlines. SDW-002/005 must implement bounded transport/cancellation and retain admission until underlying work really ends, following `ExecutionReadGate`; do not spawn a new unbounded thread after a client disconnect. Shutdown drains owned work before closing holders. Refresh cancellation never cancels managed engine work.

## 5. Typed actions, ownership and recovery

The catalog is server-owned and versioned. SDW-003 must implement the minimal plan/apply foundation for worker/self-test; SDW-004 extends it to supported recovery. Do not defer the safety boundary until after the first live action.

| Action ID | Typed arguments | Required grant / permitted effect |
| --- | --- | --- |
| `diagnostics.refresh` | none | read; refresh derived passive facts only |
| `worker.probe` | candidate_id | probe; bounded call to that configured client |
| `worker.bind` | candidate_id, probe_receipt_id | configure; select a verified, preauthorized candidate binding; retain prior binding/version, never write raw env/key values |
| `provider.probe` | provider_id | probe; bounded metadata/connectivity check through configured provider adapter |
| `git.probe` | repository_id | probe; bounded read-only query of the registered repository and configured ref, no fetch/push or caller command |
| `telegram.probe` | none | probe; getMe/getWebhookInfo only, safe structured comparison with owned origin |
| `setup.open` | topic enum | read; a fixed private help/setup view, not arbitrary link/navigation |
| `selftest.start` | test_profile_id | selftest + start + any required publication grants on the already registered target test project; native Managed Sprint only |
| `selftest.stop` | self_test_id | stop on the exact owned run; existing client/supervisor cancellation, preserve history |
| `recovery.open` | instance_id | read exact authorized instance context/next steps; no impersonated Coordinator decision |

No generic retry, webhook overwrite, tunnel restart, Git push or secret edit is silently supported. Enable a further operation only when a specific authoritative API, ownership proof and typed contract exist; otherwise return UNSUPPORTED_OPERATION with the fixed setup path. The existing Coordinator owns recovery decisions; the wizard cannot synthesize its role token or submit review/result decisions. The existing legacy Gemini key input is not a suitable reference-management UI; v1 setup instructions must use the operator's existing protected configuration workflow, not collect tokens in advisor chat.

A plan binds actor, canonical source/target projects, action/arguments, resources, required permissions, exact expected authority fingerprint, preconditions, predicted effects, expiry (default 120 seconds, maximum 300) and whether explicit confirmation is required. All network, configuration, start/stop and provider-cost operations require an explicit product click with visible consequences. Development authorization for this sprint is not end-user product consent. GET and model output never imply confirmation.

Apply reauthenticates, resolves all IDs, verifies expiry and current fingerprint/ownership, and atomically claims `(actor, canonical project, idempotency_key)` bound to the entire plan request. Changed reuse is IDEMPOTENCY_CONFLICT; stale plan is PLAN_STALE; current revocation returns ACCESS_DENIED even for receipt disclosure. Store immutable redacted request digest and operation receipt in additive tables in the same existing store. External effects use the existing intent-before-effect and read-back recovery pattern; never hold a global SQLite write lock across network. A durable in-progress receipt plus native attempt/session is resumed, never treated as proof of failure or blindly repeated. If an adapter cannot reconcile an ambiguous effect, keep UNKNOWN/BLOCKED with the same ownership instead of retrying under a new key. Receipt states `PLANNED/APPLYING/SUCCEEDED/FAILED/BLOCKED/UNKNOWN` describe control operations only; SUCCEEDED for Start means native activation/ACK recorded, not task completion.

## 6. Genuine managed self-test

Use a **different, already registered, explicitly allowed test project**, never project 9000 while this development sprint occupies it. A test profile contains an operator-approved repository/source/template, two distinct reviewers, unique safe role phones, fresh topic branches and no arbitrary process declaration. It is not a project alias or copied control DB. Missing profile/client/grants yields NOT_CONFIGURED/BLOCKED with concrete operator setup steps.

Plan shows the exact target, source commit, manifest/checksum digest, branch/role set, publication effect, execution/client policy and stop limitations. Apply uses the ordinary Git provider/provenance and importer VALIDATE/PREPARE/ACTIVATE path, then the existing client's exact descriptor/session and actual initial assignment IDs. It must acquire the same common project slot used by old starts and Launcher. New private ownership must be explicit (`owner_kind=private_operator`, actual actor and policy generation); **do not manufacture Telegram Principal values** to reuse `LauncherOperations`. SDW-003 must factor a transport-neutral reservation/launch adapter with versioned additive owner metadata and legacy-absent compatibility, leaving Telegram `current_principal` checks intact. The shared importer guard must understand this real owner; an adapter-only lock is insufficient. Any required schema evolution needs both independent reviews before activation.

Wizard receipts link the native operation/attempt/sprint/session; they do not contain or manufacture tasks, reviewer decisions or a second schedule. Progress reads native assignment/result/review/transition evidence. Live proof requires a genuine worker-produced result commit, exact from/result/branch provenance, **two distinct genuine independent reviewer decisions on that result**, completed native terminal evidence and cleanup. Deterministic fixture reviewers prove contracts only. Health/READY/ACCEPTED are separate intermediate checks. A real writer with fake reviewers does not satisfy the live gate. Stop keeps STOP_PENDING/reservation until the same external client quiesces and native ownership/death checks settle resources. No history cleanup is part of the wizard.

## 7. Advisory model contract

Reuse the existing configured provider through a narrow adapter, not a new autonomous agent loop. Input contains pseudonymous check IDs/status/scope/freshness/error codes, bounded redacted facts, schema versions, permitted action IDs with approved opaque candidate IDs and a sanitized question. Repository/log/service/user prose is untrusted data; no full files, paths, environment, credential references, secrets or direct tools are exposed. Do not call legacy clone-preview, auto-select a new provider, buy credits or mint a key.

Output is `AdviceResponse`: snapshot binding, bounded explanation, claims referencing existing check IDs with explicit uncertainty, and typed proposed actions from the current catalog. Reject unknown/missing check IDs, unknown actions/arguments, fabricated evidence, executable text, extra keys and stale snapshots. An output saying PASS/DONE does not change a check or engine state. No numeric confidence can override uncertainty. UI labels model inference separately from verified facts and gives deterministic next steps if no provider, timeout, invalid output or policy refusal. A model suggestion only opens a server-generated plan; no auto-apply.

## 8. Implementation order, acceptance and remaining gaps

| Stage | Required deliverable / dependency |
| --- | --- |
| SDW-002 diagnostic-core | Read-only snapshot/store adapter, versioned contracts, private auth/project policy, safe source/error projection, bounds/cache/admission; passive executor inventory first |
| SDW-003 worker-connection-selftest | Candidate/probe binding, optional real-client capability extension, minimum plan/apply, explicit private owner/shared-slot adapter and one native test-profile flow; external client remains a separately required existing integration |
| SDW-004 safe-operations-recovery | Full bounded action catalog, stale/current-policy fences, receipt replay/restart, fixed setup/recovery views; unsupported effects remain unavailable |
| SDW-005 llm-advisor | Hardened configured provider seam and strict advisory parser; deterministic fallback; real provider evidence only if available |
| SDW-006 wizard-ui | Private Russian reusable setup/diagnostics flow, clear evidence levels, plan/confirm, busy/stale/revoked UX, keyboard/mobile-width/XSS coverage |
| SDW-007 qualification | Relevant isolated tests and exact-commit evidence, engine/Launcher regressions, genuine external proofs separately labeled |
| SDW-008 release-runbook | Installed-version setup/rollback, actual external evidence and PR/reviews; GO only for all mandatory gates, otherwise NO_GO through Coordinator |

[acceptance-matrix.json](acceptance-matrix.json) maps each requirement to stages, concrete assertions and existing regression tests where they actually exist. New test identifiers are not falsely presented as implemented methods. Architecture checks are distinct from product tests; all implementation/external cases initially remain NOT_RUN.

Known gaps to resolve: no bundled HTTP executor; no standalone capabilities endpoint; no transport-neutral private wizard owner in the launcher reservation/launch adapter; no pure unified diagnostic snapshot; no strict advisor transport/parser or reference-management wizard; actual worker/provider/test-project setup unknown. These do not block a truthful deterministic UI or this architecture deliverable. They do block claims of live automated execution/LLM readiness. All current protected roots, historical sprint evidence and the previous launcher's external NO_GO remain unchanged.

Execution constraint for downstream implementation: this sprint's source `files[]` pins several files likely to change, including `managed_import.py`, `managed_continuity.py`, `telegram_launcher_execution.py` and `launcher_gateway.py`. The engine rechecks declared artifacts at each future assignment's initial HEAD. A successor after such a change can therefore receive `ASSIGNMENT_ARTIFACT_MISMATCH`. Preserve the actual result and route through the existing Coordinator's authorized future-only repair, with the real repair-source commit and verified replacement checksum metadata before the affected successor runs. Never remove this guard, invent hashes or edit durable graph state by hand. This first architecture node adds only new documentation and leaves all source-bound artifacts intact.
