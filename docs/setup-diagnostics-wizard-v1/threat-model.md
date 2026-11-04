# Setup and Diagnostics Wizard v1 — threat model

Baseline: `89f7f09b318a3d9d78ceb2d8c92b6e730e644626`. This is the SDW-001 design, not a claim that the proposed controls are implemented. Exact contracts and stage owners are in [architecture.md](architecture.md); test gates are in [acceptance-matrix.json](acceptance-matrix.json).

## Assets and trust boundaries

Protect canonical project identity, the one-project slot, immutable source/result/review history, current operator and role authority, repository credentials, bot/webhook/executor/provider credentials, owned processes/leases/workspaces and service responsiveness. The wizard is a private administration feature; the public launcher gateway remains a separate allowlisted ASGI app. A loopback address is a routing restriction, not authenticated operator identity.

Trust boundaries: browser -> authenticated private API -> current project/action policy -> typed catalog -> existing store/importer/continuity/Git/supervisor; trusted provider adapter -> remote service; sanitized structured facts -> untrusted model -> validated advisory renderer. Model output has no reverse capability to invoke apply. Test fixtures are a separate evidence category. Raw configuration, source, logs and external response bodies never cross into the advisor.

## Threats, controls and falsifiable gates

| Threat | Required control | Acceptance |
| --- | --- | --- |
| Public gateway reaches wizard/admin routes through a fallback, encoding or wildcard | Register only on private app; preserve separate gateway app, no proxy; deny all new paths/methods and encoded variants publicly; single lifespan remains | A02, A25 |
| CSRF, stolen URL/plan handle, forged Telegram identity or a revoked browser session | Existing operator session/admin boundary plus current private project/action grant; Origin/CSRF; plan is not bearer authority; bind policy generation and revoke sessions/receipt disclosure | A02, A10, A13 |
| Cross-project discovery or action; alias bypass of busy project | Resolve canonical registry identity server-side; filter reads; include source and target project grants; shared old/new slot guard, no registration endpoint or control DB copy | A03, A14, A19 |
| Read-only GET starts network, migrates a DB, creates a lock directory or releases the project slot | Use initialized read transaction + pure projections, not LauncherOperations.get/project_control or runtime constructors; no Git fetch/status index writes; compare state/queue/lease/config evidence before/after GET | A01, A04, A23 |
| LLM/log/README/service-response prompt injection becomes an operation | Minimize structured input; do not ingest raw prose as policy; strict closed response schema; known check IDs and action allowlist; model cannot call plan/apply; UI click gets a fresh server plan | A17, A18 |
| Secrets or sensitive content leak through prompt, error, URL, evidence, child env or Git | Ref-only trusted adapter; protected control namespace; sanitize before persistence/model/logging; do not expose reference locators in advisor; bounded value-free errors; canary negative tests including encoded/nested values | A07, A17, A18 |
| Existing GEMINI_API_KEY silently delegated to a child or raw clone endpoint reused | Do not broaden process/Git resolver; explicit protected advisor reference and narrow provider wrapper; no raw-key form or silent credential migration; preserve legacy clone functionality | A07, A16, A26 |
| SSRF, DNS rebinding, redirect, inherited proxy, metadata/IP scan | Configured candidate IDs only; existing exact-IP loopback rules for worker; fixed provider host/validated connected peer; no redirects/proxies/arbitrary URLs; no scan; cap reads/time/concurrency | A05, A06, A08 |
| 200/READY/self-reported capabilities are sold as execution proof | Separate passive/connection/execution scope and evidence kind; real current assignment IDs and session; final genuine result commit plus two independent real reviews and native terminal proof | A05, A09, A19, E01 |
| 401 or timeout is called expired token, causing unnecessary key replacement | Classify unknown network vs auth rejection separately; expiration needs authoritative validated signal; no automatic key mutation | A08, A16 |
| Stale plan/policy/source/adapter/host changes between preview and effect | Server fingerprint, expiry, per-target fenced claim and current authorization before each effect; changed binding rejects; no cached permission | A10, A11, A12, A13 |
| Duplicate click/restart/lost reply creates duplicate activation, binding or inference | Immutable request fingerprint + scoped idempotency receipt; shared guard; native attempt/session replay; reconcile exact intent after ambiguous response; no blind retry or new key | A11, A12, A20 |
| New private wizard owner is represented as a fake Telegram Principal | Explicit versioned private_operator owner and transport-neutral shared adapter; preserve Telegram authorization path; old absent metadata retains behavior; no forged user/chat/grant | A13, A14, A26 |
| Browser operator impersonates Coordinator/reviewer to recover | Wizard exposes only supported native authority; default recovery.open is information/navigation; no synthesized role credential, DONE or APPROVE; actual independent reviews at exact commit | A15, A19, A20 |
| Stop kills another process, releases foreign leases, erases pending review history | Existing stop fence, same external session, supervisor identity/death proof and shared-index checks; retain STOP_PENDING on uncertainty; no direct PID kill/SQL mutation/history deletion | A15, A20, A24 |
| Self-test occupies current development project or false fixture success | Separate existing allowed test project/profile; no aliases; project guard applies to native old starts too; fixture client and reviewer evidence explicitly MOCK; external gate distinct | A14, A19, E01 |
| Slow probes/model calls block engine loop or canceled callers free concurrency early | Bounded admission/body/time; worker completion owns permit; drain on shutdown, total deadline beyond per-read socket timeout; no network under global writer lock | A06, A12, A23, A25 |
| Model/provider output becomes HTML/script or unsafe external link | textContent/escaped fixed UI components; CSP; no Markdown HTML, arbitrary redirects or executable snippets; fixed help destinations | A18, A21 |
| Cache hides current state, revocation or corrupt ownership | Cache only derived facts, partition by policy/source/project; current auth every read, fresh authoritative apply; corrupt/stale observations remain UNKNOWN/BLOCKED | A03, A04, A10, A23 |
| Destructive recovery disguised as setup | Small typed catalog; no shell/SQL/env/file operations; webhook_registration/setWebhook and tunnel monitor are not probe helpers; absent safe API means unsupported | A22, A24 |

## Credentials and provider scope

The existing trusted resolver accepts only `env:NGINX_QA_LAUNCHER_CONTROL_*`; it is not a general vault implementation. The wizard may report whether an approved reference is configured using a trusted metadata interface, without enumerating all environment names or reading secret values into the snapshot. Secret resolution occurs only inside the trusted, authorized transport immediately before a call. Never include keys in model context, operation arguments, child environment or result evidence. Preserve the existing scope namespace and denial of control aliases through both source locators and destination environment names.

Provider errors are mapped to fixed codes before logging/return. Do not preserve raw exception strings or credential-bearing request URLs. Adapter-defined metadata calls have explicit bounds and expected response fields; getWebhookInfo never calls setWebhook. The advisor's budget is a server policy, not a model promise. Where provider token/cost accounting is unavailable, say UNKNOWN and enforce conservative request caps, rather than claiming a monetary guarantee.

## Transactions, lifetime and recovery

Only additive wizard plan/receipt/binding metadata may be stored beside the existing engine. It does not confer assignment ownership. External side effects need a durable intent before the call, exact scoped replay/reconciliation afterward and current authority for any next effect. Cancellation/disconnect is not proof a synchronous worker stopped. Unknown outcome retains the intent and resource reservation; no background wizard loop schedules graph work or retries unauthorised effects.

Passive GET captures one coherent already-initialized store view without mutation. Filesystem observations are bounded and owned; handle missing/corrupt/non-atomic data as unknown. All logs/events are capped and redacted, with retention for wizard metadata only; retention never deletes execution history. The model sees the minimum current facts, not the durable receipt store.

## Residual risks and release decisions

A configured external worker can lie about READY/capabilities. The protocol's session/descriptor checks prevent accidental misbinding, not malicious execution; real task/review provenance is a separate gate, and configured code remains trusted code rather than an OS sandbox. A compromised local machine/operator can read its own credentials; this design prevents unintended disclosure and delegation, not host compromise. An external model may give poor advice despite valid JSON; validated facts and explicit operator decisions remain authoritative.

This inventory did not inspect live credentials, probe providers or test a real background worker. Existing fixtures are useful regression evidence only when actually run at a recorded commit. Real worker+two-reviewer self-test (E01), real configured advisor roundtrip (E02), private browser flow (E03) and final release/review provenance (E04) remain NOT_RUN until actual evidence exists. Missing external configuration is a precise Coordinator blocker, never permission to weaken these gates.
