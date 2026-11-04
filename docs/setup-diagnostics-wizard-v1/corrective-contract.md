# Corrective implementation contract

This document supersedes the release-readiness claims in the original SDW-001–009
evidence. Those historical commits recorded test runs, but their tests did not
establish working native self-test execution, actor ownership, or exact UI/API
contracts. Do not treat the old acceptance matrix as qualification of this code.

## Private operator setup

The wizard is served at `/setup` on the private listener. The public Telegram
gateway does not expose it. Use the existing GET `/api/v1/operator/session`
pairing flow. A privileged local operator approves the browser's pairing code
through POST `/api/v1/operator/session/authorize`, with the existing admin header
and body `{"pairing_code":"CODE_FROM_BROWSER","operator_id":"madoev"}`.
The optional name is selected only by the authenticated administrator. It binds
the session to `operator-named-madoev`; reauthorizing that name after restart
restores ownership of its plans, bindings and test instances. Different names
remain different owners. Unnamed legacy sessions may read/probe but cannot
create persistent worker bindings or start self-tests.

The browser never receives the admin credential. Operator cookies remain
HttpOnly and SameSite=Strict; requests carry the session CSRF token. Logout or
expiry requires pairing again. Named identity is not a browser-supplied login.

Set `NGINX_QA_SETUP_POLICY_PATH` to an operator-owned JSON file, outside agent
workspaces. By default it is `setup-policy.json` in the owned staging state root.
No policy means no project permissions. Example grants (adjust registered IDs):

```json
{
  "schema_version": 1,
  "grants": [{
    "actor_id": "operator-named-madoev",
    "projects": ["9000", "9010"],
    "actions": ["read", "advise", "diagnostics.refresh", "worker.probe", "worker.bind", "git.probe", "provider.probe", "telegram.probe", "setup.open", "selftest.start", "selftest.stop", "recovery.open"]
  }],
  "selftest_profiles": {}
}
```

Every request reads current grants. Plan/apply also fence the owner, project,
issued snapshot, URL plan ID, native state, registry and policy. Plans expire in
120 seconds. Operation intents and exact idempotency mappings are atomically
persisted before effects. An interrupted effect is reconciled, never blindly
repeated. Reuse the original apply key after a lost response.

## Real self-test

Register a separate disposable Git repository under a separate canonical project.
An alias of the development repository is rejected. Grant the named operator
access to both source and test project. Configure and bind the authenticated
client described in [managed-codex-client.md](managed-codex-client.md).

A self-test profile contains exactly `target_project_id`, `repository_id`, `ref`
(a published full Git ref), `commit` (its exact 40-character manifest commit),
`manifest_path` (repository-relative), and `manifest` (the parsed object).
Publish the manifest as canonical JSON bytes using the existing
`nginx_qa.sprint_types.canonical_json_bytes` helper. The native engine compares
the raw published bytes, pinned commit and manifest hash. The client separately
derives the same native sprint ID from exact publication provenance.

Each immutable profile is one native sprint identity. Repeated delivery of the
same action is deduplicated. A new self-test run needs a newly published manifest
commit and distinct assigned branches, followed by updating the trusted profile
and the client's pinned configuration. The wizard does not silently rewrite
completed sprints or claim a new run for an existing identity.

The private ingress uses the existing shared reservation, native `prepare`,
`activate_prepared`, READY/ACCEPTED client receipts and native cancellation.
No Telegram identity is impersonated; private records cannot produce Telegram
notifications or controls. Start remains APPLYING until the engine records real
completion and reviews. Stop uses the original client binding/session with current
stop permission, even after start permission is revoked. Changed original
transport/credential provenance requires explicit operator reconciliation.

## Evidence and operational limits

- Run one host process per owned state root. Worker binding/advisor caches are
  process-local projections backed by their documented journals; ASGI multiworker
  operation is not supported.
- Passive reads never checkpoint SQLite or create WAL/SHM files. A stable DB is
  copied into RAM, with a shared one-image admission limit across snapshot,
  plan/apply authority, status and recovery readers. DB limit is64MiB; transient
  image memory is approximately128MiB. Active WAL/journal or concurrent capture
  reports unavailable/stale/busy instead of stale success.
- Native validation and resource counts cover current indexed/active/preparing
  sprints. Historical payloads are not revalidated on each diagnostic request.
  Terminal blocked history does not occupy the active slot.
- Local Git metadata does not prove publication rights or remote freshness.
  `git.probe` proves registered remote readability, not push authority.
- The real Codex client initially supports an explicitly admitted `docs-only-v1`
  profile. Coordinator/recovery execution requires an operator. Model processes
  have fixed workspace/sandbox settings, bounded Windows Jobs and no control
  credentials. Runtime result/review APIs own every graph transition.
- Advisor HTTP transport uses explicit provider/model/secret-reference
  configuration, no tools and strict output validation. No provider credential is
  created by the wizard. Codex CLI advisory mode reports
  `CODEX_TOOL_ISOLATION_UNVERIFIED` and uses deterministic guidance; CLI executor
  capability is separate. Missing provider health adapter is explicitly BLOCKED.
- Fixture tests are not live model/tunnel/Telegram evidence. Final qualification
  must identify actual commands, commits, native receipts and any external limits.

The isolated launch profile is `setup-diagnostics-wizard-v1`: checkout
`D:/nq-sdw-launcher`, branch `codex/sdw-qualified-host-20261104`, state `C:/nqt37`,
private port18038, child ports18800–18849. Existing control/release instances
remain separate. Run `run_staging.ps1 -Action Setup|Check|Start -Profile
setup-diagnostics-wizard-v1 -ExpectedCommit <exact qualified SHA>`.
