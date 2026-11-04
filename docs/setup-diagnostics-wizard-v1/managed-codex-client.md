# Existing Codex client adapter

This optional Windows service connects an installed, already authenticated Codex
CLI to the existing nginx-qa Managed Sprint Engine. It does not import sprints,
choose graph transitions, create assignments, or keep a second execution database.
The native engine remains authoritative. The local journal contains client
session receipts, owned process attempts, and exact retryable submission payloads.

The initial admitted execution profile is `docs-only-v1`. An operator must pin an
imported manifest and explicitly allow the test project, native workspace
ancestor, branch prefix, exact origin URL, and documentation paths. Ordinary
assignments and two independent reviews are supported. Coordinator/recovery
assignments fail closed with `CLIENT_ROLE_REQUIRES_OPERATOR`. This is an explicit
limit, not a completed recovery claim.

## Operator configuration

Save the configuration outside agent workspaces and start the service using the
same signed-in Windows account as the installed CLI:

```powershell
C:/Python312/python.exe -m nginx_qa.managed_codex_client --config C:/operator/client.json
```

When launching with PowerShell `Start-Process`, use `-WindowStyle Hidden`.

```json
{
  "state_dir": "C:/operator/codex-client",
  "listen_url": "http://127.0.0.1:18039",
  "engine_url": "http://127.0.0.1:18035",
  "control_secret_ref": "env:NGINX_QA_LAUNCHER_CONTROL_CLIENT_TOKEN",
  "node_path": "C:/Program Files/nodejs/node.exe",
  "codex_script": "C:/Users/madoev/AppData/Roaming/npm/node_modules/@openai/codex/bin/codex.js",
  "role_timeout_seconds": 600,
  "role_secret_refs": {},
  "projects": {
    "9010": {
      "profile_id": "docs-only-v1",
      "manifest_path": "C:/operator/immutable-selftest-manifest.json",
      "manifest_sha256": "REPLACE_WITH_SHA256_OF_EXACT_FILE_BYTES",
      "repository_id": "REPLACE_WITH_NATIVE_REGISTERED_REPOSITORY_ID",
      "manifest_commit": "REPLACE_WITH_FULL_NATIVE_MANIFEST_COMMIT",
      "git_manifest_path": "orchestration/selftest/sequential-sprint.json",
      "workspace_root": "C:/native-managed/projects/9010",
      "branch_prefix": "agent/setup-selftest-",
      "repository_url": "REPLACE_WITH_EXACT_CONFIGURED_ORIGIN",
      "allowed_document_paths": ["docs/setup-selftest.md"]
    }
  }
}
```

Configure a dedicated protected control token in the service environment.
The setup host points `NGINX_QA_LAUNCHER_EXECUTOR_URL` at this service and
`NGINX_QA_LAUNCHER_EXECUTOR_SECRET_REF` at its protected token reference.
For scope-controlled phones, add each native role's protected
`env:NGINX_QA_SCOPE_CONTROL_...` reference to `role_secret_refs`; no token
value is accepted over the HTTP configuration protocol or passed to CLI children.
Existing Codex ChatGPT login is reused; no new API key is required.

The example ports and project are placeholders. This module does not register
projects, allocate production ports, provision credentials, or select a remote
repository. Configuration files are trusted operator input, not model output.
The admitted sprint ID is derived with the engine's `SprintProvenance` from the
project, registered repository ID, manifest commit, exact Git-relative manifest
path and hash of the pinned manifest bytes. Matching phones alone never authorizes
a different sprint. Both prepare and start reject a provenance mismatch; stopping
an exact already-owned session remains possible after admission policy changes.

## Contract and evidence

All endpoints require the configured Bearer credential. Only literal IPv4
loopback endpoints are allowed. Capabilities are declared adapter guarantees;
they are not proof of task completion.

- `GET /managed-client/capabilities` returns the exact WorkerCapabilities contract.
- `POST /managed-client/prepare` accepts the native descriptor and returns exact
  `READY` only after existing CLI login readiness and admission checks.
- `POST /managed-client/start` requires that session plus actual native initial
  assignment IDs. It returns exact `ACCEPTED` after verifying native identities.
- `POST /managed-client/stop` returns exact `STOPPED` only after the client loop
  and all recorded owned Windows Jobs are quiescent.

Acceptance is delivery acknowledgement, not sprint success. Completion and review
outcomes come from native identity/result/review/observability APIs. Every native
identity and result/review receipt is checked against repository schemas.

Current identity is read through the phone-specific native `whoami` endpoint;
there is no generic repository dequeue. Governed identities supply full effective
scope, which takes precedence and is acknowledged before execution. Ungoverned
revision-1 assignments use the operator-pinned native manifest's exact node tasks.
An updated graph without effective scope is rejected. Root, branch, repository,
scope and result HEAD are checked again before submission.
The first CLI execution additionally requires HEAD to equal the native assignment's
`initial_head_commit`. Before any push/result, the complete committed delta and
each new commit are checked against the documentation allowlist, allowed A/M
statuses and regular-file modes, including when the CLI left a clean worktree.
Merge commits and more than 32 new commits require operator reconciliation.
The worktree check includes ignored paths and rejects ignored artifacts; this
initial documentation-only profile requires a workspace without ignored build,
cache or environment artifacts. Git internals are excluded by Git itself.

Each author and each Reviewer runs a fresh ephemeral Codex CLI process and context.
Reviewers receive the exact source task, source/result commits, and author summary;
no other reviewer's feedback is provided. Review execution uses read-only sandbox.
These are independent model sessions, not separate Windows user accounts.
Both role commands explicitly set `sandbox_workspace_write.network_access=false`,
disable web search and use their fixed sandbox modes with approval policy `never`.
The installed CLI 0.161.0 schema declares read-only networking disabled by default;
there is no separate `sandbox_read_only` configuration key. Shell network denial
must also be exercised during live qualification. The workspace-write setting is
documented in the [official configuration reference](https://learn.chatgpt.com/docs/config-file/config-reference).

CLI processes and Git helpers run in existing native Windows Job primitives.
There are at most two active client sessions and two simultaneous CLI processes,
one session per admitted project. Each role has bounded wall time, memory, process
count, CPU, output and prompt size. Child environments omit protected control
credentials. Git commits/pushes are restricted to the assigned branch and exact
documentation allowlist; there is no force push/reset.

A process crash kills Jobs without inherited handles. Restart reclaims only
recorded owned Jobs, resumes accepted client sessions, and replays immutable
submission payloads through the native idempotency contract.
The retained SUBMITTING payloads are replayed before polling new role identities,
because the author's identity may disappear as soon as the native result is
accepted. Replays keep the exact original payload and are fenced by session stop,
current configuration/provenance and native role/scope authorization. Native
quiescent `blocked` is recorded as a terminal client state and releases admission.
Interrupted CLI work
may be resumed in its existing workspace; files are never silently reset.
Changed service configuration requires operator reconciliation instead of
automatic session restart. The JSON journal must be protected as operator state.

## Verification

```powershell
C:/Python312/python.exe -m unittest tests.test_worker_connection_selftest tests.test_setup_worker_corrective -v
```

These tests cover authenticated wire receipts, malformed capability rejection,
binding ownership/configuration/freshness and restart, absolute HTTP deadlines,
native identity admission, real temporary Git commit/push, independent mock role
attempts, exact sprint provenance, committed/ignored-file scope denial, lost-ACK
restart before both reviews, native blocked-state release and real local Windows
Job termination. Protocol fixtures and mock model
outputs are explicitly identified. They do not constitute a real model/Managed
Sprint execution; that requires a separately authorized native self-test and its
native result and two-review evidence.
