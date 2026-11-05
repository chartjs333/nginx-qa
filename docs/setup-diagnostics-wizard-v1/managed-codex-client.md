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

The task-launcher extension adds the separately admitted `java-maven-v1` profile
and trusted per-project manifest admission. See [Java/Maven client](java-maven-client.md)
for its exact source ceiling, private Git credential boundary, offline build
requirements and separate live qualification requirements. The original
documentation-only evidence below remains tied to its original source and run.

An explicitly selected, separately owned WSL2 backend is now available for this
same limited profile. See [WSL installation, boundaries and qualification](wsl-managed-client.md).
It extends the existing client without changing native graph or result authority.
The Windows MXC-specific limits below still apply when `sandbox_backend` is `mxc`.

## Operator configuration

Save the configuration outside agent workspaces. First check the configured CLI
under the same signed-in Windows account:

```powershell
C:/Python312/python.exe -m nginx_qa.managed_codex_client --config C:/operator/client.json --check
```

`--diagnose` is an alias. This command prints sanitized JSON and exits 0 for PASS
or 2 for BLOCKED. It does not create the service journal, bind an HTTP listener,
resume a session, contact the native engine, run a model or create a sprint.
On a supported host it runs bounded trusted native probes in disposable local
directories and checks the existing CLI login. Capabilities are declarations;
they do not substitute for this readiness check. Start the service after PASS:

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
  "sandbox_backend": "mxc",
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
  `READY` only after actual sandbox boundary, existing CLI login and admission checks.
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
disable web search, ignore user configuration/rules and use their fixed sandbox
modes with approval policy `never`. The backend is explicitly `windows.sandbox=mxc`;
there is no fallback. Every prepare and each resumed/direct role execution first
runs native boundary canaries, before any model process. The canary must actually
read a file, allow an author document write, deny a Reviewer write, attempt and
deny a native socket connection to a verified reachable disposable listener, and
attempt and deny a `.git/config` write. The parent verifies socket receipts and
unchanged Git bytes. Missing attempts, a failed probe or model claims alone fail.

This adapter currently admits strict MXC with CLI 0.162.0 or newer and a supported
Windows 11 build (26100.9278/26200.9278 or newer), followed by the actual canary.
See the [official Windows sandbox guide](https://learn.chatgpt.com/docs/windows/windows-sandbox).
The current qualification machine is Windows 10 22H2 build 19045.6456 with CLI
0.161.0. Its honest readiness result is:

```json
{"state":"BLOCKED","error":"CODEX_SANDBOX_UNAVAILABLE","reason":"HOST_MXC_UNSUPPORTED"}
```

Upgrading only the CLI does not supply MXC to Windows 10. Native socket canaries
on this machine reached the test listener under `unelevated` in both role modes,
including with explicit network-disabled permission profiles. That weaker backend
is rejected; a nominal network flag is not treated as isolation proof.

Windows 10's official elevated backend may provide strong isolation after proper
operator provisioning. This adapter does **not yet admit it**: a valid public
`CODEX_HOME/.sandbox/setup_marker.json` alone is insufficient. Official account
admission may trigger automatic setup/UAC when accounts are missing, disabled or
expired, even with a valid marker. A backend extension with a documented
noninteractive/no-setup admission path and the same actual canaries is required.
Setting `sandbox_backend` to `elevated` returns
`ELEVATED_NONINTERACTIVE_ADMISSION_UNSUPPORTED`; provisioning alone does not change
that result. No sandbox secret files are read by this adapter and it never
provisions accounts, launches UAC or changes global Codex settings.

An automatic path is a supported, already configured Windows 11 host with a
pinned compatible CLI, existing login and a passing `--check`. The explicit
`wsl2-bwrap` docs-only backend provides a separately qualified path on the current
Windows 10 machine; follow its dedicated runbook and resource limits. An
explicitly authorized operator client can also perform native
assignments and independent reviews on this machine; that is supervised execution,
not proof that the automatic CLI backend passed qualification.

For the `mxc` backend, CLI processes and Git helpers run in existing native Windows Job primitives.
There are at most two active client sessions and two simultaneous CLI processes,
one session per admitted project. Each role has bounded wall time, memory, process
count, CPU, output and prompt size. Child environments omit protected control
credentials and all inherited `CODEX_*` / `OPENAI_*` overrides, including a parent
agent's permission profile, thread/session identity and custom Codex home. Existing
login is read from the service account's default Codex home. Git commits/pushes are restricted to the assigned branch and exact
documentation allowlist; there is no force push/reset.

For a verified `git://` origin on Windows, publication applies
`-c sendpack.sideband=false` to that command only. Git for Windows 2.49.0's
installed release notes document the protocol deadlock in
[upstream issue #907](https://github.com/git-for-windows/git/issues/907).
An actual disposable daemon push reproduced the hang with both ordinary
subprocess execution and Windows Jobs; dry-run did not exercise pack transfer.
The command-scoped workaround completed the same transfer while preserving Job
limits. Repository/global Git settings and other transports are unchanged.
Fetch URL and every configured push URL must match the operator-pinned origin.
After a successful push, the exact remote branch SHA must match the validated
commit before a native result is submitted. This matters because disabling
side-band can delay or suppress remote diagnostic text.

A process crash kills Jobs without inherited handles. Restart reclaims only
recorded owned Jobs, resumes accepted client sessions, and replays immutable
submission payloads through the native idempotency contract.
Before Git publication, the author output and locally validated commit are
checkpointed as `OUTPUT_READY`. A publication failure/restart reuses this exact
output without rerunning the author. Resume requires unchanged native authority,
configuration, scope, root/branch/origin, exact HEAD and clean allowed files; drift
blocks publication. Failures before this checkpoint still require reconciliation.
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
C:/Python312/python.exe -m unittest tests.test_managed_client_git_transport -v
```

These tests cover authenticated wire receipts, malformed capability rejection,
binding ownership/configuration/freshness and restart, absolute HTTP deadlines,
native identity admission, real temporary Git commit/push, independent mock role
attempts, exact sprint provenance, committed/ignored-file scope denial, lost-ACK
restart before both reviews, native blocked-state release and real local Windows
Job termination. Sandbox regressions include real local native socket/Git writes
deliberately run without isolation to verify rejection, preflight-before-model,
inherited-authority removal, missing-attempt rejection and diagnostic redaction.
Protocol fixtures and mock model
outputs are explicitly identified. They do not constitute a real model/Managed
Sprint execution; that requires a separately authorized native self-test and its
native result and two-review evidence.
