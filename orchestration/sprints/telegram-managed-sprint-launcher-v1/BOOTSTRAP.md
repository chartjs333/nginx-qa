# Telegram Managed Sprint Launcher v1: isolated execution

This checkout hosts the existing nginx-qa engine that will implement the sprint.
It does not already implement the Telegram launcher described by the manifest.

| Boundary | Value |
| --- | --- |
| Service checkout | `D:\nq-tmsl` |
| Service branch | `codex/tmsl-v1-bootstrap` |
| Service URL | `http://127.0.0.1:18035` |
| Owned state base | `C:\nqt35` |
| Interpreter | `C:\nqt35\v\Scripts\python.exe` |
| Runtime / prompt / managed roots | `C:\nqt35\r`, `C:\nqt35\p`, `C:\nqt35\m` |
| Child port pool | `18600–18649` |

The regular `run_staging.ps1` gates remain mandatory: exact local and remote
commit, standalone checkout, clean tracked files, independent interpreter,
ownership marker, non-reparse paths, protected roots, and free port pool.
The original checkout and Madoev EEG are protected and are not queried or changed.
Telegram and cloudflared are disabled for this control server; the sprint will
test its new integrations in disposable environments first.

## Setup and start

The manifest is unchanged from the validated preparation artifact. Its workspace
source is `main@2482beb33ea55d8e5f8565cda9bd546df9c6bc32`. Its manifest commit is
the separately published commit of this service branch; these are different
provenance fields. Do not insert a fabricated commit or bypass start-from-git.
Publication of the service branch must be authorized before these commands can
pass their remote-head checks.

```powershell
Set-Location D:\nq-tmsl
Copy-Item -LiteralPath orchestration\sprints\telegram-managed-sprint-launcher-v1\staging-18035.env.example -Destination .env.staging
$approvedCommit = (git rev-parse HEAD).Trim()
.\run_staging.ps1 -Profile telegram-managed-sprint-launcher-v1 -Action Check -ExpectedCommit $approvedCommit
.\run_staging.ps1 -Profile telegram-managed-sprint-launcher-v1 -Action Setup -ExpectedCommit $approvedCommit
.\run_staging.ps1 -Profile telegram-managed-sprint-launcher-v1 -Action Start -ExpectedCommit $approvedCommit
```

Use `Setup` once on the fresh owned state base. Do not update the service branch
under the running server. Implementation commits belong to workspaces assigned
by the engine; code updates to the control server require an explicit owned
restart and a deliberate provenance update, not an edited ownership marker.

## Register, activate, and act as a client

1. Verify this server's identity, loopback listener and isolated roots.
2. Register the actual `origin` through `POST /project-manager/0001`; use the
   returned project phone, never a guessed phone from another instance.
3. Call the existing `/api/v1/projects/{project_id}/sprints/start-from-git` with
   `repository_id: main`, `ref: refs/heads/codex/tmsl-v1-bootstrap`, manifest path
   `orchestration/sprints/telegram-managed-sprint-launcher-v1/sequential-sprint.json`,
   and one persisted idempotency key. Retain the actual response/provenance.
4. Follow the existing identity protocol. Managed per-role identity/result
   requests use `POST /api/v1/projects/{project_id}/agents/{phone}/whoami`.
   Verify the managed response schema, scope requirements, actual workspace,
   assigned branch, assignment ID and initial commit before doing work.
5. Complete only the assigned task in that workspace. Commit locally and submit
   the actual assignment ID, graph outcome, result, from_commit, git_commit and
   git_branch according to `managed-assignment-result-v1.schema.json`.
6. Two independent reviewers inspect the exact result commit. Each uses its own
   current review assignment and submits only assignment_id, APPROVE/REJECT and
   feedback according to the review schema. The implementer must not generate
   approvals on their behalf. Scope-controlled roles also require their own ACK.
7. Let the engine deliver the next assignment after both approvals. Do not edit
   durable state, advance nodes manually, simulate reviews or create a second
   scheduler. Rejections follow existing rework/continuity rules.

The manifest's `planning.preparation_only` and `NOT_RUN` entries describe its
creation-time plan; execution truth lives in the engine's current state and
evidence, not in edits to that immutable manifest. Preserve results and report
external blockers accurately. No automatic main merge or deployment is allowed.
