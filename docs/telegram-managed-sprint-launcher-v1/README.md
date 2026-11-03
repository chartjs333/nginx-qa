# Telegram Managed Sprint Launcher v1

An optional Telegram front end for the existing nginx-qa Managed Sprint Engine.
It exports current authoring contracts, accepts a versioned JSON envelope and
prepares an isolated managed instance. `autonomous_mode=false` waits for Start;
`true` requests activation through an explicitly configured existing agent client.
One pending/prepared/running operation reserves each canonical project. Different
projects and parallel nodes within one graph remain supported.

The implementation uses the existing importer, control database, continuity graph,
workspaces, leases, result API and two independent Reviewer gates. The launcher
does not bundle a model provider or an autonomous worker service. Missing client
configuration yields `EXECUTOR_UNAVAILABLE`, not an assertion that agents work.

## Delivery and acceptance

The development sprint itself runs through the existing managed graph: architecture,
prompt export, intake, preparation, launch modes, Telegram controls, gateway,
qualification and release. The operator acts as the worker client; two separate
Reviewer clients inspect exact commits and submit their own engine decisions.

[Automated qualification](qualification.md) records 890 passing tests, zero errors,
failures or skips at the specified code commit, with a clean source provenance
receipt. [Acceptance matrix](acceptance-matrix.json) separates automated evidence
from external acceptance. Historical stage reports retain their original dates
and limits. An old NOT_RUN entry is not a claim about a later run.

Actual deployment, tunnel and final reviewer receipts are kept outside the frozen
source checkout under `C:/nqt35/client` and referenced in the delivery PR. The host
ownership marker pins its approved commit; embedding a later run receipt must not
silently change that marker or the running code. A real phone/tablet Telegram demo
and a configured existing executor are required before full product acceptance.

## Install the isolated host

This release's opt-in Windows profile uses `D:/nq-tmsl-launcher`, external state
`C:/nqt36`, private loopback port 18036, public gateway loopback port 18037 and
child ports 18700–18749. Check those paths/ports before setup. A pre-existing
directory or unproved owner is a conflict, not permission to reset or delete it.

```powershell
git clone --config core.autocrlf=false --single-branch --branch agent/tmsl-v1-release-documentation https://github.com/chartjs333/nginx-qa.git D:\nq-tmsl-launcher
Set-Location D:\nq-tmsl-launcher
$releaseCommit = '<approved 40-character commit from the delivery report>'
Copy-Item -LiteralPath .\orchestration\sprints\telegram-managed-sprint-launcher-v1\gateway-18036.env.example -Destination .\.env.staging
$env:PIP_CONSTRAINT = (Resolve-Path .\docs\telegram-managed-sprint-launcher-v1\evidence\qualified-dependencies.txt).Path
.\run_staging.ps1 -Profile telegram-managed-sprint-launcher-v1-gateway -Action Check -ExpectedCommit $releaseCommit
.\run_staging.ps1 -Profile telegram-managed-sprint-launcher-v1-gateway -Action Setup -ExpectedCommit $releaseCommit
& C:\nqt36\v\Scripts\python.exe -I -B -m pip install -r $env:PIP_CONSTRAINT
# Configure trusted credentials, canonical project policy and existing client below.
.\run_staging.ps1 -Profile telegram-managed-sprint-launcher-v1-gateway -Action Start -ExpectedCommit $releaseCommit
```

Run Check/Setup/Start from the exact clean approved branch. The venv/base Python,
ownership marker, source remote and isolated roots are verified before main is
imported. Both listeners share one engine lifespan. The main UI is available only
on `http://127.0.0.1:18036/`; the tunnel targets the narrow gateway on 18037.
Creating this host does not register projects, grant users access or start agents.

## Canonical project, credentials and capabilities

Use the canonical project phone already registered on the owning control server.
For a genuinely new permitted project on this isolated host, the existing API is:

```powershell
$body = @{git_address='https://github.com/OWNER/REPOSITORY.git'; project_name='Permitted test project'} | ConvertTo-Json
$registered = Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:18036/project-manager/0001' -ContentType 'application/json' -Body $body
$projectPhone = [string]$registered.project_phone
$contextKey = [string]$registered.project.git_context_key
```

Do not register the same active repository on another control database or use a
new `git_context_key` to bypass its slot. In particular, the development sprint's
canonical project remains owned by its original control server until its recorded
lifecycle and resource retirement permit a handover.

The `main` repository alias defaults to the registered Git address. Managed Git
publication uses the server-owned project registry's `repositories.main` entry:

```json
{"canonical_remote":"github.com/OWNER/REPOSITORY","transport_url":"https://github.com/OWNER/REPOSITORY.git","credential_reference":"env:NGINX_QA_GIT_PUBLISH_CREDENTIALS"}
```

Private `GET /git-config` identifies the owned configuration path. Its
`projects[git_context_key]` entry may be configured through the operator's trusted
integration, or edited while this host is stopped, preserving the entire registry
and a backup. `POST /git-config` does not configure repository credentials.
The referenced environment value is a JSON map of ephemeral Git environment
overrides, such as an operator-managed `GIT_ASKPASS` adapter. It is not a raw token.
Do not override PATH/HOME/USERPROFILE, `GIT_CONFIG_*` or protected control variables.
Give this adapter only the repository publication rights the operator intends.

Supply actual values through the trusted secret provider, not committed files or
messages. The public gateway requires protected references:

```powershell
$env:NGINX_QA_LAUNCHER_BOT_SECRET_REF = 'env:NGINX_QA_LAUNCHER_CONTROL_BOT_TOKEN'
$env:NGINX_QA_LAUNCHER_WEBHOOK_SECRET_REF = 'env:NGINX_QA_LAUNCHER_CONTROL_WEBHOOK_SECRET'
$env:NGINX_QA_LAUNCHER_EXECUTOR_SECRET_REF = 'env:NGINX_QA_LAUNCHER_CONTROL_EXECUTOR_TOKEN'
$env:NGINX_QA_LAUNCHER_EXECUTOR_URL = 'http://127.0.0.1:<actual existing client port>'
$telegramUserId = '<explicitly permitted positive Telegram user ID>'
$policy = @{
  schema_version=1
  grants=@(@{user_id=$telegramUserId; chat_id=$telegramUserId; project_id=$projectPhone
    actions=@('prompt','read','prepare','manifest_publish','start','stop')})
  projects=@{ $projectPhone=@{repository_id='main'; source_ref='refs/heads/main'; checksum_paths=@('README.md')} }
}
$env:NGINX_QA_TELEGRAM_LAUNCHER_POLICY_JSON = $policy | ConvertTo-Json -Depth 8 -Compress
```

Set these in the process environment that launches the host. `.env.staging`
contains configuration/reference names; values remain in the secret provider.
Remove any capabilities that have not been granted. `autonomous_mode` never grants
publication, deploy, merge, or user access by itself. The private user ID and chat
ID must match. Empty/malformed policy fails closed.

The existing client must implement the authenticated READY / ACCEPTED / STOPPED
protocol in [launch modes](launch-modes.md), durably deduplicate its session and
execute actual assignments through normal identity/result/review APIs. A dummy
HTTP acknowledgement is not a substitute for a working client. Stop must wait
for that client's real writes/workers and owned native processes to quiesce.

## Telegram workflow and public access

Enable and configure one [owned gateway/tunnel](gateway.md). Use a stable named
tunnel for ongoing service; the optional owned Quick Tunnel is temporary. Its
upstream is only `http://127.0.0.1:18037`. Register only the bot assigned to this
gateway; a foreign existing webhook is rejected and pending updates are preserved.

1. In a private bot chat, send `/sprint_prompt <phone> --file <task>` or `--text`.
2. Give the complete export to an external AI and retain its returned JSON.
3. Send `managed-sprint.json`, or a JSON document captioned `/sprint_import <phone>`.
4. With `autonomous_mode=false`, the instance waits without assignments. The three
   buttons are **Запустить спринт**, **Открыть Web UI**, **Статус спринта**.
5. Open the Mini App from the private bot chat. Valid signed Telegram identity
   establishes a one-use exchange and scoped session without a separate password.
6. Start once; repeated delivery/clicks reuse the same operation. Use status or
   `/sprint_status <phone>` to refresh controls. Stop is available in the Mini App.
7. After terminal completion or proved Stop/resource retirement, use a new
   delivery with fresh write branches for another run. `autonomous_mode=true`
   uses the same authorized activation path automatically.

Unmarked old documents retain their legacy dispatch. The new envelope requires an
explicit boolean mode; absence in old legacy JSON retains its previous behavior.
An outdated source commit requires a fresh prompt. Another request for a reserved
project gets PROJECT_BUSY; it cannot replace the running or prepared instance.

## Recovery and remaining demonstration

Keep the import database, marker, roots, registry and Git history during restart.
The same owned source/profile resumes durable intents; intentionally waiting
manual instances do not start on restart. Notifications may repeat after a lost
Telegram acknowledgement, but activation is deduplicated. Results/reviews/logs
and cancellation evidence remain retained. No recursive history deletion is part
of v1 cleanup.

To remove public access, stop the exact owned tunnel using its documented Stop
command. To stop a foreground host, use Ctrl+C and wait for both listeners and
the single lifespan to drain. Use a background service manager's graceful stop
for managed background deployments. Never kill a process merely because it owns
a familiar port or has the cloudflared name. Preserve state when rolling back;
restore only an approved compatible source, never rewrite the marker to bypass
its commit/ownership check.

Full acceptance requires actual private Telegram delivery, a real phone/tablet
login through Cloudflare and manual plus autonomous execution with the configured
client in a permitted disposable project. Capture the exact source commit,
operation/instance IDs, two review identities, single-project rejection and
terminal receipts. Local fake-client/browser tests remain separate evidence.
If these prerequisites are absent, the release graph records NO_GO and routes to
Coordinator BLOCKED_EXTERNAL with the preserved implementation and unblock steps.

See [architecture](architecture.md), [prompt export](prompt-export.md),
[intake](intake.md), [preparation](instance-preparation.md),
[controls](telegram-controls.md), [gateway security](gateway.md) and
[qualification](qualification.md) for contracts and error/recovery details.
