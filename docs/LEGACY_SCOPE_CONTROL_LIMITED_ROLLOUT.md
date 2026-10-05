# Legacy scope control: limited rollout for Delta on port 18025

Status: **prepared, not executed**.

This document is an operator runbook for a separately approved maintenance
window. Preparing this document did **not** deploy code, restart either nginx-qa
instance, apply an amendment, acknowledge scope, dequeue work, import a sprint,
or advance the Delta graph.

The instance on port `8025` is explicitly out of scope. Never select or stop a
process by image name (`python.exe`, `cmd.exe`) or by `main.py` alone.

## 1. Frozen identities and trusted inputs

nginx-qa source base:

```text
9375675cdcc56fd3d05861de9375ceb19a71400c
```

The release checkout currently serving Delta:

```text
D:\nginx-qa-release\umse-9375675
```

The implementation to deploy must be the exact published commit supplied by
the release owner:

```text
<FINAL_SHA>
```

Do not substitute a moving branch name for `<FINAL_SHA>` during deployment.

Execution order is: prepare and pin the published objects in section 1; inspect
the topology in section 2; provision section 4; then, only in the approved
maintenance window, execute sections 5–8 in order. Section 3 is the read-only
API reference invoked by section 8 **after the new code is running**; that
endpoint does not exist in the old base. Section 9 is a later, separately
authorized operation and is not part of deployment.

Before the maintenance window, obtain the published branch and pin its exact
commit in the service checkout. This fetch changes Git objects/remote refs
only; it must not switch the running checkout or modify runtime files:

```powershell
$ServiceRoot = 'D:\nginx-qa-release\umse-9375675'
$BaseSha = '9375675cdcc56fd3d05861de9375ceb19a71400c'
$FinalSha = '<FINAL_SHA>'
$PublishedBranch = 'agent/legacy-scope-control-v1'

if ($FinalSha -notmatch '^[0-9a-f]{40}$') {
    throw 'Replace FINAL_SHA with the exact approved published commit'
}
$LiveHead = git -C $ServiceRoot rev-parse HEAD
if ($LASTEXITCODE -ne 0 -or $LiveHead.Trim() -ne $BaseSha) {
    throw 'The serving checkout no longer has the audited base HEAD'
}
git -C $ServiceRoot fetch --no-tags origin (
    "refs/heads/${PublishedBranch}:refs/remotes/origin/${PublishedBranch}"
)
if ($LASTEXITCODE -ne 0) { throw 'Published implementation fetch failed' }
$PublishedHead = git -C $ServiceRoot rev-parse "refs/remotes/origin/$PublishedBranch"
if ($LASTEXITCODE -ne 0 -or $PublishedHead.Trim() -ne $FinalSha) {
    throw 'Published branch does not name the approved final SHA'
}
git -C $ServiceRoot cat-file -e "$FinalSha^{commit}"
if ($LASTEXITCODE -ne 0) { throw 'Pinned implementation object is unavailable' }
```

Delta Git inputs:

```text
repository key: github.com/chartjs333/delta
canonical branch: refs/heads/agent/isc-s16-continuous-sprint

base commit:
0c8fe0426586810c1e1a342d11f0d5c42ce2de16

target commit:
65074657df5f9ba3f16b7ac4912a563128d902ee
```

The SHA-256 values below are hashes of the exact Git blob bytes, not hashes of
pretty-printed or reserialized JSON:

| Input | Commit | SHA-256 |
|---|---|---|
| `orchestration/sprints/isc-s16-continuous/sequential-sprint.json` | base | `4f4e320e75b3d2bb1c95a89b2828637c5823f6906f8c165b78838f12108d9ca4` |
| `orchestration/sprints/isc-s16-continuous/sequential-sprint.json` | target | `9e8824d142978b849d9240fd5004794f8c66483e979b94d04f174671dc646260` |
| `orchestration/sprints/isc-s16-continuous/scope-amendments/ISC-S16-D01-RECOVERY-PROOF-ONLY.json` | target | `1637f837aca3e028a66e5e1a306a83c695a4d010e35f7b53e54e44cecd38d655` |
| `orchestration/sprints/isc-s16-continuous/handoffs/ISC-S16-CONTINUITY-SCOPE-SYNC.json` | target | `29ea0ba3a63d44054f3f135fc12a416be5968649c6df7a05b5f29e13fb1baa1f` |

Verify the local Delta checkout without changing a ref:

```powershell
$DeltaRoot = 'D:\delta'
$ExpectedBase = '0c8fe0426586810c1e1a342d11f0d5c42ce2de16'
$ExpectedTarget = '65074657df5f9ba3f16b7ac4912a563128d902ee'
$TargetRef = 'refs/heads/agent/isc-s16-continuous-sprint'

$RemoteLine = @(git -C $DeltaRoot ls-remote --heads origin $TargetRef)
if ($LASTEXITCODE -ne 0 -or $RemoteLine.Count -ne 1) {
    throw 'Canonical remote ref could not be verified exactly once'
}
$RemoteFields = $RemoteLine[0] -split '\s+'
if ($RemoteFields[0] -ne $ExpectedTarget -or $RemoteFields[1] -ne $TargetRef) {
    throw 'The published canonical branch no longer names the approved target'
}

if ((git -C $DeltaRoot rev-parse $ExpectedBase).Trim() -ne $ExpectedBase) {
    throw 'Delta base commit is unavailable'
}
if ((git -C $DeltaRoot rev-parse $ExpectedTarget).Trim() -ne $ExpectedTarget) {
    throw 'Delta target commit is unavailable'
}
if ((git -C $DeltaRoot rev-parse $TargetRef).Trim() -ne $ExpectedTarget) {
    throw 'The canonical Delta branch no longer points at the approved target'
}
```

`ls-remote` is read-only and confirms the published branch; the following
`rev-parse` checks confirm the local immutable objects and local ref used by
the runtime verifier. If the network check is unavailable or any value
differs, stop. Do not fetch, reset, import, or repair as an implicit part of
this procedure.

## 2. Observed live topology

At the preparation audit, port `18025` had this process chain:

```text
explorer.exe
└─ cmd.exe       PID 5264
   command: C:\Windows\system32\cmd.exe /c ""D:\nginx-qa-release\umse-9375675\run.bat" "
   cwd: D:\nginx-qa-release\umse-9375675
   └─ python.exe PID 57044
      executable: D:\nginx-qa-release\umse-9375675\.venv\Scripts\python.exe
      cwd: D:\nginx-qa-release\umse-9375675
      └─ python.exe PID 34324
         executable: C:\Python312\python.exe
         command: -m uvicorn main:app --host 0.0.0.0 --port 18025
         cwd: D:\nginx-qa-release\umse-9375675
```

PIDs are observations, not deployment constants. Rediscover them immediately
before maintenance. `run.bat` has auto-restart enabled, so stopping only the
listener is insufficient.

Port `8025` was separately traced to:

```text
D:\nginx-qa\run.bat
cmd.exe PID 48912 -> python PID 33752 -> python PID 51384 -> :8025
```

That tree must remain running and untouched.

### Path-scoped process preflight

Run in PowerShell 7:

```powershell
$ServiceRoot = [IO.Path]::GetFullPath('D:\nginx-qa-release\umse-9375675')
$ExpectedServiceRoot = 'D:\nginx-qa-release\umse-9375675'
$Port = 18025

if ($ServiceRoot -ne $ExpectedServiceRoot) {
    throw "Unexpected service root: $ServiceRoot"
}

$listeners = @(Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction SilentlyContinue)
if ($listeners.Count -ne 1) {
    throw "Expected exactly one listener on $Port; found $($listeners.Count)"
}

$allProcesses = @(Get-CimInstance Win32_Process)
$launchers = @($allProcesses | Where-Object {
    $_.Name -eq 'cmd.exe' -and
    $_.CommandLine -like "*$ExpectedServiceRoot\run.bat*"
})
if ($launchers.Count -ne 1) {
    throw "Expected one path-scoped run.bat launcher; found $($launchers.Count)"
}

$launcher = $launchers[0]
$treeIds = [Collections.Generic.HashSet[int]]::new()
$null = $treeIds.Add([int]$launcher.ProcessId)
do {
    $added = $false
    foreach ($process in $allProcesses) {
        if ($treeIds.Contains([int]$process.ParentProcessId)) {
            $added = $treeIds.Add([int]$process.ProcessId) -or $added
        }
    }
} while ($added)

$listenerPid = [int]$listeners[0].OwningProcess
if (-not $treeIds.Contains($listenerPid)) {
    throw 'The 18025 listener is not a descendant of the expected launcher'
}

$allProcesses |
    Where-Object { $treeIds.Contains([int]$_.ProcessId) } |
    Select-Object ProcessId, ParentProcessId, Name, ExecutablePath, CommandLine
```

Optionally verify working directories using the already installed system
Python and `psutil`. This prints paths only and excludes the verifier process:

```powershell
@'
import os
import psutil

expected = os.path.normcase(r"D:\nginx-qa-release\umse-9375675")
for process in psutil.process_iter(["pid", "ppid", "name", "cmdline"]):
    if process.pid == os.getpid():
        continue
    try:
        cwd = os.path.normcase(process.cwd())
    except (psutil.AccessDenied, psutil.NoSuchProcess, OSError):
        continue
    if cwd == expected:
        print(process.pid, process.ppid(), process.name(), process.cwd(), process.cmdline())
'@ | C:\Python312\python.exe -
```

Before maintenance, also require that no connection on `18025` is in an
active state other than `Listen`/`TimeWait`. The server binds `0.0.0.0`, so a
LAN client can otherwise race the backup.

## 3. Read-only Delta preflight reference (used after section 8 startup)

After `<FINAL_SHA>` is running, but before any amendment, call only the
read-only endpoint. Do not execute this section before sections 4–8. The
recorded values below are also the offline backup acceptance baseline:

```powershell
$BaseUrl = 'http://127.0.0.1:18025'
$ProjectId = '9000'
$SprintId = 'sprint-0001-783ef52c'
$AdminHeaders = @{
    'X-Nginx-QA-Scope-Control-Token' =
        [Environment]::GetEnvironmentVariable(
            'NGINX_QA_SCOPE_CONTROL_ADMIN_TOKEN',
            'Process'
        )
}

$Preflight = Invoke-RestMethod -Method Get -Uri (
    "$BaseUrl/api/v1/projects/$ProjectId/sprints/$SprintId/" +
    'scope-amendments/preflight'
) -Headers $AdminHeaders
$Preflight | Format-List
```

The preflight is read-only but operator-only because it returns exact CAS
values and the trusted local checkout path. Once the server credential is
configured, a missing or invalid admin header fails with `403`; an unavailable
server credential fails closed with `503`. Never place the token in the URL.

Required values at the audited checkpoint are:

```text
runtime_type              legacy_sequential_conditional_graph
applicable                true
project_id                9000
sprint_id                 sprint-0001-783ef52c
execution_revision        52
assignment_id             898cbe80-1f5f-4348-ae65-4e6fc83239bc
node_id                   continuity-coordinator
phase                     node
agent_id                  isc-s16-continuity-coordinator
issued_task_sha256        b51c1bb5ccb25a8eace67237de861ef8d13b42d105d9f00d0a591fbd62fb0d81
issued_message_sha256     4ea64574a6f8086c78b9065260804ba0d1380029a7080dd5a4f604fbc9cc91f4
scope_control_present     false
amendment_mode            one_shot_v1
accepts_new_amendment     true
source_repository.available true
source_repository.resolution operator_repository_map
source_repository.path    D:\delta
minimum_runtime_capability legacy_scope_control_v1
mutated                   false
```

These are observed values, not permission to use stale CAS data. Immediately
before a separately authorized apply, retrieve preflight again and construct
the payload from that response. Any assignment, node, phase, revision, task
hash, sprint, or source-ref difference is `ABORT`.

The legacy state baseline additionally contains:

```text
assignments:                     22 (8 graph_node + 14 transition_review)
embedded review decisions:      14 (13 APPROVE + 1 REJECT)
transition_review assignments:  14 (13 approved + 1 rejected)
identity deliveries:            22
formal-linkage visits:           2
continuity-coordinator visits: 3
active task / queue item id:  3fe1172a-b165-48d7-bef5-e4111d2c1d05
```

`last_transition.reviews` repeats two of those embedded decisions and is not
added to either review count. Use the formulas above and a canonical snapshot
or hash comparison; do not recursively count every object named `reviews`.

Do not use a general `whoami` as a read-only preflight; it can claim/dequeue a
task. Use the amendment preflight and project state GET only.

## 4. Credential provisioning

The feature fails closed unless every required value is at least 32 characters
and is present in the environment inherited by the server process.

Required environment variable names:

```text
NGINX_QA_SCOPE_CONTROL_ADMIN_TOKEN
NGINX_QA_SCOPE_CONTROL_ROLE_TOKEN_2750
NGINX_QA_SCOPE_CONTROL_ROLE_TOKEN_2753
NGINX_QA_SCOPE_CONTROL_ROLE_TOKEN_2754
NGINX_QA_SCOPE_CONTROL_ROLE_TOKEN_2791
NGINX_QA_SCOPE_CONTROL_ROLE_TOKEN_2792
```

The release checkout and the trusted Delta checkout are not siblings. The
source verifier therefore also requires the explicit, non-secret repository
registry below; without it the 18025 topology fails closed with
`SCOPE_SOURCE_UNAVAILABLE`. Build the JSON with PowerShell so path escaping is
unambiguous:

```powershell
$env:NGINX_QA_SCOPE_CONTROL_REPOSITORY_MAP = @{
    'github.com/chartjs333/delta' = 'D:\delta'
} | ConvertTo-Json -Compress

$RepositoryMap = $env:NGINX_QA_SCOPE_CONTROL_REPOSITORY_MAP |
    ConvertFrom-Json -AsHashtable
if ($RepositoryMap['github.com/chartjs333/delta'] -ne 'D:\delta') {
    throw 'Unexpected scope-control repository mapping'
}
```

The mapped path must be absolute, must contain a Git checkout, and its Git
remote must canonicalize to the mapped key. The server rechecks all three
conditions before reading blobs. Do not place a token or Git credential in
this map.

Provision values from the approved secret manager into the launching process
environment. Do not put values in Git, `.env`, this runbook, evidence, command
arguments, URLs, JSON bodies, screenshots, logs, or prompts. Do not print them.

Before startup, check presence and length without displaying a value:

```powershell
$RequiredCredentialNames = @(
    'NGINX_QA_SCOPE_CONTROL_ADMIN_TOKEN',
    'NGINX_QA_SCOPE_CONTROL_ROLE_TOKEN_2750',
    'NGINX_QA_SCOPE_CONTROL_ROLE_TOKEN_2753',
    'NGINX_QA_SCOPE_CONTROL_ROLE_TOKEN_2754',
    'NGINX_QA_SCOPE_CONTROL_ROLE_TOKEN_2791',
    'NGINX_QA_SCOPE_CONTROL_ROLE_TOKEN_2792'
)

foreach ($name in $RequiredCredentialNames) {
    $value = [Environment]::GetEnvironmentVariable($name, 'Process')
    if ($null -eq $value -or $value.Length -lt 32) {
        throw "Required process credential is unavailable: $name"
    }
}

$CredentialValues = @(
    $RequiredCredentialNames | ForEach-Object {
        [Environment]::GetEnvironmentVariable($_, 'Process')
    }
)
if (@($CredentialValues | Sort-Object -Unique).Count -ne $CredentialValues.Count) {
    throw 'Admin and role credentials must all be distinct'
}
```

The operator credential is supplied only in
`X-Nginx-QA-Scope-Control-Token`. Role credentials are supplied only in
`X-Nginx-QA-Scope-Token`.

## 5. Freeze and stop all writers

This step requires explicit deployment/restart authorization.

1. Announce a maintenance freeze to the Delta coordinator, formal roles, both
   reviewers, and any API operators.
2. Disable or pause any external ingress to `18025`.
3. Confirm there is no cloudflared process targeting `18025`, no Telegram
   auto-register process, no scheduled launcher, and no active TCP connection.
4. Retain the path-scoped PID set produced in section 2.
5. In the exact visible `run.bat` console for `18025`, press `Ctrl+C` and
   confirm termination of the batch job if prompted.
6. Wait longer than the configured five-second restart delay.
7. Verify that the listener and every retained descendant are gone.

If an interactive graceful stop is unavailable, the fallback order is:

1. stop the exact path-verified `cmd.exe` launcher first, preventing restart;
2. stop only its previously recorded descendants;
3. verify no process with the release cwd remains.

Use only the PID/creation-time snapshot produced by the path-scoped preflight.
The following fallback deliberately stops the launcher first and refuses a PID
that has been reused since discovery:

```powershell
$RecordedTree = @(
    $allProcesses |
        Where-Object { $treeIds.Contains([int]$_.ProcessId) } |
        Select-Object ProcessId, ParentProcessId, Name, CreationDate, CommandLine
)
$StopOrder = @(
    $RecordedTree | Where-Object ProcessId -eq $launcher.ProcessId
    $RecordedTree | Where-Object ProcessId -ne $launcher.ProcessId |
        Sort-Object ProcessId
)

foreach ($recorded in $StopOrder) {
    $current = Get-CimInstance Win32_Process -Filter (
        "ProcessId=$($recorded.ProcessId)"
    ) -ErrorAction SilentlyContinue
    if ($null -eq $current) { continue }
    if (
        $current.Name -ne $recorded.Name -or
        $current.CreationDate -ne $recorded.CreationDate
    ) {
        throw "PID reuse detected; refusing to stop $($recorded.ProcessId)"
    }
    Stop-Process -Id $current.ProcessId -Force -ErrorAction Stop
}
```

This is an emergency fallback after ingress is frozen and active connections
are absent, not the preferred graceful shutdown path.

Do not use wildcards or image-name termination. Do not touch the separately
observed `D:\nginx-qa`/`:8025` tree.

Post-stop gate:

```powershell
if (Get-NetTCPConnection -State Listen -LocalPort 18025 -ErrorAction SilentlyContinue) {
    throw 'Port 18025 is still listening'
}

$remaining = @(Get-CimInstance Win32_Process | Where-Object {
    $_.CommandLine -like '*D:\nginx-qa-release\umse-9375675\run.bat*'
})
if ($remaining.Count -ne 0) {
    throw 'The path-scoped launcher is still alive'
}
```

Run the cwd verifier from section 2 again; it must print no process except the
short-lived verifier itself, which it excludes.

## 6. Coherent verified backup

Do not copy live state while a writer is running. The canonical legacy state is
co-located with the service checkout:

```text
conversation_log.jsonl
port_git_map.json
agents.json
project_sprints.json
pending_project_sprints.json       if present
email_routes.json                  if present
specializations.json               if present
runtime_state\
attachments\                       if present
screenshot_folders\
evidence_folders\
```

The following are derived delivery pointers rather than authoritative state,
but must be checkpointed so a rollback cannot leave a post-amendment prompt as
the apparent latest instruction:

```text
D:\Prompt\delta\latest-response.json
D:\Prompt\delta\latest.txt
D:\Prompt\delta_*-latest.prompt
```

Create the backup only after the post-stop gate succeeds. Use a new explicit
directory; never reuse or mirror into an existing backup:

```powershell
$ServiceRoot = 'D:\nginx-qa-release\umse-9375675'
$BackupParent = 'D:\nginx-qa-backups\delta-18025'
$Stamp = [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssZ')
$BackupRoot = Join-Path $BackupParent "$Stamp-pre-deploy"

if ([IO.Path]::GetFullPath($ServiceRoot) -ne 'D:\nginx-qa-release\umse-9375675') {
    throw 'Unexpected service root'
}
if (Test-Path -LiteralPath $BackupRoot) {
    throw "Backup target already exists: $BackupRoot"
}

$null = New-Item -ItemType Directory -Path $BackupRoot
$principal = [Security.Principal.WindowsIdentity]::GetCurrent().Name
& icacls.exe $BackupRoot /inheritance:r /grant:r `
    "${principal}:(OI)(CI)F" 'SYSTEM:(OI)(CI)F' | Out-Null

$StateDestination = Join-Path $BackupRoot 'state'
$null = New-Item -ItemType Directory -Path $StateDestination
$StateEntries = @(
    'conversation_log.jsonl',
    'port_git_map.json',
    'agents.json',
    'project_sprints.json',
    'pending_project_sprints.json',
    'email_routes.json',
    'specializations.json',
    'runtime_state',
    'attachments',
    'screenshot_folders',
    'evidence_folders'
)

$EntryPresence = foreach ($relative in $StateEntries) {
    $source = Join-Path $ServiceRoot $relative
    $exists = Test-Path -LiteralPath $source
    if ($exists) {
        $destination = Join-Path $StateDestination $relative
        $sourceItem = Get-Item -LiteralPath $source -Force
        if ($sourceItem.PSIsContainer) {
            $null = New-Item -ItemType Directory -Path $destination
            Get-ChildItem -LiteralPath $source -Force |
                Copy-Item -Destination $destination -Recurse -Force
        } else {
            $parent = Split-Path -Parent $destination
            $null = New-Item -ItemType Directory -Path $parent -Force
            Copy-Item -LiteralPath $source -Destination $destination
        }
    }
    [pscustomobject]@{ path = $relative; exists = $exists }
}

$PromptDestination = Join-Path $BackupRoot 'prompt-pointers'
$null = New-Item -ItemType Directory -Path $PromptDestination
$PromptPointers = @(
    Get-Item -LiteralPath 'D:\Prompt\delta\latest-response.json' -ErrorAction SilentlyContinue
    Get-Item -LiteralPath 'D:\Prompt\delta\latest.txt' -ErrorAction SilentlyContinue
    Get-ChildItem -LiteralPath 'D:\Prompt' -File -Filter 'delta_*-latest.prompt' -ErrorAction SilentlyContinue
)
foreach ($file in $PromptPointers) {
    Copy-Item -LiteralPath $file.FullName -Destination (Join-Path $PromptDestination $file.Name)
}

$LauncherDestination = Join-Path $BackupRoot 'launcher-local'
$null = New-Item -ItemType Directory -Path $LauncherDestination
Copy-Item -LiteralPath (Join-Path $ServiceRoot 'run.bat') -Destination $LauncherDestination
if (Test-Path -LiteralPath (Join-Path $ServiceRoot 'QUICKSTART.md')) {
    Copy-Item -LiteralPath (Join-Path $ServiceRoot 'QUICKSTART.md') -Destination $LauncherDestination
}
```

Build and verify a SHA-256 manifest. Lock files are not authoritative state and
must not be restored:

```powershell
$SourceFiles = foreach ($relative in $StateEntries) {
    $source = Join-Path $ServiceRoot $relative
    if (-not (Test-Path -LiteralPath $source)) { continue }
    $item = Get-Item -LiteralPath $source -Force
    if ($item.PSIsContainer) {
        Get-ChildItem -LiteralPath $source -File -Force -Recurse |
            Where-Object Name -NotLike '*.lock'
    } elseif ($item.Name -notlike '*.lock') {
        $item
    }
}

$FileManifest = foreach ($file in $SourceFiles) {
    $relative = [IO.Path]::GetRelativePath($ServiceRoot, $file.FullName)
    $destination = Join-Path $StateDestination $relative
    if (-not (Test-Path -LiteralPath $destination)) {
        throw "Backup file is missing: $relative"
    }
    $sourceHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $file.FullName).Hash.ToLowerInvariant()
    $destinationHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $destination).Hash.ToLowerInvariant()
    $destinationLength = (Get-Item -LiteralPath $destination).Length
    if ($sourceHash -ne $destinationHash -or $file.Length -ne $destinationLength) {
        throw "Backup verification failed: $relative"
    }
    [pscustomobject]@{
        path = $relative
        bytes = $file.Length
        last_write_time_utc = $file.LastWriteTimeUtc.ToString('o')
        sha256 = $sourceHash
    }
}

$PromptManifest = foreach ($source in $PromptPointers) {
    $destination = Join-Path $PromptDestination $source.Name
    $sourceHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $source.FullName).Hash.ToLowerInvariant()
    $destinationHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $destination).Hash.ToLowerInvariant()
    if ($sourceHash -ne $destinationHash) {
        throw "Prompt-pointer backup verification failed: $($source.FullName)"
    }
    [pscustomobject]@{
        source_path = $source.FullName
        backup_name = $source.Name
        bytes = $source.Length
        sha256 = $sourceHash
    }
}

$PromptArchiveInventory = @(
    Get-ChildItem -LiteralPath 'D:\Prompt\delta' -File -Force -ErrorAction SilentlyContinue |
        Select-Object Name, Length, LastWriteTimeUtc
)

$ManifestDocument = [ordered]@{
    schema_version = 1
    created_at_utc = [DateTime]::UtcNow.ToString('o')
    service_root = $ServiceRoot
    nginx_qa_base_sha = '9375675cdcc56fd3d05861de9375ceb19a71400c'
    entry_presence = @($EntryPresence)
    files = @($FileManifest)
    prompt_pointers = @($PromptManifest)
    prompt_archive_inventory = @($PromptArchiveInventory)
}
$ManifestPath = Join-Path $BackupRoot 'backup-manifest.json'
$ManifestDocument | ConvertTo-Json -Depth 8 |
    Set-Content -LiteralPath $ManifestPath -Encoding utf8
Get-FileHash -Algorithm SHA256 -LiteralPath $ManifestPath
```

Validate the backup copies, not the live files:

```powershell
$JsonFiles = @(
    'port_git_map.json',
    'agents.json',
    'project_sprints.json',
    'pending_project_sprints.json',
    'email_routes.json',
    'specializations.json',
    'runtime_state\queues\worker-all.json',
    'runtime_state\scheduled_tasks.json',
    'runtime_state\sequential-prompt-settings.json'
)
foreach ($relative in $JsonFiles) {
    $path = Join-Path $StateDestination $relative
    if (Test-Path -LiteralPath $path) {
        $null = Get-Content -LiteralPath $path -Raw | ConvertFrom-Json -Depth 100
    }
}

$history = Join-Path $StateDestination 'conversation_log.jsonl'
$lineNumber = 0
Get-Content -LiteralPath $history | ForEach-Object {
    $lineNumber++
    try {
        $null = $_ | ConvertFrom-Json -Depth 100
    } catch {
        throw "Invalid JSONL at backup line $lineNumber"
    }
}
```

Then assert the exact Delta checkpoint from the **backup copies**. This uses
the already installed system Python and does not import the application,
contact a server, or mutate either instance. Any mismatch invalidates this
runbook's frozen checkpoint and stops the deployment:

```powershell
@'
import hashlib
import json
import pathlib
import sys

root = pathlib.Path(sys.argv[1])
key = "github.com/chartjs333/delta"
config = json.loads((root / "port_git_map.json").read_text(encoding="utf-8-sig"))
sprints = json.loads((root / "project_sprints.json").read_text(encoding="utf-8-sig"))
project = config["projects"][key]
execution = project["agent_assignment"]
sprint_project = sprints["projects"][key]
assert str(project["project_phone"]) == "9000", "Wrong Delta project"
assert str(sprint_project["project_phone"]) == "9000", "Wrong sprint project"
assert sprint_project["current_sprint_id"] == "sprint-0001-783ef52c", "Sprint changed"
assert execution["mode"] == "sequential", "Runtime mode changed"
assert execution["strategy"] == "conditional_graph", "Runtime strategy changed"
assert execution["status"] == "active", "Sprint is not active"
assert execution["revision"] == 52, "Execution revision changed"
assignment_id = "898cbe80-1f5f-4348-ae65-4e6fc83239bc"
assert execution["current_assignment_id"] == assignment_id, "Assignment changed"
assert execution["current_node_id"] == "continuity-coordinator", "Node changed"
assert execution["phase"] == "node", "Phase changed"
assert execution["current_agent_id"] == "isc-s16-continuity-coordinator", "Actor changed"
assert "scope_control" not in execution, "Scope control is already present"
assignments = execution["assignments"]
assert len(assignments) == 22, "Assignment count changed"
assert sum(a.get("kind") == "graph_node" for a in assignments) == 8, "Graph count changed"
reviews = [a for a in assignments if a.get("kind") == "transition_review"]
assert len(reviews) == 14, "Review assignment count changed"
assert sum(a["status"] == "approved" for a in reviews) == 13, "Approval count changed"
assert sum(a["status"] == "rejected" for a in reviews) == 1, "Rejection count changed"
decisions = [r for a in assignments for r in a.get("reviews", [])]
assert len(decisions) == 14, "Embedded review count changed"
assert sum(r["decision"] == "APPROVE" for r in decisions) == 13, "Accepted reviews changed"
assert sum(r["decision"] == "REJECT" for r in decisions) == 1, "Rejected reviews changed"
assert len(execution["identity_deliveries"]) == 22, "Delivery count changed"
assert execution["visit_counts"]["formal-linkage"] == 2, "Formal occurrence changed"
assert execution["visit_counts"]["continuity-coordinator"] == 3, "Coordinator occurrence changed"
assert execution["workflow"]["required_approvals"] == 2, "Two-review gate changed"
active = execution["active_task"]
assert active["id"] == "3fe1172a-b165-48d7-bef5-e4111d2c1d05", "Active task changed"
assert active["metadata"]["assignment_id"] == assignment_id, "Task assignment mismatch"
canonical = json.dumps(active, ensure_ascii=False, sort_keys=True,
                       separators=(",", ":"), allow_nan=False).encode("utf-8")
assert hashlib.sha256(canonical).hexdigest() == (
    "b51c1bb5ccb25a8eace67237de861ef8d13b42d105d9f00d0a591fbd62fb0d81"
), "Issued task hash changed"
assert hashlib.sha256(active["message"].encode("utf-8")).hexdigest() == (
    "4ea64574a6f8086c78b9065260804ba0d1380029a7080dd5a4f604fbc9cc91f4"
), "Issued message hash changed"
print("PASS: copied Delta checkpoint, counts, two-review gate and issued hashes")
'@ | C:\Python312\python.exe - $StateDestination
if ($LASTEXITCODE -ne 0) { throw 'Copied Delta checkpoint verification failed' }
```

The backup contains operational prompts and state. Keep it outside Git and
evidence storage, under the restricted ACL. A backup is not accepted until
copy hashes, JSON checks, the expected Delta assignment, and the absence or
presence inventory all pass.

## 7. Deploy the exact prepared commit

This section changes code but does not apply an amendment.

```powershell
$ServiceRoot = 'D:\nginx-qa-release\umse-9375675'
$BaseSha = '9375675cdcc56fd3d05861de9375ceb19a71400c'
$FinalSha = '<FINAL_SHA>'

$LiveHead = git -C $ServiceRoot rev-parse HEAD
if ($LASTEXITCODE -ne 0 -or $LiveHead.Trim() -ne $BaseSha) {
    throw 'Pre-switch HEAD must be the exact audited live base'
}
$ActualLocalChanges = @(git -C $ServiceRoot status --porcelain=v1)
if ($LASTEXITCODE -ne 0) { throw 'Could not inspect serving worktree' }
$ExpectedLocalChanges = @(' M run.bat', '?? QUICKSTART.md')
if (@(Compare-Object ($ExpectedLocalChanges | Sort-Object) (
    $ActualLocalChanges | Sort-Object
)).Count -ne 0) {
    throw 'Serving worktree changes differ from the audited launcher and QUICKSTART'
}

git -C $ServiceRoot cat-file -e "$FinalSha^{commit}"
if ($LASTEXITCODE -ne 0) { throw 'Final commit is unavailable' }

git -C $ServiceRoot merge-base --is-ancestor $BaseSha $FinalSha
if ($LASTEXITCODE -ne 0) { throw 'Final commit is not based on the qualified base' }

$ForbiddenChanges = @(
    git -C $ServiceRoot diff --name-only $BaseSha $FinalSha -- `
        run.bat QUICKSTART.md requirements.txt requirements-dev.txt
)
if ($ForbiddenChanges.Count -ne 0) {
    throw "Unexpected launcher/dependency changes: $($ForbiddenChanges -join ', ')"
}

git -C $ServiceRoot switch --detach $FinalSha
if ((git -C $ServiceRoot rev-parse HEAD).Trim() -ne $FinalSha) {
    throw 'Checkout did not reach the exact final SHA'
}

git -C $ServiceRoot status --short
```

The only expected pre-existing worktree entries are the local `M run.bat` and
`?? QUICKSTART.md`. Any additional path is `ABORT`.

Record and compare the tested virtual environment. The current audited runtime
used Python `3.12.1`, FastAPI `0.142.2`, Uvicorn `0.54.0`, and jsonschema
`4.26.0`. `run.bat` performs dependency installation on startup, so an
unexpected `pip freeze --all` change is a deployment failure, not a harmless
side effect.

## 8. Start and verify preservation before apply

While the service is still stopped, run the compatibility gate from the exact
checked-out release code. At this pre-amendment checkpoint, scope control must
be absent:

```powershell
$ServiceRoot = 'D:\nginx-qa-release\umse-9375675'
Push-Location -LiteralPath $ServiceRoot
try {
    & "$ServiceRoot\.venv\Scripts\python.exe" -m `
        nginx_qa.scope_control_prestart `
        --state-file "$ServiceRoot\port_git_map.json" `
        --project 9000 `
        --require-scope-control absent
    if ($LASTEXITCODE -ne 0) {
        throw 'Scope-control runtime compatibility gate failed'
    }
} finally {
    Pop-Location
}
```

The command is read-only. An intended release that does not contain this
module is not a compatible post-amendment release and must not be started.

Launch `run.bat` from the same secure PowerShell process that received the
credentials, so the child inherits them without exposing them in arguments.
The audited launcher infers quick-tunnel startup from placeholder webhook
settings unless explicitly disabled. It also loads `.env` **after** inheriting
the environment, so inherited flags alone are insufficient. Check for `.env`
overrides without printing any values, then set the explicit startup policy:

```powershell
$ServiceRoot = 'D:\nginx-qa-release\umse-9375675'
$LocalEnvPath = Join-Path $ServiceRoot '.env'
if (Test-Path -LiteralPath $LocalEnvPath) {
    foreach ($line in Get-Content -LiteralPath $LocalEnvPath) {
        if ($line -match '^\s*(CLOUDFLARED_QUICK_TUNNEL|TELEGRAM_WEBHOOK_AUTO_REGISTER|TELEGRAM_DROP_PENDING_UPDATES|NGINX_QA_HTTP_PORT|NGINX_QA_SCOPE_CONTROL_[A-Z0-9_]+)\s*=') {
            throw "Local .env overrides protected startup setting: $($Matches[1])"
        }
    }
}
$env:CLOUDFLARED_QUICK_TUNNEL = '0'
$env:TELEGRAM_WEBHOOK_AUTO_REGISTER = '0'
$env:TELEGRAM_DROP_PENDING_UPDATES = '0'
$env:NGINX_QA_HTTP_PORT = '18025'

Start-Process -FilePath 'D:\nginx-qa-release\umse-9375675\run.bat' `
    -WorkingDirectory 'D:\nginx-qa-release\umse-9375675' `
    -WindowStyle Hidden
```

If the `.env` gate fails, stop and resolve the local configuration explicitly
before the maintenance attempt; do not edit or echo secrets. The startup policy
must remain in effect for every restart in sections 9, 11 and 12. No bootstrap
server, cloudflared helper, Telegram webhook registration, or pending-update
deletion may occur. If any such helper starts, stop the path-scoped service tree
and fail the deployment. Keep external ingress frozen throughout verification.

Then repeat the path-scoped topology check. Require exactly one listener on
`18025` descended from this checkout. Check `8025` only to confirm it remained
unchanged; do not stop or restart it.

Before amendment, verify all of the following:

- HEAD is exactly `<FINAL_SHA>`;
- current sprint is `sprint-0001-783ef52c`;
- preflight returns the audited assignment/node/phase and `mutated:false`;
- `source_repository.available:true`, resolution is
  `operator_repository_map`, and path is exactly `D:\delta`;
- `scope_control_present:false`;
- revision remains `52`;
- assignment count remains `22` (`8` graph + `14` review assignments), and
  embedded review decisions remain `14` (`13 APPROVE`, `1 REJECT`);
- current assignment and active task IDs are unchanged;
- issued task/message hashes match pre-deploy values;
- `visit_counts`, historical assignments, results, reviews, approvals,
  transitions, node IDs, and `required_approvals=2` are unchanged;
- no task was added to or removed from a queue;
- the canonical state-file hashes still match the coherent backup.

If startup or GET changes canonical state, stop the new binary and use the
pre-amendment rollback below. Do not continue to apply.

## 9. Separately authorized Delta apply

**Do not execute this section as part of deployment approval.** It requires a
separate live-scope authorization after sections 1–8 pass.

At the start of that later apply window, repeat sections 2, 3, 5, and 6 and
create a new restricted backup named `*-pre-amendment`. Restart the exact
`<FINAL_SHA>`, confirm that the restarted state matches this new backup, and
only then prepare the POST. Do not assume that an older pre-deploy backup is a
valid pre-amendment checkpoint. If the preserved assignment is no longer
`898cbe80-1f5f-4348-ae65-4e6fc83239bc`, this amendment is no longer applicable
and the operation must stop.

Retrieve fresh preflight and construct the exact request from it:

```powershell
$BaseUrl = 'http://127.0.0.1:18025'
$ProjectId = '9000'
$SprintId = 'sprint-0001-783ef52c'
$AdminHeaders = @{
    'X-Nginx-QA-Scope-Control-Token' =
        [Environment]::GetEnvironmentVariable(
            'NGINX_QA_SCOPE_CONTROL_ADMIN_TOKEN',
            'Process'
        )
}
$PreflightUri = (
    "$BaseUrl/api/v1/projects/$ProjectId/sprints/$SprintId/" +
    'scope-amendments/preflight'
)
$Preflight = Invoke-RestMethod -Method Get -Uri $PreflightUri `
    -Headers $AdminHeaders

if (-not $Preflight.applicable -or $Preflight.scope_control_present) {
    throw 'Delta is not at the expected pre-amendment checkpoint'
}
if ($Preflight.assignment_id -ne '898cbe80-1f5f-4348-ae65-4e6fc83239bc') {
    throw 'Active assignment changed'
}

$ApplyPayload = [ordered]@{
    schema_version = 1
    amendment_id = 'ISC-S16-D01'
    expected_execution_revision = [int]$Preflight.execution_revision
    expected_assignment_id = [string]$Preflight.assignment_id
    expected_node_id = [string]$Preflight.node_id
    expected_phase = [string]$Preflight.phase
    expected_issued_task_sha256 = [string]$Preflight.issued_task_sha256
    expected_issued_message_sha256 = [string]$Preflight.issued_message_sha256
    idempotency_key = 'delta:isc-s16:d01:65074657:v1'
    source = [ordered]@{
        repository_key = 'github.com/chartjs333/delta'
        base_commit = '0c8fe0426586810c1e1a342d11f0d5c42ce2de16'
        target_commit = '65074657df5f9ba3f16b7ac4912a563128d902ee'
        target_ref = 'refs/heads/agent/isc-s16-continuous-sprint'
        manifest = [ordered]@{
            path = 'orchestration/sprints/isc-s16-continuous/sequential-sprint.json'
            base_sha256 = '4f4e320e75b3d2bb1c95a89b2828637c5823f6906f8c165b78838f12108d9ca4'
            target_sha256 = '9e8824d142978b849d9240fd5004794f8c66483e979b94d04f174671dc646260'
        }
        amendment = [ordered]@{
            path = 'orchestration/sprints/isc-s16-continuous/scope-amendments/ISC-S16-D01-RECOVERY-PROOF-ONLY.json'
            sha256 = '1637f837aca3e028a66e5e1a306a83c695a4d010e35f7b53e54e44cecd38d655'
        }
        supporting_documents = @(
            [ordered]@{
                path = 'orchestration/sprints/isc-s16-continuous/handoffs/ISC-S16-CONTINUITY-SCOPE-SYNC.json'
                sha256 = '29ea0ba3a63d44054f3f135fc12a416be5968649c6df7a05b5f29e13fb1baa1f'
            }
        )
    }
    targets = [ordered]@{
        active_assignment = $true
        future_node_ids = @(
            'continuity-coordinator',
            'formal-linkage',
            'formal-qualification'
        )
        reviewer_agent_ids = @(
            'isc-s16-continuous-architecture-reviewer',
            'isc-s16-continuous-evidence-reviewer'
        )
        terminal_node_ids = @('completed')
    }
}

$ApplyJson = $ApplyPayload | ConvertTo-Json -Depth 20

# This POST is the live mutation and needs separate authorization.
$Receipt = Invoke-RestMethod -Method Post -Uri (
    "$BaseUrl/api/v1/projects/$ProjectId/sprints/$SprintId/scope-amendments"
) -Headers $AdminHeaders -ContentType 'application/json' -Body $ApplyJson
$AdminHeaders.Clear()
```

The first successful receipt must report:

```text
assignment_id          898cbe80-1f5f-4348-ae65-4e6fc83239bc
assignment_preserved   true
graph_advanced         false
queue_changed          false
from_execution_revision 52
execution_revision     53
effective_revision     1
deduplicated           false
```

An exact repeat with the same idempotency key and body must return the same
receipt with `deduplicated:true`. Do not test a mutation conflict against live
merely for evidence.

An exact retry also repairs a missing amendment audit receipt without
reapplying the state change. The response reports `audit_history_recorded`
and `audit_history_created`; the latter is true only when that request added
the missing audit entry.

Version 1 is deliberately one-shot. A different second amendment is rejected
with `SCOPE_ADDITIONAL_AMENDMENT_UNSUPPORTED`; it is not silently overlaid on
the first. A later amendment requires a separately qualified compositional
runtime version or a compatible forward migration.

Immediately after apply, confirm that the original issued active task and all
pre-amendment assignments/results/reviews are unchanged. Only execution
revision, timestamps, append-only amendment audit, and the separate
`scope_control`/binding are expected to change. There must be no dequeue,
enqueue, transition, new assignment, or graph movement.

## 10. Effective scope, ACK, and ordinary submissions

For every governed assignment, the executor performs this sequence:

1. receive a newly queued assignment through `POST /api/v1/agents/whoami`
   followed by its repository reply, with the role token header;
2. GET its `effective-scope`, with the same role token;
3. treat `effective` as authoritative and `issued` as immutable historical
   context;
4. ACK the exact returned `scope_context`, with the same role token;
5. include that exact `scope_context` in the ordinary result/review/handoff
   body and include the role token header again.

Do not claim a governed identity through the legacy
`GET /worker/all/{project}?to_phone={project}` poll. It returns
`SCOPE_CONTROLLED_IDENTITY_REQUIRES_WHOAMI` without removing the item. Generic
queue deletion similarly returns `SCOPE_CONTROLLED_QUEUE_MUTATION_FORBIDDEN`.
These guards ensure role-token validation and delivery registration happen
before GET/ACK/submission. The already active coordinator does not claim a new
item: it uses its preserved assignment ID and the effective-scope endpoint
shown below.

Example for the current coordinator, without exposing its token:

```powershell
$AssignmentId = '898cbe80-1f5f-4348-ae65-4e6fc83239bc'
$RoleHeaders = @{
    'X-Nginx-QA-Scope-Token' =
        [Environment]::GetEnvironmentVariable(
            'NGINX_QA_SCOPE_CONTROL_ROLE_TOKEN_2750',
            'Process'
        )
}

$Effective = Invoke-RestMethod -Method Get -Uri (
    "$BaseUrl/api/v1/projects/9000/assignments/$AssignmentId/effective-scope"
) -Headers $RoleHeaders

if ($Effective.precedence.rule -ne 'effective_scope_supersedes_conflicting_issued_scope') {
    throw 'Instruction precedence is not explicit'
}
if ($Effective.scope_context.assignment_id -ne $AssignmentId) {
    throw 'Scope context is not bound to this assignment'
}

$EffectiveJson = $Effective | ConvertTo-Json -Depth 100 -Compress
$ComputedEffectiveHash = $EffectiveJson | & `
    'D:\nginx-qa-release\umse-9375675\.venv\Scripts\python.exe' -c `
    "import hashlib,json,sys; d=json.load(sys.stdin); print(hashlib.sha256(json.dumps(d['effective_core'],ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False).encode('utf-8')).hexdigest())"
if ($ComputedEffectiveHash.Trim() -ne $Effective.effective_scope_integrity.sha256) {
    throw 'Effective scope integrity hash does not recompute'
}
if ($Effective.effective_scope_integrity.projected_active_task_in_hash -ne $false) {
    throw 'Unexpected effective scope hash contract'
}

$AckBody = [ordered]@{
    schema_version = 1
    scope_context = $Effective.scope_context
} | ConvertTo-Json -Depth 20

$Ack = Invoke-RestMethod -Method Post -Uri (
    "$BaseUrl/api/v1/projects/9000/assignments/$AssignmentId/effective-scope/ack"
) -Headers $RoleHeaders -ContentType 'application/json' -Body $AckBody
```

Do not copy `scope_context` by hand. Use the exact object returned for the
current assignment. ACKs are assignment-, actor-, amendment-, and effective
revision-bound; an ACK for an earlier occurrence or another reviewer is not
transferable.

`effective_scope_sha256` is independently reproducible over canonical UTF-8
JSON of `effective_core`, exactly `{profile,tasks}`. The rendered
`effective.active_task` is a transport projection that carries the same
profile/tasks, precedence banner and context; it is explicitly excluded from
that hash so current and not-yet-rendered future assignments use one stable
contract. The cached active task is deterministically rederived from the
issued snapshot and effective core and compared by the shared runtime/prestart
validator; it is not an independent, unverified source of authority.
Assignment ID, node, occurrence, actor, amendment source commit and
source blob hash are bound separately in `scope_context`.

| Governed role | Phone/token environment | Required action |
|---|---|---|
| Current `continuity-coordinator` | `2750` / `NGINX_QA_SCOPE_CONTROL_ROLE_TOKEN_2750` | GET + new ACK before its handoff; submit exact context |
| First new architecture review | `2791` / `..._2791` | claim with token, GET + assignment-specific ACK, `APPROVE` with exact context |
| Second new evidence review | `2792` / `..._2792` | independent claim, GET + independent ACK, `APPROVE` with exact context |
| New `formal-linkage` visit 3 | `2753` / `..._2753` | GET + new ACK before `NO_GO` or other result |
| Two reviewers after formal result | `2791`, `2792` | each new review assignment requires a new GET + ACK + exact context |
| Returned coordinator occurrence | `2750` / `..._2750` | new assignment ID, new GET, new ACK; receives the active effective scope |
| `formal-qualification`, if reached | `2754` / `..._2754` | GET + ACK + exact context |
| Historical completed assignments/reviews | none | never retroactively ACKed or rewritten |
| Terminal node | none | no executor token |

The two reviewer assignments must carry the amendment inherited from their
source transition and the amended reviewer profile. The two historical
reviewers/approvals remain unchanged.

Raw tokens must never appear in:

- request JSON or query strings;
- `scope_context`, receipts, state, queues, history, or sprint archives;
- Git, evidence, screenshots, logs, response archives, or agent prompt files.

## 11. Rollback before amendment

Before `scope_control` exists, binary rollback is allowed only after stopping
the complete `18025` process tree and verifying offline that:

```powershell
$Config = Get-Content -LiteralPath (
    'D:\nginx-qa-release\umse-9375675\port_git_map.json'
) -Raw | ConvertFrom-Json -Depth 100
$Execution = $Config.projects.'github.com/chartjs333/delta'.agent_assignment
if ($null -ne $Execution.scope_control) {
    throw 'scope_control exists; pre-amendment rollback is forbidden'
}
```

If canonical state hashes still equal the coherent backup:

1. switch the stopped checkout back to
   `9375675cdcc56fd3d05861de9375ceb19a71400c`;
2. confirm only the known local `run.bat` and `QUICKSTART.md` changes remain;
3. launch from the same release directory;
4. repeat the assignment/history preservation checks.

If any canonical hash differs, restore the complete coherent pre-deployment
backup before starting the old binary. Do not mix individual files from
different checkpoints.

## 12. Rollback after amendment

Never run `9375675cdcc56fd3d05861de9375ceb19a71400c` directly against a state
containing `scope_control`. The old code can ignore the effective layer and
serve the historical issued instruction, bypassing token/ACK/context guards.

There are only two safe cases:

### 12.1 No ACK or graph movement occurred

With separate rollback authorization:

1. freeze ingress and stop every path-scoped writer;
2. archive the complete post-amendment state under a new restricted directory;
3. prove there was no ACK, result, review, handoff, new assignment, or graph
   transition after apply;
4. restore the **entire** pre-amendment coherent backup, including optional-file
   absence and stable prompt pointers;
5. quarantine prompt archive files absent from the pre-amendment inventory;
6. verify offline that `scope_control` is absent and every restored SHA matches;
7. only then switch to and start the old base.

Move current state to a validated quarantine path rather than deleting it.
Never recursively remove a computed or unresolved path.

### 12.2 ACK, review, result, or graph movement occurred

Restoring the old snapshot would erase valid post-amendment history and is not
allowed. Keep the service stopped or deploy a compatible descendant/forward
fix that supports:

```text
minimum_runtime_capability = legacy_scope_control_v1
schema_version = 1
```

Even a compensating amendment does not make the old base compatible with the
remaining amendment and ACK audit records. Rollback is therefore forward-only
once governed work has begun.

Before starting any compatible descendant against post-amendment state, while
all writers remain stopped, require the executable gate to accept the state:

```powershell
$ServiceRoot = 'D:\nginx-qa-release\umse-9375675'
Push-Location -LiteralPath $ServiceRoot
try {
    & "$ServiceRoot\.venv\Scripts\python.exe" -m `
        nginx_qa.scope_control_prestart `
        --state-file "$ServiceRoot\port_git_map.json" `
        --project 9000 `
        --require-scope-control present
    if ($LASTEXITCODE -ne 0) {
        throw 'Post-amendment state is incompatible; do not start this release'
    }
} finally {
    Pop-Location
}
```

## 13. Go/no-go summary

Proceed to deployment only when all are true:

- exact base and `<FINAL_SHA>` are proven;
- the targeted scope-control and affected existing regression results for
  `<FINAL_SHA>` are accepted, with unexecuted checks explicitly recorded;
- the `18025` process tree is path-scoped and `8025` is excluded;
- all writers and ingress can be frozen;
- a coherent backup can be created and verified after stop;
- secrets are provisioned out of band and all required names pass presence
  checks without revealing values;
- post-start state is unchanged and preflight is applicable;
- a separate authorization exists for the amendment POST.

Otherwise stop at preparation. Deployment authorization does not authorize
scope apply, ACK, handoff, review, import, graph transition, or rollback.
