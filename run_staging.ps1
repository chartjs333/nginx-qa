[CmdletBinding()]
param(
    [ValidateSet("Check", "Setup", "Start")]
    [string]$Action = "Start"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$serviceRoot = [System.IO.Path]::GetFullPath($PSScriptRoot).TrimEnd("\")
$stageRoot = [System.IO.Path]::GetFullPath(
    (Split-Path -Parent $serviceRoot)
).TrimEnd("\")
$expectedServiceRoot = "D:\nginx-qa-staging\universal-managed-sprint-engine"
$expectedBranch = "agent/umse-07-staging-qualification"
$expectedOrigin = "https://github.com/chartjs333/nginx-qa.git"
$liveRoot = "D:\nginx-qa"
$environmentPath = Join-Path $serviceRoot ".env.staging"
$venvRoot = Join-Path $stageRoot ".venv"
$pythonPath = Join-Path $venvRoot "Scripts\python.exe"

function Resolve-NormalizedPath {
    param([Parameter(Mandatory = $true)][string]$Path)

    return [System.IO.Path]::GetFullPath($Path).TrimEnd("\")
}

function Test-PathOverlap {
    param(
        [Parameter(Mandatory = $true)][string]$First,
        [Parameter(Mandatory = $true)][string]$Second
    )

    $left = (Resolve-NormalizedPath $First) + "\"
    $right = (Resolve-NormalizedPath $Second) + "\"
    return $left.StartsWith($right, [System.StringComparison]::OrdinalIgnoreCase) -or
        $right.StartsWith($left, [System.StringComparison]::OrdinalIgnoreCase)
}

function Import-StagingEnvironment {
    if (-not (Test-Path -LiteralPath $environmentPath -PathType Leaf)) {
        throw "Missing $environmentPath. Copy .env.staging.example to .env.staging first."
    }

    $seen = @{}
    foreach ($rawLine in Get-Content -LiteralPath $environmentPath) {
        $line = $rawLine.Trim()
        if (-not $line -or $line.StartsWith("#")) {
            continue
        }
        $separator = $line.IndexOf("=")
        if ($separator -le 0) {
            throw "Invalid .env.staging line: $rawLine"
        }
        $name = $line.Substring(0, $separator).Trim()
        $value = $line.Substring($separator + 1).Trim()
        if ($name -notmatch '^NGINX_QA_[A-Z0-9_]+$') {
            throw "Unexpected .env.staging variable: $name"
        }
        if ($seen.ContainsKey($name)) {
            throw "Duplicate .env.staging variable: $name"
        }
        $seen[$name] = $true
        [System.Environment]::SetEnvironmentVariable(
            $name,
            $value,
            [System.EnvironmentVariableTarget]::Process
        )
    }
}

function Assert-ExactValue {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][string]$Expected
    )

    $actual = [System.Environment]::GetEnvironmentVariable($Name, "Process")
    if ($actual -cne $Expected) {
        throw "$Name must be exactly '$Expected'; got '$actual'."
    }
}

if ($serviceRoot -ine $expectedServiceRoot) {
    throw "Staging checkout must be exactly $expectedServiceRoot; got $serviceRoot."
}
if (-not (Test-Path -LiteralPath (Join-Path $serviceRoot ".git"))) {
    throw "Staging service root is not a Git checkout."
}
foreach ($path in @($stageRoot, $serviceRoot)) {
    if ((Get-Item -LiteralPath $path).Attributes -band [System.IO.FileAttributes]::ReparsePoint) {
        throw "Staging path must not be a filesystem reparse point: $path"
    }
}

$origin = (& git -C $serviceRoot remote get-url origin).Trim()
if ($LASTEXITCODE -ne 0 -or $origin -cne $expectedOrigin) {
    throw "Staging origin must be exactly $expectedOrigin; got '$origin'."
}
$branch = (& git -C $serviceRoot branch --show-current).Trim()
if ($LASTEXITCODE -ne 0 -or $branch -cne $expectedBranch) {
    throw "Staging branch must be exactly $expectedBranch; got '$branch'."
}

Import-StagingEnvironment

$expectedValues = [ordered]@{
    NGINX_QA_HTTP_HOST = "127.0.0.1"
    NGINX_QA_HTTP_PORT = "18025"
    NGINX_QA_SERVICE_ROOT = "D:/nginx-qa-staging/universal-managed-sprint-engine"
    NGINX_QA_PROTECTED_ROOTS = '["D:/nginx-qa","D:/nginx-qa-umse","D:/Prompt"]'
    NGINX_QA_GIT_FETCH_TIMEOUT_SECONDS = "120"
    NGINX_QA_CHILD_PORT_RANGE = "18100-18199"
    NGINX_QA_INSTANCE_ID = "universal-managed-sprint-engine-staging"
    NGINX_QA_DISABLE_TELEGRAM = "1"
    NGINX_QA_DISABLE_TUNNEL = "1"
}
foreach ($entry in $expectedValues.GetEnumerator()) {
    Assert-ExactValue -Name $entry.Key -Expected $entry.Value
}

$isolatedRoots = @(
    $serviceRoot,
    (Resolve-NormalizedPath $env:NGINX_QA_RUNTIME_ROOT),
    (Resolve-NormalizedPath $env:NGINX_QA_PROMPT_ROOT),
    (Resolve-NormalizedPath $env:NGINX_QA_MANAGED_ROOT),
    (Resolve-NormalizedPath $venvRoot)
)
for ($index = 0; $index -lt $isolatedRoots.Count; $index++) {
    if (Test-PathOverlap -First $isolatedRoots[$index] -Second $liveRoot) {
        throw "Staging path overlaps protected live root: $($isolatedRoots[$index])"
    }
    for ($other = $index + 1; $other -lt $isolatedRoots.Count; $other++) {
        if (Test-PathOverlap -First $isolatedRoots[$index] -Second $isolatedRoots[$other]) {
            throw "Staging roots overlap: $($isolatedRoots[$index]) and $($isolatedRoots[$other])"
        }
    }
}

$status = & git -C $serviceRoot status --porcelain=v1 --untracked-files=normal
if ($LASTEXITCODE -ne 0) {
    throw "Unable to verify staging Git state."
}
if ($status) {
    throw "Staging checkout is dirty; refusing to start.`n$($status -join "`n")"
}

$listener = Get-NetTCPConnection -LocalPort 18025 -State Listen -ErrorAction SilentlyContinue
if ($listener) {
    $owners = ($listener | Select-Object -ExpandProperty OwningProcess -Unique) -join ", "
    throw "Port 18025 is already listening (PID: $owners)."
}

if ($Action -eq "Setup") {
    if (-not (Test-Path -LiteralPath $pythonPath -PathType Leaf)) {
        & python -m venv $venvRoot
        if ($LASTEXITCODE -ne 0) {
            throw "Unable to create isolated staging virtual environment."
        }
    }
    & $pythonPath -m pip install --upgrade pip
    if ($LASTEXITCODE -ne 0) {
        throw "Unable to upgrade pip in the staging virtual environment."
    }
    & $pythonPath -m pip install -r (Join-Path $serviceRoot "requirements.txt") -r (Join-Path $serviceRoot "requirements-dev.txt")
    if ($LASTEXITCODE -ne 0) {
        throw "Unable to install staging dependencies."
    }
}

if (-not (Test-Path -LiteralPath $pythonPath -PathType Leaf)) {
    throw "Missing isolated staging Python at $pythonPath. Run with -Action Setup first."
}

$validationScript = @'
from nginx_qa.managed_import import normalize_managed_runtime_config
from nginx_qa.sprint_types import managed_runtime_config_invariant_issues
from nginx_qa.workspace_manager import ManagedWorkspaceManager
from pathlib import Path
import sys

config = normalize_managed_runtime_config()
issues = managed_runtime_config_invariant_issues(config)
if issues:
    raise SystemExit("invalid staging runtime config: " + ",".join(issues))
forbidden = tuple(config["protected_roots"]) + (
    config["service_root"],
    str(Path(sys.prefix).resolve(strict=False)),
    str(Path(sys.base_prefix).resolve(strict=False)),
)
for field in ("runtime_root", "prompt_root", "managed_root"):
    ManagedWorkspaceManager.validate_isolated_root(
        config[field], protected_roots=forbidden
    )
'@
Push-Location $serviceRoot
try {
    $validationScript | & $pythonPath -
    if ($LASTEXITCODE -ne 0) {
        throw "Managed staging configuration validation failed."
    }

    if ($Action -eq "Check" -or $Action -eq "Setup") {
        Write-Output "Staging isolation check passed."
        exit 0
    }

    foreach ($path in @(
        $env:NGINX_QA_RUNTIME_ROOT,
        $env:NGINX_QA_PROMPT_ROOT,
        $env:NGINX_QA_MANAGED_ROOT
    )) {
        New-Item -ItemType Directory -Path $path -Force | Out-Null
    }

    $legacyRuntimeRoot = Join-Path $serviceRoot "runtime_state"
    $promptSettingsPath = Join-Path $legacyRuntimeRoot "sequential-prompt-settings.json"
    $isolatedPromptRoot = Resolve-NormalizedPath $env:NGINX_QA_PROMPT_ROOT
    $expectedPromptSettings = [ordered]@{
        directory_template = Join-Path $isolatedPromptRoot "{repository}"
        agent_latest_file_template = Join-Path $isolatedPromptRoot "{repository}_{agent_phone}-latest.prompt"
    }
    New-Item -ItemType Directory -Path $legacyRuntimeRoot -Force | Out-Null
    if (Test-Path -LiteralPath $promptSettingsPath -PathType Leaf) {
        $actualPromptSettings = Get-Content -LiteralPath $promptSettingsPath -Raw | ConvertFrom-Json
        if (
            $actualPromptSettings.directory_template -cne $expectedPromptSettings.directory_template -or
            $actualPromptSettings.agent_latest_file_template -cne $expectedPromptSettings.agent_latest_file_template
        ) {
            throw "Existing staging prompt settings do not use the isolated prompt root."
        }
    }
    else {
        $promptSettingsJson = $expectedPromptSettings | ConvertTo-Json
        [System.IO.File]::WriteAllText(
            $promptSettingsPath,
            $promptSettingsJson,
            [System.Text.UTF8Encoding]::new($false)
        )
    }

    Remove-Item Env:TELEGRAM_BOT_TOKEN -ErrorAction SilentlyContinue
    Remove-Item Env:TELEGRAM_WEBHOOK_URL -ErrorAction SilentlyContinue
    Remove-Item Env:CLOUDFLARED_PUBLIC_URL -ErrorAction SilentlyContinue
    Remove-Item Env:CLOUDFLARED_QUICK_TUNNEL -ErrorAction SilentlyContinue

    & $pythonPath -m uvicorn main:app --host $env:NGINX_QA_HTTP_HOST --port $env:NGINX_QA_HTTP_PORT
    exit $LASTEXITCODE
}
finally {
    Pop-Location
}
