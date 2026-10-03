[CmdletBinding()]
param(
    [ValidateSet("Check", "Setup", "Start")]
    [string]$Action = "Start",

    [Parameter(Mandatory = $true)]
    [ValidatePattern("^[0-9A-Fa-f]{40}$")]
    [string]$ExpectedCommit
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# BEGIN TESTABLE FUNCTIONS
function Resolve-NormalizedPath {
    param([Parameter(Mandatory = $true)][string]$Path)

    if ([string]::IsNullOrWhiteSpace($Path)) {
        throw "Path must not be empty."
    }
    if ($Path.StartsWith("\\") -or $Path.StartsWith("//")) {
        throw "UNC and device paths are not allowed: $Path"
    }
    $full = [System.IO.Path]::GetFullPath($Path)
    $root = [System.IO.Path]::GetPathRoot($full)
    if (-not $root -or $root -notmatch '^[A-Za-z]:[\\/]$') {
        throw "Path must be an absolute DOS path: $Path"
    }
    $normalized = $full.Replace("/", "\").TrimEnd("\")
    if ($normalized -match '^[A-Za-z]:$') {
        $normalized += "\"
    }
    return $normalized
}

function Assert-CanonicalNonReparsePath {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [ValidateSet("Any", "Container", "Leaf")]
        [string]$PathKind = "Any",
        [switch]$AllowMissing,
        [string]$Label = "path"
    )

    $full = Resolve-NormalizedPath $Path
    $root = [System.IO.Path]::GetPathRoot($full)
    $relative = $full.Substring($root.Length).Trim("\")
    $current = $root
    if ($relative) {
        foreach ($segment in $relative.Split([char]'\')) {
            if (-not $segment) {
                continue
            }
            $current = Join-Path $current $segment
            $item = Get-Item -LiteralPath $current -Force -ErrorAction SilentlyContinue
            if ($null -eq $item) {
                continue
            }
            if ($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) {
                throw "$Label contains a filesystem reparse point: $current"
            }
            $itemPath = Resolve-NormalizedPath $item.FullName
            $candidatePath = Resolve-NormalizedPath $current
            if ($itemPath -ine $candidatePath) {
                throw "$Label contains a filesystem alias: $current -> $itemPath"
            }
        }
    }

    $exists = Test-Path -LiteralPath $full
    if (-not $exists) {
        if (-not $AllowMissing) {
            throw "$Label does not exist: $full"
        }
        return $full
    }
    if ($PathKind -eq "Container" -and -not (Test-Path -LiteralPath $full -PathType Container)) {
        throw "$Label must be a directory: $full"
    }
    if ($PathKind -eq "Leaf" -and -not (Test-Path -LiteralPath $full -PathType Leaf)) {
        throw "$Label must be a regular file: $full"
    }
    return $full
}

function Test-PathOverlap {
    param(
        [Parameter(Mandatory = $true)][string]$First,
        [Parameter(Mandatory = $true)][string]$Second
    )

    $left = Resolve-NormalizedPath $First
    $right = Resolve-NormalizedPath $Second
    if ($left -ieq $right) {
        return $true
    }
    $leftPrefix = if ($left.EndsWith("\")) { $left } else { $left + "\" }
    $rightPrefix = if ($right.EndsWith("\")) { $right } else { $right + "\" }
    return $leftPrefix.StartsWith($rightPrefix, [System.StringComparison]::OrdinalIgnoreCase) -or
        $rightPrefix.StartsWith($leftPrefix, [System.StringComparison]::OrdinalIgnoreCase)
}

function Test-PathIsWithin {
    param(
        [Parameter(Mandatory = $true)][string]$Child,
        [Parameter(Mandatory = $true)][string]$Parent
    )

    $childPath = Resolve-NormalizedPath $Child
    $parentPath = Resolve-NormalizedPath $Parent
    if ($childPath -ieq $parentPath) {
        return $true
    }
    $prefix = if ($parentPath.EndsWith("\")) { $parentPath } else { $parentPath + "\" }
    return $childPath.StartsWith($prefix, [System.StringComparison]::OrdinalIgnoreCase)
}

function Assert-StandaloneGitCheckout {
    param(
        [Parameter(Mandatory = $true)][string]$ServiceRoot,
        [Parameter(Mandatory = $true)][string]$ExpectedOrigin,
        [Parameter(Mandatory = $true)][string]$ExpectedBranch,
        [Parameter(Mandatory = $true)][string]$ExpectedCommit
    )

    $root = Assert-CanonicalNonReparsePath -Path $ServiceRoot -PathKind Container -Label "service root"
    $gitDirectory = Assert-CanonicalNonReparsePath `
        -Path (Join-Path $root ".git") `
        -PathKind Container `
        -Label "service Git directory"

    $topLevel = @(& git -C $root rev-parse --show-toplevel 2>$null)
    if ($LASTEXITCODE -ne 0 -or $topLevel.Count -ne 1) {
        throw "Unable to resolve the staging Git top-level."
    }
    if ((Resolve-NormalizedPath $topLevel[0].Trim()) -ine $root) {
        throw "Staging Git top-level escaped the service root."
    }

    $absoluteGitDirectory = @(& git -C $root rev-parse --absolute-git-dir 2>$null)
    if ($LASTEXITCODE -ne 0 -or $absoluteGitDirectory.Count -ne 1) {
        throw "Unable to resolve the staging Git directory."
    }
    if ((Resolve-NormalizedPath $absoluteGitDirectory[0].Trim()) -ine $gitDirectory) {
        throw "Staging checkout uses an external or shared Git directory."
    }
    foreach ($redirect in @("objects\info\alternates", "commondir")) {
        if (Test-Path -LiteralPath (Join-Path $gitDirectory $redirect)) {
            throw "Staging checkout uses forbidden Git storage indirection: $redirect"
        }
    }

    $origin = @(& git -C $root remote get-url origin 2>$null)
    if ($LASTEXITCODE -ne 0 -or $origin.Count -ne 1 -or $origin[0].Trim() -cne $ExpectedOrigin) {
        throw "Staging origin must be exactly $ExpectedOrigin."
    }
    $branch = @(& git -C $root branch --show-current 2>$null)
    if ($LASTEXITCODE -ne 0 -or $branch.Count -ne 1 -or $branch[0].Trim() -cne $ExpectedBranch) {
        throw "Staging branch must be exactly $ExpectedBranch."
    }
    $head = @(& git -C $root rev-parse "HEAD^{commit}" 2>$null)
    if ($LASTEXITCODE -ne 0 -or $head.Count -ne 1 -or $head[0].Trim() -ine $ExpectedCommit) {
        throw "Staging HEAD does not match the explicitly approved commit $ExpectedCommit."
    }

    $remoteRef = "refs/heads/$ExpectedBranch"
    $remoteHead = @(& git -C $root ls-remote --exit-code origin $remoteRef 2>$null)
    if ($LASTEXITCODE -ne 0 -or $remoteHead.Count -ne 1) {
        throw "Unable to verify the exact remote staging branch head."
    }
    $remoteCommit = ($remoteHead[0] -split '\s+')[0]
    if ($remoteCommit -ine $ExpectedCommit) {
        throw "Remote staging branch is not the explicitly approved commit $ExpectedCommit."
    }

    $status = @(& git -C $root status --porcelain=v1 --untracked-files=all 2>$null)
    if ($LASTEXITCODE -ne 0) {
        throw "Unable to verify staging Git state."
    }
    if ($status.Count -gt 0) {
        throw "Staging checkout is dirty; refusing to continue.`n$($status -join "`n")"
    }
    return $head[0].Trim().ToLowerInvariant()
}

function Get-FileSha256 {
    param([Parameter(Mandatory = $true)][string]$Path)

    $stream = [System.IO.File]::Open(
        $Path,
        [System.IO.FileMode]::Open,
        [System.IO.FileAccess]::Read,
        [System.IO.FileShare]::Read
    )
    try {
        $algorithm = [System.Security.Cryptography.SHA256]::Create()
        try {
            $digest = $algorithm.ComputeHash($stream)
        }
        finally {
            $algorithm.Dispose()
        }
    }
    finally {
        $stream.Dispose()
    }
    return ([System.BitConverter]::ToString($digest)).Replace("-", "").ToLowerInvariant()
}

function Assert-TrustedBaseInterpreter {
    param(
        [Parameter(Mandatory = $true)][string]$PythonPath,
        [Parameter(Mandatory = $true)][string[]]$ProtectedRoots,
        [Parameter(Mandatory = $true)][string[]]$ForbiddenRoots
    )

    $executable = Assert-CanonicalNonReparsePath `
        -Path $PythonPath -PathKind Leaf -Label "base Python interpreter"
    foreach ($root in @($ProtectedRoots) + @($ForbiddenRoots)) {
        if (Test-PathOverlap -First $executable -Second $root) {
            throw "Base Python interpreter overlaps a protected or staging mutation root: $root"
        }
    }

    $hashBefore = Get-FileSha256 -Path $executable
    $probeCode = 'import json,platform,sys; print(json.dumps(dict(executable=sys.executable,prefix=sys.prefix,base_prefix=sys.base_prefix,version=platform.python_version()), sort_keys=True))'
    $rawProbe = @(& $executable -I -B -c $probeCode 2>$null)
    if ($LASTEXITCODE -ne 0 -or $rawProbe.Count -ne 1) {
        throw "Unable to verify base Python interpreter provenance."
    }
    try {
        $probe = $rawProbe[0] | ConvertFrom-Json
    }
    catch {
        throw "Base Python interpreter returned invalid provenance."
    }
    $reportedExecutable = Assert-CanonicalNonReparsePath `
        -Path ([string]$probe.executable) -PathKind Leaf -Label "reported base Python interpreter"
    $prefix = Assert-CanonicalNonReparsePath `
        -Path ([string]$probe.prefix) -PathKind Container -Label "base Python prefix"
    $basePrefix = Assert-CanonicalNonReparsePath `
        -Path ([string]$probe.base_prefix) -PathKind Container -Label "base Python base prefix"
    if ($reportedExecutable -ine $executable -or $prefix -ine $basePrefix) {
        throw "Configured base Python must be a non-virtualenv interpreter with exact executable identity."
    }
    if (-not (Test-PathIsWithin -Child $executable -Parent $basePrefix)) {
        throw "Base Python executable is outside its reported base prefix."
    }
    foreach ($root in @($ProtectedRoots) + @($ForbiddenRoots)) {
        if (Test-PathOverlap -First $basePrefix -Second $root) {
            throw "Base Python prefix overlaps a protected or staging mutation root: $root"
        }
    }
    $hashAfter = Get-FileSha256 -Path $executable
    if ($hashAfter -cne $hashBefore) {
        throw "Base Python interpreter changed during provenance verification."
    }
    $version = [string]$probe.version
    if ($version -notmatch '^\d+\.\d+\.\d+(?:[A-Za-z0-9.+-]*)$') {
        throw "Base Python interpreter returned an invalid version."
    }
    return [pscustomobject]@{
        executable = $executable
        prefix = $prefix
        base_prefix = $basePrefix
        version = $version
        sha256 = $hashAfter
    }
}

function Assert-StagingVirtualEnvironment {
    param(
        [Parameter(Mandatory = $true)][string]$VenvRoot,
        [Parameter(Mandatory = $true)][string]$PythonPath,
        [Parameter(Mandatory = $true)][string]$ExpectedBasePrefix
    )

    $root = Assert-CanonicalNonReparsePath `
        -Path $VenvRoot -PathKind Container -Label "staging virtual environment"
    $executable = Assert-CanonicalNonReparsePath `
        -Path $PythonPath -PathKind Leaf -Label "staging virtualenv Python"
    $null = Assert-CanonicalNonReparsePath `
        -Path (Join-Path $root "pyvenv.cfg") -PathKind Leaf -Label "staging pyvenv.cfg"
    if (-not (Test-PathIsWithin -Child $executable -Parent $root)) {
        throw "Staging virtualenv Python escaped its configured root."
    }

    $probeCode = 'import json,sys; print(json.dumps(dict(executable=sys.executable,prefix=sys.prefix,base_prefix=sys.base_prefix), sort_keys=True))'
    $rawProbe = @(& $executable -I -B -c $probeCode 2>$null)
    if ($LASTEXITCODE -ne 0 -or $rawProbe.Count -ne 1) {
        throw "Unable to verify staging virtual environment provenance."
    }
    $probe = $rawProbe[0] | ConvertFrom-Json
    $reportedExecutable = Assert-CanonicalNonReparsePath `
        -Path ([string]$probe.executable) -PathKind Leaf -Label "reported staging Python"
    $reportedPrefix = Assert-CanonicalNonReparsePath `
        -Path ([string]$probe.prefix) -PathKind Container -Label "reported staging prefix"
    $reportedBasePrefix = Assert-CanonicalNonReparsePath `
        -Path ([string]$probe.base_prefix) -PathKind Container -Label "reported staging base prefix"
    if (
        $reportedExecutable -ine $executable -or
        $reportedPrefix -ine $root -or
        $reportedBasePrefix -ine (Resolve-NormalizedPath $ExpectedBasePrefix)
    ) {
        throw "Staging virtual environment does not have the expected exact provenance."
    }
    return $probe
}

function Get-OwnershipState {
    param(
        [Parameter(Mandatory = $true)][string]$StateBase,
        [Parameter(Mandatory = $true)][string]$MarkerPath,
        [Parameter(Mandatory = $true)][System.Collections.IDictionary]$ExpectedRecord
    )

    $base = Assert-CanonicalNonReparsePath `
        -Path $StateBase -PathKind Container -AllowMissing -Label "staging state base"
    $marker = Resolve-NormalizedPath $MarkerPath
    $expectedMarkerPath = Resolve-NormalizedPath (Join-Path $base ".nginx-qa-staging-owner.json")
    if ($marker -ine $expectedMarkerPath) {
        throw "Ownership marker must be the fixed root-local marker $expectedMarkerPath."
    }
    if (Test-Path -LiteralPath $marker) {
        $null = Assert-CanonicalNonReparsePath `
            -Path $marker -PathKind Leaf -Label "staging ownership marker"
        $markerItem = Get-Item -LiteralPath $marker -Force
        if ($markerItem.Length -gt 32768) {
            throw "Staging ownership marker is unexpectedly large."
        }
        try {
            $actual = Get-Content -LiteralPath $marker -Raw | ConvertFrom-Json
        }
        catch {
            throw "Staging ownership marker is not valid JSON."
        }
        $parsedInitializationId = [guid]::Empty
        if (
            $null -eq $actual.initialization_id -or
            -not [guid]::TryParseExact(
                [string]$actual.initialization_id,
                "D",
                [ref]$parsedInitializationId
            ) -or
            $parsedInitializationId -eq [guid]::Empty
        ) {
            throw "Staging ownership marker has an invalid initialization_id."
        }
        $expectedMarker = [ordered]@{
            schema_version = 1
            initialization_id = $parsedInitializationId.ToString("D")
            ownership = $ExpectedRecord
        }
        $expectedJson = ([pscustomobject]$expectedMarker | ConvertTo-Json -Depth 12 -Compress)
        $actualJson = ($actual | ConvertTo-Json -Depth 12 -Compress)
        if ($actualJson -cne $expectedJson) {
            throw "Staging ownership marker does not exactly match this launch."
        }
        return [pscustomobject]@{
            Mode = "Restart"
            InitializationId = $parsedInitializationId.ToString("D")
        }
    }
    if (Test-Path -LiteralPath $base) {
        $entries = @(Get-ChildItem -LiteralPath $base -Force)
        if ($entries.Count -gt 0) {
            throw "Staging state base has unknown content but no ownership marker: $base"
        }
    }
    return [pscustomobject]@{
        Mode = "FirstUse"
        InitializationId = $null
    }
}

function Initialize-OwnershipMarker {
    param(
        [Parameter(Mandatory = $true)][string]$StateBase,
        [Parameter(Mandatory = $true)][string]$MarkerPath,
        [Parameter(Mandatory = $true)][System.Collections.IDictionary]$ExpectedRecord
    )

    $base = Resolve-NormalizedPath $StateBase
    $marker = Resolve-NormalizedPath $MarkerPath
    if (Test-Path -LiteralPath $base) {
        $entries = @(Get-ChildItem -LiteralPath $base -Force)
        if ($entries.Count -gt 0) {
            throw "First-use staging state base must be empty: $base"
        }
    }
    else {
        New-Item -ItemType Directory -Path $base | Out-Null
    }
    $null = Assert-CanonicalNonReparsePath `
        -Path $base -PathKind Container -Label "initialized staging state base"
    if (Test-Path -LiteralPath $marker) {
        throw "Ownership marker appeared concurrently; refusing to overwrite it."
    }

    $initializationId = [guid]::NewGuid().ToString("D")
    $markerRecord = [ordered]@{
        schema_version = 1
        initialization_id = $initializationId
        ownership = $ExpectedRecord
    }
    $temporary = Join-Path $base (".nginx-qa-staging-owner." + [guid]::NewGuid().ToString("N") + ".tmp")
    try {
        $json = ([pscustomobject]$markerRecord | ConvertTo-Json -Depth 12)
        [System.IO.File]::WriteAllText(
            $temporary,
            $json,
            [System.Text.UTF8Encoding]::new($false)
        )
        [System.IO.File]::Move($temporary, $marker)
    }
    finally {
        if (Test-Path -LiteralPath $temporary) {
            Remove-Item -LiteralPath $temporary -Force
        }
    }
    return Get-OwnershipState `
        -StateBase $base `
        -MarkerPath $marker `
        -ExpectedRecord $ExpectedRecord
}

function Assert-LegacyStateOwnership {
    param(
        [Parameter(Mandatory = $true)][string[]]$Paths,
        [Parameter(Mandatory = $true)][bool]$OwnershipEstablished
    )

    foreach ($path in $Paths) {
        $item = Get-Item -LiteralPath $path -Force -ErrorAction SilentlyContinue
        if ($null -eq $item) {
            continue
        }
        if (-not $OwnershipEstablished) {
            throw "Unknown legacy staging state exists before ownership initialization: $path"
        }
        $null = Assert-CanonicalNonReparsePath `
            -Path $path -PathKind Any -Label "owned legacy staging state"
    }
}

function ConvertTo-AllowedChildOwnerMap {
    param(
        [Parameter(Mandatory = $true)]
        [AllowEmptyCollection()]
        [object[]]$AuthenticatedListeners
    )

    $result = @{}
    foreach ($record in $AuthenticatedListeners) {
        $port = [int]$record.port
        $ownerPid = [int]$record.pid
        $leaderPid = [int]$record.leader_pid
        if (
            $port -lt 1 -or $port -gt 65535 -or
            $ownerPid -le 0 -or $leaderPid -le 0 -or
            [string]::IsNullOrWhiteSpace([string]$record.process_id) -or
            [string]::IsNullOrWhiteSpace([string]$record.job_object_id)
        ) {
            throw "Authenticated child-listener record is malformed."
        }
        $key = "$port/$ownerPid"
        if ($result.ContainsKey($key)) {
            throw "Duplicate authenticated child-listener record: $key"
        }
        $result[$key] = [pscustomobject]@{
            Port = $port
            OwnerPid = $ownerPid
            LeaderPid = $leaderPid
            ProcessId = [string]$record.process_id
            JobObjectId = [string]$record.job_object_id
        }
    }
    return $result
}

function Assert-PortPool {
    param(
        [Parameter(Mandatory = $true)][int]$HttpPort,
        [Parameter(Mandatory = $true)][int]$ChildStart,
        [Parameter(Mandatory = $true)][int]$ChildEnd,
        [hashtable]$AllowedChildOwners = @{}
    )

    if ($ChildStart -gt $ChildEnd -or ($HttpPort -ge $ChildStart -and $HttpPort -le $ChildEnd)) {
        throw "Invalid staging HTTP/child port relationship."
    }
    if (-not (Get-Command Get-NetTCPConnection -ErrorAction SilentlyContinue)) {
        throw "Get-NetTCPConnection is required for staging port validation."
    }
    $listeners = @(Get-NetTCPConnection -State Listen -ErrorAction Stop | Where-Object {
        $_.LocalPort -eq $HttpPort -or
        ($_.LocalPort -ge $ChildStart -and $_.LocalPort -le $ChildEnd)
    })
    $conflicts = @()
    foreach ($listener in $listeners) {
        $port = [int]$listener.LocalPort
        $pidValue = [int]$listener.OwningProcess
        if ($port -eq $HttpPort) {
            $conflicts += "HTTP $port/PID $pidValue"
            continue
        }
        $key = "$port/$pidValue"
        if (-not $AllowedChildOwners.ContainsKey($key)) {
            $conflicts += "child $port/PID $pidValue"
        }
    }
    if ($conflicts.Count -gt 0) {
        throw "Unknown staging port listeners detected: $($conflicts -join ', ')"
    }
}

function Get-ExpectedStagingEnvironment {
    return [ordered]@{
        NGINX_QA_HTTP_HOST = "127.0.0.1"
        NGINX_QA_HTTP_PORT = "18025"
        NGINX_QA_SERVICE_ROOT = "D:/nginx-qa-staging/universal-managed-sprint-engine"
        NGINX_QA_PROTECTED_ROOTS = '["D:/nginx-qa","D:/nginx-qa-umse","D:/Prompt"]'
        NGINX_QA_STAGING_STATE_BASE = "C:/nginx-qa-staging-state/umse-007"
        NGINX_QA_STAGING_VENV_ROOT = "C:/nginx-qa-staging-state/umse-007/.venv"
        NGINX_QA_STAGING_BASE_PYTHON = "C:/Python312/python.exe"
        NGINX_QA_RUNTIME_ROOT = "C:/nginx-qa-staging-state/umse-007/runtime_state"
        NGINX_QA_PROMPT_ROOT = "C:/nginx-qa-staging-state/umse-007/prompt"
        NGINX_QA_MANAGED_ROOT = "C:/nginx-qa-staging-state/umse-007/managed"
        NGINX_QA_GIT_FETCH_TIMEOUT_SECONDS = "120"
        NGINX_QA_CHILD_PORT_RANGE = "18100-18199"
        NGINX_QA_INSTANCE_ID = "universal-managed-sprint-engine-staging"
        NGINX_QA_DISABLE_TELEGRAM = "1"
        NGINX_QA_DISABLE_TUNNEL = "1"
    }
}

function Get-RequiredEnvironmentValue {
    param([Parameter(Mandatory = $true)][string]$Name)

    $value = [System.Environment]::GetEnvironmentVariable($Name, "Process")
    if ([string]::IsNullOrWhiteSpace($value)) {
        throw "Missing required staging environment variable: $Name"
    }
    return $value
}

function Assert-ExactValue {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][string]$Expected
    )

    $actual = Get-RequiredEnvironmentValue $Name
    if ($actual -cne $Expected) {
        throw "$Name must be exactly '$Expected'; got '$actual'."
    }
}

function Assert-ExactStagingEnvironment {
    param(
        [Parameter(Mandatory = $true)]
        [System.Collections.IDictionary]$ExpectedValues
    )

    foreach ($entry in $ExpectedValues.GetEnumerator()) {
        Assert-ExactValue -Name $entry.Key -Expected $entry.Value
    }
}
# END TESTABLE FUNCTIONS

$serviceRoot = Resolve-NormalizedPath $PSScriptRoot
$expectedServiceRoot = "D:\nginx-qa-staging\universal-managed-sprint-engine"
$expectedBranch = "agent/umse-07-staging-qualification"
$expectedOrigin = "https://github.com/chartjs333/nginx-qa.git"
$environmentPath = Join-Path $serviceRoot ".env.staging"
$ExpectedCommit = $ExpectedCommit.ToLowerInvariant()

if ($serviceRoot -ine $expectedServiceRoot) {
    throw "Staging checkout must be exactly $expectedServiceRoot; got $serviceRoot."
}
$approvedCommit = Assert-StandaloneGitCheckout `
    -ServiceRoot $serviceRoot `
    -ExpectedOrigin $expectedOrigin `
    -ExpectedBranch $expectedBranch `
    -ExpectedCommit $ExpectedCommit

function Import-StagingEnvironment {
    if (-not (Test-Path -LiteralPath $environmentPath -PathType Leaf)) {
        throw "Missing $environmentPath. Copy .env.staging.example to .env.staging first."
    }
    $null = Assert-CanonicalNonReparsePath `
        -Path $environmentPath -PathKind Leaf -Label "staging environment file"

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

Import-StagingEnvironment

$expectedValues = Get-ExpectedStagingEnvironment
Assert-ExactStagingEnvironment -ExpectedValues $expectedValues

try {
    $protectedDecoded = (Get-RequiredEnvironmentValue "NGINX_QA_PROTECTED_ROOTS") | ConvertFrom-Json
}
catch {
    throw "NGINX_QA_PROTECTED_ROOTS must be a JSON array."
}
$protectedRoots = @($protectedDecoded)
if ($protectedRoots.Count -eq 0) {
    throw "NGINX_QA_PROTECTED_ROOTS must not be empty."
}
$normalizedProtectedRoots = @()
foreach ($root in $protectedRoots) {
    if ($root -isnot [string]) {
        throw "Every protected root must be a string."
    }
    $normalizedProtectedRoots += Assert-CanonicalNonReparsePath `
        -Path $root -AllowMissing -PathKind Container -Label "protected root"
}

$stateBase = Assert-CanonicalNonReparsePath `
    -Path (Get-RequiredEnvironmentValue "NGINX_QA_STAGING_STATE_BASE") `
    -PathKind Container -AllowMissing -Label "staging state base"
$venvRoot = Assert-CanonicalNonReparsePath `
    -Path (Get-RequiredEnvironmentValue "NGINX_QA_STAGING_VENV_ROOT") `
    -PathKind Container -AllowMissing -Label "staging virtual environment"
$basePythonPath = Get-RequiredEnvironmentValue "NGINX_QA_STAGING_BASE_PYTHON"
$pythonPath = Join-Path $venvRoot "Scripts\python.exe"
$runtimeRoot = Assert-CanonicalNonReparsePath `
    -Path (Get-RequiredEnvironmentValue "NGINX_QA_RUNTIME_ROOT") `
    -PathKind Container -AllowMissing -Label "managed runtime root"
$promptRoot = Assert-CanonicalNonReparsePath `
    -Path (Get-RequiredEnvironmentValue "NGINX_QA_PROMPT_ROOT") `
    -PathKind Container -AllowMissing -Label "managed prompt root"
$managedRoot = Assert-CanonicalNonReparsePath `
    -Path (Get-RequiredEnvironmentValue "NGINX_QA_MANAGED_ROOT") `
    -PathKind Container -AllowMissing -Label "managed workspace root"
$legacyRuntimeRoot = Assert-CanonicalNonReparsePath `
    -Path (Join-Path $serviceRoot "runtime_state") `
    -PathKind Container -AllowMissing -Label "legacy staging runtime root"

foreach ($root in @($venvRoot, $runtimeRoot, $promptRoot, $managedRoot)) {
    $parent = Resolve-NormalizedPath (Split-Path -Parent $root)
    if ($parent -ine $stateBase) {
        throw "Every mutable staging root must be an immediate child of $stateBase; got $root."
    }
}
$isolatedRoots = @($serviceRoot, $venvRoot, $runtimeRoot, $promptRoot, $managedRoot)
for ($index = 0; $index -lt $isolatedRoots.Count; $index++) {
    foreach ($protectedRoot in $normalizedProtectedRoots) {
        if (Test-PathOverlap -First $isolatedRoots[$index] -Second $protectedRoot) {
            throw "Staging path overlaps a protected root: $($isolatedRoots[$index])"
        }
    }
    for ($other = $index + 1; $other -lt $isolatedRoots.Count; $other++) {
        if (Test-PathOverlap -First $isolatedRoots[$index] -Second $isolatedRoots[$other]) {
            throw "Staging roots overlap: $($isolatedRoots[$index]) and $($isolatedRoots[$other])"
        }
    }
}
foreach ($protectedRoot in $normalizedProtectedRoots) {
    if (Test-PathOverlap -First $stateBase -Second $protectedRoot) {
        throw "Staging state base overlaps a protected root: $protectedRoot"
    }
}
if (Test-PathOverlap -First $stateBase -Second $serviceRoot) {
    throw "Staging state base must not overlap the service checkout."
}

$baseProbe = Assert-TrustedBaseInterpreter `
    -PythonPath $basePythonPath `
    -ProtectedRoots $normalizedProtectedRoots `
    -ForbiddenRoots @($serviceRoot, $stateBase)

$validationScript = @'
from nginx_qa.managed_import import normalize_managed_runtime_config
from nginx_qa.sprint_types import managed_runtime_config_invariant_issues
from nginx_qa.workspace_manager import ManagedWorkspaceManager
from pathlib import Path
import os
import sys

config = normalize_managed_runtime_config()
issues = managed_runtime_config_invariant_issues(config)
if issues:
    raise SystemExit("invalid staging runtime config: " + ",".join(issues))
state_base = os.environ["NGINX_QA_STAGING_STATE_BASE"]
venv_root = os.environ["NGINX_QA_STAGING_VENV_ROOT"]
forbidden = tuple(config["protected_roots"]) + (
    config["service_root"],
    str(Path(sys.prefix).resolve(strict=False)),
    str(Path(sys.base_prefix).resolve(strict=False)),
)
for value in (
    state_base,
    venv_root,
    config["runtime_root"],
    config["prompt_root"],
    config["managed_root"],
):
    ManagedWorkspaceManager.validate_isolated_root(
        value, protected_roots=forbidden
    )
'@
Push-Location $serviceRoot
try {
    $validationScript | & $baseProbe.executable -E -s -B -
    if ($LASTEXITCODE -ne 0) {
        throw "Managed staging configuration validation failed."
    }
}
finally {
    Pop-Location
}

$ownershipMarkerPath = Join-Path $stateBase ".nginx-qa-staging-owner.json"
$ownershipRecord = [ordered]@{
    instance_id = Get-RequiredEnvironmentValue "NGINX_QA_INSTANCE_ID"
    approved_commit = $approvedCommit
    service_root = $serviceRoot
    origin = $expectedOrigin
    branch = $expectedBranch
    state_base = $stateBase
    venv_root = $venvRoot
    base_python = $baseProbe.executable
    base_python_prefix = $baseProbe.base_prefix
    base_python_version = $baseProbe.version
    base_python_sha256 = $baseProbe.sha256
    runtime_root = $runtimeRoot
    prompt_root = $promptRoot
    managed_root = $managedRoot
    legacy_runtime_root = $legacyRuntimeRoot
    http_host = Get-RequiredEnvironmentValue "NGINX_QA_HTTP_HOST"
    http_port = [int](Get-RequiredEnvironmentValue "NGINX_QA_HTTP_PORT")
    child_port_range = Get-RequiredEnvironmentValue "NGINX_QA_CHILD_PORT_RANGE"
    protected_roots = @($normalizedProtectedRoots)
}
$ownershipState = Get-OwnershipState `
    -StateBase $stateBase `
    -MarkerPath $ownershipMarkerPath `
    -ExpectedRecord $ownershipRecord

$legacyMutablePaths = @(
    $legacyRuntimeRoot,
    (Join-Path $serviceRoot "conversation_log.jsonl"),
    (Join-Path $serviceRoot "conversation_log.jsonl.lock"),
    (Join-Path $serviceRoot "agents.json"),
    (Join-Path $serviceRoot "agents.json.lock"),
    (Join-Path $serviceRoot "specializations.json"),
    (Join-Path $serviceRoot "email_routes.json"),
    (Join-Path $serviceRoot "port_git_map.json"),
    (Join-Path $serviceRoot "port_git_map.json.lock"),
    (Join-Path $serviceRoot "project_sprints.json"),
    (Join-Path $serviceRoot "project_sprints.json.lock"),
    (Join-Path $serviceRoot "pending_project_sprints.json"),
    (Join-Path $serviceRoot "pending_project_sprints.json.lock"),
    (Join-Path $serviceRoot "attachments"),
    (Join-Path $serviceRoot "screenshot_folders"),
    (Join-Path $serviceRoot "evidence_folders")
)
$legacyMutablePaths += @(
    Get-ChildItem -LiteralPath $serviceRoot -Force -ErrorAction SilentlyContinue |
        Where-Object {
            $_.Name -like "queue_backup_before_restart_*.json" -or
            $_.Name -like ".port_git_map.json.*.tmp" -or
            $_.Name -like ".pending_project_sprints.json.*.tmp"
        } |
        Select-Object -ExpandProperty FullName
)
Assert-LegacyStateOwnership `
    -Paths $legacyMutablePaths `
    -OwnershipEstablished ($ownershipState.Mode -eq "Restart")

$rangeParts = (Get-RequiredEnvironmentValue "NGINX_QA_CHILD_PORT_RANGE").Split("-")
$httpPort = [int](Get-RequiredEnvironmentValue "NGINX_QA_HTTP_PORT")
$childStart = [int]$rangeParts[0]
$childEnd = [int]$rangeParts[1]

function Get-OwnedChildListenerMap {
    param([Parameter(Mandatory = $true)][string]$AuditPython)

    $auditScript = @'
import json
import hashlib
import os
import re
import sqlite3
import sys
from pathlib import Path

from nginx_qa.process_supervisor import _WindowsJob, port_owner_pids, process_identity

runtime_root = Path(sys.argv[1])
instance_id = sys.argv[2]
host = sys.argv[3]
port_start = int(sys.argv[4])
port_end = int(sys.argv[5])
database = runtime_root / "leases" / "managed-import.sqlite3"
if not database.is_file():
    print("[]")
    raise SystemExit(0)
uri = database.resolve(strict=True).as_uri() + "?mode=ro"
connection = sqlite3.connect(uri, uri=True, timeout=5)
try:
    rows = connection.execute(
        """
        SELECT l.port, l.lease_id, l.process_id, l.status,
               o.pid, o.state, o.process_json
        FROM managed_port_leases AS l
        JOIN managed_process_owners AS o ON o.process_id = l.process_id
        WHERE l.instance_id = ? AND l.host = ?
          AND l.port BETWEEN ? AND ?
          AND l.status IN ('reserved', 'bound')
          AND o.state IN ('PREPARED', 'STARTING', 'HEALTHY')
        """,
        (instance_id, host, port_start, port_end),
    ).fetchall()
finally:
    connection.close()

owned = []
for port, lease_id, process_id, _lease_status, owner_pid, _state, process_json in rows:
    if not isinstance(process_id, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", process_id):
        continue
    receipt_path = runtime_root / "pids" / (process_id + ".json")
    try:
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        process = json.loads(process_json)
        pid = int(receipt["pid"])
    except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError):
        continue
    if (
        receipt.get("process_id") != process_id
        or receipt.get("port_lease_id") != lease_id
        or receipt.get("phase") not in {"launch_gated", "launched"}
        or (owner_pid is not None and int(owner_pid) != pid)
        or process.get("executable_path") != receipt.get("executable_path")
        or process.get("cwd") != receipt.get("cwd")
        or process.get("launch_nonce") != receipt.get("launch_nonce")
        or process.get("assignment_id") != receipt.get("assignment_id")
        or process.get("port_lease_id") != lease_id
        or process.get("port_lease_id") != receipt.get("port_lease_id")
    ):
        continue
    launch_nonce = process.get("launch_nonce")
    if not isinstance(launch_nonce, str) or not launch_nonce:
        continue
    expected_job = "Global\\nginx-qa-managed-" + hashlib.sha256(
        (process_id + "\0" + launch_nonce).encode("utf-8")
    ).hexdigest()[:40]
    if (
        os.name != "nt"
        or process.get("job_object_id") != expected_job
        or receipt.get("job_object_id") != expected_job
        or receipt.get("process_group_id") != expected_job
    ):
        continue
    identity = process_identity(pid)
    if identity is None:
        continue
    if (
        identity.birth_token != receipt.get("os_process_birth_token")
        or os.path.normcase(os.path.realpath(identity.executable_path))
        != os.path.normcase(os.path.realpath(receipt["executable_path"]))
        or (
            identity.cwd is not None
            and os.path.normcase(os.path.realpath(identity.cwd))
            != os.path.normcase(os.path.realpath(receipt["cwd"]))
        )
    ):
        continue
    job = _WindowsJob.open(expected_job)
    if job is None:
        continue
    try:
        if not job.contains_exact(identity):
            continue
        listener_pids = port_owner_pids(host, int(port))
        if not listener_pids or not all(job.contains(listener_pid) for listener_pid in listener_pids):
            continue
        for listener_pid in sorted(listener_pids):
            owned.append(
                {
                    "port": int(port),
                    "pid": int(listener_pid),
                    "leader_pid": pid,
                    "process_id": process_id,
                    "job_object_id": expected_job,
                }
            )
    finally:
        job.close()
print(json.dumps(owned, sort_keys=True))
'@
    Push-Location $serviceRoot
    try {
        $raw = @(
            $auditScript | & $AuditPython -E -s -B - `
                $runtimeRoot `
                (Get-RequiredEnvironmentValue "NGINX_QA_INSTANCE_ID") `
                (Get-RequiredEnvironmentValue "NGINX_QA_HTTP_HOST") `
                $childStart `
                $childEnd
        )
        if ($LASTEXITCODE -ne 0 -or $raw.Count -ne 1) {
            throw "Unable to authenticate durable staging child listeners."
        }
        $decoded = @($raw[0] | ConvertFrom-Json)
        return ConvertTo-AllowedChildOwnerMap -AuthenticatedListeners $decoded
    }
    finally {
        Pop-Location
    }
}

if ($ownershipState.Mode -eq "FirstUse") {
    Assert-PortPool `
        -HttpPort $httpPort `
        -ChildStart $childStart `
        -ChildEnd $childEnd
    if ($Action -eq "Check") {
        Write-Output "Staging isolation check passed; state base is empty and ready for Setup."
        exit 0
    }
    if ($Action -ne "Setup") {
        throw "First use requires -Action Setup before Start."
    }
    $ownershipState = Initialize-OwnershipMarker `
        -StateBase $stateBase `
        -MarkerPath $ownershipMarkerPath `
        -ExpectedRecord $ownershipRecord
}

if ($ownershipState.Mode -ne "Restart") {
    throw "Staging ownership was not established."
}

if ($Action -eq "Setup") {
    if (Test-Path -LiteralPath $venvRoot) {
        if (-not (Test-Path -LiteralPath $pythonPath -PathType Leaf)) {
            throw "Owned staging virtual environment is incomplete; use a new empty state base."
        }
    }
    else {
        & $baseProbe.executable -I -B -m venv $venvRoot
        if ($LASTEXITCODE -ne 0) {
            throw "Unable to create isolated staging virtual environment."
        }
    }
}

if (-not (Test-Path -LiteralPath $pythonPath -PathType Leaf)) {
    throw "Missing owned staging Python at $pythonPath. Run with -Action Setup first."
}
$null = Assert-StagingVirtualEnvironment `
    -VenvRoot $venvRoot `
    -PythonPath $pythonPath `
    -ExpectedBasePrefix $baseProbe.base_prefix

$allowedOwners = Get-OwnedChildListenerMap -AuditPython $pythonPath
Assert-PortPool `
    -HttpPort $httpPort `
    -ChildStart $childStart `
    -ChildEnd $childEnd `
    -AllowedChildOwners $allowedOwners

if ($Action -eq "Setup") {
    & $pythonPath -I -B -m pip install --upgrade pip
    if ($LASTEXITCODE -ne 0) {
        throw "Unable to upgrade pip in the staging virtual environment."
    }
    & $pythonPath -I -B -m pip install `
        -r (Join-Path $serviceRoot "requirements.txt") `
        -r (Join-Path $serviceRoot "requirements-dev.txt")
    if ($LASTEXITCODE -ne 0) {
        throw "Unable to install staging dependencies."
    }
    Write-Output "Staging setup passed with exact ownership and interpreter provenance."
    exit 0
}

if ($Action -eq "Check") {
    Write-Output "Staging isolation check passed for an exactly owned restart."
    exit 0
}

foreach ($path in @($runtimeRoot, $promptRoot, $managedRoot)) {
    if (-not (Test-Path -LiteralPath $path)) {
        New-Item -ItemType Directory -Path $path | Out-Null
    }
    $null = Assert-CanonicalNonReparsePath `
        -Path $path -PathKind Container -Label "owned mutable staging root"
}

$promptSettingsPath = Join-Path $legacyRuntimeRoot "sequential-prompt-settings.json"
$expectedPromptSettings = [ordered]@{
    directory_template = Join-Path $promptRoot "{repository}"
    agent_latest_file_template = Join-Path $promptRoot "{repository}_{agent_phone}-latest.prompt"
}
if (-not (Test-Path -LiteralPath $legacyRuntimeRoot)) {
    New-Item -ItemType Directory -Path $legacyRuntimeRoot | Out-Null
}
$null = Assert-CanonicalNonReparsePath `
    -Path $legacyRuntimeRoot -PathKind Container -Label "owned legacy staging runtime root"
if (Test-Path -LiteralPath $promptSettingsPath -PathType Leaf) {
    $null = Assert-CanonicalNonReparsePath `
        -Path $promptSettingsPath -PathKind Leaf -Label "staging prompt settings"
    $actualPromptSettings = Get-Content -LiteralPath $promptSettingsPath -Raw | ConvertFrom-Json
    if (
        $actualPromptSettings.directory_template -cne $expectedPromptSettings.directory_template -or
        $actualPromptSettings.agent_latest_file_template -cne $expectedPromptSettings.agent_latest_file_template
    ) {
        throw "Existing staging prompt settings do not use the isolated prompt root."
    }
}
else {
    $temporaryPromptSettings = Join-Path $legacyRuntimeRoot `
        (".sequential-prompt-settings." + [guid]::NewGuid().ToString("N") + ".tmp")
    try {
        [System.IO.File]::WriteAllText(
            $temporaryPromptSettings,
            ($expectedPromptSettings | ConvertTo-Json),
            [System.Text.UTF8Encoding]::new($false)
        )
        [System.IO.File]::Move($temporaryPromptSettings, $promptSettingsPath)
    }
    finally {
        if (Test-Path -LiteralPath $temporaryPromptSettings) {
            Remove-Item -LiteralPath $temporaryPromptSettings -Force
        }
    }
}

foreach ($name in @(
    "TELEGRAM_BOT_TOKEN",
    "TELEGRAM_WEBHOOK_URL",
    "TELEGRAM_WEBHOOK_AUTO_REGISTER",
    "CLOUDFLARED_PUBLIC_URL",
    "CLOUDFLARED_QUICK_TUNNEL",
    "CLOUDFLARED_QUICK_TUNNEL_ORIGIN"
)) {
    Remove-Item "Env:$name" -ErrorAction SilentlyContinue
}

Push-Location $serviceRoot
try {
    & $pythonPath -E -s -B -m uvicorn main:app `
        --host (Get-RequiredEnvironmentValue "NGINX_QA_HTTP_HOST") `
        --port (Get-RequiredEnvironmentValue "NGINX_QA_HTTP_PORT")
    exit $LASTEXITCODE
}
finally {
    Pop-Location
}
