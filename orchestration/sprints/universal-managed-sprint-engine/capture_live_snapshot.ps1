[CmdletBinding()]
param(
    [string]$OutputPath,

    [string]$LiveRoot = 'D:\nginx-qa'
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$ExpectedLiveRoot = [System.IO.Path]::GetFullPath('D:\nginx-qa').TrimEnd([char]92)
$ExpectedStateBase = [System.IO.Path]::GetFullPath(
    'C:\nginx-qa-staging-state\umse-007'
).TrimEnd([char]92)
$ExpectedEvidenceRoot = [System.IO.Path]::GetFullPath(
    'C:\nginx-qa-staging-state\umse-007\evidence'
).TrimEnd([char]92)
$LivePort = 8025
$DurableScope = 'nginx-qa:live-durable-state:v1'
$DurableAlgorithm = (
    'sha256:utf8:ordinal-posix-path-nul-size-nul-mtime-utc-ticks-lf:v1'
)
$RequiredTopLevelState = @(
    'agents.json',
    'agents.json.lock',
    'conversation_log.jsonl',
    'conversation_log.jsonl.lock',
    'email_routes.json',
    'group_templates.json',
    'pending_project_sprints.json',
    'pending_project_sprints.json.lock',
    'port_git_map.json',
    'port_git_map.json.lock',
    'project_sprints.json',
    'project_sprints.json.lock',
    'specializations.json'
)
$OptionalTopLevelState = @('group_templates.json.lock')

$ResolvedLiveRoot = (Resolve-Path -LiteralPath $LiveRoot).Path.TrimEnd([char]92)
if (-not [string]::Equals(
    $ResolvedLiveRoot,
    $ExpectedLiveRoot,
    [System.StringComparison]::OrdinalIgnoreCase
)) {
    throw "LiveRoot must be the exact protected checkout: $ExpectedLiveRoot"
}
$LiveRootItem = Get-Item -LiteralPath $ResolvedLiveRoot -Force
if (
    -not $LiveRootItem.PSIsContainer -or
    ($LiveRootItem.Attributes -band [System.IO.FileAttributes]::ReparsePoint)
) {
    throw 'The protected live checkout must be a real directory.'
}

$ResolvedOutput = $null
if ($PSBoundParameters.ContainsKey('OutputPath')) {
    if (
        [string]::IsNullOrWhiteSpace($OutputPath) -or
        -not [System.IO.Path]::IsPathFullyQualified($OutputPath)
    ) {
        throw 'OutputPath must be an absolute JSON path.'
    }
    $ResolvedOutput = [System.IO.Path]::GetFullPath($OutputPath)
    if (-not $ResolvedOutput.EndsWith(
        '.json',
        [System.StringComparison]::OrdinalIgnoreCase
    )) {
        throw 'OutputPath must name a JSON file.'
    }
    $OutputParent = [System.IO.Path]::GetDirectoryName($ResolvedOutput)
    if (-not [string]::Equals(
        $OutputParent.TrimEnd([char]92),
        $ExpectedEvidenceRoot,
        [System.StringComparison]::OrdinalIgnoreCase
    )) {
        throw "OutputPath must be directly inside: $ExpectedEvidenceRoot"
    }
    if (Test-Path -LiteralPath $ResolvedOutput) {
        throw "Refusing to replace an existing snapshot: $ResolvedOutput"
    }
    if (-not (Test-Path -LiteralPath $ExpectedStateBase -PathType Container)) {
        throw 'The exact staging state base is missing; run Setup first.'
    }
    $StateBaseItem = Get-Item -LiteralPath $ExpectedStateBase -Force
    if ($StateBaseItem.Attributes -band [System.IO.FileAttributes]::ReparsePoint) {
        throw 'The exact staging state base must not be a reparse point.'
    }
    $OwnershipMarker = Join-Path $ExpectedStateBase '.nginx-qa-staging-owner.json'
    if (-not (Test-Path -LiteralPath $OwnershipMarker -PathType Leaf)) {
        throw 'The exact staging ownership marker is missing; run Setup first.'
    }
    $MarkerItem = Get-Item -LiteralPath $OwnershipMarker -Force
    if ($MarkerItem.Attributes -band [System.IO.FileAttributes]::ReparsePoint) {
        throw 'The staging ownership marker must not be a reparse point.'
    }
    if (-not (Test-Path -LiteralPath $ExpectedEvidenceRoot -PathType Container)) {
        New-Item -ItemType Directory -Path $ExpectedEvidenceRoot | Out-Null
    }
    $EvidenceRootItem = Get-Item -LiteralPath $ExpectedEvidenceRoot -Force
    if (
        -not $EvidenceRootItem.PSIsContainer -or
        ($EvidenceRootItem.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -or
        -not [string]::Equals(
            $EvidenceRootItem.FullName.TrimEnd([char]92),
            $ExpectedEvidenceRoot,
            [System.StringComparison]::OrdinalIgnoreCase
        )
    ) {
        throw 'The owned evidence root is not an exact real directory.'
    }
}

function Get-LiveListenerIdentity {
    $listeners = @(
        Get-NetTCPConnection -State Listen -LocalPort $LivePort -ErrorAction Stop
    )
    if (
        $listeners.Count -ne 1 -or
        $listeners[0].LocalAddress -cne '0.0.0.0' -or
        [int]$listeners[0].LocalPort -ne $LivePort
    ) {
        throw "Expected exactly one listener bound as 0.0.0.0:$LivePort."
    }
    $owner = Get-Process -Id ([int]$listeners[0].OwningProcess) -ErrorAction Stop
    return [ordered]@{
        host       = '0.0.0.0'
        port       = $LivePort
        pid        = [int]$owner.Id
        started_at = $owner.StartTime.ToUniversalTime().ToString(
            'o',
            [System.Globalization.CultureInfo]::InvariantCulture
        )
    }
}

function Invoke-ReadOnlyGit {
    param([Parameter(Mandatory = $true)][string[]]$Arguments)

    $output = @(
        & git --no-optional-locks -C $ResolvedLiveRoot @Arguments 2>&1
    )
    if ($LASTEXITCODE -ne 0) {
        throw "Live Git inspection failed: git $($Arguments -join ' ')"
    }
    return (([string[]]$output -join "`n").Trim())
}

function Get-LiveGitIdentity {
    $priorOptionalLocks = [System.Environment]::GetEnvironmentVariable(
        'GIT_OPTIONAL_LOCKS',
        'Process'
    )
    try {
        [System.Environment]::SetEnvironmentVariable(
            'GIT_OPTIONAL_LOCKS',
            '0',
            'Process'
        )
        $top = Invoke-ReadOnlyGit -Arguments @('rev-parse', '--show-toplevel')
        $head = (
            Invoke-ReadOnlyGit -Arguments @('rev-parse', '--verify', 'HEAD')
        ).ToLowerInvariant()
        $branch = Invoke-ReadOnlyGit -Arguments @('branch', '--show-current')
        $status = Invoke-ReadOnlyGit -Arguments @(
            'status',
            '--porcelain=v1',
            '--untracked-files=all'
        )
    }
    finally {
        [System.Environment]::SetEnvironmentVariable(
            'GIT_OPTIONAL_LOCKS',
            $priorOptionalLocks,
            'Process'
        )
    }

    if (-not [string]::Equals(
        [System.IO.Path]::GetFullPath($top).TrimEnd([char]92),
        $ExpectedLiveRoot,
        [System.StringComparison]::OrdinalIgnoreCase
    )) {
        throw 'Live Git top-level differs from the protected checkout.'
    }
    if (
        $head -notmatch '^[0-9a-f]{40}([0-9a-f]{24})?$' -or
        [string]::IsNullOrWhiteSpace($branch)
    ) {
        throw 'Live Git identity is invalid.'
    }
    if (-not [string]::IsNullOrEmpty($status)) {
        throw 'The protected live Git worktree is not clean.'
    }
    return [ordered]@{
        head   = $head
        branch = $branch
        status = 'clean'
    }
}

function Get-LiveFileIdentity {
    $paths = [System.Collections.Generic.List[string]]::new()
    foreach ($name in $RequiredTopLevelState) {
        $path = Join-Path $ResolvedLiveRoot $name
        if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
            throw "Required live state file is missing: $name"
        }
        $paths.Add($path)
    }
    foreach ($name in $OptionalTopLevelState) {
        $path = Join-Path $ResolvedLiveRoot $name
        if (Test-Path -LiteralPath $path -PathType Leaf) {
            $paths.Add($path)
        }
    }

    $runtimeRoot = Join-Path $ResolvedLiveRoot 'runtime_state'
    $runtimeRootItem = Get-Item -LiteralPath $runtimeRoot -Force -ErrorAction Stop
    if (
        -not $runtimeRootItem.PSIsContainer -or
        ($runtimeRootItem.Attributes -band [System.IO.FileAttributes]::ReparsePoint)
    ) {
        throw 'runtime_state must be a real directory.'
    }
    foreach ($entry in (
        Get-ChildItem -LiteralPath $runtimeRoot -Force -Recurse -ErrorAction Stop
    )) {
        if ($entry.Attributes -band [System.IO.FileAttributes]::ReparsePoint) {
            throw "Reparse point found in runtime_state: $($entry.FullName)"
        }
        if (-not $entry.PSIsContainer -and $entry.Extension -ine '.log') {
            $paths.Add($entry.FullName)
        }
    }

    $seen = [System.Collections.Generic.HashSet[string]]::new(
        [System.StringComparer]::OrdinalIgnoreCase
    )
    $records = [System.Collections.Generic.List[string]]::new()
    [long]$totalBytes = 0
    [long]$newestTicks = 0
    foreach ($path in $paths) {
        $file = Get-Item -LiteralPath $path -Force -ErrorAction Stop
        $file.Refresh()
        if (
            -not $file.Exists -or
            $file.PSIsContainer -or
            ($file.Attributes -band [System.IO.FileAttributes]::ReparsePoint)
        ) {
            throw "Live state file is not an exact regular file: $path"
        }
        $relative = [System.IO.Path]::GetRelativePath(
            $ResolvedLiveRoot,
            $file.FullName
        ).Replace([char]92, [char]47)
        if (-not $seen.Add($relative)) {
            throw "Duplicate live state path: $relative"
        }

        [long]$length = $file.Length
        [long]$ticks = $file.LastWriteTimeUtc.Ticks
        $records.Add(
            $relative + [char]0 +
            $length.ToString([System.Globalization.CultureInfo]::InvariantCulture) +
            [char]0 +
            $ticks.ToString([System.Globalization.CultureInfo]::InvariantCulture)
        )
        $totalBytes += $length
        if ($ticks -gt $newestTicks) {
            $newestTicks = $ticks
        }
    }
    if ($records.Count -eq 0) {
        throw 'The durable live state scope unexpectedly contains no files.'
    }

    $orderedRecords = $records.ToArray()
    [System.Array]::Sort(
        $orderedRecords,
        [System.StringComparer]::Ordinal
    )
    $canonicalMaterial = [string]::Join("`n", $orderedRecords) + "`n"
    $hasher = [System.Security.Cryptography.SHA256]::Create()
    try {
        $digest = [System.Convert]::ToHexString(
            $hasher.ComputeHash(
                [System.Text.Encoding]::UTF8.GetBytes($canonicalMaterial)
            )
        ).ToLowerInvariant()
    }
    finally {
        $hasher.Dispose()
    }

    return [pscustomobject]@{
        Public = [ordered]@{
            root             = $ExpectedLiveRoot.Replace([char]92, [char]47)
            scope            = $DurableScope
            algorithm        = $DurableAlgorithm
            count            = [int]$orderedRecords.Length
            total_bytes      = $totalBytes
            metadata_sha256  = $digest
            newest_write_utc = (
                [datetime]::new($newestTicks, [System.DateTimeKind]::Utc)
            ).ToString(
                'o',
                [System.Globalization.CultureInfo]::InvariantCulture
            )
        }
        Canonical = $canonicalMaterial
    }
}

$listenerBefore = Get-LiveListenerIdentity
$gitBefore = Get-LiveGitIdentity
$filesBefore = Get-LiveFileIdentity
$filesAfter = Get-LiveFileIdentity
$gitAfter = Get-LiveGitIdentity
$listenerAfter = Get-LiveListenerIdentity

if (($listenerBefore | ConvertTo-Json -Compress) -cne (
    $listenerAfter | ConvertTo-Json -Compress
)) {
    throw 'Live listener identity changed while the snapshot was captured.'
}
if (($gitBefore | ConvertTo-Json -Compress) -cne (
    $gitAfter | ConvertTo-Json -Compress
)) {
    throw 'Live Git identity changed while the snapshot was captured.'
}
if ($filesBefore.Canonical -cne $filesAfter.Canonical) {
    throw 'Live durable-state metadata changed while the snapshot was captured.'
}

$snapshot = [ordered]@{
    schema_version = 1
    captured_at    = [datetime]::UtcNow.ToString(
        'o',
        [System.Globalization.CultureInfo]::InvariantCulture
    )
    listener       = $listenerAfter
    files          = $filesAfter.Public
    git            = $gitAfter
}
$json = $snapshot | ConvertTo-Json -Depth 6 -Compress

if ($null -ne $ResolvedOutput) {
    $temporary = "$ResolvedOutput.tmp.$PID.$([guid]::NewGuid().ToString('N'))"
    try {
        [System.IO.File]::WriteAllText(
            $temporary,
            $json + [Environment]::NewLine,
            [System.Text.UTF8Encoding]::new($false)
        )
        Move-Item -LiteralPath $temporary -Destination $ResolvedOutput
    }
    finally {
        if (Test-Path -LiteralPath $temporary) {
            Remove-Item -LiteralPath $temporary -Force
        }
    }
}

Write-Output $json
