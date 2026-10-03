[CmdletBinding()]
param(
    [string]$OutputPath,

    [string]$LiveRoot = 'D:\nginx-qa'
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
Add-Type -AssemblyName System.Net.Http

# BEGIN TESTABLE SNAPSHOT FUNCTIONS
function Resolve-SnapshotNormalizedPath {
    param([Parameter(Mandatory = $true)][string]$Path)

    if ([string]::IsNullOrWhiteSpace($Path)) {
        throw 'Path must not be empty.'
    }
    $raw = $Path.Replace('/', '\')
    if ($raw.StartsWith('\')) {
        throw "UNC and device paths are not allowed: $Path"
    }
    if ($raw -notmatch '^[A-Za-z]:\\') {
        throw "Path must be an absolute DOS path: $Path"
    }

    $relative = $raw.Substring(3)
    if ($relative) {
        $invalidCharacters = [System.IO.Path]::GetInvalidFileNameChars()
        foreach ($segment in $relative.Split([char]'\')) {
            if ([string]::IsNullOrEmpty($segment)) {
                throw "Path contains an empty component: $Path"
            }
            if ($segment -eq '.' -or $segment -eq '..') {
                throw "Path traversal components are not allowed: $Path"
            }
            if ($segment.EndsWith(' ') -or $segment.EndsWith('.')) {
                throw "Path components must not end in a space or dot: $Path"
            }
            if ($segment.IndexOfAny($invalidCharacters) -ge 0) {
                throw "Path contains an invalid or alternate-stream component: $Path"
            }
        }
    }

    $full = [System.IO.Path]::GetFullPath($raw)
    $root = [System.IO.Path]::GetPathRoot($full)
    if (-not $root -or $root -notmatch '^[A-Za-z]:[\\/]$') {
        throw "Path must be an absolute DOS path: $Path"
    }
    $normalized = $full.Replace('/', '\').TrimEnd('\')
    if ($normalized -match '^[A-Za-z]:$') {
        $normalized += '\'
    }
    return $normalized
}

function Assert-SnapshotCanonicalNonReparsePath {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [ValidateSet('Any', 'Container', 'Leaf')]
        [string]$PathKind = 'Any',
        [switch]$AllowMissing,
        [string]$Label = 'path'
    )

    $full = Resolve-SnapshotNormalizedPath $Path
    $root = [System.IO.Path]::GetPathRoot($full)
    $relative = $full.Substring($root.Length).Trim('\')
    $components = [System.Collections.Generic.List[string]]::new()
    $components.Add($root)
    $current = $root
    if ($relative) {
        foreach ($segment in $relative.Split([char]'\')) {
            $current = Join-Path $current $segment
            $components.Add($current)
        }
    }

    foreach ($candidate in $components) {
        $item = Get-Item -LiteralPath $candidate -Force -ErrorAction SilentlyContinue
        if ($null -eq $item) {
            continue
        }
        if ($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) {
            throw "$Label contains a filesystem reparse point: $candidate"
        }
        $itemPath = Resolve-SnapshotNormalizedPath $item.FullName
        $candidatePath = Resolve-SnapshotNormalizedPath $candidate
        if ($itemPath -ine $candidatePath) {
            throw "$Label contains a filesystem alias: $candidate -> $itemPath"
        }
    }

    $exists = Test-Path -LiteralPath $full
    if (-not $exists) {
        if (-not $AllowMissing) {
            throw "$Label does not exist: $full"
        }
        return $full
    }
    if (
        $PathKind -eq 'Container' -and
        -not (Test-Path -LiteralPath $full -PathType Container)
    ) {
        throw "$Label must be a directory: $full"
    }
    if (
        $PathKind -eq 'Leaf' -and
        -not (Test-Path -LiteralPath $full -PathType Leaf)
    ) {
        throw "$Label must be a regular file: $full"
    }
    return $full
}

function Assert-SafeSnapshotLeafName {
    param(
        [Parameter(Mandatory = $true)][string]$LeafName,
        [switch]$RequireJson
    )

    if (
        [string]::IsNullOrWhiteSpace($LeafName) -or
        $LeafName -in @('.', '..') -or
        $LeafName.EndsWith(' ') -or
        $LeafName.EndsWith('.') -or
        $LeafName.Contains(':') -or
        $LeafName.IndexOfAny([System.IO.Path]::GetInvalidFileNameChars()) -ge 0 -or
        $LeafName -notmatch '^[A-Za-z0-9][A-Za-z0-9._-]*$'
    ) {
        throw "Unsafe snapshot file name: $LeafName"
    }
    $deviceStem = $LeafName.Split([char]'.')[0]
    if ($deviceStem -match '^(?i:CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])$') {
        throw "Reserved device name is not allowed: $LeafName"
    }
    if (
        $RequireJson -and
        -not $LeafName.EndsWith(
            '.json',
            [System.StringComparison]::OrdinalIgnoreCase
        )
    ) {
        throw 'OutputPath must name a JSON file.'
    }
}

function Initialize-SnapshotOutputBoundary {
    param(
        [Parameter(Mandatory = $true)][string]$RequestedOutput,
        [Parameter(Mandatory = $true)][string]$StateBase,
        [Parameter(Mandatory = $true)][string]$EvidenceRoot
    )

    $expectedStateBase = Resolve-SnapshotNormalizedPath $StateBase
    $expectedEvidenceRoot = Resolve-SnapshotNormalizedPath $EvidenceRoot
    if (-not [string]::Equals(
        (Resolve-SnapshotNormalizedPath (
            [System.IO.Path]::GetDirectoryName($expectedEvidenceRoot)
        )),
        $expectedStateBase,
        [System.StringComparison]::OrdinalIgnoreCase
    )) {
        throw 'The evidence root must be a direct child of the owned state base.'
    }

    $resolvedOutput = Resolve-SnapshotNormalizedPath $RequestedOutput
    $outputLeaf = [System.IO.Path]::GetFileName($resolvedOutput)
    Assert-SafeSnapshotLeafName -LeafName $outputLeaf -RequireJson
    $outputParent = Resolve-SnapshotNormalizedPath (
        [System.IO.Path]::GetDirectoryName($resolvedOutput)
    )
    if (-not [string]::Equals(
        $outputParent,
        $expectedEvidenceRoot,
        [System.StringComparison]::OrdinalIgnoreCase
    )) {
        throw "OutputPath must be directly inside: $expectedEvidenceRoot"
    }

    $ownershipMarker = Join-Path $expectedStateBase '.nginx-qa-staging-owner.json'
    $null = Assert-SnapshotCanonicalNonReparsePath `
        -Path $expectedStateBase -PathKind Container `
        -Label 'exact staging state base'
    $null = Assert-SnapshotCanonicalNonReparsePath `
        -Path $ownershipMarker -PathKind Leaf `
        -Label 'staging ownership marker'
    $null = Assert-SnapshotCanonicalNonReparsePath `
        -Path $expectedEvidenceRoot -PathKind Container -AllowMissing `
        -Label 'owned evidence root'
    $null = Assert-SnapshotCanonicalNonReparsePath `
        -Path $resolvedOutput -PathKind Leaf -AllowMissing `
        -Label 'snapshot output path'
    if (Test-Path -LiteralPath $resolvedOutput) {
        throw "Refusing to replace an existing snapshot: $resolvedOutput"
    }

    if (-not (Test-Path -LiteralPath $expectedEvidenceRoot -PathType Container)) {
        # Re-prove every ancestor immediately before the first filesystem write.
        $null = Assert-SnapshotCanonicalNonReparsePath `
            -Path $expectedStateBase -PathKind Container `
            -Label 'exact staging state base'
        $null = Assert-SnapshotCanonicalNonReparsePath `
            -Path $ownershipMarker -PathKind Leaf `
            -Label 'staging ownership marker'
        $null = Assert-SnapshotCanonicalNonReparsePath `
            -Path $expectedEvidenceRoot -PathKind Container -AllowMissing `
            -Label 'owned evidence root'
        $null = Assert-SnapshotCanonicalNonReparsePath `
            -Path $resolvedOutput -PathKind Leaf -AllowMissing `
            -Label 'snapshot output path'
        New-Item -ItemType Directory -Path $expectedEvidenceRoot | Out-Null
    }

    $null = Assert-SnapshotCanonicalNonReparsePath `
        -Path $expectedEvidenceRoot -PathKind Container `
        -Label 'owned evidence root'
    $null = Assert-SnapshotCanonicalNonReparsePath `
        -Path $resolvedOutput -PathKind Leaf -AllowMissing `
        -Label 'snapshot output path'
    if (Test-Path -LiteralPath $resolvedOutput) {
        throw "Refusing to replace an existing snapshot: $resolvedOutput"
    }

    return [pscustomobject]@{
        StateBase      = $expectedStateBase
        EvidenceRoot   = $expectedEvidenceRoot
        OwnershipMarker = $ownershipMarker
        OutputPath     = $resolvedOutput
        OutputLeaf     = $outputLeaf
    }
}
# END TESTABLE SNAPSHOT FUNCTIONS

$ExpectedLiveRoot = Resolve-SnapshotNormalizedPath 'D:\nginx-qa'
$ExpectedStateBase = Resolve-SnapshotNormalizedPath (
    'C:\nginx-qa-staging-state\umse-007'
)
$ExpectedEvidenceRoot = Resolve-SnapshotNormalizedPath (
    'C:\nginx-qa-staging-state\umse-007\evidence'
)
$LivePort = 8025
$LiveHealthUri = 'http://127.0.0.1:8025/'

$ResolvedLiveRoot = Assert-SnapshotCanonicalNonReparsePath `
    -Path $LiveRoot -PathKind Container -Label 'protected live checkout'
if (-not [string]::Equals(
    $ResolvedLiveRoot,
    $ExpectedLiveRoot,
    [System.StringComparison]::OrdinalIgnoreCase
)) {
    throw "LiveRoot must be the exact protected checkout: $ExpectedLiveRoot"
}

$OutputBoundary = $null
if ($PSBoundParameters.ContainsKey('OutputPath')) {
    $OutputBoundary = Initialize-SnapshotOutputBoundary `
        -RequestedOutput $OutputPath `
        -StateBase $ExpectedStateBase `
        -EvidenceRoot $ExpectedEvidenceRoot
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
        $branch = Invoke-ReadOnlyGit -Arguments @(
            'symbolic-ref',
            '--quiet',
            '--short',
            'HEAD'
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
        (Resolve-SnapshotNormalizedPath $top),
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
    return [ordered]@{
        head   = $head
        branch = $branch
    }
}

function Get-LiveHealthIdentity {
    $handler = [System.Net.Http.HttpClientHandler]::new()
    $handler.AllowAutoRedirect = $false
    $handler.UseProxy = $false
    $client = [System.Net.Http.HttpClient]::new($handler)
    $client.Timeout = [timespan]::FromSeconds(5)
    $request = [System.Net.Http.HttpRequestMessage]::new(
        [System.Net.Http.HttpMethod]::Get,
        [uri]$LiveHealthUri
    )
    $response = $null
    try {
        $response = $client.SendAsync(
            $request,
            [System.Net.Http.HttpCompletionOption]::ResponseHeadersRead
        ).GetAwaiter().GetResult()
        $statusCode = [int]$response.StatusCode
        if ($statusCode -ne 200) {
            throw "Live health endpoint returned HTTP $statusCode."
        }
        $contentType = [string]$response.Content.Headers.ContentType.MediaType
        if ($contentType -cne 'text/html') {
            throw "Live health endpoint returned unexpected content type: $contentType"
        }
        return [ordered]@{
            endpoint     = $LiveHealthUri
            method       = 'GET'
            status_code  = $statusCode
            content_type = $contentType
        }
    }
    finally {
        if ($null -ne $response) {
            $response.Dispose()
        }
        $request.Dispose()
        $client.Dispose()
        $handler.Dispose()
    }
}

$listenerBefore = Get-LiveListenerIdentity
$gitBefore = Get-LiveGitIdentity
$health = Get-LiveHealthIdentity
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

$snapshot = [ordered]@{
    schema_version = 2
    captured_at    = [datetime]::UtcNow.ToString(
        'o',
        [System.Globalization.CultureInfo]::InvariantCulture
    )
    listener       = $listenerAfter
    health         = $health
    git            = $gitAfter
}
$json = $snapshot | ConvertTo-Json -Depth 6 -Compress

if ($null -ne $OutputBoundary) {
    $resolvedOutput = $OutputBoundary.OutputPath
    $temporaryLeaf = (
        $OutputBoundary.OutputLeaf +
        ".tmp.$PID.$([guid]::NewGuid().ToString('N'))"
    )
    Assert-SafeSnapshotLeafName -LeafName $temporaryLeaf
    $temporary = Join-Path $OutputBoundary.EvidenceRoot $temporaryLeaf
    try {
        # Re-prove the complete owned boundary immediately before writing.
        $null = Assert-SnapshotCanonicalNonReparsePath `
            -Path $OutputBoundary.StateBase -PathKind Container `
            -Label 'exact staging state base'
        $null = Assert-SnapshotCanonicalNonReparsePath `
            -Path $OutputBoundary.OwnershipMarker -PathKind Leaf `
            -Label 'staging ownership marker'
        $null = Assert-SnapshotCanonicalNonReparsePath `
            -Path $OutputBoundary.EvidenceRoot -PathKind Container `
            -Label 'owned evidence root'
        $null = Assert-SnapshotCanonicalNonReparsePath `
            -Path $resolvedOutput -PathKind Leaf -AllowMissing `
            -Label 'snapshot output path'
        $null = Assert-SnapshotCanonicalNonReparsePath `
            -Path $temporary -PathKind Leaf -AllowMissing `
            -Label 'snapshot temporary path'
        if (
            (Test-Path -LiteralPath $resolvedOutput) -or
            (Test-Path -LiteralPath $temporary)
        ) {
            throw 'Snapshot output path appeared before the atomic write.'
        }
        [System.IO.File]::WriteAllText(
            $temporary,
            $json + [Environment]::NewLine,
            [System.Text.UTF8Encoding]::new($false)
        )
        $null = Assert-SnapshotCanonicalNonReparsePath `
            -Path $temporary -PathKind Leaf `
            -Label 'snapshot temporary path'
        $null = Assert-SnapshotCanonicalNonReparsePath `
            -Path $resolvedOutput -PathKind Leaf -AllowMissing `
            -Label 'snapshot output path'
        if (Test-Path -LiteralPath $resolvedOutput) {
            throw "Refusing to replace an existing snapshot: $resolvedOutput"
        }
        Move-Item -LiteralPath $temporary -Destination $resolvedOutput
        $null = Assert-SnapshotCanonicalNonReparsePath `
            -Path $resolvedOutput -PathKind Leaf `
            -Label 'written snapshot output path'
    }
    finally {
        if (Test-Path -LiteralPath $temporary) {
            Remove-Item -LiteralPath $temporary -Force
        }
    }
}

Write-Output $json
