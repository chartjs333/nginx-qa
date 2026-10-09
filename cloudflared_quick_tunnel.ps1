param(
    [ValidateSet("Start", "Stop")]
    [string]$Action = "Start",
    [string]$Origin = "http://127.0.0.1:8025",
    [int]$StartupTimeoutSeconds = 45,
    [int]$DnsTimeoutSeconds = 120
)

$ErrorActionPreference = "Stop"

$runtimeDirectory = Join-Path $PSScriptRoot "runtime_state"
$statePath = Join-Path $runtimeDirectory "cloudflared-quick-tunnel.json"
$urlPath = Join-Path $runtimeDirectory "cloudflared-quick-tunnel.url"
$stdoutPath = Join-Path $runtimeDirectory "cloudflared-quick-tunnel.out.log"
$stderrPath = Join-Path $runtimeDirectory "cloudflared-quick-tunnel.err.log"

function Get-CloudflaredExecutable {
    $command = Get-Command "cloudflared.exe" -ErrorAction SilentlyContinue
    if ($command) {
        return $command.Source
    }

    $candidates = @()
    foreach ($programFilesRoot in @($env:ProgramFiles, ${env:ProgramFiles(x86)})) {
        if ($programFilesRoot) {
            $candidates += Join-Path $programFilesRoot "cloudflared\cloudflared.exe"
        }
    }
    if ($env:LOCALAPPDATA) {
        $candidates += Join-Path $env:LOCALAPPDATA "Microsoft\WinGet\Links\cloudflared.exe"
        $packageRoot = Join-Path $env:LOCALAPPDATA "Microsoft\WinGet\Packages"
        if (Test-Path -LiteralPath $packageRoot) {
            $candidates += Get-ChildItem -LiteralPath $packageRoot -Directory -ErrorAction SilentlyContinue |
                Where-Object { $_.Name -like "Cloudflare.cloudflared_*" } |
                ForEach-Object {
                    Get-ChildItem -LiteralPath $_.FullName -Filter "cloudflared.exe" -File -Recurse -ErrorAction SilentlyContinue |
                        Select-Object -ExpandProperty FullName
                }
        }
    }

    return $candidates |
        Where-Object { $_ -and (Test-Path -LiteralPath $_ -PathType Leaf) } |
        Select-Object -First 1
}

function Install-Cloudflared {
    $winget = Get-Command "winget.exe" -ErrorAction SilentlyContinue
    if (-not $winget) {
        throw "cloudflared is not installed and winget is unavailable. Install cloudflared from https://developers.cloudflare.com/tunnel/downloads/."
    }

    Write-Host "cloudflared is not installed. Installing Cloudflare.cloudflared with winget..."
    & $winget.Source install --id Cloudflare.cloudflared --exact --source winget --accept-package-agreements --accept-source-agreements --silent --disable-interactivity
    if ($LASTEXITCODE -ne 0) {
        throw "winget could not install Cloudflare.cloudflared (exit code $LASTEXITCODE)."
    }
}

function Stop-ManagedTunnel {
    if (-not (Test-Path -LiteralPath $statePath -PathType Leaf)) {
        Remove-Item -LiteralPath $urlPath -Force -ErrorAction SilentlyContinue
        return
    }

    try {
        $state = Get-Content -LiteralPath $statePath -Raw | ConvertFrom-Json
        $expectedExecutable = [System.IO.Path]::GetFullPath([string]$state.executable)
        $expectedOrigin = [string]$state.origin
        $processIds = if ($state.process_ids) { @($state.process_ids) } else { @($state.process_id) }
        $stoppedProcessIds = @()
        foreach ($managedProcessId in $processIds) {
            $processId = [int]$managedProcessId
            $process = Get-CimInstance Win32_Process -Filter "ProcessId = $processId" -ErrorAction SilentlyContinue
            if ($process -and $process.Name -ieq "cloudflared.exe") {
                $actualExecutable = if ($process.ExecutablePath) {
                    [System.IO.Path]::GetFullPath([string]$process.ExecutablePath)
                } else {
                    ""
                }
                $expectedCommand = "--url $expectedOrigin"
                if (
                    $actualExecutable -and
                    $actualExecutable -ieq $expectedExecutable -and
                    [string]$process.CommandLine -like "*$expectedCommand*"
                ) {
                    Stop-Process -Id $processId -Force -ErrorAction SilentlyContinue
                    $stoppedProcessIds += $processId
                }
            }
        }
        foreach ($stoppedProcessId in $stoppedProcessIds) {
            Wait-Process -Id $stoppedProcessId -Timeout 10 -ErrorAction SilentlyContinue
        }
    } finally {
        Remove-Item -LiteralPath $statePath -Force -ErrorAction SilentlyContinue
        Remove-Item -LiteralPath $urlPath -Force -ErrorAction SilentlyContinue
    }
}

if ($Action -eq "Stop") {
    Stop-ManagedTunnel
    exit 0
}

New-Item -ItemType Directory -Path $runtimeDirectory -Force | Out-Null
Stop-ManagedTunnel
Remove-Item -LiteralPath $stdoutPath -Force -ErrorAction SilentlyContinue
Remove-Item -LiteralPath $stderrPath -Force -ErrorAction SilentlyContinue

$cloudflared = Get-CloudflaredExecutable
if (-not $cloudflared) {
    Install-Cloudflared
    $cloudflared = Get-CloudflaredExecutable
}
if (-not $cloudflared) {
    throw "cloudflared was installed but cloudflared.exe could not be located. Open a new terminal and run run.bat again."
}

Write-Host "Starting Cloudflare Quick Tunnel for $Origin..."
$preexistingProcessIds = @(
    Get-CimInstance Win32_Process -Filter "Name = 'cloudflared.exe'" -ErrorAction SilentlyContinue |
        Select-Object -ExpandProperty ProcessId
)
$startParameters = @{
    FilePath = $cloudflared
    ArgumentList = @("tunnel", "--url", $Origin, "--no-autoupdate")
    WorkingDirectory = $PSScriptRoot
    WindowStyle = "Hidden"
    RedirectStandardOutput = $stdoutPath
    RedirectStandardError = $stderrPath
    PassThru = $true
}
$tunnelProcess = Start-Process @startParameters

$deadline = [DateTime]::UtcNow.AddSeconds($StartupTimeoutSeconds)
$publicUrl = $null
$urlPattern = 'https://[a-z0-9-]+\.trycloudflare\.com'

while ([DateTime]::UtcNow -lt $deadline) {
    Start-Sleep -Milliseconds 500
    $tunnelProcess.Refresh()
    $logText = ""
    if (Test-Path -LiteralPath $stdoutPath) {
        $logText += Get-Content -LiteralPath $stdoutPath -Raw -ErrorAction SilentlyContinue
    }
    if (Test-Path -LiteralPath $stderrPath) {
        $logText += Get-Content -LiteralPath $stderrPath -Raw -ErrorAction SilentlyContinue
    }
    $match = [regex]::Match($logText, $urlPattern, [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
    if ($match.Success) {
        $publicUrl = $match.Value.TrimEnd('/')
        break
    }
    if ($tunnelProcess.HasExited) {
        break
    }
}

if (-not $publicUrl) {
    $newTunnelProcesses = @(
        Get-CimInstance Win32_Process -Filter "Name = 'cloudflared.exe'" -ErrorAction SilentlyContinue |
            Where-Object {
                $_.ProcessId -notin $preexistingProcessIds -and
                    [string]$_.CommandLine -like "*--url $Origin*"
            }
    )
    foreach ($newTunnelProcess in $newTunnelProcesses) {
        Stop-Process -Id $newTunnelProcess.ProcessId -Force -ErrorAction SilentlyContinue
    }
    $details = ""
    if (Test-Path -LiteralPath $stderrPath) {
        $details = (Get-Content -LiteralPath $stderrPath -Tail 12 -ErrorAction SilentlyContinue) -join [Environment]::NewLine
    }
    throw "Cloudflare Quick Tunnel did not provide a public URL within $StartupTimeoutSeconds seconds.`n$details"
}

$managedProcessIds = @(
    Get-CimInstance Win32_Process -Filter "Name = 'cloudflared.exe'" -ErrorAction SilentlyContinue |
        Where-Object {
            $candidateExecutable = if ($_.ExecutablePath) {
                [System.IO.Path]::GetFullPath([string]$_.ExecutablePath)
            } else {
                ""
            }
            $candidateExecutable -ieq [System.IO.Path]::GetFullPath($cloudflared) -and
                $_.ProcessId -notin $preexistingProcessIds -and
                [string]$_.CommandLine -like "*--url $Origin*"
        } |
        Select-Object -ExpandProperty ProcessId
)
if (-not $managedProcessIds) {
    $managedProcessIds = @($tunnelProcess.Id)
}

$publicHost = ([uri]$publicUrl).DnsSafeHost
$dnsDeadline = [DateTime]::UtcNow.AddSeconds($DnsTimeoutSeconds)
$dnsReady = $false
Write-Host "Waiting for public DNS for $publicHost..."
while ([DateTime]::UtcNow -lt $dnsDeadline -and -not $dnsReady) {
    foreach ($dnsServer in @("1.1.1.1", "8.8.8.8")) {
        try {
            Resolve-DnsName -Name $publicHost -Type A -Server $dnsServer -DnsOnly -QuickTimeout -ErrorAction Stop |
                Out-Null
            $dnsReady = $true
            break
        } catch {
            # The quick-tunnel hostname can take several seconds to propagate.
        }
    }
    if (-not $dnsReady) {
        Start-Sleep -Seconds 2
    }
}
if (-not $dnsReady) {
    foreach ($managedProcessId in $managedProcessIds) {
        Stop-Process -Id $managedProcessId -Force -ErrorAction SilentlyContinue
    }
    throw "Quick Tunnel hostname $publicHost did not appear in public DNS within $DnsTimeoutSeconds seconds."
}
Write-Host "Public DNS is ready for $publicHost."

$state = [ordered]@{
    process_id = $managedProcessIds[0]
    process_ids = $managedProcessIds
    executable = [System.IO.Path]::GetFullPath($cloudflared)
    public_url = $publicUrl
    origin = $Origin
    started_at = [DateTime]::UtcNow.ToString("o")
}
$state | ConvertTo-Json | Set-Content -LiteralPath $statePath -Encoding UTF8
$publicUrl | Set-Content -LiteralPath $urlPath -Encoding Ascii
Write-Host "Cloudflare Quick Tunnel is ready: $publicUrl"
