# Starts only the exactly pinned, owned local installation; UI qualification may still report findings. It creates no job.
$ErrorActionPreference = 'Stop'

function Invoke-SetupStartupSteps {
    param([Parameter(Mandatory=$true)][string[]]$Steps)
    $powerShell = 'C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe'
    foreach ($step in $Steps) {
        if (-not (Test-Path -LiteralPath $step -PathType Leaf)) {
            throw "Required startup helper is missing: $step"
        }
    }
    foreach ($step in $Steps) {
        & $powerShell -NoProfile -NonInteractive -ExecutionPolicy Bypass -File $step
        $stepExitCode = $LASTEXITCODE
        if ($stepExitCode -ne 0) {
            throw "Startup stopped at $([IO.Path]::GetFileName($step)) (exit $stepExitCode). Later steps were not started."
        }
    }
}

function Confirm-SetupPage {
    for ($attempt = 0; $attempt -lt 20; $attempt++) {
        $response = $null
        try {
            $request = [Net.HttpWebRequest]::Create('http://127.0.0.1:18038/setup')
            $request.Proxy = $null
            $request.AllowAutoRedirect = $false
            $request.Timeout = 2000
            $response = $request.GetResponse()
            if ([int]$response.StatusCode -eq 200) { return }
        } catch {
            # This unauthenticated readiness GET sends no credential.
        } finally {
            if ($null -ne $response) { $response.Close() }
        }
        Start-Sleep -Milliseconds 500
    }
    throw 'Owned startup checks finished, but the setup page did not become ready. Inspect the protected startup logs.'
}

# Dot sourcing defines the functions for isolated failure-propagation tests only.
if ($MyInvocation.InvocationName -ne '.') {
    try {
        $qualified = 'C:\nqt35\client\sdw-ui-recovery-20261104\qualification-final'
        Invoke-SetupStartupSteps -Steps @(
            (Join-Path $qualified 'start_git_fixture.ps1'),
            (Join-Path $qualified 'start_client.ps1'),
            (Join-Path $qualified 'start_wizard.ps1')
        )
        Confirm-SetupPage
        # The first-start helper returns after spawning; prove the actual ready
        # listener as well. Without --stop this only checks ownership/quiescence.
        & 'C:\nqt37\v\Scripts\python.exe' -X utf8 -B (Join-Path $qualified 'stop_wizard.py')
        if ($LASTEXITCODE -ne 0) { throw 'Ready page owner could not be proved; no process was stopped.' }
        Write-Output 'Local wizard is ready: http://127.0.0.1:18038/setup'
        exit 0
    } catch {
        [Console]::Error.WriteLine($_.Exception.Message)
        exit 1
    }
}
