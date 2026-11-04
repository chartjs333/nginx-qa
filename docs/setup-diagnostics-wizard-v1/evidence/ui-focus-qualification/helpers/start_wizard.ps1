$ErrorActionPreference='Stop'
$taskRoot='C:\nqt35\client\sdw-ui-recovery-20261104\focus-qualification'
$launchScript='D:\nq-sdw-launcher\run_staging.ps1'
$commit='c9193d7d6cab67f0b191a86f5e0e268bce074f9e'
$branch='codex/sdw-qualified-ui-focus-recovery-20261104'
$python='C:\nqt37\v\Scripts\python.exe'
$pidPath=Join-Path $taskRoot 'host-launch-pid.txt'
$listener=@(Get-NetTCPConnection -State Listen -LocalPort 18038 -ErrorAction SilentlyContinue)
if ($listener.Count) {
    & $python -X utf8 -B (Join-Path $taskRoot 'stop_wizard.py')
    if ($LASTEXITCODE -ne 0) {throw 'Port 18038 has an unproved owner; nothing was changed.'}
    Write-Output 'Wizard is already running: http://127.0.0.1:18038/setup'
    exit 0
}
& 'C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe' -NoProfile -ExecutionPolicy Bypass -File $launchScript -Action Check -Profile setup-diagnostics-wizard-v1 -ExpectedCommit $commit -DeploymentBranch $branch
if ($LASTEXITCODE -ne 0) {throw 'Staging ownership check failed.'}
$stamp=Get-Date -Format 'yyyyMMdd-HHmmss-fff'
$process=Start-Process -FilePath 'C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe' -ArgumentList @('-NoProfile','-ExecutionPolicy','Bypass','-File',$launchScript,'-Action','Start','-Profile','setup-diagnostics-wizard-v1','-ExpectedCommit',$commit,'-DeploymentBranch',$branch) -WorkingDirectory 'D:\nq-sdw-launcher' -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $taskRoot "host-$stamp.stdout.log") -RedirectStandardError (Join-Path $taskRoot "host-$stamp.stderr.log")
$process.Id | Set-Content -LiteralPath $pidPath -Encoding ascii
[ordered]@{pid=$process.Id;created_utc=$process.StartTime.ToUniversalTime().ToString('o');source_commit=$commit;branch=$branch}|ConvertTo-Json|Set-Content -LiteralPath (Join-Path $taskRoot "host-launch-$stamp.json") -Encoding utf8
Write-Output 'Owned launcher started: http://127.0.0.1:18038/setup'
