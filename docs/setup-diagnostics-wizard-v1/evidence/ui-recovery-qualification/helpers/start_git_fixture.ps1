# Start/status only. Never terminates a process, writes a repository, or exports
# another directory. The daemon is the accepted loopback-only docs test fixture.
$ErrorActionPreference='Stop'
$taskRoot='C:\nqt35\client\sdw-ui-recovery-20261104\qualification-final'
$receipt=Join-Path $taskRoot 'git-fixture-launch.json'
$git='C:\Program Files\Git\cmd\git.exe'
$fixture='C:\nqt37\qualification\git\setup-selftest.git'
$daemonArgs='--reuseaddr --base-path=C:/nqt37/qualification/git --export-all --enable=receive-pack --listen=127.0.0.1 --port=9418 C:/nqt37/qualification/git'
function Assert-OwnedGitFixture {
    if (Test-Path -LiteralPath $receipt) {$launch=Get-Content -LiteralPath $receipt -Raw|ConvertFrom-Json}
    else {$launch=Get-Content -LiteralPath 'C:\nqt35\client\sdw-continuation-20261104\r4-helper-launch.json' -Raw|ConvertFrom-Json}
    $before=@(Get-CimInstance Win32_Process)
    $listeners=@(Get-NetTCPConnection -State Listen -LocalPort 9418 -ErrorAction Stop)
    if ($listeners.Count -ne 1 -or $listeners[0].LocalAddress -ne '127.0.0.1') {throw 'Git fixture listener differs.'}
    $leaf=$before|Where-Object ProcessId -eq $listeners[0].OwningProcess
    $middle=$before|Where-Object ProcessId -eq $leaf.ParentProcessId
    $root=$before|Where-Object ProcessId -eq $middle.ParentProcessId
    if (-not $root -or $root.ProcessId -ne $launch.git_pid) {throw 'Git fixture root receipt differs.'}
    $expected=@(
        @($root,$git,('"'+$git+'" daemon '+$daemonArgs)),
        @($middle,'C:\Program Files\Git\mingw64\bin\git.exe',('git.exe daemon '+$daemonArgs)),
        @($leaf,'C:\Program Files\Git\mingw64\libexec\git-core\git-daemon.exe',('git-daemon '+$daemonArgs))
    )
    $held=@()
    try {
        foreach ($item in $expected) {
            $row=$item[0]
            if ($row.ExecutablePath -ine $item[1] -or $row.CommandLine.Trim() -cne $item[2]) {throw 'Git fixture executable or full arguments differ.'}
            $process=[System.Diagnostics.Process]::GetProcessById($row.ProcessId)
            $null=$process.Handle
            $held+=$process
            if ($process.HasExited -or [Math]::Abs(($process.StartTime.ToUniversalTime()-$row.CreationDate.ToUniversalTime()).Ticks) -ge 10) {throw 'Git fixture birth differs.'}
        }
        $birth=[DateTimeOffset]::Parse($launch.git_created_utc).UtcDateTime
        if ([Math]::Abs(($held[0].StartTime.ToUniversalTime()-$birth).Ticks) -ge 10) {throw 'Git fixture root PID was reused.'}
        if ($root.CreationDate -gt $middle.CreationDate -or $middle.CreationDate -gt $leaf.CreationDate) {throw 'Git fixture ancestry birth differs.'}
        $after=@(Get-CimInstance Win32_Process)
        foreach ($item in $expected) {
            $row=$item[0];$now=$after|Where-Object ProcessId -eq $row.ProcessId
            if (-not $now -or $now.CommandLine -cne $row.CommandLine -or $now.ParentProcessId -ne $row.ParentProcessId -or $now.CreationDate -ne $row.CreationDate) {throw 'Git fixture changed during proof.'}
        }
        [ordered]@{status='PASS';loopback_only=$true;port=9418;root_pid=$root.ProcessId;root_created_utc=$birth.ToString('o');listener_pid=$leaf.ProcessId;held_process_birth_and_full_argv_proved=$true;fixture=$fixture}|ConvertTo-Json|Set-Content -LiteralPath (Join-Path $taskRoot 'git-fixture-owner-proof.json') -Encoding utf8
    } finally {foreach ($process in $held) {$process.Dispose()}}
}
if (@(Get-NetTCPConnection -State Listen -LocalPort 9418 -ErrorAction SilentlyContinue).Count) {
    Assert-OwnedGitFixture
    Write-Output 'Owned Git fixture is already available on loopback port 9418.'
    exit 0
}
if (-not (Test-Path -LiteralPath (Join-Path $fixture 'HEAD') -PathType Leaf)) {throw 'Accepted Git fixture is missing; do not create a substitute.'}
$stamp=Get-Date -Format 'yyyyMMdd-HHmmss-fff'
$p=Start-Process -FilePath $git -ArgumentList ('daemon '+$daemonArgs) -WorkingDirectory 'C:\nqt37\qualification\git' -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $taskRoot "git-$stamp.stdout.log") -RedirectStandardError (Join-Path $taskRoot "git-$stamp.stderr.log")
[ordered]@{kind='owned_git_fixture_launch';git_pid=$p.Id;git_created_utc=$p.StartTime.ToUniversalTime().ToString('o')}|ConvertTo-Json|Set-Content -LiteralPath $receipt -Encoding utf8
for ($attempt=0; $attempt -lt 20; $attempt++) {
    if (@(Get-NetTCPConnection -State Listen -LocalPort 9418 -ErrorAction SilentlyContinue).Count) {break}
    if ($p.HasExited) {throw 'Git fixture exited; inspect the protected log.'}
    Start-Sleep -Milliseconds 500
}
Assert-OwnedGitFixture
Write-Output 'Owned Git fixture is ready on loopback port 9418.'
