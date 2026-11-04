$ErrorActionPreference='Stop'
$taskRoot='C:\nqt35\client\sdw-ui-recovery-20261104\qualification-final'
$python='C:\nqt37\v\Scripts\python.exe'
$listener=@(Get-NetTCPConnection -State Listen -LocalPort 18039 -ErrorAction SilentlyContinue)
if ($listener.Count) {
    & $python -X utf8 -B (Join-Path $taskRoot 'verify_stable_client.py')
    if ($LASTEXITCODE -ne 0) {throw 'Client owner or quiescence could not be proved. Nothing changed.'}
    Write-Output 'Owned client is already running on 127.0.0.1:18039.'
    exit 0
}
$stamp=Get-Date -Format 'yyyyMMdd-HHmmss-fff'
$p=Start-Process -FilePath $python -ArgumentList @('-X','utf8','-B','-u',(Join-Path $taskRoot 'serve_stable_client.py')) -WorkingDirectory 'D:\nq-sdw-launcher' -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $taskRoot "stable-client-$stamp.stdout.log") -RedirectStandardError (Join-Path $taskRoot "stable-client-$stamp.stderr.log")
[ordered]@{kind='stable_owned_client_launch';client_source_commit='fdafbf58db85555706b8ea04e11c1d4450b0f120';client_pid=$p.Id;client_created_utc=$p.StartTime.ToUniversalTime().ToString('o')}|ConvertTo-Json|Set-Content -LiteralPath (Join-Path $taskRoot 'stable-client-launch.json') -Encoding utf8
for ($attempt=0; $attempt -lt 20; $attempt++) {
    $listener=@(Get-NetTCPConnection -State Listen -LocalPort 18039 -ErrorAction SilentlyContinue)
    if ($listener.Count) {break}
    if ($p.HasExited) {throw 'Client exited; inspect the protected startup log.'}
    Start-Sleep -Milliseconds 500
}
if ($listener.Count -ne 1 -or $listener[0].LocalAddress -ne '127.0.0.1') {throw 'Expected client listener did not appear.'}
$owner=Get-CimInstance Win32_Process -Filter ('ProcessId='+$listener[0].OwningProcess)
$parent=Get-CimInstance Win32_Process -Filter ('ProcessId='+$p.Id)
if (-not $parent -or $owner.ParentProcessId -ne $parent.ProcessId) {throw 'Client ancestry differs; no process was stopped.'}
[ordered]@{listener=$listener[0]|Select-Object LocalAddress,LocalPort,OwningProcess;parent=$parent|Select-Object ProcessId,ParentProcessId,@{n='CreationDate';e={$_.CreationDate.ToUniversalTime().ToString('o')}},ExecutablePath,CommandLine;owner=$owner|Select-Object ProcessId,ParentProcessId,@{n='CreationDate';e={$_.CreationDate.ToUniversalTime().ToString('o')}},ExecutablePath,CommandLine}|ConvertTo-Json -Depth 5|Set-Content -LiteralPath (Join-Path $taskRoot 'stable-client-owners.json') -Encoding utf8
& $python -X utf8 -B (Join-Path $taskRoot 'verify_stable_client.py')
if ($LASTEXITCODE -ne 0) {throw 'Client identity could not be proved after startup.'}
Write-Output 'Owned client is ready on 127.0.0.1:18039.'
