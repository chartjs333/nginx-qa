"""Real idempotent startup of the owned accepted stack; no model job is launched."""
import hashlib
import http.client
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PS = r'C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe'
JOURNAL = Path('C:/nqt37/wsl-client-r4/client-receipts.json')

def owners():
    script = r'''$ErrorActionPreference='Stop'; $rows=@(); foreach($port in @(9418,18039,18038)) { $ls=@(Get-NetTCPConnection -State Listen -LocalPort $port); if($ls.Count -ne 1 -or $ls[0].LocalAddress -ne '127.0.0.1'){throw 'Unexpected listener'}; $p=Get-CimInstance Win32_Process -Filter ('ProcessId='+$ls[0].OwningProcess); $rows += [ordered]@{port=$port;pid=$p.ProcessId;created=$p.CreationDate.ToUniversalTime().ToString('o');parent=$p.ParentProcessId;image=$p.ExecutablePath} }; ConvertTo-Json -InputObject $rows'''
    result = subprocess.run([PS, '-NoProfile', '-NonInteractive', '-Command', script], check=True, capture_output=True, timeout=20)
    return json.loads(result.stdout.decode('utf-8-sig'))

before = owners()
before_digest = hashlib.sha256(JOURNAL.read_bytes()).hexdigest()
result = subprocess.run([PS, '-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass', '-File', str(ROOT/'start_all.ps1')], capture_output=True, timeout=90)
(ROOT/'startup-live.stdout.txt').write_bytes(result.stdout)
(ROOT/'startup-live.stderr.txt').write_bytes(result.stderr)
after = owners()
after_digest = hashlib.sha256(JOURNAL.read_bytes()).hexdigest()
assert result.returncode == 0, (result.returncode, result.stderr.decode(errors='replace'))
assert before == after and before_digest == after_digest
c = http.client.HTTPConnection('127.0.0.1', 18038, timeout=5)
try:
    c.request('GET', '/setup'); response=c.getresponse(); code=response.status; response.read()
finally:
    c.close()
assert code == 200
report = {'at': datetime.now(timezone.utc).isoformat(), 'status':'PASS', 'kind':'actual_already_running_owned_stack_startup',
          'returncode':result.returncode, 'listeners_before':before, 'listeners_after':after, 'same_processes':True,
          'client_journal_sha256_before':before_digest, 'client_journal_sha256_after':after_digest, 'no_new_native_client_attempt':True,
          'setup_http_status':code, 'new_process_start_path_exercised':False,
          'wrapper_sha256':hashlib.sha256((ROOT/'start_all.ps1').read_bytes()).hexdigest(),
          'stdout_sha256':hashlib.sha256(result.stdout).hexdigest(), 'stderr_sha256':hashlib.sha256(result.stderr).hexdigest(),
          'product_commit':'a1841e420a517dfdb9ed2c5a4b44eaeb6b3286ca',
          'limits':['After reboot startup order is concrete, but no machine reboot or fresh Git daemon launch was performed.',
                    'Existing qualified helpers require quiescence; use the running UI for active work.']}
(ROOT/'startup-live-evidence.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
print(json.dumps(report,ensure_ascii=False,indent=2))
