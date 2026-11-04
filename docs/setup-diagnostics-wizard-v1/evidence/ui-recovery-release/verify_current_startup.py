"""Qualify the existing pinned startup command without restarting any service."""
import hashlib
import json
import sqlite3
import subprocess
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
HELPERS = ROOT.parent / 'focus-qualification'
SOURCE = Path('D:/nq-sdw-launcher')
PRODUCT = 'c9193d7d6cab67f0b191a86f5e0e268bce074f9e'
PYTHON = 'C:/nqt37/v/Scripts/python.exe'
PS = 'C:/Windows/System32/WindowsPowerShell/v1.0/powershell.exe'

def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()

def snapshot():
    script = r'''$ports = @(9418,18038,18039)
    $result = @()
    foreach ($port in $ports) {
      $listener = @(Get-NetTCPConnection -State Listen -LocalPort $port -ErrorAction Stop | Where-Object { $_.LocalAddress -eq '127.0.0.1' })
      if ($listener.Count -ne 1) { throw "Expected one loopback listener on $port" }
      $ownerId = [int]$listener[0].OwningProcess
      $chain = @()
      for ($i = 0; $i -lt 3 -and $ownerId -gt 0; $i++) {
        $process = Get-CimInstance Win32_Process -Filter "ProcessId=$ownerId"
        if ($null -eq $process) { break }
        $chain += [pscustomobject]@{pid=[int]$process.ProcessId; parent=[int]$process.ParentProcessId; born=$process.CreationDate.ToUniversalTime().ToString('o'); image=$process.ExecutablePath; command=$process.CommandLine}
        $ownerId = [int]$process.ParentProcessId
      }
      $result += [pscustomobject]@{port=$port; chain=$chain}
    }
    ConvertTo-Json -InputObject $result -Depth 6 -Compress'''
    processes = json.loads(subprocess.check_output([PS, '-NoProfile', '-NonInteractive', '-Command', script], text=True, encoding='utf-8'))
    database = sqlite3.connect('file:C:/nqt37/r/leases/managed-import.sqlite3?mode=ro', uri=True)
    try:
        database.execute('PRAGMA query_only=ON')
        native = [{'sprint_id': s, 'status': status, 'state_sha256': hashlib.sha256(raw.encode()).hexdigest()}
                  for s, status, raw in database.execute('SELECT sprint_id,status,state_json FROM managed_sprints ORDER BY sprint_id')]
    finally:
        database.close()
    return {'processes': processes, 'native': native,
            'client_sha256': sha('C:/nqt37/wsl-client-r4/client-receipts.json'),
            'environment_sha256': sha(SOURCE / '.env.staging'),
            'owner_sha256': sha('C:/nqt37/.nginx-qa-staging-owner.json')}

def run_logged(name, command):
    with (ROOT / name).open('wb') as output:
        completed = subprocess.run(command, stdout=output, stderr=subprocess.STDOUT, timeout=90, creationflags=subprocess.CREATE_NO_WINDOW)
    assert completed.returncode == 0, (name, completed.returncode)

assert subprocess.check_output(['git', '-C', str(SOURCE), 'rev-parse', 'HEAD'], text=True).strip() == PRODUCT
assert not subprocess.check_output(['git', '-C', str(SOURCE), 'status', '--porcelain', '--untracked-files=all'], text=True).strip()
run_logged('pre-host-proof.txt', [PYTHON, '-X', 'utf8', '-B', str(HELPERS / 'stop_wizard.py')])
run_logged('pre-client-proof.txt', [PYTHON, '-X', 'utf8', '-B', str(HELPERS / 'verify_stable_client.py')])
before = snapshot()
run_logged('startup-live.log', [PS, '-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass', '-File', str(HELPERS / 'start_all.ps1')])
after = snapshot()
assert before == after, 'Idempotent startup changed process identity, native/client history, environment or owner'
record = {'at': datetime.now(timezone.utc).isoformat(), 'status': 'PASS', 'kind': 'release_current_owned_idempotent_startup',
          'tested_product_commit': PRODUCT, 'helpers': str(HELPERS), 'startup_sha256': sha(HELPERS / 'start_all.ps1'),
          'before': before, 'after': after, 'process_identity_unchanged': True, 'native_history_unchanged': True,
          'new_job_or_model_run': False, 'restart_performed': False,
          'logs': {n: sha(ROOT / n) for n in ('pre-host-proof.txt','pre-client-proof.txt','startup-live.log')}}
(ROOT / 'startup-live-evidence.json').write_text(json.dumps(record, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
print(json.dumps({'status': 'PASS', 'unchanged_ports': [p['port'] for p in before['processes']], 'native_rows': len(before['native']), 'restart_performed': False}))
