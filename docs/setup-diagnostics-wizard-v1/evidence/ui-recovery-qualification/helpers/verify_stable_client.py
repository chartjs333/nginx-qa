"""Crash only the proven idle test client by native handles, for restart qualification.

This is not a general process stopper. No console-wide signals or raw-PID kills
are used. The actual R4 native run and all client sessions must already be terminal.
"""
import argparse
import base64
import ctypes
from ctypes import wintypes
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess

ROOT = Path(__file__).resolve().parent
STATE = Path('C:/nqt37/wsl-client-r4/client-receipts.json')
HISTORY = ROOT
SCRIPT = str(HISTORY / 'serve_stable_client.py')
PS = r'C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe'
def require(value, message):
    if not value:
        raise RuntimeError(message)

def terminal_state():
    raw = STATE.read_bytes()
    sessions = json.loads(raw)['sessions']
    require(bool(sessions) and all(s['phase'] == 'TERMINAL' for s in sessions.values()), 'Client has nonterminal state')
    db = sqlite3.connect('file:C:/nqt37/r/leases/managed-import.sqlite3?mode=ro', uri=True)
    db.execute('PRAGMA query_only=ON')
    try:
        for status, state in db.execute('SELECT status,state_json FROM managed_sprints'):
            require(status in ('completed', 'stopped', 'blocked', 'failed') and not json.loads(state).get('active_assignment_ids'), 'Native work is not quiescent')
    finally:
        db.close()
    return hashlib.sha256(raw).hexdigest()

def ticks(value):
    instant = datetime.fromisoformat(value.replace('Z', '+00:00')).astimezone(timezone.utc)
    delta = instant - datetime(1601, 1, 1, tzinfo=timezone.utc)
    return ((delta.days * 86400 + delta.seconds) * 1000000 + delta.microseconds) * 10

def snapshot():
    command = r'''
$ErrorActionPreference='Stop'
[Console]::OutputEncoding=New-Object System.Text.UTF8Encoding($false)
$processes=@(Get-CimInstance Win32_Process | ForEach-Object {
 [ordered]@{pid=[long]$_.ProcessId;parent=[long]$_.ParentProcessId;image=$_.ExecutablePath;command=$_.CommandLine;created=$_.CreationDate.ToUniversalTime().ToString('o')}
})
$listeners=@(Get-NetTCPConnection -State Listen -LocalPort 18039 -ErrorAction SilentlyContinue | ForEach-Object {
 [ordered]@{pid=[long]$_.OwningProcess;address=$_.LocalAddress;port=[int]$_.LocalPort}
})
[ordered]@{processes=$processes;listeners=$listeners}|ConvertTo-Json -Depth 4 -Compress
'''
    result = subprocess.run([PS, '-NoProfile', '-NonInteractive', '-EncodedCommand',
        base64.b64encode(command.encode('utf-16le')).decode()], capture_output=True, timeout=20,
        creationflags=subprocess.CREATE_NO_WINDOW)
    require(result.returncode == 0 and len(result.stdout) < 4000000, 'Cannot prove process ownership')
    return json.loads(result.stdout.decode('utf-8-sig'))

parser = argparse.ArgumentParser()
parser.add_argument('--terminate-owned-idle-client', action='store_true')
args = parser.parse_args()
before_hash = terminal_state()
launch = json.loads((HISTORY / 'stable-client-launch.json').read_text(encoding='utf-8-sig'))
old = json.loads((HISTORY / 'stable-client-owners.json').read_text(encoding='utf-8-sig'))
old_child = old['owner']
data = snapshot()
processes = {p['pid']: p for p in data['processes']}
parent = processes.get(launch['client_pid'])
child = processes.get(old_child['ProcessId'])
require(parent and child and child['parent'] == parent['pid'], 'Recorded client ancestry changed')
require(data['listeners'] == [{'pid': child['pid'], 'address': '127.0.0.1', 'port': 18039}], 'Listener owner changed')
expected = [(child, 'C:/Python312/python.exe', old_child['CreationDate']),
            (parent, 'C:/nqt37/v/Scripts/python.exe', launch['client_created_utc'])]
k = ctypes.WinDLL('kernel32', use_last_error=True)
k.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]; k.OpenProcess.restype = wintypes.HANDLE
k.GetProcessTimes.argtypes = [wintypes.HANDLE] + [ctypes.POINTER(wintypes.FILETIME)] * 4
k.GetProcessTimes.restype = wintypes.BOOL
k.QueryFullProcessImageNameW.argtypes = [wintypes.HANDLE, wintypes.DWORD, wintypes.LPWSTR, ctypes.POINTER(wintypes.DWORD)]
k.QueryFullProcessImageNameW.restype = wintypes.BOOL
k.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]; k.WaitForSingleObject.restype = wintypes.DWORD
k.TerminateProcess.argtypes = [wintypes.HANDLE, wintypes.UINT]; k.TerminateProcess.restype = wintypes.BOOL
k.CloseHandle.argtypes = [wintypes.HANDLE]; k.CloseHandle.restype = wintypes.BOOL
held = []
try:
    for process, image, created in expected:
        require(Path(process['image']).resolve() == Path(image).resolve(), 'Process image changed')
        wanted = '-X utf8 -B -u ' + SCRIPT.replace('/', '\\')
        require(process['command'].strip(' \t') in ('"' + image.replace('/', '\\') + '" ' + wanted,
                                       image.replace('/', '\\') + ' ' + wanted), 'Process command changed')
        handle = k.OpenProcess(0x1000 | 0x100000 | 0x1, False, process['pid'])
        require(handle, 'Cannot hold process identity'); held.append((handle, process))
        times = [wintypes.FILETIME() for _ in range(4)]
        require(k.GetProcessTimes(handle, *(ctypes.byref(t) for t in times)), 'Cannot verify process birth')
        actual = (times[0].dwHighDateTime << 32) | times[0].dwLowDateTime
        require(abs(actual - ticks(created)) < 10 and abs(actual - ticks(process['created'])) < 10, 'PID birth differs')
        buffer = ctypes.create_unicode_buffer(32768); length = wintypes.DWORD(len(buffer))
        require(k.QueryFullProcessImageNameW(handle, 0, buffer, ctypes.byref(length)), 'Cannot verify held image')
        require(Path(buffer.value).resolve() == Path(image).resolve(), 'Held image differs')
        require(k.WaitForSingleObject(handle, 0) == 258, 'Process exited during proof')
    require(terminal_state() == before_hash, 'Client state changed during proof')
    record = {'kind': 'forced_idle_client_restart_qualification', 'state_sha256': before_hash,
              'owned_pids': [p['pid'] for _, p in held], 'source_commit': launch['client_source_commit'],
              'native_handle_and_birth_verified': True, 'terminated': False}
    if args.terminate_owned_idle_client:
        for handle, process in held:
            if k.WaitForSingleObject(handle, 0) != 0:
                require(k.TerminateProcess(handle, 0x41), 'Owned handle termination failed')
            require(k.WaitForSingleObject(handle, 10000) == 0, 'Owned exit not confirmed')
        require(terminal_state() == before_hash, 'Durable terminal state changed')
        record['terminated'] = True
    (ROOT / ('stable-client-stop.json' if record['terminated'] else 'stable-client-owner-proof.json')).write_text(json.dumps(record, indent=2) + '\n', encoding='utf-8')
    print(json.dumps(record))
finally:
    for handle, _ in held:
        k.CloseHandle(handle)
