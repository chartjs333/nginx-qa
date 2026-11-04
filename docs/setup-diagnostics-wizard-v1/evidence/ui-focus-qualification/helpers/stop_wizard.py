"""Signal only the proven wizard console, retaining native process identities.

No raw-PID termination, unknown console, active native job, or other host is
accepted. Historical mode is only for the exact old deployment transition.
"""
import argparse
import ctypes
from ctypes import wintypes
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import socket
import sqlite3
import time

ROOT = Path(__file__).resolve().parent


def require(value, message):
    if not value:
        raise RuntimeError(message)


def quiescent():
    db = sqlite3.connect('file:C:/nqt37/r/leases/managed-import.sqlite3?mode=ro', uri=True)
    db.execute('PRAGMA query_only=ON')
    try:
        rows = list(db.execute('SELECT sprint_id,status,state_json FROM managed_sprints'))
        for _, status, state in rows:
            require(status in ('completed', 'stopped', 'failed') and not json.loads(state).get('active_assignment_ids'), 'Native work is not quiescent')
    finally:
        db.close()
    raw = Path('C:/nqt37/wsl-client-r4/client-receipts.json').read_bytes()
    sessions = json.loads(raw)['sessions']
    require(all(s['phase'] in ('STOPPED', 'TERMINAL') for s in sessions.values()), 'Client work is not terminal')
    return {'native_sprints': [{'id': sid, 'status': status, 'sha256': hashlib.sha256(state.encode()).hexdigest()} for sid, status, state in rows],
            'client_journal_sha256': hashlib.sha256(raw).hexdigest()}


def ticks(value):
    instant = datetime.fromisoformat(value).astimezone(timezone.utc)
    delta = instant - datetime(1601, 1, 1, tzinfo=timezone.utc)
    return ((delta.days * 86400 + delta.seconds) * 1000000 + delta.microseconds) * 10


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--historical-owner', action='store_true')
    parser.add_argument('--stop', action='store_true')
    args = parser.parse_args()
    path = (Path('C:/nqt35/client/sdw-corrective-20261104') if args.historical_owner else ROOT) / 'authorize_wizard_browser.py'
    spec = importlib.util.spec_from_file_location('recipient_proof', path)
    proof = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(proof)
    proof.verify_static_owner()
    marker = json.loads(proof.OWNER_PATH.read_bytes())
    require(marker['initialization_id'] == '024a41a1-ecba-47f2-9df2-b1d6d3773425' and marker['ownership']['approved_commit'] == proof.COMMIT, 'Owned deployment identity differs')
    root_pid = proof.read_launcher_pid()
    before = quiescent()
    with socket.create_connection((proof.HOST, proof.PORT), timeout=10) as connected:
        snapshot = proof.collect_ownership()
        listener_pid = proof.verify_snapshot(snapshot, root_pid, connected.getsockname()[1])
    processes = proof.process_index(snapshot['before'])
    chain = []
    pid = listener_pid
    while True:
        chain.append(processes[pid])
        if pid == root_pid:
            break
        pid = processes[pid]['parent_pid']
    k = ctypes.WinDLL('kernel32', use_last_error=True)
    k.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    k.OpenProcess.restype = wintypes.HANDLE
    k.GetProcessTimes.argtypes = [wintypes.HANDLE] + [ctypes.POINTER(wintypes.FILETIME)] * 4
    k.GetProcessTimes.restype = wintypes.BOOL
    k.QueryFullProcessImageNameW.argtypes = [wintypes.HANDLE, wintypes.DWORD, wintypes.LPWSTR, ctypes.POINTER(wintypes.DWORD)]
    k.QueryFullProcessImageNameW.restype = wintypes.BOOL
    k.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
    k.WaitForSingleObject.restype = wintypes.DWORD
    k.CloseHandle.argtypes = [wintypes.HANDLE]
    k.CloseHandle.restype = wintypes.BOOL
    k.AttachConsole.argtypes = [wintypes.DWORD]
    k.GetConsoleProcessList.argtypes = [ctypes.POINTER(wintypes.DWORD), wintypes.DWORD]
    held = []
    attached = False
    try:
        for process in chain:
            handle = k.OpenProcess(0x1000 | 0x100000, False, process['pid'])
            require(handle, 'Cannot hold process identity')
            held.append((handle, process))
            times = [wintypes.FILETIME() for _ in range(4)]
            require(k.GetProcessTimes(handle, *(ctypes.byref(t) for t in times)), 'Cannot verify process birth')
            actual = (times[0].dwHighDateTime << 32) | times[0].dwLowDateTime
            require(abs(actual - ticks(process['created'])) < 10, 'Held process birth differs')
            buffer = ctypes.create_unicode_buffer(32768)
            length = wintypes.DWORD(len(buffer))
            require(k.QueryFullProcessImageNameW(handle, 0, buffer, ctypes.byref(length)), 'Cannot verify held image')
            require(Path(buffer.value).resolve() == Path(process['executable']).resolve(), 'Held image differs')
            require(k.WaitForSingleObject(handle, 0) == 258, 'Process exited during proof')
        require(quiescent() == before, 'Durable state changed during proof')
        require(proof.read_launcher_pid() == root_pid, 'Launcher receipt changed')
        record = {'kind': 'wizard_console_stop_with_held_native_identities', 'at': datetime.now(timezone.utc).isoformat(),
                  'source_commit': proof.COMMIT, 'ownership': chain, 'before': before,
                  'native_handle_birth_and_image_verified': True, 'signal_sent': False}
        if args.stop:
            k.FreeConsole()
            require(k.AttachConsole(root_pid), 'Cannot attach owned host console')
            attached = True
            allowed = {p['pid'] for _, p in held} | {os.getpid()}
            buffer = (wintypes.DWORD * 512)()
            count = k.GetConsoleProcessList(buffer, len(buffer))
            actual = set(buffer[:count])
            require(0 < count <= len(buffer) and not actual - allowed and root_pid in actual and listener_pid in actual, 'Console contains unproved owners')
            require(all(k.WaitForSingleObject(h, 0) == 258 for h, _ in held), 'Held owner exited before signal')
            require(k.SetConsoleCtrlHandler(None, True), 'Cannot protect helper handler')
            require(k.GenerateConsoleCtrlEvent(0, 0), 'Owned console signal failed')
            record['signal_sent'] = True
            record['console_pids'] = sorted(actual)
            time.sleep(0.3)
            k.FreeConsole()
            attached = False
            for handle, _ in held:
                require(k.WaitForSingleObject(handle, 30000) == 0, 'Owned process did not exit after graceful signal')
            require(quiescent() == before, 'Durable native/client state changed')
            record['all_owned_processes_exited'] = True
        stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')
        (ROOT / ('host-stop-' + stamp + '.json')).write_text(json.dumps(record, indent=2) + '\n', encoding='utf-8')
        print(json.dumps({'status': 'PASS', 'signal_sent': args.stop, 'source_commit': proof.COMMIT, 'owned_pids': [p['pid'] for _, p in held]}))
    finally:
        if attached:
            k.FreeConsole()
        for handle, _ in held:
            k.CloseHandle(handle)


if __name__ == '__main__':
    main()
