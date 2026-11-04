"""One exact, quiescent, backed-up old-to-new wizard ownership transition.

Run only after stop_wizard.py --historical-owner --stop. Secret-bearing backups
stay in the ACL-protected operator journal and must never be committed.
"""
import hashlib
import json
from pathlib import Path
import shutil
import socket
import sqlite3
import subprocess

ROOT = Path(__file__).resolve().parent
SOURCE = Path('D:/nq-sdw-launcher')
STATE = Path('C:/nqt37')
OLD = 'a1841e420a517dfdb9ed2c5a4b44eaeb6b3286ca'
NEW = 'fdafbf58db85555706b8ea04e11c1d4450b0f120'
BRANCH = 'codex/sdw-qualified-ui-recovery-20261104'


def git(*args):
    return subprocess.check_output(['git', '-C', str(SOURCE), *args]).decode().strip()


def main():
    from stop_wizard import quiescent
    marker_path = STATE / '.nginx-qa-staging-owner.json'
    env_path = SOURCE / '.env.staging'
    expected = json.loads((ROOT.parent / 'owned-host-upgrade-preflight.json').read_bytes())['owner']
    marker_raw = marker_path.read_bytes()
    marker = json.loads(marker_raw)
    assert marker == expected and marker['ownership']['approved_commit'] == OLD
    assert git('rev-parse', 'HEAD') == OLD
    assert not git('status', '--porcelain=v1', '--untracked-files=all')
    with socket.socket() as probe:
        assert probe.connect_ex(('127.0.0.1', 18038)) != 0, 'Old host still listens'
    before = quiescent()
    backup = ROOT / 'protected-pre-upgrade-backup'
    env_raw = env_path.read_bytes()
    if backup.exists():
        manifest = json.loads((ROOT / 'backup-manifest.json').read_bytes())
        assert manifest == {str(p.relative_to(backup)): hashlib.sha256(p.read_bytes()).hexdigest() for p in backup.rglob('*') if p.is_file()}
        assert (backup / 'owner.json').read_bytes() == marker_raw
        assert (backup / 'environment.SECRET').read_bytes() == env_raw
        assert (backup / 'client-receipts.json').read_bytes() == (STATE / 'wsl-client-r4/client-receipts.json').read_bytes()
    else:
        backup.mkdir(exist_ok=False)
        shutil.copy2(marker_path, backup / 'owner.json')
        shutil.copy2(env_path, backup / 'environment.SECRET')
        shutil.copy2(STATE / 'setup-policy.json', backup / 'setup-policy.json')
        shutil.copytree(STATE / 'setup', backup / 'setup')
        shutil.copy2(STATE / 'wsl-client-r4/client-receipts.json', backup / 'client-receipts.json')
        db = sqlite3.connect('file:C:/nqt37/r/leases/managed-import.sqlite3?mode=ro', uri=True)
        target = sqlite3.connect(backup / 'managed-import.sqlite3')
        try:
            db.backup(target)
            assert target.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
        finally:
            target.close()
            db.close()
        manifest = {str(p.relative_to(backup)): hashlib.sha256(p.read_bytes()).hexdigest() for p in backup.rglob('*') if p.is_file()}
        (ROOT / 'backup-manifest.json').write_text(json.dumps(manifest, indent=2) + '\n', encoding='utf-8')
    # The deployment clone can have a single-branch fetch configuration.
    refspec = '+refs/heads/' + BRANCH + ':refs/remotes/origin/' + BRANCH
    if refspec not in git('config', '--get-all', 'remote.origin.fetch').splitlines():
        git('remote', 'set-branches', '--add', 'origin', BRANCH)
    git('fetch', 'origin', BRANCH)
    assert git('rev-parse', 'origin/' + BRANCH) == NEW
    assert git('ls-remote', '--heads', 'origin', 'refs/heads/codex/sdw-qualified-host-continuation-20261104').split()[0] == OLD
    git('switch', '-c', BRANCH, '--track', 'origin/' + BRANCH)
    assert git('rev-parse', 'HEAD') == NEW
    assert marker_path.read_bytes() == marker_raw and env_path.read_bytes() == env_raw
    assert quiescent() == before
    updated = json.loads(marker_raw)
    updated['ownership']['approved_commit'] = NEW
    updated['ownership']['branch'] = BRANCH
    assert {k for k in marker['ownership'] if marker['ownership'][k] != updated['ownership'][k]} == {'approved_commit', 'branch'}
    staged = marker_path.with_suffix('.transition.tmp')
    staged.write_text(json.dumps(updated, indent=2) + '\n', encoding='utf-8')
    staged.replace(marker_path)
    values = {'NGINX_QA_SETUP_ADVISOR_PROVIDER': 'ollama', 'NGINX_QA_SETUP_ADVISOR_MODEL': 'qwen2.5:7b'}
    lines = env_raw.decode('utf-8-sig').splitlines()
    for key, value in values.items():
        positions = [i for i, line in enumerate(lines) if line.startswith(key + '=')]
        assert len(positions) <= 1
        if positions:
            lines[positions[0]] = key + '=' + value
        else:
            lines.append(key + '=' + value)
    env_path.write_text('\n'.join(lines) + '\n', encoding='utf-8')
    assert quiescent() == before
    report = {'status': 'PASS', 'kind': 'exact_owned_host_deployment_transition', 'old_commit': OLD, 'new_commit': NEW,
              'branch': BRANCH, 'rollback_remote_unchanged': True, 'owner_changed_fields': ['approved_commit', 'branch'],
              'initialization_id': updated['initialization_id'], 'protected_backup_manifest': 'backup-manifest.json',
              'native_and_client_state_unchanged': before, 'environment_written_keys': sorted(values)}
    (ROOT / 'deployment-transition.json').write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    print(json.dumps({'status': 'PASS', 'new_commit': NEW, 'initialization_id_preserved': True}))


if __name__ == '__main__':
    main()
