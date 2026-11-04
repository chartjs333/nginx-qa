"""Verify actual model artifacts, native decisions, publication, replay and retirement."""
import hashlib
import json
from pathlib import Path
import sqlite3
import subprocess
from datetime import datetime, timezone
from http_qualification import Client

output = Path(__file__).resolve().parent
root = Path('C:/nqt35/client/sdw-continuation-20261104')
start = json.loads((root / 'r4-native-start.json').read_bytes())['start']
sprint = start['receipt']['native_sprint_id']
operation = start['receipt']['native_operation_id']
db = sqlite3.connect('file:C:/nqt37/r/leases/managed-import.sqlite3?mode=ro', uri=True)
db.execute('PRAGMA query_only=ON')
state = json.loads(db.execute('SELECT state_json FROM managed_sprints WHERE project_id=? AND sprint_id=?', ('9010', sprint)).fetchone()[0])
slot = db.execute('SELECT holds_slot FROM telegram_launcher_operations WHERE operation_id=?', (operation,)).fetchone()[0]
db.close()
assert state['status'] == 'completed' and not state['active_assignment_ids']
authors = [a for a in state['assignments'] if a['node_id'] == 'document']
assert len(authors) == 1 and authors[0]['outcome'] == 'DONE'
author = authors[0]
commit = author['result_commit']
reviews = state['reviews']
assert len(reviews) == 2 and len({r['reviewer_id'] for r in reviews}) == 2
assert all(r['decision'] == 'APPROVE' and r['result_commit'] == commit for r in reviews)
assert slot == 0
assert all(x['status'] != 'active' and x['released_at'] for x in state['branch_leases'])
assert not state['port_leases'] and not state['processes']
client_root = Path('C:/nqt37/wsl-client-r4')
sessions = json.loads((client_root / 'client-receipts.json').read_bytes())['sessions']
matching = [(sid, s) for sid, s in sessions.items() if s['descriptor']['sprint_id'] == sprint]
assert len(matching) == 1
sid, session = matching[0]
assert session['phase'] == 'TERMINAL' and session['native_terminal_status'] == 'completed'
assert len(session['assignments']) == 3
model_runs = []
for aid, attempt in session['assignments'].items():
    assert attempt['phase'] == 'SUBMITTED'
    receipt = attempt['native_receipt']
    assert receipt['assignment_id'] == aid
    folder = client_root / sid / aid
    launch = json.loads((folder / 'wsl-attempt.json').read_bytes())
    result = json.loads((folder / 'wsl-result.json').read_bytes())
    assert result['job'] == launch['job'] == hashlib.sha256(attempt['job_name'].encode()).hexdigest()
    assert result['quiescent'] is True
    assert result['output']['status'] == attempt['payload']['status']
    if attempt['kind'] == 'review':
        assert launch['role'] == 'review' and launch['head'] == commit and not result['changed_paths']
        assert receipt['decision'] == 'APPROVE'
    else:
        assert launch['role'] == 'author' and result['changed_paths'] == ['docs/setup-selftest.md']
        assert receipt['result_commit'] == commit and receipt['outcome'] == 'DONE'
    model_runs.append({'assignment_id': aid, 'phone': attempt['phone'], 'role': launch['role'],
        'job': result['job'], 'native_receipt': receipt, 'model_output': result['output'],
        'bundle_sha256': launch['bundle_sha256'], 'result_sha256': hashlib.sha256((folder / 'wsl-result.json').read_bytes()).hexdigest()})
assert len({r['job'] for r in model_runs}) == 3
author_attempt = session['assignments'][author['assignment_id']]
branch = author_attempt['payload']['git_branch']
def git(*args):
    return subprocess.check_output(['git', '--git-dir=C:/nqt37/qualification/git/setup-selftest.git', *args])
assert git('rev-parse', 'refs/heads/' + branch).decode().strip() == commit
assert git('diff', '--name-only', author['initial_head_commit'], commit).decode().splitlines() == ['docs/setup-selftest.md']
document = git('show', commit + ':docs/setup-selftest.md').decode('utf-8')
assert all(heading in document for heading in ('Purpose', 'Execution', 'Review', 'Limitations'))
client = Client()
actor = client.pair()
receipt = client.req('/api/v1/projects/9000/setup/operations/' + start['receipt']['operation_id'])
assert receipt['state'] == 'SUCCEEDED'
replay = client.req(start['apply_path'], start['apply_body'])
assert replay['operation_id'] == receipt['operation_id'] and replay['state'] == 'SUCCEEDED'
new_session = Client()
assert new_session.pair() == actor
restored = new_session.req('/api/v1/projects/9000/setup/operations/' + receipt['operation_id'])
assert restored['state'] == 'SUCCEEDED'
view = new_session.req('/api/v1/projects/9010/sprints/' + sprint + '/observability')
assert view['execution']['status'] == 'completed'
assert {a['assignment_id'] for a in view['assignments']} == set(session['assignments'])
report = {'at': datetime.now(timezone.utc).isoformat(), 'status': 'PASS',
    'kind': 'real_unattended_native_author_and_two_independent_model_reviews',
    'client_source_commit': 'be1443c8f03e2e0875d8225979d9aa3a1a0a4ccc',
    'original_model_run_host': '65662f69db52c27b6c0972ccec8031cfb68a02fc',
    'revalidated_host_source_commit': 'c9193d7d6cab67f0b191a86f5e0e268bce074f9e',
    'installed_client_source_commit': 'c9193d7d6cab67f0b191a86f5e0e268bce074f9e',
    'no_new_model_run_requested': True,
    'sprint_id': sprint, 'native_operation_id': operation, 'real_result_commit': commit,
    'model_runs': model_runs, 'document': document, 'operation_receipt': receipt,
    'reviews': reviews, 'branch_leases': state['branch_leases'],
    'slot_released': True, 'native_processes_and_ports_retired': True,
    'client_phase': 'TERMINAL', 'exact_apply_replay_same_operation': True,
    'named_operator_repair_recovers_receipt': True,
    'limitations': ['docs-only-v1 profile', 'Current UI is evidenced by browser-recovery-evidence.json; real model advice retains historical Q provenance',
                   'No Telegram/mobile or general coding acceptance claim']}
(output / 'native-history-on-installed-host.json').write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
print(json.dumps({k: report[k] for k in ('status', 'sprint_id', 'real_result_commit', 'client_phase', 'slot_released', 'exact_apply_replay_same_operation')}))
