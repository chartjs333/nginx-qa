"""Verify persistent native/history/advisor state around an actual host restart."""
import argparse
import hashlib
import json
from pathlib import Path
from datetime import datetime, timezone
from http_qualification import Client
from stop_wizard import quiescent

ROOT = Path(__file__).resolve().parent


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--after', action='store_true')
    args = parser.parse_args()
    client = Client()
    actor = client.pair()
    browser = json.loads((ROOT / 'browser-evidence.json').read_bytes())
    oid = browser['apply_response']['operation_id']
    receipt = client.req('/api/v1/projects/9000/setup/operations/' + oid)
    assert receipt['state'] == 'SUCCEEDED' and receipt['operation_id'] == oid
    history = client.req('/api/v1/projects/9000/setup/operations')['operations']
    assert len([r for r in history if r['operation_id'] == oid]) == 1
    fresh = client.req('/api/v1/projects/9010/setup/snapshot')
    assert fresh['source']['code_commit'] == 'a1841e420a517dfdb9ed2c5a4b44eaeb6b3286ca'
    assert all(next(c for c in fresh['checks'] if c['check_id'] == key)['status'] == 'PASS'
               for key in ('project.slot', 'engine.runtime', 'engine.resources', 'engine.delivery', 'engine.result_reviews', 'engine.recovery'))
    advisor = Path('C:/nqt37/setup/advisor')
    durable = {str(p.relative_to(advisor)): hashlib.sha256(p.read_bytes()).hexdigest()
               for p in advisor.rglob('*') if p.is_file() and not p.name.endswith('.lock')}
    # Successful terminal generation removes the admission marker. An empty
    # directory is expected; never invent a persisted advice/result ledger.
    assert advisor.is_dir() and not (advisor / 'ollama-request.json').exists()
    before_path = ROOT / 'host-restart-before.json'
    if args.after:
        previous = json.loads(before_path.read_bytes())
        assert quiescent() == previous['quiescent']
        assert durable == previous['advisor_state_hashes']
        assert receipt == previous['browser_operation_receipt']
        assert actor == previous['actor']
        start = json.loads((ROOT.parent / 'r4-native-start.json').read_bytes())['start']
        replay = client.req(start['apply_path'], start['apply_body'])
        assert replay['operation_id'] == start['receipt']['operation_id'] and replay['state'] == 'SUCCEEDED'
        record = {'status': 'PASS', 'kind': 'real_owned_host_restart', 'at': datetime.now(timezone.utc).isoformat(),
            'source_commit': fresh['source']['code_commit'], 'named_actor_restored': True,
            'browser_exact_operation_preserved': oid, 'native_client_history_byte_identical': True,
            'advisor_admission_quiescent_before_and_after': True, 'fresh_project9010_native_checks_pass': True,
            'exact_r4_apply_replay_no_new_run': replay, 'fresh_snapshot_id': fresh['snapshot_id']}
        (ROOT / 'host-restart-evidence.json').write_text(json.dumps(record, indent=2) + '\n', encoding='utf-8')
        print(json.dumps({'status': 'PASS', 'kind': record['kind'], 'operation': oid}))
    else:
        probe = client.action('worker.probe', {'candidate_id': 'default-executor'}, 'sdwn-final-installed-client-probe-20261104')
        assert probe['receipt']['state'] == 'SUCCEEDED'
        report = {'actor': actor, 'quiescent': quiescent(), 'advisor_state_hashes': durable,
                  'browser_operation_receipt': receipt, 'actual_installed_client_probe': probe}
        before_path.write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
        print(json.dumps({'status': 'PASS', 'worker_probe': probe['receipt']['state'], 'ready_for_owned_restart': True}))


if __name__ == '__main__':
    main()
