"""Start the reviewed native client; no replacement scheduler or model shim."""
import json
import os
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parent
SOURCE = Path('D:/nq-sdw-launcher')
EXPECTED = 'a1841e420a517dfdb9ed2c5a4b44eaeb6b3286ca'
def git(*args):
    return subprocess.check_output(['git', '-C', str(SOURCE), *args]).decode().strip()
assert git('rev-parse', 'HEAD') == EXPECTED
assert not git('status', '--porcelain=v1', '--untracked-files=all')
config = json.loads((ROOT.parent / 'autonomous-client-r4.PREPARED.json').read_bytes())
assert config['listen_url'] == 'http://127.0.0.1:18039'
assert config['state_dir'] == 'C:/nqt37/wsl-client-r4'
assert set(config['projects']) == {'9010'}
state = Path(config['state_dir']) / 'client-receipts.json'
if state.exists():
    data = json.loads(state.read_bytes())
    assert all(s['phase'] in ('STOPPED', 'TERMINAL') for s in data['sessions'].values()), 'Explicit reconciliation is required for live sessions'
# Read only the existing executor control secret. Do not load the admin token,
# provider keys, unrelated environment or a full Codex home into this service.
key = 'NGINX_QA_LAUNCHER_CONTROL_CLIENT_TOKEN'
values = [line.split('=', 1)[1].strip().strip('"') for line in
          Path('D:/nq-sdw-launcher/.env.staging').read_text().splitlines()
          if line.startswith(key + '=')]
assert len(values) == 1 and re.fullmatch(r'[A-Za-z0-9_-]{32,256}', values[0])
os.environ[key] = values[0]
sys.path.insert(0, str(SOURCE))
from nginx_qa.managed_codex_client import serve
import nginx_qa.managed_wsl_runner
# Verify the actual transitive imported repository code matches the real R4 run.
original = 'be1443c8f03e2e0875d8225979d9aa3a1a0a4ccc'
modules = {}
for module in tuple(sys.modules.values()):
    filename = getattr(module, '__file__', None)
    if filename:
        path = Path(filename).resolve()
        if path.is_relative_to(SOURCE.resolve()):
            rel = path.relative_to(SOURCE.resolve()).as_posix()
            if rel.endswith('.py'):
                previous = git('rev-parse', original + ':' + rel)
                current = git('rev-parse', EXPECTED + ':' + rel)
                assert previous == current, 'Client runtime changed: ' + rel
                modules[rel] = current
assert 'nginx_qa/managed_codex_client.py' in modules and 'nginx_qa/managed_wsl_runner.py' in modules
(ROOT / 'client-runtime-blob-equality.json').write_text(json.dumps({'original_actual_model_source': original, 'installed_source': EXPECTED, 'modules': modules, 'identical': True}, indent=2) + '\n', encoding='utf-8')
print(json.dumps({'kind': 'reviewed_native_client_launch_intent', 'source_commit': EXPECTED,
                  'listen': config['listen_url'], 'admitted_projects': ['9010'],
                  'backend': config['sandbox_backend']}), flush=True)
serve(config)
