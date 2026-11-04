"""Explicit paid/model transport smoke in a disposable repo, not native acceptance.

No model runs on import or under unittest discovery. --run-model is required.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import threading
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from nginx_qa.managed_wsl_runner import WslCodexRunner


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True)
    parser.add_argument('--fixture-parent', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--run-model', action='store_true')
    args = parser.parse_args()
    if not args.run_model:
        parser.error('This explicit smoke requires --run-model')
    config = json.loads(Path(args.config).read_bytes())
    runner = WslCodexRunner(config)
    fixture = Path(args.fixture_parent).resolve(strict=True) / ('cli-smoke-' + uuid.uuid4().hex[:8])
    repo = fixture / 'repo'
    repo.mkdir(parents=True)
    marker = uuid.uuid4().hex
    (repo / 'README.md').write_text('Fixture marker: ' + marker + '\n', encoding='utf-8')

    def git(*argv):
        return subprocess.run(['git', '-C', str(repo), *argv], capture_output=True, check=True)

    git('init')
    git('config', 'user.name', 'Disposable worker smoke')
    git('config', 'user.email', 'fixture@example.invalid')
    git('add', 'README.md')
    git('commit', '-m', 'Disposable smoke source')
    before = runner.auth_path.read_bytes()
    schema = {'title': 'author', 'type': 'object', 'additionalProperties': False,
              'required': ['status', 'summary'], 'properties': {
                  'status': {'type': 'string', 'enum': ['DONE']},
                  'summary': {'type': 'string'}}}
    result = runner.run(
        cwd=repo, directory=fixture / 'receipt', job_name=fixture.name,
        stop=threading.Event(), schema=schema, allowed_paths=['docs/probe.md'],
        prompt='Read README.md. Write docs/probe.md with exactly the title "# Isolated worker smoke", '
               'a blank line, then the entire Fixture marker line copied from README.md, then a newline. '
               'Read the written file back. Do not change other files. This is a disposable CLI '
               'transport smoke and does not represent native sprint acceptance.')
    expected = '# Isolated worker smoke\n\nFixture marker: ' + marker + '\n'
    assert (repo / 'docs/probe.md').read_text(encoding='utf-8') == expected
    assert runner.auth_path.read_bytes() == before, 'Source login unexpectedly changed'
    assert git('status', '--porcelain').stdout.strip() == b'?? docs/'
    source = Path(__file__).resolve().parents[1]
    files = ('nginx_qa/managed_codex_client.py', 'nginx_qa/managed_wsl_runner.py',
             'nginx_qa/wsl_worker_supervisor.py', 'scripts/install_wsl_worker.py',
             'tests/wsl_cli_smoke.py', 'tests/wsl_runtime_qualification.py')
    report = {
        'status': 'PASS', 'kind': 'real_cli_transport_smoke_not_native_acceptance',
        'at_utc': datetime.now(timezone.utc).isoformat(), 'result': result,
        'document': expected, 'source_auth_unchanged': True,
        'refresh_authority_delegated': False, 'fixture': str(fixture),
        'supervisor_sha256': config['wsl_supervisor_sha256'],
        'runtime_hashes': config['wsl_runtime_hashes'],
        'source_sha256': {p: hashlib.sha256((source / p).read_bytes()).hexdigest() for p in files},
        'native_sprint_acceptance': False,
    }
    Path(args.output).write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    print(json.dumps({'status': 'PASS', 'output': args.output, 'fixture': str(fixture)}))


if __name__ == '__main__':
    main()
