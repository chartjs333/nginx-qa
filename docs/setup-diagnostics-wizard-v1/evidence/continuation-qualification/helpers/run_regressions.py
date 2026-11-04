"""Consolidated regressions against the exact installed commit in an isolated export."""
from datetime import datetime, timezone
import hashlib
import importlib
import inspect
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

SOURCE = Path('D:/nq-sdw-launcher')
EXPECTED = 'a1841e420a517dfdb9ed2c5a4b44eaeb6b3286ca'
OUT = Path(__file__).resolve().parent
MODULES = ['tests.' + name for name in (
    'test_windows_launcher', 'test_staging_isolation', 'test_staging_legacy_inventory',
    'test_staging_deployment_branch', 'test_safe_operations_recovery',
    'test_setup_private_api', 'test_operator_session', 'test_setup_diagnostics',
    'test_setup_diagnostics_regressions', 'test_llm_advisor', 'test_wizard_ui',
    'test_setup_native', 'test_setup_worker_corrective', 'test_worker_connection_selftest',
    'test_launcher_transport_migration', 'test_managed_client_git_transport',
    'test_managed_wsl_runner', 'test_ollama_advisor')]


def main():
    if len(sys.argv) == 2:
        report_path = Path(sys.argv[1])
        suite = unittest.TestSuite()
        for name in MODULES:
            module = importlib.import_module(name)
            suite.addTests(unittest.defaultTestLoader.loadTestsFromModule(module))
            # unittest does not discover the three existing function-style
            # Windows launcher tests; include those explicitly without pytest.
            for key, value in vars(module).items():
                if key.startswith('test_') and inspect.isfunction(value):
                    assert not inspect.signature(value).parameters, key
                    suite.addTest(unittest.FunctionTestCase(value))
        result = unittest.TextTestRunner(verbosity=2).run(suite)
        report_path.write_text(json.dumps({'run': result.testsRun, 'failures': len(result.failures),
            'errors': len(result.errors), 'skipped': result.skipped, 'successful': result.wasSuccessful()}, indent=2) + '\n', encoding='utf-8')
        return 0 if result.wasSuccessful() else 1
    def git(*args):
        return subprocess.check_output(['git', '-C', str(SOURCE), *args]).decode().strip()
    assert git('rev-parse', 'HEAD') == EXPECTED
    assert not git('status', '--porcelain=v1', '--untracked-files=all')
    export = Path(tempfile.mkdtemp(prefix='nq-sdwn-final-tests-', dir='C:/'))
    manifest = {}
    for name in git('ls-files', '-z').split('\0'):
        source = SOURCE / name
        if name and source.is_file():
            destination = export / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)
            manifest[name] = hashlib.sha256(source.read_bytes()).hexdigest()
    environment = {k: v for k, v in os.environ.items() if not (
        k.upper().startswith(('NGINX_QA_', 'TELEGRAM_', 'GEMINI_', 'OPENAI_', 'ANTHROPIC_'))
        or any(s in k.upper() for s in ('TOKEN', 'SECRET', 'API_KEY', 'PROXY')))}
    state = export / 'test-state'
    state.mkdir()
    environment.update(NGINX_QA_STAGING_STATE_BASE=str(state), NGINX_QA_RUNTIME_ROOT=str(state / 'runtime'),
                       NGINX_QA_PROMPT_ROOT=str(state / 'prompts'), PYTHONDONTWRITEBYTECODE='1',
                       PYTHONIOENCODING='utf-8', PYTHONPATH=str(export))
    result_path = OUT / 'regression-counts.json'
    log = OUT / 'regressions.txt'
    started = datetime.now(timezone.utc).isoformat()
    with log.open('w', encoding='utf-8') as stream:
        run = subprocess.run([sys.executable, '-X', 'utf8', '-B', str(Path(__file__).resolve()), str(result_path)],
            cwd=export, env=environment, stdout=stream, stderr=subprocess.STDOUT, timeout=900)
    report = {'source_commit': EXPECTED, 'started_at': started, 'finished_at': datetime.now(timezone.utc).isoformat(),
              'export': str(export), 'modules': MODULES, 'exit_code': run.returncode,
              'counts': json.loads(result_path.read_bytes()) if result_path.exists() else None,
              'log_sha256': hashlib.sha256(log.read_bytes()).hexdigest(), 'source_hashes': manifest}
    (OUT / 'regression-run.json').write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    print(json.dumps({k: report[k] for k in ('source_commit', 'exit_code', 'counts')}))
    print(log.read_text(encoding='utf-8')[-3000:])
    return run.returncode


if __name__ == '__main__':
    raise SystemExit(main())
