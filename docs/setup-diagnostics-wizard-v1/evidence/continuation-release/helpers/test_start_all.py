"""Exercise real PowerShell child exits; fixtures never touch a server or secret."""
import json
import subprocess
import unittest
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PS = r'C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe'
FIXTURES = Path('C:/nqt35/client/sdw-continuation-20261104/release-final/startup-test-fixtures')


def ps_quote(value):
    return "'" + str(value).replace("'", "''") + "'"


class StartupTests(unittest.TestCase):
    def run_fixture(self, exit_codes, missing=None):
        folder = FIXTURES / uuid.uuid4().hex
        folder.mkdir(parents=True)
        trace = folder / 'order.txt'
        steps = []
        for index, code in enumerate(exit_codes):
            step = folder / f'step {index}.ps1'
            steps.append(step)
            if index != missing:
                step.write_text(f"Add-Content -LiteralPath {ps_quote(trace)} -Value '{index}'\nexit {code}\n", encoding='utf-8')
        harness = folder / 'invoke.ps1'
        harness.write_text(
            f". {ps_quote(ROOT / 'start_all.ps1')}\n"
            + 'try { Invoke-SetupStartupSteps -Steps @(' + ','.join(ps_quote(p) for p in steps) + ') }\n'
            + 'catch { [Console]::Error.WriteLine($_.Exception.Message); exit 23 }\nexit 0\n', encoding='utf-8')
        result = subprocess.run([PS, '-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass', '-File', str(harness)],
                                capture_output=True, timeout=30)
        order = trace.read_text(encoding='utf-8-sig').splitlines() if trace.exists() else []
        return result, order

    def test_all_steps_run_in_order(self):
        result, order = self.run_fixture([0, 0, 0])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(order, ['0', '1', '2'])

    def test_failure_prevents_later_steps(self):
        result, order = self.run_fixture([0, 7, 0])
        self.assertEqual(result.returncode, 23)
        self.assertIn(b'exit 7', result.stderr)
        self.assertEqual(order, ['0', '1'])

    def test_missing_helper_prevents_any_start(self):
        result, order = self.run_fixture([0, 0, 0], missing=2)
        self.assertEqual(result.returncode, 23)
        self.assertIn(b'Required startup helper is missing', result.stderr)
        self.assertEqual(order, [])


if __name__ == '__main__':
    unittest.main(verbosity=2)
