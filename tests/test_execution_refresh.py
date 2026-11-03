"""Offline browser scheduling contracts; no main import or real HTTP."""

from pathlib import Path
import shutil
import subprocess
import unittest


ROOT = Path(__file__).resolve().parents[1]
ASSET = ROOT / "nginx_qa" / "static" / "execution.js"
HARNESS = ROOT / "tests" / "execution_refresh_contract.cjs"


@unittest.skipUnless(shutil.which("node"), "Node.js unavailable for isolated UI checks")
class ExecutionRefreshTests(unittest.TestCase):
    def run_scenario(self, scenario):
        result = subprocess.run(
            [shutil.which("node"), str(HARNESS), str(ASSET), scenario],
            capture_output=True, text=True, encoding="utf-8", timeout=20,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("PASS " + scenario, result.stdout)

    def test_initial_load_and_two_minute_timer(self):
        self.run_scenario("initial")

    def test_notification_load_uses_same_two_minute_timer(self):
        self.run_scenario("notification")

    def test_manual_refresh_is_immediate_and_read_only(self):
        self.run_scenario("manual")

    def test_polling_is_single_flight_and_does_not_queue(self):
        self.run_scenario("single-flight")

    def test_busy_selection_changes_queue_one_latest_refresh(self):
        self.run_scenario("selection")

    def test_error_reenables_manual_refresh(self):
        self.run_scenario("error")


if __name__ == "__main__":
    unittest.main()
