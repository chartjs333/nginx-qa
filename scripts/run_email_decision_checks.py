"""Targeted email plus affected auth/CAS/persistence/UI/schema regression."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts.run_scope_v2_checks import EvidenceResult, flatten, source_fingerprint

MODULES = [
    "tests.test_decision_notifications", "tests.test_email_decision_api",
    "tests.test_email_decision_ui", "tests.test_email_runtime_adapters",
    "tests.test_email_notification_lifecycle", "tests.test_operator_session",
    "tests.test_scope_workflow", "tests.test_scope_runtime_adapters",
    "tests.test_legacy_scope_control", "tests.test_scope_revisions",
    "tests.test_execution_observability", "tests.test_queue_persistence",
    "tests.test_sprint_type_contract",
]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    suite = unittest.TestLoader().loadTestsFromNames(MODULES)
    unique = {test.id(): test for test in flatten(suite)}
    started = datetime.now(timezone.utc).isoformat()
    before = source_fingerprint()
    result = unittest.TextTestRunner(verbosity=2, resultclass=EvidenceResult).run(unittest.TestSuite(unique.values()))
    stable = before == source_fingerprint()
    all_modules = {"tests." + path.stem for path in (ROOT / "tests").glob("test_*.py")}
    evidence = {
        "schema_version": 1, "kind": "email_decision_affected_regression",
        "base_commit": "8581390511838efb15a69cb2fadac89b12483b3b",
        "started_at": started, "finished_at": datetime.now(timezone.utc).isoformat(),
        "python": sys.version, "source_fingerprint": before,
        "source_unchanged_during_run": stable, "modules": MODULES,
        "tests_run": result.testsRun, "failures": len(result.failures),
        "errors": len(result.errors), "skipped": len(result.skipped),
        "passed": result.wasSuccessful() and stable, "tests": result.records,
        "not_run_suites": sorted(all_modules - set(MODULES)),
        "not_run_external": ["Real SMTP provider/inbox delivery and real credentials",
            "Production reverse proxy / remote mobile reachability",
            "Live deployment, restart, migration, decision, ACK or graph mutations",
            "Full repository regression / stress and load campaign"],
        "live_deployment": False, "port18025_restart": False, "port8025_changed": False,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(evidence, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({key: evidence[key] for key in ("tests_run", "passed", "failures", "errors", "skipped", "source_unchanged_during_run")}))
    return 0 if evidence["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
