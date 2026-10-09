"""Run the scope change's affected checks on isolated temporary test stores."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
import time
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
MODULES = [
    "tests.test_legacy_scope_control", "tests.test_scope_revisions",
    "tests.test_scope_migration", "tests.test_scope_workflow",
    "tests.test_scope_runtime_adapters", "tests.test_operator_session",
    "tests.test_execution_observability", "tests.test_groups",
    "tests.test_queue_persistence", "tests.test_project_actor_import",
    "tests.test_managed_continuity", "tests.test_history_ui",
    "tests.test_sequential_prompt_ui", "tests.test_cycle_graph_ui",
    "tests.test_sprint_type_contract", "tests.test_auto_refresh_ui",
    "tests.test_managed_scope_secret_boundary",
    "tests.test_managed_import.ManagedCredentialResolverTests",
]


class EvidenceResult(unittest.TextTestResult):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.records = []

    def startTest(self, test):
        self.started = time.monotonic()
        super().startTest(test)

    def record(self, test, outcome, detail=None):
        self.records.append({"test": test.id(), "outcome": outcome,
                             "seconds": round(time.monotonic() - self.started, 4), "detail": detail})

    def addSuccess(self, test):
        super().addSuccess(test)
        self.record(test, "passed")

    def addFailure(self, test, err):
        super().addFailure(test, err)
        self.record(test, "failed", self._exc_info_to_string(err, test))

    def addError(self, test, err):
        super().addError(test, err)
        self.record(test, "error", self._exc_info_to_string(err, test))

    def addSkip(self, test, reason):
        super().addSkip(test, reason)
        self.record(test, "skipped", reason)


def flatten(suite):
    for test in suite:
        if isinstance(test, unittest.TestSuite):
            yield from flatten(test)
        else:
            yield test


def source_fingerprint():
    # Evidence/docs do not recursively hash themselves. Runtime/venv/secrets are
    # never inspected; only the explicit source + test + schema inputs below.
    files = [ROOT / "main.py", ROOT / "requirements.txt", ROOT / "requirements-dev.txt"]
    for folder, suffixes in (("nginx_qa", {".py", ".js", ".css", ".html"}), ("tests", {".py"}), ("schemas", {".json"}), ("scripts", {".py", ".ps1"}), ("tools", {".py"})):
        files += [p for p in (ROOT / folder).rglob("*") if p.is_file() and p.suffix in suffixes]
    mapping = {p.relative_to(ROOT).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(set(files))}
    return {"files": mapping, "sha256": hashlib.sha256(json.dumps(mapping, sort_keys=True, separators=(",", ":")).encode()).hexdigest()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--list", action="store_true")
    args = parser.parse_args()
    loader = unittest.TestLoader()
    tests = {test.id(): test for test in flatten(loader.loadTestsFromNames(MODULES))}
    if args.list:
        print(json.dumps({"modules": MODULES, "unique_test_count": len(tests)}, indent=2))
        return 0
    started = datetime.now(timezone.utc).isoformat()
    before = source_fingerprint()
    result = unittest.TextTestRunner(verbosity=2, resultclass=EvidenceResult).run(unittest.TestSuite(tests.values()))
    after = source_fingerprint()
    stable = before == after
    evidence = {"schema_version": 1, "kind": "affected_scope_regression",
        "requirements_commit": "e72e6f88abf81037a2afec408f8d023980607d66",
        "source_base": "d28726bbcbb552dd21f8cda1042f39f2ee2a21ec",
        "started_at": started, "finished_at": datetime.now(timezone.utc).isoformat(),
        "python": sys.version, "source_unchanged_during_run": stable,
        "source_fingerprint": before, "modules": MODULES, "tests_run": result.testsRun,
        "passed": result.wasSuccessful() and stable, "failures": len(result.failures),
        "errors": len(result.errors), "skipped": len(result.skipped), "tests": result.records,
        "not_run": ["Full repository regression (outside affected checks)",
            "Live deployment/restart/migration/amendment/ACK (not authorized)",
            "External production integrations and performance/load campaign"],
        "live_mutations": 0}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(evidence, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({key: evidence[key] for key in ("tests_run", "passed", "failures", "errors", "skipped", "source_unchanged_during_run")}))
    return 0 if evidence["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
