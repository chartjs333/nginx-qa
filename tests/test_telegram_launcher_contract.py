from __future__ import annotations

import copy
import json
import unittest
from pathlib import Path

from nginx_qa.managed_import import managed_schema_errors, parse_strict_json_object
from nginx_qa.sprint_types import managed_graph_semantic_issues

ROOT = Path(__file__).resolve().parents[1]
SCHEMA = "telegram-managed-sprint-launcher-v1.schema.json"


class TelegramLauncherEnvelopeContractTests(unittest.TestCase):
    def setUp(self):
        self.manifest = json.loads(
            (ROOT / "orchestration/sprints/prompt-storage-compaction-v1/sequential-sprint.json")
            .read_text(encoding="utf-8")
        )
        self.envelope = {
            "kind": "telegram_managed_sprint_v1",
            "schema_version": 1,
            "project_id": "9000",
            "autonomous_mode": False,
            "manifest": self.manifest,
        }

    def test_manual_and_autonomous_use_actual_nested_managed_schema(self):
        for mode in (False, True):
            with self.subTest(mode=mode):
                self.envelope["autonomous_mode"] = mode
                self.assertEqual([], managed_schema_errors(self.envelope, SCHEMA))
                self.assertEqual((), managed_graph_semantic_issues(self.envelope["manifest"]))

    def test_explicit_envelope_never_defaults_the_mode(self):
        for mode in (None, 0, 1, "false", "true", [], {}):
            with self.subTest(mode=mode):
                self.envelope["autonomous_mode"] = mode
                self.assertTrue(managed_schema_errors(self.envelope, SCHEMA))
        del self.envelope["autonomous_mode"]
        self.assertTrue(managed_schema_errors(self.envelope, SCHEMA))

    def test_no_transport_or_authority_fields_can_be_injected(self):
        for field, value in {
            "repository_url": "https://example.invalid/other.git",
            "runtime_root": "C:/outside",
            "instance_id": "caller-selected",
            "operator_policy": {"start": True},
            "token": "not-an-authority",
        }.items():
            with self.subTest(field=field):
                candidate = copy.deepcopy(self.envelope)
                candidate[field] = value
                self.assertTrue(managed_schema_errors(candidate, SCHEMA))

    def test_version_project_and_kind_are_explicit(self):
        for field, value in [
            ("kind", "telegram_managed_sprint_v2"),
            ("schema_version", 2),
            ("project_id", 9000),
            ("project_id", "0001"),
            ("project_id", "../9000"),
            ("project_id", "same-repo-alias"),
        ]:
            with self.subTest(field=field, value=value):
                candidate = copy.deepcopy(self.envelope)
                candidate[field] = value
                self.assertTrue(managed_schema_errors(candidate, SCHEMA))

    def test_nested_reviewer_schema_and_semantic_graph_are_both_required(self):
        candidate = copy.deepcopy(self.envelope)
        candidate["manifest"]["execution"]["reviewers"].pop()
        self.assertTrue(managed_schema_errors(candidate, SCHEMA))
        candidate = copy.deepcopy(self.envelope)
        task = next(n for n in candidate["manifest"]["nodes"] if n.get("type") != "terminal")
        task["transitions"]["DONE"] = "missing-node"
        self.assertEqual([], managed_schema_errors(candidate, SCHEMA))
        self.assertIn("GRAPH_TRANSITION_TARGET_UNKNOWN",
                      managed_graph_semantic_issues(candidate["manifest"]))

    def test_unwrapped_managed_manifest_contract_is_unchanged(self):
        self.assertEqual([], managed_schema_errors(
            self.manifest, "managed-workspace-sprint-v1.schema.json"))
        self.assertTrue(managed_schema_errors(self.manifest, SCHEMA))
        self.assertNotIn("autonomous_mode", self.manifest)

    def test_existing_strict_parser_rejects_ambiguous_json(self):
        for raw in (b'{"a":1,"a":2}', b'{"a":NaN}', b'{"a":Infinity}', b'[]'):
            with self.subTest(raw=raw):
                with self.assertRaises(ValueError):
                    parse_strict_json_object(raw)

    def test_acceptance_matrix_preserves_16_cases_and_single_project_rule(self):
        matrix = json.loads((ROOT / "docs/telegram-managed-sprint-launcher-v1/acceptance-matrix.json")
                            .read_text(encoding="utf-8"))
        self.assertEqual([f"TMSL-AT-{i:02}" for i in range(1, 17)],
                         [case["id"] for case in matrix["cases"]])
        case = matrix["cases"][7]
        self.assertIn("two PROJECT_BUSY", case["positive"])
        allowed = {"PASS", "FAIL", "NOT_RUN", "BLOCKED"}
        docs = ROOT / "docs/telegram-managed-sprint-launcher-v1"
        for case in matrix["cases"]:
            with self.subTest(case=case["id"]):
                self.assertIn(case["status"], allowed)
                if case["status"] == "PASS":
                    self.assertEqual("PASS", case.get("automated_status"))
                if case["external_evidence_required"]:
                    external = case.get("external_status", "NOT_RUN")
                    self.assertIn(external, allowed)
                    if case["status"] == "PASS":
                        self.assertEqual("PASS", external)
                        external_path = (docs / case["external_evidence"]).resolve()
                        self.assertTrue(external_path.is_relative_to(docs.resolve()))
                        self.assertTrue(external_path.is_file())
                if case.get("automated_status") == "PASS":
                    evidence_path = (docs / case["automated_evidence"]).resolve()
                    self.assertTrue(evidence_path.is_relative_to(docs.resolve()))
                    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
                    self.assertEqual("PASS", evidence["status"])
                    self.assertEqual(0, evidence["failures"])
                    self.assertEqual(0, evidence["errors"])
                    self.assertEqual(matrix["automated_source_commit"], evidence["source_commit"])
                    records = {entry["test"]: entry for entry in evidence["results"]}
                    self.assertEqual(len(records), len(evidence["results"]))
                    self.assertEqual(matrix["automated_total_unique_tests"], evidence["tests_run"])
                    self.assertTrue(case["test_ids"])
                    self.assertEqual(len(case["test_ids"]), len(set(case["test_ids"])))
                    for test_id in case["test_ids"]:
                        self.assertEqual("PASS", records[test_id]["status"])
        # Matrix packaging changes are test-input changes, even without Python edits.
        from scripts.qualify_telegram_launcher import source_inventory
        self.assertIn("docs/telegram-managed-sprint-launcher-v1/acceptance-matrix.json",
                      source_inventory())


if __name__ == "__main__":
    unittest.main()
