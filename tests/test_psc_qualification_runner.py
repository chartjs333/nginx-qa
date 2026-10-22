import copy
import pathlib
import tempfile
import types
import unittest
from unittest import mock

from tools import run_psc_qualification as runner


REPOSITORY_ROOT = pathlib.Path(__file__).resolve().parents[1]
SELECTOR_PATH = (
    REPOSITORY_ROOT
    / "orchestration"
    / "sprints"
    / "prompt-storage-compaction-v1"
    / "qualification-selectors-v1.json"
)


class PscQualificationRunnerTests(unittest.TestCase):
    def assertQualificationCode(self, expected, callback, *args, **kwargs):
        with self.assertRaises(runner.QualificationError) as captured:
            callback(*args, **kwargs)
        self.assertEqual(captured.exception.code, expected)

    def test_committed_selector_is_strict_and_maps_every_acceptance(self) -> None:
        selector = runner._load_selector(SELECTOR_PATH)
        self.assertEqual(selector["expected_unique_test_count"], 943)
        self.assertEqual(
            set(selector["acceptance_map"]),
            {f"PSC-A{number:02d}" for number in range(1, 21)},
        )
        tolerance = {
            key: value
            for key, value in selector["pass_policy"].items()
            if key != "required_unique_test_count"
        }
        self.assertEqual(set(tolerance.values()), {0})
        self.assertEqual(
            selector["external_evidence"][0]["expected_kind"],
            "psc_a05_a16_a17_exact_head_benchmark_evidence",
        )

    def test_integrity_seal_detects_payload_tamper(self) -> None:
        document = runner._with_integrity({"schema_version": 1, "value": [1, 2]})
        runner._verify_integrity(document)
        document["value"].append(3)
        self.assertQualificationCode(
            "PSC_QUALIFICATION_INTEGRITY_MISMATCH",
            runner._verify_integrity,
            document,
        )

    def test_immutable_writer_is_idempotent_and_rejects_conflict(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "receipt.json"
            runner._write_immutable(path, b"one")
            runner._write_immutable(path, b"one")
            self.assertEqual(path.read_bytes(), b"one")
            self.assertQualificationCode(
                "PSC_QUALIFICATION_IMMUTABLE_ARTIFACT_CONFLICT",
                runner._write_immutable,
                path,
                b"two",
            )

    def test_windows_lexical_guard_has_component_boundary(self) -> None:
        self.assertTrue(runner._lexically_within(r"d:\prompt", r"D:\Prompt"))
        self.assertTrue(
            runner._lexically_within(r"D:\Prompt\nested\item", r"D:\Prompt")
        )
        self.assertFalse(
            runner._lexically_within(r"D:\Prompt-copy\item", r"D:\Prompt")
        )
        self.assertQualificationCode(
            "PSC_QUALIFICATION_FORBIDDEN_ROOT_ACCESS",
            runner._reject_forbidden_path,
            pathlib.Path(r"D:\Prompt\live.json"),
            runner.BUILTIN_FORBIDDEN_ROOTS,
        )

    def test_run_root_overlap_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            base = pathlib.Path(directory)
            workspace = base / "workspace"
            workspace.mkdir()
            self.assertQualificationCode(
                "PSC_QUALIFICATION_RUN_ROOT_OVERLAP",
                runner._validate_run_root,
                workspace / "run",
                workspace,
                [],
            )
            forbidden = base / "protected"
            self.assertQualificationCode(
                "PSC_QUALIFICATION_RUN_ROOT_OVERLAP",
                runner._validate_run_root,
                forbidden / "run",
                workspace,
                [str(forbidden)],
            )

    def test_output_path_must_be_below_run_root(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory) / "run"
            root.mkdir()
            runner._require_path_below(root / "ledger.json", root)
            self.assertQualificationCode(
                "PSC_QUALIFICATION_OUTPUT_OUTSIDE_RUN_ROOT",
                runner._require_path_below,
                root.parent / "ledger.json",
                root,
            )
            first = runner._safe_worker_environment(root, "shard-0001", 1)
            second = runner._safe_worker_environment(root, "shard-0001", 2)
            self.assertNotEqual(first["TEMP"], second["TEMP"])
            self.assertIn("attempt-0001", first["TEMP"])
            self.assertIn("attempt-0002", second["TEMP"])

    def test_workspace_import_path_is_added_once_for_script_execution(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workspace = pathlib.Path(directory).resolve()
            original = list(runner.sys.path)
            try:
                runner.sys.path[:] = [
                    value
                    for value in runner.sys.path
                    if pathlib.Path(value or ".").absolute() != workspace
                ]
                runner._ensure_workspace_import_path(workspace)
                runner._ensure_workspace_import_path(workspace)
                normalized = runner.os.path.normcase(str(workspace))
                matches = [
                    value
                    for value in runner.sys.path
                    if runner.os.path.normcase(
                        runner.os.path.abspath(value or runner.os.curdir)
                    )
                    == normalized
                ]
                self.assertEqual(matches, [str(workspace)])
            finally:
                runner.sys.path[:] = original

    def test_owned_collection_excludes_imported_test_case_classes(self) -> None:
        module_name = "synthetic.owned"
        imported_name = "synthetic.imported"
        module = types.ModuleType(module_name)
        owned = type(
            "OwnedTests",
            (unittest.TestCase,),
            {"__module__": module_name, "test_owned": lambda self: None},
        )
        imported = type(
            "ImportedTests",
            (unittest.TestCase,),
            {"__module__": imported_name, "test_imported": lambda self: None},
        )
        module.OwnedTests = owned
        module.ImportedTests = imported
        expected_id = f"{module_name}.OwnedTests.test_owned"
        selector = {
            "categories": [
                {
                    "id": "synthetic",
                    "expected_owned_test_count": 1,
                    "expected_test_ids_sha256": runner._sha256(
                        f"{expected_id}\n".encode("utf-8")
                    ),
                    "modules": [module_name],
                }
            ]
        }
        with mock.patch.object(runner.importlib, "import_module", return_value=module):
            tests, categories, loader_errors = runner._collect_owned_tests(selector)
        self.assertEqual([item["id"] for item in tests], [expected_id])
        self.assertEqual(categories[0]["test_count"], 1)
        self.assertEqual(loader_errors, [])

    def test_plan_prioritizes_depth_tests_and_splits_singleton_module(self) -> None:
        rows = [
            {"id": "tests.a.Case.test_priority", "module": "tests.a"},
            {"id": "tests.a.Case.test_singleton", "module": "tests.a"},
            {"id": "tests.b.Case.test_grouped", "module": "tests.b"},
        ]
        collection = {
            "suite_id": "synthetic",
            "run_key": "a" * 64,
            "candidate": {"head": "b" * 40, "tree": "c" * 40},
            "selector": {
                "priority_test_ids": [rows[0]["id"]],
                "singleton_modules": ["tests.a"],
                "categories": [{"modules": ["tests.a", "tests.b"]}],
            },
            "selection": {"tests": rows, "test_ids_sha256": "d" * 64},
        }
        plan = runner._plan_document(collection)
        self.assertEqual([item["kind"] for item in plan["shards"]], [
            "priority",
            "singleton",
            "module",
        ])
        planned = [
            test_id for shard in plan["shards"] for test_id in shard["test_ids"]
        ]
        self.assertEqual(sorted(planned), sorted(item["id"] for item in rows))
        self.assertEqual(len(planned), len(set(planned)))

    def test_result_counts_successful_subtests_without_hiding_parent(self) -> None:
        class SuccessfulSubtests(unittest.TestCase):
            def test_values(self):
                for value in (1, 2):
                    with self.subTest(value=value):
                        self.assertGreater(value, 0)

        case = SuccessfulSubtests("test_values")
        result = runner.QualificationTestResult([case.id()])
        unittest.TestSuite([case]).run(result)
        summary = result.summary()
        self.assertEqual(summary["parent"]["success"], 1)
        self.assertEqual(summary["subtests"]["success"], 2)
        self.assertEqual(summary["subtests"]["total"], 2)

    def test_result_exposes_subtest_failure_as_parent_failure(self) -> None:
        class FailingSubtest(unittest.TestCase):
            def test_values(self):
                with self.subTest(value=1):
                    self.fail("expected synthetic failure")

        case = FailingSubtest("test_values")
        result = runner.QualificationTestResult([case.id()])
        unittest.TestSuite([case]).run(result)
        summary = result.summary()
        self.assertEqual(summary["parent"]["subtest_failure"], 1)
        self.assertEqual(summary["subtests"]["failure"], 1)
        self.assertEqual(summary["parent"]["success"], 0)

    def test_result_keeps_skip_expected_failure_and_unexpected_success_distinct(self) -> None:
        class ExceptionalOutcomes(unittest.TestCase):
            @unittest.skip("synthetic")
            def test_skip(self):
                pass

            @unittest.expectedFailure
            def test_expected_failure(self):
                self.fail("synthetic")

            @unittest.expectedFailure
            def test_unexpected_success(self):
                pass

        suite = unittest.defaultTestLoader.loadTestsFromTestCase(ExceptionalOutcomes)
        ids = [case.id() for case in runner._flatten_suite(suite)]
        result = runner.QualificationTestResult(ids)
        suite.run(result)
        summary = result.summary()
        self.assertEqual(summary["parent"]["skip"], 1)
        self.assertEqual(summary["parent"]["expected_failure"], 1)
        self.assertEqual(summary["parent"]["unexpected_success"], 1)

    def test_worker_pass_validator_rejects_skip_even_with_pass_status(self) -> None:
        test_id = "tests.synthetic.Case.test_ok"
        candidate = {
            "head": "a" * 40,
            "tree": "b" * 40,
            "branch": "synthetic",
            "clean": True,
            "status_sha256": runner._sha256(b""),
            "status_line_count": 0,
        }
        result = {
            "status": "PASS",
            "candidate": candidate,
            "shard": {"test_count": 1, "test_ids": [test_id]},
            "loader": {
                "requested_ids_equal_loaded_ids": True,
                "loaded_test_count": 1,
                "error_count": 0,
                "error_hashes": [],
            },
            "outcomes": {
                "tests_run": 1,
                "parent": {
                    "success": 1,
                    "failure": 0,
                    "error": 0,
                    "skip": 0,
                    "expected_failure": 0,
                    "unexpected_success": 0,
                    "subtest_failure": 0,
                    "subtest_error": 0,
                    "subtest_skip": 0,
                    "not_run": 0,
                },
                "subtests": {
                    "success": 0,
                    "failure": 0,
                    "error": 0,
                    "skip": 0,
                    "total": 0,
                },
                "fixture_error_count": 0,
                "unexpected_parent_id_count": 0,
            },
            "tests": [{"id": test_id, "outcome": "success", "subtests": []}],
            "fixture_errors": [],
            "unexpected_parent_id_count": 0,
            "forbidden_root_attempts": [],
            "execution_exception": None,
            "git_before": candidate,
            "git_after": candidate,
        }
        runner._validate_worker_pass(result)
        forged = copy.deepcopy(result)
        forged["outcomes"]["parent"]["success"] = 0
        forged["outcomes"]["parent"]["skip"] = 1
        forged["tests"][0]["outcome"] = "skip"
        self.assertQualificationCode(
            "PSC_QUALIFICATION_WORKER_PASS_INVALID",
            runner._validate_worker_pass,
            forged,
        )

    def test_external_evidence_requires_seal_contract_and_pass_values(self) -> None:
        specification = {
            "expected_schema_version": 2,
            "expected_kind": "benchmark",
            "seal_field": "payload_sha256",
            "required_pointer_values": {"/acceptance/overall_pass": True},
        }
        evidence = {
            "schema_version": 2,
            "kind": "benchmark",
            "acceptance": {"overall_pass": True},
        }
        evidence["payload_sha256"] = runner._sha256(
            runner._canonical_json_bytes(evidence)
        )
        runner._verify_external_evidence(evidence, specification)
        tampered = copy.deepcopy(evidence)
        tampered["acceptance"]["overall_pass"] = False
        self.assertQualificationCode(
            "PSC_QUALIFICATION_EVIDENCE_INTEGRITY_MISMATCH",
            runner._verify_external_evidence,
            tampered,
            specification,
        )
        tampered["payload_sha256"] = runner._sha256(
            runner._canonical_json_bytes(
                {key: value for key, value in tampered.items() if key != "payload_sha256"}
            )
        )
        self.assertQualificationCode(
            "PSC_QUALIFICATION_EVIDENCE_NOT_PASSING",
            runner._verify_external_evidence,
            tampered,
            specification,
        )

    def test_controller_binding_rejects_wrong_worker_hash(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            result_path = pathlib.Path(directory) / "result.json"
            result_path.write_bytes(b"worker")
            candidate = {"head": "a" * 40, "tree": "b" * 40}
            collection = {"run_key": "c" * 64, "candidate": candidate}
            shard = {"id": "shard-0001", "test_count": 1, "test_ids": ["x"]}
            result = {"status": "FAIL"}
            start_path = pathlib.Path(directory) / "start.json"
            start = runner._with_integrity(
                {
                    "kind": runner.START_KIND,
                    "run_key": collection["run_key"],
                    "candidate": candidate,
                    "shard_id": shard["id"],
                    "attempt": 1,
                    "command": ["python", "_worker"],
                }
            )
            start_path.write_bytes(runner._canonical_bytes(start))
            runner._validate_start_receipt(
                start_path,
                collection=collection,
                shard=shard,
                attempt=1,
            )
            self.assertQualificationCode(
                "PSC_QUALIFICATION_START_RECEIPT_INVALID",
                runner._validate_start_receipt,
                start_path,
                collection=collection,
                shard=shard,
                attempt=2,
            )
            controller = runner._with_integrity(
                {
                    "kind": runner.CONTROLLER_KIND,
                    "run_key": collection["run_key"],
                    "candidate": candidate,
                    "shard_id": shard["id"],
                    "attempt": 1,
                    "status": "FAIL",
                    "return_code": 2,
                    "worker_result_sha256": "0" * 64,
                }
            )
            self.assertQualificationCode(
                "PSC_QUALIFICATION_CONTROLLER_INVALID",
                runner._validate_controller,
                controller,
                collection=collection,
                shard=shard,
                attempt=1,
                result_path=result_path,
                result=result,
                require_pass=False,
            )

    def test_external_evidence_arguments_reject_duplicate_ids(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            first = pathlib.Path(directory) / "one.json"
            second = pathlib.Path(directory) / "two.json"
            first.write_text("{}", encoding="utf-8")
            second.write_text("{}", encoding="utf-8")
            self.assertQualificationCode(
                "PSC_QUALIFICATION_EVIDENCE_ARGUMENT_INVALID",
                runner._parse_external_evidence,
                [f"benchmark={first}", f"benchmark={second}"],
            )
            self.assertQualificationCode(
                "PSC_QUALIFICATION_EVIDENCE_UNREADABLE",
                runner._parse_external_evidence,
                [f"benchmark={pathlib.Path(directory) / 'missing.json'}"],
            )

    def test_secret_shaped_output_is_hashed_but_not_excerpted(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = pathlib.Path(directory) / "stdout.txt"
            path.write_text("api_key=do-not-publish-this", encoding="utf-8")
            digest = runner._output_digest(path, {})
        self.assertEqual(digest["excerpt_status"], "omitted_secret_pattern")
        self.assertIsNone(digest["excerpt"])
        self.assertRegex(digest["sha256"], r"^[0-9a-f]{64}$")


if __name__ == "__main__":
    unittest.main()
