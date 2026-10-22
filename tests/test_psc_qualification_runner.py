import copy
import inspect
import json
import pathlib
import subprocess
import sys
import tempfile
import threading
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
        self.assertEqual(selector["expected_unique_test_count"], 951)
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
        required = selector["external_evidence"][0]["required_pointer_values"]
        self.assertIs(required["/execution_policy/durable_archive_or_blob_delete"], False)
        self.assertIs(required["/execution_policy/storage_gc"], False)
        self.assertNotIn(
            "/execution_policy/filesystem_delete_or_storage_gc", required
        )
        self.assertEqual(
            set(selector["required_protected_root_labels"]),
            runner.REQUIRED_PROTECTED_ROOT_LABELS,
        )
        partitions = selector["coverage_partitions"]
        self.assertEqual(
            partitions["psc_a15_regression"]["expected_unique_test_count"], 910
        )
        self.assertEqual(
            partitions["qualification_tooling_preflight"][
                "expected_unique_test_count"
            ],
            41,
        )
        self.assertNotIn(
            "qualification_harness",
            selector["acceptance_map"]["PSC-A15"]["categories"],
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

    def test_immutable_writer_is_race_safe_and_fault_leaves_no_final(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            path = root / "race.json"
            barrier = threading.Barrier(2)
            outcomes = []

            def publish(payload):
                barrier.wait()
                try:
                    runner._write_immutable(path, payload)
                    outcomes.append("published")
                except runner.QualificationError as exc:
                    outcomes.append(exc.code)

            threads = [
                threading.Thread(target=publish, args=(b"one",)),
                threading.Thread(target=publish, args=(b"two",)),
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            self.assertEqual(outcomes.count("published"), 1)
            self.assertEqual(
                outcomes.count("PSC_QUALIFICATION_IMMUTABLE_ARTIFACT_CONFLICT"),
                1,
            )
            self.assertIn(path.read_bytes(), {b"one", b"two"})

            same_path = root / "same.json"
            same_barrier = threading.Barrier(2)
            same_errors = []

            def publish_same():
                try:
                    same_barrier.wait()
                    runner._write_immutable(same_path, b"identical")
                except BaseException as exc:
                    same_errors.append(exc)

            same_threads = [threading.Thread(target=publish_same) for _ in range(2)]
            for thread in same_threads:
                thread.start()
            for thread in same_threads:
                thread.join()
            self.assertEqual(same_errors, [])
            self.assertEqual(same_path.read_bytes(), b"identical")

            failed = root / "failed.json"
            with mock.patch.object(
                runner, "_publish_temp_no_replace", side_effect=OSError("synthetic")
            ):
                self.assertQualificationCode(
                    "PSC_QUALIFICATION_ARTIFACT_WRITE_FAILED",
                    runner._write_immutable,
                    failed,
                    b"complete",
                )
            self.assertFalse(failed.exists())
            self.assertEqual(list(root.glob(".failed.json.psc-owned-*.tmp")), [])

            truncated = root / "truncated.json"
            truncated.write_bytes(b"partial")
            self.assertQualificationCode(
                "PSC_QUALIFICATION_IMMUTABLE_ARTIFACT_CONFLICT",
                runner._write_immutable,
                truncated,
                b"complete",
            )
            self.assertEqual(truncated.read_bytes(), b"partial")

    def test_run_key_lease_rejects_concurrent_controller_process(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            run_key = "a" * 64
            script = (
                "from pathlib import Path; "
                "from tools.run_psc_qualification import _run_key_lease; "
                f"root=Path({str(root)!r}); key={run_key!r}; "
                "ctx=_run_key_lease(root,key); ctx.__enter__(); "
                "print('ready',flush=True); input(); ctx.__exit__(None,None,None)"
            )
            process = subprocess.Popen(
                [sys.executable, "-B", "-c", script],
                cwd=str(REPOSITORY_ROOT),
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            try:
                self.assertEqual(process.stdout.readline().strip(), "ready")
                self.assertQualificationCode(
                    "PSC_QUALIFICATION_RUN_KEY_BUSY",
                    lambda: runner._run_key_lease(root, run_key).__enter__(),
                )
            finally:
                process.communicate("\n", timeout=10)
            with runner._run_key_lease(root, run_key):
                pass

    def test_windows_lexical_guard_has_component_boundary(self) -> None:
        self.assertTrue(runner._lexically_within(r"d:\prompt", r"D:\Prompt"))
        self.assertTrue(
            runner._lexically_within(r"D:\Prompt\nested\item", r"D:\Prompt")
        )
        self.assertFalse(
            runner._lexically_within(r"D:\Prompt-copy\item", r"D:\Prompt")
        )
        self.assertQualificationCode(
            "PSC_QUALIFICATION_PROTECTED_ROOT_ACCESS",
            runner._reject_forbidden_path,
            pathlib.Path(r"D:\Prompt\live.json"),
            tuple(runner.BUILTIN_PROTECTED_ROOTS.values()),
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

    def test_named_protected_roots_cover_subst_alias_and_allow_workspace(self) -> None:
        mappings = {
            "C:": r"\Device\HarddiskVolumeC",
            "D:": r"\Device\HarddiskVolumeD",
            "E:": "\\??\\D:\\",
            "F:": "\\??\\C:\\state",
            "G:": r"\Device\HarddiskVolumeG",
            "H:": r"\Device\HarddiskVolumeH",
            "I:": r"\Device\HarddiskVolumeI",
            "J:": r"\Device\HarddiskVolumeJ",
        }

        def query(drive):
            return mappings[drive.upper()]

        roots = runner._parse_protected_roots(
            [
                r"service_root=C:\service",
                r"service_state=C:\state",
                r"service_runtime=C:\state\runtime",
                r"service_prompt=C:\state\prompt",
            ],
            device_query=query,
        )
        runner._validate_path_isolation(
            pathlib.Path(r"C:\qualification"),
            pathlib.Path(r"C:\service\managed-workspace"),
            roots,
            device_query=query,
            inspect_existing=False,
        )
        state_workspace = pathlib.Path(
            r"C:\state\managed\projects\9000\sprints\sprint-1\nodes\qualification"
        )
        runner._validate_path_isolation(
            pathlib.Path(r"C:\qualification"),
            state_workspace,
            roots,
            device_query=query,
            inspect_existing=False,
        )
        self.assertQualificationCode(
            "PSC_QUALIFICATION_RUN_ROOT_OVERLAP",
            runner._validate_path_isolation,
            pathlib.Path(r"C:\state\qualification-run"),
            state_workspace,
            roots,
            device_query=query,
            inspect_existing=False,
        )
        state_guard = runner.QualificationAccessGuard(
            protected_roots=roots,
            allowed_roots=(state_workspace,),
            device_query=query,
        )
        ordinary = types.SimpleNamespace(
            st_mode=runner.stat.S_IFDIR, st_file_attributes=0
        )
        state_guard._original_lstat = lambda _path: ordinary
        state_guard.check("open", state_workspace / "test.py")
        with self.assertRaises(PermissionError):
            state_guard.check("open", r"C:\state\managed\sibling\state.json")
        with self.assertRaises(PermissionError):
            state_guard.check("open", r"F:\managed\sibling\state.json")
        self.assertEqual(
            [item["root_label"] for item in state_guard.protected_attempts],
            ["service_state", "service_state"],
        )
        guard = runner.QualificationAccessGuard(
            protected_roots=roots,
            allowed_roots=(pathlib.Path(r"C:\service\managed-workspace"),),
            device_query=query,
            long_path_query=lambda path: path,
        )
        guard._original_lstat = lambda _path: ordinary
        guard.check("open", runner.os.devnull)
        guard.check("open", r"C:\service\managed-workspace\test.py")
        guard.check(
            "io.open",
            r"C:\Python312\Lib\site-packages\~-p.dist-info\entry_points.txt",
        )
        with self.assertRaises(PermissionError):
            guard.check("open", r"C:\service\sibling\state.json")
        with self.assertRaises(PermissionError):
            guard.check("open", r"E:\Prompt\latest-response.json")
        with self.assertRaises(PermissionError):
            guard.check(
                "open",
                r"C:\service\managed-workspace\WORKSP~1\secret.json",
            )
        redirected = types.SimpleNamespace(
            st_mode=runner.stat.S_IFDIR, st_file_attributes=0x400
        )

        def lstat_with_junction(path):
            return (
                redirected
                if str(path).casefold().endswith(r"\redirect")
                else ordinary
            )

        guard._original_lstat = lstat_with_junction
        with self.assertRaises(PermissionError):
            guard.check(
                "open", r"C:\service\managed-workspace\redirect\secret.json"
            )
        with self.assertRaises(PermissionError):
            guard.check("open", r"C:\external\redirect\secret.json")
        self.assertEqual(
            {item["root_label"] for item in guard.protected_attempts},
            {"service_root", "live_prompt"},
        )
        self.assertEqual(
            guard.unsafe_alias_attempts,
            [{"event": "open"}, {"event": "open"}, {"event": "open"}],
        )
        self.assertQualificationCode(
            "PSC_QUALIFICATION_PATH_ALIAS_UNSUPPORTED",
            runner._strict_local_windows_path,
            r"\\?\D:\Prompt",
        )

        disjoint_roots = runner._parse_protected_roots(
            [
                r"service_root=G:\service",
                r"service_state=H:\state",
                r"service_runtime=I:\runtime",
                r"service_prompt=J:\prompt",
            ],
            device_query=query,
        )
        for exact_root in (pathlib.Path(r"G:\service"), pathlib.Path(r"H:\state")):
            self.assertQualificationCode(
                "PSC_QUALIFICATION_WORKSPACE_OVERLAP",
                runner._validate_path_isolation,
                pathlib.Path(r"C:\qualification"),
                exact_root,
                disjoint_roots,
                device_query=query,
                inspect_existing=False,
            )
        for prohibited_subtree in (
            pathlib.Path(r"I:\runtime\workspace"),
            pathlib.Path(r"J:\prompt\workspace"),
            pathlib.Path(r"D:\Prompt\workspace"),
        ):
            self.assertQualificationCode(
                "PSC_QUALIFICATION_WORKSPACE_OVERLAP",
                runner._validate_path_isolation,
                pathlib.Path(r"C:\qualification"),
                prohibited_subtree,
                disjoint_roots,
                device_query=query,
                inspect_existing=False,
            )
        runner._validate_path_isolation(
            pathlib.Path(r"C:\qualification"),
            pathlib.Path(r"H:\state\managed-workspace"),
            disjoint_roots,
            device_query=query,
            inspect_existing=False,
        )

        alias_guard = runner.QualificationAccessGuard(
            protected_roots=disjoint_roots,
            allowed_roots=(pathlib.Path(r"G:\service\managed-workspace"),),
            device_query=query,
            long_path_query=lambda _path: r"C:\Program Files",
        )
        alias_guard._original_lstat = lambda _path: ordinary
        with self.assertRaises(PermissionError):
            alias_guard.check("open", r"C:\PROGRA~1")
        self.assertEqual(alias_guard.unsafe_alias_attempts, [{"event": "open"}])

        collect_source = inspect.getsource(runner._collect_command)
        self.assertLess(
            collect_source.index("guard.install()"),
            collect_source.index("toolchain = _toolchain()"),
        )

    def test_existing_reparse_chain_is_rejected(self) -> None:
        ordinary = types.SimpleNamespace(
            st_mode=runner.stat.S_IFDIR, st_file_attributes=0
        )
        redirected = types.SimpleNamespace(
            st_mode=runner.stat.S_IFDIR, st_file_attributes=0x400
        )
        self.assertQualificationCode(
            "PSC_QUALIFICATION_PATH_REDIRECTED",
            runner._assert_existing_chain_not_redirected,
            pathlib.Path(r"C:\safe\path"),
            lstat_function=mock.Mock(side_effect=[ordinary, redirected]),
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
            "guard_evidence": {
                "protected_root_attempts": [],
                "unsafe_alias_attempts": [],
                "network_attempts": [],
                "guard_checks_by_event": {},
                "descriptor_checks_by_event": {},
                "native_path_checks_by_entrypoint": {},
                "native_guard_entrypoints": [],
            },
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
        specification = runner._load_selector(SELECTOR_PATH)["external_evidence"][0]
        evidence = {
            "schema_version": 2,
            "kind": "psc_a05_a16_a17_exact_head_benchmark_evidence",
            "candidate": {"commit": "a" * 40, "tree": "b" * 40, "clean": True},
            "acceptance": {
                "overall_pass": True,
                "PSC-A05": {"pass": True},
                "PSC-A16": {"pass": True},
                "PSC-A17": {"pass": True},
            },
            "execution_policy": {
                "generated_fixture_only": True,
                "sequential": True,
                "durable_archive_or_blob_delete": False,
                "storage_gc": False,
                "services_called_or_restarted": False,
                "production_authoritative_mode_enabled": False,
                "isolated_authoritative_mode_only": True,
            },
            "restrictions": {
                "live_data_read_or_written": False,
                "runtime_state_or_sqlite_mutated": False,
                "service_restart": False,
                "merge_or_deploy": False,
                "start_from_git_or_sprint_import": False,
                "generated_run_root_deleted": False,
            },
        }
        evidence["payload_sha256"] = runner._sha256(
            runner._canonical_json_bytes(evidence)
        )
        runner._verify_external_evidence(evidence, specification)
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            workspace = root / "workspace"
            workspace.mkdir()
            run_root = root / "run"
            attempt_root = run_root / "attempts" / "shard-0001"
            candidate = {
                "head": "a" * 40,
                "tree": "b" * 40,
                "branch": "synthetic",
                "clean": True,
                "status_sha256": runner._sha256(b""),
                "status_line_count": 0,
            }
            test_id = "tests.synthetic.Case.test_ok"
            shard = {
                "id": "shard-0001",
                "test_count": 1,
                "test_ids": [test_id],
            }
            empty_guard = {
                "protected_root_attempts": [],
                "unsafe_alias_attempts": [],
                "network_attempts": [],
            }
            collection = {
                "suite_id": "synthetic",
                "run_key": "c" * 64,
                "candidate": candidate,
                "selection": {"unique_test_count": 1},
                "toolchain": {},
                "selector": {
                    "external_evidence": [specification],
                    "acceptance_map": {},
                    "singleton_modules": [],
                    "priority_test_ids": [],
                },
                "environment": {"collection_guard_evidence": empty_guard},
            }
            plan = {
                "shards": [shard],
                "shard_count": 1,
                "unique_test_count": 1,
                "sequential": True,
            }
            lease = {"owner_id": "d" * 32, "mechanism": "msvcrt_byte_range"}
            runner._write_json(
                attempt_root / "attempt-0001.start.json",
                {
                    "kind": runner.START_KIND,
                    "run_key": collection["run_key"],
                    "candidate": candidate,
                    "shard_id": shard["id"],
                    "attempt": 1,
                    "command": ["python", "_worker"],
                    "lease_owner_id": lease["owner_id"],
                    "lease_mechanism": lease["mechanism"],
                },
            )
            outcomes = {
                "tests_run": 1,
                "parent": {
                    name: 1 if name == "success" else 0
                    for name in (
                        "success", "failure", "error", "skip", "expected_failure",
                        "unexpected_success", "subtest_failure", "subtest_error",
                        "subtest_skip", "not_run",
                    )
                },
                "subtests": {
                    "success": 0, "failure": 0, "error": 0, "skip": 0, "total": 0
                },
                "fixture_error_count": 0,
                "unexpected_parent_id_count": 0,
            }
            result_path = attempt_root / "attempt-0001.result.json"
            runner._write_json(
                result_path,
                {
                    "kind": runner.WORKER_KIND,
                    "run_key": collection["run_key"],
                    "candidate": candidate,
                    "shard": shard,
                    "status": "PASS",
                    "duration_ns": 1,
                    "loader": {
                        "requested_ids_equal_loaded_ids": True,
                        "loaded_test_count": 1,
                        "error_count": 0,
                        "error_hashes": [],
                    },
                    "outcomes": outcomes,
                    "tests": [{"id": test_id, "outcome": "success", "subtests": []}],
                    "fixture_errors": [],
                    "unexpected_parent_id_count": 0,
                    "guard_evidence": empty_guard,
                    "execution_exception": None,
                    "git_before": candidate,
                    "git_after": candidate,
                    "captured_output": {},
                },
            )
            runner._write_json(
                attempt_root / "attempt-0001.controller.json",
                {
                    "kind": runner.CONTROLLER_KIND,
                    "run_key": collection["run_key"],
                    "candidate": candidate,
                    "shard_id": shard["id"],
                    "attempt": 1,
                    "status": "PASS",
                    "return_code": 0,
                    "worker_result_sha256": runner._sha256(result_path.read_bytes()),
                    "lease_owner_id": lease["owner_id"],
                    "lease_mechanism": lease["mechanism"],
                },
            )
            evidence_path = root / "benchmark.json"
            evidence_path.write_bytes(runner._canonical_json_bytes(evidence))
            args = types.SimpleNamespace(
                external_evidence=[f"exact_head_benchmark={evidence_path}"],
                output=str(run_root / "ledger.json"),
            )
            with mock.patch.object(
                runner, "_assert_git_binding", return_value=candidate
            ):
                finalized = runner._finalize_under_lease(
                    args,
                    run_root=run_root,
                    workspace=workspace,
                    protected_roots=(),
                    collection=collection,
                    plan=plan,
                    lease=lease,
                )
            self.assertEqual(finalized["status"], "PASS")
            ledger = runner._read_json(run_root / "ledger.json")
            self.assertEqual(
                ledger["external_evidence"][0]["id"], "exact_head_benchmark"
            )
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

    def test_committed_selector_accepts_sealed_benchmark_shaped_evidence(self) -> None:
        specification = runner._load_selector(SELECTOR_PATH)["external_evidence"][0]
        evidence = {
            "schema_version": 2,
            "kind": "psc_a05_a16_a17_exact_head_benchmark_evidence",
            "candidate": {"commit": "a" * 40, "tree": "b" * 40, "clean": True},
            "acceptance": {
                "overall_pass": True,
                "PSC-A05": {"pass": True},
                "PSC-A16": {"pass": True},
                "PSC-A17": {"pass": True},
            },
            "execution_policy": {
                "generated_fixture_only": True,
                "sequential": True,
                "durable_archive_or_blob_delete": False,
                "storage_gc": False,
                "services_called_or_restarted": False,
                "production_authoritative_mode_enabled": False,
                "isolated_authoritative_mode_only": True,
            },
            "restrictions": {
                "live_data_read_or_written": False,
                "runtime_state_or_sqlite_mutated": False,
                "service_restart": False,
                "merge_or_deploy": False,
                "start_from_git_or_sprint_import": False,
                "generated_run_root_deleted": False,
            },
        }
        evidence["payload_sha256"] = runner._sha256(
            runner._canonical_json_bytes(evidence)
        )
        runner._verify_external_evidence(evidence, specification)

    def test_start_only_attempt_requires_explicit_abandonment(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            attempt_root = pathlib.Path(directory)
            candidate = {"head": "a" * 40, "tree": "b" * 40}
            collection = {"run_key": "c" * 64, "candidate": candidate}
            shard = {"id": "shard-0001", "test_count": 1, "test_ids": ["x"]}
            lease = {"owner_id": "d" * 32, "mechanism": "msvcrt_byte_range"}
            runner._write_json(
                attempt_root / "attempt-0001.start.json",
                {
                    "kind": runner.START_KIND,
                    "run_key": collection["run_key"],
                    "candidate": candidate,
                    "shard_id": shard["id"],
                    "attempt": 1,
                    "command": ["python", "_worker"],
                    "lease_owner_id": lease["owner_id"],
                    "lease_mechanism": lease["mechanism"],
                },
            )
            history = runner._inspect_attempt_history(
                attempt_root, collection=collection, shard=shard
            )
            self.assertEqual(history["unsettled"], [1])
            runner._write_abandonment(
                attempt_root,
                collection=collection,
                shard=shard,
                attempt=1,
                lease=lease,
                reason="controller_interrupted",
            )
            history = runner._inspect_attempt_history(
                attempt_root, collection=collection, shard=shard
            )
            self.assertEqual(history["unsettled"], [])
            self.assertEqual(history["abandoned"][0]["attempt"], 1)

    def test_higher_start_only_attempt_cannot_be_silently_finalized(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            attempt_root = pathlib.Path(directory)
            candidate = {"head": "a" * 40, "tree": "b" * 40}
            collection = {"run_key": "c" * 64, "candidate": candidate}
            test_id = "tests.synthetic.Case.test_ok"
            shard = {"id": "shard-0001", "test_count": 1, "test_ids": [test_id]}
            lease = {"owner_id": "d" * 32, "mechanism": "msvcrt_byte_range"}
            for attempt in (1, 2):
                runner._write_json(
                    attempt_root / f"attempt-{attempt:04d}.start.json",
                    {
                        "kind": runner.START_KIND,
                        "run_key": collection["run_key"],
                        "candidate": candidate,
                        "shard_id": shard["id"],
                        "attempt": attempt,
                        "command": ["python", "_worker"],
                        "lease_owner_id": lease["owner_id"],
                        "lease_mechanism": lease["mechanism"],
                    },
                )
            outcomes = {
                "tests_run": 1,
                "parent": {
                    name: 1 if name == "success" else 0
                    for name in (
                        "success", "failure", "error", "skip", "expected_failure",
                        "unexpected_success", "subtest_failure", "subtest_error",
                        "subtest_skip", "not_run",
                    )
                },
                "subtests": {
                    "success": 0, "failure": 0, "error": 0, "skip": 0, "total": 0
                },
                "fixture_error_count": 0,
                "unexpected_parent_id_count": 0,
            }
            result_path = attempt_root / "attempt-0001.result.json"
            runner._write_json(
                result_path,
                {
                    "kind": runner.WORKER_KIND,
                    "run_key": collection["run_key"],
                    "candidate": candidate,
                    "shard": shard,
                    "status": "PASS",
                    "loader": {
                        "requested_ids_equal_loaded_ids": True,
                        "loaded_test_count": 1,
                        "error_count": 0,
                        "error_hashes": [],
                    },
                    "outcomes": outcomes,
                    "tests": [{"id": test_id, "outcome": "success", "subtests": []}],
                    "fixture_errors": [],
                    "unexpected_parent_id_count": 0,
                    "guard_evidence": {
                        "protected_root_attempts": [],
                        "unsafe_alias_attempts": [],
                        "network_attempts": [],
                    },
                    "execution_exception": None,
                    "git_before": candidate,
                    "git_after": candidate,
                },
            )
            runner._write_json(
                attempt_root / "attempt-0001.controller.json",
                {
                    "kind": runner.CONTROLLER_KIND,
                    "run_key": collection["run_key"],
                    "candidate": candidate,
                    "shard_id": shard["id"],
                    "attempt": 1,
                    "status": "PASS",
                    "return_code": 0,
                    "worker_result_sha256": runner._sha256(result_path.read_bytes()),
                    "lease_owner_id": lease["owner_id"],
                    "lease_mechanism": lease["mechanism"],
                },
            )
            history = runner._inspect_attempt_history(
                attempt_root, collection=collection, shard=shard
            )
            self.assertEqual(len(history["completed"]), 1)
            self.assertEqual(history["unsettled"], [2])

    def test_restrictions_are_derived_and_disclose_unmeasured_claims(self) -> None:
        empty_guard = {
            "protected_root_attempts": [],
            "unsafe_alias_attempts": [],
            "network_attempts": [],
        }
        restrictions = runner._derive_restrictions(
            {"environment": {"collection_guard_evidence": empty_guard}},
            [{"guard_evidence": empty_guard}],
            {"sequential": True},
            {"mechanism": "msvcrt_byte_range"},
        )
        self.assertEqual(
            restrictions["protected_root_access"]["status"],
            "enforced_and_observed_zero",
        )
        self.assertIn("external_api_mutation", restrictions["claims_not_made"])
        self.assertNotIn("api_or_service_mutation", restrictions)

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
                    "lease_owner_id": "d" * 32,
                    "lease_mechanism": "msvcrt_byte_range",
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
                    "lease_owner_id": "d" * 32,
                    "lease_mechanism": "msvcrt_byte_range",
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
            with mock.patch.object(
                runner,
                "_assert_existing_chain_not_redirected",
                side_effect=runner.QualificationError(
                    "PSC_QUALIFICATION_PATH_REDIRECTED"
                ),
            ):
                self.assertQualificationCode(
                    "PSC_QUALIFICATION_PATH_REDIRECTED",
                    runner._parse_external_evidence,
                    [f"benchmark={first}"],
                )

    def test_secret_shaped_output_is_streamed_and_never_persisted_raw(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            marker = "do-not-publish-this"
            digests = []
            for text in (
                f"prefix api_key={marker} suffix",
                f'{{"api_key":"{marker}"}}',
                f"{{'Authorization': 'Bearer {marker}'}}",
            ):
                capture = runner._BoundedOutputCapture()
                midpoint = len(text) // 2
                capture.write(text[:midpoint])
                capture.write(text[midpoint:])
                digests.append(capture.snapshot({}))
            bounded = runner._BoundedOutputCapture()
            bounded.write("x" * (1024 * 1024))
            bounded_digest = bounded.snapshot({})
            return_code, _duration, child_stdout, child_stderr = (
                runner._run_process_captured(
                    [
                        sys.executable,
                        "-c",
                        f"import sys; print('token={marker}'); "
                        f"print('password={marker}', file=sys.stderr)",
                    ],
                    cwd=root,
                    environment=dict(runner.os.environ),
                    replacements={},
                )
            )
            self.assertEqual(return_code, 0)
            runner._write_json(
                root / "artifact.json",
                {
                    "captured": digests,
                    "child_stdout": child_stdout,
                    "child_stderr": child_stderr,
                },
            )
            persisted = b"".join(
                path.read_bytes() for path in root.rglob("*") if path.is_file()
            )
        for digest in digests:
            self.assertIsNone(digest["excerpt"])
            self.assertRegex(digest["sha256"], r"^[0-9a-f]{64}$")
        self.assertNotIn(marker.encode("utf-8"), persisted)
        self.assertEqual(bounded_digest["byte_size"], 1024 * 1024)
        self.assertEqual(bounded_digest["excerpt_status"], "omitted_by_policy")
        self.assertIsNone(bounded_digest["excerpt"])


if __name__ == "__main__":
    unittest.main()
