import base64
import contextlib
import copy
import hashlib
import io
import json
import subprocess
import sys
import textwrap
import unittest
from pathlib import Path
from unittest import mock

import tools.run_psc_exact_head_benchmark as benchmark
from tools.run_psc_exact_head_benchmark import (
    _AccessGuard,
    BenchmarkFailure,
    FROZEN_INPUTS,
    FROZEN_WORKLOAD_MANIFEST_SHA256,
    PREDECESSOR_DECODED_BYTES,
    PREDECESSOR_DECODED_SHA256,
    PREDECESSOR_EVIDENCE_PATH,
    REPOSITORY_ROOT,
    _canonical_json,
    _decode_predecessor_evidence,
    _device_lexical_windows_path,
    _lexical_windows_path,
    _preflight_path_isolation,
    _sha256,
    _strict_local_windows_path,
    _under,
    audit_path_arguments,
    evaluate_a05_append_bounds,
    frozen_workload,
    frozen_workload_manifest,
    full_inventory_equal,
    main,
    parse_forbidden_roots,
    paths_overlap,
    seal_evidence,
    validate_frozen_workload,
    verify_sealed_evidence_bytes,
)


class PscExactHeadBenchmarkTests(unittest.TestCase):
    def test_frozen_workload_matches_committed_predecessor_lengths_and_hashes(self) -> None:
        validate_frozen_workload()
        observed = {
            item["label"]: (len(item["raw"]), _sha256(item["raw"]))
            for item in frozen_workload()
        }
        self.assertEqual(observed, FROZEN_INPUTS)
        self.assertEqual(
            [item["record_count"] for item in frozen_workload()],
            [65, 66, 67, 68, 65, 66, 65],
        )
        self.assertEqual(
            _sha256(_canonical_json(frozen_workload_manifest())),
            FROZEN_WORKLOAD_MANIFEST_SHA256,
        )
        benchmark.validate_delta_gap_workload()
        delta = benchmark.delta_gap_workload()
        self.assertEqual([item["record_count"] for item in delta], [8, 9, 10, 11])
        self.assertEqual(
            [item["alias_location"] for item in delta],
            ["project_state", "project_state", "assignment", "assignment"],
        )
        self.assertEqual(
            {
                item["label"]: (len(item["raw"]), _sha256(item["raw"]))
                for item in delta
            },
            benchmark.DELTA_GAP_FROZEN_INPUTS,
        )
        self.assertEqual(
            _sha256(_canonical_json(benchmark.delta_gap_workload_manifest())),
            benchmark.DELTA_GAP_WORKLOAD_MANIFEST_SHA256,
        )
        self.assertFalse(
            benchmark.delta_gap_workload_manifest()["middle_insert_delete_included"]
        )

    def test_committed_predecessor_carrier_is_exact_and_has_same_workload(self) -> None:
        self.assertEqual(
            benchmark.READ_ONLY_GIT_COMMAND_DISCLOSURE,
            (
                "branch --show-current",
                "ls-files --others --directory --no-empty-directory",
                "merge-base --is-ancestor",
                "rev-parse",
                "show",
                (
                    "status --porcelain=v1 --untracked-files=no "
                    "--ignore-submodules=all"
                ),
            ),
        )
        self.assertEqual(
            benchmark.FORBIDDEN_GIT_METADATA_CONTROLS,
            (
                ".gitmodules",
                ".git/config.worktree",
                ".git/info/grafts",
                ".git/objects/info/alternates",
                ".git/objects/info/http-alternates",
                ".git/shallow",
            ),
        )
        document = _decode_predecessor_evidence()
        carrier = (REPOSITORY_ROOT / PREDECESSOR_EVIDENCE_PATH).read_bytes()
        decoded = base64.b64decode(b"".join(carrier.split()), validate=True)
        self.assertEqual(len(decoded), PREDECESSOR_DECODED_BYTES)
        self.assertEqual(hashlib.sha256(decoded).hexdigest(), PREDECESSOR_DECODED_SHA256)
        self.assertEqual(document["kind"], "psc_a05_a16_a17_isolated_qualification_evidence")
        context = benchmark._validate_delta_baseline(benchmark._git("rev-parse", "HEAD"))
        self.assertTrue(context["runtime_is_ancestor_of_carrier"])
        self.assertTrue(context["carrier_is_ancestor_of_candidate"])
        self.assertEqual(
            context["raw_carrier_byte_size"], benchmark.PREDECESSOR_CARRIER_BYTES
        )
        with mock.patch.object(benchmark, "_git_is_ancestor", return_value=False):
            with self.assertRaisesRegex(
                BenchmarkFailure, "BENCHMARK_BASELINE_ANCESTRY_INVALID"
            ):
                benchmark._validate_delta_baseline(benchmark._git("rev-parse", "HEAD"))
        self.assertTrue(
            benchmark._validate_repository_git_layout()[
                "ordinary_in_worktree_git_directory"
            ]
        )
        text_result = mock.Mock(returncode=0, stdout="value\n")
        bytes_result = mock.Mock(returncode=0, stdout=b"value\n")
        with mock.patch.object(
            benchmark.subprocess,
            "run",
            side_effect=(text_result, bytes_result, bytes_result),
        ) as run:
            self.assertEqual(benchmark._git("rev-parse", "HEAD"), "value")
            self.assertEqual(benchmark._git_bytes("show", "HEAD:" + PREDECESSOR_EVIDENCE_PATH), b"value\n")
            self.assertTrue(benchmark._git_is_ancestor("a" * 40, "b" * 40))
        for call in run.call_args_list:
            command = call.args[0]
            environment = call.kwargs["env"]
            self.assertIn("--no-optional-locks", command)
            self.assertIn("--no-replace-objects", command)
            self.assertIn("core.fsmonitor=false", command)
            self.assertIn("core.untrackedCache=false", command)
            self.assertIn("maintenance.auto=false", command)
            self.assertEqual(environment["GIT_OPTIONAL_LOCKS"], "0")
            self.assertEqual(environment["GIT_CONFIG_NOSYSTEM"], "1")
            self.assertEqual(environment["GIT_NO_LAZY_FETCH"], "1")
            self.assertEqual(environment["GIT_NO_REPLACE_OBJECTS"], "1")
            self.assertEqual(environment["GIT_TERMINAL_PROMPT"], "0")
        with self.assertRaisesRegex(
            BenchmarkFailure, "BENCHMARK_GIT_COMMAND_NOT_ALLOWED"
        ):
            benchmark._read_only_git_invocation("branch", "-D", "unsafe")
        regular_file = mock.Mock(st_mode=benchmark.stat.S_IFREG, st_file_attributes=0)
        with mock.patch.object(benchmark, "_ORIGINAL_LSTAT", return_value=regular_file):
            with self.assertRaisesRegex(
                BenchmarkFailure, "BENCHMARK_GIT_LAYOUT_INVALID"
            ):
                benchmark._validate_repository_git_layout()

    def test_windows_lexical_deny_matching_has_component_boundary(self) -> None:
        root = _lexical_windows_path(r"D:\Prompt")
        self.assertTrue(_under(_lexical_windows_path(r"d:\prompt"), root))
        self.assertTrue(_under(_lexical_windows_path(r"D:\Prompt\nested\x"), root))
        self.assertFalse(_under(_lexical_windows_path(r"D:\Prompt-copy\x"), root))
        self.assertTrue(paths_overlap(r"C:\isolated", r"c:\isolated\child"))
        self.assertFalse(paths_overlap(r"C:\isolated", r"C:\isolated-other"))

    def test_windows_alias_forms_are_rejected_before_any_probe(self) -> None:
        for value in (
            r"\\localhost\d$\Prompt\bench",
            r"\\?\D:\Prompt\bench",
            r"\\.\D:\Prompt\bench",
            r"\Prompt\bench",
            r"C:\PROGRA~1\bench",
            r"C:\safe\trailing.\bench",
            r"C:\safe\file:stream",
        ):
            with self.subTest(value=value), self.assertRaises(BenchmarkFailure):
                _strict_local_windows_path(value)

    def test_device_mapping_collapses_subst_and_guard_denies_equivalent_root(self) -> None:
        mappings = {
            "C:": r"\Device\HarddiskVolume1",
            "D:": r"\Device\HarddiskVolume2",
            "S:": r"\??\D:\Prompt",
        }

        def query(drive: str) -> str:
            return mappings[drive.upper()]

        direct = _device_lexical_windows_path(
            r"D:\Prompt\nested", device_query=query
        )
        subst = _device_lexical_windows_path(r"S:\nested", device_query=query)
        self.assertEqual(subst, direct)
        forbidden = ({
            "label": "live_prompt",
            "lexical_path": _strict_local_windows_path(r"D:\Prompt"),
            "device_path": _device_lexical_windows_path(
                r"D:\Prompt", device_query=query
            ),
        },)
        audit = {
            "phase": "benchmark",
            "forbidden_attempts": [],
            "unsafe_alias_attempts": [],
            "protected_fixture_benchmark_attempts": [],
            "authorized_fixture_inventory_events": 0,
            "guard_checks_by_event": {},
            "descriptor_checks_by_event": {},
            "native_path_checks_by_entrypoint": {},
            "native_guard_entrypoints": [],
            "write_telemetry": {"active_transition": None, "by_transition": {}},
        }
        guard = _AccessGuard(
            forbidden=forbidden,
            protected=Path(r"C:\isolated\protected"),
            audit=audit,
            device_query=query,
        )
        with self.assertRaises(PermissionError):
            guard.check("os.stat", r"S:\nested")
        self.assertEqual(
            audit["forbidden_attempts"],
            [{"event": "os.stat", "root_label": "live_prompt"}],
        )

    def test_managed_repository_subtree_exemption_denies_sibling_and_subst(self) -> None:
        mappings = {
            "C:": r"\Device\HarddiskVolume1",
            "D:": r"\Device\HarddiskVolume2",
            "T:": r"\??\C:\service-state",
        }

        def query(drive: str) -> str:
            return mappings[drive.upper()]

        state_root = {
            "label": "service_state",
            "lexical_path": _strict_local_windows_path(r"C:\service-state"),
            "device_path": _device_lexical_windows_path(
                r"C:\service-state", device_query=query
            ),
        }
        audit = {
            "phase": "benchmark",
            "forbidden_attempts": [],
            "unsafe_alias_attempts": [],
            "protected_fixture_benchmark_attempts": [],
            "authorized_fixture_inventory_events": 0,
            "guard_checks_by_event": {},
            "descriptor_checks_by_event": {},
            "native_path_checks_by_entrypoint": {},
            "native_guard_entrypoints": [],
            "write_telemetry": {"active_transition": None, "by_transition": {}},
        }
        guard = _AccessGuard(
            forbidden=(state_root,),
            protected=Path(r"D:\isolated\protected"),
            allowed_roots=(Path(r"C:\service-state\managed\workspace"),),
            audit=audit,
            device_query=query,
        )

        guard.check("os.stat", r"C:\service-state\managed\workspace\file.py")
        with self.assertRaises(PermissionError):
            guard.check("os.truncate", 987654321, write_intent=True)
        for denied in (
            r"C:\service-state\managed\sibling\file.py",
            r"T:\managed\workspace\file.py",
            r"C:\service-state",
        ):
            with self.subTest(path=denied), self.assertRaises(PermissionError):
                guard.check("os.stat", denied)
        for event, denied, write_intent in (
            (
                "os.remove",
                r"C:\service-state\managed\workspace\tracked.py",
                None,
            ),
            (
                "open",
                r"C:\service-state\managed\workspace\.git\config",
                False,
            ),
            (
                "os.open",
                r"C:\service-state\managed\workspace\tracked.py",
                True,
            ),
        ):
            with self.subTest(event=event), self.assertRaises(PermissionError):
                guard.check(event, denied, write_intent=write_intent)
        self.assertEqual(
            audit["forbidden_attempts"],
            [
                {"event": "os.stat", "root_label": "service_state"},
                {"event": "os.stat", "root_label": "service_state"},
                {"event": "os.stat", "root_label": "service_state"},
                {"event": "os.remove", "root_label": "service_state"},
                {"event": "open", "root_label": "service_state"},
                {"event": "os.open", "root_label": "service_state"},
            ],
        )

        ordinary = mock.Mock(
            st_mode=benchmark.stat.S_IFDIR,
            st_file_attributes=0,
        )
        redirected = mock.Mock(
            st_mode=benchmark.stat.S_IFDIR,
            st_file_attributes=0x400,
        )

        def lstat(path: Path):
            return redirected if str(path).casefold().endswith("\\junction") else ordinary

        reparse_guard = _AccessGuard(
            forbidden=(state_root,),
            protected=Path(r"D:\isolated\protected"),
            allowed_roots=(Path(r"C:\service-state\managed\workspace"),),
            audit=audit,
            lstat_function=lstat,
            device_query=query,
        )
        with self.assertRaises(PermissionError):
            reparse_guard.check(
                "os.stat",
                r"C:\service-state\managed\workspace\junction\outside.json",
            )
        self.assertEqual(
            audit["unsafe_alias_attempts"],
            [{"event": "os.truncate"}, {"event": "os.stat"}],
        )

    def test_preflight_rejects_repository_subst_equivalent_to_forbidden_root(self) -> None:
        mappings = {
            "C:": r"\Device\HarddiskVolume1",
            "D:": r"\Device\HarddiskVolume2",
            "S:": r"\??\D:\Prompt",
        }

        def query(drive: str) -> str:
            return mappings[drive.upper()]

        forbidden = ({
            "label": "live_prompt",
            "lexical_path": _strict_local_windows_path(r"D:\Prompt"),
            "device_path": _device_lexical_windows_path(
                r"D:\Prompt", device_query=query
            ),
        },)
        with mock.patch.object(benchmark, "REPOSITORY_ROOT", Path(r"S:\repo")):
            with self.assertRaisesRegex(
                BenchmarkFailure, "BENCHMARK_REPOSITORY_OVERLAPS_FORBIDDEN"
            ):
                _preflight_path_isolation(
                    Path(r"C:\isolated-benchmark"),
                    forbidden,
                    device_query=query,
                )

    def test_preflight_allows_only_strict_managed_repository_parent(self) -> None:
        mappings = {
            "C:": r"\Device\HarddiskVolume1",
            "D:": r"\Device\HarddiskVolume2",
            "T:": r"\??\C:\service-state",
        }

        def query(drive: str) -> str:
            return mappings[drive.upper()]

        state = ({
            "label": "service_state",
            "lexical_path": _strict_local_windows_path(r"C:\service-state"),
            "device_path": _device_lexical_windows_path(
                r"C:\service-state", device_query=query
            ),
        },)
        with mock.patch.object(
            benchmark,
            "REPOSITORY_ROOT",
            Path(r"C:\service-state\managed\workspace"),
        ):
            isolation = _preflight_path_isolation(
                Path(r"D:\isolated-benchmark"), state, device_query=query
            )
        self.assertEqual(isolation["repository_parent_labels"], ("service_state",))

        denied_cases = (
            ("service_state", Path(r"C:\service-state")),
            ("service_root", Path(r"C:\service-state\managed\workspace")),
            ("service_runtime", Path(r"C:\service-state\managed\workspace")),
            ("service_state", Path(r"T:\managed\workspace")),
        )
        for label, repository in denied_cases:
            denied = ({**state[0], "label": label},)
            with self.subTest(label=label, repository=repository), mock.patch.object(
                benchmark, "REPOSITORY_ROOT", repository
            ), self.assertRaisesRegex(
                BenchmarkFailure, "BENCHMARK_REPOSITORY_OVERLAPS_FORBIDDEN"
            ):
                _preflight_path_isolation(
                    Path(r"D:\isolated-benchmark"), denied, device_query=query
                )

    def test_repository_parent_chain_reparse_still_fails_closed(self) -> None:
        ordinary = mock.Mock(
            st_mode=benchmark.stat.S_IFDIR,
            st_file_attributes=0,
        )
        redirected = mock.Mock(
            st_mode=benchmark.stat.S_IFDIR,
            st_file_attributes=0x400,
        )
        metadata = iter((ordinary, redirected))
        with self.assertRaisesRegex(
            BenchmarkFailure, "BENCHMARK_RUN_ROOT_REDIRECTED"
        ):
            benchmark._assert_existing_chain_not_redirected(
                Path(r"C:\service-state\managed\workspace"),
                lstat_function=lambda _path: next(metadata),
            )

    def test_audit_event_extraction_ignores_utime_metadata(self) -> None:
        metadata = (1_000, 2_000)
        self.assertEqual(
            audit_path_arguments("os.utime", (r"C:\fixture\file", None, metadata, -1)),
            (r"C:\fixture\file",),
        )
        self.assertEqual(
            audit_path_arguments("os.rename", ("source", "target", -1, -1)),
            ("source", "target"),
        )

    def test_a05_quantitative_bound_rejects_o_n_append(self) -> None:
        def record(
            attempts: int,
            cas_bytes: int,
            footprint: int,
            *,
            allocation: int | None = None,
            write_payload: int | None = None,
        ) -> dict:
            return {
                "logical_change_bytes": 4_868,
                "cas": {
                    "put_attempts": attempts,
                    "new_stored_bytes": cas_bytes,
                },
                "physical": {
                    "final_touched_content_footprint_bytes": footprint,
                    "final_touched_allocation_footprint_bytes": (
                        allocation if allocation is not None else footprint * 8
                    ),
                },
                "write_api": {
                    "write_calls": 10,
                    "payload_bytes": (
                        write_payload if write_payload is not None else footprint * 2
                    ),
                },
            }

        initial = record(18_284, 3_401_058, 3_528_673)
        append = [
            record(315, 80_502, 207_922),
            record(587, 83_550, 211_240),
            record(315, 80_887, 208_307),
        ]
        middle = [record(15_150, 127_875, 255_671), record(14_921, 86_975, 214_771)]
        checks, _observations = evaluate_a05_append_bounds(initial, append, middle)
        self.assertTrue(all(checks.values()))
        delta_checks, delta_observations = benchmark.evaluate_delta_append_bounds(
            initial, append
        )
        self.assertTrue(all(delta_checks.values()))
        self.assertEqual(
            delta_observations["sample_kind"],
            "append_only_delta_gap_no_synthetic_middle",
        )
        self.assertNotIn(
            "middle_minimum_to_append_max_put_attempt_ratio", delta_observations
        )
        o_n_append = copy.deepcopy(append)
        for item in o_n_append:
            item["cas"]["put_attempts"] = 15_000
        failed, _observations = evaluate_a05_append_bounds(
            initial, o_n_append, middle
        )
        self.assertFalse(failed["append_put_attempts_within_frozen_bound"])
        self.assertFalse(failed["middle_put_attempts_materially_exceed_append"])
        inflated = copy.deepcopy(append)
        inflated[0]["physical"][
            "final_touched_allocation_footprint_bytes"
        ] = 10_000_000
        inflated[1]["write_api"]["payload_bytes"] = 3_000_000
        failed, _observations = evaluate_a05_append_bounds(
            initial, inflated, middle
        )
        self.assertFalse(
            failed["append_final_touched_allocation_within_frozen_bound"]
        )
        self.assertFalse(failed["append_write_api_payload_within_frozen_bound"])

    def test_write_telemetry_counts_repeated_payloads_not_final_size(self) -> None:
        observed: list[int] = []
        backing = io.BytesIO()
        stream = benchmark._WriteTelemetryStream(backing, observed.append)
        self.assertEqual(stream.write(b"abcdef"), 6)
        backing.seek(0)
        self.assertEqual(stream.write(b"xy"), 2)
        self.assertEqual(observed, [6, 2])
        self.assertEqual(len(backing.getvalue()), 6)

    def test_full_inventory_detects_added_empty_directory(self) -> None:
        baseline = {
            "entries": {
                "protected/.": {
                    "kind": "directory",
                    "mtime_ns": 1,
                    "inode": 10,
                },
                "protected/nested": {"kind": "directory", "mtime_ns": 1},
                "protected/nested/file": {
                    "kind": "file",
                    "mtime_ns": 1,
                    "size": 1,
                    "allocated": 4_096,
                    "sha256": "0" * 64,
                },
            },
            "summary": {"file_count": 1, "directory_count": 1},
        }
        changed = copy.deepcopy(baseline)
        changed["entries"]["protected/empty"] = {
            "kind": "directory",
            "mtime_ns": 1,
        }
        changed["summary"]["directory_count"] = 2
        self.assertFalse(full_inventory_equal(baseline, changed))
        root_changed = copy.deepcopy(baseline)
        root_changed["entries"]["protected/."]["mtime_ns"] = 2
        self.assertFalse(full_inventory_equal(baseline, root_changed))

    def test_candidate_loader_binds_every_runtime_api(self) -> None:
        benchmark._load_candidate_modules()
        for name in (
            "ArchiveOccurrence",
            "PromptArchiveIncremental",
            "response_id_v1",
            "ExternalLatestCoordinator",
            "PromptArchiveRuntime",
            "PromptArchiveRuntimeConfig",
            "PromptArchiveRuntimeRegistry",
            "canonical_json_bytes_v1",
        ):
            self.assertTrue(callable(getattr(benchmark, name)), name)

    def test_candidate_native_root_open_is_denied_before_createfilew(self) -> None:
        source = textwrap.dedent(
            r"""
            from pathlib import Path
            import tools.run_psc_exact_head_benchmark as benchmark

            denied = r"C:\service-state"
            managed_repository = r"C:\service-state\managed\workspace"
            audit = {
                "phase": "benchmark",
                "forbidden_attempts": [],
                "unsafe_alias_attempts": [],
                "protected_fixture_benchmark_attempts": [],
                "authorized_fixture_inventory_events": 0,
                "guard_checks_by_event": {},
                "descriptor_checks_by_event": {},
                "native_path_checks_by_entrypoint": {},
                "native_guard_entrypoints": [],
                "write_telemetry": {"active_transition": None, "by_transition": {}},
                "native_path_checks_by_entrypoint": {},
                "native_guard_entrypoints": [],
                "write_telemetry": {"active_transition": None, "by_transition": {}},
            }
            forbidden = ({
                "label": "synthetic",
                "lexical_path": benchmark._strict_local_windows_path(denied),
                "device_path": benchmark._device_lexical_windows_path(denied),
            },)
            guard = benchmark._AccessGuard(
                forbidden=forbidden,
                protected=Path(r"C:\psc-native-protected-never-create"),
                allowed_roots=(Path(managed_repository),),
                audit=audit,
            )
            benchmark._ACTIVE_ACCESS_GUARD = guard
            benchmark._load_candidate_modules()
            benchmark._install_candidate_native_guards(audit)
            try:
                benchmark.CandidateStorageModule._windows_open_pinned_directory(
                    Path(managed_repository)
                )
            except PermissionError:
                pass
            else:
                raise SystemExit(3)
            if audit["forbidden_attempts"] != [{
                "event": "prompt_archive_storage.CreateFileW",
                "root_label": "synthetic",
            }]:
                raise SystemExit(4)
            """
        )
        completed = subprocess.run(
            [sys.executable, "-B", "-c", source],
            cwd=REPOSITORY_ROOT,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            text=True,
            encoding="utf-8",
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_main_unexpected_exception_fails_closed_without_message_leak(self) -> None:
        stderr = io.StringIO()
        with mock.patch.object(
            benchmark, "_run", side_effect=RuntimeError("sensitive detail")
        ), contextlib.redirect_stderr(stderr):
            result = main(
                [
                    "--run-root", r"C:\isolated-never-created",
                    "--expected-head", "0" * 40,
                    "--expected-tree", "1" * 40,
                ]
            )
        self.assertEqual(result, 2)
        self.assertIn('"code":"BENCHMARK_FAILED_CLOSED"', stderr.getvalue())
        self.assertNotIn("sensitive detail", stderr.getvalue())
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            benchmark._parser().parse_args(
                [
                    "--run-root",
                    r"C:\isolated-never-created",
                    "--expected-head",
                    "0" * 40,
                    "--expected-tree",
                    "1" * 40,
                    "--workload-profile",
                    "unknown",
                ]
            )

    def test_evidence_seal_is_canonical_and_excludes_its_own_field(self) -> None:
        payload = {"z": [3, 2, 1], "a": "Без потерь"}
        first = seal_evidence(payload)
        second = seal_evidence(payload)
        self.assertEqual(first, second)
        self.assertEqual(
            first["payload_sha256"],
            _sha256(_canonical_json(payload)),
        )
        encoded = _canonical_json(first)
        self.assertEqual(encoded, _canonical_json(json.loads(encoded)))
        self.assertEqual(verify_sealed_evidence_bytes(encoded + b"\n"), first)

    def test_forbidden_root_inventory_requires_named_absolute_service_roots(self) -> None:
        values = [
            r"service_runtime=C:\service\runtime",
            r"service_prompt=C:\service\prompt",
            r"service_state=C:\service\state",
            r"service_root=D:\service\source",
        ]
        roots = parse_forbidden_roots(values)
        self.assertEqual(
            {item["label"] for item in roots},
            {
                "live_prompt",
                "service_runtime",
                "service_prompt",
                "service_state",
                "service_root",
            },
        )
        with self.assertRaises(BenchmarkFailure):
            parse_forbidden_roots(values[:-1])
        with self.assertRaises(BenchmarkFailure):
            parse_forbidden_roots([*values[:-1], "service_root=relative"])

    def test_irreversible_access_guards_deny_stat_open_enumeration_and_remove(self) -> None:
        source = textwrap.dedent(
            r"""
            import builtins
            import os
            import tempfile
            from pathlib import Path
            from tools.run_psc_exact_head_benchmark import (
                _device_lexical_windows_path,
                _install_access_guards,
            )

            denied = r"C:\psc-benchmark-denied-never-create"
            fixture_root = Path(tempfile.mkdtemp(prefix="psc-guard-fd-"))
            protected_root = fixture_root / "protected"
            protected_root.mkdir()
            protected_file = protected_root / "sentinel.bin"
            protected_file.write_bytes(b"protected")
            safe_file = fixture_root / "safe.bin"
            safe_file.write_bytes(b"safe-data")
            repository_file = Path.cwd() / "tools" / "run_psc_exact_head_benchmark.py"
            protected_fd = os.open(protected_file, os.O_RDWR)
            safe_fd = os.open(safe_file, os.O_RDWR)
            repository_fd = os.open(repository_file, os.O_RDONLY)
            audit = {
                "phase": "benchmark",
                "forbidden_attempts": [],
                "unsafe_alias_attempts": [],
                "protected_fixture_benchmark_attempts": [],
                "authorized_fixture_inventory_events": 0,
                "guard_checks_by_event": {},
                "descriptor_checks_by_event": {},
                "write_telemetry": {"active_transition": None, "by_transition": {}},
            }
            _install_access_guards(
                forbidden=({
                    "label": "synthetic",
                    "lexical_path": denied.lower(),
                    "device_path": _device_lexical_windows_path(denied),
                },),
                protected=protected_root,
                audit=audit,
            )
            checks = (
                lambda: os.stat(denied),
                lambda: os.lstat(denied),
                lambda: os.listdir(denied),
                lambda: os.scandir(denied),
                lambda: builtins.open(denied, "rb"),
                lambda: os.open(denied, os.O_RDONLY),
                lambda: os.remove(denied),
            )
            for check in checks:
                try:
                    check()
                except PermissionError:
                    pass
                else:
                    raise SystemExit(3)
            required = {
                "os.stat", "os.lstat", "os.listdir", "os.scandir",
                "open", "os.open", "os.remove",
            }
            observed = {item["event"] for item in audit["forbidden_attempts"]}
            if not required.issubset(observed):
                raise SystemExit(4)
            aliases = (
                r"\\localhost\c$\never-probe",
                r"\\?\C:\never-probe",
                r"\\.\C:\never-probe",
                r"\never-probe",
                r"C:\PROGRA~1\never-probe",
            )
            for alias in aliases:
                try:
                    os.stat(alias)
                except PermissionError:
                    pass
                else:
                    raise SystemExit(5)
            if len(audit["unsafe_alias_attempts"]) != len(aliases):
                raise SystemExit(6)
            os.fstat(repository_fd)
            os.read(repository_fd, 1)
            for operation in (
                lambda: os.write(repository_fd, b"x"),
                lambda: os.ftruncate(repository_fd, 0),
                lambda: os.fsync(repository_fd),
            ):
                try:
                    operation()
                except PermissionError:
                    pass
                else:
                    raise SystemExit(7)
            for operation in (
                lambda: os.fstat(protected_fd),
                lambda: os.read(protected_fd, 1),
                lambda: os.write(protected_fd, b"x"),
                lambda: os.ftruncate(protected_fd, 0),
                lambda: os.fsync(protected_fd),
                lambda: os.fdopen(protected_fd, "rb", closefd=False),
            ):
                try:
                    operation()
                except PermissionError:
                    pass
                else:
                    raise SystemExit(8)
            os.fstat(safe_fd)
            os.read(safe_fd, 1)
            os.write(safe_fd, b"x")
            os.ftruncate(safe_fd, 4)
            os.fsync(safe_fd)
            exclusive_file = fixture_root / "evidence.bin"
            import tools.run_psc_exact_head_benchmark as benchmark
            benchmark._write_exclusive(exclusive_file, b"exact-evidence")
            if exclusive_file.read_bytes() != b"exact-evidence":
                raise SystemExit(9)
            os.close(repository_fd)
            os.close(protected_fd)
            os.close(safe_fd)
            audit["phase"] = "pre_inventory"
            os.remove(protected_file)
            os.remove(safe_file)
            os.remove(exclusive_file)
            os.rmdir(protected_root)
            os.rmdir(fixture_root)
            """
        )
        completed = subprocess.run(
            [sys.executable, "-B", "-c", source],
            cwd=REPOSITORY_ROOT,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            text=True,
            encoding="utf-8",
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)


if __name__ == "__main__":
    unittest.main()
