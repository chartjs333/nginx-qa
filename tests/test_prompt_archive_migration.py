from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import subprocess
import sys
import tempfile
import threading
import unittest
from types import SimpleNamespace
from unittest import mock

from jsonschema import Draft202012Validator, FormatChecker

import nginx_qa.prompt_archive_migration as migration_module
from nginx_qa.prompt_archive_migration import (
    OFFLINE_OPERATOR_GATE,
    PromptArchiveMigration,
    PromptArchiveMigrationError,
    inventory_isolated_root,
)
from nginx_qa.prompt_archive_storage import (
    PromptArchiveStorageError,
    canonical_json_bytes_v1,
)


PROJECT_ID = "9000"
HANDLE_ID = "isolated-fixture-1"
STAMP = "2026-10-11T12:00:00Z"


class InjectedCrash(RuntimeError):
    pass


class PromptArchiveMigrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary.name)
        self.source = self.base / "source"
        self.archive = self.base / "archive"
        self.private_evidence = self.base / "private-evidence.json"
        self.source.mkdir()

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def write_fixture(self) -> dict[str, bytes]:
        payloads = {
            "latest-response.json": (
                b' \r\n{"z":1.2300e+04,"duplicate":"first",'
                b'"duplicate":"second","unicode":"\\u0061"}\r\n'
            ),
            "transitions/transition-response-1.json": (
                b'{"project_state":{"history_with_patches":[1,2]},"ok":true}\n'
            ),
        }
        for relative, raw in payloads.items():
            target = self.source.joinpath(*relative.split("/"))
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(raw)
        return payloads

    def migration(self, *, fault_hook=None) -> PromptArchiveMigration:
        return PromptArchiveMigration(
            input_root=self.source,
            archive_root=self.archive,
            project_id=PROJECT_ID,
            source_root_handle_id=HANDLE_ID,
            operator_gate=OFFLINE_OPERATOR_GATE,
            operator_principal_id="operator-psc-005",
            private_evidence_file=self.private_evidence,
            protected_roots=(),
            clock=lambda: STAMP,
            fault_hook=fault_hook,
        )

    def assert_code(self, code: str, operation) -> None:
        with self.assertRaises(PromptArchiveMigrationError) as raised:
            operation()
        self.assertEqual(raised.exception.code, code)

    def assert_schema(self, name: str, document: object) -> None:
        schema_path = Path(__file__).resolve().parents[1] / "schemas" / name
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        Draft202012Validator.check_schema(schema)
        errors = list(
            Draft202012Validator(
                schema,
                format_checker=FormatChecker(),
            ).iter_errors(document)
        )
        self.assertEqual([], errors, [error.message for error in errors])

    def test_inventory_frozen_two_file_golden_vector(self) -> None:
        (self.source / "a.json").write_bytes(b"one")
        nested = self.source / "nested"
        nested.mkdir()
        (nested / "b.json").write_bytes(b"two")

        inventory = inventory_isolated_root(self.source)

        self.assertEqual(
            inventory.inventory_sha256,
            "aa06ae62c2f278fa7e29ffbc0a886effac09967afadd3915bd14c36a34db27b6",
        )
        self.assertEqual(
            [entry.relative_path for entry in inventory.entries],
            ["a.json", "nested/b.json"],
        )

    def test_migrate_is_byte_exact_path_free_and_idempotent(self) -> None:
        payloads = self.write_fixture()
        original_metadata = {
            relative: (
                self.source.joinpath(*relative.split("/")).stat().st_mtime_ns,
                hashlib.sha256(raw).hexdigest(),
            )
            for relative, raw in payloads.items()
        }
        migration = self.migration()

        first = migration.migrate()
        second = migration.migrate()

        self.assertEqual(first, second)
        self.assertEqual(first["status"], "complete")
        self.assertIsNone(first["sprint_id"])
        self.assertEqual(len(first["archives"]), len(payloads))
        self.assertEqual(
            first["journal"]["pre_inventory"],
            first["journal"]["post_inventory"],
        )
        for relative, raw in payloads.items():
            target = self.source.joinpath(*relative.split("/"))
            self.assertEqual(target.read_bytes(), raw)
            self.assertEqual(target.stat().st_mtime_ns, original_metadata[relative][0])

        plan = migration.storage.read_immutable_json(migration._plan_path())
        journal = migration.storage.read_current_json(migration._journal_path())
        serialized = json.dumps({"plan": plan, "journal": journal}, sort_keys=True)
        self.assertNotIn(str(self.source), serialized)
        self.assertNotIn(str(self.archive), serialized)
        self.assertNotIn("latest-response.json", serialized)
        self.assertNotIn("transition-response-1.json", serialized)
        self.assertEqual(plan["sprint_id"], None)
        private = json.loads(self.private_evidence.read_text(encoding="utf-8"))
        self.assertEqual(private["pre"]["entries"], private["post"]["entries"])
        self.assert_schema(
            "prompt-archive-migration-batch-plan-v1.schema.json", plan
        )
        self.assert_schema(
            "prompt-archive-migration-batch-journal-v1.schema.json", journal
        )
        self.assert_schema(
            "prompt-archive-migration-evidence-v1.schema.json", first
        )
        self.assert_schema(
            "prompt-archive-migration-private-evidence-v1.schema.json", private
        )

    def test_crash_after_manifest_publication_resumes_without_duplicate_result(self) -> None:
        self.write_fixture()
        fired = False

        def crash(stage: str) -> None:
            nonlocal fired
            if stage == "after_entry_publication" and not fired:
                fired = True
                raise InjectedCrash()

        with self.assertRaises(InjectedCrash):
            self.migration(fault_hook=crash).migrate()

        evidence = self.migration().migrate()

        self.assertEqual(evidence["status"], "complete")
        entry_ids = [item["entry_id"] for item in evidence["archives"]]
        self.assertEqual(len(entry_ids), len(set(entry_ids)))
        self.assertEqual(len(entry_ids), 2)

    def test_crash_after_outer_journal_generation_resumes_through_current(self) -> None:
        self.write_fixture()
        fired = False

        def crash(stage: str) -> None:
            nonlocal fired
            if stage == "after_batch_journal_generation" and not fired:
                fired = True
                raise InjectedCrash()

        with self.assertRaises(InjectedCrash):
            self.migration(fault_hook=crash).migrate()

        evidence = self.migration().migrate()
        self.assertEqual(evidence["status"], "complete")
        self.assertEqual(len(evidence["archives"]), 2)

    def test_outer_journal_fork_and_corrupt_predecessor_fail_closed(self) -> None:
        self.write_fixture()
        migration = self.migration()
        migration.migrate()
        current = migration.storage.read_current_json(migration._journal_path())
        fork = json.loads(json.dumps(current))
        fork["updated_at"] = "2026-10-11T12:00:01Z"
        fork_raw = canonical_json_bytes_v1(fork, maximum=8 * 1024 * 1024)
        fork_digest = hashlib.sha256(fork_raw).hexdigest()
        migration.storage.publish_immutable_bytes(
            migration._journal_generation_path(
                current["journal_generation"], fork_digest
            ),
            fork_raw,
            expected_sha256=fork_digest,
        )

        self.assert_code(
            "PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT",
            migration.verify_rollback,
        )

        fork_path = migration.storage._immutable_path(
            migration._journal_generation_path(
                current["journal_generation"], fork_digest
            )
        )
        fork_path.unlink()
        predecessor_path = migration.storage._immutable_path(
            migration._journal_generation_path(
                current["previous_journal_generation"],
                current["previous_journal_sha256"],
            )
        )
        predecessor_path.write_bytes(b"{}")
        with self.assertRaises(PromptArchiveMigrationError) as raised:
            migration.verify_rollback()
        self.assertIn(
            raised.exception.code,
            {
                "PROMPT_ARCHIVE_MIGRATION_INTEGRITY_MISMATCH",
                "PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT",
            },
        )

    def test_concurrent_same_batch_writers_converge_on_one_plan_and_result(self) -> None:
        self.write_fixture()
        barrier = threading.Barrier(2)

        def worker() -> dict[str, object]:
            waited = False

            def synchronize(stage: str) -> None:
                nonlocal waited
                if stage == "after_full_pre_inventory" and not waited:
                    waited = True
                    barrier.wait(timeout=30)

            return self.migration(fault_hook=synchronize).migrate()

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(worker) for _ in range(2)]
            results = [future.result(timeout=90) for future in futures]

        self.assertEqual(results[0], results[1])
        self.assertEqual(results[0]["status"], "complete")
        entry_ids = [item["entry_id"] for item in results[0]["archives"]]
        self.assertEqual(len(entry_ids), len(set(entry_ids)))

    def test_batch_control_lock_storage_failure_uses_stable_migration_error(self) -> None:
        self.write_fixture()
        migration = self.migration()
        with mock.patch.object(
            migration.storage,
            "control_lock",
            side_effect=PromptArchiveStorageError("PROMPT_ARCHIVE_UNAVAILABLE"),
        ):
            self.assert_code(
                "PROMPT_ARCHIVE_MIGRATION_UNAVAILABLE",
                migration.migrate,
            )

    def test_source_change_at_prepublication_fault_fails_closed(self) -> None:
        self.write_fixture()
        changed = False

        def mutate(stage: str) -> None:
            nonlocal changed
            if stage == "after_source_read" and not changed:
                changed = True
                (self.source / "latest-response.json").write_bytes(b'{"changed":true}')

        migration = self.migration(fault_hook=mutate)
        self.assert_code(
            "PROMPT_ARCHIVE_MIGRATION_SOURCE_CHANGED", migration.migrate
        )
        journal = migration.storage.read_current_json(migration._journal_path())
        self.assertNotEqual(journal["state"], "complete")
        self.assertIsNone(journal["post_inventory"])

    def test_changed_inventory_is_rejected_on_resume_before_publication(self) -> None:
        self.write_fixture()
        fired = False

        def crash(stage: str) -> None:
            nonlocal fired
            if stage == "after_batch_plan_publication" and not fired:
                fired = True
                raise InjectedCrash()

        with self.assertRaises(InjectedCrash):
            self.migration(fault_hook=crash).migrate()
        (self.source / "latest-response.json").write_bytes(b'{"changed":true}')

        self.assert_code(
            "PROMPT_ARCHIVE_MIGRATION_SOURCE_CHANGED",
            self.migration().migrate,
        )

    def test_reconstruct_and_rollback_verify_are_exact_and_repeatable(self) -> None:
        payloads = self.write_fixture()
        migration = self.migration()
        migration.migrate()
        reconstructed = self.base / "reconstructed"

        first = migration.reconstruct(reconstructed)
        second = migration.reconstruct(reconstructed)
        rollback = migration.verify_rollback()

        self.assertEqual(first, second)
        self.assertTrue(first["raw_files_equal"])
        self.assert_schema(
            "prompt-archive-migration-reconstruction-evidence-v1.schema.json",
            first,
        )
        self.assertEqual(rollback["status"], "verified")
        self.assert_schema(
            "prompt-archive-migration-verification-v1.schema.json",
            rollback,
        )
        for relative, raw in payloads.items():
            self.assertEqual(
                reconstructed.joinpath(*relative.split("/")).read_bytes(), raw
            )

    def test_reconstruct_never_overwrites_different_existing_file(self) -> None:
        self.write_fixture()
        migration = self.migration()
        migration.migrate()
        reconstructed = self.base / "reconstructed"
        reconstructed.mkdir()
        (reconstructed / "latest-response.json").write_bytes(b'{"other":true}')

        self.assert_code(
            "PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT",
            lambda: migration.reconstruct(reconstructed),
        )
        self.assertEqual(
            (reconstructed / "latest-response.json").read_bytes(), b'{"other":true}'
        )

    def test_input_output_overlap_and_configured_protected_roots_are_rejected(self) -> None:
        self.write_fixture()
        self.assert_code(
            "PROMPT_ARCHIVE_MIGRATION_ROOT_OVERLAP",
            lambda: PromptArchiveMigration(
                input_root=self.source,
                archive_root=self.source / "archive",
                project_id=PROJECT_ID,
                source_root_handle_id=HANDLE_ID,
                operator_gate=OFFLINE_OPERATOR_GATE,
                operator_principal_id="operator-psc-005",
            ),
        )
        protected = self.base / "live"
        protected.mkdir()
        self.assert_code(
            "PROMPT_ARCHIVE_MIGRATION_ROOT_OVERLAP",
            lambda: PromptArchiveMigration(
                input_root=self.source,
                archive_root=protected / "archive",
                project_id=PROJECT_ID,
                source_root_handle_id=HANDLE_ID,
                operator_gate=OFFLINE_OPERATOR_GATE,
                operator_principal_id="operator-psc-005",
                protected_roots=(protected,),
            ),
        )

    def test_protected_overlap_is_rejected_without_touching_protected_tree(self) -> None:
        protected = self.base / "live-do-not-touch"
        candidate = protected / "copied-prompts"
        archive = self.base / "isolated-archive"
        with mock.patch.object(
            Path,
            "lstat",
            side_effect=AssertionError("protected tree must not be probed"),
        ):
            self.assert_code(
                "PROMPT_ARCHIVE_MIGRATION_ROOT_OVERLAP",
                lambda: PromptArchiveMigration(
                    input_root=candidate,
                    archive_root=archive,
                    project_id=PROJECT_ID,
                    source_root_handle_id=HANDLE_ID,
                    operator_gate=OFFLINE_OPERATOR_GATE,
                    operator_principal_id="operator-psc-005",
                    protected_roots=(protected,),
                ),
            )
        self.assertFalse(protected.exists())
        self.assertFalse(archive.exists())

    def test_hardlink_and_symlink_sources_are_rejected(self) -> None:
        original = self.source / "a.json"
        original.write_bytes(b"{}")
        hardlink = self.source / "b.json"
        try:
            os.link(original, hardlink)
        except OSError as exc:
            self.skipTest(f"hard links unavailable: {exc}")
        self.assert_code(
            "PROMPT_ARCHIVE_MIGRATION_SOURCE_UNSAFE",
            lambda: inventory_isolated_root(self.source),
        )

        hardlink.unlink()
        try:
            os.symlink(original, self.source / "linked.json")
        except OSError:
            # Windows hosts without Developer Mode cannot create a symlink;
            # the reparse branch is exercised independently below.
            pass
        else:
            self.assert_code(
                "PROMPT_ARCHIVE_MIGRATION_SOURCE_UNSAFE",
                lambda: inventory_isolated_root(self.source),
            )

    @unittest.skipUnless(os.name == "nt", "Windows reparse contract")
    def test_mocked_junction_reparse_source_is_rejected(self) -> None:
        junction = self.source / "junction"
        junction.mkdir()
        (junction / "latest-response.json").write_bytes(b"{}")
        actual = migration_module._is_redirected

        def redirected(path: Path) -> bool:
            return Path(path) == junction or actual(Path(path))

        with mock.patch.object(
            migration_module, "_is_redirected", side_effect=redirected
        ):
            self.assert_code(
                "PROMPT_ARCHIVE_MIGRATION_SOURCE_UNSAFE",
                lambda: inventory_isolated_root(self.source),
            )

    def test_unsafe_windows_components_are_rejected_before_traversal(self) -> None:
        for component in (
            "payload:stream.json",
            "trailing-dot.",
            "trailing-space ",
            "CON",
            "nul.json",
            "LPT9.txt",
        ):
            with self.subTest(component=component):
                self.assert_code(
                    "PROMPT_ARCHIVE_MIGRATION_SOURCE_UNSAFE",
                    lambda component=component: migration_module._validate_component(
                        component
                    ),
                )
        seen: set[str] = set()
        migration_module._claim_casefolded_path(seen, "Folder/A.json")
        self.assert_code(
            "PROMPT_ARCHIVE_MIGRATION_SOURCE_UNSAFE",
            lambda: migration_module._claim_casefolded_path(
                seen, "folder/a.JSON"
            ),
        )

    @unittest.skipUnless(os.name == "nt", "Windows namespace contract")
    def test_windows_alias_namespaces_and_same_volume_drive_alias_fail_closed(self) -> None:
        for raw in (
            r"\\?\C:\isolated",
            r"\\.\C:\isolated",
            r"\??\C:\isolated",
            r"\\server\share\isolated",
        ):
            with self.subTest(raw=raw):
                self.assert_code(
                    "PROMPT_ARCHIVE_MIGRATION_REQUEST_INVALID",
                    lambda raw=raw: migration_module._windows_reject_aliased_namespace(
                        raw, Path(r"C:\isolated")
                    ),
                )
        with mock.patch.object(
            migration_module,
            "_windows_drive_device",
            return_value=r"\Device\Mup\server\share",
        ):
            self.assert_code(
                "PROMPT_ARCHIVE_MIGRATION_REQUEST_INVALID",
                lambda: migration_module._windows_reject_aliased_namespace(
                    r"Z:\isolated", Path(r"Z:\isolated")
                ),
            )
        with mock.patch.object(
            migration_module,
            "_windows_drive_device",
            return_value=r"\Device\HarddiskVolume99",
        ):
            self.assertTrue(
                migration_module._overlap(
                    Path(r"C:\isolated"), Path(r"D:\isolated\child")
                )
            )
        self.assert_code(
            "PROMPT_ARCHIVE_MIGRATION_REQUEST_INVALID",
            lambda: migration_module._absolute_root(r"C:\ISOLAT~1\source"),
        )

    def test_selected_file_hydration_limit_fails_before_durable_plan(self) -> None:
        (self.source / "latest-response.json").write_bytes(b"0123456789")
        migration = self.migration()
        fake_storage = SimpleNamespace(
            limits=SimpleNamespace(max_hydrated_bytes=9)
        )
        migration._storage = fake_storage

        self.assert_code(
            "PROMPT_ARCHIVE_MIGRATION_LIMIT_EXCEEDED",
            migration.migrate,
        )

        self.assertFalse(self.archive.exists())
        self.assertFalse(self.private_evidence.exists())

    def test_oversized_plan_and_private_witness_map_to_stable_limit_error(self) -> None:
        self.write_fixture()
        migration = self.migration()
        with mock.patch.object(migration_module, "_MAX_CONTROL_BYTES", 128):
            self.assert_code(
                "PROMPT_ARCHIVE_MIGRATION_LIMIT_EXCEEDED",
                migration.migrate,
            )
        self.assertFalse(any(self.archive.rglob("plan.json")))

        stages: list[str] = []
        migration = self.migration(fault_hook=stages.append)
        with mock.patch.object(
            migration_module, "_MAX_PRIVATE_EVIDENCE_BYTES", 128
        ):
            self.assert_code(
                "PROMPT_ARCHIVE_MIGRATION_LIMIT_EXCEEDED",
                migration.migrate,
            )
        self.assertNotIn("after_entry_publication", stages)
        self.assertFalse(self.private_evidence.exists())

    def test_oversized_final_public_evidence_is_stable_and_resumable(self) -> None:
        (self.source / "latest-response.json").write_bytes(b'{"one":true}\r\n')
        outer_currents = 0

        def shrink_after_complete_journal(stage: str) -> None:
            nonlocal outer_currents
            if stage == "after_batch_journal_current":
                outer_currents += 1
                if outer_currents == 3:
                    migration_module._MAX_CONTROL_BYTES = 128

        migration = self.migration(fault_hook=shrink_after_complete_journal)
        original_limit = migration_module._MAX_CONTROL_BYTES
        try:
            self.assert_code(
                "PROMPT_ARCHIVE_MIGRATION_LIMIT_EXCEEDED",
                migration.migrate,
            )
        finally:
            migration_module._MAX_CONTROL_BYTES = original_limit

        evidence = self.migration().migrate()
        self.assertEqual(evidence["status"], "complete")

    def test_safety_snapshot_detects_mtime_only_and_same_byte_identity_replacement(self) -> None:
        target = self.source / "latest-response.json"
        raw = b'{"same":true}\r\n'
        target.write_bytes(raw)
        baseline = inventory_isolated_root(self.source)

        changed_mtime = baseline.entries[0].mtime_ns + 2_000_000_000
        os.utime(target, ns=(changed_mtime, changed_mtime))
        after_mtime = inventory_isolated_root(self.source)
        self.assertEqual(baseline.inventory_sha256, after_mtime.inventory_sha256)
        self.assertNotEqual(
            baseline.safety_snapshot_sha256,
            after_mtime.safety_snapshot_sha256,
        )

        replacement = self.base / "replacement.tmp"
        replacement.write_bytes(raw)
        os.utime(
            replacement,
            ns=(after_mtime.entries[0].mtime_ns, after_mtime.entries[0].mtime_ns),
        )
        os.replace(replacement, target)
        after_replacement = inventory_isolated_root(self.source)
        self.assertEqual(
            after_mtime.inventory_sha256, after_replacement.inventory_sha256
        )
        self.assertEqual(
            after_mtime.entries[0].mtime_ns,
            after_replacement.entries[0].mtime_ns,
        )
        self.assertNotEqual(
            after_mtime.entries[0].identity_sha256,
            after_replacement.entries[0].identity_sha256,
        )
        self.assertNotEqual(
            after_mtime.safety_snapshot_sha256,
            after_replacement.safety_snapshot_sha256,
        )

    def test_private_evidence_conflict_preflights_before_entry_publication(self) -> None:
        self.write_fixture()
        self.private_evidence.write_bytes(b"operator-owned-conflict")
        stages: list[str] = []
        migration = self.migration(fault_hook=stages.append)

        self.assert_code(
            "PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT",
            migration.migrate,
        )

        self.assertNotIn("after_private_evidence_preflight", stages)
        self.assertNotIn("after_entry_publication", stages)
        self.assertEqual(
            self.private_evidence.read_bytes(), b"operator-owned-conflict"
        )

    def test_owned_partial_temp_is_resumed_without_touching_unowned_temp(self) -> None:
        raw = b'{"preserved":"byte-exact"}\r\n'
        target = self.base / "private" / "witness.json"
        target.parent.mkdir()
        operation_id = "parop1-" + "a" * 32
        unrelated = target.parent / ".unrelated.tmp"
        unrelated.write_bytes(b"do-not-touch")
        with migration_module._open_pinned_mutation_directory(
            target.parent
        ) as parent:
            temp_name, _temp_path, descriptor = (
                migration_module._open_owned_relative_temp(
                    parent,
                    target.name,
                    label="par-private",
                    ownership_id=operation_id,
                )
            )
            os.write(descriptor, raw[:7])
            os.fsync(descriptor)
            os.close(descriptor)
        self.assertTrue((target.parent / temp_name).exists())

        digest, size = migration_module._publish_private_evidence_file(
            target,
            raw,
            ownership_id=operation_id,
        )

        self.assertEqual(digest, hashlib.sha256(raw).hexdigest())
        self.assertEqual(size, len(raw))
        self.assertEqual(target.read_bytes(), raw)
        self.assertFalse((target.parent / temp_name).exists())
        self.assertEqual(unrelated.read_bytes(), b"do-not-touch")

    def test_reconstruction_owned_partial_temp_resumes_exactly(self) -> None:
        raw = b"opaque\x00legacy\r\n"
        source_file = self.source / "latest-response.json"
        source_file.write_bytes(raw)
        entry = inventory_isolated_root(self.source).entries[0]
        target_root = self.base / "reconstruction"
        target_root.mkdir()
        operation_id = "parop1-" + "b" * 32
        with migration_module._pinned_relative_parent(
            target_root, entry.relative_path
        ) as (parent, target_name, _target):
            temp_name, _temp_path, descriptor = (
                migration_module._open_owned_relative_temp(
                    parent,
                    target_name,
                    label="par-reconstruct",
                    ownership_id=operation_id,
                )
            )
            os.write(descriptor, raw[:-2])
            os.fsync(descriptor)
            os.close(descriptor)

        migration_module._publish_reconstructed_file(
            target_root,
            entry,
            raw,
            ownership_id=operation_id,
        )

        self.assertEqual((target_root / entry.relative_path).read_bytes(), raw)
        self.assertFalse((target_root / temp_name).exists())

    def test_opaque_invalid_json_is_preserved_without_parsing(self) -> None:
        (self.source / "bad.json").write_bytes(b'{"unterminated":')
        (self.source / "active.prompt").write_bytes(b"small working prompt")
        (self.source / "latest-response.json").write_bytes(
            b'\xef\xbb\xbf{"duplicate":1,"duplicate":2}\r\n'
        )
        migration = self.migration()
        evidence = migration.migrate()

        self.assertEqual(evidence["status"], "complete")
        self.assertEqual(len(evidence["archives"]), 1)
        self.assertEqual(evidence["source_inventory"]["entry_count"], 3)

    def test_analyze_does_not_create_or_open_output_artifacts(self) -> None:
        self.write_fixture()
        migration = self.migration()

        evidence = migration.analyze()

        self.assertEqual(evidence["status"], "ready")
        self.assert_schema(
            "prompt-archive-migration-analysis-v1.schema.json",
            evidence,
        )
        self.assertFalse(self.archive.exists())
        self.assertFalse(self.private_evidence.exists())
        self.assertIsNone(migration._storage)
        self.assertIsNone(migration._runtime)

    def test_reconstruct_succeeds_after_source_tree_is_unavailable(self) -> None:
        payloads = self.write_fixture()
        migration = self.migration()
        migration.migrate()
        unavailable = self.base / "source-unavailable"
        self.source.rename(unavailable)
        reconstructed = self.base / "reconstructed"

        evidence = migration.reconstruct(reconstructed)

        self.assertTrue(evidence["source_tree_opened"] is False)
        for relative, raw in payloads.items():
            self.assertEqual(
                reconstructed.joinpath(*relative.split("/")).read_bytes(), raw
            )

    def test_source_free_resume_requires_outer_and_entry_journal_integrity(self) -> None:
        (self.source / "latest-response.json").write_bytes(b'{"resume":true}\r\n')
        runtime_currents = 0

        def crash(stage: str) -> None:
            nonlocal runtime_currents
            if stage == "after_journal_current":
                runtime_currents += 1
                if runtime_currents == 2:
                    raise InjectedCrash()

        migration = self.migration(fault_hook=crash)
        with self.assertRaises(InjectedCrash):
            migration.migrate()
        plan = migration.storage.read_immutable_json(migration._plan_path())
        reservation = plan["reservations"][0]
        self.source.rename(self.base / "source-unavailable")

        evidence = self.migration().resume_operation(
            response_id=reservation["response_id"],
            operation_id=reservation["operation_id"],
        )

        self.assertEqual(evidence["status"], "completed")
        self.assertFalse(evidence["source_tree_opened"])
        self.assertTrue(evidence["batch_completion_deferred_until_post_inventory"])
        self.assert_schema(
            "prompt-archive-migration-resume-evidence-v1.schema.json",
            evidence,
        )

    def test_source_free_resume_rejects_tampered_outer_plan(self) -> None:
        (self.source / "latest-response.json").write_bytes(b'{"resume":true}\r\n')
        runtime_currents = 0

        def crash(stage: str) -> None:
            nonlocal runtime_currents
            if stage == "after_journal_current":
                runtime_currents += 1
                if runtime_currents == 2:
                    raise InjectedCrash()

        migration = self.migration(fault_hook=crash)
        with self.assertRaises(InjectedCrash):
            migration.migrate()
        plan = migration.storage.read_immutable_json(migration._plan_path())
        reservation = plan["reservations"][0]
        plan["reservations"][0]["source_byte_size"] += 1
        plan_path = migration.storage._immutable_path(migration._plan_path())
        plan_path.write_bytes(canonical_json_bytes_v1(plan, maximum=8 * 1024 * 1024))

        self.assert_code(
            "PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT",
            lambda: migration.resume_operation(
                response_id=reservation["response_id"],
                operation_id=reservation["operation_id"],
            ),
        )

    def test_source_free_resume_rejects_nested_plan_type_corruption_stably(self) -> None:
        (self.source / "latest-response.json").write_bytes(b'{"resume":true}\r\n')
        runtime_currents = 0

        def crash(stage: str) -> None:
            nonlocal runtime_currents
            if stage == "after_journal_current":
                runtime_currents += 1
                if runtime_currents == 2:
                    raise InjectedCrash()

        migration = self.migration(fault_hook=crash)
        with self.assertRaises(InjectedCrash):
            migration.migrate()
        plan = migration.storage.read_immutable_json(migration._plan_path())
        reservation = plan["reservations"][0]
        plan_path = migration.storage._immutable_path(migration._plan_path())

        mutations = (
            lambda value: value.__setitem__("source_inventory", "bad"),
            lambda value: value.__setitem__("source_safety", None),
            lambda value: value["source_inventory"].__setitem__(
                "inventory_ref", []
            ),
            lambda value: value["reservations"][0].__setitem__(
                "source_byte_size", "1"
            ),
            lambda value: value["reservations"][0].__setitem__(
                "decision_occurrence_id", None
            ),
        )
        for mutate in mutations:
            with self.subTest(mutation=mutate):
                corrupted = json.loads(json.dumps(plan))
                mutate(corrupted)
                plan_path.write_bytes(
                    canonical_json_bytes_v1(
                        corrupted, maximum=8 * 1024 * 1024
                    )
                )
                self.assert_code(
                    "PROMPT_ARCHIVE_MIGRATION_STATE_CONFLICT",
                    lambda: migration.resume_operation(
                        response_id=reservation["response_id"],
                        operation_id=reservation["operation_id"],
                    ),
                )
        plan_path.write_bytes(
            canonical_json_bytes_v1(plan, maximum=8 * 1024 * 1024)
        )

    def test_malformed_private_identity_fails_with_stable_error(self) -> None:
        self.write_fixture()
        migration = self.migration()
        migration.migrate()
        witness = json.loads(self.private_evidence.read_text(encoding="utf-8"))
        witness["pre"]["source_root_identity"] = "bad"
        witness["post"] = witness["pre"]
        self.private_evidence.write_bytes(
            canonical_json_bytes_v1(witness, maximum=512 * 1024 * 1024)
        )

        self.assert_code(
            "PROMPT_ARCHIVE_MIGRATION_INTEGRITY_MISMATCH",
            lambda: migration.reconstruct(self.base / "reconstructed"),
        )

    def test_directory_depth_limit_fails_closed(self) -> None:
        deepest = self.source
        for name in ("one", "two", "three"):
            deepest = deepest / name
            deepest.mkdir()
        (deepest / "latest-response.json").write_bytes(b"{}")

        with mock.patch(
            "nginx_qa.prompt_archive_migration._MAX_DIRECTORY_DEPTH", 2
        ):
            self.assert_code(
                "PROMPT_ARCHIVE_MIGRATION_LIMIT_EXCEEDED",
                lambda: inventory_isolated_root(self.source),
            )

    def test_cli_rejects_missing_gate_without_output_side_effects(self) -> None:
        self.write_fixture()
        tool = Path(__file__).resolve().parents[1] / "tools" / "prompt_archive_migrate.py"
        completed = subprocess.run(
            [
                sys.executable,
                str(tool),
                "analyze",
                "--input-root",
                str(self.source),
                "--archive-root",
                str(self.archive),
                "--project-id",
                PROJECT_ID,
                "--source-root-handle-id",
                HANDLE_ID,
                "--operator-principal-id",
                "operator-psc-005",
            ],
            capture_output=True,
            check=False,
            text=True,
        )

        self.assertEqual(completed.returncode, 2)
        self.assert_schema(
            "prompt-archive-migration-error-v1.schema.json",
            json.loads(completed.stdout),
        )
        self.assertFalse(self.archive.exists())
        self.assertFalse(self.private_evidence.exists())

    def test_cli_invalid_argument_matrix_has_zero_output_side_effects(self) -> None:
        self.write_fixture()
        tool = Path(__file__).resolve().parents[1] / "tools" / "prompt_archive_migrate.py"
        base = [
            "--input-root",
            str(self.source),
            "--project-id",
            PROJECT_ID,
            "--source-root-handle-id",
            HANDLE_ID,
            "--operator-gate",
            OFFLINE_OPERATOR_GATE,
            "--operator-principal-id",
            "operator-psc-005",
        ]
        cases = (
            ("wrong-gate", "analyze", ["--operator-gate", "wrong"]),
            ("missing-principal", "analyze", ["--operator-principal-id", ""]),
            ("missing-private", "migrate", []),
            (
                "irrelevant-private",
                "analyze",
                ["--private-evidence-file", str(self.private_evidence)],
            ),
            ("missing-reconstruction", "reconstruct", ["--private-evidence-file", str(self.private_evidence)]),
            ("resume-missing-ids", "resume-operation", []),
            ("ids-on-analyze", "analyze", ["--response-id", "x"]),
            ("unknown-option", "analyze", ["--not-a-real-option"]),
        )
        for index, (name, command, extra) in enumerate(cases):
            with self.subTest(name=name):
                archive = self.base / f"invalid-archive-{index}"
                arguments = [item for item in base]
                arguments[arguments.index("--operator-gate") + 1] = (
                    "wrong" if name == "wrong-gate" else OFFLINE_OPERATOR_GATE
                )
                if name == "missing-principal":
                    principal_index = arguments.index("--operator-principal-id")
                    del arguments[principal_index : principal_index + 2]
                completed = subprocess.run(
                    [
                        sys.executable,
                        str(tool),
                        command,
                        "--archive-root",
                        str(archive),
                        *arguments,
                        *(
                            []
                            if name in {"wrong-gate", "missing-principal"}
                            else extra
                        ),
                    ],
                    capture_output=True,
                    check=False,
                    text=True,
                )
                self.assertEqual(completed.returncode, 2)
                self.assertFalse(archive.exists())
        self.assertFalse(self.private_evidence.exists())


if __name__ == "__main__":
    unittest.main()
