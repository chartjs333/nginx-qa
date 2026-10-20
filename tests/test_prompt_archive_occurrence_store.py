import dataclasses
import hashlib
import json
import multiprocessing
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import nginx_qa.prompt_archive_occurrence_store as occurrence_store_module
from nginx_qa.prompt_archive_incremental import ArchiveOccurrence, response_id_v1
from nginx_qa.prompt_archive_occurrence_store import (
    PromptArchiveNativeOccurrenceProof,
    PromptArchiveNativeOccurrenceSource,
    PromptArchiveOccurrenceReceiptStore,
    PromptArchiveOccurrenceSource,
)
from nginx_qa.prompt_archive_storage import (
    PromptArchiveStorageError,
    canonical_json_bytes_v1,
)
from nginx_qa.prompt_archive_source_metadata import (
    SourceMetadataScreenSnapshot,
    project_source_metadata,
)


PROJECT_ID = "9000"
SPRINT_ID = "sprint-occurrence-store"
OCCURRENCE_ID = "transition-occurrence-store-1"
SOURCE_KEY = "deadbeef0123456789ab"
PAYLOAD = b'{"committed":true,"value":37}\n'
NATIVE_PAYLOAD = b'{"active_task":{"message":"bounded native response"}}'
MANIFEST_BYTES = b'{"kind":"prompt_archive_manifest","schema_version":1}'
ARCHIVE_ID = "par1-" + "c" * 64


def occurrence(*, state_revision=37, assignment_id="assignment-store-1"):
    return ArchiveOccurrence(
        project_id=PROJECT_ID,
        sprint_id=SPRINT_ID,
        response_id=response_id_v1(
            PROJECT_ID,
            SPRINT_ID,
            "transition-response",
            "transition",
            OCCURRENCE_ID,
        ),
        response_kind="transition-response",
        occurrence_kind="transition",
        occurrence_id=OCCURRENCE_ID,
        transition_id=OCCURRENCE_ID,
        source={
            "source_kind": "runtime_shadow",
            "source_commit": "a" * 40,
            "assignment_id": assignment_id,
            "review_id": "review-store-1",
            "result_key": "result-store-1",
            "predecessor_archive_id": "par1-" + "b" * 64,
        },
        authorization={
            "auth_model_version": "prompt-archive-authorization-v1",
            "principal_kind": "legacy_local_principal",
            "principal_id": PROJECT_ID,
            "binding_id": None,
            "scope_source": "legacy_project_context",
            "effective_scope_revision": None,
            "effective_scope_sha256": None,
            "decision_occurrence_id": OCCURRENCE_ID,
        },
        created_at="2026-10-11T20:00:00+02:00",
        verified_at="2026-10-11T20:00:01+02:00",
        state_revision=state_revision,
        source_response_key=SOURCE_KEY,
        source_byte_size=len(PAYLOAD),
        source_sha256=hashlib.sha256(PAYLOAD).hexdigest(),
    )


def locator(*, state_revision=37, occurrence_id=OCCURRENCE_ID):
    return {
        "locator_kind": "runtime_occurrence",
        "locator_version": "prompt-archive-runtime-occurrence-locator-v1",
        "runtime_instance_id": "psc-runtime-18032",
        "runtime_occurrence_id": occurrence_id,
        "state_revision": state_revision,
    }


def source(payload):
    return PromptArchiveOccurrenceSource(
        response_key=SOURCE_KEY,
        byte_size=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
    )


def native_occurrence(*, state_revision=37, assignment_id="assignment-store-1"):
    return dataclasses.replace(
        occurrence(
            state_revision=state_revision,
            assignment_id=assignment_id,
        ),
        source={
            "source_kind": "native_runtime",
            "source_commit": "a" * 40,
            "assignment_id": assignment_id,
            "review_id": "review-store-1",
            "result_key": "result-store-1",
            "predecessor_archive_id": "par1-" + "b" * 64,
        },
        source_response_key=None,
        source_byte_size=None,
        source_sha256=None,
    )


def manifest_ref():
    digest = hashlib.sha256(MANIFEST_BYTES).hexdigest()
    return {
        "schema_version": 1,
        "kind": "prompt_archive_blob_ref",
        "hash_algorithm": "sha256",
        "digest": digest,
        "logical_kind": "manifest",
        "media_type": "application/vnd.nginx-qa.prompt-archive-manifest-v1+json",
        "content_encoding": "identity",
        "uncompressed_size": len(MANIFEST_BYTES),
        "stored_size": len(MANIFEST_BYTES),
        "stored_sha256": digest,
    }


def native_source(payload=NATIVE_PAYLOAD):
    return PromptArchiveNativeOccurrenceSource(
        byte_size=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
        archive_id=ARCHIVE_ID,
        manifest_ref=manifest_ref(),
    )


class FileBackedReader:
    def __init__(self, path):
        self.path = Path(path)
        self.calls = []

    def __call__(
        self,
        *,
        project_id,
        response_key,
        response_kind,
        maximum_bytes,
    ):
        self.calls.append(
            (project_id, response_key, response_kind, maximum_bytes)
        )
        try:
            payload = self.path.read_bytes()
        except FileNotFoundError:
            return None
        if len(payload) > maximum_bytes:
            return payload[: maximum_bytes + 1]
        return payload


def _register_worker(receipt_root, source_path, payload, output):
    try:
        store = PromptArchiveOccurrenceReceiptStore(
            receipt_root,
            FileBackedReader(source_path),
            max_source_bytes=1024 * 1024,
        )
        store.register_occurrence(
            project_id=PROJECT_ID,
            locator=locator(),
            occurrence=occurrence(),
            source=source(payload),
        )
        proof = store.load_occurrence(
            project_id=PROJECT_ID,
            locator=locator(),
        )
        output.put(
            (
                "ok",
                None if proof is None else proof.status,
                None if proof is None else proof.occurrence.state_revision,
            )
        )
    except BaseException as exc:  # pragma: no cover - returned to parent
        output.put(("error", type(exc).__name__, getattr(exc, "code", None)))


class PromptArchiveOccurrenceReceiptStoreTests(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary_directory.name)
        self.receipt_root = self.base / "receipts"
        self.source_path = self.base / "trusted-response.json"
        self.payload = PAYLOAD
        self.source_path.write_bytes(self.payload)
        self.reader = FileBackedReader(self.source_path)
        self.store = PromptArchiveOccurrenceReceiptStore(
            self.receipt_root,
            self.reader,
            max_source_bytes=1024 * 1024,
        )

    def tearDown(self):
        self.temporary_directory.cleanup()

    def register(self, *, selected_occurrence=None, selected_source=None):
        self.store.register_occurrence(
            project_id=PROJECT_ID,
            locator=locator(),
            occurrence=selected_occurrence or occurrence(),
            source=selected_source or source(self.payload),
        )

    def register_native(self, *, selected_occurrence=None, selected_source=None):
        self.store.register_native_occurrence(
            project_id=PROJECT_ID,
            locator=locator(),
            occurrence=selected_occurrence or native_occurrence(),
            source=selected_source or native_source(),
        )

    def test_round_trip_restores_every_occurrence_field_and_verifies_source(self):
        expected = occurrence()
        expected_source = source(self.payload)
        self.register(
            selected_occurrence=expected,
            selected_source=expected_source,
        )

        proof = self.store.load_occurrence(
            project_id=PROJECT_ID,
            locator=locator(),
        )

        self.assertIsNotNone(proof)
        self.assertEqual(proof.status, "verified")
        self.assertEqual(proof.occurrence, expected)
        self.assertEqual(proof.source, expected_source)
        self.assertEqual(proof.occurrence.state_revision, 37)
        self.assertEqual(
            self.reader.calls[-1],
            (
                PROJECT_ID,
                SOURCE_KEY,
                "transition-response",
                1024 * 1024,
            ),
        )

    def test_native_round_trip_binds_content_and_manifest_without_source_read(self):
        expected_occurrence = native_occurrence()
        expected_source = native_source()
        self.register_native(
            selected_occurrence=expected_occurrence,
            selected_source=expected_source,
        )

        with mock.patch.object(
            self.store,
            "_source_reader",
            side_effect=AssertionError("native proof must not read legacy source"),
        ):
            proof = self.store.load_occurrence(
                project_id=PROJECT_ID,
                locator=locator(),
            )

        self.assertIsInstance(proof, PromptArchiveNativeOccurrenceProof)
        self.assertEqual(proof.status, "verified")
        self.assertEqual(proof.occurrence, expected_occurrence)
        self.assertEqual(proof.source, expected_source)
        self.assertEqual(proof.source.archive_id, ARCHIVE_ID)
        self.assertEqual(proof.source.manifest_ref, manifest_ref())
        self.assertEqual(self.reader.calls, [])

        raw = self._receipt_path().read_bytes()
        document = json.loads(raw)
        self.assertEqual(
            document["kind"],
            "prompt_archive_native_runtime_occurrence_receipt",
        )
        self.assertEqual(
            set(document["source"]),
            {"byte_size", "sha256", "archive_id", "manifest_ref"},
        )
        self.assertNotIn(NATIVE_PAYLOAD, raw)
        self.assertNotIn(str(self.source_path).encode(), raw)
        self.assertNotIn(SOURCE_KEY.encode(), raw)

    def test_native_create_or_verify_is_idempotent_and_divergence_conflicts(self):
        self.register_native()
        before = self._receipt_path().read_bytes()
        self.register_native()
        self.assertEqual(self._receipt_path().read_bytes(), before)

        changed = dataclasses.replace(
            native_source(),
            sha256="d" * 64,
        )
        with self.assertRaises(PromptArchiveStorageError) as divergent_error:
            self.register_native(selected_source=changed)
        self.assertEqual(
            divergent_error.exception.code,
            "PROMPT_ARCHIVE_GENERATION_CONFLICT",
        )
        self.assertEqual(self._receipt_path().read_bytes(), before)

        with self.assertRaises(PromptArchiveStorageError) as cross_kind_error:
            self.register()
        self.assertEqual(
            cross_kind_error.exception.code,
            "PROMPT_ARCHIVE_GENERATION_CONFLICT",
        )
        self.assertEqual(self._receipt_path().read_bytes(), before)

    def test_native_registration_rejects_legacy_transport_and_malformed_manifest_ref(self):
        with self.assertRaises(PromptArchiveStorageError) as legacy_error:
            self.store.register_native_occurrence(
                project_id=PROJECT_ID,
                locator=locator(),
                occurrence=occurrence(),
                source=native_source(),
            )
        self.assertEqual(
            legacy_error.exception.code,
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
        )

        bad_ref = {**manifest_ref(), "logical_kind": "journal"}
        with self.assertRaises(PromptArchiveStorageError) as ref_error:
            self.register_native(
                selected_source=dataclasses.replace(
                    native_source(),
                    manifest_ref=bad_ref,
                )
            )
        self.assertEqual(
            ref_error.exception.code,
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
        )
        self.assertFalse(self._receipt_path().exists())

    def test_migration_occurrence_rejects_native_source_metadata_authority(self):
        occurrence_id = "migration-occurrence-store-1"
        projected = project_source_metadata(
            {
                "source_type": "manual",
                "source_id": "must-not-bind-to-offline-migration",
                "observed_at": "2026-10-11T20:00:00+02:00",
            },
            screen=SourceMetadataScreenSnapshot((), (), ()),
        )
        selected = dataclasses.replace(
            occurrence(),
            sprint_id=None,
            response_id=response_id_v1(
                PROJECT_ID,
                None,
                "migration-snapshot",
                "migration",
                occurrence_id,
            ),
            response_kind="migration-snapshot",
            occurrence_kind="migration",
            occurrence_id=occurrence_id,
            transition_id=None,
            source_metadata_projection=projected.document,
            source_metadata_receipt=projected.receipt,
        )
        selected_locator = {
            **locator(occurrence_id=occurrence_id),
            "state_revision": selected.state_revision,
        }

        with self.assertRaises(PromptArchiveStorageError) as raised:
            self.store.register_occurrence(
                project_id=PROJECT_ID,
                locator=selected_locator,
                occurrence=selected,
                source=source(self.payload),
            )

        self.assertEqual(
            raised.exception.code,
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
        )

    def test_native_tampering_is_rejected_and_winner_remains_immutable(self):
        self.register_native()
        target = self._receipt_path()
        document = json.loads(target.read_bytes())
        document["source"]["sha256"] = "e" * 64
        target.write_bytes(
            canonical_json_bytes_v1(document, maximum=64 * 1024)
        )

        with self.assertRaises(PromptArchiveStorageError) as raised:
            self.store.load_occurrence(
                project_id=PROJECT_ID,
                locator=locator(),
            )
        self.assertEqual(
            raised.exception.code,
            "PROMPT_ARCHIVE_INTEGRITY_MISMATCH",
        )

    def test_missing_receipt_and_source_statuses_are_distinct(self):
        self.assertIsNone(
            self.store.load_occurrence(
                project_id=PROJECT_ID,
                locator=locator(),
            )
        )
        self.register()
        self.source_path.unlink()

        proof = self.store.load_occurrence(
            project_id=PROJECT_ID,
            locator=locator(),
        )

        self.assertIsNotNone(proof)
        self.assertEqual(proof.status, "source_unavailable")
        self.assertEqual(proof.occurrence, occurrence())

    def test_changed_source_size_or_digest_is_reported_without_payload_storage(self):
        self.register()
        for changed in (self.payload[:-1] + b"x", self.payload + b"changed"):
            with self.subTest(changed_size=len(changed)):
                self.source_path.write_bytes(changed)
                proof = self.store.load_occurrence(
                    project_id=PROJECT_ID,
                    locator=locator(),
                )
                self.assertIsNotNone(proof)
                self.assertEqual(proof.status, "source_changed")
        raw = self._receipt_path().read_bytes()
        self.assertNotIn(self.payload, raw)
        self.assertNotIn(str(self.source_path).encode(), raw)
        self.assertNotIn(str(self.receipt_root).encode(), raw)

    def test_bounded_reader_limit_is_reported_as_changed_source(self):
        self.register()

        def oversized_reader(**_kwargs):
            raise PromptArchiveStorageError("PROMPT_ARCHIVE_LIMIT_EXCEEDED")

        restarted = PromptArchiveOccurrenceReceiptStore(
            self.receipt_root,
            oversized_reader,
            max_source_bytes=1024 * 1024,
        )
        proof = restarted.load_occurrence(
            project_id=PROJECT_ID,
            locator=locator(),
        )

        self.assertIsNotNone(proof)
        self.assertEqual(proof.status, "source_changed")

    def test_source_reader_failures_become_deterministic_recovery_statuses(self):
        self.register()
        expected = {
            "PROMPT_ARCHIVE_UNAVAILABLE": "source_unavailable",
            "PROMPT_ARCHIVE_NOT_FOUND": "source_unavailable",
            "PROMPT_ARCHIVE_REFERENCE_INVALID": "source_changed",
            "PROMPT_ARCHIVE_INTEGRITY_MISMATCH": "source_changed",
        }
        for code, status in expected.items():
            with self.subTest(code=code):
                def failed_reader(**_kwargs):
                    raise PromptArchiveStorageError(code)

                restarted = PromptArchiveOccurrenceReceiptStore(
                    self.receipt_root,
                    failed_reader,
                    max_source_bytes=1024 * 1024,
                )
                proof = restarted.load_occurrence(
                    project_id=PROJECT_ID,
                    locator=locator(),
                )
                self.assertIsNotNone(proof)
                self.assertEqual(proof.status, status)

    def test_create_or_verify_is_idempotent_and_conflict_is_immutable(self):
        self.register()
        before = self._receipt_path().read_bytes()
        self.register()
        self.assertEqual(self._receipt_path().read_bytes(), before)

        with self.assertRaises(PromptArchiveStorageError) as raised:
            self.register(
                selected_occurrence=occurrence(
                    assignment_id="assignment-store-2"
                )
            )
        self.assertEqual(raised.exception.code, "PROMPT_ARCHIVE_GENERATION_CONFLICT")
        self.assertEqual(self._receipt_path().read_bytes(), before)

    def test_rejects_closed_locator_project_revision_and_path_mismatches(self):
        malformed = {**locator(), "filesystem_path": "C:\\secret\\response.json"}
        with self.assertRaises(PromptArchiveStorageError) as malformed_error:
            self.store.load_occurrence(
                project_id=PROJECT_ID,
                locator=malformed,
            )
        self.assertEqual(
            malformed_error.exception.code,
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
        )

        with self.assertRaises(PromptArchiveStorageError) as project_error:
            self.store.register_occurrence(
                project_id="9001",
                locator=locator(),
                occurrence=occurrence(),
                source=source(self.payload),
            )
        self.assertEqual(
            project_error.exception.code,
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
        )

        with self.assertRaises(PromptArchiveStorageError) as revision_error:
            self.store.register_occurrence(
                project_id=PROJECT_ID,
                locator=locator(state_revision=38),
                occurrence=occurrence(),
                source=source(self.payload),
            )
        self.assertEqual(
            revision_error.exception.code,
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
        )

        bad_source = dataclasses.replace(
            source(self.payload),
            response_key="..\\trusted-response.json",
        )
        with self.assertRaises(PromptArchiveStorageError) as path_error:
            self.store.register_occurrence(
                project_id=PROJECT_ID,
                locator=locator(),
                occurrence=occurrence(),
                source=bad_source,
            )
        self.assertEqual(
            path_error.exception.code,
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
        )

    def test_rejects_payload_secret_or_path_fields_in_occurrence_metadata(self):
        for injected in (
            {"payload": {"copied": True}},
            {"raw_path": "C:\\Prompt\\latest-response.json"},
            {"token": "github_pat_not-a-receipt-field"},
        ):
            selected = dataclasses.replace(
                occurrence(),
                source={**occurrence().source, **injected},
            )
            with self.subTest(injected=injected):
                with self.assertRaises(PromptArchiveStorageError) as raised:
                    self.store.register_occurrence(
                        project_id=PROJECT_ID,
                        locator=locator(),
                        occurrence=selected,
                        source=source(self.payload),
                    )
                self.assertEqual(
                    raised.exception.code,
                    "PROMPT_ARCHIVE_REFERENCE_INVALID",
                )

    def test_tampered_or_cross_project_receipt_is_rejected_even_if_rehashed(self):
        self.register()
        target = self._receipt_path()
        document = json.loads(target.read_bytes())
        document["project_id"] = "9001"
        core = dict(document)
        core.pop("receipt_sha256")
        document["receipt_sha256"] = hashlib.sha256(
            canonical_json_bytes_v1(core, maximum=64 * 1024)
        ).hexdigest()
        target.write_bytes(
            canonical_json_bytes_v1(document, maximum=64 * 1024)
        )

        with self.assertRaises(PromptArchiveStorageError) as raised:
            self.store.load_occurrence(
                project_id=PROJECT_ID,
                locator=locator(),
            )
        self.assertEqual(
            raised.exception.code,
            "PROMPT_ARCHIVE_INTEGRITY_MISMATCH",
        )

    def test_atomic_publish_failure_leaves_no_visible_partial_receipt(self):
        with mock.patch.object(
            self.store,
            "_atomic_publish_no_replace",
            side_effect=PromptArchiveStorageError(
                "PROMPT_ARCHIVE_UNAVAILABLE"
            ),
        ):
            with self.assertRaises(PromptArchiveStorageError) as raised:
                self.register()
        self.assertEqual(raised.exception.code, "PROMPT_ARCHIVE_UNAVAILABLE")
        self.assertIsNone(
            self.store.load_occurrence(
                project_id=PROJECT_ID,
                locator=locator(),
            )
        )

    def test_publish_never_calls_path_based_replace(self):
        with mock.patch(
            "nginx_qa.prompt_archive_occurrence_store.os.replace",
            side_effect=AssertionError("immutable receipt must never replace"),
        ):
            self.register()
        self.assertTrue(self._receipt_path().is_file())

    def test_no_replace_race_preserves_existing_winner(self):
        target = self._receipt_path()
        winner = b"winner-must-not-be-overwritten"

        def publish_winner(_descriptor, parent, _temporary_name, target_name):
            self.assertEqual(parent.path, target.parent)
            self.assertEqual(target_name, target.name)
            target.write_bytes(winner)
            return False

        with mock.patch.object(
            self.store,
            "_atomic_publish_no_replace",
            side_effect=publish_winner,
        ):
            with self.assertRaises(PromptArchiveStorageError) as raised:
                self.register()
        self.assertEqual(
            raised.exception.code,
            "PROMPT_ARCHIVE_GENERATION_CONFLICT",
        )
        self.assertEqual(target.read_bytes(), winner)

    def test_receipt_and_lock_hardlink_aliases_are_rejected(self):
        self.register()
        target = self._receipt_path()
        receipt_alias = target.with_name("receipt-hardlink-alias")
        try:
            os.link(target, receipt_alias)
        except OSError as exc:
            self.assertIsInstance(exc, OSError)
            return
        with self.assertRaises(PromptArchiveStorageError) as receipt_error:
            self.store.load_occurrence(
                project_id=PROJECT_ID,
                locator=locator(),
            )
        self.assertEqual(
            receipt_error.exception.code,
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
        )
        receipt_alias.unlink()

        locator_raw = canonical_json_bytes_v1(locator(), maximum=4 * 1024)
        locator_digest = hashlib.sha256(locator_raw).hexdigest()
        _receipt, lock_path = self.store._derived_paths(PROJECT_ID, locator_digest)
        lock_alias = lock_path.with_name("lock-hardlink-alias")
        os.link(lock_path, lock_alias)
        with self.assertRaises(PromptArchiveStorageError) as lock_error:
            self.register()
        self.assertEqual(
            lock_error.exception.code,
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
        )

    def test_receipt_read_does_not_follow_leaf_redirect(self):
        self.register()
        target = self._receipt_path()
        outside = self.base / "outside-receipt.json"
        outside.write_bytes(target.read_bytes())
        target.unlink()
        try:
            target.symlink_to(outside)
        except (OSError, NotImplementedError) as exc:
            self.assertIsInstance(exc, (OSError, NotImplementedError))
            return
        with self.assertRaises(PromptArchiveStorageError) as raised:
            self.store.load_occurrence(
                project_id=PROJECT_ID,
                locator=locator(),
            )
        self.assertEqual(
            raised.exception.code,
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
        )

    def test_constructor_does_not_create_through_redirected_ancestor(self):
        outside = self.base / "outside-bootstrap"
        outside.mkdir()
        redirected = self.base / "redirected-bootstrap"
        try:
            redirected.symlink_to(outside, target_is_directory=True)
        except (OSError, NotImplementedError) as exc:
            if os.name != "nt":
                self.assertIsInstance(exc, (OSError, NotImplementedError))
                return
            junction = subprocess.run(
                [
                    "cmd.exe",
                    "/d",
                    "/c",
                    "mklink",
                    "/J",
                    str(redirected),
                    str(outside),
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            if junction.returncode != 0:
                self.assertNotEqual(junction.returncode, 0)
                return

        with self.assertRaises(PromptArchiveStorageError) as raised:
            PromptArchiveOccurrenceReceiptStore(
                redirected / "new-receipt-root",
                self.reader,
                max_source_bytes=1024 * 1024,
            )
        self.assertIn(
            raised.exception.code,
            {"PROMPT_ARCHIVE_REFERENCE_INVALID", "PROMPT_ARCHIVE_UNAVAILABLE"},
        )
        self.assertFalse((outside / "new-receipt-root").exists())

    def test_mocked_receipt_reparse_fails_closed_without_platform_privileges(self):
        self.register()
        target = self._receipt_path()
        original = occurrence_store_module._is_redirected

        def selected_reparse(path):
            if Path(path) == target:
                return True
            return original(path)

        with mock.patch(
            "nginx_qa.prompt_archive_occurrence_store._is_redirected",
            side_effect=selected_reparse,
        ):
            with self.assertRaises(PromptArchiveStorageError) as raised:
                self.store.load_occurrence(
                    project_id=PROJECT_ID,
                    locator=locator(),
                )
        self.assertEqual(
            raised.exception.code,
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
        )

    def test_descendant_parent_redirect_is_rejected_before_outside_read(self):
        self.register()
        target = self._receipt_path()
        parent = target.parent
        outside = self.base / "outside-parent"
        outside.mkdir()
        (outside / target.name).write_bytes(target.read_bytes())
        target.unlink()
        parent.rmdir()
        try:
            parent.symlink_to(outside, target_is_directory=True)
        except (OSError, NotImplementedError) as exc:
            if os.name != "nt":
                self.assertIsInstance(exc, (OSError, NotImplementedError))
                return
            junction = subprocess.run(
                ["cmd.exe", "/d", "/c", "mklink", "/J", str(parent), str(outside)],
                check=False,
                capture_output=True,
                text=True,
            )
            if junction.returncode != 0:
                self.assertNotEqual(junction.returncode, 0)
                return
        with self.assertRaises(PromptArchiveStorageError) as raised:
            self.store.load_occurrence(
                project_id=PROJECT_ID,
                locator=locator(),
            )
        self.assertEqual(
            raised.exception.code,
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
        )

    def test_windows_case_alias_and_ads_components_fail_closed(self):
        for component in ("receipt:stream", "trailing.", "trailing ", "CON"):
            with self.subTest(component=component):
                with self.assertRaises(PromptArchiveStorageError) as raised:
                    self.store._validate_relative_component(component)
                self.assertEqual(
                    raised.exception.code,
                    "PROMPT_ARCHIVE_REFERENCE_INVALID",
                )

        if os.name != "nt":
            return
        exact_root = self.base / "BootstrapExactCase"
        exact_root.mkdir()
        with self.assertRaises(PromptArchiveStorageError) as root_case_error:
            PromptArchiveOccurrenceReceiptStore(
                self.base / "bootstrapexactcase" / "receipts",
                self.reader,
                max_source_bytes=1024 * 1024,
            )
        self.assertEqual(
            root_case_error.exception.code,
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
        )
        self.assertFalse((exact_root / "receipts").exists())

        wrong_case = self.receipt_root / "Receipts"
        wrong_case.mkdir()
        with self.assertRaises(PromptArchiveStorageError) as raised:
            self.register()
        self.assertEqual(
            raised.exception.code,
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
        )

    def test_direct_operations_do_not_enumerate_directories(self):
        with mock.patch(
            "nginx_qa.prompt_archive_occurrence_store.os.scandir",
            side_effect=AssertionError("directory scan is forbidden"),
        ):
            self.register()
            proof = self.store.load_occurrence(
                project_id=PROJECT_ID,
                locator=locator(),
            )
        self.assertIsNotNone(proof)
        self.assertEqual(proof.status, "verified")

    def test_cross_process_create_or_verify_has_one_canonical_receipt(self):
        context = multiprocessing.get_context("spawn")
        output = context.Queue()
        processes = [
            context.Process(
                target=_register_worker,
                args=(
                    str(self.receipt_root),
                    str(self.source_path),
                    self.payload,
                    output,
                ),
            )
            for _ in range(4)
        ]
        for process in processes:
            process.start()
        for process in processes:
            process.join(30)
            self.assertEqual(process.exitcode, 0)
        outcomes = [output.get(timeout=5) for _ in processes]
        self.assertEqual(outcomes, [("ok", "verified", 37)] * 4)
        raw = self._receipt_path().read_bytes()
        self.assertEqual(
            canonical_json_bytes_v1(json.loads(raw), maximum=64 * 1024),
            raw,
        )

    def _receipt_path(self):
        locator_value = locator()
        locator_raw = canonical_json_bytes_v1(
            locator_value,
            maximum=4 * 1024,
        )
        locator_digest = hashlib.sha256(locator_raw).hexdigest()
        target, _lock = self.store._derived_paths(PROJECT_ID, locator_digest)
        return target


if __name__ == "__main__":
    unittest.main()
