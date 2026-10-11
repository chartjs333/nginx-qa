import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from nginx_qa.prompt_archive_incremental import (
    ArchiveOccurrence,
    PromptArchiveIncremental,
    SequenceLeaf,
    response_id_v1,
)
from nginx_qa.prompt_archive_storage import (
    PromptArchiveStorage,
    PromptArchiveStorageError,
    canonical_json_bytes_v1,
)


PROJECT_ID = "9000"
SPRINT_ID = "sprint-incremental-tests"
RECORD_MEDIA = "application/vnd.nginx-qa.prompt-archive-record-v1+json"
MANIFEST_MEDIA = "application/vnd.nginx-qa.prompt-archive-manifest-v1+json"


class PromptArchiveIncrementalTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary_directory.name)

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    def archive(self, *, sprint_id: str | None = SPRINT_ID) -> PromptArchiveIncremental:
        storage = PromptArchiveStorage(
            self.base / ("offline" if sprint_id is None else "runtime"),
            PROJECT_ID,
            protected_roots=(),
        )
        return PromptArchiveIncremental(storage, PROJECT_ID, sprint_id)

    @staticmethod
    def provenance(*, transition: str = "transition-1") -> dict:
        return {
            "source_kind": "synthetic_test",
            "origin_response_id": "parresp1-" + "1" * 64,
            "origin_transition_id": transition,
            "source_id": "fixture-source",
        }

    @staticmethod
    def migration_occurrence(ordinal: int = 1) -> ArchiveOccurrence:
        occurrence_id = f"migration-{ordinal}"
        return ArchiveOccurrence(
            project_id=PROJECT_ID,
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
            source={"source_kind": "offline_migration"},
            authorization={
                "auth_model_version": "prompt-archive-authorization-v1",
                "principal_kind": "offline_operator",
                "principal_id": "operator-1",
                "binding_id": None,
                "scope_source": "offline_operator_gate",
                "effective_scope_revision": None,
                "effective_scope_sha256": None,
                "decision_occurrence_id": f"decision-{ordinal}",
            },
            created_at=f"2026-10-11T10:{ordinal:02d}:00Z",
            verified_at=f"2026-10-11T10:{ordinal:02d}:01Z",
        )

    def assert_code(self, code: str, operation) -> None:
        with self.assertRaises(PromptArchiveStorageError) as raised:
            operation()
        self.assertEqual(raised.exception.code, code)

    def test_recursive_json_round_trip_preserves_lexemes_and_keeps_user_refs_inert(self) -> None:
        archive = self.archive()
        source = (
            b' \r\n{"number":1.2300e+04,"escaped":"\\u0061",'
            b'"payload":{"$cas_ref":{"sha256":"not-a-reference"}},'
            b'"array":[true, null, {"x":"y"}]}\r\n'
        )
        first = archive.store_json_value(source)
        before = archive.work_metrics()
        second = archive.store_json_value(source)

        self.assertEqual(archive.hydrate_json_value(first), source)
        self.assertEqual(first.ref, second.ref)
        self.assertEqual(archive.work_metrics().delta(before).new_object_count, 0)
        self.assertGreater(first.value_nodes, 1)
        self.assertGreater(first.edges, 1)

    def test_recursive_json_rejects_duplicate_decoded_names_and_invalid_json(self) -> None:
        archive = self.archive()
        self.assert_code(
            "PROMPT_ARCHIVE_REQUEST_INVALID",
            lambda: archive.store_json_value(b'{"a":1,"\\u0061":2}'),
        )
        self.assert_code(
            "PROMPT_ARCHIVE_REQUEST_INVALID",
            lambda: archive.store_json_value(b'{"a":NaN}'),
        )

    def test_every_record_family_round_trips_and_uses_its_typed_sequence(self) -> None:
        archive = self.archive()
        source_metadata = canonical_json_bytes_v1(
            {
                "schema_version": 1,
                "kind": "prompt_archive_source_metadata",
                "projection_version": "prompt-archive-source-projection-v1",
                "redaction_version": "prompt-archive-source-redaction-v1",
                "source_type": "manual",
                "conversation_id": "sha256:" + "a" * 64,
                "sender_label": "[REDACTED]",
                "redaction_count": 1,
                "redacted_fields": ["sender_label"],
                "discarded_field_count": 2,
            },
            maximum=16 * 1024,
        )
        payloads = {
            "event": b'{"event":"created"}',
            "patch": b'{"patch":"@@ -1 +1 @@"}',
            "history_text_fragment": b"escaped\\ntext",
            "result": b'{"status":"DONE"}',
            "feedback": b'{"text":"revise"}',
            "review_context": b'{"review_id":"review-1"}',
            "execution_audit": b'{"checkpoints":[{"revision":1}]}',
            "source_metadata": source_metadata,
            "current_field": b'{"active_task":{"message":"work"}}',
        }
        for record_type, payload in payloads.items():
            with self.subTest(record_type=record_type):
                receipt = archive.build_record(
                    record_type,
                    payload,
                    ordinal=0,
                    provenance=self.provenance(),
                    occurred_at="2026-10-11T10:00:00Z",
                )
                self.assertEqual(archive.hydrate_record(receipt), payload)
                sequence = archive.build_record_sequence(
                    receipt.sequence_kind,
                    [receipt],
                )
                self.assertIsNotNone(sequence)
                assert sequence is not None
                self.assertEqual(sequence.logical_bytes, len(payload))

    def test_cross_transition_payload_dedup_keeps_distinct_record_provenance(self) -> None:
        archive = self.archive()
        payload = b'{"deep":{"unknown_alias":[1,2,3]}}'
        first = archive.build_record(
            "execution_audit",
            payload,
            ordinal=0,
            provenance=self.provenance(transition="transition-1"),
        )
        before = archive.work_metrics()
        second = archive.build_record(
            "execution_audit",
            payload,
            ordinal=1,
            provenance=self.provenance(transition="transition-2"),
        )
        delta = archive.work_metrics().delta(before)

        first_document = json.loads(archive.storage.read_blob(first.ref))
        second_document = json.loads(archive.storage.read_blob(second.ref))
        self.assertEqual(
            first_document["payload"]["value_root"],
            second_document["payload"]["value_root"],
        )
        self.assertNotEqual(first.ref, second.ref)
        self.assertEqual(delta.new_object_count, 1)

    def test_record_sequence_rejects_reusing_an_ordinal_zero_record_at_ordinal_one(self) -> None:
        archive = self.archive()
        record = archive.build_record(
            "event",
            b'{"event":1}',
            ordinal=0,
            provenance=self.provenance(),
        )
        self.assert_code(
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
            lambda: archive.build_sequence(
                "events",
                [
                    SequenceLeaf(record.ref, record.payload_size),
                    SequenceLeaf(record.ref, record.payload_size),
                ],
            ),
        )

    def test_source_metadata_must_be_closed_canonical_and_semantically_consistent(self) -> None:
        archive = self.archive()
        invalid = canonical_json_bytes_v1(
            {
                "schema_version": 1,
                "kind": "prompt_archive_source_metadata",
                "projection_version": "prompt-archive-source-projection-v1",
                "redaction_version": "prompt-archive-source-redaction-v1",
                "source_type": "email",
                "sender_label": "[REDACTED]",
                "redaction_count": 0,
                "redacted_fields": [],
                "discarded_field_count": 0,
            },
            maximum=16 * 1024,
        )
        self.assert_code(
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
            lambda: archive.build_record(
                "source_metadata",
                invalid,
                ordinal=0,
                provenance=self.provenance(),
            ),
        )
        unsafe = json.loads(invalid)
        unsafe["sender_label"] = "Alice"
        unsafe["title"] = "https://alice:hunter2@example.test/x"
        unsafe["redaction_count"] = 0
        unsafe["redacted_fields"] = []
        unsafe_bytes = canonical_json_bytes_v1(unsafe, maximum=16 * 1024)
        self.assert_code(
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
            lambda: archive.build_record(
                "source_metadata",
                unsafe_bytes,
                ordinal=0,
                provenance=self.provenance(),
            ),
        )

    def test_legacy_raw_archive_is_exact_for_duplicate_keys_and_crlf(self) -> None:
        archive = self.archive(sprint_id=None)
        raw = b'{"duplicate":1,"duplicate":2}\r\n'
        occurrence = ArchiveOccurrence(
            project_id=PROJECT_ID,
            sprint_id=None,
            response_id=response_id_v1(
                PROJECT_ID, None, "migration-snapshot", "migration", "migration-1"
            ),
            response_kind="migration-snapshot",
            occurrence_kind="migration",
            occurrence_id="migration-1",
            transition_id=None,
            source={"source_kind": "offline_migration"},
            authorization={
                "auth_model_version": "prompt-archive-authorization-v1",
                "principal_kind": "offline_operator",
                "principal_id": "operator-1",
                "binding_id": None,
                "scope_source": "offline_operator_gate",
                "effective_scope_revision": None,
                "effective_scope_sha256": None,
                "decision_occurrence_id": "decision-1",
            },
            created_at="2026-10-11T10:00:00Z",
            verified_at="2026-10-11T10:00:01Z",
        )
        built = archive.build_legacy_raw_archive(raw, occurrence)

        self.assertEqual(archive.hydrate_archive_bytes(built.manifest_ref), raw)
        self.assertEqual(built.hydrated_sha256, built.manifest["integrity"]["hydrated_sha256"])

        tampered = copy.deepcopy(built.manifest)
        tampered["unexpected"] = True
        tampered_ref = archive.storage.put_blob(
            canonical_json_bytes_v1(tampered, maximum=256 * 1024),
            logical_kind="manifest",
            media_type=MANIFEST_MEDIA,
        )
        self.assert_code(
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
            lambda: archive.hydrate_archive_bytes(tampered_ref),
        )

    def test_legacy_builder_final_scrub_detects_leaf_removed_after_render(self) -> None:
        archive = self.archive(sprint_id=None)
        raw = b'{"migration":true}\r\n'
        original_counts = archive._raw_graph_counts
        original_put = archive.storage.put_blob_with_outcome
        written_kinds: list[str] = []
        raw_chunk_ref = None

        def recording_put(data, *, logical_kind, media_type):
            nonlocal raw_chunk_ref
            written_kinds.append(logical_kind)
            result = original_put(
                data,
                logical_kind=logical_kind,
                media_type=media_type,
            )
            if logical_kind == "raw_chunk":
                raw_chunk_ref = dict(result[0])
            return result

        def remove_leaf_before_counts(receipt):
            self.assertIsNotNone(raw_chunk_ref)
            archive.storage.blob_path(raw_chunk_ref).unlink()
            return original_counts(receipt)

        with mock.patch.object(
            archive.storage,
            "put_blob_with_outcome",
            side_effect=recording_put,
        ), mock.patch.object(
            archive,
            "_raw_graph_counts",
            side_effect=remove_leaf_before_counts,
        ):
            self.assert_code(
                "PROMPT_ARCHIVE_BLOB_MISSING",
                lambda: archive.build_legacy_raw_archive(
                    raw,
                    self.migration_occurrence(),
                ),
            )
        self.assertNotIn("manifest", written_kinds)

    def test_legacy_builder_rejects_renderer_mismatch_before_manifest(self) -> None:
        archive = self.archive(sprint_id=None)
        raw = b'{"migration":"exact-original"}\r\n'
        original_put = archive.storage.put_blob_with_outcome
        written_kinds: list[str] = []

        def recording_put(data, *, logical_kind, media_type):
            written_kinds.append(logical_kind)
            return original_put(
                data,
                logical_kind=logical_kind,
                media_type=media_type,
            )

        with mock.patch.object(
            archive.storage,
            "put_blob_with_outcome",
            side_effect=recording_put,
        ), mock.patch.object(
            archive,
            "hydrate_byte_stream",
            return_value=b'{"migration":"corrupt-render"}\r\n',
        ):
            self.assert_code(
                "PROMPT_ARCHIVE_INTEGRITY_MISMATCH",
                lambda: archive.build_legacy_raw_archive(
                    raw,
                    self.migration_occurrence(),
                ),
            )

        self.assertNotIn("manifest", written_kinds)

    def test_legacy_reader_final_render_does_not_reuse_count_cache(self) -> None:
        archive = self.archive(sprint_id=None)
        raw = b'{"migration":true}\r\n'
        built = archive.build_legacy_raw_archive(
            raw,
            self.migration_occurrence(),
        )
        root = archive._receipt_from_root(built.roots["raw_bytes"], "raw_bytes")
        leaf = archive.sequence_leaves(root)[0]
        leaf_path = archive.storage.blob_path(leaf.ref)
        original_hydrate = archive.hydrate_byte_stream
        removed = False

        def remove_leaf_before_render(receipt, *, media_type=None):
            nonlocal removed
            if not removed:
                leaf_path.unlink()
                removed = True
            return original_hydrate(receipt, media_type=media_type)

        with mock.patch.object(
            archive,
            "hydrate_byte_stream",
            side_effect=remove_leaf_before_render,
        ):
            self.assert_code(
                "PROMPT_ARCHIVE_BLOB_MISSING",
                lambda: archive.hydrate_archive_bytes(built.manifest_ref),
            )
        self.assertTrue(removed)

    def test_invalid_rfc3339_occurrence_fails_before_manifest(self) -> None:
        archive = self.archive(sprint_id=None)
        occurrence = ArchiveOccurrence(
            project_id=PROJECT_ID,
            sprint_id=None,
            response_id=response_id_v1(
                PROJECT_ID, None, "migration-snapshot", "migration", "migration-2"
            ),
            response_kind="migration-snapshot",
            occurrence_kind="migration",
            occurrence_id="migration-2",
            transition_id=None,
            source={"source_kind": "offline_migration"},
            authorization={
                "auth_model_version": "prompt-archive-authorization-v1",
                "principal_kind": "offline_operator",
                "principal_id": "operator-1",
                "binding_id": None,
                "scope_source": "offline_operator_gate",
                "effective_scope_revision": None,
                "effective_scope_sha256": None,
                "decision_occurrence_id": "decision-2",
            },
            created_at="2026-99-99T99:99:99Z",
            verified_at="2026-10-11T10:00:01Z",
        )
        self.assert_code(
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
            lambda: archive.build_legacy_raw_archive(b"{}", occurrence),
        )


if __name__ == "__main__":
    unittest.main()
