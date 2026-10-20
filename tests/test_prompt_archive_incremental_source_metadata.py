import copy
import dataclasses
import json
import tempfile
import unittest
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource

from nginx_qa.prompt_archive_incremental import (
    ArchiveOccurrence,
    PromptArchiveIncremental,
    response_id_v1,
)
from nginx_qa.prompt_archive_source_metadata import (
    freeze_source_metadata_screen,
    project_source_metadata,
)
from nginx_qa.prompt_archive_storage import (
    PromptArchiveStorage,
    PromptArchiveStorageError,
    canonical_json_bytes_v1,
)


PROJECT_ID = "9000"
SPRINT_ID = "sprint-source-metadata-sidecar"
MANIFEST_MEDIA = "application/vnd.nginx-qa.prompt-archive-manifest-v1+json"
SCHEMA_ROOT = Path(__file__).resolve().parents[1] / "schemas"


class PromptArchiveIncrementalSourceMetadataTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.storage = PromptArchiveStorage(
            Path(self.temporary_directory.name) / "runtime",
            PROJECT_ID,
            protected_roots=(),
        )
        self.archive = PromptArchiveIncremental(
            self.storage,
            PROJECT_ID,
            SPRINT_ID,
        )
        self.projection = project_source_metadata(
            {
                "source_type": "email",
                "conversation_id": "conversation-1",
                "sender_label": "Alice",
                "observed_at": "2026-10-20T10:00:00Z",
                "ignored": "discard-me",
            },
            screen=freeze_source_metadata_screen(),
        )

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    @staticmethod
    def response(ordinal: int) -> bytes:
        # Sidecar metadata must not alter or participate in these exact bytes.
        return (
            json.dumps(
                {"active_task": {"message": f"work-{ordinal}"}, "known": True},
                ensure_ascii=False,
                separators=(", ", ": "),
            ).encode("utf-8")
            + b"\r\n"
        )

    def occurrence(
        self,
        ordinal: int,
        *,
        predecessor_archive_id: str | None = None,
        include_metadata: bool = True,
    ) -> ArchiveOccurrence:
        occurrence_id = f"occurrence-{ordinal}"
        return ArchiveOccurrence(
            project_id=PROJECT_ID,
            sprint_id=SPRINT_ID,
            response_id=response_id_v1(
                PROJECT_ID,
                SPRINT_ID,
                "transition-response",
                "transition",
                occurrence_id,
            ),
            response_kind="transition-response",
            occurrence_kind="transition",
            occurrence_id=occurrence_id,
            transition_id=f"transition-{ordinal}",
            source={
                "source_kind": "runtime_shadow",
                "source_commit": "a" * 40,
                "assignment_id": "assignment-source-metadata",
                "predecessor_archive_id": predecessor_archive_id,
            },
            authorization={
                "auth_model_version": "prompt-archive-authorization-v1",
                "principal_kind": "synthetic_test",
                "principal_id": "source-metadata-test",
                "binding_id": None,
                "scope_source": "synthetic_test",
                "effective_scope_revision": None,
                "effective_scope_sha256": None,
                "decision_occurrence_id": f"decision-{ordinal}",
            },
            created_at=f"2026-10-20T10:{ordinal:02d}:00Z",
            verified_at=f"2026-10-20T10:{ordinal:02d}:01Z",
            source_metadata_projection=(
                self.projection.document if include_metadata else None
            ),
            source_metadata_receipt=(
                self.projection.receipt if include_metadata else None
            ),
        )

    def assert_archive_error(self, code: str, operation) -> None:
        with self.assertRaises(PromptArchiveStorageError) as raised:
            operation()
        self.assertEqual(raised.exception.code, code)

    def build(self, ordinal: int, occurrence: ArchiveOccurrence, **kwargs):
        source = self.response(ordinal)
        return self.archive.build_incremental_archive(
            source,
            occurrence,
            legacy_committed_bytes=source,
            **kwargs,
        )

    def test_source_metadata_is_one_non_rendered_current_occurrence_root(self) -> None:
        first_occurrence = self.occurrence(1)
        first = self.build(1, first_occurrence)

        self.assertEqual(
            self.archive.hydrate_archive_bytes(first.manifest_ref),
            self.response(1),
        )
        self.assertEqual(
            first.manifest["source_metadata_receipt"],
            self.projection.receipt,
        )
        sequence = self.archive._receipt_from_root(
            first.roots["source_metadata"],
            "source_metadata",
        )
        leaves = self.archive.sequence_leaves(sequence)
        self.assertEqual(sequence.item_count, 1)
        self.assertEqual(len(leaves), 1)
        self.assertEqual(
            self.archive.hydrate_record(
                leaves[0].ref,
                expected_record_type="source_metadata",
            ),
            self.projection.canonical_bytes,
        )

        second_occurrence = self.occurrence(
            2,
            predecessor_archive_id=first.archive_id,
        )
        second = self.build(
            2,
            second_occurrence,
            previous_roots=first.roots,
            previous_manifest_ref=first.manifest_ref,
        )
        second_sequence = self.archive._receipt_from_root(
            second.roots["source_metadata"],
            "source_metadata",
        )
        second_leaves = self.archive.sequence_leaves(second_sequence)
        self.assertEqual(second_sequence.item_count, 1)
        self.assertEqual(len(second_leaves), 1)
        self.assertNotEqual(leaves[0].ref, second_leaves[0].ref)
        self.assertEqual(
            self.archive.hydrate_archive_bytes(second.manifest_ref),
            self.response(2),
        )

    def test_projection_and_receipt_are_biconditional_before_any_cas_write(self) -> None:
        cases = []
        occurrence = self.occurrence(1)
        cases.append(
            dataclasses.replace(occurrence, source_metadata_receipt=None)
        )
        cases.append(
            dataclasses.replace(occurrence, source_metadata_projection=None)
        )
        bad_receipt = self.projection.receipt
        bad_receipt["projection_sha256"] = "f" * 64
        cases.append(
            dataclasses.replace(occurrence, source_metadata_receipt=bad_receipt)
        )
        for candidate in cases:
            with self.subTest(candidate=candidate.source_metadata_receipt is None):
                before = self.archive.work_metrics()
                self.assert_archive_error(
                    "PROMPT_ARCHIVE_REFERENCE_INVALID",
                    lambda candidate=candidate: self.build(1, candidate),
                )
                delta = self.archive.work_metrics().delta(before)
                self.assertEqual(delta.cas_put_attempts, 0)
                self.assertEqual(delta.new_object_count, 0)

    def test_recovery_rechecks_receipt_against_hydrated_sidecar_root(self) -> None:
        built = self.build(1, self.occurrence(1))
        tampered = copy.deepcopy(built.manifest)
        tampered["source_metadata_receipt"]["projection_byte_size"] += 1
        tampered_ref = self.storage.put_blob(
            canonical_json_bytes_v1(tampered, maximum=256 * 1024),
            logical_kind="manifest",
            media_type=MANIFEST_MEDIA,
        )
        self.assert_archive_error(
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
            lambda: self.archive.hydrate_archive_bytes(tampered_ref),
        )

        missing_root = copy.deepcopy(built.manifest)
        missing_root["roots"].pop("source_metadata")
        self.assert_archive_error(
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
            lambda: self.archive._validate_native_manifest(missing_root),
        )

    def test_old_occurrence_without_source_metadata_remains_compatible(self) -> None:
        built = self.build(1, self.occurrence(1, include_metadata=False))
        self.assertNotIn("source_metadata", built.roots)
        self.assertNotIn("source_metadata_receipt", built.manifest)
        self.assertEqual(
            self.archive.hydrate_archive_bytes(built.manifest_ref),
            self.response(1),
        )

    def test_manifest_schema_accepts_bound_sidecar_and_rejects_one_sided_pair(self) -> None:
        schemas = {}
        registry = Registry()
        for name in (
            "prompt-archive-ref-v1.schema.json",
            "prompt-archive-manifest-v1.schema.json",
        ):
            schema = json.loads((SCHEMA_ROOT / name).read_text(encoding="utf-8"))
            schemas[name] = schema
            registry = registry.with_resource(
                schema["$id"],
                Resource.from_contents(schema),
            )
        validator = Draft202012Validator(
            schemas["prompt-archive-manifest-v1.schema.json"],
            registry=registry,
            format_checker=FormatChecker(),
        )
        built = self.build(1, self.occurrence(1))
        validator.validate(built.manifest)

        without_receipt = copy.deepcopy(built.manifest)
        without_receipt.pop("source_metadata_receipt")
        self.assertTrue(tuple(validator.iter_errors(without_receipt)))
        without_root = copy.deepcopy(built.manifest)
        without_root["roots"].pop("source_metadata")
        self.assertTrue(tuple(validator.iter_errors(without_root)))


if __name__ == "__main__":
    unittest.main()
