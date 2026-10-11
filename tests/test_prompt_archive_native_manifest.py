import copy
import dataclasses
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource

from nginx_qa.prompt_archive_incremental import (
    ArchiveOccurrence,
    ByteStreamReceipt,
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
SPRINT_ID = "sprint-native-manifest-tests"
MANIFEST_MEDIA = "application/vnd.nginx-qa.prompt-archive-manifest-v1+json"
SCHEMA_ROOT = Path(__file__).resolve().parents[1] / "schemas"


class PromptArchiveNativeManifestTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.schemas = {}
        registry = Registry()
        for name in (
            "prompt-archive-ref-v1.schema.json",
            "prompt-archive-envelope-v1.schema.json",
            "prompt-archive-manifest-v1.schema.json",
        ):
            schema = json.loads((SCHEMA_ROOT / name).read_text(encoding="utf-8"))
            cls.schemas[name] = schema
            registry = registry.with_resource(
                schema["$id"], Resource.from_contents(schema)
            )
        cls.registry = registry

    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary_directory.name)
        storage = PromptArchiveStorage(
            self.base / "runtime",
            PROJECT_ID,
            protected_roots=(),
        )
        self.archive = PromptArchiveIncremental(storage, PROJECT_ID, SPRINT_ID)

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    def validator(self, name: str) -> Draft202012Validator:
        return Draft202012Validator(
            self.schemas[name],
            registry=self.registry,
            format_checker=FormatChecker(),
        )

    @staticmethod
    def occurrence(
        ordinal: int,
        *,
        predecessor_archive_id: str | None = None,
    ) -> ArchiveOccurrence:
        occurrence_id = f"occurrence-{ordinal}"
        source = {
            "source_kind": "runtime_shadow",
            "source_commit": "a" * 40,
            "assignment_id": "assignment-native-tests",
            "predecessor_archive_id": predecessor_archive_id,
        }
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
            source=source,
            authorization={
                "auth_model_version": "prompt-archive-authorization-v1",
                "principal_kind": "synthetic_test",
                "principal_id": "native-test-runner",
                "binding_id": None,
                "scope_source": "synthetic_test",
                "effective_scope_revision": None,
                "effective_scope_sha256": None,
                "decision_occurrence_id": f"decision-{ordinal}",
            },
            created_at=f"2026-10-11T10:{ordinal:02d}:00Z",
            verified_at=f"2026-10-11T10:{ordinal:02d}:01Z",
        )

    @staticmethod
    def response(events: list[dict], *, message: str = "do work") -> bytes:
        # Deliberately use CRLF and non-canonical spacing. Native hydration must
        # reproduce destination bytes, not merely an equivalent JSON value.
        document = {
            "project_state": {"recent_activity": events},
            "active_task": {"message": message, "known": True},
        }
        encoded = json.dumps(
            document,
            ensure_ascii=False,
            separators=(", ", ": "),
        ).encode("utf-8")
        return encoded + b"\r\n"

    def envelope(self, built) -> dict:
        descriptor = built.manifest["envelope"]
        root = self.archive._receipt_from_root(  # contract-level graph inspection
            descriptor["chunks_root"], "render_plan_chunks"
        )
        leaves = self.archive.sequence_leaves(root)
        stream = ByteStreamReceipt(
            sequence=root,
            byte_size=descriptor["byte_size"],
            sha256=descriptor["sha256"],
            chunk_count=len(leaves),
        )
        return json.loads(self.archive.hydrate_byte_stream(stream))

    def envelope_stream(self, built) -> ByteStreamReceipt:
        descriptor = built.manifest["envelope"]
        root = self.archive._receipt_from_root(
            descriptor["chunks_root"], "render_plan_chunks"
        )
        leaves = self.archive.sequence_leaves(root)
        return ByteStreamReceipt(
            sequence=root,
            byte_size=descriptor["byte_size"],
            sha256=descriptor["sha256"],
            chunk_count=len(leaves),
        )

    def native_graph_depth_fixture(self) -> tuple[ByteStreamReceipt, dict]:
        """Return a minimal envelope stream and one opaque terminal ref."""

        envelope_stream = self.archive.store_byte_stream(
            b"{}", sequence_kind="render_plan_chunks"
        )
        terminal_ref = self.archive.storage.put_blob(
            b"depth-terminal",
            logical_kind="literal_chunk",
            media_type="application/vnd.nginx-qa.prompt-archive-json-fragment-v1",
        )
        return envelope_stream, terminal_ref

    def wrap_native_graph_ref(self, ref: dict, edge_count: int) -> dict:
        """Wrap a ref in canonical control objects, one serialized edge each."""

        wrapped = dict(ref)
        for _index in range(edge_count):
            wrapped = self.archive._put_control(
                {"child_ref": wrapped}, "json_value"
            )
        return wrapped

    def whole_document_current_plan(
        self,
        source: bytes,
        occurrence: ArchiveOccurrence,
    ) -> tuple[dict[str, dict], dict]:
        """Build a byte-equal but potentially non-canonical whole-document plan."""
        record = self.archive.build_record(
            "current_field",
            source,
            ordinal=0,
            provenance=self.archive._record_provenance(occurrence),
            occurred_at=occurrence.created_at,
        )
        sequence = self.archive.build_record_sequence("current_fields", [record])
        assert sequence is not None
        roots = {"current_fields": dict(sequence.root_ref)}
        envelope = {
            "schema_version": 1,
            "kind": "prompt_archive_response_envelope",
            "media_type": "application/vnd.nginx-qa.prompt-response-envelope-v1+json",
            "builder_version": "pas1-render-plan-builder-v1",
            "renderer_version": "pas1-concat-renderer-v1",
            "json_value_model": "pas1-recursive-json-merkle-v1",
            "response_id": occurrence.response_id,
            "response_kind": occurrence.response_kind,
            "source_byte_size": len(source),
            "source_sha256": hashlib.sha256(source).hexdigest(),
            "steps": [
                {
                    "step_kind": "current_field",
                    "source_start": 0,
                    "source_end": len(source),
                    "value_ordinal": 0,
                    "record_ref": dict(record.ref),
                }
            ],
        }
        return roots, envelope

    def assert_archive_error(self, operation) -> PromptArchiveStorageError:
        with self.assertRaises(PromptArchiveStorageError) as raised:
            operation()
        return raised.exception

    def test_native_round_trip_manifest_and_envelope_follow_closed_v1_contract(self) -> None:
        source = self.response(
            [{"event": "created", "escaped": "\\u0061", "number": 1.2300}]
        )
        occurrence = self.occurrence(1)

        built = self.archive.build_incremental_archive(
            source, occurrence, legacy_committed_bytes=source
        )
        envelope = self.envelope(built)

        self.assertEqual(built.archive_format, "incremental_transition_v1")
        self.assertEqual(self.archive.hydrate_archive_bytes(built.manifest_ref), source)
        self.validator("prompt-archive-manifest-v1.schema.json").validate(
            built.manifest
        )
        self.validator("prompt-archive-envelope-v1.schema.json").validate(envelope)

        manifest = built.manifest
        self.assertEqual(manifest["renderer_version"], "pas1-concat-renderer-v1")
        self.assertEqual(
            manifest["json_value_model"], "pas1-recursive-json-merkle-v1"
        )
        self.assertEqual(manifest["publication"]["mode"], "shadow")
        self.assertTrue(manifest["publication"]["legacy_authoritative"])
        self.assertEqual(manifest["source"]["source_kind"], "runtime_shadow")
        self.assertEqual(manifest["integrity"]["semantics"], "native-byte-equality")
        self.assertEqual(
            manifest["legacy_comparison"],
            {
                "byte_size": len(source),
                "sha256": hashlib.sha256(source).hexdigest(),
                "renderer_version": "pas1-concat-renderer-v1",
                "matched": True,
            },
        )
        self.assertEqual(set(manifest["roots"]), {"current_fields", "recent_activity"})

        self.assertEqual(envelope["response_id"], occurrence.response_id)
        self.assertEqual(envelope["response_kind"], occurrence.response_kind)
        self.assertEqual(envelope["source_byte_size"], len(source))
        self.assertEqual(envelope["source_sha256"], hashlib.sha256(source).hexdigest())
        self.assertEqual(envelope["steps"][0]["source_start"], 0)
        self.assertEqual(envelope["steps"][-1]["source_end"], len(source))
        for left, right in zip(envelope["steps"], envelope["steps"][1:]):
            self.assertEqual(left["source_end"], right["source_start"])
        sequence_steps = [
            step for step in envelope["steps"] if step["step_kind"] == "sequence"
        ]
        self.assertEqual(len(sequence_steps), 1)
        self.assertEqual(
            sequence_steps[0]["slot_id"], "project_state.recent_activity"
        )
        self.assertEqual(sequence_steps[0]["sequence_kind"], "events")
        self.assertEqual(sequence_steps[0]["index_root"], manifest["roots"]["recent_activity"])

        counts = manifest["counts"]
        self.assertEqual(counts["raw_chunks"], 0)
        classified = sum(
            counts[name]
            for name in (
                "records",
                "raw_chunks",
                "payload_chunks",
                "literal_chunks",
                "render_plan_chunks",
                "json_value_nodes",
                "json_edges",
                "index_nodes",
            )
        )
        self.assertLessEqual(counts["unique_referenced_objects"], classified)
        self.assertGreater(counts["records"], 0)
        self.assertGreater(counts["render_plan_chunks"], 0)
        self.assertGreater(counts["index_nodes"], 0)

    def test_all_promoted_nonempty_response_keeps_canonical_sequence_partition(self) -> None:
        source = b'{"project_state":{"recent_activity":[{"event":"created"}]}}'

        built = self.archive.build_incremental_archive(
            source,
            self.occurrence(1),
            legacy_committed_bytes=source,
        )
        envelope = self.envelope(built)

        self.assertEqual(self.archive.hydrate_archive_bytes(built.manifest_ref), source)
        self.validator("prompt-archive-manifest-v1.schema.json").validate(
            built.manifest
        )
        self.validator("prompt-archive-envelope-v1.schema.json").validate(envelope)
        self.assertEqual(set(built.manifest["roots"]), {"recent_activity"})
        sequence_steps = [
            step for step in envelope["steps"] if step["step_kind"] == "sequence"
        ]
        self.assertEqual(len(sequence_steps), 1)
        self.assertEqual(
            sequence_steps[0]["slot_id"],
            "project_state.recent_activity",
        )
        self.assertFalse(
            any(step["step_kind"] == "current_field" for step in envelope["steps"])
        )

    def test_empty_only_promoted_response_has_one_canonical_fine_grained_partition(self) -> None:
        cases = (
            (
                b'{"project_state":{"recent_activity":[]}}',
                b"[]",
            ),
            (
                b'{"project_state":{"history_with_patches":{"text":""}}}',
                b'""',
            ),
        )

        for ordinal, (source, empty_token) in enumerate(cases, start=10):
            with self.subTest(source=source):
                occurrence = self.occurrence(ordinal)
                built = self.archive.build_incremental_archive(
                    source,
                    occurrence,
                    legacy_committed_bytes=source,
                )
                envelope = self.envelope(built)
                empty_start = source.index(empty_token)

                self.assertEqual(
                    self.archive.hydrate_archive_bytes(built.manifest_ref), source
                )
                self.assertEqual(set(built.roots), {"current_fields"})
                current_steps = [
                    step
                    for step in envelope["steps"]
                    if step["step_kind"] == "current_field"
                ]
                self.assertEqual(len(current_steps), 1)
                self.assertEqual(
                    (
                        current_steps[0]["source_start"],
                        current_steps[0]["source_end"],
                    ),
                    (empty_start, empty_start + len(empty_token)),
                )
                self.assertNotEqual(
                    (current_steps[0]["source_start"], current_steps[0]["source_end"]),
                    (0, len(source)),
                )

                alternate_roots, alternate_envelope = (
                    self.whole_document_current_plan(source, occurrence)
                )
                error = self.assert_archive_error(
                    lambda: self.archive._render_native_envelope(
                        alternate_envelope, alternate_roots
                    )
                )
                self.assertEqual(error.code, "PROMPT_ARCHIVE_REFERENCE_INVALID")

    def test_mixed_empty_and_nonempty_promoted_values_have_one_canonical_partition(self) -> None:
        source = (
            b'{"project_state":{"recent_activity":[{"event":"created"}],'
            b'"history_with_patches":{"timeline":[],"text":""}}}'
        )
        occurrence = self.occurrence(12)

        built = self.archive.build_incremental_archive(
            source,
            occurrence,
            legacy_committed_bytes=source,
        )
        envelope = self.envelope(built)
        current_ranges = [
            (step["source_start"], step["source_end"])
            for step in envelope["steps"]
            if step["step_kind"] == "current_field"
        ]
        sequence_steps = [
            step for step in envelope["steps"] if step["step_kind"] == "sequence"
        ]

        self.assertEqual(self.archive.hydrate_archive_bytes(built.manifest_ref), source)
        self.assertEqual(set(built.roots), {"current_fields", "recent_activity"})
        self.assertEqual(len(sequence_steps), 1)
        self.assertEqual(
            sequence_steps[0]["slot_id"], "project_state.recent_activity"
        )
        self.assertEqual(
            current_ranges,
            [
                (source.index(b"[]"), source.index(b"[]") + 2),
                (source.index(b'""'), source.index(b'""') + 2),
            ],
        )
        self.assertNotIn((0, len(source)), current_ranges)

        alternate_roots, alternate_envelope = self.whole_document_current_plan(
            source, occurrence
        )
        error = self.assert_archive_error(
            lambda: self.archive._render_native_envelope(
                alternate_envelope, alternate_roots
            )
        )
        self.assertEqual(error.code, "PROMPT_ARCHIVE_REFERENCE_INVALID")

    def test_wrong_promoted_target_shapes_are_rejected_by_builder_and_renderer(self) -> None:
        cases = (
            b'{"project_state":{"recent_activity":{"event":"created"}}}',
            b'{"project_state":{"recent_activity":1}}',
            b'{"project_state":{"history_with_patches":{"text":[]}}}',
        )

        for ordinal, source in enumerate(cases, start=20):
            with self.subTest(source=source):
                occurrence = self.occurrence(ordinal)
                before_inventory = self.archive.storage_inventory()
                before_objects = self.archive.work_metrics().new_object_count
                builder_error = self.assert_archive_error(
                    lambda: self.archive.build_incremental_archive(
                        source,
                        occurrence,
                        legacy_committed_bytes=source,
                    )
                )
                self.assertEqual(
                    builder_error.code, "PROMPT_ARCHIVE_REQUEST_INVALID"
                )
                self.assertEqual(self.archive.storage_inventory(), before_inventory)
                self.assertEqual(
                    self.archive.work_metrics().new_object_count,
                    before_objects,
                )

                forged_roots, forged_envelope = self.whole_document_current_plan(
                    source, occurrence
                )
                renderer_error = self.assert_archive_error(
                    lambda: self.archive._render_native_envelope(
                        forged_envelope, forged_roots
                    )
                )
                self.assertEqual(
                    renderer_error.code, "PROMPT_ARCHIVE_REFERENCE_INVALID"
                )

    def test_append_reuses_old_prefix_and_has_bounded_physical_delta(self) -> None:
        first_source = self.response([{"ordinal": value} for value in range(8)])
        before = self.archive.storage_inventory()
        metrics_before = self.archive.work_metrics()
        first = self.archive.build_incremental_archive(
            first_source,
            self.occurrence(1),
            legacy_committed_bytes=first_source,
        )
        after_first = self.archive.storage_inventory()
        metrics_after_first = self.archive.work_metrics()

        second_source = self.response([{"ordinal": value} for value in range(9)])
        second_occurrence = self.occurrence(
            2, predecessor_archive_id=first.archive_id
        )
        second = self.archive.build_incremental_archive(
            second_source,
            second_occurrence,
            previous_roots=first.roots,
            previous_manifest_ref=first.manifest_ref,
            legacy_committed_bytes=second_source,
        )
        after_second = self.archive.storage_inventory()
        metrics_after_second = self.archive.work_metrics()

        old_events = self.archive._receipt_from_root(
            first.roots["recent_activity"], "events"
        )
        new_events = self.archive._receipt_from_root(
            second.roots["recent_activity"], "events"
        )
        self.assertEqual((old_events.level, old_events.item_count), (0, 8))
        self.assertEqual((new_events.level, new_events.item_count), (0, 9))
        self.assertEqual(
            [leaf.ref for leaf in self.archive.sequence_leaves(old_events)],
            [leaf.ref for leaf in self.archive.sequence_leaves(new_events)[:8]],
        )
        self.assertEqual(self.archive.hydrate_archive_bytes(second.manifest_ref), second_source)
        self.assertEqual(
            second.manifest["source"]["predecessor_archive_id"], first.archive_id
        )

        initial_inventory = after_first.delta(before)
        append_inventory = after_second.delta(after_first)
        initial_work = metrics_after_first.delta(metrics_before)
        append_work = metrics_after_second.delta(metrics_after_first)
        self.assertGreater(append_inventory.file_count, 0)
        self.assertLess(append_inventory.file_count, initial_inventory.file_count)
        self.assertLess(append_inventory.content_bytes, initial_inventory.content_bytes)
        self.assertLess(append_work.new_object_count, initial_work.new_object_count)
        self.assertLess(append_work.new_stored_bytes, initial_work.new_stored_bytes)

    def test_current_field_replacement_reuses_unchanged_refs_and_provenance(self) -> None:
        first_source = (
            b'{"alpha":{"value":1},"beta":{"value":2},'
            b'"gamma":{"value":3}}'
        )
        first_occurrence = self.occurrence(1)
        first = self.archive.build_incremental_archive(
            first_source,
            first_occurrence,
            legacy_committed_bytes=first_source,
        )
        second_source = first_source.replace(b'"value":2', b'"value":9')
        second_occurrence = self.occurrence(
            2,
            predecessor_archive_id=first.archive_id,
        )

        with patch.object(
            self.archive,
            "_build_record",
            wraps=self.archive._build_record,
        ) as build_record:
            second = self.archive.build_incremental_archive(
                second_source,
                second_occurrence,
                previous_roots=first.roots,
                previous_manifest_ref=first.manifest_ref,
                legacy_committed_bytes=second_source,
            )

        first_sequence = self.archive._receipt_from_root(
            first.roots["current_fields"],
            "current_fields",
        )
        second_sequence = self.archive._receipt_from_root(
            second.roots["current_fields"],
            "current_fields",
        )
        first_leaves = self.archive.sequence_leaves(first_sequence)
        second_leaves = self.archive.sequence_leaves(second_sequence)
        self.assertEqual(len(first_leaves), 3)
        self.assertEqual(len(second_leaves), 3)
        self.assertEqual(first_leaves[0].ref, second_leaves[0].ref)
        self.assertNotEqual(first_leaves[1].ref, second_leaves[1].ref)
        self.assertEqual(first_leaves[2].ref, second_leaves[2].ref)
        current_builds = [
            call
            for call in build_record.call_args_list
            if call.args and call.args[0] == "current_field"
        ]
        self.assertEqual(len(current_builds), 1)

        record_media = "application/vnd.nginx-qa.prompt-archive-record-v1+json"
        unchanged_record = json.loads(self.archive._read_blob(
            second_leaves[0].ref,
            logical_kind="current_field_record",
            media_type=record_media,
        ))
        changed_record = json.loads(self.archive._read_blob(
            second_leaves[1].ref,
            logical_kind="current_field_record",
            media_type=record_media,
        ))
        self.assertEqual(
            unchanged_record["provenance"]["origin_response_id"],
            first_occurrence.response_id,
        )
        self.assertEqual(
            changed_record["provenance"]["origin_response_id"],
            second_occurrence.response_id,
        )
        self.assertEqual(
            self.archive.hydrate_archive_bytes(second.manifest_ref),
            second_source,
        )

    def test_current_fields_sparse_replacement_work_is_not_cardinality_linear(self) -> None:
        record_media = "application/vnd.nginx-qa.prompt-archive-record-v1+json"
        old_ref = self.archive.storage.put_blob(
            b"old",
            logical_kind="current_field_record",
            media_type=record_media,
        )
        new_ref = self.archive.storage.put_blob(
            b"new",
            logical_kind="current_field_record",
            media_type=record_media,
        )

        def accept_leaf(
            sequence_kind,
            ref,
            *,
            logical_bytes=None,
            ordinal=None,
        ):
            self.assertEqual(sequence_kind, "current_fields")
            return dict(ref)

        observations = []
        with patch.object(
            self.archive,
            "_validate_leaf_ref",
            side_effect=accept_leaf,
        ), patch.object(self.archive, "_hydrate_record", return_value=b"old"):
            for cardinality in (65, 4096):
                previous = self.archive.build_sequence(
                    "current_fields",
                    [SequenceLeaf(old_ref, 3)] * cardinality,
                )
                assert previous is not None
                before = self.archive.work_metrics()
                with self.archive._reference_budget():
                    replaced = self.archive._replace_sequence_leaves(
                        previous,
                        {cardinality - 1: SequenceLeaf(new_ref, 3)},
                        sequence_kind="current_fields",
                    )
                delta = self.archive.work_metrics().delta(before)
                observations.append(delta.index_nodes_serialized)
                self.assertEqual(replaced.item_count, cardinality)
                self.assertEqual(replaced.level, 1)

                old_root = self.archive._load_index_document(
                    previous.root_ref,
                    sequence_kind="current_fields",
                    expected_level=1,
                )
                new_root = self.archive._load_index_document(
                    replaced.root_ref,
                    sequence_kind="current_fields",
                    expected_level=1,
                )
                self.assertEqual(
                    [child["ref"] for child in old_root["children"][:-1]],
                    [child["ref"] for child in new_root["children"][:-1]],
                )
                self.assertNotEqual(
                    old_root["children"][-1]["ref"],
                    new_root["children"][-1]["ref"],
                )

        # Both trees have the same two-level shape. Replacing one ordinal
        # serializes exactly its leaf index and the root whether there are 65
        # or 4096 current fields; it never rebuilds the other sibling nodes.
        self.assertEqual(observations, [2, 2])

    def test_native_fanout_boundary_has_bounded_complete_graph_delta(self) -> None:
        built = None
        builds = []
        observations = []
        for ordinal, item_count in enumerate((63, 64, 65), start=1):
            source = self.response(
                [{"ordinal": value} for value in range(item_count)]
            )
            occurrence = self.occurrence(
                ordinal,
                predecessor_archive_id=None if built is None else built.archive_id,
            )
            before_inventory = self.archive.storage_inventory()
            before_work = self.archive.work_metrics()
            built = self.archive.build_incremental_archive(
                source,
                occurrence,
                previous_roots=None if built is None else built.roots,
                previous_manifest_ref=None if built is None else built.manifest_ref,
                legacy_committed_bytes=source,
            )
            builds.append(built)
            inventory_delta = self.archive.storage_inventory().delta(before_inventory)
            work_delta = self.archive.work_metrics().delta(before_work)
            observations.append((inventory_delta, work_delta))
            self.assertEqual(
                self.archive.hydrate_archive_bytes(built.manifest_ref),
                source,
            )
            self.assertEqual(inventory_delta.file_count, work_delta.new_object_count)
            self.assertEqual(inventory_delta.content_bytes, work_delta.new_stored_bytes)
            self.assertGreaterEqual(
                inventory_delta.allocated_bytes,
                inventory_delta.content_bytes,
            )

        initial_inventory, initial_work = observations[0]
        for inventory_delta, work_delta in observations[1:]:
            self.assertLess(inventory_delta.file_count, initial_inventory.file_count)
            self.assertLess(inventory_delta.content_bytes, initial_inventory.content_bytes)
            self.assertLess(inventory_delta.allocated_bytes, initial_inventory.allocated_bytes)
            self.assertLess(work_delta.new_object_count, initial_work.new_object_count)
            self.assertLess(work_delta.new_stored_bytes, initial_work.new_stored_bytes)
            # The native delta includes records, persistent indexes, envelope,
            # reachability accounting, and manifest -- not just the event tree.
            self.assertLessEqual(work_delta.new_object_count, 32)
            self.assertLessEqual(work_delta.new_stored_bytes, 128 * 1024)

        assert built is not None
        replay_source = self.response([{"ordinal": value} for value in range(65)])
        replay_occurrence = self.occurrence(
            3,
            predecessor_archive_id=builds[1].archive_id,
        )
        before_inventory = self.archive.storage_inventory()
        before_work = self.archive.work_metrics()
        replay = self.archive.build_incremental_archive(
            replay_source,
            replay_occurrence,
            previous_roots=builds[1].roots,
            previous_manifest_ref=builds[1].manifest_ref,
            legacy_committed_bytes=replay_source,
        )
        self.assertEqual(replay.archive_id, built.archive_id)
        self.assertEqual(self.archive.storage_inventory().delta(before_inventory).file_count, 0)
        replay_work = self.archive.work_metrics().delta(before_work)
        self.assertEqual(replay_work.new_object_count, 0)
        self.assertEqual(replay_work.new_stored_bytes, 0)

    def test_final_graph_pass_does_not_reuse_render_cache_after_corruption(self) -> None:
        source = self.response([{"ordinal": 0}])
        built = self.archive.build_incremental_archive(
            source,
            self.occurrence(1),
            legacy_committed_bytes=source,
        )
        events = self.archive._receipt_from_root(
            built.roots["recent_activity"],
            "events",
        )
        event_ref = self.archive.sequence_leaves(events)[0].ref
        event_path = self.archive.storage.blob_path(event_ref)
        original_start_epoch = self.archive._start_fresh_read_epoch
        corrupted = False

        def corrupt_then_start_epoch() -> None:
            nonlocal corrupted
            if not corrupted:
                event_path.unlink()
                corrupted = True
            original_start_epoch()

        with patch.object(
            self.archive,
            "_start_fresh_read_epoch",
            side_effect=corrupt_then_start_epoch,
        ):
            error = self.assert_archive_error(
                lambda: self.archive.hydrate_archive_bytes(built.manifest_ref)
            )
        self.assertTrue(corrupted)
        self.assertEqual(error.code, "PROMPT_ARCHIVE_BLOB_MISSING")

    def test_previous_roots_require_exact_verified_predecessor_manifest_before_writes(self) -> None:
        first_source = self.response([{"ordinal": 0}])
        first = self.archive.build_incremental_archive(
            first_source,
            self.occurrence(1),
            legacy_committed_bytes=first_source,
        )
        unrelated_source = self.response([{"ordinal": 99}])
        unrelated = self.archive.build_incremental_archive(
            unrelated_source,
            self.occurrence(2),
            legacy_committed_bytes=unrelated_source,
        )
        next_source = self.response([{"ordinal": 0}, {"ordinal": 1}])

        cases = (
            (
                "missing predecessor manifest proof",
                self.occurrence(3, predecessor_archive_id=first.archive_id),
                first.roots,
                None,
            ),
            (
                "fake predecessor manifest ref",
                self.occurrence(3, predecessor_archive_id=first.archive_id),
                first.roots,
                first.roots["current_fields"],
            ),
            (
                "manifest roots do not match supplied previous roots",
                self.occurrence(3, predecessor_archive_id=first.archive_id),
                unrelated.roots,
                first.manifest_ref,
            ),
            (
                "manifest archive id does not match claimed predecessor",
                self.occurrence(3, predecessor_archive_id=first.archive_id),
                unrelated.roots,
                unrelated.manifest_ref,
            ),
            (
                "previous roots supplied without predecessor identity",
                self.occurrence(3),
                first.roots,
                None,
            ),
        )

        for label, occurrence, previous_roots, previous_manifest_ref in cases:
            with self.subTest(label=label):
                before_inventory = self.archive.storage_inventory()
                before_objects = self.archive.work_metrics().new_object_count
                error = self.assert_archive_error(
                    lambda: self.archive.build_incremental_archive(
                        next_source,
                        occurrence,
                        previous_roots=previous_roots,
                        previous_manifest_ref=previous_manifest_ref,
                        legacy_committed_bytes=next_source,
                    )
                )
                self.assertEqual(error.code, "PROMPT_ARCHIVE_REFERENCE_INVALID")
                self.assertEqual(self.archive.storage_inventory(), before_inventory)
                self.assertEqual(
                    self.archive.work_metrics().new_object_count,
                    before_objects,
                )

    def test_authoritative_mode_requires_exact_gate_and_native_source(self) -> None:
        source = self.response([{"event": "created"}])
        shadow_occurrence = self.occurrence(1)
        native_occurrence = dataclasses.replace(
            shadow_occurrence,
            source={
                **shadow_occurrence.source,
                "source_kind": "native_runtime",
            },
        )

        missing_gate = self.assert_archive_error(
            lambda: self.archive.build_incremental_archive(
                source,
                native_occurrence,
                publication_mode="authoritative",
            )
        )
        self.assertEqual(missing_gate.code, "PROMPT_ARCHIVE_REFERENCE_INVALID")

        built = self.archive.build_incremental_archive(
            source,
            native_occurrence,
            publication_mode="authoritative",
            operator_gate="prompt-archive-authoritative-v1",
        )
        self.validator("prompt-archive-manifest-v1.schema.json").validate(
            built.manifest
        )
        self.assertEqual(built.manifest["source"]["source_kind"], "native_runtime")
        self.assertEqual(built.manifest["publication"]["mode"], "authoritative")
        self.assertFalse(built.manifest["publication"]["legacy_authoritative"])
        self.assertEqual(
            built.manifest["publication"]["operator_gate"],
            "prompt-archive-authoritative-v1",
        )
        self.assertNotIn("legacy_comparison", built.manifest)
        self.assertEqual(self.archive.hydrate_archive_bytes(built.manifest_ref), source)

    def test_occurrence_replay_is_idempotent_in_cas_and_inventory(self) -> None:
        first_source = self.response([{"ordinal": value} for value in range(8)])
        first = self.archive.build_incremental_archive(
            first_source,
            self.occurrence(1),
            legacy_committed_bytes=first_source,
        )
        source = self.response([{"ordinal": value} for value in range(9)])
        occurrence = self.occurrence(2, predecessor_archive_id=first.archive_id)
        built = self.archive.build_incremental_archive(
            source,
            occurrence,
            previous_roots=first.roots,
            previous_manifest_ref=first.manifest_ref,
            legacy_committed_bytes=source,
        )
        before_inventory = self.archive.storage_inventory()
        before_work = self.archive.work_metrics()

        replayed = self.archive.build_incremental_archive(
            source,
            occurrence,
            previous_roots=first.roots,
            previous_manifest_ref=first.manifest_ref,
            legacy_committed_bytes=source,
        )

        self.assertEqual(replayed.archive_id, built.archive_id)
        self.assertEqual(replayed.manifest_ref, built.manifest_ref)
        self.assertEqual(replayed.roots, built.roots)
        self.assertEqual(
            self.archive.storage_inventory().delta(before_inventory).file_count, 0
        )
        replay_work = self.archive.work_metrics().delta(before_work)
        self.assertEqual(replay_work.new_object_count, 0)
        self.assertEqual(replay_work.new_stored_bytes, 0)
        self.assertEqual(replay_work.new_uncompressed_bytes, 0)

    def test_native_manifest_tamper_is_rejected_and_duplicate_source_fails_closed(self) -> None:
        source = self.response([{"event": "created"}])
        built = self.archive.build_incremental_archive(
            source,
            self.occurrence(1),
            legacy_committed_bytes=source,
        )

        tampered = copy.deepcopy(built.manifest)
        tampered["counts"]["unique_referenced_objects"] += 1
        tampered_ref = self.archive.storage.put_blob(
            canonical_json_bytes_v1(tampered, maximum=256 * 1024),
            logical_kind="manifest",
            media_type=MANIFEST_MEDIA,
        )
        error = self.assert_archive_error(
            lambda: self.archive.hydrate_archive_bytes(tampered_ref)
        )
        self.assertIn(
            error.code,
            {"PROMPT_ARCHIVE_INTEGRITY_MISMATCH", "PROMPT_ARCHIVE_REFERENCE_INVALID"},
        )

        before = self.archive.storage_inventory()
        duplicate = b'{"project_state":{"recent_activity":[],"recent_activity":[]}}'
        error = self.assert_archive_error(
            lambda: self.archive.build_incremental_archive(
                duplicate,
                self.occurrence(3),
                legacy_committed_bytes=duplicate,
            )
        )
        self.assertEqual(error.code, "PROMPT_ARCHIVE_REQUEST_INVALID")
        self.assertEqual(self.archive.storage_inventory().delta(before).file_count, 0)

    def test_shadow_committed_byte_mismatch_fails_before_any_cas_write(self) -> None:
        source = self.response([{"event": "created"}], message="native")
        committed = self.response([{"event": "created"}], message="legacy")
        before_inventory = self.archive.storage_inventory()
        before_work = self.archive.work_metrics()

        error = self.assert_archive_error(
            lambda: self.archive.build_incremental_archive(
                source,
                self.occurrence(1),
                legacy_committed_bytes=committed,
            )
        )

        self.assertEqual(error.code, "PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
        inventory_delta = self.archive.storage_inventory().delta(before_inventory)
        work_delta = self.archive.work_metrics().delta(before_work)
        self.assertEqual(inventory_delta.file_count, 0)
        self.assertEqual(inventory_delta.content_bytes, 0)
        self.assertEqual(work_delta.new_object_count, 0)
        self.assertEqual(work_delta.new_stored_bytes, 0)
        self.assertEqual(work_delta.new_uncompressed_bytes, 0)

    def test_cross_role_same_digest_round_trips_and_is_counted_once_physically(self) -> None:
        # The string interior is stored as an opaque history payload while the
        # number is stored as a recursive-JSON literal. Both bytes are b"1".
        source = (
            b'{"project_state":{"history_with_patches":{"text":"1"}},'
            b'"scalar":1}'
        )
        built = self.archive.build_incremental_archive(
            source,
            self.occurrence(1),
            legacy_committed_bytes=source,
        )
        stream = self.envelope_stream(built)
        observed_refs: list[tuple[str, str, str, dict]] = []
        original_read = self.archive._read_blob

        def recording_read(ref, *, logical_kind, media_type):
            observed_refs.append(
                (ref["digest"], logical_kind, media_type, dict(ref))
            )
            return original_read(
                ref,
                logical_kind=logical_kind,
                media_type=media_type,
            )

        with patch.object(self.archive, "_read_blob", side_effect=recording_read):
            counted = self.archive._native_graph_counts(
                built.roots,
                stream,
                logical_bytes=len(source),
            )

        digest = hashlib.sha256(b"1").hexdigest()
        same_content = [item for item in observed_refs if item[0] == digest]
        self.assertEqual(
            {(item[1], item[2]) for item in same_content},
            {
                (
                    "literal_chunk",
                    "application/vnd.nginx-qa.prompt-archive-json-fragment-v1",
                ),
                (
                    "payload_chunk",
                    "application/vnd.nginx-qa.prompt-archive-json-string-fragment-v1",
                ),
            },
        )
        self.assertEqual(
            len({self.archive.storage.blob_path(item[3]) for item in same_content}),
            1,
        )
        self.assertEqual(
            counted["unique_referenced_objects"],
            len({item[0] for item in observed_refs}),
        )
        self.assertEqual(counted, built.manifest["counts"])
        self.assertEqual(self.archive.hydrate_archive_bytes(built.manifest_ref), source)

    def test_native_graph_accepts_reference_at_exact_depth_512(self) -> None:
        envelope_stream, terminal_ref = self.native_graph_depth_fixture()
        # The root is depth 1.  Each wrapper contributes one serialized ref
        # edge, so 511 wrappers place the terminal exactly at depth 512.
        root = self.wrap_native_graph_ref(terminal_ref, 511)

        counts = self.archive._native_graph_counts(
            {"raw_bytes": root}, envelope_stream, logical_bytes=0
        )

        self.assertEqual(counts["json_value_nodes"], 511)
        self.assertEqual(counts["literal_chunks"], 1)
        self.assertEqual(counts["unique_referenced_objects"], 514)
        self.assertEqual(counts["graph_edges"], 512)

    def test_native_graph_rejects_reference_at_depth_513(self) -> None:
        envelope_stream, terminal_ref = self.native_graph_depth_fixture()
        # One more wrapper puts the terminal at depth 513 and must fail
        # closed before it is opened or accepted as reachable.
        root = self.wrap_native_graph_ref(terminal_ref, 512)

        error = self.assert_archive_error(
            lambda: self.archive._native_graph_counts(
                {"raw_bytes": root}, envelope_stream, logical_bytes=0
            )
        )

        self.assertEqual(error.code, "PROMPT_ARCHIVE_LIMIT_EXCEEDED")

    def test_native_graph_memo_does_not_hide_deeper_shared_subtree(self) -> None:
        envelope_stream, terminal_ref = self.native_graph_depth_fixture()
        shared = self.wrap_native_graph_ref(terminal_ref, 1)
        # _ROOT_ORDER visits raw_bytes first, memoizing ``shared`` at depth 1.
        # The second root reaches that same control object at depth 512; its
        # terminal child is therefore depth 513 and still has to be rejected.
        deep_root = self.wrap_native_graph_ref(shared, 511)

        error = self.assert_archive_error(
            lambda: self.archive._native_graph_counts(
                {
                    "raw_bytes": shared,
                    "current_fields": deep_root,
                },
                envelope_stream,
                logical_bytes=0,
            )
        )

        self.assertEqual(error.code, "PROMPT_ARCHIVE_LIMIT_EXCEEDED")

    def test_render_rejects_byte_equal_alternate_array_record_partition(self) -> None:
        source = self.response([1, 2])
        occurrence = self.occurrence(1)
        built = self.archive.build_incremental_archive(
            source,
            occurrence,
            legacy_committed_bytes=source,
        )
        envelope = self.envelope(built)
        self.assertEqual(
            self.archive._render_native_envelope(envelope, built.roots), source
        )

        # Repackage the entire array as one valid event record and move both
        # brackets out of framing. The bytes remain identical, but this is not
        # the parser-derived V1 array partition and therefore must fail closed.
        alternate_record = self.archive.build_record(
            "event",
            b"[1, 2]",
            ordinal=0,
            provenance={
                "source_kind": "runtime_transition",
                "origin_response_id": occurrence.response_id,
                "origin_transition_id": occurrence.transition_id,
                "source_id": occurrence.occurrence_id,
                "source_commit": occurrence.source["source_commit"],
                "assignment_id": occurrence.source["assignment_id"],
            },
            occurred_at=occurrence.created_at,
        )
        alternate_events = self.archive.build_record_sequence(
            "events", [alternate_record]
        )
        assert alternate_events is not None
        alternate_roots = copy.deepcopy(dict(built.roots))
        alternate_roots["recent_activity"] = dict(alternate_events.root_ref)
        alternate_envelope = copy.deepcopy(envelope)
        sequence_step = next(
            step
            for step in alternate_envelope["steps"]
            if step["step_kind"] == "sequence"
        )
        sequence_step["index_root"] = dict(alternate_events.root_ref)
        sequence_step["prefix_b64"] = ""
        sequence_step["separator_b64"] = ""
        sequence_step["suffix_b64"] = ""

        error = self.assert_archive_error(
            lambda: self.archive._render_native_envelope(
                alternate_envelope, alternate_roots
            )
        )
        self.assertEqual(error.code, "PROMPT_ARCHIVE_REFERENCE_INVALID")

    def test_wrong_deterministic_response_id_fails_before_writes_and_golden_is_stable(self) -> None:
        # Independent frozen migration vector: changing PAS1 field order,
        # null framing, or domain separation must break this assertion.
        self.assertEqual(
            response_id_v1(
                "9000",
                None,
                "migration-snapshot",
                "migration",
                "migration-1",
            ),
            "parresp1-414f4b1d086de160366fa809b875272ddba775141671c15d54cccda7aa73884c",
        )
        source = self.response([{"event": "created"}])
        wrong = dataclasses.replace(
            self.occurrence(7), response_id="parresp1-" + ("f" * 64)
        )
        before_inventory = self.archive.storage_inventory()
        before_work = self.archive.work_metrics()

        error = self.assert_archive_error(
            lambda: self.archive.build_incremental_archive(
                source,
                wrong,
                legacy_committed_bytes=source,
            )
        )

        self.assertEqual(error.code, "PROMPT_ARCHIVE_REFERENCE_INVALID")
        inventory_delta = self.archive.storage_inventory().delta(before_inventory)
        work_delta = self.archive.work_metrics().delta(before_work)
        self.assertEqual(inventory_delta.file_count, 0)
        self.assertEqual(inventory_delta.content_bytes, 0)
        self.assertEqual(work_delta.new_object_count, 0)
        self.assertEqual(work_delta.new_stored_bytes, 0)
        self.assertEqual(work_delta.new_uncompressed_bytes, 0)


if __name__ == "__main__":
    unittest.main()
