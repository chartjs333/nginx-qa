import ctypes
import hashlib
import json
import os
import stat
import tempfile
import unittest
from collections.abc import Mapping
from pathlib import Path

from nginx_qa.prompt_archive_incremental import (
    ArchiveOccurrence,
    ByteStreamReceipt,
    PromptArchiveIncremental,
    response_id_v1,
)
from nginx_qa.prompt_archive_storage import PromptArchiveStorage


PROJECT_ID = "9000"
SPRINT_ID = "sprint-native-full-fixture"


def _allocated_bytes(path: Path, metadata: os.stat_result) -> int:
    """Measure allocation independently from PromptArchiveIncremental."""

    blocks = getattr(metadata, "st_blocks", None)
    if type(blocks) is int:
        return blocks * 512
    if os.name != "nt":
        return metadata.st_size
    from ctypes import wintypes

    high = wintypes.DWORD()
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    get_size = kernel32.GetCompressedFileSizeW
    get_size.argtypes = (wintypes.LPCWSTR, ctypes.POINTER(wintypes.DWORD))
    get_size.restype = wintypes.DWORD
    ctypes.set_last_error(0)
    low = get_size(str(path), ctypes.byref(high))
    if low == 0xFFFFFFFF and ctypes.get_last_error() != 0:
        raise OSError(ctypes.get_last_error(), "GetCompressedFileSizeW failed")
    return (int(high.value) << 32) | int(low)


class PromptArchiveNativeFullFixtureTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary_directory.name)
        self.storage = PromptArchiveStorage(
            self.base / "runtime",
            PROJECT_ID,
            protected_roots=(),
        )
        self.archive = PromptArchiveIncremental(
            self.storage,
            PROJECT_ID,
            SPRINT_ID,
        )

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    @staticmethod
    def occurrence(
        ordinal: int,
        *,
        predecessor_archive_id: str | None = None,
    ) -> ArchiveOccurrence:
        occurrence_id = f"fixture-occurrence-{ordinal}"
        source = {
            "source_kind": "runtime_shadow",
            "source_commit": "c" * 40,
            "assignment_id": "assignment-native-full-fixture",
            "review_id": "review-native-full-fixture",
            "result_key": "result-native-full-fixture",
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
            transition_id=f"fixture-transition-{ordinal}",
            source=source,
            authorization={
                "auth_model_version": "prompt-archive-authorization-v1",
                "principal_kind": "synthetic_test",
                "principal_id": "native-full-fixture-runner",
                "binding_id": None,
                "scope_source": "synthetic_test",
                "effective_scope_revision": None,
                "effective_scope_sha256": None,
                "decision_occurrence_id": f"fixture-decision-{ordinal}",
            },
            created_at=f"2026-10-11T12:{ordinal:02d}:00Z",
            verified_at=f"2026-10-11T12:{ordinal:02d}:01Z",
        )

    @staticmethod
    def fixture(*, changed_leaf: str) -> bytes:
        shared_event = {
            "event": "accepted",
            "details": {"actor": "orchestrator", "ordinal": 7},
        }
        shared_patch = {
            "path": "docs/example.md",
            "patch": "@@ -1 +1 @@",
            "metadata": {"commit": "d" * 40, "ordinal": 3},
        }
        source_provenance = {
            "source_type": "api",
            "message_id": "sha256:" + ("e" * 64),
            "observed_at": "2026-10-11T11:59:59Z",
            "lineage": {"producer": "fixture", "version": 1},
        }
        document = {
            "project_state": {
                "recent_activity": [shared_event],
                "history_with_patches": {
                    "text": "first line\\nsecond line",
                    "timeline": [shared_event],
                    "patches": [shared_patch],
                    "summary": {"preserved": True},
                },
                "activity_with_patches": [shared_event],
                "code_patches": [shared_patch],
                "future_project_state": {"schema": 2},
            },
            "work_history": [shared_event],
            "assignment": {
                "assignment_id": "assignment-native-full-fixture",
                "execution_audit": {"checkpoints": [shared_event]},
            },
            "result": {
                "kind": "result-fixture",
                "payload": {"status": "DONE", "nested": {"score": 1.2300}},
                "source_provenance": source_provenance,
            },
            "feedback": {
                "kind": "feedback-fixture",
                "payload": {"decision": "APPROVE", "notes": ["a", "b"]},
                "source_provenance": source_provenance,
            },
            "review_context": {
                "kind": "review-context-fixture",
                "payload": {"reviewers": ["api", "operations"]},
                "source_provenance": source_provenance,
            },
            "source_provenance": source_provenance,
            "future_extension_v99": {
                "kind": "unknown-future-field",
                "stable_sibling": {"nested": {"value": "stable"}},
                "changed_sibling": {
                    "keep": {"nested": [1, 2, 3]},
                    "leaf": changed_leaf,
                },
            },
        }
        # Non-canonical spaces and a CRLF terminator are intentional. Exact
        # byte reconstruction, rather than decoded JSON equivalence, matters.
        return (
            json.dumps(
                document,
                ensure_ascii=False,
                separators=(", ", ": "),
            ).encode("utf-8")
            + b"\r\n"
        )

    def _envelope_stream(self, built) -> ByteStreamReceipt:
        descriptor = built.manifest["envelope"]
        sequence = self.archive._receipt_from_root(
            descriptor["chunks_root"],
            "render_plan_chunks",
        )
        return ByteStreamReceipt(
            sequence=sequence,
            byte_size=descriptor["byte_size"],
            sha256=descriptor["sha256"],
            chunk_count=len(self.archive.sequence_leaves(sequence)),
        )

    @staticmethod
    def _nested_refs(value):
        if isinstance(value, Mapping):
            if value.get("kind") == "prompt_archive_blob_ref":
                yield dict(value)
                return
            for child in value.values():
                yield from PromptArchiveNativeFullFixtureTests._nested_refs(child)
        elif type(value) is list:
            for child in value:
                yield from PromptArchiveNativeFullFixtureTests._nested_refs(child)

    def _independent_reachability(self, built, source_size: int):
        """Traverse public ref documents without using _native_graph_counts."""

        roles = {
            "records": set(),
            "raw_chunks": set(),
            "payload_chunks": set(),
            "literal_chunks": set(),
            "render_plan_chunks": set(),
            "json_value_nodes": set(),
            "json_edges": set(),
            "index_nodes": set(),
        }
        physical = {}
        typed_seen = set()
        expanded_controls = set()
        graph_edges = 0

        def visit(ref):
            nonlocal graph_edges
            copied = dict(ref)
            digest = copied["digest"]
            logical_kind = copied["logical_kind"]
            media_type = copied["media_type"]
            typed = (digest, logical_kind, media_type)
            signature = {
                name: value
                for name, value in copied.items()
                if name not in {"logical_kind", "media_type"}
            }
            if digest in physical:
                self.assertEqual(physical[digest], signature)
            else:
                physical[digest] = signature

            if logical_kind == "index_node":
                roles["index_nodes"].add(digest)
            elif logical_kind == "json_value":
                roles["json_value_nodes"].add(digest)
            elif logical_kind == "json_edge":
                roles["json_edges"].add(digest)
            elif logical_kind.endswith("_record"):
                roles["records"].add(digest)
            elif logical_kind in {
                "raw_chunk",
                "payload_chunk",
                "literal_chunk",
                "render_plan_chunk",
            }:
                roles[logical_kind + "s"].add(digest)
            else:  # pragma: no cover - makes future role additions explicit.
                self.fail(f"unexpected graph role: {logical_kind}")

            if typed in typed_seen:
                return
            typed_seen.add(typed)
            raw = self.storage.read_blob(copied)
            is_control = logical_kind in {"index_node", "json_value", "json_edge"} or (
                logical_kind.endswith("_record")
            )
            if is_control and digest not in expanded_controls:
                document = json.loads(raw.decode("utf-8"))
                children = list(self._nested_refs(document))
                graph_edges += len(children)
                expanded_controls.add(digest)
                for child in children:
                    visit(child)

        for ref in built.roots.values():
            visit(ref)
        envelope_stream = self._envelope_stream(built)
        visit(envelope_stream.sequence.root_ref)
        envelope = json.loads(
            self.archive.hydrate_byte_stream(envelope_stream).decode("utf-8")
        )
        envelope_children = list(self._nested_refs(envelope))
        graph_edges += len(envelope_children)
        for child in envelope_children:
            visit(child)

        counts = {name: len(digests) for name, digests in roles.items()}
        counts.update(
            {
                "unique_referenced_objects": len(physical),
                "graph_edges": graph_edges,
                "logical_bytes": source_size,
                "referenced_stored_bytes": sum(
                    item["stored_size"] for item in physical.values()
                ),
                "referenced_uncompressed_bytes": sum(
                    item["uncompressed_size"] for item in physical.values()
                ),
            }
        )
        return counts, set(physical)

    def _physical_inventory(self):
        files = []
        directories = 0
        root = self.storage._project_root
        if root.exists():
            for path in root.rglob("*"):
                metadata = path.lstat()
                self.assertFalse(stat.S_ISLNK(metadata.st_mode), path)
                if stat.S_ISDIR(metadata.st_mode):
                    directories += 1
                elif stat.S_ISREG(metadata.st_mode):
                    files.append((path, metadata))
                else:  # pragma: no cover - CAS inventory is regular files/dirs.
                    self.fail(f"unexpected filesystem entry: {path}")
        return {
            "files": len(files),
            "directories": directories,
            "content": sum(metadata.st_size for _path, metadata in files),
            "allocated": sum(
                _allocated_bytes(path, metadata) for path, metadata in files
            ),
            "digests": {path.stem for path, _metadata in files},
        }

    def _record_payloads(self, roots):
        sequence = self.archive._receipt_from_root(
            roots["current_fields"],
            "current_fields",
        )
        observed = []
        for leaf in self.archive.sequence_leaves(sequence):
            raw = self.archive._hydrate_record(
                leaf.ref,
                expected_record_type="current_field",
            )
            record = json.loads(self.storage.read_blob(leaf.ref).decode("utf-8"))
            observed.append((json.loads(raw), dict(leaf.ref), record["payload"]["value_root"]))
        return observed

    def _object_children(self, value_ref):
        document = json.loads(self.storage.read_blob(value_ref).decode("utf-8"))
        self.assertEqual(document["node_kind"], "object")
        sequence = self.archive._receipt_from_root(
            document["entries_root"],
            "json_edges",
        )
        children = {}
        for leaf in self.archive.sequence_leaves(sequence):
            edge = json.loads(self.storage.read_blob(leaf.ref).decode("utf-8"))
            key_token = self.archive._hydrate_literal_span(
                edge["key_token"],
                parent_reference_depth=0,
            )
            children[json.loads(key_token)] = dict(edge["value_ref"])
        return children

    @staticmethod
    def _payload_by_kind(payloads, kind):
        matches = [
            item
            for item in payloads
            if isinstance(item[0], dict) and item[0].get("kind") == kind
        ]
        if len(matches) != 1:
            raise AssertionError(f"expected one {kind!r} current payload, got {len(matches)}")
        return matches[0]

    def test_full_native_fixture_round_trips_deduplicates_and_reuses_inner_siblings(self) -> None:
        first_source = self.fixture(changed_leaf="OLD")
        first = self.archive.build_incremental_archive(
            first_source,
            self.occurrence(1),
            legacy_committed_bytes=first_source,
        )

        expected_roots = {
            "current_fields",
            "recent_activity",
            "history_text",
            "history_timeline",
            "activity_with_patches",
            "history_patches",
            "code_patches",
            "work_history",
            "execution_audit",
        }
        self.assertEqual(set(first.roots), expected_roots)
        self.assertEqual(
            self.archive.hydrate_archive_bytes(first.manifest_ref),
            first_source,
        )
        self.assertEqual(first.hydrated_size, len(first_source))
        self.assertEqual(first.hydrated_sha256, hashlib.sha256(first_source).hexdigest())

        # Four structural aliases have byte-identical event payloads, and two
        # patch aliases have byte-identical patch payloads. Their immutable
        # sequence roots therefore collapse to one physical graph each.
        event_aliases = {
            first.roots[name]["digest"]
            for name in (
                "recent_activity",
                "history_timeline",
                "activity_with_patches",
                "work_history",
            )
        }
        patch_aliases = {
            first.roots[name]["digest"]
            for name in ("history_patches", "code_patches")
        }
        self.assertEqual(len(event_aliases), 1)
        self.assertEqual(len(patch_aliases), 1)

        # The execution-audit record has a distinct record type, but its
        # byte-identical nested event still reuses the recursive JSON value.
        event_sequence = self.archive._receipt_from_root(
            first.roots["recent_activity"], "events"
        )
        audit_sequence = self.archive._receipt_from_root(
            first.roots["execution_audit"], "execution_audit"
        )
        event_record = json.loads(
            self.storage.read_blob(self.archive.sequence_leaves(event_sequence)[0].ref)
        )
        audit_record = json.loads(
            self.storage.read_blob(self.archive.sequence_leaves(audit_sequence)[0].ref)
        )
        self.assertEqual(
            event_record["payload"]["value_root"],
            audit_record["payload"]["value_root"],
        )

        first_payloads = self._record_payloads(first.roots)
        result = self._payload_by_kind(first_payloads, "result-fixture")
        feedback = self._payload_by_kind(first_payloads, "feedback-fixture")
        review = self._payload_by_kind(first_payloads, "review-context-fixture")
        provenance = next(
            item
            for item in first_payloads
            if isinstance(item[0], dict) and item[0].get("source_type") == "api"
        )
        provenance_refs = {
            self._object_children(item[2])["source_provenance"]["digest"]
            for item in (result, feedback, review)
        }
        provenance_refs.add(provenance[2]["digest"])
        self.assertEqual(len(provenance_refs), 1)

        future_first = self._payload_by_kind(first_payloads, "unknown-future-field")
        future_first_children = self._object_children(future_first[2])
        changed_first_children = self._object_children(
            future_first_children["changed_sibling"]
        )

        second_source = self.fixture(changed_leaf="NEW")
        second = self.archive.build_incremental_archive(
            second_source,
            self.occurrence(2, predecessor_archive_id=first.archive_id),
            previous_roots=first.roots,
            previous_manifest_ref=first.manifest_ref,
            legacy_committed_bytes=second_source,
        )
        self.assertEqual(
            self.archive.hydrate_archive_bytes(second.manifest_ref),
            second_source,
        )
        for root_name in expected_roots - {"current_fields"}:
            self.assertEqual(first.roots[root_name], second.roots[root_name])

        second_payloads = self._record_payloads(second.roots)
        future_second = self._payload_by_kind(second_payloads, "unknown-future-field")
        future_second_children = self._object_children(future_second[2])
        changed_second_children = self._object_children(
            future_second_children["changed_sibling"]
        )
        self.assertNotEqual(future_first[1], future_second[1])
        self.assertEqual(
            future_first_children["stable_sibling"],
            future_second_children["stable_sibling"],
        )
        self.assertNotEqual(
            future_first_children["changed_sibling"],
            future_second_children["changed_sibling"],
        )
        self.assertEqual(
            changed_first_children["keep"],
            changed_second_children["keep"],
        )
        self.assertNotEqual(
            changed_first_children["leaf"],
            changed_second_children["leaf"],
        )

        # All unrelated nested current values retain their records (including
        # their original provenance) across the one-leaf successor change.
        for kind in (
            "result-fixture",
            "feedback-fixture",
            "review-context-fixture",
        ):
            self.assertEqual(
                self._payload_by_kind(first_payloads, kind)[1],
                self._payload_by_kind(second_payloads, kind)[1],
            )

    def test_first_build_inventory_and_reachability_are_independently_accounted(self) -> None:
        source = self.fixture(changed_leaf="OLD")
        before_api = self.archive.storage_inventory()
        before_physical = self._physical_inventory()
        before_work = self.archive.work_metrics()

        built = self.archive.build_incremental_archive(
            source,
            self.occurrence(1),
            legacy_committed_bytes=source,
        )

        after_api = self.archive.storage_inventory()
        after_physical = self._physical_inventory()
        work_delta = self.archive.work_metrics().delta(before_work)
        api_delta = after_api.delta(before_api)
        independent_delta = {
            "files": after_physical["files"] - before_physical["files"],
            "content": after_physical["content"] - before_physical["content"],
            "allocated": after_physical["allocated"] - before_physical["allocated"],
        }
        self.assertEqual(api_delta.file_count, independent_delta["files"])
        self.assertEqual(api_delta.content_bytes, independent_delta["content"])
        self.assertEqual(api_delta.allocated_bytes, independent_delta["allocated"])
        self.assertEqual(api_delta.file_count, work_delta.new_object_count)
        self.assertEqual(api_delta.content_bytes, work_delta.new_stored_bytes)
        self.assertGreater(api_delta.allocated_bytes, 0)

        independent_counts, reachable_digests = self._independent_reachability(
            built,
            len(source),
        )
        self.assertEqual(independent_counts, built.manifest["counts"])
        self.assertEqual(
            after_physical["digests"],
            reachable_digests | {built.manifest_ref["digest"]},
        )
        self.assertEqual(
            after_physical["files"],
            independent_counts["unique_referenced_objects"] + 1,
        )
        self.assertEqual(
            after_physical["content"],
            independent_counts["referenced_stored_bytes"]
            + built.manifest_ref["stored_size"],
        )


if __name__ == "__main__":
    unittest.main()
