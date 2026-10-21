import copy
import dataclasses
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from nginx_qa.prompt_archive_incremental import (
    ArchiveOccurrence,
    PromptArchiveIncremental,
    pas1_id_v1,
    response_id_v1,
    sprint_storage_key_v1,
)
from nginx_qa.prompt_archive_latest import ExternalLatestCoordinator
from nginx_qa.prompt_archive_occurrence_store import (
    PromptArchiveOccurrenceProof,
    PromptArchiveOccurrenceSource,
)
from nginx_qa.prompt_archive_runtime import (
    PromptArchiveRuntime,
    PromptArchiveRuntimeConfig,
    PromptArchiveRuntimeRegistry,
    _reachable_set_document,
)
from nginx_qa.prompt_archive_storage import (
    PromptArchiveCancellationToken,
    PromptArchiveLimits,
    PromptArchiveStorage,
    PromptArchiveStorageError,
    canonical_json_bytes_v1,
)


PROJECT_ID = "9000"
SPRINT_ID = "sprint-runtime-tests"


class ClosedOccurrenceProver:
    """Test double for an immutable, locator-keyed durable occurrence store."""

    def __init__(self) -> None:
        self.records: dict[tuple[str, bytes], PromptArchiveOccurrenceProof] = {}
        self.load_calls = 0

    @staticmethod
    def _key(project_id: str, locator) -> tuple[str, bytes]:
        return (
            project_id,
            canonical_json_bytes_v1(dict(locator), maximum=64 * 1024),
        )

    @staticmethod
    def _copy_occurrence(occurrence: ArchiveOccurrence) -> ArchiveOccurrence:
        return dataclasses.replace(
            occurrence,
            source=copy.deepcopy(dict(occurrence.source)),
            authorization=copy.deepcopy(dict(occurrence.authorization)),
        )

    @classmethod
    def _copy_proof(
        cls,
        proof: PromptArchiveOccurrenceProof,
    ) -> PromptArchiveOccurrenceProof:
        return PromptArchiveOccurrenceProof(
            proof.status,
            cls._copy_occurrence(proof.occurrence),
            proof.source,
        )

    def register_occurrence(
        self,
        *,
        project_id,
        locator,
        occurrence,
        source,
    ) -> None:
        key = self._key(project_id, locator)
        frozen = self._copy_occurrence(occurrence)
        proof = PromptArchiveOccurrenceProof("verified", frozen, source)
        existing = self.records.get(key)
        if existing is not None and (
            existing.occurrence != frozen or existing.source != source
        ):
            raise PromptArchiveStorageError("PROMPT_ARCHIVE_GENERATION_CONFLICT")
        if existing is None:
            self.records[key] = proof

    def load_occurrence(self, *, project_id, locator):
        self.load_calls += 1
        observed = self.records.get(self._key(project_id, locator))
        return None if observed is None else self._copy_proof(observed)


class ClosedCompatibilityPublisher:
    def __init__(self, root: Path, runtime: PromptArchiveRuntime) -> None:
        root.mkdir(parents=True, exist_ok=True)
        self.root = root
        self.transition_path = root / "unprepared.json"
        self.coordinator = ExternalLatestCoordinator(
            root / "latest-response.json",
            project_id=PROJECT_ID,
            archive_owner_verifier=runtime.verify_authoritative_archive_owner,
        )

    @property
    def target_authority_id(self):
        return self.coordinator.target_authority_id

    def prepare_authoritative(
        self,
        *,
        descriptor_bytes,
        descriptor_ref,
        occurrence,
        operation_id,
        published_at,
    ):
        self.transition_path = self.root / (
            f"{occurrence.response_kind}-{occurrence.response_id}.json"
        )
        return self.coordinator.prepare_descriptor_intent(
            descriptor_bytes,
            transition_stub_path=self.transition_path,
            occurrence_id=occurrence.occurrence_id,
            operation_id=operation_id,
            published_at=published_at,
            source_response_id=occurrence.response_id,
            source_sprint_id=occurrence.sprint_id,
            source_revision=occurrence.state_revision,
            descriptor_ref=descriptor_ref,
        )

    def publish_authoritative(self, compatibility_publication):
        descriptor = compatibility_publication["descriptor"]["document"]
        raw = canonical_json_bytes_v1(descriptor, maximum=64 * 1024)
        result = self.coordinator.publish_prepared_descriptor(
            raw,
            prepared_intent=compatibility_publication["latest_intent"],
            transition_stub_path=self.transition_path,
            archive_owner=compatibility_publication["archive_owner"],
            expected_target_authority_id=compatibility_publication[
                "target_authority_id"
            ],
        )
        return dataclasses.asdict(result)

    def verify_authoritative(self, compatibility_publication, publication):
        result = self.coordinator.verify_prepared_publication(
            prepared_intent=compatibility_publication["latest_intent"],
            publication=publication,
            archive_owner=compatibility_publication["archive_owner"],
        )
        return dataclasses.asdict(result)


class PromptArchiveRuntimeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary_directory.name)
        self.root = self.base / "archive"

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    @staticmethod
    def occurrence(
        ordinal: int = 1,
        *,
        committed_bytes: bytes | None = None,
    ) -> ArchiveOccurrence:
        occurrence_id = f"transition-runtime-{ordinal}"
        response_id = response_id_v1(
            PROJECT_ID,
            SPRINT_ID,
            "transition-response",
            "transition",
            occurrence_id,
        )
        return ArchiveOccurrence(
            project_id=PROJECT_ID,
            sprint_id=SPRINT_ID,
            response_id=response_id,
            response_kind="transition-response",
            occurrence_kind="transition",
            occurrence_id=occurrence_id,
            transition_id=occurrence_id,
            source={
                "source_kind": "runtime_shadow",
                "assignment_id": "assignment-runtime-1",
            },
            authorization={
                "auth_model_version": "prompt-archive-authorization-v1",
                "principal_kind": "legacy_local_principal",
                "principal_id": PROJECT_ID,
                "binding_id": None,
                "scope_source": "legacy_project_context",
                "effective_scope_revision": None,
                "effective_scope_sha256": None,
                "decision_occurrence_id": occurrence_id,
            },
            created_at=f"2026-10-11T12:{ordinal:02d}:00Z",
            verified_at=f"2026-10-11T12:{ordinal:02d}:01Z",
            source_response_key=(
                f"{ordinal:020x}" if committed_bytes is not None else None
            ),
            source_byte_size=(
                len(committed_bytes) if committed_bytes is not None else None
            ),
            source_sha256=(
                hashlib.sha256(committed_bytes).hexdigest()
                if committed_bytes is not None
                else None
            ),
        )

    def registry(
        self,
        *,
        mode: str = "shadow",
        occurrence_prover=None,
    ) -> PromptArchiveRuntimeRegistry:
        return PromptArchiveRuntimeRegistry(
            PromptArchiveRuntimeConfig(
                mode=mode,
                root=self.root,
                max_hydrated_bytes=8 * 1024 * 1024,
                operator_gate=(
                    "prompt-archive-authoritative-v1"
                    if mode == "authoritative"
                    else None
                ),
            ),
            occurrence_prover=occurrence_prover,
        )

    def assert_code(self, code: str, operation) -> None:
        with self.assertRaises(PromptArchiveStorageError) as raised:
            operation()
        self.assertEqual(raised.exception.code, code)

    def test_default_legacy_is_inert_and_authoritative_requires_exact_gate(self) -> None:
        config = PromptArchiveRuntimeConfig.from_environment({})
        self.assertEqual(config.mode, "legacy")
        self.assertIsNone(config.root)
        runtime = PromptArchiveRuntimeRegistry(config).for_project(PROJECT_ID)
        self.assertIsNone(runtime.publish_shadow(b'{"ok":true}', self.occurrence()))
        self.assertFalse(self.root.exists())
        self.assert_code(
            "PROMPT_ARCHIVE_NOT_FOUND",
            lambda: runtime.lookup_archive(
                sprint_id=SPRINT_ID,
                archive_id="par1-" + "0" * 64,
            ),
        )
        self.assert_code(
            "PROMPT_ARCHIVE_REQUEST_INVALID",
            lambda: PromptArchiveRuntimeConfig.from_environment(
                {
                    "NGINX_QA_PROMPT_ARCHIVE_MODE": "authoritative",
                    "NGINX_QA_PROMPT_ARCHIVE_ROOT": str(self.root),
                }
            ),
        )
        authoritative = PromptArchiveRuntimeConfig.from_environment(
            {
                "NGINX_QA_PROMPT_ARCHIVE_MODE": "authoritative",
                "NGINX_QA_PROMPT_ARCHIVE_ROOT": str(self.root),
                "NGINX_QA_PROMPT_ARCHIVE_OPERATOR_GATE": (
                    "prompt-archive-authoritative-v1"
                ),
            }
        )
        self.assertEqual(authoritative.mode, "authoritative")

    def test_authoritative_native_publish_is_discoverable_without_hot_path_hydration(
        self,
    ) -> None:
        runtime = self.registry(mode="authoritative").for_project(PROJECT_ID)
        raw = b'{"active_task":{"message":"bounded native response"},"response_compact":true}'
        occurrence = dataclasses.replace(
            self.occurrence(),
            source={"source_kind": "native_runtime", "assignment_id": "assignment-one"},
            source_response_key=None,
            source_byte_size=None,
            source_sha256=None,
        )
        with mock.patch.object(
            PromptArchiveIncremental,
            "hydrate_archive_bytes",
            side_effect=AssertionError("authoritative hot path hydrated content"),
        ):
            publication = runtime.publish_authoritative(
                raw,
                occurrence,
                compatibility_publisher=ClosedCompatibilityPublisher(
                    self.base / "prompt", runtime
                ),
            )
        selected = runtime.lookup_archive(
            sprint_id=SPRINT_ID,
            archive_id=publication.archive_id,
        )
        manifest = runtime.read_manifest(selected)
        self.assertEqual(manifest["publication"]["mode"], "authoritative")
        self.assertFalse(manifest["publication"]["legacy_authoritative"])
        self.assertNotIn("legacy_comparison", manifest)
        self.assertEqual(runtime.hydrate_content(selected), raw)
        self.assertEqual(runtime.descriptor_stub(publication)["archive_id"], publication.archive_id)

    def test_shadow_publish_is_exact_discoverable_and_idempotent(self) -> None:
        runtime = self.registry().for_project(PROJECT_ID)
        raw = (
            b'\r\n {"active_task":{"message":"keep complete task"},'
            b'"project_state":{"history_with_patches":{"timeline":[1,2,3]}},'
            b'"spelling":1.2300e+04}\r\n'
        )
        first = runtime.publish_shadow(raw, self.occurrence(committed_bytes=raw))
        self.assertIsNotNone(first)
        second = runtime.publish_shadow(raw, self.occurrence(committed_bytes=raw))
        self.assertEqual(second.archive_id, first.archive_id)
        self.assertEqual(second.pointer_id, first.pointer_id)
        selected = runtime.lookup_archive(
            sprint_id=SPRINT_ID,
            archive_id=first.archive_id,
        )
        manifest = runtime.read_manifest(selected)
        self.assertEqual(manifest["archive_id"], first.archive_id)
        self.assertEqual(manifest["publication"]["mode"], "shadow")
        self.assertTrue(manifest["legacy_comparison"]["matched"])
        self.assertEqual(runtime.hydrate_content(selected), raw)
        cancelled = PromptArchiveCancellationToken.with_timeout(5)
        cancelled.cancel()
        self.assert_code(
            "PROMPT_ARCHIVE_UNAVAILABLE",
            lambda: runtime.hydrate_content(
                selected,
                cancellation=cancelled,
            ),
        )
        self.assertEqual(first.scope["response_id"], self.occurrence().response_id)
        self.assertEqual(first.integrity["hydrated_size"], len(raw))
        self.assertEqual(first.created_at, self.occurrence().created_at)

        sprint_key = sprint_storage_key_v1(PROJECT_ID, SPRINT_ID)
        project_key = runtime.storage.project_storage_key
        pointer_path = (
            self.root
            / "projects"
            / project_key
            / "sprints"
            / sprint_key
            / "pointers"
            / "v1"
            / f"{self.occurrence().response_id}.json"
        )
        locator_path = (
            self.root
            / "projects"
            / project_key
            / "sprints"
            / sprint_key
            / "archives"
            / "v1"
            / f"{first.archive_id}.json"
        )
        self.assertTrue(pointer_path.is_file())
        self.assertTrue(locator_path.is_file())
        self.assertLessEqual(locator_path.stat().st_size, 64 * 1024)

        operation_id = runtime._operation_id(self.occurrence())
        operation_root = (
            self.root
            / "projects"
            / project_key
            / "operations"
            / "v1"
            / self.occurrence().response_id
            / operation_id
        )
        self.assertTrue((operation_root / "plan.json").is_file())
        current_journal = json.loads(
            (operation_root / "journal.json").read_text(encoding="utf-8")
        )
        self.assertEqual(current_journal["state"], "complete")
        self.assertEqual(current_journal["journal_generation"], 6)
        self.assertEqual(
            len(tuple((operation_root / "journal-generations").glob("*.json"))),
            7,
        )
        shard_id = current_journal["pending_registry"]["shard"]["shard_id"]
        pending_root = self.root / "projects" / project_key / "pending" / "v1"
        current_registry = json.loads(
            (pending_root / "shards" / f"{shard_id}.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(current_registry["entries"], [])
        self.assertGreaterEqual(
            len(tuple((pending_root / "generations" / shard_id).glob("*.json"))),
            8,
        )

    def test_publication_metrics_use_exact_accepted_cas_outcomes(self) -> None:
        registry = self.registry()
        runtime = registry.for_project(PROJECT_ID)
        raw = b'{"metric":"closed","payload":[1,2,3]}'
        occurrence = self.occurrence(9, committed_bytes=raw)

        with runtime.storage.account_cas_writes() as first_observed:
            first = runtime.publish_shadow(raw, occurrence)
        first_cas = first_observed.snapshot()
        first_metrics = registry.metrics.snapshot()
        self.assertIsNotNone(first)
        self.assertEqual(first_metrics.publication_count, 1)
        self.assertEqual(first_metrics.logical_bytes, len(raw))
        self.assertEqual(
            first_metrics.new_physical_bytes, first_cas.new_stored_bytes
        )
        self.assertEqual(first_metrics.dedup_hits, first_cas.dedup_hits)
        self.assertGreater(first_cas.new_object_count, 0)
        self.assertGreater(first_cas.new_stored_bytes, 0)

        with runtime.storage.account_cas_writes() as replay_observed:
            replay = runtime.publish_shadow(raw, occurrence)
        replay_cas = replay_observed.snapshot()
        replay_metrics = registry.metrics.snapshot()
        self.assertEqual(replay.archive_id, first.archive_id)
        self.assertEqual(replay_cas.new_object_count, 0)
        self.assertEqual(replay_cas.new_stored_bytes, 0)
        self.assertGreater(replay_cas.dedup_hits, 0)
        self.assertEqual(replay_metrics.publication_count, 2)
        self.assertEqual(replay_metrics.logical_bytes, len(raw) * 2)
        self.assertEqual(
            replay_metrics.new_physical_bytes, first_cas.new_stored_bytes
        )
        self.assertEqual(
            replay_metrics.dedup_hits,
            first_cas.dedup_hits + replay_cas.dedup_hits,
        )

    def test_failed_publication_does_not_enter_success_metrics(self) -> None:
        config = PromptArchiveRuntimeConfig(
            mode="shadow",
            root=self.root,
            max_hydrated_bytes=8 * 1024 * 1024,
        )

        def fail_after_plan(stage: str) -> None:
            if stage == "after_publication_plan":
                raise PromptArchiveStorageError("PROMPT_ARCHIVE_UNAVAILABLE")

        storage = PromptArchiveStorage(
            self.root,
            PROJECT_ID,
            protected_roots=(),
            limits=PromptArchiveLimits(
                max_hydrated_bytes=config.max_hydrated_bytes
            ),
            fault_hook=fail_after_plan,
        )
        runtime = PromptArchiveRuntime(config, PROJECT_ID, storage)
        raw = b'{"metric":"failed-publication"}'

        with storage.account_cas_writes() as observed:
            self.assert_code(
                "PROMPT_ARCHIVE_UNAVAILABLE",
                lambda: runtime.publish_shadow(
                    raw,
                    self.occurrence(10, committed_bytes=raw),
                ),
            )
        self.assertGreater(observed.snapshot().new_object_count, 0)
        self.assertEqual(runtime.metrics.snapshot().publication_count, 0)
        self.assertEqual(runtime.metrics.snapshot().logical_bytes, 0)
        self.assertEqual(runtime.metrics.snapshot().new_physical_bytes, 0)
        self.assertEqual(runtime.metrics.snapshot().dedup_hits, 0)

    def test_completed_legacy_plan_preserves_metadata_absence_on_upgrade_retry(
        self,
    ) -> None:
        config = PromptArchiveRuntimeConfig(
            mode="authoritative",
            root=self.root,
            max_hydrated_bytes=8 * 1024 * 1024,
            operator_gate="prompt-archive-authoritative-v1",
        )
        raw = b'{"upgrade":"completed-legacy-plan"}'
        occurrence = dataclasses.replace(
            self.occurrence(11),
            source={"source_kind": "native_runtime", "assignment_id": "a11"},
        )
        first_runtime = PromptArchiveRuntimeRegistry(config).for_project(PROJECT_ID)
        prompt_root = self.base / "prompt-upgrade-completed"
        first = first_runtime.publish_authoritative(
            raw,
            occurrence,
            compatibility_publisher=ClosedCompatibilityPublisher(
                prompt_root,
                first_runtime,
            ),
        )

        restarted = PromptArchiveRuntimeRegistry(config).for_project(PROJECT_ID)
        retried = restarted.publish_authoritative(
            raw,
            occurrence,
            compatibility_publisher=ClosedCompatibilityPublisher(
                prompt_root,
                restarted,
            ),
            raw_source_metadata={"source_type": "api", "title": "new-owner-data"},
            source_metadata_screen_factory=lambda: (_ for _ in ()).throw(
                AssertionError("legacy retry re-screened source metadata")
            ),
        )

        self.assertEqual(retried.archive_id, first.archive_id)
        selected = restarted.lookup_archive(
            sprint_id=SPRINT_ID,
            archive_id=retried.archive_id,
        )
        self.assertNotIn("source_metadata", selected.manifest["roots"])
        self.assertNotIn("source_metadata_receipt", selected.manifest)

    def test_plan_only_legacy_shadow_retry_preserves_metadata_absence(self) -> None:
        root = self.base / "plan-only-upgrade"
        config = PromptArchiveRuntimeConfig(
            mode="shadow",
            root=root,
            max_hydrated_bytes=8 * 1024 * 1024,
        )

        def fail_after_plan(stage: str) -> None:
            if stage == "after_publication_plan":
                raise PromptArchiveStorageError("PROMPT_ARCHIVE_UNAVAILABLE")

        storage = PromptArchiveStorage(
            root,
            PROJECT_ID,
            protected_roots=(),
            limits=PromptArchiveLimits(max_hydrated_bytes=config.max_hydrated_bytes),
            fault_hook=fail_after_plan,
        )
        prover = ClosedOccurrenceProver()
        failing = PromptArchiveRuntime(
            config,
            PROJECT_ID,
            storage,
            occurrence_prover=prover,
        )
        raw = b'{"upgrade":"plan-only-legacy-shadow"}'
        occurrence = self.occurrence(12, committed_bytes=raw)
        self.assert_code(
            "PROMPT_ARCHIVE_UNAVAILABLE",
            lambda: failing.publish_shadow(raw, occurrence),
        )

        restarted = PromptArchiveRuntimeRegistry(
            config,
            occurrence_prover=prover,
        ).for_project(PROJECT_ID)
        retried = restarted.publish_shadow(
            raw,
            occurrence,
            raw_source_metadata={"source_type": "api", "title": "new-owner-data"},
            source_metadata_screen_factory=lambda: (_ for _ in ()).throw(
                AssertionError("legacy plan retry re-screened source metadata")
            ),
        )

        selected = restarted.lookup_archive(
            sprint_id=SPRINT_ID,
            archive_id=retried.archive_id,
        )
        self.assertNotIn("source_metadata", selected.manifest["roots"])
        self.assertNotIn("source_metadata_receipt", selected.manifest)

    def test_reachable_set_rejects_one_digest_in_multiple_roles(self) -> None:
        storage = PromptArchiveStorage(self.root, PROJECT_ID, protected_roots=())
        manifest_ref = storage.put_blob(
            b"{}",
            logical_kind="manifest",
            media_type="application/vnd.nginx-qa.prompt-archive-manifest-v1+json",
        )
        literal_ref = storage.put_blob(
            b"1",
            logical_kind="literal_chunk",
            media_type="application/vnd.nginx-qa.prompt-archive-json-fragment-v1",
        )
        payload_ref = storage.put_blob(
            b"1",
            logical_kind="payload_chunk",
            media_type=(
                "application/vnd.nginx-qa.prompt-archive-json-string-fragment-v1"
            ),
        )
        self.assertEqual(literal_ref["digest"], payload_ref["digest"])
        self.assert_code(
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
            lambda: _reachable_set_document(
                (manifest_ref, literal_ref, payload_ref)
            ),
        )

    def test_recovery_graph_verification_rejects_deep_shared_subtree_before_mutation(
        self,
    ) -> None:
        runtime = self.registry(mode="authoritative").for_project(PROJECT_ID)
        incremental = PromptArchiveIncremental(
            runtime.storage,
            PROJECT_ID,
            SPRINT_ID,
        )
        refs: list[dict] = []
        terminal_ref = runtime.storage.put_blob(
            b"runtime-recovery-depth-terminal",
            logical_kind="literal_chunk",
            media_type=(
                "application/vnd.nginx-qa.prompt-archive-json-fragment-v1"
            ),
        )
        refs.append(terminal_ref)

        def wrap(ref: dict, edge_count: int) -> dict:
            wrapped = dict(ref)
            for _index in range(edge_count):
                wrapped = incremental._put_control(
                    {"child_ref": wrapped},
                    "json_value",
                )
                refs.append(wrapped)
            return wrapped

        shared = wrap(terminal_ref, 1)
        deep_root = wrap(shared, 510)
        manifest = {
            "a_shallow": shared,
            "z_deep": deep_root,
            "integrity": {
                "semantics": "native-byte-equality",
                "hydrated_size": 0,
                "hydrated_sha256": "0" * 64,
            },
            "legacy_comparison": None,
        }
        manifest_ref = incremental._put_control(manifest, "manifest")
        refs.append(manifest_ref)
        plan = {
            "operation_kind": "native_publish",
            "scope": {"sprint_id": SPRINT_ID},
            "intended_manifest": {
                "document": manifest,
                "manifest_ref": manifest_ref,
            },
            # This is the complete unique typed closure.  The old traversal
            # incorrectly accepted it after memoizing ``shared`` shallowly.
            "planned_reachable_set": _reachable_set_document(tuple(refs)),
        }
        journal = {"state": "intent"}

        def filesystem_snapshot() -> dict[str, bytes]:
            return {
                path.relative_to(self.root).as_posix(): path.read_bytes()
                for path in self.root.rglob("*")
                if path.is_file()
            }

        before = filesystem_snapshot()
        with mock.patch.object(
            runtime,
            "_intended_pointer_is_published",
            return_value=True,
        ):
            self.assert_code(
                "PROMPT_ARCHIVE_LIMIT_EXCEEDED",
                lambda: runtime._perform_state_action(plan, journal),
            )

        self.assertEqual(filesystem_snapshot(), before)

    def test_metadata_lookup_never_reads_leaf_closure_but_content_does(self) -> None:
        runtime = self.registry().for_project(PROJECT_ID)
        raw = (
            b'{"active_task":{"message":"bounded metadata"},'
            b'"project_state":{"recent_activity":[1,2,3]}}'
        )
        publication = runtime.publish_shadow(
            raw,
            self.occurrence(11, committed_bytes=raw),
        )
        observed_kinds: list[str] = []
        original_read_blob = runtime.storage.read_blob

        def counted_read_blob(ref, *args, **kwargs):
            observed_kinds.append(str(ref.get("logical_kind")))
            return original_read_blob(ref, *args, **kwargs)

        with mock.patch.object(
            runtime.storage,
            "read_blob",
            side_effect=counted_read_blob,
        ):
            selected = runtime.lookup_archive_metadata(
                sprint_id=SPRINT_ID,
                archive_id=publication.archive_id,
            )
            returned_manifest = runtime.read_manifest(selected)

        self.assertEqual(returned_manifest["archive_id"], publication.archive_id)
        self.assertTrue(observed_kinds)
        self.assertLessEqual(set(observed_kinds), {"pointer", "manifest"})
        self.assertNotIn("index_node", observed_kinds)
        self.assertNotIn("json_value", observed_kinds)
        self.assertNotIn("raw_chunk", observed_kinds)

        changed_identity = copy.deepcopy(returned_manifest)
        changed_identity["integrity"]["hydrated_sha256"] = "0" * 64
        changed_raw = canonical_json_bytes_v1(
            changed_identity,
            maximum=256 * 1024,
        )
        changed_ref = runtime.storage.put_blob(
            changed_raw,
            logical_kind="manifest",
            media_type=(
                "application/vnd.nginx-qa.prompt-archive-manifest-v1+json"
            ),
        )
        self.assert_code(
            "PROMPT_ARCHIVE_INTEGRITY_MISMATCH",
            lambda: PromptArchiveIncremental(
                runtime.storage,
                PROJECT_ID,
                SPRINT_ID,
            ).read_manifest_metadata(changed_ref),
        )

        observed_kinds.clear()
        with mock.patch.object(
            runtime.storage,
            "read_blob",
            side_effect=counted_read_blob,
        ):
            self.assertEqual(runtime.hydrate_content(selected), raw)
        self.assertIn("manifest", observed_kinds)
        self.assertTrue(
            any(kind not in {"pointer", "manifest"} for kind in observed_kinds)
        )

        missing_leaf = runtime.storage.blob_path(
            selected.manifest["envelope"]["chunks_root"]
        )
        missing_leaf.unlink()
        # Metadata remains independently available; content must still perform
        # full closure verification and fail before returning any bytes.
        runtime.lookup_archive_metadata(
            sprint_id=SPRINT_ID,
            archive_id=publication.archive_id,
        )
        self.assert_code(
            "PROMPT_ARCHIVE_BLOB_MISSING",
            lambda: runtime.hydrate_content(selected),
        )

    def test_restart_replays_registered_fault_boundaries(self) -> None:
        scenarios = (
            ("after_pending_registration", 1),
            ("after_journal_current", 1),
            ("after_pointer_publication", 1),
            ("after_journal_current", 7),
        )
        for ordinal, (target, target_occurrence) in enumerate(scenarios, start=20):
            with self.subTest(target=target, target_occurrence=target_occurrence):
                root = self.base / f"archive-{ordinal}"
                config = PromptArchiveRuntimeConfig(
                    mode="shadow",
                    root=root,
                    max_hydrated_bytes=8 * 1024 * 1024,
                )
                hits = 0

                def fail_once(stage: str) -> None:
                    nonlocal hits
                    if stage != target:
                        return
                    hits += 1
                    if hits == target_occurrence:
                        raise PromptArchiveStorageError(
                            "PROMPT_ARCHIVE_UNAVAILABLE"
                        )

                storage = PromptArchiveStorage(
                    root,
                    PROJECT_ID,
                    protected_roots=(),
                    limits=PromptArchiveLimits(
                        max_hydrated_bytes=config.max_hydrated_bytes
                    ),
                    fault_hook=fail_once,
                )
                occurrence_prover = ClosedOccurrenceProver()
                failing = PromptArchiveRuntime(
                    config,
                    PROJECT_ID,
                    storage,
                    occurrence_prover=occurrence_prover,
                )
                raw = canonical_json_bytes_v1(
                    {"fault_boundary": target, "ordinal": ordinal},
                    maximum=64 * 1024,
                )
                occurrence = self.occurrence(ordinal, committed_bytes=raw)
                self.assert_code(
                    "PROMPT_ARCHIVE_UNAVAILABLE",
                    lambda: failing.publish_shadow(raw, occurrence),
                )
                self.assertEqual(hits, target_occurrence)

                restarted_registry = PromptArchiveRuntimeRegistry(
                    config,
                    occurrence_prover=occurrence_prover,
                )
                restarted = restarted_registry.for_project(PROJECT_ID)
                self.assertEqual(
                    restarted_registry.validate_and_recover((PROJECT_ID,)), 1
                )
                publication = restarted.publish_shadow(raw, occurrence)
                selected = restarted.lookup_archive(
                    sprint_id=SPRINT_ID,
                    archive_id=publication.archive_id,
                )
                self.assertEqual(restarted.hydrate_content(selected), raw)
                self.assertEqual(restarted.recover_pending(), 0)

    def test_recovery_adopts_retained_successor_before_source_recheck(self) -> None:
        root = self.base / "retained-successor"
        config = PromptArchiveRuntimeConfig(
            mode="shadow",
            root=root,
            max_hydrated_bytes=8 * 1024 * 1024,
        )
        prover = ClosedOccurrenceProver()
        fault_hits = 0

        def fail_after_second_generation(stage: str) -> None:
            nonlocal fault_hits
            if stage != "after_journal_generation":
                return
            fault_hits += 1
            if fault_hits == 2:
                raise PromptArchiveStorageError("PROMPT_ARCHIVE_UNAVAILABLE")

        storage = PromptArchiveStorage(
            root,
            PROJECT_ID,
            protected_roots=(),
            limits=PromptArchiveLimits(
                max_hydrated_bytes=config.max_hydrated_bytes
            ),
            fault_hook=fail_after_second_generation,
        )
        failing = PromptArchiveRuntime(
            config,
            PROJECT_ID,
            storage,
            occurrence_prover=prover,
        )
        raw = canonical_json_bytes_v1(
            {"retained_successor": "adopt-before-source-proof"},
            maximum=64 * 1024,
        )
        occurrence = self.occurrence(50, committed_bytes=raw)
        self.assert_code(
            "PROMPT_ARCHIVE_UNAVAILABLE",
            lambda: failing.publish_shadow(raw, occurrence),
        )
        self.assertEqual(fault_hits, 2)

        operation_root = (
            root
            / "projects"
            / storage.project_storage_key
            / "operations"
            / "v1"
            / occurrence.response_id
            / failing._operation_id(occurrence)
        )
        current_before = json.loads(
            (operation_root / "journal.json").read_text(encoding="utf-8")
        )
        self.assertEqual(current_before["state"], "intent")
        generation_one = tuple(
            (operation_root / "journal-generations").glob("1-*.json")
        )
        self.assertEqual(len(generation_one), 1)
        self.assertEqual(
            json.loads(generation_one[0].read_text(encoding="utf-8"))["state"],
            "blobs_verified",
        )

        proof_key = next(iter(prover.records))
        proof = prover.records[proof_key]
        prover.records[proof_key] = PromptArchiveOccurrenceProof(
            "source_unavailable",
            proof.occurrence,
            proof.source,
        )
        prover.load_calls = 0
        restarted_registry = PromptArchiveRuntimeRegistry(
            config,
            occurrence_prover=prover,
        )
        restarted = restarted_registry.for_project(PROJECT_ID)
        self.assertEqual(
            restarted_registry.validate_and_recover((PROJECT_ID,)),
            1,
        )
        self.assertEqual(prover.load_calls, 0)
        current_after = json.loads(
            (operation_root / "journal.json").read_text(encoding="utf-8")
        )
        self.assertEqual(current_after["state"], "complete")
        self.assertEqual(
            len(tuple((operation_root / "journal-generations").glob("1-*.json"))),
            1,
        )
        selected = restarted.lookup_archive(
            sprint_id=SPRINT_ID,
            archive_id=current_after["archive_id"],
        )
        self.assertEqual(restarted.hydrate_content(selected), raw)

    def test_recovery_rejects_dual_retained_successors(self) -> None:
        root = self.base / "dual-retained-successors"
        config = PromptArchiveRuntimeConfig(
            mode="shadow",
            root=root,
            max_hydrated_bytes=8 * 1024 * 1024,
        )
        prover = ClosedOccurrenceProver()
        fault_hits = 0

        def fail_after_second_generation(stage: str) -> None:
            nonlocal fault_hits
            if stage != "after_journal_generation":
                return
            fault_hits += 1
            if fault_hits == 2:
                raise PromptArchiveStorageError("PROMPT_ARCHIVE_UNAVAILABLE")

        storage = PromptArchiveStorage(
            root,
            PROJECT_ID,
            protected_roots=(),
            limits=PromptArchiveLimits(
                max_hydrated_bytes=config.max_hydrated_bytes
            ),
            fault_hook=fail_after_second_generation,
        )
        failing = PromptArchiveRuntime(
            config,
            PROJECT_ID,
            storage,
            occurrence_prover=prover,
        )
        raw = canonical_json_bytes_v1(
            {"retained_successor": "dual-fork"},
            maximum=64 * 1024,
        )
        occurrence = self.occurrence(51, committed_bytes=raw)
        self.assert_code(
            "PROMPT_ARCHIVE_UNAVAILABLE",
            lambda: failing.publish_shadow(raw, occurrence),
        )

        operation_id = failing._operation_id(occurrence)
        operation_root = (
            root
            / "projects"
            / storage.project_storage_key
            / "operations"
            / "v1"
            / occurrence.response_id
            / operation_id
        )
        plan = storage.read_immutable_json(
            ("operations", "v1", occurrence.response_id, operation_id, "plan.json")
        )
        plan_raw = canonical_json_bytes_v1(plan, maximum=8 * 1024 * 1024)
        plan_ref = storage.describe_blob(
            plan_raw,
            logical_kind="publication_plan",
            media_type=(
                "application/vnd.nginx-qa.prompt-archive-publication-plan-v1+json"
            ),
        )
        current = json.loads(
            (operation_root / "journal.json").read_text(encoding="utf-8")
        )
        current_raw = canonical_json_bytes_v1(current, maximum=1024 * 1024)
        current_ref = storage.describe_blob(
            current_raw,
            logical_kind="journal",
            media_type=(
                "application/vnd.nginx-qa.prompt-archive-journal-v1+json"
            ),
        )
        abort = failing._aborted_journal_document(
            plan,
            plan_ref,
            current,
            current_ref,
            reason_code="source_unavailable",
        )
        abort_raw = canonical_json_bytes_v1(abort, maximum=1024 * 1024)
        storage.put_blob(
            abort_raw,
            logical_kind="journal",
            media_type=(
                "application/vnd.nginx-qa.prompt-archive-journal-v1+json"
            ),
        )
        abort_digest = hashlib.sha256(abort_raw).hexdigest()
        storage.publish_immutable_bytes(
            failing._journal_generation_path(plan, 1, abort_digest),
            abort_raw,
            expected_sha256=abort_digest,
        )
        self.assertEqual(
            len(tuple((operation_root / "journal-generations").glob("1-*.json"))),
            2,
        )

        restarted_registry = PromptArchiveRuntimeRegistry(
            config,
            occurrence_prover=prover,
        )
        self.assert_code(
            "PROMPT_ARCHIVE_GENERATION_CONFLICT",
            lambda: restarted_registry.validate_and_recover((PROJECT_ID,)),
        )
        current_after = json.loads(
            (operation_root / "journal.json").read_text(encoding="utf-8")
        )
        self.assertEqual(current_after["state"], "intent")

    def test_recovery_proves_occurrence_or_aborts_only_before_pointer(self) -> None:
        scenarios = (
            ("matching", "after_pending_registration", 1, "complete", None, None),
            (
                "missing",
                "after_pending_registration",
                1,
                "aborted",
                "source_unavailable",
                "intent",
            ),
            (
                "different",
                "after_pending_registration",
                1,
                "aborted",
                "source_changed",
                "intent",
            ),
            (
                "missing_after_blobs_verified",
                "after_journal_current",
                3,
                "complete",
                None,
                None,
            ),
            (
                "missing_after_pointer",
                "after_pointer_publication",
                1,
                "complete",
                None,
                None,
            ),
        )
        for ordinal, (
            source_case,
            fault_stage,
            fault_occurrence,
            expected_state,
            abort_reason,
            abort_from_state,
        ) in enumerate(scenarios, start=30):
            with self.subTest(source_case=source_case):
                root = self.base / f"proof-{source_case}"
                config = PromptArchiveRuntimeConfig(
                    mode="shadow",
                    root=root,
                    max_hydrated_bytes=8 * 1024 * 1024,
                )
                prover = ClosedOccurrenceProver()
                fault_hits = 0

                def fail_once(stage: str) -> None:
                    nonlocal fault_hits
                    if stage == fault_stage:
                        fault_hits += 1
                    if stage == fault_stage and fault_hits == fault_occurrence:
                        raise PromptArchiveStorageError("PROMPT_ARCHIVE_UNAVAILABLE")

                storage = PromptArchiveStorage(
                    root,
                    PROJECT_ID,
                    protected_roots=(),
                    limits=PromptArchiveLimits(
                        max_hydrated_bytes=config.max_hydrated_bytes
                    ),
                    fault_hook=fail_once,
                )
                failing = PromptArchiveRuntime(
                    config,
                    PROJECT_ID,
                    storage,
                    occurrence_prover=prover,
                )
                raw = canonical_json_bytes_v1(
                    {"source_proof": source_case, "ordinal": ordinal},
                    maximum=64 * 1024,
                )
                occurrence = self.occurrence(ordinal, committed_bytes=raw)
                self.assert_code(
                    "PROMPT_ARCHIVE_UNAVAILABLE",
                    lambda: failing.publish_shadow(raw, occurrence),
                )
                self.assertEqual(fault_hits, fault_occurrence)
                self.assertEqual(len(prover.records), 1)
                registered_proof = next(iter(prover.records.values()))
                self.assertEqual(registered_proof.source.byte_size, len(raw))
                self.assertEqual(
                    registered_proof.source.sha256,
                    hashlib.sha256(raw).hexdigest(),
                )

                proof_key = next(iter(prover.records))
                if source_case in {
                    "missing",
                    "missing_after_blobs_verified",
                    "missing_after_pointer",
                }:
                    proof = prover.records[proof_key]
                    prover.records[proof_key] = PromptArchiveOccurrenceProof(
                        "source_unavailable",
                        proof.occurrence,
                        proof.source,
                    )
                elif source_case == "different":
                    proof = prover.records[proof_key]
                    prover.records[proof_key] = PromptArchiveOccurrenceProof(
                        "source_changed",
                        proof.occurrence,
                        proof.source,
                    )
                prover.load_calls = 0

                restarted_registry = PromptArchiveRuntimeRegistry(
                    config,
                    occurrence_prover=prover,
                )
                restarted = restarted_registry.for_project(PROJECT_ID)
                self.assertEqual(
                    restarted_registry.validate_and_recover((PROJECT_ID,)),
                    1,
                )

                operation_id = restarted._operation_id(occurrence)
                operation_root = (
                    root
                    / "projects"
                    / restarted.storage.project_storage_key
                    / "operations"
                    / "v1"
                    / occurrence.response_id
                    / operation_id
                )
                journal = json.loads(
                    (operation_root / "journal.json").read_text(encoding="utf-8")
                )
                self.assertEqual(journal["state"], expected_state)
                pointer_path = (
                    root
                    / "projects"
                    / restarted.storage.project_storage_key
                    / "sprints"
                    / sprint_storage_key_v1(PROJECT_ID, SPRINT_ID)
                    / "pointers"
                    / "v1"
                    / f"{occurrence.response_id}.json"
                )
                self.assertEqual(pointer_path.is_file(), expected_state == "complete")
                if expected_state == "aborted":
                    self.assertEqual(journal["abort_reason_code"], abort_reason)
                    self.assertEqual(
                        journal["abort_from_state"],
                        abort_from_state,
                    )
                    self.assertGreaterEqual(prover.load_calls, 1)
                    self.assertEqual(restarted.recover_pending(), 0)
                else:
                    selected = restarted.lookup_archive(
                        sprint_id=SPRINT_ID,
                        archive_id=journal["archive_id"],
                    )
                    self.assertEqual(restarted.hydrate_content(selected), raw)
                    if source_case in {
                        "missing_after_blobs_verified",
                        "missing_after_pointer",
                    }:
                        # Once the immutable graph is verified, source loss may
                        # never relabel that publication as an abort.
                        self.assertEqual(prover.load_calls, 0)

    def test_consecutive_occurrences_use_verified_bounded_predecessor(self) -> None:
        runtime = self.registry().for_project(PROJECT_ID)
        first_raw = canonical_json_bytes_v1(
            {
                "active_task": {"message": "first"},
                "project_state": {
                    "recent_activity": [{"ordinal": value} for value in range(8)],
                    "code_patches": [{"sha": "1" * 40}],
                },
            },
            maximum=64 * 1024,
        )
        second_raw = canonical_json_bytes_v1(
            {
                "active_task": {"message": "second"},
                "project_state": {
                    "recent_activity": [{"ordinal": value} for value in range(9)],
                    "code_patches": [{"sha": "1" * 40}],
                },
            },
            maximum=64 * 1024,
        )
        first = runtime.publish_shadow(
            first_raw,
            self.occurrence(1, committed_bytes=first_raw),
        )
        second = runtime.publish_shadow(
            second_raw,
            self.occurrence(2, committed_bytes=second_raw),
        )
        first_read = runtime.lookup_archive(
            sprint_id=SPRINT_ID, archive_id=first.archive_id
        )
        second_read = runtime.lookup_archive(
            sprint_id=SPRINT_ID, archive_id=second.archive_id
        )
        self.assertEqual(
            second_read.manifest["source"]["predecessor_archive_id"],
            first.archive_id,
        )
        self.assertEqual(
            first_read.manifest["roots"]["code_patches"],
            second_read.manifest["roots"]["code_patches"],
        )
        archive = PromptArchiveIncremental(
            runtime.storage, PROJECT_ID, SPRINT_ID
        )
        first_events = archive._receipt_from_root(
            first_read.manifest["roots"]["recent_activity"], "events"
        )
        second_events = archive._receipt_from_root(
            second_read.manifest["roots"]["recent_activity"], "events"
        )
        first_leaves = archive.sequence_leaves(first_events)
        second_leaves = archive.sequence_leaves(second_events)
        self.assertEqual(
            [leaf.ref for leaf in first_leaves],
            [leaf.ref for leaf in second_leaves[: len(first_leaves)]],
        )
        self.assertEqual(runtime.hydrate_content(second_read), second_raw)
        replayed_first = runtime.publish_shadow(
            first_raw,
            self.occurrence(1, committed_bytes=first_raw),
        )
        self.assertEqual(replayed_first.archive_id, first.archive_id)
        self.assertEqual(
            runtime._predecessor_archives[SPRINT_ID], second.archive_id
        )

        # The second plan durably contains the predecessor that the runtime
        # selected from its stream head.  A clean process restart has no such
        # cache, so replay must accept the caller's original source without the
        # internally-added field, while rejecting every other immutable input
        # change for the same occurrence.
        restarted = self.registry().for_project(PROJECT_ID)
        replayed_second = restarted.publish_shadow(
            second_raw,
            self.occurrence(2, committed_bytes=second_raw),
        )
        self.assertEqual(replayed_second.archive_id, second.archive_id)

        original = self.occurrence(2, committed_bytes=second_raw)
        changed_source = dict(original.source)
        changed_source["assignment_id"] = "assignment-runtime-changed"
        changed_authorization = dict(original.authorization)
        changed_authorization["decision_occurrence_id"] = "transition-runtime-changed"
        mutations = (
            dataclasses.replace(original, source=changed_source),
            dataclasses.replace(original, authorization=changed_authorization),
            dataclasses.replace(original, created_at="2026-10-11T13:02:00Z"),
            dataclasses.replace(original, verified_at="2026-10-11T13:02:01Z"),
            dataclasses.replace(original, state_revision=1),
        )
        for changed in mutations:
            with self.subTest(changed=changed):
                self.assert_code(
                    "PROMPT_ARCHIVE_GENERATION_CONFLICT",
                    lambda changed=changed: restarted.publish_shadow(
                        second_raw,
                        changed,
                    ),
                )

    def test_lookup_rejects_cross_sprint_and_stable_pointer_tampering(self) -> None:
        runtime = self.registry().for_project(PROJECT_ID)
        raw = b'{"value":1}'
        publication = runtime.publish_shadow(
            raw,
            self.occurrence(committed_bytes=raw),
        )
        self.assert_code(
            "PROMPT_ARCHIVE_NOT_FOUND",
            lambda: runtime.lookup_archive(
                sprint_id="other-sprint",
                archive_id=publication.archive_id,
            ),
        )

        sprint_key = sprint_storage_key_v1(PROJECT_ID, SPRINT_ID)
        pointer_path = (
            self.root
            / "projects"
            / runtime.storage.project_storage_key
            / "sprints"
            / sprint_key
            / "pointers"
            / "v1"
            / f"{self.occurrence().response_id}.json"
        )
        pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
        pointer["archive_id"] = "par1-" + "0" * 64
        pointer_path.write_bytes(canonical_json_bytes_v1(pointer, maximum=64 * 1024))
        self.assert_code(
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
            lambda: runtime.lookup_archive(
                sprint_id=SPRINT_ID,
                archive_id=publication.archive_id,
            ),
        )

    def test_retry_completes_partial_pointer_without_mutating_it(self) -> None:
        runtime = self.registry().for_project(PROJECT_ID)
        raw = b'{"retry":"same durable occurrence"}'
        original_publish = runtime.storage.publish_immutable_bytes
        saw_pointer = False

        def fail_before_locator(components, data, **kwargs):
            nonlocal saw_pointer
            if tuple(components)[2:4] == ("pointers", "v1"):
                saw_pointer = True
                return original_publish(components, data, **kwargs)
            if tuple(components)[2:4] == ("archives", "v1"):
                raise PromptArchiveStorageError("PROMPT_ARCHIVE_UNAVAILABLE")
            return original_publish(components, data, **kwargs)

        with mock.patch.object(
            runtime.storage,
            "publish_immutable_bytes",
            side_effect=fail_before_locator,
        ):
            self.assert_code(
                "PROMPT_ARCHIVE_UNAVAILABLE",
                lambda: runtime.publish_shadow(
                    raw,
                    self.occurrence(committed_bytes=raw),
                ),
            )
        self.assertTrue(saw_pointer)
        recovered = runtime.publish_shadow(
            raw,
            self.occurrence(committed_bytes=raw),
        )
        selected = runtime.lookup_archive(
            sprint_id=SPRINT_ID,
            archive_id=recovered.archive_id,
        )
        self.assertEqual(runtime.hydrate_content(selected), raw)

    def test_legacy_mode_with_configured_root_reads_existing_archive(self) -> None:
        shadow = self.registry().for_project(PROJECT_ID)
        raw = b'{"legacy":true}'
        publication = shadow.publish_shadow(
            raw,
            self.occurrence(committed_bytes=raw),
        )
        legacy = self.registry(mode="legacy").for_project(PROJECT_ID)
        selected = legacy.lookup_archive(
            sprint_id=SPRINT_ID,
            archive_id=publication.archive_id,
        )
        self.assertEqual(legacy.hydrate_content(selected), raw)
        self.assertIsNone(legacy.publish_shadow(raw, self.occurrence()))

    def test_inline_reader_preserves_legacy_and_authorizes_stub_before_lookup(self) -> None:
        runtime = self.registry().for_project(PROJECT_ID)
        legacy = b' {"active_task":{"message":"unchanged"}}\r\n'
        authorization = mock.Mock(return_value=True)
        self.assertEqual(
            runtime.read_inline_or_archive(
                legacy,
                authorize_archive=authorization,
            ),
            legacy,
        )
        authorization.assert_not_called()
        duplicate_legacy = b'{"value":1,"value":2}'
        self.assertEqual(
            runtime.read_inline_or_archive(
                duplicate_legacy,
                authorize_archive=authorization,
            ),
            duplicate_legacy,
        )
        self.assert_code(
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
            lambda: runtime.read_inline_or_archive(
                b'{"kind":"legacy","kind":"prompt_archive_descriptor_stub"}',
                authorize_archive=authorization,
            ),
        )

        raw = b'{"archived":true}'
        occurrence = self.occurrence(committed_bytes=raw)
        publication = runtime.publish_shadow(raw, occurrence)
        stub = self.descriptor_stub(publication)
        stub_raw = canonical_json_bytes_v1(stub, maximum=64 * 1024)
        calls: list[tuple[str, str, str]] = []

        def authorize(project_id: str, sprint_id: str, archive_id: str) -> bool:
            calls.append((project_id, sprint_id, archive_id))
            return True

        self.assertEqual(
            runtime.read_inline_or_archive(stub_raw, authorize_archive=authorize),
            b'{"archived":true}',
        )
        self.assertEqual(calls, [(PROJECT_ID, SPRINT_ID, publication.archive_id)])

        denied = mock.Mock(return_value=False)
        with mock.patch.object(
            runtime,
            "lookup_archive",
            side_effect=AssertionError("lookup happened before authorization"),
        ):
            self.assert_code(
                "PROMPT_ARCHIVE_NOT_FOUND",
                lambda: runtime.read_inline_or_archive(
                    stub_raw,
                    authorize_archive=denied,
                ),
            )
        denied.assert_called_once()
        self.assert_code(
            "PROMPT_ARCHIVE_VERSION_UNSUPPORTED",
            lambda: runtime.read_inline_or_archive(
                b'{"kind":"future_descriptor"}',
                authorize_archive=lambda *_args: True,
            ),
        )

    @staticmethod
    def descriptor_stub(publication) -> dict:
        scope = copy.deepcopy(dict(publication.scope))
        manifest_ref = copy.deepcopy(dict(publication.manifest_ref))
        published_at = publication.locator["published_at"]
        stub_id = "parstub1-" + pas1_id_v1(
            "descriptor-stub-id",
            (
                ("project_id", PROJECT_ID),
                ("sprint_id", SPRINT_ID),
                ("response_id", scope["response_id"]),
                ("archive_id", publication.archive_id),
                ("pointer_id", publication.pointer_id),
                ("manifest_sha256", manifest_ref["digest"]),
                ("created_at", publication.created_at),
                ("published_at", published_at),
            ),
        )
        return {
            "schema_version": 1,
            "kind": "prompt_archive_descriptor_stub",
            "media_type": (
                "application/vnd.nginx-qa.prompt-archive-descriptor-stub-v1+json"
            ),
            "id_algorithm": "PAS1-ID-v1",
            "stub_id": stub_id,
            "archive_id": publication.archive_id,
            "archive_format": publication.archive_format,
            "scope": scope,
            "integrity": copy.deepcopy(dict(publication.integrity)),
            "pointer_id": publication.pointer_id,
            "manifest_ref": manifest_ref,
            "created_at": publication.created_at,
            "published_at": published_at,
        }

    def test_environment_validation_and_registry_are_project_scoped(self) -> None:
        config = PromptArchiveRuntimeConfig.from_environment(
            {
                "NGINX_QA_PROMPT_ARCHIVE_MODE": "shadow",
                "NGINX_QA_PROMPT_ARCHIVE_ROOT": str(self.root),
                "NGINX_QA_PROMPT_ARCHIVE_MAX_HYDRATED_BYTES": "1048576",
            }
        )
        registry = PromptArchiveRuntimeRegistry(config)
        self.assertEqual(registry.validate_and_recover(()), 0)
        self.assertTrue(self.root.is_dir())
        first = registry.for_project(PROJECT_ID)
        self.assertIs(first, registry.for_project(PROJECT_ID))
        second = registry.for_project("9001")
        self.assertNotEqual(first.project_id, second.project_id)
        self.assertNotEqual(
            first.storage.project_storage_key,
            second.storage.project_storage_key,
        )
        self.assert_code(
            "PROMPT_ARCHIVE_REQUEST_INVALID",
            lambda: registry.validate_and_recover((PROJECT_ID, PROJECT_ID)),
        )
        self.assert_code(
            "PROMPT_ARCHIVE_REQUEST_INVALID",
            lambda: PromptArchiveRuntimeConfig.from_environment(
                {
                    "NGINX_QA_PROMPT_ARCHIVE_MODE": "shadow",
                    "NGINX_QA_PROMPT_ARCHIVE_ROOT": "relative-root",
                }
            ),
        )
        self.assert_code(
            "PROMPT_ARCHIVE_REQUEST_INVALID",
            lambda: PromptArchiveRuntimeConfig.from_environment(
                {
                    "NGINX_QA_PROMPT_ARCHIVE_MODE": "shadow",
                    "NGINX_QA_PROMPT_ARCHIVE_ROOT": str(self.root),
                    "NGINX_QA_PROMPT_ARCHIVE_MAX_HYDRATED_BYTES": "0",
                }
            ),
        )

    def test_public_sprint_key_matches_frozen_golden_vector(self) -> None:
        self.assertEqual(
            sprint_storage_key_v1("9000", "sprint-example-1"),
            "52c984a54d25d125de23576c14d0d99f038af5c35cba1b3f2d835f341b3f2dff",
        )


if __name__ == "__main__":
    unittest.main()
