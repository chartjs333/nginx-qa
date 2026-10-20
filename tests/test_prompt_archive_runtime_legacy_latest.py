import copy
import dataclasses
import hashlib
import json
import tempfile
import threading
import traceback
import unittest
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource

from nginx_qa.prompt_archive_incremental import (
    ArchiveOccurrence,
    pas1_id_v1,
    response_id_v1,
)
from nginx_qa.prompt_archive_latest import ExternalLatestCoordinator
from nginx_qa.prompt_archive_occurrence_store import (
    PromptArchiveNativeOccurrenceProof,
    PromptArchiveOccurrenceProof,
)
from nginx_qa.prompt_archive_runtime import PromptArchiveRuntime, PromptArchiveRuntimeConfig
from nginx_qa.prompt_archive_storage import (
    PromptArchiveLimits,
    PromptArchiveStorage,
    PromptArchiveStorageError,
    canonical_json_bytes_v1,
)


PROJECT_ID = "9000"
SPRINT_ID = "sprint-legacy-latest-runtime"
LATEST_SLOT_MEDIA = "application/vnd.nginx-qa.prompt-archive-latest-slot-v1+json"


class InjectedRuntimeFault(RuntimeError):
    pass


class ClosedLegacyLatestPublisher:
    """Small ordered-protocol test double; production E2E uses the real one."""

    def __init__(
        self,
        storage: PromptArchiveStorage,
        *,
        target_label: str = "closed-legacy-latest",
    ) -> None:
        self.storage = storage
        self.publications: dict[str, SimpleNamespace] = {}
        self.latest_bytes: bytes | None = None
        self._target_authority_id = "parlatestauthority1-" + hashlib.sha256(
            target_label.encode("utf-8")
        ).hexdigest()

    @property
    def target_authority_id(self) -> str:
        return self._target_authority_id

    @staticmethod
    def _bootstrap() -> dict:
        return {
            "protocol": "prompt-archive-project-global-bootstrap-enrollment-v1",
            "kind": "enrolled",
            "lock_scope": "project",
            "slot_id": "latest-response",
            "registry_shard_count": 256,
            "writer_lock_policy": "all-latest-writers-pre-and-post-enrollment-v1",
            "lock_location_policy": "external-latest-colocated-stable-lock-v1",
            "registration_order": "observe-mark-unlock-register-relock-reconcile-v1",
            "bootstrap_completion_barrier": (
                "durable-marker-before-unlock-content-slot-before-final-unlock-v1"
            ),
        }

    def prepare_legacy_intent(
        self,
        content,
        *,
        write_kind,
        occurrence_id,
        operation_id,
        published_at,
        source_response_id,
        source_sprint_id,
        source_revision,
        authoritative_drain_verifier,
    ):
        del write_kind
        payload = bytes(content)
        intent = {
            "identity_version": "prompt-archive-latest-content-intent-v1",
            "intent_id": "",
            "project_id": PROJECT_ID,
            "slot_scope": "project",
            "slot_id": "latest-response",
            "publication_protocol": (
                "prompt-archive-project-global-ordered-latest-intent-v1"
            ),
            "ordering": {
                "revision_authority": {
                    "protocol": "prompt-archive-durable-project-latest-revision-v1",
                    "epoch": 1,
                    "reset_policy": "restore_high_water_mark_or_fail_closed",
                },
                "source_revision": source_revision,
                "occurrence_id": occurrence_id,
                "operation_id": operation_id,
            },
            "bootstrap_precondition": self._bootstrap(),
            "content_kind": "legacy_inline",
            "content_sha256": hashlib.sha256(payload).hexdigest(),
            "content_size": len(payload),
            "descriptor_ref": None,
            "source_sprint_id": source_sprint_id,
            "source_response_id": source_response_id,
            "published_at": published_at,
        }
        intent["intent_id"] = PromptArchiveRuntime._latest_intent_id(intent)
        barrier = {
            "protocol": "reconcile-authoritative-before-legacy-latest-write-v1",
            "lock_scope": "project",
            "slot_id": "latest-response",
            "registry_shard_count": 256,
            "observed_incomplete_operation_ids": [],
            "observed_at": published_at,
        }
        if not authoritative_drain_verifier(barrier):
            raise PromptArchiveStorageError("PROMPT_ARCHIVE_UNAVAILABLE")
        return {
            "latest_intent": intent,
            "authoritative_drain_barrier": barrier,
        }

    def publish_prepared_legacy(
        self,
        content,
        *,
        prepared,
        write_kind,
        authoritative_drain_verifier,
    ):
        self.assert_write_kind(write_kind)
        barrier = prepared["authoritative_drain_barrier"]
        if not authoritative_drain_verifier(barrier):
            raise PromptArchiveStorageError("PROMPT_ARCHIVE_UNAVAILABLE")
        intent = prepared["latest_intent"]
        payload = bytes(content)
        if (
            hashlib.sha256(payload).hexdigest() != intent["content_sha256"]
            or len(payload) != intent["content_size"]
        ):
            raise PromptArchiveStorageError("PROMPT_ARCHIVE_GENERATION_CONFLICT")
        previous = self.publications.get(intent["intent_id"])
        if previous is not None:
            return SimpleNamespace(**{**vars(previous), "outcome": "idempotent", "recovered": True})
        slot_id = "parlatest1-" + hashlib.sha256(
            intent["intent_id"].encode("ascii")
        ).hexdigest()
        slot_ref = self.storage.put_blob(
            canonical_json_bytes_v1(
                {"intent_id": intent["intent_id"]},
                maximum=64 * 1024,
            ),
            logical_kind="latest_slot",
            media_type=LATEST_SLOT_MEDIA,
        )
        result = SimpleNamespace(
            outcome="published",
            intent_id=intent["intent_id"],
            latest_slot_id=slot_id,
            latest_slot_ref=slot_ref,
            source_revision=intent["ordering"]["source_revision"],
            generation=len(self.publications) + 1,
            content_sha256=intent["content_sha256"],
            content_size=intent["content_size"],
            recovered=False,
        )
        self.publications[intent["intent_id"]] = result
        self.latest_bytes = payload
        return result

    @staticmethod
    def assert_write_kind(write_kind: str) -> None:
        if write_kind not in {"transition", "identity", "state", "rollback"}:
            raise PromptArchiveStorageError("PROMPT_ARCHIVE_REQUEST_INVALID")


class ClosedNativeOccurrenceProver:
    """Process-independent in-memory proof authority for restart tests."""

    def __init__(self) -> None:
        self.records: dict[tuple[str, bytes], object] = {}

    @staticmethod
    def _key(project_id, locator) -> tuple[str, bytes]:
        return (
            project_id,
            canonical_json_bytes_v1(dict(locator), maximum=64 * 1024),
        )

    def load_occurrence(self, *, project_id, locator):
        return self.records.get(self._key(project_id, locator))

    def register_native_occurrence(
        self,
        *,
        project_id,
        locator,
        occurrence,
        source,
    ) -> None:
        key = self._key(project_id, locator)
        proof = PromptArchiveNativeOccurrenceProof(
            "verified",
            dataclasses.replace(
                occurrence,
                source=copy.deepcopy(dict(occurrence.source)),
                authorization=copy.deepcopy(dict(occurrence.authorization)),
            ),
            source,
        )
        existing = self.records.get(key)
        if existing is not None and existing != proof:
            raise PromptArchiveStorageError("PROMPT_ARCHIVE_GENERATION_CONFLICT")
        self.records[key] = proof

    def register_occurrence(
        self,
        *,
        project_id,
        locator,
        occurrence,
        source,
    ) -> None:
        key = self._key(project_id, locator)
        proof = PromptArchiveOccurrenceProof(
            "verified",
            dataclasses.replace(
                occurrence,
                source=copy.deepcopy(dict(occurrence.source)),
                authorization=copy.deepcopy(dict(occurrence.authorization)),
            ),
            source,
        )
        existing = self.records.get(key)
        if existing is not None and existing != proof:
            raise PromptArchiveStorageError("PROMPT_ARCHIVE_GENERATION_CONFLICT")
        self.records[key] = proof


class ClosedAuthoritativePublisher:
    """Authoritative descriptor adapter sharing one physical latest target."""

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
    def target_authority_id(self) -> str:
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
        self.transition_path = self.root / (
            "transition-response-"
            f"{compatibility_publication['transition_stub']['response_id']}.json"
        )
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


class PromptArchiveRuntimeLegacyLatestTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        schema_root = Path(__file__).resolve().parents[1] / "schemas"
        cls.schemas = {
            path.name: json.loads(path.read_text(encoding="utf-8"))
            for path in schema_root.glob("*.schema.json")
        }
        registry = Registry()
        for name, schema in cls.schemas.items():
            resource = Resource.from_contents(schema)
            registry = registry.with_resource(schema["$id"], resource)
            registry = registry.with_resource(
                "https://github.com/chartjs333/nginx-qa/schemas/" + name,
                resource,
            )
        cls.schema_registry = registry

    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary_directory.name)
        self.archive_root = self.base / "archive"

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    def validator(self, name: str) -> Draft202012Validator:
        return Draft202012Validator(
            self.schemas[name],
            registry=self.schema_registry,
            format_checker=FormatChecker(),
        )

    def storage(self, *, fault_hook=None) -> PromptArchiveStorage:
        return PromptArchiveStorage(
            self.archive_root,
            PROJECT_ID,
            protected_roots=(),
            limits=PromptArchiveLimits(max_hydrated_bytes=8 * 1024 * 1024),
            fault_hook=fault_hook,
        )

    def runtime(
        self,
        storage,
        *,
        resolver=None,
        mode: str = "legacy",
        occurrence_prover=None,
        operator_gate: str | None = None,
    ) -> PromptArchiveRuntime:
        return PromptArchiveRuntime(
            PromptArchiveRuntimeConfig(
                mode=mode,
                root=self.archive_root,
                max_hydrated_bytes=8 * 1024 * 1024,
                operator_gate=(
                    "prompt-archive-authoritative-v1"
                    if mode == "authoritative"
                    else operator_gate
                ),
            ),
            PROJECT_ID,
            storage,
            occurrence_prover=occurrence_prover,
            compatibility_resolver=resolver,
        )

    def assert_code(self, code: str, operation) -> None:
        with self.assertRaises(PromptArchiveStorageError) as raised:
            operation()
        self.assertEqual(raised.exception.code, code)

    @staticmethod
    def occurrence(write_kind: str, ordinal: int) -> ArchiveOccurrence:
        response_kind, occurrence_kind = {
            "transition": ("transition-response", "transition"),
            "identity": ("identity-response", "identity-state"),
            "state": ("state-response", "identity-state"),
            "rollback": ("state-response", "identity-state"),
        }[write_kind]
        occurrence_id = f"legacy-latest-{write_kind}-{ordinal}"
        return ArchiveOccurrence(
            project_id=PROJECT_ID,
            sprint_id=SPRINT_ID,
            response_id=response_id_v1(
                PROJECT_ID,
                SPRINT_ID,
                response_kind,
                occurrence_kind,
                occurrence_id,
            ),
            response_kind=response_kind,
            occurrence_kind=occurrence_kind,
            occurrence_id=occurrence_id,
            transition_id=(occurrence_id if occurrence_kind == "transition" else None),
            source={},
            authorization={},
            created_at=f"2026-10-20T00:{ordinal:02d}:00Z",
            verified_at=f"2026-10-20T00:{ordinal:02d}:01Z",
            state_revision=ordinal,
        )

    @classmethod
    def native_occurrence(cls, ordinal: int) -> ArchiveOccurrence:
        occurrence = cls.occurrence("transition", ordinal)
        return dataclasses.replace(
            occurrence,
            source={
                "source_kind": "native_runtime",
                "assignment_id": f"assignment-native-{ordinal}",
            },
            authorization={
                "auth_model_version": "prompt-archive-authorization-v1",
                "principal_kind": "legacy_local_principal",
                "principal_id": PROJECT_ID,
                "binding_id": None,
                "scope_source": "legacy_project_context",
                "effective_scope_revision": None,
                "effective_scope_sha256": None,
                "decision_occurrence_id": occurrence.occurrence_id,
            },
        )

    @staticmethod
    def recovery_shard(occurrence: ArchiveOccurrence, operation_id: str) -> str:
        recovery_key = "parloc1-" + pas1_id_v1(
            "publication-locator",
            (
                ("project_id", occurrence.project_id),
                ("sprint_id", occurrence.sprint_id),
                ("response_id", occurrence.response_id),
                ("occurrence_id", occurrence.occurrence_id),
                ("operation_id", operation_id),
            ),
        )
        return hashlib.sha256(recovery_key.encode("utf-8")).hexdigest()[:2]

    @staticmethod
    def operation_root(storage, occurrence, operation_id) -> Path:
        return (
            storage.root
            / "projects"
            / storage.project_storage_key
            / "operations"
            / "v1"
            / occurrence.response_id
            / operation_id
        )

    @staticmethod
    def storage_tree(storage) -> dict[str, bytes | None]:
        return {
            path.relative_to(storage.root).as_posix(): (
                None if path.is_dir() else path.read_bytes()
            )
            for path in storage.root.rglob("*")
        }

    def assert_common_documents_valid(
        self,
        operation_root: Path,
        *,
        write_kind: str,
    ) -> None:
        plan = json.loads((operation_root / "plan.json").read_text(encoding="utf-8"))
        self.validator("prompt-archive-publication-plan-v1.schema.json").validate(plan)
        self.assertEqual(plan["legacy_latest"]["write_kind"], write_kind)
        journals = sorted((operation_root / "journal-generations").glob("*.json"))
        self.assertEqual(len(journals), 4)
        states = []
        for path in journals:
            document = json.loads(path.read_text(encoding="utf-8"))
            self.validator("prompt-archive-journal-v1.schema.json").validate(document)
            self.assertEqual(document["write_kind"], write_kind)
            states.append(document["state"])
        self.assertEqual(
            states,
            [
                "intent",
                "legacy_payload_verified",
                "compatibility_published",
                "complete",
            ],
        )
        terminal = json.loads(
            (operation_root / "journal.json").read_text(encoding="utf-8")
        )
        self.validator("prompt-archive-journal-v1.schema.json").validate(terminal)
        self.assertEqual(terminal["state"], "complete")
        self.assertEqual(terminal["write_kind"], write_kind)

    def test_all_write_kinds_emit_schema_valid_lossless_plan_and_journal(self) -> None:
        storage = self.storage()
        runtime = self.runtime(
            storage,
            operator_gate="prompt-archive-authoritative-v1",
        )
        publisher = ClosedLegacyLatestPublisher(storage)
        selected_occurrence = None
        selected_receipt = None
        selected_payload = None
        with mock.patch.object(runtime, "recover_pending", return_value=0), mock.patch.object(
            runtime,
            "_verify_authoritative_drain_barrier",
            return_value=True,
        ):
            for ordinal, write_kind in enumerate(
                ("transition", "identity", "state", "rollback"),
                start=1,
            ):
                with self.subTest(write_kind=write_kind):
                    occurrence = self.occurrence(write_kind, ordinal)
                    payload = json.dumps(
                        {"write_kind": write_kind, "ordinal": ordinal},
                        separators=(",", ":"),
                    ).encode("utf-8")
                    if write_kind == "rollback":
                        self.assertIsNotNone(selected_occurrence)
                        self.assertIsNotNone(selected_receipt)
                        self.assertIsNotNone(selected_payload)
                        payload = selected_payload
                        rollback_operation_id = runtime._legacy_latest_operation_id(
                            occurrence,
                            "rollback",
                        )
                        runtime.authorize_legacy_latest_rollback_selection(
                            occurrence=occurrence,
                            operation_id=rollback_operation_id,
                            selected_response_id=selected_occurrence.response_id,
                            selected_operation_id=selected_receipt["operation_id"],
                            expected_content_sha256=hashlib.sha256(payload).hexdigest(),
                            expected_content_size=len(payload),
                            expected_target_authority_id=(
                                publisher.target_authority_id
                            ),
                            published_at=occurrence.verified_at,
                        )
                    receipt = runtime.publish_legacy_latest(
                        payload,
                        occurrence,
                        write_kind=write_kind,
                        compatibility_publisher=publisher,
                    )
                    operation_root = self.operation_root(
                        storage,
                        occurrence,
                        receipt["operation_id"],
                    )
                    self.assert_common_documents_valid(
                        operation_root,
                        write_kind=write_kind,
                    )
                    plan = json.loads(
                        (operation_root / "plan.json").read_text(encoding="utf-8")
                    )
                    self.assertEqual(plan["legacy_latest"]["write_kind"], write_kind)
                    self.assertNotIn(payload.decode("utf-8"), json.dumps(plan))
                    self.assertEqual(runtime._legacy_latest_payload_bytes(plan), payload)
                    self.assertEqual(publisher.latest_bytes, payload)
                    if write_kind == "state":
                        selected_occurrence = occurrence
                        selected_receipt = receipt
                        selected_payload = payload

    def test_runtime_fault_boundaries_recover_to_one_exact_result(self) -> None:
        stages = (
            "after_publication_plan",
            "after_pending_registration",
            "after_journal_generation",
            "after_journal_current",
            "after_journal_registry_bind",
            "after_compatibility_publication",
            "after_terminal_registry_removal",
        )
        for ordinal, stage in enumerate(stages, start=10):
            with self.subTest(stage=stage):
                stage_base = self.base / stage
                archive_root = stage_base / "archive"
                fired = False

                def inject(observed: str) -> None:
                    nonlocal fired
                    if observed == stage and not fired:
                        fired = True
                        raise InjectedRuntimeFault(stage)

                storage = PromptArchiveStorage(
                    archive_root,
                    PROJECT_ID,
                    protected_roots=(),
                    limits=PromptArchiveLimits(
                        max_hydrated_bytes=8 * 1024 * 1024
                    ),
                    fault_hook=inject,
                )
                runtime = PromptArchiveRuntime(
                    PromptArchiveRuntimeConfig(mode="legacy", root=archive_root),
                    PROJECT_ID,
                    storage,
                )
                publisher = ClosedLegacyLatestPublisher(storage)
                occurrence = self.occurrence("transition", ordinal)
                payload = f'{{"stage":"{stage}"}}'.encode("utf-8")
                with mock.patch.object(runtime, "recover_pending", return_value=0), mock.patch.object(
                    runtime,
                    "_verify_authoritative_drain_barrier",
                    return_value=True,
                ), self.assertRaises(InjectedRuntimeFault):
                    runtime.publish_legacy_latest(
                        payload,
                        occurrence,
                        write_kind="transition",
                        compatibility_publisher=publisher,
                    )
                self.assertTrue(fired)

                recovered_storage = PromptArchiveStorage(
                    archive_root,
                    PROJECT_ID,
                    protected_roots=(),
                    limits=PromptArchiveLimits(
                        max_hydrated_bytes=8 * 1024 * 1024
                    ),
                )
                recovered_runtime = PromptArchiveRuntime(
                    PromptArchiveRuntimeConfig(mode="legacy", root=archive_root),
                    PROJECT_ID,
                    recovered_storage,
                )
                publisher.storage = recovered_storage
                with mock.patch.object(
                    recovered_runtime,
                    "recover_pending",
                    return_value=0,
                ), mock.patch.object(
                    recovered_runtime,
                    "_verify_authoritative_drain_barrier",
                    return_value=True,
                ):
                    receipt = recovered_runtime.publish_legacy_latest(
                        payload,
                        occurrence,
                        write_kind="transition",
                        compatibility_publisher=publisher,
                    )
                operation_root = self.operation_root(
                    recovered_storage,
                    occurrence,
                    receipt["operation_id"],
                )
                self.assert_common_documents_valid(
                    operation_root,
                    write_kind="transition",
                )
                self.assertEqual(publisher.latest_bytes, payload)

    def test_real_ordered_protocol_supersedes_older_common_plan_without_clobber(self) -> None:
        external = self.base / "external"
        external.mkdir()
        storage = self.storage()
        runtime = self.runtime(storage)
        coordinator = ExternalLatestCoordinator(
            external / "latest-response.json",
            project_id=PROJECT_ID,
        )
        newer = self.occurrence("state", 50)
        older = self.occurrence("state", 40)
        newer_payload = b'{"revision":50}'
        older_payload = b'{"revision":40}'
        with mock.patch.object(runtime, "recover_pending", return_value=0), mock.patch.object(
            runtime,
            "_verify_authoritative_drain_barrier",
            return_value=True,
        ):
            runtime.publish_legacy_latest(
                newer_payload,
                newer,
                write_kind="state",
                source_revision=50,
                compatibility_publisher=coordinator,
            )
            receipt = runtime.publish_legacy_latest(
                older_payload,
                older,
                write_kind="state",
                source_revision=40,
                compatibility_publisher=coordinator,
            )
        self.assertEqual(receipt["latest_slot_outcome"], "superseded")
        self.assertEqual((external / "latest-response.json").read_bytes(), newer_payload)
        operation_root = self.operation_root(
            storage,
            older,
            receipt["operation_id"],
        )
        self.assert_common_documents_valid(
            operation_root,
            write_kind="state",
        )

    def test_fixed_shard_startup_recovery_finishes_registered_legacy_plan(self) -> None:
        fired = False

        def inject(stage: str) -> None:
            nonlocal fired
            if stage == "after_pending_registration" and not fired:
                fired = True
                raise InjectedRuntimeFault(stage)

        storage = self.storage(fault_hook=inject)
        runtime = self.runtime(storage)
        publisher = ClosedLegacyLatestPublisher(storage)
        occurrence = self.occurrence("identity", 30)
        payload = b'{"identity":"recover"}'
        with mock.patch.object(runtime, "recover_pending", return_value=0), mock.patch.object(
            runtime,
            "_verify_authoritative_drain_barrier",
            return_value=True,
        ), self.assertRaises(InjectedRuntimeFault):
            runtime.publish_legacy_latest(
                payload,
                occurrence,
                write_kind="identity",
                compatibility_publisher=publisher,
            )
        self.assertTrue(fired)

        recovered_storage = self.storage()
        publisher.storage = recovered_storage
        recovered_runtime = self.runtime(
            recovered_storage,
            resolver=lambda _project_id, _plan: publisher,
        )
        with mock.patch.object(
            recovered_runtime,
            "_verify_authoritative_drain_barrier",
            return_value=True,
        ):
            self.assertEqual(recovered_runtime.recover_pending(), 1)
        self.assertEqual(publisher.latest_bytes, payload)
        operation_id = recovered_runtime._legacy_latest_operation_id(
            occurrence,
            "identity",
        )
        self.assert_common_documents_valid(
            self.operation_root(recovered_storage, occurrence, operation_id),
            write_kind="identity",
        )

    def test_malformed_legacy_prepared_intent_cannot_bind_or_publish_plan(
        self,
    ) -> None:
        class MalformedLegacyPublisher(ClosedLegacyLatestPublisher):
            def prepare_legacy_intent(self, *args, **kwargs):
                prepared = super().prepare_legacy_intent(*args, **kwargs)
                prepared["latest_intent"]["intent_id"] = (
                    "parlatestintent1-" + "0" * 64
                )
                return prepared

        storage = self.storage()
        runtime = self.runtime(storage)
        publisher = MalformedLegacyPublisher(storage)
        occurrence = self.occurrence("transition", 30)

        with mock.patch.object(
            runtime,
            "recover_pending",
            return_value=0,
        ), mock.patch.object(
            runtime,
            "_verify_authoritative_drain_barrier",
            return_value=True,
        ):
            self.assert_code(
                "PROMPT_ARCHIVE_INTEGRITY_MISMATCH",
                lambda: runtime.publish_legacy_latest(
                    b'{"malformed":"legacy-prepared-intent"}',
                    occurrence,
                    write_kind="transition",
                    compatibility_publisher=publisher,
                ),
            )

        self.assertIsNone(runtime.latest_target_authority_id())
        self.assertEqual(tuple(storage.root.rglob("plan.json")), ())

    def test_malformed_native_prepared_intent_cannot_bind_or_publish_plan(
        self,
    ) -> None:
        class MalformedAuthoritativePublisher(ClosedAuthoritativePublisher):
            def prepare_authoritative(self, **kwargs):
                prepared = super().prepare_authoritative(**kwargs)
                prepared["intent_id"] = "parlatestintent1-" + "0" * 64
                return prepared

        external = self.base / "external-malformed-native"
        storage = self.storage()
        runtime = self.runtime(
            storage,
            mode="authoritative",
            occurrence_prover=ClosedNativeOccurrenceProver(),
        )
        publisher = MalformedAuthoritativePublisher(external, runtime)
        occurrence = self.native_occurrence(30)

        self.assert_code(
            "PROMPT_ARCHIVE_INTEGRITY_MISMATCH",
            lambda: runtime.publish_authoritative(
                b'{"active_task":{"message":"malformed native intent"}}',
                occurrence,
                compatibility_publisher=publisher,
            ),
        )

        self.assertIsNone(runtime.latest_target_authority_id())
        self.assertEqual(tuple(storage.root.rglob("plan.json")), ())

    def test_authoritative_owner_proof_requires_exact_project_target_binding(
        self,
    ) -> None:
        fired = False

        def fail_after_plan(stage: str) -> None:
            nonlocal fired
            if stage == "after_publication_plan" and not fired:
                fired = True
                raise InjectedRuntimeFault(stage)

        external = self.base / "external-owner-binding"
        storage = self.storage(fault_hook=fail_after_plan)
        runtime = self.runtime(
            storage,
            mode="authoritative",
            occurrence_prover=ClosedNativeOccurrenceProver(),
        )
        publisher = ClosedAuthoritativePublisher(external, runtime)
        occurrence = self.native_occurrence(31)
        with self.assertRaises(InjectedRuntimeFault):
            runtime.publish_authoritative(
                b'{"active_task":{"message":"owner binding proof"}}',
                occurrence,
                compatibility_publisher=publisher,
            )
        self.assertTrue(fired)

        operation_id = runtime._operation_id(occurrence)
        plan = json.loads(
            (
                self.operation_root(storage, occurrence, operation_id)
                / "plan.json"
            ).read_text(encoding="utf-8")
        )
        compatibility = plan["compatibility_publication"]
        owner = compatibility["archive_owner"]
        intent = compatibility["latest_intent"]
        binding_path = (
            storage.root
            / "projects"
            / storage.project_storage_key
            / Path(*runtime._legacy_latest_target_binding_path())
        )
        self.assertTrue(binding_path.is_file())
        binding_path.unlink()

        self.assert_code(
            "PROMPT_ARCHIVE_UNAVAILABLE",
            lambda: runtime.verify_authoritative_archive_owner(owner, intent),
        )

        other_target = ClosedLegacyLatestPublisher(
            storage,
            target_label="different-owner-authority",
        ).target_authority_id
        runtime._bind_latest_target_authority_id(other_target, create=True)
        self.assert_code(
            "PROMPT_ARCHIVE_INTEGRITY_MISMATCH",
            lambda: runtime.verify_authoritative_archive_owner(owner, intent),
        )

    def test_terminal_native_recovery_restores_missing_target_before_cleanup(
        self,
    ) -> None:
        journal_current_writes = 0

        def fail_after_terminal_current(stage: str) -> None:
            nonlocal journal_current_writes
            if stage != "after_journal_current":
                return
            journal_current_writes += 1
            if journal_current_writes == 7:
                raise InjectedRuntimeFault(
                    "terminal-current-before-pending-removal"
                )

        external = self.base / "external-terminal-native"
        prover = ClosedNativeOccurrenceProver()
        storage = self.storage(fault_hook=fail_after_terminal_current)
        runtime = self.runtime(
            storage,
            mode="authoritative",
            occurrence_prover=prover,
        )
        publisher = ClosedAuthoritativePublisher(external, runtime)
        occurrence = self.native_occurrence(32)
        with self.assertRaises(InjectedRuntimeFault):
            runtime.publish_authoritative(
                b'{"active_task":{"message":"terminal native recovery"}}',
                occurrence,
                compatibility_publisher=publisher,
            )
        self.assertEqual(journal_current_writes, 7)

        binding_path = (
            storage.root
            / "projects"
            / storage.project_storage_key
            / Path(*runtime._legacy_latest_target_binding_path())
        )
        self.assertTrue(binding_path.is_file())
        binding_path.unlink()
        self.assertIsNone(runtime.latest_target_authority_id())

        recovered_storage = self.storage()
        recovered = self.runtime(
            recovered_storage,
            mode="authoritative",
            occurrence_prover=prover,
        )
        recovered_publisher = ClosedAuthoritativePublisher(external, recovered)
        recovered._compatibility_resolver = (
            lambda _project_id, _plan: recovered_publisher
        )
        self.assertEqual(recovered.recover_pending(), 1)
        self.assertEqual(
            recovered.latest_target_authority_id(),
            recovered_publisher.target_authority_id,
        )
        self.assertEqual(recovered.recover_pending(), 0)

    def test_pending_recovery_rejects_target_drift_and_never_writes_new_target(
        self,
    ) -> None:
        external_a = self.base / "external-a"
        external_b = self.base / "external-b"
        external_a.mkdir()
        external_b.mkdir()
        publisher_a = ExternalLatestCoordinator(
            external_a / "latest-response.json",
            project_id=PROJECT_ID,
        )
        publisher_b = ExternalLatestCoordinator(
            external_b / "latest-response.json",
            project_id=PROJECT_ID,
        )
        fired = False

        def fail_after_registration(stage: str) -> None:
            nonlocal fired
            if stage == "after_pending_registration" and not fired:
                fired = True
                raise InjectedRuntimeFault(stage)

        storage = self.storage(fault_hook=fail_after_registration)
        runtime = self.runtime(storage)
        occurrence = self.occurrence("transition", 31)
        payload = b'{"target":"authority-a"}'
        with self.assertRaises(InjectedRuntimeFault):
            runtime.publish_legacy_latest(
                payload,
                occurrence,
                write_kind="transition",
                compatibility_publisher=publisher_a,
            )
        self.assertTrue(fired)

        # Model state admitted by the pre-binding release: the immutable plan
        # and pending entry identify authority A, while the newer binding
        # artifact is absent.  A retry that supplies B must not poison that
        # missing binding before it reads the admitted plan.
        binding_path = (
            storage.root
            / "projects"
            / storage.project_storage_key
            / Path(*runtime._legacy_latest_target_binding_path())
        )
        self.assertTrue(binding_path.is_file())
        binding_path.unlink()
        self.assertFalse(binding_path.exists())

        recovered_storage = self.storage()
        drifted = self.runtime(
            recovered_storage,
            resolver=lambda _project_id, _plan: publisher_b,
        )
        self.assert_code("PROMPT_ARCHIVE_UNAVAILABLE", drifted.recover_pending)
        self.assertFalse((external_b / "latest-response.json").exists())
        self.assertFalse(binding_path.exists())

        correct = self.runtime(
            recovered_storage,
            resolver=lambda _project_id, _plan: publisher_a,
        )
        self.assertEqual(correct.recover_pending(), 1)
        self.assertEqual(
            (external_a / "latest-response.json").read_bytes(),
            payload,
        )
        self.assertEqual(
            correct.latest_target_authority_id(),
            publisher_a.target_authority_id,
        )

    def test_corrupt_recovery_lower_bound_cannot_restore_missing_target_binding(
        self,
    ) -> None:
        fired = False

        def fail_after_registration(stage: str) -> None:
            nonlocal fired
            if stage == "after_pending_registration" and not fired:
                fired = True
                raise InjectedRuntimeFault(stage)

        storage = self.storage(fault_hook=fail_after_registration)
        runtime = self.runtime(storage)
        publisher = ClosedLegacyLatestPublisher(storage)
        occurrence = self.occurrence("identity", 31)
        payload = b'{"identity":"corrupt-lower-bound"}'
        with self.assertRaises(InjectedRuntimeFault):
            runtime.publish_legacy_latest(
                payload,
                occurrence,
                write_kind="identity",
                compatibility_publisher=publisher,
            )
        self.assertTrue(fired)

        operation_id = runtime._legacy_latest_operation_id(
            occurrence,
            "identity",
        )
        shard_id = self.recovery_shard(occurrence, operation_id)
        binding_path = (
            storage.root
            / "projects"
            / storage.project_storage_key
            / Path(*runtime._legacy_latest_target_binding_path())
        )
        self.assertTrue(binding_path.is_file())
        binding_path.unlink()

        fake_journal_ref = storage.put_blob(
            b"{}",
            logical_kind="journal",
            media_type=(
                "application/vnd.nginx-qa.prompt-archive-journal-v1+json"
            ),
        )
        with storage.control_lock(runtime._registry_lock_path(shard_id)):
            registry = runtime._read_registry_locked(shard_id)
            self.assertIsNotNone(registry)
            entries = copy.deepcopy(registry["entries"])
            self.assertEqual(len(entries), 1)
            entries[0]["state"] = "journaled"
            entries[0]["journal_ref"] = fake_journal_ref
            runtime._write_registry_locked(
                shard_id=shard_id,
                previous=registry,
                entries=entries,
                published_at="2026-10-20T23:59:59Z",
            )

        def durable_tree() -> dict[str, bytes]:
            return {
                path.relative_to(storage.root).as_posix(): path.read_bytes()
                for path in storage.root.rglob("*")
                if path.is_file() and path.suffix != ".lock"
            }

        before = durable_tree()
        resolver_calls: list[dict] = []

        def resolver(_project_id, plan):
            resolver_calls.append(copy.deepcopy(dict(plan)))
            return publisher

        recovered = self.runtime(self.storage(), resolver=resolver)
        self.assert_code(
            "PROMPT_ARCHIVE_INTEGRITY_MISMATCH",
            recovered.recover_pending,
        )
        self.assertEqual(resolver_calls, [])
        self.assertIsNone(recovered.latest_target_authority_id())
        self.assertEqual(durable_tree(), before)

    def test_plan_publication_fault_binds_target_before_another_operation_can_redirect(
        self,
    ) -> None:
        fired = False

        def fail_after_plan(stage: str) -> None:
            nonlocal fired
            if stage == "after_publication_plan" and not fired:
                fired = True
                raise InjectedRuntimeFault(stage)

        storage = self.storage(fault_hook=fail_after_plan)
        runtime = self.runtime(storage)
        publisher_a = ClosedLegacyLatestPublisher(storage, target_label="authority-a")
        occurrence_a = self.occurrence("transition", 42)
        payload_a = b'{"target":"authority-a"}'
        with self.assertRaises(InjectedRuntimeFault):
            runtime.publish_legacy_latest(
                payload_a,
                occurrence_a,
                write_kind="transition",
                compatibility_publisher=publisher_a,
            )
        self.assertTrue(fired)
        self.assertEqual(
            runtime.latest_target_authority_id(),
            publisher_a.target_authority_id,
        )
        self.assertEqual(len(tuple(storage.root.rglob("plan.json"))), 1)

        restarted_storage = self.storage()
        restarted = self.runtime(restarted_storage)
        publisher_b = ClosedLegacyLatestPublisher(
            restarted_storage,
            target_label="authority-b",
        )
        occurrence_b = self.occurrence("identity", 43)
        self.assert_code(
            "PROMPT_ARCHIVE_UNAVAILABLE",
            lambda: restarted.publish_legacy_latest(
                b'{"target":"authority-b"}',
                occurrence_b,
                write_kind="identity",
                compatibility_publisher=publisher_b,
            ),
        )
        self.assertIsNone(publisher_b.latest_bytes)
        self.assertEqual(
            restarted.latest_target_authority_id(),
            publisher_a.target_authority_id,
        )
        self.assertEqual(len(tuple(restarted_storage.root.rglob("plan.json"))), 1)

        publisher_a.storage = restarted_storage
        receipt = restarted.publish_legacy_latest(
            payload_a,
            occurrence_a,
            write_kind="transition",
            compatibility_publisher=publisher_a,
        )
        self.assertEqual(receipt["latest_slot_outcome"], "published")
        self.assertEqual(publisher_a.latest_bytes, payload_a)
        self.assertEqual(
            restarted.latest_target_authority_id(),
            publisher_a.target_authority_id,
        )

    def test_public_retry_supplies_publisher_to_pending_recovery_without_resolver(
        self,
    ) -> None:
        external = self.base / "external-retry"
        external.mkdir()
        publisher = ExternalLatestCoordinator(
            external / "latest-response.json",
            project_id=PROJECT_ID,
        )
        fired = False

        def fail_after_registration(stage: str) -> None:
            nonlocal fired
            if stage == "after_pending_registration" and not fired:
                fired = True
                raise InjectedRuntimeFault(stage)

        storage = self.storage(fault_hook=fail_after_registration)
        runtime = self.runtime(storage)
        occurrence = self.occurrence("identity", 32)
        payload = b'{"identity":"retry-with-explicit-publisher"}'
        with self.assertRaises(InjectedRuntimeFault):
            runtime.publish_legacy_latest(
                payload,
                occurrence,
                write_kind="identity",
                compatibility_publisher=publisher,
            )

        restarted = self.runtime(self.storage())
        receipt = restarted.publish_legacy_latest(
            payload,
            occurrence,
            write_kind="identity",
            compatibility_publisher=publisher,
        )
        self.assertEqual(receipt["operation_id"], restarted._legacy_latest_operation_id(
            occurrence,
            "identity",
        ))
        self.assertEqual((external / "latest-response.json").read_bytes(), payload)
        self.assertEqual(restarted.recover_pending(), 0)

    def test_mixed_pending_recovery_completes_native_before_legacy(self) -> None:
        selected: tuple[ArchiveOccurrence, ArchiveOccurrence] | None = None
        for legacy_ordinal in range(40, 59):
            legacy = self.occurrence("state", legacy_ordinal)
            legacy_shard = self.recovery_shard(
                legacy,
                PromptArchiveRuntime._legacy_latest_operation_id(legacy, "state"),
            )
            for native_ordinal in range(10, 40):
                native = self.native_occurrence(native_ordinal)
                native_shard = self.recovery_shard(
                    native,
                    PromptArchiveRuntime._operation_id(native),
                )
                if legacy_shard < native_shard:
                    selected = (legacy, native)
                    break
            if selected is not None:
                break
        self.assertIsNotNone(selected)
        legacy, native = selected
        legacy_shard = self.recovery_shard(
            legacy,
            PromptArchiveRuntime._legacy_latest_operation_id(legacy, "state"),
        )
        native_shard = self.recovery_shard(
            native,
            PromptArchiveRuntime._operation_id(native),
        )
        self.assertLess(legacy_shard, native_shard)

        external = self.base / "external-mixed"
        external.mkdir()
        prover = ClosedNativeOccurrenceProver()
        legacy_payload = b'{"state":"legacy-after-native-drain"}'
        native_payload = b'{"active_task":{"message":"native pending"}}'

        def pending_fault(stage: str) -> None:
            if stage == "after_pending_registration":
                raise InjectedRuntimeFault(stage)

        first_storage = self.storage(fault_hook=pending_fault)
        first_runtime = self.runtime(
            first_storage,
            mode="authoritative",
            occurrence_prover=prover,
        )
        legacy_publisher = ExternalLatestCoordinator(
            external / "latest-response.json",
            project_id=PROJECT_ID,
        )
        with self.assertRaises(InjectedRuntimeFault):
            first_runtime.publish_legacy_latest(
                legacy_payload,
                legacy,
                write_kind="state",
                compatibility_publisher=legacy_publisher,
            )

        second_storage = self.storage(fault_hook=pending_fault)
        second_runtime = self.runtime(
            second_storage,
            mode="authoritative",
            occurrence_prover=prover,
        )
        native_publisher = ClosedAuthoritativePublisher(external, second_runtime)
        self.assertEqual(
            native_publisher.target_authority_id,
            legacy_publisher.target_authority_id,
        )
        with self.assertRaises(InjectedRuntimeFault):
            second_runtime.publish_authoritative(
                native_payload,
                native,
                compatibility_publisher=native_publisher,
            )

        restarted_storage = self.storage()
        restarted = self.runtime(
            restarted_storage,
            mode="authoritative",
            occurrence_prover=prover,
        )
        restarted_native = ClosedAuthoritativePublisher(external, restarted)

        def resolver(_project_id, plan):
            if plan.get("operation_kind") == "native_publish":
                return restarted_native
            return restarted_native.coordinator

        restarted._compatibility_resolver = resolver
        self.assertEqual(restarted.recover_pending(), 2)
        self.assertEqual((external / "latest-response.json").read_bytes(), legacy_payload)
        self.assertEqual(restarted.recover_pending(), 0)

    def test_admission_fence_blocks_native_registration_between_drain_and_write(
        self,
    ) -> None:
        external = self.base / "external-admission-fence"
        external.mkdir()
        storage = self.storage()
        legacy_runtime = self.runtime(storage, mode="authoritative")
        native_runtime = self.runtime(
            storage,
            mode="authoritative",
            occurrence_prover=ClosedNativeOccurrenceProver(),
        )

        final_publish_started = threading.Event()
        final_scan_paused = threading.Event()
        release_final_scan = threading.Event()
        native_registration_attempted = threading.Event()
        native_admission_lock_entered = threading.Event()
        native_admission_lock_acquired = threading.Event()
        native_registration_completed = threading.Event()
        external_decision_completed = threading.Event()
        event_order: list[str] = []
        event_order_lock = threading.Lock()

        def record(name: str) -> None:
            with event_order_lock:
                event_order.append(name)

        class ObservedLegacyPublisher(ClosedLegacyLatestPublisher):
            def publish_prepared_legacy(self, *args, **kwargs):
                final_publish_started.set()
                result = super().publish_prepared_legacy(*args, **kwargs)
                record("external_decision_completed")
                external_decision_completed.set()
                return result

        native_publisher = ClosedAuthoritativePublisher(external, native_runtime)
        legacy_publisher = ObservedLegacyPublisher(storage)
        legacy_publisher._target_authority_id = (  # noqa: SLF001 - test double
            native_publisher.target_authority_id
        )
        self.assertEqual(
            legacy_publisher.target_authority_id,
            native_publisher.target_authority_id,
        )

        native_occurrence = next(
            occurrence
            for ordinal in range(1, 59)
            if (
                occurrence := self.native_occurrence(ordinal)
            )
            and self.recovery_shard(
                occurrence,
                PromptArchiveRuntime._operation_id(occurrence),
            )
            != "ff"
        )
        native_shard = self.recovery_shard(
            native_occurrence,
            PromptArchiveRuntime._operation_id(native_occurrence),
        )
        self.assertLess(native_shard, "ff")
        legacy_occurrence = self.occurrence("state", 59)
        legacy_payload = b'{"state":"legacy-linearization-point"}'
        native_payload = b'{"active_task":{"message":"native-after-fence"}}'

        original_read_registry = legacy_runtime._read_registry_locked
        original_verify_barrier = legacy_runtime._verify_authoritative_drain_barrier
        pause_once = False
        verify_calls = 0

        def pause_after_last_shard(shard_id: str):
            nonlocal pause_once
            registry = original_read_registry(shard_id)
            if (
                final_publish_started.is_set()
                and shard_id == "ff"
                and not pause_once
            ):
                pause_once = True
                record("final_scan_paused")
                final_scan_paused.set()
                if not release_final_scan.wait(timeout=30):
                    raise AssertionError("timed out waiting to release drain scan")
            return registry

        def verify_only_final_barrier(barrier) -> bool:
            nonlocal verify_calls
            verify_calls += 1
            if verify_calls == 1:
                # Preparation freezes the same closed barrier.  The race under
                # test is the second proof held across the physical decision.
                return True
            return original_verify_barrier(barrier)

        original_register_planned = native_runtime._register_planned
        original_control_lock = storage.control_lock
        native_thread_name = "prompt-archive-native-admission-test"

        @contextmanager
        def observe_control_lock(components):
            is_native_admission = (
                threading.current_thread().name == native_thread_name
                and tuple(components)
                == native_runtime._registry_admission_lock_path()
            )
            if is_native_admission:
                native_admission_lock_entered.set()
            with original_control_lock(components):
                if is_native_admission:
                    native_admission_lock_acquired.set()
                yield

        def observe_native_registration(plan, plan_ref) -> None:
            self.assertEqual(
                plan["pending_registry"]["shard"]["shard_id"],
                native_shard,
            )
            record("native_registration_attempted")
            native_registration_attempted.set()
            original_register_planned(plan, plan_ref)
            record("native_registration_completed")
            native_registration_completed.set()
            # The regression concerns admission linearization only.  Stop
            # before the native compatibility adapter can contend for the
            # same external coordinator's private recovery journal.
            raise InjectedRuntimeFault("after_fenced_native_registration")

        legacy_results: list[object] = []
        native_stopped = threading.Event()
        thread_errors: list[tuple[BaseException, str]] = []

        def run_legacy() -> None:
            try:
                legacy_results.append(
                    legacy_runtime.publish_legacy_latest(
                        legacy_payload,
                        legacy_occurrence,
                        write_kind="state",
                        compatibility_publisher=legacy_publisher,
                    )
                )
            except BaseException as exc:  # pragma: no cover - asserted below
                thread_errors.append((exc, traceback.format_exc()))

        def run_native() -> None:
            try:
                native_runtime.publish_authoritative(
                    native_payload,
                    native_occurrence,
                    compatibility_publisher=native_publisher,
                )
            except InjectedRuntimeFault:
                native_stopped.set()
            except BaseException as exc:  # pragma: no cover - asserted below
                thread_errors.append((exc, traceback.format_exc()))

        with mock.patch.object(
            legacy_runtime,
            "recover_pending",
            return_value=0,
        ), mock.patch.object(
            legacy_runtime,
            "_verify_authoritative_drain_barrier",
            side_effect=verify_only_final_barrier,
        ), mock.patch.object(
            legacy_runtime,
            "_read_registry_locked",
            side_effect=pause_after_last_shard,
        ), mock.patch.object(
            native_runtime,
            "_register_planned",
            side_effect=observe_native_registration,
        ), mock.patch.object(
            storage,
            "control_lock",
            side_effect=observe_control_lock,
        ):
            legacy_thread = threading.Thread(target=run_legacy, daemon=True)
            native_thread = threading.Thread(
                target=run_native,
                name=native_thread_name,
                daemon=True,
            )
            try:
                legacy_thread.start()
                self.assertTrue(
                    final_scan_paused.wait(timeout=120),
                    "legacy publication did not reach the final drained shard",
                )
                native_thread.start()
                self.assertTrue(
                    native_registration_attempted.wait(timeout=120),
                    "native publication did not attempt pending registration",
                )
                self.assertTrue(
                    native_admission_lock_entered.wait(timeout=120),
                    "native registration did not enter the admission fence",
                )

                # The native shard is already behind the scan cursor.  The
                # common admission fence must nevertheless keep its
                # registration outside the scan-to-external-write interval.
                self.assertFalse(native_admission_lock_acquired.is_set())
                self.assertFalse(native_registration_completed.is_set())
                self.assertFalse(external_decision_completed.is_set())
            finally:
                release_final_scan.set()

            legacy_thread.join(timeout=120)
            native_thread.join(timeout=120)
            self.assertFalse(legacy_thread.is_alive())
            self.assertFalse(native_thread.is_alive())

        if thread_errors:
            self.fail(
                f"event_order={event_order!r}\n\n"
                + "\n\n".join(trace for _error, trace in thread_errors)
            )
        self.assertEqual(len(legacy_results), 1)
        self.assertTrue(native_stopped.is_set())
        self.assertTrue(external_decision_completed.is_set())
        self.assertTrue(native_admission_lock_acquired.is_set())
        self.assertTrue(native_registration_completed.is_set())
        self.assertLess(
            event_order.index("final_scan_paused"),
            event_order.index("native_registration_attempted"),
        )
        self.assertLess(
            event_order.index("native_registration_attempted"),
            event_order.index("external_decision_completed"),
        )
        self.assertLess(
            event_order.index("external_decision_completed"),
            event_order.index("native_registration_completed"),
        )
        self.assertEqual(legacy_publisher.latest_bytes, legacy_payload)

    def test_rollback_requires_immutable_durable_selection_receipt(self) -> None:
        storage = self.storage()
        runtime = self.runtime(
            storage,
            operator_gate="prompt-archive-authoritative-v1",
        )
        publisher = ClosedLegacyLatestPublisher(storage)
        selected_occurrence = self.occurrence("state", 33)
        selected_payload = b'{"state":"known-good-selection"}'
        with mock.patch.object(
            runtime,
            "recover_pending",
            return_value=0,
        ), mock.patch.object(
            runtime,
            "_verify_authoritative_drain_barrier",
            return_value=True,
        ):
            selected_receipt = runtime.publish_legacy_latest(
                selected_payload,
                selected_occurrence,
                write_kind="state",
                compatibility_publisher=publisher,
            )

        missing = self.occurrence("rollback", 34)
        missing_operation_id = runtime._legacy_latest_operation_id(
            missing,
            "rollback",
        )
        unauthorized_payload = b'{"state":"not-selected-for-rollback"}'
        before_missing = self.storage_tree(storage)
        self.assert_code(
            "PROMPT_ARCHIVE_NOT_FOUND",
            lambda: runtime.publish_legacy_latest(
                unauthorized_payload,
                missing,
                write_kind="rollback",
                compatibility_publisher=publisher,
            ),
        )
        after_missing = self.storage_tree(storage)
        self.assertEqual(after_missing, before_missing)
        self.assertFalse(
            self.operation_root(
                storage,
                missing,
                missing_operation_id,
            ).exists()
        )
        self.assertEqual(publisher.latest_bytes, selected_payload)

        tampered = self.occurrence("rollback", 35)
        tampered_operation_id = runtime._legacy_latest_operation_id(
            tampered,
            "rollback",
        )
        runtime.authorize_legacy_latest_rollback_selection(
            occurrence=tampered,
            operation_id=tampered_operation_id,
            selected_response_id=selected_occurrence.response_id,
            selected_operation_id=selected_receipt["operation_id"],
            expected_content_sha256=hashlib.sha256(selected_payload).hexdigest(),
            expected_content_size=len(selected_payload),
            expected_target_authority_id=publisher.target_authority_id,
            published_at=tampered.verified_at,
        )
        selection_path = (
            storage.root
            / "projects"
            / storage.project_storage_key
            / Path(*runtime._rollback_selection_path(tampered_operation_id))
        )
        selection = json.loads(selection_path.read_text(encoding="utf-8"))
        selection["content_size"] += 1
        selection_path.write_bytes(
            canonical_json_bytes_v1(selection, maximum=64 * 1024)
        )
        before_tampered = self.storage_tree(storage)
        self.assert_code(
            "PROMPT_ARCHIVE_INTEGRITY_MISMATCH",
            lambda: runtime.publish_legacy_latest(
                selected_payload,
                tampered,
                write_kind="rollback",
                compatibility_publisher=publisher,
            ),
        )
        after_tampered = self.storage_tree(storage)
        self.assertEqual(after_tampered, before_tampered)
        self.assertFalse(
            self.operation_root(
                storage,
                tampered,
                tampered_operation_id,
            ).exists()
        )
        self.assertEqual(publisher.latest_bytes, selected_payload)

    def test_rollback_selection_restores_source_target_and_rejects_target_drift(
        self,
    ) -> None:
        storage = self.storage()
        runtime = self.runtime(
            storage,
            operator_gate="prompt-archive-authoritative-v1",
        )
        publisher_a = ClosedLegacyLatestPublisher(
            storage,
            target_label="rollback-source-authority-a",
        )
        selected_occurrence = self.occurrence("state", 44)
        selected_payload = b'{"state":"rollback-source-authority-a"}'
        with mock.patch.object(
            runtime,
            "recover_pending",
            return_value=0,
        ), mock.patch.object(
            runtime,
            "_verify_authoritative_drain_barrier",
            return_value=True,
        ):
            selected_receipt = runtime.publish_legacy_latest(
                selected_payload,
                selected_occurrence,
                write_kind="state",
                compatibility_publisher=publisher_a,
            )

        # Model an operation admitted by the release before durable target
        # binding. Rollback selection must recover authority A from that exact
        # completed plan before it creates the immutable selection receipt.
        binding_path = (
            storage.root
            / "projects"
            / storage.project_storage_key
            / Path(*runtime._legacy_latest_target_binding_path())
        )
        binding_path.unlink()
        self.assertIsNone(runtime.latest_target_authority_id())

        publisher_b = ClosedLegacyLatestPublisher(
            storage,
            target_label="rollback-destination-authority-b",
        )
        rollback = self.occurrence("rollback", 45)
        rollback_operation_id = runtime._legacy_latest_operation_id(
            rollback,
            "rollback",
        )
        before_wrong_selection = self.storage_tree(storage)
        self.assert_code(
            "PROMPT_ARCHIVE_INTEGRITY_MISMATCH",
            lambda: runtime.authorize_legacy_latest_rollback_selection(
                occurrence=rollback,
                operation_id=rollback_operation_id,
                selected_response_id=selected_occurrence.response_id,
                selected_operation_id=selected_receipt["operation_id"],
                expected_content_sha256=hashlib.sha256(
                    selected_payload
                ).hexdigest(),
                expected_content_size=len(selected_payload),
                expected_target_authority_id=publisher_b.target_authority_id,
                published_at=rollback.verified_at,
            ),
        )
        self.assertEqual(self.storage_tree(storage), before_wrong_selection)
        self.assertIsNone(runtime.latest_target_authority_id())
        self.assertFalse(
            (
                storage.root
                / "projects"
                / storage.project_storage_key
                / Path(*runtime._rollback_selection_path(rollback_operation_id))
            ).exists()
        )

        before_invalid_timestamp = self.storage_tree(storage)
        self.assert_code(
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
            lambda: runtime.authorize_legacy_latest_rollback_selection(
                occurrence=rollback,
                operation_id=rollback_operation_id,
                selected_response_id=selected_occurrence.response_id,
                selected_operation_id=selected_receipt["operation_id"],
                expected_content_sha256=hashlib.sha256(
                    selected_payload
                ).hexdigest(),
                expected_content_size=len(selected_payload),
                expected_target_authority_id=publisher_a.target_authority_id,
                published_at="not-a-timestamp",
            ),
        )
        self.assertEqual(
            self.storage_tree(storage),
            before_invalid_timestamp,
        )
        self.assertIsNone(runtime.latest_target_authority_id())

        runtime.authorize_legacy_latest_rollback_selection(
            occurrence=rollback,
            operation_id=rollback_operation_id,
            selected_response_id=selected_occurrence.response_id,
            selected_operation_id=selected_receipt["operation_id"],
            expected_content_sha256=hashlib.sha256(
                selected_payload
            ).hexdigest(),
            expected_content_size=len(selected_payload),
            expected_target_authority_id=publisher_a.target_authority_id,
            published_at=rollback.verified_at,
        )
        self.assertEqual(
            runtime.latest_target_authority_id(),
            publisher_a.target_authority_id,
        )

        before_drift = self.storage_tree(storage)
        with mock.patch.object(
            runtime,
            "recover_pending",
            return_value=0,
        ), mock.patch.object(
            runtime,
            "_verify_authoritative_drain_barrier",
            return_value=True,
        ):
            self.assert_code(
                "PROMPT_ARCHIVE_UNAVAILABLE",
                lambda: runtime.publish_legacy_latest(
                    selected_payload,
                    rollback,
                    write_kind="rollback",
                    compatibility_publisher=publisher_b,
                ),
            )
        self.assertEqual(self.storage_tree(storage), before_drift)
        self.assertIsNone(publisher_b.latest_bytes)
        self.assertFalse(
            self.operation_root(
                storage,
                rollback,
                rollback_operation_id,
            ).exists()
        )

        with mock.patch.object(
            runtime,
            "recover_pending",
            return_value=0,
        ), mock.patch.object(
            runtime,
            "_verify_authoritative_drain_barrier",
            return_value=True,
        ):
            receipt = runtime.publish_legacy_latest(
                selected_payload,
                rollback,
                write_kind="rollback",
                compatibility_publisher=publisher_a,
            )
        self.assertEqual(receipt["latest_slot_outcome"], "published")
        self.assertEqual(publisher_a.latest_bytes, selected_payload)
        self.assertEqual(
            runtime.latest_target_authority_id(),
            publisher_a.target_authority_id,
        )

    def test_rollback_selection_and_new_admission_require_runtime_gate(self) -> None:
        storage = self.storage()
        gated = self.runtime(
            storage,
            operator_gate="prompt-archive-authoritative-v1",
        )
        publisher = ClosedLegacyLatestPublisher(storage)
        selected_occurrence = self.occurrence("state", 37)
        selected_payload = b'{"state":"gate-selected"}'
        with mock.patch.object(
            gated,
            "recover_pending",
            return_value=0,
        ), mock.patch.object(
            gated,
            "_verify_authoritative_drain_barrier",
            return_value=True,
        ):
            selected_receipt = gated.publish_legacy_latest(
                selected_payload,
                selected_occurrence,
                write_kind="state",
                compatibility_publisher=publisher,
            )

        authorized = self.occurrence("rollback", 38)
        authorized_operation_id = gated._legacy_latest_operation_id(
            authorized,
            "rollback",
        )
        gated.authorize_legacy_latest_rollback_selection(
            occurrence=authorized,
            operation_id=authorized_operation_id,
            selected_response_id=selected_occurrence.response_id,
            selected_operation_id=selected_receipt["operation_id"],
            expected_content_sha256=hashlib.sha256(selected_payload).hexdigest(),
            expected_content_size=len(selected_payload),
            expected_target_authority_id=publisher.target_authority_id,
            published_at=authorized.verified_at,
        )
        before = self.storage_tree(storage)

        ungated = self.runtime(storage)
        unauthorized_selection = self.occurrence("rollback", 39)
        unauthorized_operation_id = ungated._legacy_latest_operation_id(
            unauthorized_selection,
            "rollback",
        )
        self.assert_code(
            "PROMPT_ARCHIVE_REQUEST_INVALID",
            lambda: ungated.authorize_legacy_latest_rollback_selection(
                occurrence=unauthorized_selection,
                operation_id=unauthorized_operation_id,
                selected_response_id=selected_occurrence.response_id,
                selected_operation_id=selected_receipt["operation_id"],
                expected_content_sha256=hashlib.sha256(
                    selected_payload
                ).hexdigest(),
                expected_content_size=len(selected_payload),
                expected_target_authority_id=publisher.target_authority_id,
                published_at=unauthorized_selection.verified_at,
            ),
        )
        self.assert_code(
            "PROMPT_ARCHIVE_REQUEST_INVALID",
            lambda: ungated.publish_legacy_latest(
                selected_payload,
                authorized,
                write_kind="rollback",
                compatibility_publisher=publisher,
            ),
        )

        after = self.storage_tree(storage)
        self.assertEqual(after, before)
        self.assertFalse(
            self.operation_root(
                storage,
                authorized,
                authorized_operation_id,
            ).exists()
        )
        self.assertFalse(
            (
                storage.root
                / "projects"
                / storage.project_storage_key
                / Path(
                    *ungated._rollback_selection_path(
                        unauthorized_operation_id
                    )
                )
            ).exists()
        )
        self.assertEqual(publisher.latest_bytes, selected_payload)

    def test_admitted_authorized_rollback_recovers_without_live_gate(self) -> None:
        armed = False

        def fail_after_registration(stage: str) -> None:
            if armed and stage == "after_pending_registration":
                raise InjectedRuntimeFault(stage)

        storage = self.storage(fault_hook=fail_after_registration)
        gated = self.runtime(
            storage,
            operator_gate="prompt-archive-authoritative-v1",
        )
        publisher = ClosedLegacyLatestPublisher(storage)
        selected_occurrence = self.occurrence("state", 40)
        selected_payload = b'{"state":"recover-authorized-rollback"}'
        with mock.patch.object(
            gated,
            "recover_pending",
            return_value=0,
        ), mock.patch.object(
            gated,
            "_verify_authoritative_drain_barrier",
            return_value=True,
        ):
            selected_receipt = gated.publish_legacy_latest(
                selected_payload,
                selected_occurrence,
                write_kind="state",
                compatibility_publisher=publisher,
            )

        rollback = self.occurrence("rollback", 41)
        rollback_operation_id = gated._legacy_latest_operation_id(
            rollback,
            "rollback",
        )
        gated.authorize_legacy_latest_rollback_selection(
            occurrence=rollback,
            operation_id=rollback_operation_id,
            selected_response_id=selected_occurrence.response_id,
            selected_operation_id=selected_receipt["operation_id"],
            expected_content_sha256=hashlib.sha256(selected_payload).hexdigest(),
            expected_content_size=len(selected_payload),
            expected_target_authority_id=publisher.target_authority_id,
            published_at=rollback.verified_at,
        )
        armed = True
        with mock.patch.object(
            gated,
            "recover_pending",
            return_value=0,
        ), mock.patch.object(
            gated,
            "_verify_authoritative_drain_barrier",
            return_value=True,
        ), self.assertRaises(InjectedRuntimeFault):
            gated.publish_legacy_latest(
                selected_payload,
                rollback,
                write_kind="rollback",
                compatibility_publisher=publisher,
            )

        recovered_storage = self.storage()
        publisher.storage = recovered_storage
        recovered = self.runtime(
            recovered_storage,
            resolver=lambda _project_id, _plan: publisher,
        )
        self.assertEqual(recovered.recover_pending(), 1)
        self.assertEqual(publisher.latest_bytes, selected_payload)
        self.assertEqual(recovered.recover_pending(), 0)
        self.assert_common_documents_valid(
            self.operation_root(
                recovered_storage,
                rollback,
                rollback_operation_id,
            ),
            write_kind="rollback",
        )

    def test_revision_and_occurrence_provenance_are_closed_before_admission(self) -> None:
        storage = self.storage()
        runtime = self.runtime(storage)
        publisher = ClosedLegacyLatestPublisher(storage)
        occurrence = self.occurrence("transition", 36)
        payload = b'{"transition":"closed-provenance"}'

        self.assert_code(
            "PROMPT_ARCHIVE_REQUEST_INVALID",
            lambda: runtime.publish_legacy_latest(
                payload,
                occurrence,
                write_kind="transition",
                source_revision=occurrence.state_revision + 1,
                compatibility_publisher=publisher,
            ),
        )
        self.assert_code(
            "PROMPT_ARCHIVE_REQUEST_INVALID",
            lambda: runtime.publish_legacy_latest(
                payload,
                dataclasses.replace(
                    occurrence,
                    transition_id="different-transition",
                ),
                write_kind="transition",
                compatibility_publisher=publisher,
            ),
        )
        self.assert_code(
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
            lambda: runtime.publish_legacy_latest(
                payload,
                dataclasses.replace(occurrence, sprint_id="different-sprint"),
                write_kind="transition",
                compatibility_publisher=publisher,
            ),
        )
        self.assertIsNone(publisher.latest_bytes)

    def test_persisted_plan_recomputes_response_id_from_exact_occurrence(self) -> None:
        storage = self.storage()
        runtime = self.runtime(storage)
        publisher = ClosedLegacyLatestPublisher(storage)
        occurrence = self.occurrence("transition", 37)
        payload = b'{"transition":"response-id-binding"}'
        with mock.patch.object(
            runtime,
            "recover_pending",
            return_value=0,
        ), mock.patch.object(
            runtime,
            "_verify_authoritative_drain_barrier",
            return_value=True,
        ):
            receipt = runtime.publish_legacy_latest(
                payload,
                occurrence,
                write_kind="transition",
                compatibility_publisher=publisher,
            )
        operation_root = self.operation_root(
            storage,
            occurrence,
            receipt["operation_id"],
        )
        plan = json.loads((operation_root / "plan.json").read_text(encoding="utf-8"))
        tampered = copy.deepcopy(plan)
        tampered["occurrence"]["response_id"] = "parresp1-" + "f" * 64
        raw = canonical_json_bytes_v1(tampered, maximum=8 * 1024 * 1024)
        plan_ref = storage.put_blob(
            raw,
            logical_kind="publication_plan",
            media_type=(
                "application/vnd.nginx-qa.prompt-archive-publication-plan-v1+json"
            ),
        )
        self.assert_code(
            "PROMPT_ARCHIVE_INTEGRITY_MISMATCH",
            lambda: runtime._validate_plan(tampered, plan_ref),
        )

    def test_replay_rejects_journal_write_kind_not_bound_to_plan(self) -> None:
        storage = self.storage()
        runtime = self.runtime(storage)
        publisher = ClosedLegacyLatestPublisher(storage)
        occurrence = self.occurrence("transition", 38)
        with mock.patch.object(
            runtime,
            "recover_pending",
            return_value=0,
        ), mock.patch.object(
            runtime,
            "_verify_authoritative_drain_barrier",
            return_value=True,
        ):
            receipt = runtime.publish_legacy_latest(
                b'{"transition":"journal-write-kind-binding"}',
                occurrence,
                write_kind="transition",
                compatibility_publisher=publisher,
            )
        operation_root = self.operation_root(
            storage,
            occurrence,
            receipt["operation_id"],
        )
        plan = json.loads((operation_root / "plan.json").read_text(encoding="utf-8"))
        plan_raw = canonical_json_bytes_v1(plan, maximum=8 * 1024 * 1024)
        plan_ref = storage.describe_blob(
            plan_raw,
            logical_kind="publication_plan",
            media_type=(
                "application/vnd.nginx-qa.prompt-archive-publication-plan-v1+json"
            ),
        )
        terminal = json.loads(
            (operation_root / "journal.json").read_text(encoding="utf-8")
        )
        tampered = copy.deepcopy(terminal)
        tampered["write_kind"] = "identity"
        self.validator("prompt-archive-journal-v1.schema.json").validate(tampered)
        tampered_raw = canonical_json_bytes_v1(
            tampered,
            maximum=8 * 1024 * 1024,
        )
        tampered_digest = hashlib.sha256(tampered_raw).hexdigest()
        storage.put_blob(
            tampered_raw,
            logical_kind="journal",
            media_type="application/vnd.nginx-qa.prompt-archive-journal-v1+json",
        )
        storage.publish_immutable_bytes(
            runtime._journal_generation_path(
                plan,
                terminal["journal_generation"],
                tampered_digest,
            ),
            tampered_raw,
            expected_sha256=tampered_digest,
        )
        with storage.control_lock(runtime._operation_lock_path(plan)):
            storage.replace_current_json(runtime._journal_current_path(plan), tampered)
            self.assert_code(
                "PROMPT_ARCHIVE_INTEGRITY_MISMATCH",
                lambda: runtime._load_journal_chain_locked(plan, plan_ref),
            )


if __name__ == "__main__":
    unittest.main()
