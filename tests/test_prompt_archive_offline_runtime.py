from __future__ import annotations

import copy
import dataclasses
import hashlib
import tempfile
import unittest
from pathlib import Path

from nginx_qa.prompt_archive_incremental import (
    ArchiveOccurrence,
    PromptArchiveIncremental,
    pas1_id_v1,
    response_id_v1,
    sprint_storage_key_v1,
)
from nginx_qa.prompt_archive_runtime import (
    PromptArchiveRuntime,
    PromptArchiveRuntimeConfig,
)
from nginx_qa.prompt_archive_storage import (
    PromptArchiveLimits,
    PromptArchiveStorage,
    PromptArchiveStorageError,
    canonical_json_bytes_v1,
)
from nginx_qa.prompt_archive_source_metadata import (
    SourceMetadataScreenSnapshot,
    project_source_metadata,
)


PROJECT_ID = "9000"
INVENTORY_CREATED_AT = "2026-10-11T11:20:00Z"
RAW_LEGACY_RESPONSE = (
    b"\xef\xbb\xbf{\r\n"
    b'  "duplicate": "first",\r\n'
    b'  "duplicate": "second",\r\n'
    b'  "escaped": "\\u0061"\r\n'
    b"}\r\n"
)


class InjectedCrash(RuntimeError):
    pass


class PromptArchiveOfflineRuntimeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary.name)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    @staticmethod
    def inventory_descriptor() -> dict:
        source_identity = "a" * 64
        inventory_sha256 = "b" * 64
        inventory_id = "parinv1-" + pas1_id_v1(
            "source-inventory-id",
            (
                ("project_id", PROJECT_ID),
                ("source_root_handle_id", "isolated-root-1"),
                ("source_root_identity_sha256", source_identity),
                ("inventory_sha256", inventory_sha256),
                ("entry_count", "2"),
                ("created_at", INVENTORY_CREATED_AT),
            ),
        )
        return {
            "schema_version": 1,
            "kind": "prompt_archive_source_inventory",
            "media_type": (
                "application/vnd.nginx-qa.prompt-archive-source-inventory-v1+json"
            ),
            "id_algorithm": "PAS1-ID-v1",
            "inventory_id": inventory_id,
            "inventory_format": "prompt-archive-isolated-inventory-v1",
            "project_id": PROJECT_ID,
            "source_root_handle_id": "isolated-root-1",
            "source_root_identity_sha256": source_identity,
            "inventory_sha256": inventory_sha256,
            "entry_count": 2,
            "created_at": INVENTORY_CREATED_AT,
        }

    @staticmethod
    def batch_id() -> str:
        return "parmb1-" + pas1_id_v1(
            "offline-migration-batch-id",
            (
                ("project_id", PROJECT_ID),
                ("source_root_handle_id", "isolated-root-1"),
            ),
        )

    @classmethod
    def reservation(
        cls,
        ordinal: int = 1,
        raw: bytes = RAW_LEGACY_RESPONSE,
    ) -> dict:
        operation_id = cls.operation_id(ordinal)
        source_ordinal = 0
        path_sha256 = hashlib.sha256(
            f"legacy-response-{ordinal}.json".encode("utf-8")
        ).hexdigest()
        source_sha256 = hashlib.sha256(raw).hexdigest()
        entry_id = "parme1-" + pas1_id_v1(
            "offline-migration-entry-id",
            (
                ("inventory_id", cls.inventory_descriptor()["inventory_id"]),
                ("ordinal", str(source_ordinal)),
                ("path_sha256", path_sha256),
                ("byte_size", str(len(raw))),
                ("sha256", source_sha256),
            ),
        )
        occurrence_id = "migration-" + pas1_id_v1(
            "offline-migration-occurrence",
            (("batch_id", cls.batch_id()), ("entry_id", entry_id)),
        )
        return {
            "ordinal": source_ordinal,
            "entry_id": entry_id,
            "path_sha256": path_sha256,
            "source_byte_size": len(raw),
            "source_sha256": source_sha256,
            "operation_id": operation_id,
            "occurrence_id": occurrence_id,
            "response_id": response_id_v1(
                PROJECT_ID,
                None,
                "migration-snapshot",
                "migration",
                occurrence_id,
            ),
            "decision_occurrence_id": "decision-" + entry_id[-64:],
        }

    @classmethod
    def occurrence(
        cls,
        ordinal: int = 1,
        raw: bytes = RAW_LEGACY_RESPONSE,
    ) -> ArchiveOccurrence:
        reservation = cls.reservation(ordinal, raw)
        return ArchiveOccurrence(
            project_id=PROJECT_ID,
            sprint_id=None,
            response_id=reservation["response_id"],
            response_kind="migration-snapshot",
            occurrence_kind="migration",
            occurrence_id=reservation["occurrence_id"],
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
                "decision_occurrence_id": reservation["decision_occurrence_id"],
            },
            created_at=INVENTORY_CREATED_AT,
            verified_at=INVENTORY_CREATED_AT,
        )

    @staticmethod
    def source_metadata_projection():
        return project_source_metadata(
            {
                "source_type": "manual",
                "source_id": "offline-source-must-not-be-persisted",
                "observed_at": INVENTORY_CREATED_AT,
            },
            screen=SourceMetadataScreenSnapshot((), (), ()),
        )

    @staticmethod
    def operation_id(ordinal: int = 1) -> str:
        return f"parop1-{ordinal:032x}"

    def runtime(
        self,
        root: Path | None = None,
        *,
        fault_hook=None,
    ) -> PromptArchiveRuntime:
        archive_root = self.base / "archive" if root is None else root
        config = PromptArchiveRuntimeConfig(
            mode="shadow",
            root=archive_root,
            max_hydrated_bytes=8 * 1024 * 1024,
        )
        storage = PromptArchiveStorage(
            archive_root,
            PROJECT_ID,
            protected_roots=(),
            limits=PromptArchiveLimits(max_hydrated_bytes=8 * 1024 * 1024),
            fault_hook=fault_hook,
        )
        return PromptArchiveRuntime(config, PROJECT_ID, storage)

    def publish_batch_plan(
        self,
        runtime: PromptArchiveRuntime,
        *,
        ordinal: int = 1,
        raw: bytes = RAW_LEGACY_RESPONSE,
    ) -> tuple[dict, dict, dict]:
        descriptor = self.inventory_descriptor()
        descriptor_raw = canonical_json_bytes_v1(descriptor, maximum=64 * 1024)
        inventory_ref = runtime.storage.describe_blob(
            descriptor_raw,
            logical_kind="source_inventory",
            media_type=(
                "application/vnd.nginx-qa.prompt-archive-source-inventory-v1+json"
            ),
        )
        inventory_locator = {
            "locator_kind": "isolated_inventory",
            "locator_version": "prompt-archive-isolated-inventory-locator-v1",
            "inventory_id": descriptor["inventory_id"],
            "inventory_format": descriptor["inventory_format"],
            "inventory_sha256": descriptor["inventory_sha256"],
            "entry_count": descriptor["entry_count"],
            "inventory_ref": inventory_ref,
        }
        reservation = self.reservation(ordinal, raw)
        plan = {
            "schema_version": 1,
            "kind": "prompt_archive_migration_batch_plan",
            "media_type": (
                "application/vnd.nginx-qa.prompt-archive-migration-batch-plan-v1+json"
            ),
            "id_algorithm": "PAS1-ID-v1",
            "batch_id": self.batch_id(),
            "operation_kind": "offline_migration_batch",
            "contract_version": "prompt-archive-offline-migration-contract-v1",
            "tool_version": "prompt-archive-migration-v1",
            "tool_source_sha256": "c" * 64,
            "project_id": PROJECT_ID,
            "sprint_id": None,
            "source_root_handle_id": descriptor["source_root_handle_id"],
            "operator_authorization": {
                "gate": "prompt-archive-offline-migration-v1",
                "principal_kind": "offline_operator",
                "principal_id": "operator-1",
                "scope_source": "offline_operator_gate",
            },
            "source_inventory": inventory_locator,
            "source_safety": {
                "source_root_identity_sha256": descriptor[
                    "source_root_identity_sha256"
                ],
                "safety_snapshot_sha256": "d" * 64,
                "total_bytes": len(raw),
            },
            "reservations": [reservation],
            "created_at": descriptor["created_at"],
        }
        plan_raw = canonical_json_bytes_v1(plan, maximum=8 * 1024 * 1024)
        runtime.storage.publish_immutable_bytes(
            ("migrations", "v1", self.batch_id(), "plan.json"),
            plan_raw,
            expected_sha256=hashlib.sha256(plan_raw).hexdigest(),
        )
        return plan, reservation, inventory_locator

    def register_inventory(
        self,
        runtime: PromptArchiveRuntime,
        *,
        ordinal: int = 1,
        raw: bytes = RAW_LEGACY_RESPONSE,
    ) -> tuple[dict, dict]:
        descriptor = self.inventory_descriptor()
        self.publish_batch_plan(runtime, ordinal=ordinal, raw=raw)
        locator = runtime.publish_offline_inventory_descriptor(descriptor)
        return descriptor, locator

    def publish(
        self,
        runtime: PromptArchiveRuntime,
        *,
        ordinal: int = 1,
        raw: bytes = RAW_LEGACY_RESPONSE,
    ):
        descriptor, locator = self.register_inventory(
            runtime,
            ordinal=ordinal,
            raw=raw,
        )
        occurrence = self.occurrence(ordinal, raw)
        operation_id = self.operation_id(ordinal)
        publication = runtime.publish_offline_migration(
            raw,
            occurrence,
            operation_id=operation_id,
            inventory_descriptor=descriptor,
            inventory_ref=locator["inventory_ref"],
            inventory_verifier=lambda observed: (
                "verified" if observed == locator else "source_changed"
            ),
        )
        return publication, occurrence, operation_id, descriptor, locator

    def assert_code(self, code: str, operation) -> PromptArchiveStorageError:
        with self.assertRaises(PromptArchiveStorageError) as caught:
            operation()
        self.assertEqual(caught.exception.code, code)
        return caught.exception

    def test_registers_valid_inventory_descriptor_idempotently(self) -> None:
        runtime = self.runtime()
        descriptor, first = self.register_inventory(runtime)

        second = runtime.publish_offline_inventory_descriptor(descriptor)

        self.assertEqual(first, second)
        self.assertEqual(first["inventory_id"], descriptor["inventory_id"])
        self.assertEqual(first["inventory_sha256"], descriptor["inventory_sha256"])
        self.assertEqual(first["entry_count"], descriptor["entry_count"])
        self.assertEqual(first["inventory_ref"]["logical_kind"], "source_inventory")
        stable = runtime.storage.read_immutable_json(
            ("inventories", "v1", f"{descriptor['inventory_id']}.json")
        )
        self.assertEqual(stable, descriptor)

    def test_direct_offline_bypass_requires_batch_plan_and_offline_operator(self) -> None:
        runtime = self.runtime()
        descriptor = self.inventory_descriptor()

        self.assert_code(
            "PROMPT_ARCHIVE_NOT_FOUND",
            lambda: runtime.publish_offline_inventory_descriptor(descriptor),
        )
        self.assert_code(
            "PROMPT_ARCHIVE_NOT_FOUND",
            lambda: runtime.storage.read_immutable_json(
                ("inventories", "v1", f"{descriptor['inventory_id']}.json")
            ),
        )

        descriptor, locator = self.register_inventory(runtime)
        occurrence = self.occurrence()
        synthetic = dataclasses.replace(
            occurrence,
            authorization={
                "auth_model_version": "prompt-archive-authorization-v1",
                "principal_kind": "synthetic_test",
                "principal_id": "synthetic-test",
                "binding_id": None,
                "scope_source": "synthetic_test",
                "effective_scope_revision": None,
                "effective_scope_sha256": None,
                "decision_occurrence_id": "synthetic-decision-1",
            },
        )
        self.assert_code(
            "PROMPT_ARCHIVE_GENERATION_CONFLICT",
            lambda: runtime.publish_offline_migration(
                RAW_LEGACY_RESPONSE,
                synthetic,
                operation_id=self.operation_id(),
                inventory_descriptor=descriptor,
                inventory_ref=locator["inventory_ref"],
                inventory_verifier=lambda _observed: "verified",
            ),
        )
        self.assert_code(
            "PROMPT_ARCHIVE_NOT_FOUND",
            lambda: runtime.storage.read_immutable_json(
                (
                    "operations",
                    "v1",
                    occurrence.response_id,
                    self.operation_id(),
                    "plan.json",
                )
            ),
        )

    def test_first_offline_write_rejects_every_source_metadata_shape_before_verifier_or_publication(
        self,
    ) -> None:
        projection = self.source_metadata_projection()
        variants = (
            ("projection-only", projection.document, None),
            ("receipt-only", None, projection.receipt),
            ("projection-and-receipt", projection.document, projection.receipt),
        )

        for ordinal, (name, document, receipt) in enumerate(variants, start=60):
            with self.subTest(name=name):
                runtime = self.runtime(self.base / f"first-write-{name}")
                descriptor, locator = self.register_inventory(
                    runtime,
                    ordinal=ordinal,
                )
                occurrence = dataclasses.replace(
                    self.occurrence(ordinal),
                    source_metadata_projection=document,
                    source_metadata_receipt=receipt,
                )
                operation_id = self.operation_id(ordinal)
                verifier_calls: list[dict] = []

                def verifier(observed: dict) -> str:
                    verifier_calls.append(copy.deepcopy(observed))
                    return "verified"

                with runtime.storage.account_cas_writes() as observed_writes:
                    self.assert_code(
                        "PROMPT_ARCHIVE_REFERENCE_INVALID",
                        lambda: runtime.publish_offline_migration(
                            RAW_LEGACY_RESPONSE,
                            occurrence,
                            operation_id=operation_id,
                            inventory_descriptor=descriptor,
                            inventory_ref=locator["inventory_ref"],
                            inventory_verifier=verifier,
                        ),
                    )

                self.assertEqual(verifier_calls, [])
                outcomes = observed_writes.snapshot()
                self.assertEqual(outcomes.new_object_count, 0)
                self.assertEqual(outcomes.new_stored_bytes, 0)
                self.assertEqual(outcomes.dedup_hits, 0)
                self.assert_code(
                    "PROMPT_ARCHIVE_NOT_FOUND",
                    lambda: runtime.storage.read_immutable_json(
                        (
                            "operations",
                            "v1",
                            occurrence.response_id,
                            operation_id,
                            "plan.json",
                        )
                    ),
                )

    def test_retained_offline_plan_rejects_source_metadata_retry_without_poisoning_replay(
        self,
    ) -> None:
        root = self.base / "retained-plan-source-metadata-retry"

        def crash_after_plan(stage: str) -> None:
            if stage == "after_publication_plan":
                raise InjectedCrash()

        failing = self.runtime(root, fault_hook=crash_after_plan)
        descriptor, locator = self.register_inventory(failing, ordinal=70)
        occurrence = self.occurrence(70)
        operation_id = self.operation_id(70)
        with self.assertRaises(InjectedCrash):
            failing.publish_offline_migration(
                RAW_LEGACY_RESPONSE,
                occurrence,
                operation_id=operation_id,
                inventory_descriptor=descriptor,
                inventory_ref=locator["inventory_ref"],
                inventory_verifier=lambda _observed: "verified",
            )

        restarted = self.runtime(root)
        plan_path = (
            "operations",
            "v1",
            occurrence.response_id,
            operation_id,
            "plan.json",
        )
        retained_plan = restarted.storage.read_immutable_json(plan_path)
        projection = self.source_metadata_projection()
        poisoned = dataclasses.replace(
            occurrence,
            source_metadata_projection=projection.document,
            source_metadata_receipt=projection.receipt,
        )
        poisoned_verifier_calls: list[dict] = []

        def poisoned_verifier(observed: dict) -> str:
            poisoned_verifier_calls.append(copy.deepcopy(observed))
            return "verified"

        self.assert_code(
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
            lambda: restarted.publish_offline_migration(
                RAW_LEGACY_RESPONSE,
                poisoned,
                operation_id=operation_id,
                inventory_descriptor=descriptor,
                inventory_ref=locator["inventory_ref"],
                inventory_verifier=poisoned_verifier,
            ),
        )
        self.assertEqual(poisoned_verifier_calls, [])
        self.assertEqual(restarted.storage.read_immutable_json(plan_path), retained_plan)

        def replay_unpoisoned():
            return restarted.publish_offline_migration(
                RAW_LEGACY_RESPONSE,
                occurrence,
                operation_id=operation_id,
                inventory_descriptor=descriptor,
                inventory_ref=locator["inventory_ref"],
                inventory_verifier=lambda observed: (
                    "verified" if observed == locator else "source_changed"
                ),
            )

        first = replay_unpoisoned()
        second = replay_unpoisoned()
        self.assertEqual(first, second)
        self.assertEqual(restarted.storage.read_immutable_json(plan_path), retained_plan)
        selected = restarted.lookup_offline_archive_metadata(
            archive_id=first.archive_id
        )
        self.assertEqual(restarted.hydrate_content(selected), RAW_LEGACY_RESPONSE)

    def test_legacy_manifest_rejects_source_metadata_root_and_receipt_tampering(
        self,
    ) -> None:
        runtime = self.runtime(self.base / "legacy-source-metadata-tampering")
        publication, _, _, _, _ = self.publish(runtime, ordinal=80)
        selected = runtime.lookup_offline_archive_metadata(
            archive_id=publication.archive_id
        )
        incremental = PromptArchiveIncremental(runtime.storage, PROJECT_ID, None)
        projection = self.source_metadata_projection()
        source_metadata_root = copy.deepcopy(selected.manifest["roots"]["raw_bytes"])

        def add_root(manifest: dict) -> None:
            manifest["roots"]["source_metadata"] = copy.deepcopy(
                source_metadata_root
            )

        def add_receipt(manifest: dict) -> None:
            manifest["source_metadata_receipt"] = projection.receipt

        def add_root_and_receipt(manifest: dict) -> None:
            add_root(manifest)
            add_receipt(manifest)

        for name, mutate in (
            ("source-metadata-root", add_root),
            ("source-metadata-receipt", add_receipt),
            ("source-metadata-root-and-receipt", add_root_and_receipt),
        ):
            with self.subTest(name=name):
                tampered = copy.deepcopy(selected.manifest)
                mutate(tampered)
                tampered_raw = canonical_json_bytes_v1(
                    tampered,
                    maximum=8 * 1024 * 1024,
                )
                tampered_ref = runtime.storage.put_blob(
                    tampered_raw,
                    logical_kind="manifest",
                    media_type=(
                        "application/vnd.nginx-qa.prompt-archive-manifest-v1+json"
                    ),
                )
                self.assert_code(
                    "PROMPT_ARCHIVE_REFERENCE_INVALID",
                    lambda ref=tampered_ref: incremental.read_manifest_metadata(ref),
                )

    def test_bound_locator_rejects_batch_plan_sha_and_reservation_tamper(self) -> None:
        runtime = self.runtime()
        _, occurrence, operation_id, _, _ = self.publish(runtime)
        plan = runtime.storage.read_immutable_json(
            (
                "operations",
                "v1",
                occurrence.response_id,
                operation_id,
                "plan.json",
            )
        )
        source_locator = plan["source_locator"]
        verified, _ = runtime._validate_inventory_locator(
            source_locator,
            require_migration_binding=True,
        )
        self.assertEqual(verified, source_locator)

        wrong_plan_sha = copy.deepcopy(source_locator)
        wrong_plan_sha["migration_binding"]["batch_plan_sha256"] = "0" * 64
        self.assert_code(
            "PROMPT_ARCHIVE_INTEGRITY_MISMATCH",
            lambda: runtime._validate_inventory_locator(
                wrong_plan_sha,
                require_migration_binding=True,
            ),
        )

        wrong_reservation = copy.deepcopy(source_locator)
        wrong_reservation["migration_binding"]["reservation"][
            "source_sha256"
        ] = "0" * 64
        self.assert_code(
            "PROMPT_ARCHIVE_INTEGRITY_MISMATCH",
            lambda: runtime._validate_inventory_locator(
                wrong_reservation,
                require_migration_binding=True,
            ),
        )

    def test_intent_verifier_receives_unbound_base_inventory_locator(self) -> None:
        runtime = self.runtime()
        descriptor, locator = self.register_inventory(runtime)
        occurrence = self.occurrence()
        observed_locators: list[dict] = []

        def verify(observed: dict) -> str:
            observed_locators.append(copy.deepcopy(observed))
            return "verified" if observed == locator else "source_changed"

        runtime.publish_offline_migration(
            RAW_LEGACY_RESPONSE,
            occurrence,
            operation_id=self.operation_id(),
            inventory_descriptor=descriptor,
            inventory_ref=locator["inventory_ref"],
            inventory_verifier=verify,
        )

        self.assertGreaterEqual(len(observed_locators), 1)
        self.assertTrue(all(observed == locator for observed in observed_locators))
        self.assertTrue(
            all("migration_binding" not in observed for observed in observed_locators)
        )
        plan = runtime.storage.read_immutable_json(
            (
                "operations",
                "v1",
                occurrence.response_id,
                self.operation_id(),
                "plan.json",
            )
        )
        self.assertIn("migration_binding", plan["source_locator"])

    def test_null_sprint_publication_is_byte_exact_and_closed_lookup_hydrates(self) -> None:
        runtime = self.runtime()

        publication, occurrence, _, _, _ = self.publish(runtime)
        selected = runtime.lookup_offline_archive_metadata(
            archive_id=publication.archive_id
        )

        self.assertIsNone(publication.sprint_id)
        self.assertIsNone(selected.sprint_id)
        self.assertEqual(selected.locator["archive_id"], publication.archive_id)
        self.assertEqual(selected.locator["pointer_id"], publication.pointer_id)
        self.assertEqual(selected.pointer["response_id"], occurrence.response_id)
        self.assertEqual(
            selected.pointer["manifest_ref"], selected.locator["manifest_ref"]
        )
        self.assertEqual(selected.manifest["scope"]["sprint_id"], None)
        self.assertEqual(selected.manifest["source"]["source_kind"], "offline_migration")
        self.assertEqual(runtime.read_manifest(selected), selected.manifest)
        self.assertEqual(runtime.hydrate_content(selected), RAW_LEGACY_RESPONSE)
        self.assertEqual(
            selected.manifest["integrity"]["hydrated_sha256"],
            hashlib.sha256(RAW_LEGACY_RESPONSE).hexdigest(),
        )

    def test_normal_sprint_lookup_rejects_none(self) -> None:
        runtime = self.runtime()
        publication, _, _, _, _ = self.publish(runtime)

        self.assert_code(
            "PROMPT_ARCHIVE_REQUEST_INVALID",
            lambda: runtime.lookup_archive(
                sprint_id=None,  # type: ignore[arg-type]
                archive_id=publication.archive_id,
            ),
        )

    def test_operation_id_replay_is_idempotent(self) -> None:
        runtime = self.runtime()
        descriptor, locator = self.register_inventory(runtime)
        occurrence = self.occurrence()
        operation_id = self.operation_id()

        def execute():
            return runtime.publish_offline_migration(
                RAW_LEGACY_RESPONSE,
                occurrence,
                operation_id=operation_id,
                inventory_descriptor=descriptor,
                inventory_ref=locator["inventory_ref"],
                inventory_verifier=lambda observed: (
                    "verified" if observed == locator else "source_changed"
                ),
            )

        first = execute()
        second = execute()

        self.assertEqual(first, second)
        selected = runtime.lookup_offline_archive_metadata(archive_id=first.archive_id)
        self.assertEqual(selected.pointer["generation"], 1)
        journal = runtime.storage.read_current_json(
            (
                "operations",
                "v1",
                occurrence.response_id,
                operation_id,
                "journal.json",
            )
        )
        self.assertEqual(journal["state"], "complete")
        self.assertEqual(journal["journal_generation"], 6)

    def test_changed_or_unavailable_source_at_intent_aborts_without_pointer(self) -> None:
        for ordinal, status in enumerate(
            ("source_changed", "source_unavailable"), start=10
        ):
            with self.subTest(status=status):
                root = self.base / status
                journal_currents = 0

                def crash_after_intent(stage: str) -> None:
                    nonlocal journal_currents
                    if stage == "after_journal_current":
                        journal_currents += 1
                        if journal_currents == 1:
                            raise InjectedCrash()

                failing = self.runtime(root, fault_hook=crash_after_intent)
                descriptor, locator = self.register_inventory(
                    failing,
                    ordinal=ordinal,
                )
                occurrence = self.occurrence(ordinal)
                operation_id = self.operation_id(ordinal)
                with self.assertRaises(InjectedCrash):
                    failing.publish_offline_migration(
                        RAW_LEGACY_RESPONSE,
                        occurrence,
                        operation_id=operation_id,
                        inventory_descriptor=descriptor,
                        inventory_ref=locator["inventory_ref"],
                        inventory_verifier=lambda _observed: "verified",
                    )

                restarted = self.runtime(root)
                self.assertEqual(restarted.recover_pending(), 0)
                plan = restarted.storage.read_immutable_json(
                    (
                        "operations",
                        "v1",
                        occurrence.response_id,
                        operation_id,
                        "plan.json",
                    )
                )
                before = restarted.storage.read_current_json(
                    restarted._journal_current_path(plan)
                )
                self.assertEqual(before["state"], "intent")

                self.assert_code(
                    "PROMPT_ARCHIVE_UNAVAILABLE",
                    lambda: restarted.resume_offline_migration(
                        response_id=occurrence.response_id,
                        operation_id=operation_id,
                        inventory_verifier=lambda _observed, value=status: value,
                    ),
                )
                aborted = restarted.storage.read_current_json(
                    restarted._journal_current_path(plan)
                )
                self.assertEqual(aborted["state"], "aborted")
                self.assertEqual(aborted["abort_from_state"], "intent")
                self.assertEqual(aborted["abort_reason_code"], status)

                sprint_key = sprint_storage_key_v1(PROJECT_ID, None)
                self.assert_code(
                    "PROMPT_ARCHIVE_NOT_FOUND",
                    lambda: restarted.storage.read_immutable_json(
                        (
                            "sprints",
                            sprint_key,
                            "pointers",
                            "v1",
                            f"{occurrence.response_id}.json",
                        )
                    ),
                )
                self.assert_code(
                    "PROMPT_ARCHIVE_NOT_FOUND",
                    lambda: restarted.lookup_offline_archive_metadata(
                        archive_id=plan["intended_manifest"]["archive_id"]
                    ),
                )

    def test_explicit_resume_after_blobs_verified_needs_no_source_verifier(self) -> None:
        root = self.base / "resume-after-blobs"
        journal_currents = 0

        def crash_after_blobs_verified(stage: str) -> None:
            nonlocal journal_currents
            if stage == "after_journal_current":
                journal_currents += 1
                if journal_currents == 2:
                    raise InjectedCrash()

        failing = self.runtime(root, fault_hook=crash_after_blobs_verified)
        descriptor, locator = self.register_inventory(failing, ordinal=20)
        occurrence = self.occurrence(20)
        operation_id = self.operation_id(20)
        with self.assertRaises(InjectedCrash):
            failing.publish_offline_migration(
                RAW_LEGACY_RESPONSE,
                occurrence,
                operation_id=operation_id,
                inventory_descriptor=descriptor,
                inventory_ref=locator["inventory_ref"],
                inventory_verifier=lambda _observed: "verified",
            )

        restarted = self.runtime(root)
        self.assertEqual(restarted.recover_pending(), 0)
        plan = restarted.storage.read_immutable_json(
            (
                "operations",
                "v1",
                occurrence.response_id,
                operation_id,
                "plan.json",
            )
        )
        interrupted = restarted.storage.read_current_json(
            restarted._journal_current_path(plan)
        )
        self.assertEqual(interrupted["state"], "blobs_verified")

        publication = restarted.resume_offline_migration(
            response_id=occurrence.response_id,
            operation_id=operation_id,
            inventory_verifier=None,
        )

        selected = restarted.lookup_offline_archive_metadata(
            archive_id=publication.archive_id
        )
        self.assertEqual(restarted.hydrate_content(selected), RAW_LEGACY_RESPONSE)
        completed = restarted.storage.read_current_json(
            restarted._journal_current_path(plan)
        )
        self.assertEqual(completed["state"], "complete")

    def test_recover_pending_validates_offline_corrupt_lower_bound_before_skip(
        self,
    ) -> None:
        root = self.base / "corrupt-offline-lower-bound"
        journal_currents = 0

        def crash_after_blobs_verified(stage: str) -> None:
            nonlocal journal_currents
            if stage == "after_journal_current":
                journal_currents += 1
                if journal_currents == 2:
                    raise InjectedCrash()

        failing = self.runtime(root, fault_hook=crash_after_blobs_verified)
        descriptor, locator = self.register_inventory(failing, ordinal=30)
        occurrence = self.occurrence(30)
        operation_id = self.operation_id(30)
        with self.assertRaises(InjectedCrash):
            failing.publish_offline_migration(
                RAW_LEGACY_RESPONSE,
                occurrence,
                operation_id=operation_id,
                inventory_descriptor=descriptor,
                inventory_ref=locator["inventory_ref"],
                inventory_verifier=lambda _observed: "verified",
            )

        restarted = self.runtime(root)
        plan = restarted.storage.read_immutable_json(
            (
                "operations",
                "v1",
                occurrence.response_id,
                operation_id,
                "plan.json",
            )
        )
        current = restarted.storage.read_current_json(
            restarted._journal_current_path(plan)
        )
        self.assertEqual(current["state"], "blobs_verified")
        corrupt_raw = canonical_json_bytes_v1(
            {"journal_generation": current["journal_generation"]},
            maximum=64 * 1024,
        )
        corrupt_ref = restarted.storage.put_blob(
            corrupt_raw,
            logical_kind="journal",
            media_type=(
                "application/vnd.nginx-qa.prompt-archive-journal-v1+json"
            ),
        )
        shard_id = plan["pending_registry"]["shard"]["shard_id"]
        with restarted.storage.control_lock(restarted._registry_lock_path(shard_id)):
            registry = restarted._read_registry_locked(shard_id)
            self.assertIsNotNone(registry)
            entries = copy.deepcopy(registry["entries"])
            matching = [
                entry
                for entry in entries
                if entry["operation_id"] == operation_id
            ]
            self.assertEqual(len(matching), 1)
            self.assertEqual(matching[0]["state"], "journaled")
            matching[0]["journal_ref"] = corrupt_ref
            restarted._write_registry_locked(
                shard_id=shard_id,
                previous=registry,
                entries=entries,
                published_at=registry["published_at"],
            )

        self.assert_code(
            "PROMPT_ARCHIVE_INTEGRITY_MISMATCH",
            restarted.recover_pending,
        )

    def test_recover_pending_removes_terminal_offline_entry_without_republishing(
        self,
    ) -> None:
        root = self.base / "terminal-offline-recovery"
        journal_currents = 0

        def crash_after_terminal_journal(stage: str) -> None:
            nonlocal journal_currents
            if stage == "after_journal_current":
                journal_currents += 1
                if journal_currents == 7:
                    raise InjectedCrash()

        failing = self.runtime(root, fault_hook=crash_after_terminal_journal)
        descriptor, locator = self.register_inventory(failing, ordinal=40)
        occurrence = self.occurrence(40)
        operation_id = self.operation_id(40)
        with self.assertRaises(InjectedCrash):
            failing.publish_offline_migration(
                RAW_LEGACY_RESPONSE,
                occurrence,
                operation_id=operation_id,
                inventory_descriptor=descriptor,
                inventory_ref=locator["inventory_ref"],
                inventory_verifier=lambda _observed: "verified",
            )

        restarted = self.runtime(root)
        plan = restarted.storage.read_immutable_json(
            (
                "operations",
                "v1",
                occurrence.response_id,
                operation_id,
                "plan.json",
            )
        )
        before = restarted.storage.read_current_json(
            restarted._journal_current_path(plan)
        )
        self.assertEqual(before["state"], "complete")
        self.assertEqual(before["journal_generation"], 6)
        shard_id = plan["pending_registry"]["shard"]["shard_id"]
        with restarted.storage.control_lock(restarted._registry_lock_path(shard_id)):
            pending_before = restarted._read_registry_locked(shard_id)
        self.assertIsNotNone(pending_before)
        matching_before = [
            entry
            for entry in pending_before["entries"]
            if entry["operation_id"] == operation_id
        ]
        self.assertEqual(len(matching_before), 1)
        self.assertEqual(matching_before[0]["state"], "journaled")

        self.assertEqual(restarted.recover_pending(), 1)
        self.assertEqual(restarted.recover_pending(), 0)

        after = restarted.storage.read_current_json(
            restarted._journal_current_path(plan)
        )
        self.assertEqual(after, before)
        with restarted.storage.control_lock(restarted._registry_lock_path(shard_id)):
            pending_after = restarted._read_registry_locked(shard_id)
        self.assertIsNotNone(pending_after)
        self.assertFalse(
            any(
                entry["operation_id"] == operation_id
                for entry in pending_after["entries"]
            )
        )

        sprint_key = sprint_storage_key_v1(PROJECT_ID, None)
        pointer = restarted.storage.read_immutable_json(
            (
                "sprints",
                sprint_key,
                "pointers",
                "v1",
                f"{occurrence.response_id}.json",
            )
        )
        archive_locator = restarted.storage.read_immutable_json(
            (
                "sprints",
                sprint_key,
                "archives",
                "v1",
                f"{plan['intended_manifest']['archive_id']}.json",
            )
        )
        self.assertEqual(pointer, plan["intended_pointer"]["document"])
        self.assertEqual(
            archive_locator,
            plan["intended_archive_locator"]["document"],
        )
        selected = restarted.lookup_offline_archive_metadata(
            archive_id=plan["intended_manifest"]["archive_id"]
        )
        self.assertEqual(restarted.hydrate_content(selected), RAW_LEGACY_RESPONSE)

    def test_offline_plan_and_journal_validate_accepted_contracts(self) -> None:
        # Import the contract fixture as a module so unittest does not discover
        # its TestCase a second time in this focused module.
        from tests import test_prompt_archive_contract as contract

        runtime = self.runtime()
        _, occurrence, operation_id, descriptor, _ = self.publish(runtime)
        plan = runtime.storage.read_immutable_json(
            (
                "operations",
                "v1",
                occurrence.response_id,
                operation_id,
                "plan.json",
            )
        )
        current = runtime.storage.read_current_json(runtime._journal_current_path(plan))
        journals = {current["journal_generation"]: current}
        cursor = current
        while cursor["previous_journal_generation"] is not None:
            predecessor = runtime.storage.read_immutable_json(
                runtime._journal_generation_path(
                    plan,
                    cursor["previous_journal_generation"],
                    cursor["previous_journal_sha256"],
                )
            )
            self.assertEqual(
                predecessor["journal_generation"],
                cursor["previous_journal_generation"],
            )
            journals[predecessor["journal_generation"]] = predecessor
            cursor = predecessor
        self.assertEqual(set(journals), set(range(7)))

        contract.PromptArchiveContractTests.setUpClass()
        validator_fixture = contract.PromptArchiveContractTests()
        validator_fixture.validator(
            "prompt-archive-source-inventory-v1.schema.json"
        ).validate(descriptor)
        validator_fixture.validator(
            "prompt-archive-publication-plan-v1.schema.json"
        ).validate(plan)
        self.assertEqual(contract.publication_plan_semantic_issues(plan), ())
        journal_validator = validator_fixture.validator(
            "prompt-archive-journal-v1.schema.json"
        )
        for generation in range(7):
            with self.subTest(journal_generation=generation):
                journal = journals[generation]
                predecessor = journals.get(generation - 1)
                journal_validator.validate(journal)
                self.assertEqual(
                    contract.journal_semantic_issues(journal, plan, predecessor),
                    (),
                )


if __name__ == "__main__":
    unittest.main()
