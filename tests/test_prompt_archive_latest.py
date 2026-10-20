import dataclasses
import hashlib
import json
import multiprocessing
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from nginx_qa.prompt_archive_latest import (
    FAULT_AFTER_CURRENT_INSTALL,
    FAULT_AFTER_OPERATION_PLAN,
    FAULT_AFTER_PENDING_BIND,
    FAULT_AFTER_TERMINAL_BEFORE_PENDING_REMOVE,
    FAULT_STAGES,
    ExternalLatestCoordinator,
    publish_authoritative_transition_stub,
)
from nginx_qa.prompt_archive_storage import PromptArchiveStorageError
from nginx_qa.prompt_archive_storage import canonical_json_bytes_v1


PROJECT_ID = "9000"


def operation_id(label: str) -> str:
    return "parop1-" + hashlib.sha256(label.encode()).hexdigest()[:32]


def response_id(label: str) -> str:
    return "parresp1-" + hashlib.sha256(label.encode()).hexdigest()


def descriptor_ref(payload: bytes) -> dict:
    digest = hashlib.sha256(payload).hexdigest()
    return {
        "schema_version": 1,
        "kind": "prompt_archive_blob_ref",
        "hash_algorithm": "sha256",
        "digest": digest,
        "logical_kind": "descriptor_stub",
        "media_type": (
            "application/vnd.nginx-qa.prompt-archive-descriptor-stub-v1+json"
        ),
        "content_encoding": "identity",
        "uncompressed_size": len(payload),
        "stored_size": len(payload),
        "stored_sha256": digest,
    }


def archive_owner(
    coordinator: ExternalLatestCoordinator,
    intent: dict,
    label: str,
    transition_stub_path: Path,
    *,
    response_kind: str = "transition-response",
) -> dict:
    digest = hashlib.sha256(label.encode()).hexdigest()
    owner = {
        "protocol": "prompt-archive-latest-archive-owner-v1",
        "project_id": PROJECT_ID,
        "sprint_id": intent["source_sprint_id"],
        "response_id": intent["source_response_id"],
        "operation_id": intent["ordering"]["operation_id"],
        "recovery_key": f"parloc1-{digest}",
        "outer_plan_identity_sha256": "",
        "archive_id": f"par1-{digest}",
        "pointer_id": f"parptr1-{digest}",
        "locator_id": f"pararc1-{digest}",
        "target_authority_id": coordinator.target_authority_id,
        "response_kind": response_kind,
        "transition_stub_name": transition_stub_path.name,
        "descriptor_stub_id": f"parstub1-{digest}",
    }
    projection = {
        key: owner[key]
        for key in (
            "project_id",
            "sprint_id",
            "response_id",
            "operation_id",
            "recovery_key",
            "archive_id",
            "pointer_id",
            "locator_id",
            "target_authority_id",
            "response_kind",
            "transition_stub_name",
            "descriptor_stub_id",
        )
    }
    owner["outer_plan_identity_sha256"] = hashlib.sha256(
        canonical_json_bytes_v1(projection, maximum=64 * 1024)
    ).hexdigest()
    return owner


def process_publish_latest(
    target: str,
    revision: int,
    label: str,
    payload: bytes,
    result_queue,
) -> None:
    try:
        result = ExternalLatestCoordinator(
            Path(target), project_id=PROJECT_ID
        ).publish(
            payload,
            occurrence_id=f"occ-{label}",
            operation_id=operation_id(label),
            published_at=f"2026-10-12T00:00:{revision:02d}Z",
            source_response_id=response_id(label),
            source_revision=revision,
        )
        result_queue.put(("ok", dataclasses.asdict(result)))
    except PromptArchiveStorageError as exc:  # pragma: no cover - parent reports
        result_queue.put(("storage-error", exc.code, str(exc)))
    except BaseException as exc:  # pragma: no cover - parent reports
        result_queue.put(("error", type(exc).__name__, str(exc)))


class InjectedLatestFault(RuntimeError):
    pass


def process_fault_publish_latest(
    target: str,
    stage: str,
    revision: int,
    label: str,
    payload: bytes,
    result_queue,
) -> None:
    fired = False

    def inject(observed: str) -> None:
        nonlocal fired
        if observed == stage and not fired:
            fired = True
            raise InjectedLatestFault(stage)

    try:
        ExternalLatestCoordinator(
            Path(target),
            project_id=PROJECT_ID,
            fault_hook=inject,
        ).publish(
            payload,
            occurrence_id=f"occ-{label}",
            operation_id=operation_id(label),
            published_at=f"2026-10-12T00:01:{revision:02d}Z",
            source_response_id=response_id(label),
            source_revision=revision,
        )
    except InjectedLatestFault:
        result_queue.put(("fault", stage, fired))
        return
    except BaseException as exc:  # pragma: no cover - parent reports
        result_queue.put(("error", type(exc).__name__, str(exc)))
        return
    result_queue.put(("unexpected-success", stage, fired))


class ExternalLatestCoordinatorTests(unittest.TestCase):
    def coordinator(
        self,
        directory: Path,
        *,
        fault_hook=None,
        archive_owner_verifier=None,
    ):
        return ExternalLatestCoordinator(
            directory / "latest-response.json",
            project_id=PROJECT_ID,
            fault_hook=fault_hook,
            archive_owner_verifier=archive_owner_verifier,
        )

    @staticmethod
    def owner_verifier(expected: dict):
        return lambda observed, _intent: observed == expected

    @staticmethod
    def owner_publish_args(
        coordinator: ExternalLatestCoordinator,
        intent: dict,
        label: str,
        transition_stub_path: Path,
    ) -> tuple[dict, dict]:
        owner = archive_owner(
            coordinator,
            intent,
            label,
            transition_stub_path,
        )
        return owner, {
            "archive_owner": owner,
            "expected_target_authority_id": coordinator.target_authority_id,
        }

    @staticmethod
    def publish_args(label: str, **overrides):
        values = {
            "occurrence_id": f"occ-{label}",
            "operation_id": operation_id(label),
            "published_at": "2026-10-12T11:35:00Z",
            "source_response_id": response_id(label),
        }
        values.update(overrides)
        return values

    def test_initial_publish_replay_restart_and_fixed_recovery_roots(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            target = directory / "latest-response.json"
            target.write_bytes(b'{"legacy":true}')
            coordinator = self.coordinator(directory)

            self.assertFalse(coordinator.enrolled())
            result = coordinator.publish(
                b'{"answer":1}', **self.publish_args("first")
            )

            self.assertEqual(result.outcome, "published")
            self.assertEqual(result.source_revision, 1)
            self.assertEqual(result.generation, 1)
            self.assertEqual(target.read_bytes(), b'{"answer":1}')
            sidecar = directory / "latest-response.json.nginx-qa-v1"
            intent_raw = (sidecar / "intent.json").read_bytes()
            slot_raw = (sidecar / "slot.json").read_bytes()
            intent = json.loads(intent_raw)
            slot = json.loads(slot_raw)
            plan_sha = hashlib.sha256(intent_raw).hexdigest()
            retained_intent = (
                sidecar
                / "intents"
                / "v1"
                / f"{intent['latest_intent']['intent_id']}-{plan_sha}.json"
            )
            retained_slot = (
                sidecar
                / "slots"
                / "v1"
                / f"1-{slot['latest_slot_id']}.json"
            )
            self.assertEqual(retained_intent.read_bytes(), intent_raw)
            self.assertEqual(retained_slot.read_bytes(), slot_raw)

            restarted = self.coordinator(directory)
            self.assertTrue(restarted.enrolled())
            recovered = restarted.recover()
            self.assertIsNotNone(recovered)
            self.assertTrue(recovered.recovered)
            self.assertEqual(recovered.outcome, "idempotent")
            replay = restarted.publish(
                b'{"answer":1}', **self.publish_args("first")
            )
            self.assertEqual(replay.outcome, "idempotent")
            self.assertEqual(replay.latest_slot_id, result.latest_slot_id)

    def test_lexicographic_order_supersedes_regressions_and_detects_divergence(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            coordinator = self.coordinator(directory)
            first = coordinator.publish(
                b'{"value":10}',
                **self.publish_args("ten", source_revision=10),
            )
            older = coordinator.publish(
                b'{"value":9}',
                **self.publish_args("nine", source_revision=9),
            )
            self.assertEqual(older.outcome, "superseded")
            self.assertEqual(older.generation, first.generation)
            self.assertEqual(
                (directory / "latest-response.json").read_bytes(), b'{"value":10}'
            )

            # Equal revisions remain totally ordered by occurrence then operation.
            tied_but_greater = coordinator.publish(
                b'{"value":11}',
                **self.publish_args(
                    "same-revision",
                    source_revision=10,
                    occurrence_id="zz-occurrence",
                ),
            )
            self.assertEqual(tied_but_greater.outcome, "published")
            self.assertEqual(tied_but_greater.generation, 2)

            with self.assertRaises(PromptArchiveStorageError) as caught:
                coordinator.publish(
                    b'{"different":true}',
                    **self.publish_args(
                        "same-revision",
                        source_revision=10,
                        occurrence_id="zz-occurrence",
                    ),
                )
            self.assertEqual(caught.exception.code, "PROMPT_ARCHIVE_GENERATION_CONFLICT")
            self.assertEqual(
                (directory / "latest-response.json").read_bytes(), b'{"value":11}'
            )

    def test_auto_revision_uses_durable_high_water_after_restart(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            first = self.coordinator(directory).publish(
                b'{"revision":41}',
                **self.publish_args("forty-one", source_revision=41),
            )
            self.assertEqual(first.source_revision, 41)
            second = self.coordinator(directory).publish(
                b'{"revision":42}', **self.publish_args("forty-two")
            )
            self.assertEqual(second.source_revision, 42)
            self.assertEqual(second.generation, 2)

    def test_incomplete_current_intent_is_reconciled_before_allocating(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)

            def stop_after_intent(stage: str) -> None:
                if stage == "after_current_intent_publish":
                    raise InjectedLatestFault

            with self.assertRaises(InjectedLatestFault):
                self.coordinator(directory, fault_hook=stop_after_intent).publish(
                    b'{"revision":50}',
                    **self.publish_args("fifty", source_revision=50),
                )
            later = self.coordinator(directory).publish(
                b'{"revision":51}', **self.publish_args("fifty-one")
            )
            self.assertEqual(later.source_revision, 51)
            self.assertEqual(later.generation, 2)
            self.assertEqual(
                (directory / "latest-response.json").read_bytes(), b'{"revision":51}'
            )

    def test_every_publication_boundary_recovers_after_restart(self) -> None:
        for stage in (
            item for item in FAULT_STAGES if item != "after_transition_stub_publish"
        ):
            with self.subTest(stage=stage), tempfile.TemporaryDirectory() as raw:
                directory = Path(raw)
                target = directory / "latest-response.json"
                target.write_bytes(b'{"old":true}')
                fired = False

                def inject(observed: str) -> None:
                    nonlocal fired
                    if observed == stage and not fired:
                        fired = True
                        raise InjectedLatestFault(stage)

                kwargs = self.publish_args(f"fault-{stage}")
                with self.assertRaises(InjectedLatestFault):
                    self.coordinator(directory, fault_hook=inject).publish(
                        b'{"new":true}', **kwargs
                    )
                self.assertTrue(fired)

                restarted = self.coordinator(directory)
                recovered = restarted.recover()
                if recovered is None:
                    recovered = restarted.publish(b'{"new":true}', **kwargs)
                self.assertIn(recovered.outcome, {"published", "idempotent"})
                self.assertEqual(target.read_bytes(), b'{"new":true}')
                self.assertEqual(restarted.recover().outcome, "idempotent")

    def test_plan_retry_boundary_and_registered_startup_boundaries(self) -> None:
        boundaries = (
            FAULT_AFTER_OPERATION_PLAN,
            FAULT_AFTER_PENDING_BIND,
            FAULT_AFTER_CURRENT_INSTALL,
            FAULT_AFTER_TERMINAL_BEFORE_PENDING_REMOVE,
        )
        for stage in boundaries:
            with self.subTest(stage=stage), tempfile.TemporaryDirectory() as raw:
                directory = Path(raw)
                fired = False

                def inject(observed: str) -> None:
                    nonlocal fired
                    if observed == stage and not fired:
                        fired = True
                        raise InjectedLatestFault(stage)

                kwargs = self.publish_args(f"focused-{stage}", source_revision=7)
                with self.assertRaises(InjectedLatestFault):
                    self.coordinator(directory, fault_hook=inject).publish(
                        b'{"focused":true}',
                        **kwargs,
                    )
                self.assertTrue(fired)

                restarted = self.coordinator(directory)
                recovered = restarted.recover()
                if stage == FAULT_AFTER_OPERATION_PLAN:
                    # The immutable plan is deliberately not discoverable
                    # before its key is accepted by a fixed pending shard.
                    self.assertIsNone(recovered)
                    recovered = restarted.publish(b'{"focused":true}', **kwargs)
                else:
                    self.assertIsNotNone(recovered)
                self.assertIn(recovered.outcome, {"published", "idempotent"})
                self.assertEqual(
                    (directory / "latest-response.json").read_bytes(),
                    b'{"focused":true}',
                )

    def test_recover_pending_reads_exact_fixed_256_shard_vector(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            coordinator = self.coordinator(Path(raw))
            observed = []
            original = coordinator._read_pending_registry_locked

            def record(shard_id: str):
                observed.append(shard_id)
                return original(shard_id)

            with mock.patch.object(
                coordinator,
                "_read_pending_registry_locked",
                side_effect=record,
            ), mock.patch.object(
                Path,
                "iterdir",
                side_effect=AssertionError("directory enumeration is forbidden"),
            ), mock.patch.object(
                Path,
                "glob",
                side_effect=AssertionError("directory enumeration is forbidden"),
            ):
                self.assertEqual(coordinator.recover_pending(), 0)
            self.assertEqual(observed, [f"{ordinal:02x}" for ordinal in range(256)])

    def test_pending_shard_rebases_unrelated_keys_and_rejects_same_key_conflict(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as raw:
            coordinator = self.coordinator(Path(raw))
            candidates = {}
            collision = None
            for ordinal in range(1, 1025):
                label = f"registry-collision-{ordinal}"
                payload = canonical_json_bytes_v1(
                    {"ordinal": ordinal},
                    maximum=64 * 1024,
                )
                plan = coordinator._new_plan(
                    content_sha256=hashlib.sha256(payload).hexdigest(),
                    content_size=len(payload),
                    content_kind="legacy_inline",
                    descriptor_ref=None,
                    source_sprint_id=None,
                    source_response_id=response_id(label),
                    source_revision=ordinal,
                    occurrence_id=f"occ-{label}",
                    operation_id=operation_id(label),
                    published_at="2026-10-12T11:36:00Z",
                    bootstrap=coordinator._bootstrap("absent"),
                )
                plan_raw = canonical_json_bytes_v1(plan, maximum=8 * 1024 * 1024)
                shard_id = coordinator._pending_shard_id(plan["recovery_key"])
                if shard_id in candidates:
                    collision = (candidates[shard_id], (plan, plan_raw, payload))
                    break
                candidates[shard_id] = (plan, plan_raw, payload)
            self.assertIsNotNone(collision)
            first, second = sorted(
                collision,
                key=lambda item: coordinator._order(item[0]["latest_intent"]),
            )
            for plan, plan_raw, payload in (first, second):
                with coordinator._operation_lock(plan):
                    coordinator._publish_operation_artifacts(
                        plan,
                        plan_raw,
                        payload,
                    )
                coordinator._register_pending(plan, plan_raw)

            shard_id = coordinator._pending_shard_id(first[0]["recovery_key"])
            with coordinator._pending_shard_lock(shard_id):
                registry = coordinator._read_pending_registry_locked(shard_id)
            self.assertEqual(len(registry["entries"]), 2)

            with self.assertRaises(PromptArchiveStorageError) as caught:
                coordinator._register_pending(first[0], first[1] + b"\n")
            self.assertEqual(caught.exception.code, "PROMPT_ARCHIVE_GENERATION_CONFLICT")

            coordinator._complete_registered_plan(first[0], first[1], recovered=False)
            with coordinator._pending_shard_lock(shard_id):
                registry = coordinator._read_pending_registry_locked(shard_id)
            self.assertEqual(
                [entry["recovery_key"] for entry in registry["entries"]],
                [second[0]["recovery_key"]],
            )
            coordinator._complete_registered_plan(second[0], second[1], recovered=False)
            with coordinator._pending_shard_lock(shard_id):
                registry = coordinator._read_pending_registry_locked(shard_id)
            self.assertEqual(registry["entries"], [])

    def test_prepared_descriptor_is_non_mutating_exact_and_replayable(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            target = directory / "latest-response.json"
            target.write_bytes(b'{"old":true}')
            transition = directory / "transition-response-descriptor.json"
            payload = b'{"kind":"prompt-archive-descriptor-stub-v1"}'
            coordinator = self.coordinator(directory)
            # Establish the stable lock once, then prove prepare changes no
            # authority artifact or external payload.
            self.assertFalse(coordinator.enrolled())
            before = {
                path.relative_to(directory): path.read_bytes()
                for path in directory.rglob("*")
                if path.is_file()
            }
            intent = coordinator.prepare_descriptor_intent(
                payload,
                transition_stub_path=transition,
                descriptor_ref=descriptor_ref(payload),
                source_sprint_id="sprint-1",
                source_revision=7,
                **self.publish_args("prepared"),
            )
            after = {
                path.relative_to(directory): path.read_bytes()
                for path in directory.rglob("*")
                if path.is_file()
            }
            self.assertEqual(after, before)
            self.assertTrue(intent["intent_id"].startswith("parlatestintent1-"))
            owner, owner_args = self.owner_publish_args(
                coordinator,
                intent,
                "prepared-owner",
                transition,
            )
            coordinator = self.coordinator(
                directory,
                archive_owner_verifier=self.owner_verifier(owner),
            )

            result = coordinator.publish_prepared_descriptor(
                payload,
                prepared_intent=intent,
                transition_stub_path=transition,
                **owner_args,
            )
            self.assertEqual(result.outcome, "published")
            self.assertEqual(target.read_bytes(), payload)
            self.assertEqual(transition.read_bytes(), payload)
            self.assertEqual(result.latest_slot_ref["logical_kind"], "latest_slot")
            self.assertEqual(
                result.latest_slot_ref["digest"],
                hashlib.sha256(
                    (coordinator.sidecar / "slot.json").read_bytes()
                ).hexdigest(),
            )

            replay = self.coordinator(
                directory,
                archive_owner_verifier=self.owner_verifier(owner),
            ).publish_prepared_descriptor(
                payload,
                prepared_intent=intent,
                transition_stub_path=transition,
                **owner_args,
            )
            self.assertEqual(replay.outcome, "idempotent")
            self.assertEqual(replay.latest_slot_id, result.latest_slot_id)

    def test_prepared_descriptor_is_bound_to_target_authority(self) -> None:
        with (
            tempfile.TemporaryDirectory() as first_raw,
            tempfile.TemporaryDirectory() as second_raw,
        ):
            first_directory = Path(first_raw)
            second_directory = Path(second_raw)
            first = self.coordinator(first_directory)
            second = self.coordinator(second_directory)
            self.assertNotEqual(
                first.target_authority_id,
                second.target_authority_id,
            )
            payload = b'{"descriptor":"portable"}'
            first_transition = first_directory / "transition.json"
            intent = first.prepare_descriptor_intent(
                payload,
                transition_stub_path=first_transition,
                descriptor_ref=descriptor_ref(payload),
                source_sprint_id="sprint-portability",
                source_revision=2,
                **self.publish_args("portability"),
            )
            owner, owner_args = self.owner_publish_args(
                first,
                intent,
                "portability-owner",
                first_transition,
            )
            second = self.coordinator(
                second_directory,
                archive_owner_verifier=self.owner_verifier(owner),
            )
            with self.assertRaises(PromptArchiveStorageError) as caught:
                second.publish_prepared_descriptor(
                    payload,
                    prepared_intent=intent,
                    transition_stub_path=second_directory / "transition.json",
                    **owner_args,
                )
            self.assertEqual(caught.exception.code, "PROMPT_ARCHIVE_REFERENCE_INVALID")
            self.assertFalse((second_directory / "latest-response.json").exists())
            self.assertFalse((second_directory / "transition.json").exists())
            self.assertFalse((second.sidecar / "intent.json").exists())

    def test_archive_owner_verifier_cross_binds_exact_latest_intent(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            transition = directory / "transition.json"
            first_payload = b'{"descriptor":"first"}'
            second_payload = b'{"descriptor":"second"}'
            planner = self.coordinator(directory)
            shared = self.publish_args("mixed-intent", source_revision=6)
            first_intent = planner.prepare_descriptor_intent(
                first_payload,
                transition_stub_path=transition,
                descriptor_ref=descriptor_ref(first_payload),
                source_sprint_id="sprint-mixed-intent",
                **shared,
            )
            second_intent = planner.prepare_descriptor_intent(
                second_payload,
                transition_stub_path=transition,
                descriptor_ref=descriptor_ref(second_payload),
                source_sprint_id="sprint-mixed-intent",
                **shared,
            )
            owner, owner_args = self.owner_publish_args(
                planner,
                first_intent,
                "mixed-intent-owner",
                transition,
            )

            def verifier(observed_owner, observed_intent):
                return observed_owner == owner and observed_intent == first_intent

            coordinator = self.coordinator(
                directory,
                archive_owner_verifier=verifier,
            )
            with self.assertRaises(PromptArchiveStorageError) as caught:
                coordinator.publish_prepared_descriptor(
                    second_payload,
                    prepared_intent=second_intent,
                    transition_stub_path=transition,
                    **owner_args,
                )
            self.assertEqual(caught.exception.code, "PROMPT_ARCHIVE_UNAVAILABLE")
            self.assertFalse(transition.exists())
            self.assertFalse((directory / "latest-response.json").exists())

    def test_descriptor_recovery_requires_exact_archive_owner_verifier(self) -> None:
        verifier_cases = {
            "absent": None,
            "false": lambda _owner, _intent: False,
            "error": lambda _owner, _intent: (_ for _ in ()).throw(
                RuntimeError("no proof")
            ),
        }
        for label, rejected_verifier in verifier_cases.items():
            with self.subTest(verifier=label), tempfile.TemporaryDirectory() as raw:
                directory = Path(raw)
                target = directory / "latest-response.json"
                target.write_bytes(b'{"old":true}')
                transition = directory / "transition.json"
                payload = b'{"descriptor":"owner-proof"}'
                planner = self.coordinator(directory)
                intent = planner.prepare_descriptor_intent(
                    payload,
                    transition_stub_path=transition,
                    descriptor_ref=descriptor_ref(payload),
                    source_sprint_id="sprint-owner-proof",
                    source_revision=8,
                    **self.publish_args(f"owner-proof-{label}"),
                )
                owner, owner_args = self.owner_publish_args(
                    planner,
                    intent,
                    f"owner-proof-{label}",
                    transition,
                )

                def stop_after_intent(stage: str) -> None:
                    if stage == "after_current_intent_publish":
                        raise InjectedLatestFault(stage)

                with self.assertRaises(InjectedLatestFault):
                    self.coordinator(
                        directory,
                        fault_hook=stop_after_intent,
                        archive_owner_verifier=self.owner_verifier(owner),
                    ).publish_prepared_descriptor(
                        payload,
                        prepared_intent=intent,
                        transition_stub_path=transition,
                        **owner_args,
                    )
                rejected = self.coordinator(
                    directory,
                    archive_owner_verifier=rejected_verifier,
                )
                with self.assertRaises(PromptArchiveStorageError) as caught:
                    rejected.recover()
                self.assertEqual(caught.exception.code, "PROMPT_ARCHIVE_UNAVAILABLE")
                with self.assertRaises(PromptArchiveStorageError) as caught:
                    rejected.publish(
                        b'{"legacy":"later"}',
                        **self.publish_args(f"legacy-after-{label}"),
                    )
                self.assertEqual(caught.exception.code, "PROMPT_ARCHIVE_UNAVAILABLE")
                self.assertFalse(transition.exists())
                self.assertEqual(target.read_bytes(), b'{"old":true}')

    def test_verify_prepared_publication_rejects_forged_receipts_and_artifacts(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            transition = directory / "transition-verified.json"
            payload = b'{"descriptor":"verified"}'
            planner = self.coordinator(directory)
            intent = planner.prepare_descriptor_intent(
                payload,
                transition_stub_path=transition,
                descriptor_ref=descriptor_ref(payload),
                source_sprint_id="sprint-verified",
                source_revision=12,
                **self.publish_args("verified"),
            )
            owner, owner_args = self.owner_publish_args(
                planner,
                intent,
                "verified-owner",
                transition,
            )
            verifier_calls = []

            def verifier(observed_owner, observed_intent):
                verifier_calls.append((observed_owner, observed_intent))
                return observed_owner == owner and observed_intent == intent

            coordinator = self.coordinator(
                directory,
                archive_owner_verifier=verifier,
            )
            result = coordinator.publish_prepared_descriptor(
                payload,
                prepared_intent=intent,
                transition_stub_path=transition,
                **owner_args,
            )
            receipt = dataclasses.asdict(result)
            verified = coordinator.verify_prepared_publication(
                prepared_intent=intent,
                publication=receipt,
                archive_owner=owner,
            )
            self.assertEqual(verified.outcome, "published")
            self.assertEqual(verified.latest_slot_id, result.latest_slot_id)
            self.assertTrue(verifier_calls)
            self.assertEqual(verifier_calls[-1], (owner, intent))

            forged_values = {
                "intent_id": "parlatestintent1-" + "0" * 64,
                "latest_slot_id": "parlatest1-" + "0" * 64,
                "source_revision": 13,
                "generation": result.generation + 1,
                "content_sha256": "0" * 64,
                "content_size": result.content_size + 1,
                "outcome": "superseded",
            }
            for field, forged_value in forged_values.items():
                with self.subTest(field=field):
                    forged = json.loads(json.dumps(receipt))
                    forged[field] = forged_value
                    with self.assertRaises(PromptArchiveStorageError) as caught:
                        coordinator.verify_prepared_publication(
                            prepared_intent=intent,
                            publication=forged,
                            archive_owner=owner,
                        )
                    self.assertEqual(
                        caught.exception.code,
                        "PROMPT_ARCHIVE_INTEGRITY_MISMATCH",
                    )

            forged_ref = json.loads(json.dumps(receipt))
            forged_ref["latest_slot_ref"]["digest"] = "0" * 64
            with self.assertRaises(PromptArchiveStorageError) as caught:
                coordinator.verify_prepared_publication(
                    prepared_intent=intent,
                    publication=forged_ref,
                    archive_owner=owner,
                )
            self.assertEqual(caught.exception.code, "PROMPT_ARCHIVE_INTEGRITY_MISMATCH")

            transition.unlink()
            with self.assertRaises(PromptArchiveStorageError) as caught:
                coordinator.verify_prepared_publication(
                    prepared_intent=intent,
                    publication=receipt,
                    archive_owner=owner,
                )
            self.assertEqual(caught.exception.code, "PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
            transition.write_bytes(payload)

            latest = directory / "latest-response.json"
            latest.write_bytes(b'{"forged":true}')
            with self.assertRaises(PromptArchiveStorageError) as caught:
                coordinator.verify_prepared_publication(
                    prepared_intent=intent,
                    publication=receipt,
                    archive_owner=owner,
                )
            self.assertEqual(caught.exception.code, "PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
            latest.write_bytes(payload)

            operation_root = (
                coordinator.sidecar
                / "operations"
                / "v1"
                / intent["source_response_id"]
                / intent["ordering"]["operation_id"]
            )
            (operation_root / "journal.json").unlink()
            with self.assertRaises(PromptArchiveStorageError) as caught:
                coordinator.verify_prepared_publication(
                    prepared_intent=intent,
                    publication=receipt,
                    archive_owner=owner,
                )
            self.assertEqual(caught.exception.code, "PROMPT_ARCHIVE_INTEGRITY_MISMATCH")

    def test_published_journal_rejects_slot_reusing_intent_id_with_other_fields(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            transition = directory / "transition-slot-binding.json"
            payload = b'{"descriptor":"slot-binding"}'
            planner = self.coordinator(directory)
            intent = planner.prepare_descriptor_intent(
                payload,
                transition_stub_path=transition,
                descriptor_ref=descriptor_ref(payload),
                source_sprint_id="sprint-slot-binding",
                source_revision=15,
                **self.publish_args("slot-binding"),
            )
            owner, owner_args = self.owner_publish_args(
                planner,
                intent,
                "slot-binding-owner",
                transition,
            )
            coordinator = self.coordinator(
                directory,
                archive_owner_verifier=self.owner_verifier(owner),
            )
            result = coordinator.publish_prepared_descriptor(
                payload,
                prepared_intent=intent,
                transition_stub_path=transition,
                **owner_args,
            )
            sidecar = coordinator.sidecar
            slot = json.loads((sidecar / "slot.json").read_bytes())
            slot["source_sprint_id"] = "sprint-forged"
            slot["latest_slot_id"] = coordinator._slot_id(slot)
            slot_raw = canonical_json_bytes_v1(slot, maximum=64 * 1024)
            forged_slot_path = (
                sidecar
                / "slots"
                / "v1"
                / f"{slot['generation']}-{slot['latest_slot_id']}.json"
            )
            forged_slot_path.write_bytes(slot_raw)

            operation_root = (
                sidecar
                / "operations"
                / "v1"
                / intent["source_response_id"]
                / intent["ordering"]["operation_id"]
            )
            journal = json.loads((operation_root / "journal.json").read_bytes())
            journal["result_latest_slot_id"] = slot["latest_slot_id"]
            journal_raw = canonical_json_bytes_v1(journal, maximum=64 * 1024)
            journal_digest = hashlib.sha256(journal_raw).hexdigest()
            (operation_root / "journal-generations" / f"1-{journal_digest}.json").write_bytes(
                journal_raw
            )
            (operation_root / "journal.json").write_bytes(journal_raw)

            with self.assertRaises(PromptArchiveStorageError) as caught:
                coordinator.verify_prepared_publication(
                    prepared_intent=intent,
                    publication=dataclasses.asdict(result),
                    archive_owner=owner,
                )
            self.assertEqual(caught.exception.code, "PROMPT_ARCHIVE_INTEGRITY_MISMATCH")

    def test_prepared_descriptor_recovers_every_publication_boundary(self) -> None:
        for stage in FAULT_STAGES:
            with self.subTest(stage=stage), tempfile.TemporaryDirectory() as raw:
                directory = Path(raw)
                target = directory / "latest-response.json"
                target.write_bytes(b'{"old":true}')
                transition = directory / f"transition-{stage}.json"
                payload = b'{"descriptor":true}'
                planner = self.coordinator(directory)
                intent = planner.prepare_descriptor_intent(
                    payload,
                    transition_stub_path=transition,
                    descriptor_ref=descriptor_ref(payload),
                    source_sprint_id="sprint-faults",
                    source_revision=21,
                    **self.publish_args(f"descriptor-{stage}"),
                )
                owner, owner_args = self.owner_publish_args(
                    planner,
                    intent,
                    f"fault-owner-{stage}",
                    transition,
                )
                fired = False

                def inject(observed: str) -> None:
                    nonlocal fired
                    if observed == stage and not fired:
                        fired = True
                        raise InjectedLatestFault(stage)

                with self.assertRaises(InjectedLatestFault):
                    self.coordinator(
                        directory,
                        fault_hook=inject,
                        archive_owner_verifier=self.owner_verifier(owner),
                    ).publish_prepared_descriptor(
                        payload,
                        prepared_intent=intent,
                        transition_stub_path=transition,
                        **owner_args,
                    )
                self.assertTrue(fired)

                restarted = self.coordinator(
                    directory,
                    archive_owner_verifier=self.owner_verifier(owner),
                )
                recovered = restarted.recover()
                if recovered is None:
                    recovered = restarted.publish_prepared_descriptor(
                        payload,
                        prepared_intent=intent,
                        transition_stub_path=transition,
                        **owner_args,
                    )
                self.assertIn(recovered.outcome, {"published", "idempotent"})
                self.assertEqual(target.read_bytes(), payload)
                self.assertEqual(transition.read_bytes(), payload)
                self.assertEqual(restarted.recover().outcome, "idempotent")

    def test_transition_boundary_is_after_durable_intent_and_before_latest(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            target = directory / "latest-response.json"
            target.write_bytes(b'{"old":true}')
            transition = directory / "transition-ordered.json"
            payload = b'{"descriptor":"ordered"}'
            planner = self.coordinator(directory)
            intent = planner.prepare_descriptor_intent(
                payload,
                transition_stub_path=transition,
                descriptor_ref=descriptor_ref(payload),
                source_sprint_id="sprint-ordered",
                source_revision=31,
                **self.publish_args("ordered"),
            )
            owner, owner_args = self.owner_publish_args(
                planner,
                intent,
                "ordered-owner",
                transition,
            )

            def stop_after_transition(stage: str) -> None:
                if stage == "after_transition_stub_publish":
                    raise InjectedLatestFault(stage)

            with self.assertRaises(InjectedLatestFault):
                self.coordinator(
                    directory,
                    fault_hook=stop_after_transition,
                    archive_owner_verifier=self.owner_verifier(owner),
                ).publish_prepared_descriptor(
                    payload,
                    prepared_intent=intent,
                    transition_stub_path=transition,
                    **owner_args,
                )
            sidecar = directory / "latest-response.json.nginx-qa-v1"
            self.assertTrue((sidecar / "intent.json").is_file())
            self.assertEqual(transition.read_bytes(), payload)
            self.assertEqual(target.read_bytes(), b'{"old":true}')
            self.assertFalse((sidecar / "slot.json").exists())

            recovered = self.coordinator(
                directory,
                archive_owner_verifier=self.owner_verifier(owner),
            ).recover()
            self.assertEqual(recovered.outcome, "published")
            self.assertEqual(target.read_bytes(), payload)

    def test_prepared_descriptor_superseded_still_publishes_transition_stub(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            coordinator = self.coordinator(directory)
            payload = b'{"descriptor":"older"}'
            transition = directory / "transition-older.json"
            intent = coordinator.prepare_descriptor_intent(
                payload,
                transition_stub_path=transition,
                descriptor_ref=descriptor_ref(payload),
                source_sprint_id="sprint-older",
                source_revision=3,
                **self.publish_args("older"),
            )
            owner, owner_args = self.owner_publish_args(
                coordinator,
                intent,
                "older-owner",
                transition,
            )
            coordinator = self.coordinator(
                directory,
                archive_owner_verifier=self.owner_verifier(owner),
            )
            winner = coordinator.publish(
                b'{"winner":true}',
                **self.publish_args("winner", source_revision=4),
            )
            result = coordinator.publish_prepared_descriptor(
                payload,
                prepared_intent=intent,
                transition_stub_path=transition,
                **owner_args,
            )
            self.assertEqual(result.outcome, "superseded")
            self.assertEqual(result.latest_slot_id, winner.latest_slot_id)
            self.assertEqual(transition.read_bytes(), payload)
            self.assertEqual(
                (directory / "latest-response.json").read_bytes(),
                b'{"winner":true}',
            )

    def test_prepared_descriptor_rejects_target_escape_and_plan_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as raw, tempfile.TemporaryDirectory() as other:
            directory = Path(raw)
            coordinator = self.coordinator(directory)
            payload = b'{"descriptor":true}'
            base = {
                **self.publish_args("negative"),
                "descriptor_ref": descriptor_ref(payload),
                "source_sprint_id": "sprint-negative",
                "source_revision": 1,
            }
            for unsafe in (
                directory / "latest-response.json",
                Path(other) / "transition.json",
                Path("relative-transition.json"),
            ):
                with self.subTest(unsafe=unsafe), self.assertRaises(
                    PromptArchiveStorageError
                ) as caught:
                    coordinator.prepare_descriptor_intent(
                        payload,
                        transition_stub_path=unsafe,
                        **base,
                    )
                self.assertIn(
                    caught.exception.code,
                    {"PROMPT_ARCHIVE_REQUEST_INVALID", "PROMPT_ARCHIVE_REFERENCE_INVALID"},
                )

            plan = coordinator.prepare_descriptor_intent(
                payload,
                transition_stub_path=directory / "transition.json",
                **base,
            )
            owner, owner_args = self.owner_publish_args(
                coordinator,
                plan,
                "negative-owner",
                directory / "transition.json",
            )
            coordinator = self.coordinator(
                directory,
                archive_owner_verifier=self.owner_verifier(owner),
            )
            tampered = json.loads(json.dumps(plan))
            tampered["ordering"]["source_revision"] = 2
            with self.assertRaises(PromptArchiveStorageError) as caught:
                coordinator.publish_prepared_descriptor(
                    payload,
                    prepared_intent=tampered,
                    transition_stub_path=directory / "different-transition.json",
                    **owner_args,
                )
            self.assertEqual(caught.exception.code, "PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
            self.assertFalse((directory / "different-transition.json").exists())
            self.assertEqual(
                (directory / "latest-response.json").exists(),
                False,
            )

            with self.assertRaises(PromptArchiveStorageError) as caught:
                coordinator.publish_prepared_descriptor(
                    payload,
                    prepared_intent=plan,
                    transition_stub_path=directory / "different-transition.json",
                    **owner_args,
                )
            self.assertEqual(caught.exception.code, "PROMPT_ARCHIVE_REFERENCE_INVALID")
            self.assertFalse((directory / "different-transition.json").exists())
            self.assertFalse((coordinator.sidecar / "intent.json").exists())

    def test_unrelated_external_bytes_block_recovery(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            target = directory / "latest-response.json"
            target.write_bytes(b'{"old":true}')

            def stop_after_intent(stage: str) -> None:
                if stage == "after_current_intent_publish":
                    raise InjectedLatestFault

            with self.assertRaises(InjectedLatestFault):
                self.coordinator(directory, fault_hook=stop_after_intent).publish(
                    b'{"new":true}', **self.publish_args("unrelated")
                )
            target.write_bytes(b'{"intruder":"SECRET_CANARY"}')
            with self.assertRaises(PromptArchiveStorageError) as caught:
                self.coordinator(directory).recover()
            self.assertEqual(caught.exception.code, "PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
            self.assertNotIn("SECRET_CANARY", str(caught.exception))
            self.assertEqual(target.read_bytes(), b'{"intruder":"SECRET_CANARY"}')

    def test_missing_intended_cas_blocks_later_writer(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)

            def stop_after_intent(stage: str) -> None:
                if stage == "after_current_intent_publish":
                    raise InjectedLatestFault

            with self.assertRaises(InjectedLatestFault):
                self.coordinator(directory, fault_hook=stop_after_intent).publish(
                    b'{"first":true}', **self.publish_args("missing-cas")
                )
            sidecar = directory / "latest-response.json.nginx-qa-v1"
            plan = json.loads((sidecar / "intent.json").read_bytes())
            digest = plan["latest_intent"]["content_sha256"]
            (sidecar / "content" / "v1" / "sha256" / digest[:2] / f"{digest}.json").unlink()

            with self.assertRaises(PromptArchiveStorageError) as caught:
                self.coordinator(directory).publish(
                    b'{"second":true}', **self.publish_args("later")
                )
            self.assertEqual(caught.exception.code, "PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
            self.assertFalse((directory / "latest-response.json").exists())

    def test_missing_current_intent_with_slot_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            coordinator = self.coordinator(directory)
            coordinator.publish(b'{"ok":true}', **self.publish_args("missing-intent"))
            (coordinator.sidecar / "intent.json").unlink()

            with self.assertRaises(PromptArchiveStorageError) as caught:
                self.coordinator(directory).recover()
            self.assertEqual(caught.exception.code, "PROMPT_ARCHIVE_INTEGRITY_MISMATCH")

    def test_descriptor_and_legacy_kinds_are_semantically_bound(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            payload = b'{"kind":"prompt-archive-descriptor-stub-v1"}'
            planner = self.coordinator(directory)
            intent = planner.prepare_descriptor_intent(
                payload,
                transition_stub_path=directory / "transition-descriptor.json",
                **self.publish_args("descriptor"),
                source_sprint_id="sprint-1",
                source_revision=1,
                descriptor_ref=descriptor_ref(payload),
            )
            owner, owner_args = self.owner_publish_args(
                planner,
                intent,
                "descriptor-owner",
                directory / "transition-descriptor.json",
            )
            result = self.coordinator(
                directory,
                archive_owner_verifier=self.owner_verifier(owner),
            ).publish_prepared_descriptor(
                payload,
                prepared_intent=intent,
                transition_stub_path=directory / "transition-descriptor.json",
                **owner_args,
            )
            self.assertEqual(result.outcome, "published")
            slot = json.loads(
                (
                    directory
                    / "latest-response.json.nginx-qa-v1"
                    / "slot.json"
                ).read_bytes()
            )
            self.assertEqual(slot["content_kind"], "descriptor_stub")
            self.assertEqual(slot["descriptor_ref"]["digest"], result.content_sha256)

        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            bad_ref = descriptor_ref(b'{"different":true}')
            with self.assertRaises(PromptArchiveStorageError) as caught:
                self.coordinator(directory).prepare_descriptor_intent(
                    b'{"descriptor":true}',
                    transition_stub_path=directory / "transition-bad.json",
                    **self.publish_args("bad-descriptor"),
                    source_sprint_id="sprint-1",
                    source_revision=1,
                    descriptor_ref=bad_ref,
                )
            self.assertEqual(caught.exception.code, "PROMPT_ARCHIVE_REFERENCE_INVALID")
            self.assertFalse((directory / "latest-response.json").exists())

    def test_target_hardlink_and_redirect_ambiguity_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            target = directory / "latest-response.json"
            target.write_bytes(b'{"secret":"HARDLINK_CANARY"}')
            os.link(target, directory / "alias.json")
            with self.assertRaises(PromptArchiveStorageError) as caught:
                self.coordinator(directory)
            self.assertEqual(caught.exception.code, "PROMPT_ARCHIVE_REFERENCE_INVALID")
            self.assertNotIn("HARDLINK_CANARY", str(caught.exception))

        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            target = directory / "latest-response.json"
            target.write_bytes(b'{"secret":"REDIRECT_CANARY"}')
            with mock.patch(
                "nginx_qa.prompt_archive_latest._path_entry_is_redirected",
                side_effect=lambda path: Path(path) == target,
            ), self.assertRaises(PromptArchiveStorageError) as caught:
                self.coordinator(directory)
            self.assertEqual(caught.exception.code, "PROMPT_ARCHIVE_REFERENCE_INVALID")
            self.assertNotIn("REDIRECT_CANARY", str(caught.exception))

    def test_hardlinked_current_control_file_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            coordinator = self.coordinator(directory)
            coordinator.publish(b'{"ok":true}', **self.publish_args("control-link"))
            intent = coordinator.sidecar / "intent.json"
            os.link(intent, coordinator.sidecar / "intent-alias.json")
            with self.assertRaises(PromptArchiveStorageError) as caught:
                self.coordinator(directory).recover()
            self.assertEqual(caught.exception.code, "PROMPT_ARCHIVE_REFERENCE_INVALID")

    @unittest.skipUnless(os.name == "nt", "Windows case aliases are filesystem aliases")
    def test_case_only_target_aliases_share_one_actual_sidecar(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            actual = directory / "latest-response.json"
            actual.write_bytes(b'{"old":true}')
            lower = ExternalLatestCoordinator(actual, project_id=PROJECT_ID)
            upper = ExternalLatestCoordinator(
                directory / "LATEST-RESPONSE.JSON", project_id=PROJECT_ID
            )
            self.assertEqual(lower.external_latest, upper.external_latest)
            self.assertEqual(lower.sidecar, upper.sidecar)

    def test_absent_non_normative_target_name_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            with self.assertRaises(PromptArchiveStorageError) as caught:
                ExternalLatestCoordinator(
                    Path(raw) / "another.json", project_id=PROJECT_ID
                )
            self.assertEqual(caught.exception.code, "PROMPT_ARCHIVE_REFERENCE_INVALID")

    def test_transition_stub_is_immutable_create_or_verify(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "transition-response.json"
            first = publish_authoritative_transition_stub(path, b'{"stub":1}')
            second = publish_authoritative_transition_stub(path, b'{"stub":1}')
            self.assertTrue(first.created)
            self.assertFalse(second.created)
            self.assertEqual(first.sha256, second.sha256)
            with self.assertRaises(PromptArchiveStorageError) as caught:
                publish_authoritative_transition_stub(path, b'{"stub":2}')
            self.assertEqual(caught.exception.code, "PROMPT_ARCHIVE_GENERATION_CONFLICT")
            self.assertEqual(path.read_bytes(), b'{"stub":1}')

    def test_transition_stub_rejects_hardlink_and_redirect_aliases(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            path = directory / "transition-response.json"
            path.write_bytes(b'{"stub":1}')
            os.link(path, directory / "stub-alias.json")
            with self.assertRaises(PromptArchiveStorageError) as caught:
                publish_authoritative_transition_stub(path, b'{"stub":1}')
            self.assertEqual(caught.exception.code, "PROMPT_ARCHIVE_REFERENCE_INVALID")

        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            alias = directory / "transition-response.json"
            alias.write_bytes(b'{"stub":1}')
            with mock.patch(
                "nginx_qa.prompt_archive_latest._path_entry_is_redirected",
                side_effect=lambda path: Path(path) == alias,
            ), self.assertRaises(PromptArchiveStorageError) as caught:
                publish_authoritative_transition_stub(alias, b'{"stub":1}')
            self.assertEqual(caught.exception.code, "PROMPT_ARCHIVE_REFERENCE_INVALID")

    def test_two_processes_converge_on_greater_ordered_intent(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            target = Path(raw) / "latest-response.json"
            context = multiprocessing.get_context("spawn")
            queue = context.Queue()
            processes = [
                context.Process(
                    target=process_publish_latest,
                    args=(str(target), revision, label, payload, queue),
                )
                for revision, label, payload in (
                    (1, "process-one", b'{"winner":1}'),
                    (2, "process-two", b'{"winner":2}'),
                )
            ]
            for process in processes:
                process.start()
            for process in processes:
                process.join(30)
                self.assertEqual(process.exitcode, 0)
            outcomes = [queue.get(timeout=5), queue.get(timeout=5)]
            self.assertTrue(all(item[0] == "ok" for item in outcomes), outcomes)
            self.assertEqual(target.read_bytes(), b'{"winner":2}')
            final = ExternalLatestCoordinator(
                target, project_id=PROJECT_ID
            ).recover()
            self.assertEqual(final.source_revision, 2)
            # Revision two may acquire the lock first (generation one), or it
            # may advance revision one's slot (generation two).
            self.assertIn(final.generation, {1, 2})

    def test_process_restart_recovers_registered_legacy_v2_after_v1(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw)
            target = directory / "latest-response.json"
            first = self.coordinator(directory).publish(
                b'{"version":1}',
                **self.publish_args("restart-v1", source_revision=1),
            )
            self.assertEqual(first.source_revision, 1)

            context = multiprocessing.get_context("spawn")
            queue = context.Queue()
            child = context.Process(
                target=process_fault_publish_latest,
                args=(
                    str(target),
                    FAULT_AFTER_PENDING_BIND,
                    2,
                    "restart-v2",
                    b'{"version":2}',
                    queue,
                ),
            )
            child.start()
            child.join(30)
            self.assertEqual(child.exitcode, 0)
            self.assertEqual(
                queue.get(timeout=5),
                ("fault", FAULT_AFTER_PENDING_BIND, True),
            )
            self.assertEqual(target.read_bytes(), b'{"version":1}')

            restarted = self.coordinator(directory)
            recovered = restarted.recover()
            self.assertEqual(recovered.source_revision, 2)
            self.assertEqual(target.read_bytes(), b'{"version":2}')
            sidecar = restarted.sidecar
            for label in ("restart-v1", "restart-v2"):
                self.assertTrue(
                    (
                        sidecar
                        / "operations"
                        / "v1"
                        / response_id(label)
                        / operation_id(label)
                        / "journal.json"
                    ).is_file()
                )
            v2_plan = json.loads(
                (
                    sidecar
                    / "operations"
                    / "v1"
                    / response_id("restart-v2")
                    / operation_id("restart-v2")
                    / "plan.json"
                ).read_bytes()
            )
            shard_id = restarted._pending_shard_id(v2_plan["recovery_key"])
            registry = json.loads(
                (
                    sidecar / "pending" / "v1" / "shards" / f"{shard_id}.json"
                ).read_bytes()
            )
            self.assertEqual(registry["entries"], [])


if __name__ == "__main__":
    unittest.main()
