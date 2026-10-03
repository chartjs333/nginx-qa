from datetime import datetime, timedelta, timezone
import json
import unittest
from unittest.mock import Mock, patch

from nginx_qa.managed_continuity import (
    ManagedContinuityError,
    ManagedContinuityRuntime,
)
from nginx_qa.managed_import import (
    ManagedImportError,
    ManagedRetryImportConflict,
)
from nginx_qa.sprint_types import (
    StartSprintFromGitRequest,
    canonical_json_bytes,
    managed_activation_invariant_issues,
)
from tests.test_managed_import import ManagedImportFixture, SimulatedCrash


FIXED_CLOCK = datetime(2026, 10, 3, 10, 0, tzinfo=timezone.utc)


class ManagedContinuityImportRecoveryTests(ManagedImportFixture):
    def _failed_import(self):
        def fail_after_preflight(point: str, _context: dict) -> None:
            if point == "after_validate":
                raise RuntimeError("injected preparation failure")

        importer = self.importer(
            clock=lambda: FIXED_CLOCK,
            fault_injector=fail_after_preflight,
        )
        with self.assertRaises(ManagedImportError):
            importer.start(self.project_id, self.request)
        parent = importer.store.lookup(
            self.project_id,
            self.request.idempotency_key,
        )
        self.assertIsNotNone(parent)
        assert parent is not None
        state = importer.store.runtime_state(
            self.project_id,
            str(parent["sprint_id"]),
        )
        self.assertIsNotNone(state)
        assert state is not None
        self.assertEqual(state["status"], "failed")
        self.assertIsNone(state["workflow"])
        self.assertEqual(managed_activation_invariant_issues(state), ())
        return parent, state, state["coordinator_contexts"][0]

    @staticmethod
    def _retry_request(
        parent: dict,
        context: dict,
        *,
        recovery_key: str = "retry-import-generation-1",
        coordinator_key: str = "coordinator-retry-import",
    ) -> bytes:
        return canonical_json_bytes(
            {
                "assignment_id": context["context_id"],
                "idempotency_key": coordinator_key,
                "action": "RETRY_IMPORT",
                "parameters": {
                    "failed_attempt_id": parent["attempt_id"],
                    "recovery_idempotency_key": recovery_key,
                },
            }
        )

    @staticmethod
    def _block_request(context: dict, *, suffix: str = "") -> bytes:
        return canonical_json_bytes(
            {
                "assignment_id": context["context_id"],
                "idempotency_key": f"coordinator-block-import{suffix}",
                "action": "BLOCK_EXTERNAL",
                "parameters": {
                    "reason_code": context["reason_code"],
                    "operator_action": "Resolve the external import dependency",
                },
            }
        )

    def test_retry_import_activates_from_pinned_provenance_and_replays(self) -> None:
        parent, failed, context = self._failed_import()
        importer = self.importer(
            clock=lambda: FIXED_CLOCK,
            attempt_id_factory=lambda: "attempt-retry-generation-1",
        )
        importer.git_provider.resolve_and_pin_commit = Mock(
            side_effect=AssertionError("retry resolved a mutable ref")
        )
        runtime = ManagedContinuityRuntime(importer, clock=lambda: FIXED_CLOCK)
        recovery_key = "retry-import-pinned-provenance"
        body = self._retry_request(
            parent,
            context,
            recovery_key=recovery_key,
        )

        with patch.object(
            importer,
            "resume_import_attempt",
            side_effect=AssertionError("HTTP recovery synchronously resumed import"),
        ):
            accepted = runtime.submit_if_managed(
                self.project_id,
                "2860",
                body,
                "retry-import",
            )
        self.assertIsNotNone(accepted)
        assert accepted is not None
        child_id = accepted.response["produced_record_ids"][0]
        self.assertEqual(accepted.response["action"], "RETRY_IMPORT")
        self.assertFalse(accepted.response["deduplicated"])
        self.assertEqual(child_id, "attempt-retry-generation-1")
        child = importer.store.lookup_attempt(self.project_id, child_id)
        self.assertIsNotNone(child)
        assert child is not None
        self.assertEqual(child["idempotency_key"], recovery_key)

        staged = importer.store.runtime_state(
            self.project_id,
            str(failed["sprint_id"]),
        )
        assert staged is not None
        self.assertEqual(staged["status"], "failed")
        self.assertEqual(
            [item["status"] for item in staged["import_attempts"]],
            ["failed", "running"],
        )
        progress = runtime.reconcile_all()
        self.assertGreater(progress[str(failed["sprint_id"])], 0)

        activated = importer.store.runtime_state(
            self.project_id,
            str(failed["sprint_id"]),
        )
        self.assertIsNotNone(activated)
        assert activated is not None
        self.assertEqual(activated["status"], "active")
        self.assertEqual(
            [item["status"] for item in activated["import_attempts"]],
            ["failed", "succeeded"],
        )
        self.assertEqual(managed_activation_invariant_issues(activated), ())
        importer.git_provider.resolve_and_pin_commit.assert_not_called()

        replay = runtime.submit_if_managed(
            self.project_id,
            "2860",
            body,
            "retry-import-replay",
        )
        self.assertIsNotNone(replay)
        assert replay is not None
        self.assertTrue(replay.response["deduplicated"])

        with self.assertRaises(ManagedContinuityError) as decided:
            runtime.submit_if_managed(
                self.project_id,
                "2860",
                self._block_request(context, suffix="-second-decision"),
                "block-import-second-decision",
            )
        self.assertEqual(decided.exception.code, "RECOVERY_CONFLICT")
        decided_state = importer.store.runtime_state(
            self.project_id,
            str(failed["sprint_id"]),
        )
        assert decided_state is not None
        self.assertEqual(len(decided_state["recovery_records"]), 1)
        self.assertEqual(replay.response["produced_record_ids"], [child_id])
        replayed = importer.store.runtime_state(
            self.project_id,
            str(failed["sprint_id"]),
        )
        assert replayed is not None
        self.assertEqual(len(replayed["import_attempts"]), 2)

    def test_new_and_persisted_supersession_terminalize_retry_runtime(self) -> None:
        parent, failed, context = self._failed_import()
        now = [FIXED_CLOCK]
        retry_importer = self.importer(
            clock=lambda: now[0],
            attempt_id_factory=lambda: "attempt-retry-superseded",
        )
        runtime = ManagedContinuityRuntime(
            retry_importer,
            clock=lambda: now[0],
        )
        accepted = runtime.submit_if_managed(
            self.project_id,
            "2860",
            self._retry_request(parent, context),
            "retry-import-before-new-winner",
        )
        self.assertIsNotNone(accepted)
        assert accepted is not None
        retry_child_id = accepted.response["produced_record_ids"][0]

        now[0] += timedelta(minutes=10)
        changed = json.loads(
            (self.source / self.manifest_path).read_text(encoding="utf-8")
        )
        changed["nodes"][0]["workspace"] = {"access": "read"}
        changed["nodes"][0]["tasks"][0]["message"] = "new activation wins"
        (self.source / self.manifest_path).write_text(
            json.dumps(changed, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        self.git("add", self.manifest_path, cwd=self.source)
        self.git("commit", "-m", "activate after expired retry", cwd=self.source)
        self.git("push", str(self.remote), "main", cwd=self.source)
        winner_request = StartSprintFromGitRequest(
            repository_id=self.request.repository_id,
            ref=self.request.ref,
            manifest_path=self.request.manifest_path,
            idempotency_key="winner-after-expired-retry",
        )
        winner = self.importer(
            clock=lambda: now[0],
            attempt_id_factory=lambda: "attempt-new-winner",
        )
        winner_result = winner.start(self.project_id, winner_request)
        self.assertEqual(winner_result.http_status, 201)

        retry_child = winner.store.lookup_attempt(
            self.project_id, retry_child_id
        )
        assert retry_child is not None
        self.assertEqual(retry_child["status"], "FAILED")
        self.assertEqual(
            retry_child["evidence"]["failure_code"], "ATTEMPT_SUPERSEDED"
        )
        superseded = winner.store.runtime_state(
            self.project_id,
            str(failed["sprint_id"]),
        )
        assert superseded is not None
        self.assertEqual(superseded["status"], "failed")
        self.assertEqual(
            [item["status"] for item in superseded["import_attempts"]],
            ["failed", "failed"],
        )
        self.assertEqual(
            superseded["coordinator_contexts"][-1]["normalized_error"][
                "issue_codes"
            ],
            ["ATTEMPT_SUPERSEDED"],
        )
        self.assertEqual(managed_activation_invariant_issues(superseded), ())
        self.assertNotIn(
            (self.project_id, str(failed["sprint_id"]), retry_child_id),
            winner.store.retryable_import_attempts(),
        )

        supersession_context = next(
            item
            for item in superseded["coordinator_contexts"]
            if item["import_attempt_id"] == retry_child_id
        )
        supersession_event = next(
            item
            for item in superseded["outbox"]
            if item["payload"].get("context_id")
            == supersession_context["context_id"]
        )
        legacy_projection = json.loads(json.dumps(superseded))
        legacy_child = next(
            item
            for item in legacy_projection["import_attempts"]
            if item["attempt_id"] == retry_child_id
        )
        legacy_child.update(
            {
                "phase": "VALIDATE",
                "status": "running",
                "updated_at": FIXED_CLOCK.isoformat(),
            }
        )
        legacy_projection["coordinator_contexts"] = [
            item
            for item in legacy_projection["coordinator_contexts"]
            if item["context_id"] != supersession_context["context_id"]
        ]
        legacy_projection["outbox"] = [
            item
            for item in legacy_projection["outbox"]
            if item["event_id"] != supersession_event["event_id"]
        ]
        with winner.store._transaction() as connection:
            connection.execute(
                """
                UPDATE managed_sprints SET state_json = ?
                WHERE project_id = ? AND sprint_id = ?
                """,
                (
                    json.dumps(
                        legacy_projection,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    self.project_id,
                    str(failed["sprint_id"]),
                ),
            )
            connection.execute(
                "DELETE FROM managed_queue_items WHERE dedupe_key = ?",
                (supersession_event["dedupe_key"],),
            )

        restarted = self.importer(clock=lambda: now[0])
        self.assertEqual(restarted.store.retryable_import_attempts(), ())
        migrated = restarted.store.runtime_state(
            self.project_id,
            str(failed["sprint_id"]),
        )
        assert migrated is not None
        self.assertEqual(
            [item["status"] for item in migrated["import_attempts"]],
            ["failed", "failed"],
        )
        restored_context = next(
            item
            for item in migrated["coordinator_contexts"]
            if item["import_attempt_id"] == retry_child_id
        )
        restored_event = next(
            item
            for item in migrated["outbox"]
            if item["payload"].get("context_id")
            == restored_context["context_id"]
        )
        self.assertIsNotNone(
            restarted.store.managed_queue_item(restored_event["dedupe_key"])
        )
        self.assertEqual(managed_activation_invariant_issues(migrated), ())

        durable_child_fence = retry_child["fencing_token"]
        with restarted.store._transaction() as connection:
            control, revision = restarted.store._load_control(
                connection, self.project_id
            )
            stale_child = restarted.store._record_by_attempt(
                control, retry_child_id
            )
            assert stale_child is not None
            stale_child.pop("created_fencing_token", None)
            stale_child["fencing_token"] = (
                int(control["activation_fencing_counter"]) + 1
            )
            control["activation_fencing_counter"] = stale_child[
                "fencing_token"
            ]
            connection.execute(
                """
                UPDATE managed_projects SET control_json = ?, revision = ?
                WHERE project_id = ?
                """,
                (
                    json.dumps(
                        control,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ),
                    revision + 1,
                    self.project_id,
                ),
            )
        repaired_control = restarted.store.project_control(self.project_id)
        repaired_child = restarted.store._record_by_attempt(
            repaired_control, retry_child_id
        )
        assert repaired_child is not None
        self.assertEqual(repaired_child["fencing_token"], durable_child_fence)
        self.assertEqual(
            repaired_child["created_fencing_token"], durable_child_fence
        )

    def test_retry_import_replay_closes_crash_after_atomic_child_creation(self) -> None:
        parent, failed, context = self._failed_import()
        importer = self.importer(
            clock=lambda: FIXED_CLOCK,
            attempt_id_factory=lambda: "attempt-retry-after-crash",
        )

        def crash_after_recovery(point: str, _context: dict) -> None:
            if point == "after_recovery_commit":
                raise SimulatedCrash()

        runtime = ManagedContinuityRuntime(
            importer,
            clock=lambda: FIXED_CLOCK,
            fault_injector=crash_after_recovery,
        )
        body = self._retry_request(parent, context)
        with self.assertRaises(SimulatedCrash):
            runtime.submit_if_managed(
                self.project_id,
                "2860",
                body,
                "retry-import-crash",
            )

        staged = importer.store.runtime_state(
            self.project_id,
            str(failed["sprint_id"]),
        )
        assert staged is not None
        self.assertEqual(staged["recovery_records"][0]["status"], "completed")
        self.assertEqual(
            [item["status"] for item in staged["import_attempts"]],
            ["failed", "running"],
        )

        restarted = ManagedContinuityRuntime(
            self.importer(clock=lambda: FIXED_CLOCK),
            clock=lambda: FIXED_CLOCK,
        )
        progress = restarted.reconcile_all()
        self.assertGreater(progress[str(failed["sprint_id"])], 0)
        replay = restarted.submit_if_managed(
            self.project_id,
            "2860",
            body,
            "retry-import-crash-replay",
        )
        self.assertIsNotNone(replay)
        assert replay is not None
        self.assertTrue(replay.response["deduplicated"])
        recovered = restarted.store.runtime_state(
            self.project_id,
            str(failed["sprint_id"]),
        )
        assert recovered is not None
        self.assertEqual(recovered["status"], "active")
        self.assertEqual(
            [item["status"] for item in recovered["import_attempts"]],
            ["failed", "succeeded"],
        )
        self.assertEqual(managed_activation_invariant_issues(recovered), ())

    def test_retry_import_ack_does_not_reread_after_atomic_commit(self) -> None:
        parent, failed, context = self._failed_import()
        importer = self.importer(
            clock=lambda: FIXED_CLOCK,
            attempt_id_factory=lambda: "attempt-retry-no-reread",
        )
        runtime = ManagedContinuityRuntime(importer, clock=lambda: FIXED_CLOCK)
        original_begin = importer.store.begin_retry_import
        original_runtime_state = importer.store.runtime_state
        committed = False

        def begin(*args, **kwargs):
            nonlocal committed
            child = original_begin(*args, **kwargs)
            committed = True
            return child

        def runtime_state(*args, **kwargs):
            if committed:
                raise RuntimeError("transient read failure after atomic commit")
            return original_runtime_state(*args, **kwargs)

        with (
            patch.object(importer.store, "begin_retry_import", side_effect=begin),
            patch.object(importer.store, "runtime_state", side_effect=runtime_state),
        ):
            accepted = runtime.submit_if_managed(
                self.project_id,
                "2860",
                self._retry_request(parent, context),
                "retry-import-no-reread",
            )

        self.assertIsNotNone(accepted)
        assert accepted is not None
        self.assertEqual(accepted.response["status"], "RECOVERY_COMPLETED")
        self.assertEqual(
            accepted.response["produced_record_ids"],
            ["attempt-retry-no-reread"],
        )
        staged = importer.store.runtime_state(
            self.project_id,
            str(failed["sprint_id"]),
        )
        assert staged is not None
        self.assertEqual(staged["recovery_records"][0]["response"], accepted.response)
        self.assertEqual(managed_activation_invariant_issues(staged), ())

    def test_retry_import_child_failure_returns_receipt_and_routes_context(self) -> None:
        parent, failed, context = self._failed_import()

        def fail_retry(point: str, _context: dict) -> None:
            if point == "after_validate":
                raise RuntimeError("retry preparation still unavailable")

        importer = self.importer(
            clock=lambda: FIXED_CLOCK,
            attempt_id_factory=lambda: "attempt-retry-failed",
            fault_injector=fail_retry,
        )
        runtime = ManagedContinuityRuntime(importer, clock=lambda: FIXED_CLOCK)
        accepted = runtime.submit_if_managed(
            self.project_id,
            "2860",
            self._retry_request(parent, context),
            "retry-import-child-failed",
        )
        self.assertIsNotNone(accepted)
        assert accepted is not None
        self.assertEqual(accepted.response["status"], "RECOVERY_COMPLETED")

        staged = importer.store.runtime_state(
            self.project_id,
            str(failed["sprint_id"]),
        )
        assert staged is not None
        self.assertEqual(
            [item["status"] for item in staged["import_attempts"]],
            ["failed", "running"],
        )
        progress = runtime.reconcile_all()
        self.assertGreater(progress[str(failed["sprint_id"])], 0)

        retried = importer.store.runtime_state(
            self.project_id,
            str(failed["sprint_id"]),
        )
        assert retried is not None
        self.assertEqual(retried["status"], "failed")
        self.assertEqual(
            [item["status"] for item in retried["import_attempts"]],
            ["failed", "failed"],
        )
        self.assertEqual(len(retried["coordinator_contexts"]), 2)
        self.assertEqual(managed_activation_invariant_issues(retried), ())

        old_context_retry = self._retry_request(
            parent,
            context,
            recovery_key="retry-import-sibling",
            coordinator_key="coordinator-retry-old-context",
        )
        with self.assertRaises(ManagedContinuityError) as old_context_error:
            runtime.submit_if_managed(
                self.project_id,
                "2860",
                old_context_retry,
                "retry-import-old-context",
            )
        self.assertEqual(old_context_error.exception.code, "RECOVERY_CONFLICT")
        unchanged = importer.store.runtime_state(
            self.project_id,
            str(failed["sprint_id"]),
        )
        assert unchanged is not None
        self.assertEqual(len(unchanged["import_attempts"]), 2)

        child_id = accepted.response["produced_record_ids"][0]
        child = importer.store.lookup_attempt(self.project_id, child_id)
        self.assertIsNotNone(child)
        assert child is not None
        child_context = next(
            item
            for item in retried["coordinator_contexts"]
            if item["import_attempt_id"] == child_id
        )
        importer.fault_injector = None
        importer.attempt_id_factory = lambda: "attempt-retry-generation-2"
        second = runtime.submit_if_managed(
            self.project_id,
            "2860",
            self._retry_request(
                child,
                child_context,
                recovery_key="retry-import-generation-2",
                coordinator_key="coordinator-retry-generation-2",
            ),
            "retry-import-generation-2",
        )
        self.assertIsNotNone(second)
        assert second is not None
        second_child_id = second.response["produced_record_ids"][0]
        second_child = importer.store.lookup_attempt(
            self.project_id,
            second_child_id,
        )
        self.assertIsNotNone(second_child)
        assert second_child is not None
        self.assertEqual(second_child["recovery_generation"], 2)
        second_progress = runtime.reconcile_all()
        self.assertGreater(
            second_progress[str(failed["sprint_id"])], 0
        )
        activated = importer.store.runtime_state(
            self.project_id,
            str(failed["sprint_id"]),
        )
        assert activated is not None
        self.assertEqual(
            [item["status"] for item in activated["import_attempts"]],
            ["failed", "failed", "succeeded"],
        )
        self.assertEqual(managed_activation_invariant_issues(activated), ())

    def test_transient_child_resume_keeps_frozen_recovery_receipt(self) -> None:
        parent, failed, context = self._failed_import()
        importer = self.importer(
            clock=lambda: FIXED_CLOCK,
            attempt_id_factory=lambda: "attempt-retry-transient",
        )
        runtime = ManagedContinuityRuntime(importer, clock=lambda: FIXED_CLOCK)
        transient = ManagedImportError(
            "SPRINT_PREFLIGHT_FAILED",
            503,
            "transient-child-resume",
            phase="PREPARE",
        )

        accepted = runtime.submit_if_managed(
            self.project_id,
            "2860",
            self._retry_request(parent, context),
            "retry-import-transient",
        )
        self.assertIsNotNone(accepted)
        assert accepted is not None
        self.assertEqual(accepted.response["status"], "RECOVERY_COMPLETED")
        staged = importer.store.runtime_state(
            self.project_id,
            str(failed["sprint_id"]),
        )
        assert staged is not None
        self.assertEqual(
            [item["status"] for item in staged["import_attempts"]],
            ["failed", "running"],
        )

        with patch.object(
            importer,
            "resume_import_attempt",
            side_effect=transient,
        ):
            transient_progress = runtime.reconcile_all()
        self.assertGreater(
            transient_progress[str(failed["sprint_id"])], 0
        )
        still_staged = importer.store.runtime_state(
            self.project_id,
            str(failed["sprint_id"]),
        )
        assert still_staged is not None
        self.assertEqual(
            [item["status"] for item in still_staged["import_attempts"]],
            ["failed", "running"],
        )

        progress = runtime.reconcile_all()
        self.assertGreater(progress[str(failed["sprint_id"])], 0)
        recovered = importer.store.runtime_state(
            self.project_id,
            str(failed["sprint_id"]),
        )
        assert recovered is not None
        self.assertEqual(recovered["status"], "active")
        self.assertEqual(managed_activation_invariant_issues(recovered), ())

    def test_block_external_completes_failed_import_without_fake_effects(self) -> None:
        _parent, failed, context = self._failed_import()
        importer = self.importer(clock=lambda: FIXED_CLOCK)
        runtime = ManagedContinuityRuntime(importer, clock=lambda: FIXED_CLOCK)
        body = self._block_request(context)

        accepted = runtime.submit_if_managed(
            self.project_id,
            "2860",
            body,
            "block-import",
        )
        self.assertIsNotNone(accepted)
        assert accepted is not None
        self.assertEqual(accepted.response["produced_record_ids"], [])
        self.assertFalse(accepted.response["deduplicated"])
        blocked = importer.store.runtime_state(
            self.project_id,
            str(failed["sprint_id"]),
        )
        assert blocked is not None
        self.assertEqual(blocked["status"], "failed")
        self.assertIsNone(blocked["workflow"])
        self.assertEqual(blocked["blocker_observations"], [])
        self.assertEqual(managed_activation_invariant_issues(blocked), ())

        replay = runtime.submit_if_managed(
            self.project_id,
            "2860",
            body,
            "block-import-replay",
        )
        self.assertIsNotNone(replay)
        assert replay is not None
        self.assertTrue(replay.response["deduplicated"])

        with self.assertRaises(ManagedContinuityError) as decided:
            runtime.submit_if_managed(
                self.project_id,
                "2860",
                self._retry_request(
                    _parent,
                    context,
                    recovery_key="retry-after-external-block",
                    coordinator_key="retry-after-external-block",
                ),
                "retry-after-external-block",
            )
        self.assertEqual(decided.exception.code, "RECOVERY_CONFLICT")
        decided_state = importer.store.runtime_state(
            self.project_id,
            str(failed["sprint_id"]),
        )
        assert decided_state is not None
        self.assertEqual(len(decided_state["recovery_records"]), 1)

    def test_retry_conflict_is_frozen_and_allows_block_external(self) -> None:
        parent, failed, context = self._failed_import()
        importer = self.importer(clock=lambda: FIXED_CLOCK)
        runtime = ManagedContinuityRuntime(importer, clock=lambda: FIXED_CLOCK)

        with patch.object(
            importer.store,
            "begin_retry_import",
            side_effect=ManagedRetryImportConflict(
                "managed retry generation is exhausted"
            ),
        ):
            with self.assertRaises(ManagedContinuityError) as raised:
                runtime.submit_if_managed(
                    self.project_id,
                    "2860",
                    self._retry_request(parent, context),
                    "retry-import-cap",
                )
        self.assertEqual(raised.exception.code, "RECOVERY_CONFLICT")
        self.assertEqual(raised.exception.http_status, 409)
        unchanged = importer.store.runtime_state(
            self.project_id,
            str(failed["sprint_id"]),
        )
        assert unchanged is not None
        self.assertEqual(len(unchanged["recovery_records"]), 1)
        self.assertEqual(unchanged["recovery_records"][0]["status"], "failed")
        self.assertEqual(
            unchanged["recovery_records"][0]["normalized_error"],
            {"code": "RECOVERY_CONFLICT", "http_status": 409},
        )
        self.assertEqual(len(unchanged["import_attempts"]), 1)

        blocked = runtime.submit_if_managed(
            self.project_id,
            "2860",
            self._block_request(context, suffix="-after-cap"),
            "block-import-after-cap",
        )
        self.assertIsNotNone(blocked)
        assert blocked is not None
        self.assertEqual(blocked.response["produced_record_ids"], [])

    def test_import_recovery_rejects_wrong_target_and_reason(self) -> None:
        parent, failed, context = self._failed_import()
        importer = self.importer(clock=lambda: FIXED_CLOCK)
        runtime = ManagedContinuityRuntime(importer, clock=lambda: FIXED_CLOCK)
        wrong_target = canonical_json_bytes(
            {
                "assignment_id": context["context_id"],
                "idempotency_key": "wrong-import-target",
                "action": "RETRY_IMPORT",
                "parameters": {
                    "failed_attempt_id": "attempt-not-the-context-target",
                    "recovery_idempotency_key": "retry-wrong-target",
                },
            }
        )
        wrong_reason = canonical_json_bytes(
            {
                "assignment_id": context["context_id"],
                "idempotency_key": "wrong-import-reason",
                "action": "BLOCK_EXTERNAL",
                "parameters": {
                    "reason_code": "DIFFERENT_FAILURE",
                    "operator_action": "Resolve externally",
                },
            }
        )

        for body, correlation_id in (
            (wrong_target, "wrong-import-target"),
            (wrong_reason, "wrong-import-reason"),
        ):
            with self.assertRaises(ManagedContinuityError) as raised:
                runtime.submit_if_managed(
                    self.project_id,
                    "2860",
                    body,
                    correlation_id,
                )
            self.assertEqual(
                raised.exception.code,
                "INVALID_MANAGED_SPRINT_REQUEST",
            )
            self.assertEqual(raised.exception.http_status, 400)

        unchanged = importer.store.runtime_state(
            self.project_id,
            str(failed["sprint_id"]),
        )
        assert unchanged is not None
        self.assertEqual(unchanged["recovery_records"], [])
        self.assertEqual(len(unchanged["import_attempts"]), 1)
        self.assertEqual(
            parent["attempt_id"],
            unchanged["import_attempts"][0]["attempt_id"],
        )


if __name__ == "__main__":
    unittest.main()
