"""Bounded read-only scope snapshots retain the full v2 security contract.

All state and Git sources are synthetic, using the isolated scope fixtures.
Only the new methods are collected; inherited compatibility tests keep their
existing independent suites.
"""
from copy import deepcopy
import json
import unittest
from unittest.mock import patch

import main
from tests import test_legacy_scope_control as fixtures
from tests.test_scope_revisions import ScopeRevisionTests


class _NoWholeHistoryCopy(list):
    def __deepcopy__(self, memo):
        raise MemoryError("regression: unrelated execution audit was deep-copied")


class ScopeSnapshotMemoryTests(ScopeRevisionTests):
    def setUp(self):
        super().setUp()
        # Scope snapshots do not need host-repository discovery during the
        # fixture's historical transitions. Its synthetic HTTPS address must
        # never cause probing of real workspaces outside the temporary repo.
        lookup = patch.object(main, "resolve_local_git_reference", return_value={})
        lookup.start()
        self.addCleanup(lookup.stop)
        repository = patch.object(main, "local_repo_for_remote", return_value=self.repo_path)
        repository.start()
        self.addCleanup(repository.stop)

    def _document(self):
        return json.loads(main.git_config_path.read_text(encoding="utf-8"))

    def _execution(self, document):
        return document[main.PROJECTS_KEY][self.PROJECT_CONTEXT][
            main.PROJECT_AGENT_ASSIGNMENT_KEY
        ]

    async def _get_scope(self, assignment_id):
        return await fixtures.asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/assignments/"
            f"{assignment_id}/effective-scope",
            headers=self._role_headers(self.COORDINATOR_PHONE),
        )

    async def test_snapshot_does_not_copy_unrelated_execution_history(self):
        assignment_id, _ = await self._v1_and_migrate()
        self._semantic_apply(2)
        self._semantic_apply(3)
        document = self._document()
        execution = self._execution(document)
        audit = _NoWholeHistoryCopy(execution.get("execution_audit") or [])
        execution["execution_audit"] = audit
        persisted_before = main.git_config_path.read_bytes()

        # Simulate the allocation failure at its cause without consuming a
        # production-sized history or exhausting the test machine's memory.
        with patch.object(main, "read_git_config_file", return_value=document):
            code, response = await self._get_scope(assignment_id)
            self.assertEqual(code, 200, response)
            snapshot = main.legacy_scope_binding_snapshot_transaction(
                self.PROJECT_PHONE, assignment_id
            )

        self.assertEqual(response["effective_revision"], 3)
        self.assertFalse(response["mutated"])
        self.assertFalse(response["acknowledged"])
        self.assertEqual(
            set(snapshot["state"]),
            {"strategy", "current_assignment_id", "assignments", "active_task"},
        )
        self.assertEqual(len(snapshot["state"]["assignments"]), 1)
        self.assertEqual(
            snapshot["state"]["assignments"][0]["assignment_id"], assignment_id
        )
        self.assertIs(execution["execution_audit"], audit)
        self.assertEqual(main.git_config_path.read_bytes(), persisted_before)

    async def test_snapshot_http_response_matches_full_state_baseline(self):
        assignment_id, _ = await self._v1_and_migrate()
        self._semantic_apply(2)
        for acknowledged in (False, True):
            with self.subTest(acknowledged=acknowledged):
                if acknowledged:
                    await self._scope_and_ack(assignment_id, self.COORDINATOR_PHONE)
                full_state = self._execution(self._document())
                full_snapshot = main.legacy_scope_binding_snapshot_transaction(
                    self.PROJECT_PHONE, assignment_id
                )
                # The old route received the complete execution document.
                # Keep that full-state baseline independent of the compact
                # snapshot representation being tested, then compare every
                # HTTP response field, including context, source and ACK.
                full_snapshot["state"] = deepcopy(full_state)
                persisted_before = main.git_config_path.read_bytes()
                with patch.object(
                    main, "legacy_scope_binding_snapshot_transaction",
                    return_value=full_snapshot,
                ):
                    baseline = await self._get_scope(assignment_id)
                actual = await self._get_scope(assignment_id)
                self.assertEqual(baseline[0], 200, baseline[1])
                self.assertEqual(actual, baseline)
                self.assertEqual(actual[1]["acknowledged"], acknowledged)
                self.assertFalse(actual[1]["mutated"])
                self.assertEqual(main.git_config_path.read_bytes(), persisted_before)

    async def test_snapshot_returned_objects_are_detached_from_parsed_state(self):
        assignment_id, _ = await self._v1_and_migrate()
        self._semantic_apply(2)
        await self._scope_and_ack(assignment_id, self.COORDINATOR_PHONE)
        document = self._document()
        agents = main.read_agents_file()
        original_document = deepcopy(document)
        original_agents = deepcopy(agents)
        with patch.object(main, "read_git_config_file", return_value=document), \
                patch.object(main, "read_agents_file", return_value=agents):
            snapshot = main.legacy_scope_binding_snapshot_transaction(
                self.PROJECT_PHONE, assignment_id
            )

        snapshot["state"]["active_task"]["message"] = "changed snapshot only"
        snapshot["state"]["active_task"]["metadata"]["assignment_id"] = "not-current"
        snapshot["state"]["assignments"][0]["status"] = "changed snapshot only"
        snapshot["assignment"]["status"] = "changed snapshot only"
        snapshot["binding"]["effective"]["profile"] = "changed snapshot only"
        snapshot["binding"]["scope_context"]["node_id"] = "changed snapshot only"
        snapshot["issued_agent"]["profile"] = "changed snapshot only"
        snapshot["effective_agent"]["profile"] = "changed snapshot only"
        snapshot["acknowledgement"]["scope_context"]["node_id"] = "changed snapshot only"
        self.assertEqual(document, original_document)
        self.assertEqual(agents, original_agents)

    async def test_snapshot_rejects_tampered_historical_v2_binding(self):
        assignment_id, _ = await self._v1_and_migrate()
        self._semantic_apply(2)
        self._semantic_apply(3)
        document = self._document()
        for failure in ("current", "active", "delivered"):
            with self.subTest(failure=failure):
                invalid = deepcopy(document)
                execution = self._execution(invalid)
                if failure == "current":
                    execution["current_assignment_id"] = "not-the-current-assignment"
                    expected_error = "SCOPE_ASSIGNMENT_NOT_CURRENT"
                elif failure == "active":
                    current = next(
                        item for item in execution["assignments"]
                        if item["assignment_id"] == assignment_id
                    )
                    current["status"] = "completed"
                    expected_error = "SCOPE_ASSIGNMENT_NOT_ACTIVE"
                else:
                    execution["active_task"] = None
                    expected_error = "SCOPE_ASSIGNMENT_NOT_DELIVERED"
                invalid_before = deepcopy(invalid)
                with patch.object(main, "read_git_config_file", return_value=invalid):
                    code, response = await self._get_scope(assignment_id)
                self.assertEqual(code, 409, response)
                self.assertEqual(response["detail"]["error"], expected_error)
                self.assertEqual(invalid, invalid_before)
        control = self._execution(document)["scope_control"]
        current_binding = deepcopy(control["assignment_bindings"][assignment_id])
        self.assertEqual(len(control["binding_history"][assignment_id]), 3)
        # Only the oldest revision is corrupt; checking merely the current
        # binding would incorrectly accept this document.
        control["binding_history"][assignment_id][0]["effective"]["profile"] = "tampered"
        self.assertEqual(control["assignment_bindings"][assignment_id], current_binding)
        broken_before = deepcopy(document)
        persisted_before = main.git_config_path.read_bytes()
        with patch.object(main, "read_git_config_file", return_value=document):
            code, response = await self._get_scope(assignment_id)
        self.assertEqual(code, 503, response)
        self.assertEqual(response["detail"]["error"], "SCOPE_BINDING_INVALID")
        self.assertEqual(document, broken_before)
        self.assertEqual(main.git_config_path.read_bytes(), persisted_before)


def load_tests(loader, tests, pattern):
    return unittest.TestSuite(
        ScopeSnapshotMemoryTests(name)
        for name in sorted(vars(ScopeSnapshotMemoryTests))
        if name.startswith("test_snapshot_")
    )
