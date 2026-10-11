import json
import os
from copy import deepcopy
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from fastapi import FastAPI, HTTPException

from nginx_qa.legacy_scope_control import (
    ADMIN_TOKEN_HEADER,
    assignment_binding,
    canonical_json_sha256,
)
from nginx_qa.scope_api import register_scope_api
from nginx_qa.scope_runtime_adapters import apply_scope_revision
from tests import test_managed_continuity as managed_fixture
from tests.test_sprint_type_contract import COMMIT, SPRINT_ID, TIMESTAMP


class PromptArchiveManagedAuthorizationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.harness = managed_fixture.ManagedContinuityTests()
        self.harness.setUp()
        self.runtime = self.harness.runtime
        self.role_token = "archive-test-role-token-" + "a" * 40
        self.environment = patch.dict(
            os.environ,
            {"NGINX_QA_SCOPE_CONTROL_ROLE_TOKEN_2861": self.role_token},
            clear=False,
        )
        self.environment.start()
        governed, _ = apply_scope_revision(
            self.harness.initial_state,
            [],
            assignment_id="assignment-1",
            amendment_id="scope-archive-read-1",
            proposal={
                "instructions": "Read bounded prompt archive evidence",
                "retained_restrictions": ["No deployment", "Read only"],
                "node_ids": ["build", "continuity"],
                "reviewer_ids": ["reviewer-a", "reviewer-b"],
            },
            source={
                "repository_key": self.harness.initial_state["repository"][
                    "repository_key"
                ],
                "commit": COMMIT,
                "path": "scope.json",
                "sha256": "a" * 64,
            },
            authorization={
                "request_id": "request-archive-read-1",
                "decision_id": "decision-archive-read-1",
                "actor": "operator-test",
            },
            expected_scope_revision=0,
            applied_at=TIMESTAMP,
        )
        self.harness.replace_state(governed)

    def tearDown(self) -> None:
        self.environment.stop()
        self.harness.tearDown()

    def authorize(self, **changes: str):
        arguments = {
            "project_id": "project-id",
            "sprint_id": SPRINT_ID,
            "agent_phone": "2861",
            "supplied_role_token": self.role_token,
        }
        arguments.update(changes)
        return self.runtime.authorize_prompt_archive_read(**arguments)

    def test_exact_governed_binding_returns_closed_non_secret_snapshot(self) -> None:
        before = self.runtime.scope_snapshot("project-id", SPRINT_ID)
        binding = assignment_binding(before, "assignment-1")
        assert binding is not None
        with (
            patch.object(
                self.runtime,
                "current_identity",
                side_effect=AssertionError("archive auth must not claim identity"),
            ),
            patch.object(
                self.runtime,
                "_publish_binding",
                side_effect=AssertionError("archive auth must not publish"),
            ),
            patch.object(
                self.runtime,
                "drain_outbox",
                side_effect=AssertionError("archive auth must not drain"),
            ),
        ):
            snapshot = self.authorize()

        self.assertEqual(before, self.runtime.scope_snapshot("project-id", SPRINT_ID))
        self.assertEqual(
            set(snapshot or {}),
            {
                "project_id",
                "sprint_id",
                "assignment_id",
                "assignment_kind",
                "agent_phone",
                "authorization",
            },
        )
        assert snapshot is not None
        self.assertEqual(snapshot["assignment_id"], "assignment-1")
        self.assertEqual(snapshot["assignment_kind"], "assignment")
        self.assertEqual(snapshot["agent_phone"], "2861")
        self.assertEqual(
            snapshot["authorization"],
            {
                "auth_model_version": "prompt-archive-authorization-v1",
                "principal_kind": "managed_agent",
                "principal_id": binding["agent_id"],
                "binding_id": "assignment-1",
                "scope_source": "managed_effective_scope",
                "effective_scope_revision": binding["effective_revision"],
                "effective_scope_sha256": binding["effective_scope_sha256"],
                "decision_occurrence_id": "assignment-1",
            },
        )
        rendered = json.dumps(snapshot, sort_keys=True)
        self.assertNotIn(self.role_token, rendered)
        self.assertNotIn("effective_core", rendered)
        self.assertNotIn("scope_context", rendered)

    def test_invalid_token_scope_absence_and_cross_sprint_are_indistinguishable(self) -> None:
        self.assertIsNone(self.authorize(supplied_role_token="wrong"))
        self.assertIsNone(self.authorize(sprint_id="msv1-other"))

        ungoverned = self.runtime.scope_snapshot("project-id", SPRINT_ID)
        ungoverned.pop("scope_control", None)
        self.harness.replace_state(ungoverned)
        self.assertIsNone(self.authorize())

    def test_stale_scope_context_revision_fails_closed(self) -> None:
        stale = self.runtime.scope_snapshot("project-id", SPRINT_ID)
        binding = stale["scope_control"]["assignment_bindings"]["assignment-1"]
        binding["scope_context"]["effective_revision"] += 1
        binding["scope_context_sha256"] = canonical_json_sha256(
            binding["scope_context"]
        )
        self.harness.replace_state(stale)

        self.assertIsNone(self.authorize())

    def test_ambiguous_same_phone_live_binding_fails_without_publication(self) -> None:
        bindings, corrupt, project_wide = self.runtime._project_binding_discovery(
            "project-id"
        )
        self.assertEqual(corrupt, ())
        self.assertFalse(project_wide)
        selected = next(
            binding
            for binding in bindings
            if binding.record.get("assignment_id") == "assignment-1"
        )
        duplicate_record = deepcopy(selected.record)
        duplicate_record["assignment_id"] = "assignment-other"
        duplicate = type(selected)(
            selected.project_id,
            "msv1-other",
            deepcopy(selected.state),
            selected.kind,
            duplicate_record,
        )
        with (
            patch.object(
                self.runtime,
                "_project_binding_discovery",
                return_value=((selected, duplicate), (), False),
            ),
            patch.object(
                self.runtime,
                "_publish_binding",
                side_effect=AssertionError("archive auth must not publish"),
            ),
        ):
            self.assertIsNone(self.authorize())

    def test_targeted_corrupt_live_ownership_fails_closed(self) -> None:
        with patch.object(
            self.runtime,
            "_project_binding_discovery",
            return_value=(
                (),
                (
                    {
                        "kind": "assignment",
                        "assignment_id": "assignment-corrupt",
                        "phone": "2861",
                        "live": True,
                    },
                ),
                False,
            ),
        ):
            self.assertIsNone(self.authorize())


class PromptArchiveOperatorPredicateTests(unittest.TestCase):
    def test_scope_registration_exposes_exact_operator_predicate_only(self) -> None:
        admin_token = "archive-test-admin-token-" + "b" * 40
        host = SimpleNamespace(app=FastAPI(), parse_strict_json_object=json.loads)
        with patch.dict(
            os.environ,
            {"NGINX_QA_SCOPE_CONTROL_ADMIN_TOKEN": admin_token},
            clear=False,
        ):
            register_scope_api(host)
            self.assertTrue(callable(host.scope_api_operator))
            request = SimpleNamespace(
                headers={ADMIN_TOKEN_HEADER: admin_token},
                cookies={},
                base_url="http://127.0.0.1:18032/",
            )
            self.assertEqual(
                host.scope_api_operator(request), "credentialed_operator"
            )
            with self.assertRaises(HTTPException) as denied:
                host.scope_api_operator(
                    SimpleNamespace(
                        headers={},
                        cookies={},
                        base_url="http://127.0.0.1:18032/",
                    )
                )
            self.assertEqual(denied.exception.status_code, 401)


if __name__ == "__main__":
    unittest.main()
