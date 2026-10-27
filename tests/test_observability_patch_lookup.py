"""Synthetic observability reads must not resolve unused Git patch history.

All store/history inputs are in-memory fixtures; no server lifespan, registry,
Git repository, credential, or live runtime is accessed.
"""
import asyncio
from collections import deque
from copy import deepcopy
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from fastapi import FastAPI

from nginx_qa.execution_observability import append_execution_checkpoint, project_execution
from nginx_qa.legacy_scope_control import make_assignment_binding
from nginx_qa.scope_api import register_scope_api
from tests.test_execution_observability import delta_fixture


PATCH_FIELDS = {
    "history_with_patches", "activity_with_patches", "code_patches", "code_patch_summary",
}
STAMP = "2026-10-27T12:00:00Z"


def snapshot_fixture():
    source = delta_fixture()
    state = source["execution"]
    current = state["assignments"][-1]
    agent = {"profile": "Synthetic read-only scope", "tasks": []}
    amendment = {"amendment_id": "synthetic-amendment", "effective_revision": 1}
    binding = make_assignment_binding(
        amendment=amendment, assignment=current, issued_agent=agent,
        effective_agent=agent, node_id=current["node_id"], phase="node",
        occurrence=4, bound_at=STAMP,
    )
    state["scope_control"] = {
        "schema_version": 1, "amendments": [amendment],
        "assignment_bindings": {current["assignment_id"]: binding},
        "acknowledgements": {current["assignment_id"]: {
            "ack_id": "synthetic-ack", "assignment_id": current["assignment_id"],
            "scope_context": deepcopy(binding["scope_context"]), "acknowledged_at": STAMP,
        }},
    }
    state["active_task"] = {"task_id": "synthetic-task", "message": "Read-only task"}
    state["scope_workflow"] = {"events": [{
        "event_id": "synthetic-event", "kind": "scope_applied", "timestamp": STAMP,
        "execution_revision": 74,
    }]}
    historical = deepcopy(source)
    historical["execution"]["revision"] = 73
    ledger = {}
    append_execution_checkpoint(ledger, historical, recorded_at=STAMP)
    state["execution_audit"] = ledger
    return {
        "project_id": source["project_id"], "project_phone": source["project_id"],
        "context_key": "example.invalid/synthetic/project",
        "project": {"git_address": "https://example.invalid/synthetic/project.git"},
        "assignment": state, "agents": [{"id": "synthetic-reviewer", "phone": "2792"}],
    }


class ProjectStatePatchLookupTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        # Imported only for these host-adapter tests, never by the route fixture.
        import main
        self.main = main
        self.source = snapshot_fixture()
        self.original = deepcopy(self.source)
        self.records = [
            {"id": "activity-a", "timestamp": STAMP, "message": "Earlier result",
             "metadata": {"git_commit": "a" * 40, "git_branch": "agent/synthetic"}},
            {"id": "activity-b", "timestamp": STAMP, "message": "Later result",
             "metadata": {"git_commit": "b" * 40, "git_branch": "agent/synthetic"}},
        ]
        replacements = {
            "run_group_write_transaction": AsyncMock(return_value=self.source),
            "read_history": AsyncMock(return_value=self.records),
            "locks": {name: asyncio.Lock() for name in main.GROUP_QUEUE_NAMES},
            "queues": {name: deque() for name in main.GROUP_QUEUE_NAMES},
            "utc_now": lambda: STAMP,
            "project_state_patch_cache": {},
        }
        for name, value in replacements.items():
            replacement = patch.object(main, name, value)
            replacement.start()
            self.addCleanup(replacement.stop)
        resolver = patch.object(main, "resolve_git_patch", return_value={
            "patch": "synthetic patch", "source": "synthetic", "patch_url": None,
            "local_repo": None,
        })
        self.resolver = resolver.start()
        self.addCleanup(resolver.stop)

    async def test_omitting_patch_history_never_resolves_git_patches(self):
        with patch.object(self.main, "history_with_patches_context", side_effect=
                          AssertionError("unused patch lookup must not run")) as builder:
            view = await self.main.project_state_json(
                "9000", history_limit=17, include_patch_content=False,
                include_patch_history=False,
            )
        builder.assert_not_called()
        self.resolver.assert_not_called()
        self.assertTrue(PATCH_FIELDS.isdisjoint(view))
        self.assertEqual(view["recent_activity"], self.records)
        self.main.read_history.assert_awaited_once_with(
            limit=17, date_from="1970-01-01", git_context=self.source["context_key"],
        )
        self.assertEqual(self.source, self.original)

    async def test_default_and_explicit_patch_history_preserve_existing_payload(self):
        default = await self.main.project_state_json("9000")
        self.resolver.assert_called_once()
        self.assertTrue(PATCH_FIELDS.issubset(default))
        self.assertEqual(default["history_with_patches"]["patch_count"], 1)
        self.assertEqual(default["code_patches"][0]["patch"], "synthetic patch")
        explicit = await self.main.project_state_json("9000", include_patch_history=True)
        self.assertEqual(default, explicit)
        # Historical include_patch_content=False still performs lookup/counting;
        # changing that public option would silently change existing semantics.
        self.main.project_state_patch_cache.clear()
        self.resolver.reset_mock()
        content_only = await self.main.project_state_json("9000", include_patch_content=False)
        self.resolver.assert_called_once()
        self.assertTrue(PATCH_FIELDS.issubset(content_only))
        self.assertEqual(content_only["code_patch_summary"]["available_patch_count"], 1)
        self.assertEqual(content_only["code_patches"], [])

    async def test_projector_full_response_cursor_and_history_match_without_patches(self):
        full = await self.main.project_state_json("9000", include_patch_content=False)
        lean = await self.main.project_state_json(
            "9000", include_patch_content=False, include_patch_history=False,
        )
        self.assertEqual({key: value for key, value in full.items() if key not in PATCH_FIELDS}, lean)
        for view in (full, lean):
            view["sprint_id"] = "synthetic-sprint"
        checkpoints = full["execution"]["execution_audit"]["checkpoints"]
        expected = project_execution(full, checkpoints=checkpoints)
        actual = project_execution(lean, checkpoints=checkpoints)
        self.assertEqual(actual, expected)
        self.assertEqual(
            project_execution(lean, checkpoints=checkpoints, cursor=expected["cursor"]),
            project_execution(full, checkpoints=checkpoints, cursor=expected["cursor"]),
        )
        self.assertTrue(project_execution(lean, checkpoints=checkpoints, cursor=expected["cursor"])["unchanged"])
        for selection in ({"at_revision": 73}, {"at_checkpoint": checkpoints[0]["checkpoint_id"]}):
            self.assertEqual(project_execution(lean, checkpoints=checkpoints, **selection),
                             project_execution(full, checkpoints=checkpoints, **selection))
        self.assertEqual(lean["execution"]["execution_audit"], self.original["assignment"]["execution_audit"])
        self.assertEqual(lean["execution"]["scope_control"], self.original["assignment"]["scope_control"])
        self.assertTrue(lean["execution"]["execution_authorized"])
        self.assertEqual(self.source, self.original)


class ObservabilityPatchRouteTests(unittest.IsolatedAsyncioTestCase):
    async def test_current_observability_opts_out_of_unused_patch_history(self):
        source = delta_fixture()
        project_id, sprint_id = source["project_id"], source["sprint_id"]
        app = FastAPI()
        host = SimpleNamespace(
            app=app, managed_continuity_runtime=None,
            project_sprint_history_snapshot=AsyncMock(return_value=(None, None, {
                "current_sprint_id": sprint_id,
            })),
            project_state_json=AsyncMock(return_value=deepcopy(source)),
        )
        register_scope_api(host)
        route = next(route for route in app.routes if route.path ==
                     "/api/v1/projects/{project_id}/sprints/{sprint_id}/observability")
        response = await route.endpoint(project_id, sprint_id)
        self.assertEqual(response, project_execution(source, scope_requests=[], checkpoints=[]))
        host.project_state_json.assert_awaited_once_with(
            project_id, include_patch_content=False, include_patch_history=False,
        )


if __name__ == "__main__":
    unittest.main()
