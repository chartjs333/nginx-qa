"""Checkpoint projection must not allocate a copy of excluded history."""
from copy import deepcopy
import unittest

from nginx_qa.execution_observability import checkpoint_payload


class ExcludedHistory:
    def __deepcopy__(self, memo):
        raise AssertionError("Excluded checkpoint history was unnecessarily copied")


class CheckpointCopyMemoryTests(unittest.TestCase):
    def test_excluded_history_is_not_copied(self):
        state = {"revision": 54, "status": "active", "execution_audit": ExcludedHistory(),
                 "observability": ExcludedHistory(), "assignments": [{"assignment_id": "current"}]}
        result = checkpoint_payload({"project_id": "p", "sprint_id": "s", "execution": state})
        self.assertEqual(result["execution"], {"revision": 54, "status": "active",
                                               "assignments": [{"assignment_id": "current"}]})
        self.assertIsInstance(state["execution_audit"], ExcludedHistory)
        result["execution"]["assignments"][0]["assignment_id"] = "changed-output-only"
        self.assertEqual(state["assignments"][0]["assignment_id"], "current")

    def test_payload_is_identical_to_existing_projection(self):
        snapshot = {"project_id": "p", "sprint_id": "s", "execution": {
            "revision": 54, "workflow": {"nodes": ["coordinator"]},
            "execution_audit": {"checkpoints": [{"snapshot": {"revision": 53}}]},
            "observability": {"history": [1, 2]}, "scope_control": {"effective_revision": 3}},
            "agents": [{"id": "coordinator"}], "pending_work": [], "recent_activity": []}
        state = deepcopy(snapshot["execution"])
        state.pop("execution_audit", None)
        state.pop("observability", None)
        expected = {"project_id": "p", "sprint_id": "s", "execution": state,
                    "workflow": deepcopy(state["workflow"]), "agents": deepcopy(snapshot["agents"]),
                    "pending_work": [], "recent_activity": [], "scope_requests": []}
        self.assertEqual(checkpoint_payload(snapshot), expected)


if __name__ == "__main__":
    unittest.main()
