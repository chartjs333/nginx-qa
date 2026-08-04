import json
import re
import shutil
import subprocess
import unittest

import main


class CycleGraphUiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.html = main.render_index_v2()

    def test_cycles_tab_and_view_have_stable_unique_markers(self) -> None:
        self.assertEqual(
            self.html.count('class="page-tab" data-view="cycles"'),
            1,
        )
        self.assertEqual(
            self.html.count(
                'class="view cycle-graph-view" data-view="cycles"'
            ),
            1,
        )
        for element_id in (
            "cycleGraphProjectSummary",
            "cycleGraphCycleSelect",
            "refreshCycleGraphButton",
            "cycleGraphStatus",
            "cycleGraphCycleList",
            "cycleGraphCanvas",
            "cycleGraphTaskLineage",
            "cycleGraphHistory",
        ):
            self.assertEqual(self.html.count(f'id="{element_id}"'), 1)

    def test_ui_uses_canonical_project_phone_and_cycles_api(self) -> None:
        project_phone_function = re.search(
            r"function cycleGraphProjectPhone\(.*?\n    \}",
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(project_phone_function)
        self.assertIn(
            "context && context.project_phone",
            project_phone_function.group(0),
        )
        self.assertNotIn("git_address", project_phone_function.group(0))
        self.assertNotIn("git_context_key", project_phone_function.group(0))
        self.assertIn(
            "/api/v1/projects/${encodeURIComponent(projectId)}/cycles?limit=100",
            self.html,
        )
        self.assertIn(
            "/api/v1/cycles/${encodeURIComponent(cycleId)}/graph",
            self.html,
        )
        self.assertIn(
            "/api/v1/cycles/${encodeURIComponent(cycleId)}/history",
            self.html,
        )

    def test_graph_is_native_accessible_svg_with_activity_and_handoff_states(self) -> None:
        for marker in (
            'role="img"',
            'aria-label="Граф агентов и задач цикла"',
            'data-node-type="agent"',
            'data-node-type="task"',
            'data-edge-type="communication"',
            'data-edge-type="task-lineage"',
            "cycle-agent-pulse",
            "cycle-handoff-flow",
            "handoff-active",
            "prefers-reduced-motion: reduce",
        ):
            self.assertIn(marker, self.html)
        self.assertNotIn("d3.min.js", self.html.lower())
        self.assertNotIn("mermaid.min.js", self.html.lower())

    def test_activity_is_derived_from_audit_events_and_cleared_on_completion(self) -> None:
        activity_function = re.search(
            r"function deriveCycleGraphActivity\(.*?\n    \}\n\n"
            r"    function renderCycleGraphHistory",
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(activity_function)
        source = activity_function.group(0)
        for event_type in (
            "TASK_STARTED",
            "HANDOFF_TRIGGERED",
            "GROUP_REPORT_SUBMITTED",
            "MESSAGE_REMOVED",
            "CYCLE_COMPLETED",
        ):
            self.assertIn(event_type, source)
        self.assertIn("activeByTask.clear()", source)

    @unittest.skipUnless(shutil.which("node"), "Node.js is required for the UI behavior test")
    def test_handoff_preserves_other_parallel_tasks_of_the_same_agent(self) -> None:
        activity_function = re.search(
            r"(function deriveCycleGraphActivity\(.*?\n    \})\n\n"
            r"    function renderCycleGraphHistory",
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(activity_function)
        events = [
            {
                "event_type": "TASK_STARTED",
                "task_node_id": "task-a",
                "to_agent_id": "agent-x",
            },
            {
                "event_type": "TASK_STARTED",
                "task_node_id": "task-b",
                "to_agent_id": "agent-x",
            },
            {
                "event_type": "HANDOFF_TRIGGERED",
                "task_node_id": "task-a-child",
                "parent_task_node_id": "task-a",
                "from_agent_id": "agent-x",
                "to_agent_id": "agent-y",
            },
            {
                "event_type": "TASK_STARTED",
                "task_node_id": "task-a-child",
                "to_agent_id": "agent-y",
            },
        ]
        script = "\n".join(
            (
                activity_function.group(1),
                f"const events = {json.dumps(events)};",
                "const afterHandoff = deriveCycleGraphActivity(events, 'in_progress');",
                "const afterReport = deriveCycleGraphActivity(events.concat([{event_type: 'GROUP_REPORT_SUBMITTED', task_node_id: 'task-b', from_agent_id: 'agent-x'}]), 'in_progress');",
                "const afterCompletion = deriveCycleGraphActivity(events.concat([{event_type: 'CYCLE_COMPLETED'}]), 'completed');",
                "console.log(JSON.stringify({",
                "  handoffAgents: [...afterHandoff.agentIds].sort(),",
                "  handoffTasks: [...afterHandoff.taskNodeIds].sort(),",
                "  reportAgents: [...afterReport.agentIds].sort(),",
                "  reportTasks: [...afterReport.taskNodeIds].sort(),",
                "  completedAgents: [...afterCompletion.agentIds],",
                "  completedTasks: [...afterCompletion.taskNodeIds]",
                "}));",
            )
        )
        result = subprocess.run(
            [shutil.which("node"), "-e", script],
            check=True,
            capture_output=True,
            text=True,
        )
        actual = json.loads(result.stdout)
        self.assertEqual(actual["handoffAgents"], ["agent-x", "agent-y"])
        self.assertEqual(actual["handoffTasks"], ["task-a-child", "task-b"])
        self.assertEqual(actual["reportAgents"], ["agent-y"])
        self.assertEqual(actual["reportTasks"], ["task-a-child"])
        self.assertEqual(actual["completedAgents"], [])
        self.assertEqual(actual["completedTasks"], [])

    def test_refresh_rejects_stale_responses_and_runs_only_for_active_view(self) -> None:
        for marker in (
            "new AbortController()",
            "cycleGraphRequestVersion",
            "requestVersion !== cycleGraphRequestVersion",
            "projectId !== cycleGraphProjectPhone()",
            "cycleId !== cycleGraphSelectedCycleId",
            "cycleGraphViewIsActive()",
            "cycleGraphRefreshInFlight",
        ):
            self.assertIn(marker, self.html)
        self.assertIn('if (view === "cycles")', self.html)
        self.assertIn("refreshCycleGraph({silent: true})", self.html)


if __name__ == "__main__":
    unittest.main()
