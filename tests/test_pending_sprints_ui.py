import re
import shutil
import subprocess
import unittest

import main


class PendingSprintsUiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.html = main.render_index_v2()
        match = re.search(r"<script>(.*)</script>", cls.html, flags=re.DOTALL)
        if match is None:
            raise AssertionError("render_index_v2 has no inline script")
        cls.javascript = match.group(1)

    def test_tab_view_and_controls_have_unique_stable_markers(self) -> None:
        self.assertEqual(
            self.html.count('class="page-tab" data-view="pending-sprints"'),
            1,
        )
        self.assertEqual(
            self.html.count(
                'class="view pending-sprints-view" data-view="pending-sprints"'
            ),
            1,
        )
        for element_id in (
            "pendingSprintsProjectSelect",
            "pendingSprintsProjectSummary",
            "refreshPendingSprintsButton",
            "pendingSprintsStatus",
            "pendingSprints",
            "pendingSprintPreviewTitle",
            "pendingSprintJsonPreview",
        ):
            self.assertEqual(self.html.count(f'id="{element_id}"'), 1)
        self.assertIn('data-help-topic="page:pending-sprints"', self.html)

    def test_ui_uses_project_scoped_pending_sprint_contract(self) -> None:
        for marker in (
            "function refreshPendingSprints()",
            "function loadPendingSprintPreview(sprintId",
            "function startPendingSprint(sprintId)",
            "/api/v1/projects/${encodeURIComponent(projectPhone)}/pending-sprints`",
            "/pending-sprints/${encodeURIComponent(selectedId)}`",
            "/pending-sprints/${encodeURIComponent(selectedId)}/start`",
            'data-action="preview-pending-sprint"',
            'data-action="start-pending-sprint"',
            "data.pending_sprints",
            "data.import_payload",
            'pendingSprintsFetchOptions({method: "POST"})',
            "window.confirm(",
        ):
            self.assertIn(marker, self.html)

    def test_deep_link_uses_exact_parameters_and_is_applied_after_git_config(self) -> None:
        for marker in (
            'initialPageParams.get("view")',
            'initialPageParams.get("project_phone")',
            'initialPageParams.get("sprint_id")',
            'initialPageParams.get("pending_token")',
            'pendingSprintsDeepLink.view !== "pending-sprints"',
            'url.searchParams.set("view", "pending-sprints")',
            'url.searchParams.set("project_phone", projectPhone)',
            'url.searchParams.set("sprint_id", pendingSprintsSelectedId)',
            'url.searchParams.set("pending_token", pendingSprintsPublicToken)',
            'setActiveView("pending-sprints", {refreshView: false, syncUrl: false})',
        ):
            self.assertIn(marker, self.javascript)
        refresh_source = re.search(
            r"async function refresh\(\) \{(.*?)\n    \}",
            self.javascript,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(refresh_source)
        source = refresh_source.group(1)
        self.assertLess(
            source.index("await refreshGitConfig();"),
            source.index("applyPendingSprintsDeepLink();"),
        )
        self.assertLess(
            source.index("applyPendingSprintsDeepLink();"),
            source.index("await refreshAgents();"),
        )

    def test_refresh_rejects_stale_project_responses(self) -> None:
        refresh_source = re.search(
            r"async function refreshPendingSprints\(\) \{(.*?)\n    \}\n\n"
            r"    async function startPendingSprint",
            self.javascript,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(refresh_source)
        source = refresh_source.group(1)
        for marker in (
            "const requestVersion = ++pendingSprintsRequestVersion;",
            "requestVersion !== pendingSprintsRequestVersion",
            "projectPhone !== pendingSprintsActiveProjectPhone()",
        ):
            self.assertIn(marker, source)

        preview_source = re.search(
            r"async function loadPendingSprintPreview\(.*?\) \{(.*?)\n    \}\n\n"
            r"    async function refreshPendingSprints",
            self.javascript,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(preview_source)
        for marker in (
            "requestVersion !== pendingSprintPreviewRequestVersion",
            "projectPhone !== pendingSprintsActiveProjectPhone()",
            "selectedId !== pendingSprintsSelectedId",
        ):
            self.assertIn(marker, preview_source.group(1))

    def test_public_token_is_sent_and_scoped_to_its_deep_link_project(self) -> None:
        for marker in (
            "const pendingSprintsPublicToken =",
            'headers.set("X-Pending-Sprints-Token", pendingSprintsPublicToken)',
            'url.searchParams.delete("pending_token")',
            "pendingSprintsPublicToken && projectPhone !== tokenProjectPhone",
            "pendingSprintsFetchOptions()",
            'pendingSprintsFetchOptions({method: "POST"})',
        ):
            self.assertIn(marker, self.javascript)
        self.assertEqual(
            self.javascript.count("pendingSprintsFetchOptions()"),
            2,
            "list and detail GET must both include the token-aware options",
        )
        self.assertIn(
            "if (pendingSprintsPublicToken && projectPhone !== tokenProjectPhone)",
            self.javascript,
        )

    def test_activating_sprint_respects_backend_startable_flag(self) -> None:
        render_source = re.search(
            r"function renderPendingSprints\(\) \{(.*?)\n    \}\n\n"
            r"    function syncPendingSprintsUrl",
            self.javascript,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(render_source)
        for marker in (
            "const canRetry = isActivating && sprint.startable === true;",
            'isActivating && !canRetry',
            '"Запускается..."',
            '"Повторить запуск"',
        ):
            self.assertIn(marker, render_source.group(1))

        start_source = re.search(
            r"async function startPendingSprint\(.*?\) \{(.*?)\n    \}\n\n"
            r"    function applyPendingSprintsDeepLink",
            self.javascript,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(start_source)
        for marker in (
            "if (isRetry && sprint.startable !== true)",
            "Предыдущая попытка запуска не завершилась",
            'isRetry ? "Повторяю запуск" : "Запускаю"',
        ):
            self.assertIn(marker, start_source.group(1))

    def test_successful_start_refreshes_all_affected_views(self) -> None:
        start_source = re.search(
            r"async function startPendingSprint\(.*?\) \{(.*?)\n    \}\n\n"
            r"    function applyPendingSprintsDeepLink",
            self.javascript,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(start_source)
        source = start_source.group(1)
        for marker in (
            "await refreshAgents();",
            "refreshPendingSprints()",
            "refreshQueues()",
            "refreshScheduledTasks()",
            "refreshHistory()",
            "refreshProjectSprints()",
        ):
            self.assertIn(marker, source)

    @unittest.skipUnless(shutil.which("node"), "Node.js is required for JS syntax check")
    def test_rendered_javascript_passes_node_syntax_check(self) -> None:
        result = subprocess.run(
            [shutil.which("node"), "--check", "-"],
            input=self.javascript,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
