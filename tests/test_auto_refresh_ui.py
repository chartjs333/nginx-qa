import re
import shutil
import subprocess
import unittest

import main


class AutoRefreshUiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.pages = {
            "legacy": main.render_index(),
            "main": main.render_index_v2(),
        }
        cls.scripts = {}
        for name, html in cls.pages.items():
            match = re.search(r"<script>(.*)</script>", html, flags=re.DOTALL)
            if match is None:
                raise AssertionError(f"{name} page has no inline script")
            cls.scripts[name] = match.group(1)

    def test_all_pages_use_two_minute_single_flight_refresh(self) -> None:
        for name, script in self.scripts.items():
            with self.subTest(page=name):
                self.assertIn(
                    "const AUTO_REFRESH_INTERVAL_MS = 2 * 60 * 1000;",
                    script,
                )
                self.assertIn("if (refreshInFlight)", script)
                self.assertIn("Promise.allSettled(tasks)", script)
                self.assertIn("window.setTimeout(() =>", script)
                self.assertIn("}, AUTO_REFRESH_INTERVAL_MS);", script)
                self.assertIn("startAutoRefreshCountdown();", script)
                self.assertNotIn("setInterval(refresh", script)
                self.assertNotIn("setInterval(refresh, 5000)", script)

    def test_countdown_bar_shrinks_and_resets(self) -> None:
        for name, script in self.scripts.items():
            with self.subTest(page=name):
                self.assertIn('progressEl.style.width = "100%";', script)
                self.assertIn('progressEl.style.width = "0%";', script)
                self.assertIn(
                    "progressEl.style.transition = `width ${AUTO_REFRESH_INTERVAL_MS}ms linear`;",
                    script,
                )
                self.assertIn(
                    'document.querySelectorAll(".auto-refresh-progress")',
                    script,
                )

    def test_every_main_page_refresh_button_has_a_countdown(self) -> None:
        html = self.pages["main"]
        refresh_button_ids = (
            "refreshButton",
            "refreshAttachmentFoldersButton",
            "refreshLaunchPromptButton",
            "refreshPendingSprintsButton",
            "refreshCycleGraphButton",
            "refreshScreenshotFoldersButton",
            "refreshEvidenceFoldersButton",
            "refreshProjectSprintsButton",
        )
        for button_id in refresh_button_ids:
            with self.subTest(button_id=button_id):
                pattern = (
                    r'<div class="refresh-control">\s*'
                    rf'<button[^>]*id="{button_id}"[^>]*>.*?</button>\s*'
                    r'<div class="auto-refresh-track"[^>]*>\s*'
                    r'<div class="auto-refresh-progress"></div>'
                )
                self.assertRegex(html, pattern)
        self.assertEqual(
            html.count('class="auto-refresh-progress"'),
            len(refresh_button_ids),
        )

    def test_section_refreshes_restart_the_shared_countdown(self) -> None:
        script = self.scripts["main"]
        self.assertIn("async function refreshSectionManually(refreshAction)", script)
        for action in (
            "refreshPendingSprints",
            "refreshLaunchPrompt",
            "refreshAttachmentFolderChoices",
            "refreshScreenshotFolders",
            "refreshEvidenceFolders",
            "refreshProjectSprints",
        ):
            self.assertIn(f"refreshSectionManually({action})", script)
        self.assertIn(
            "refreshSectionManually(() => refreshCycleGraph(",
            script,
        )

    def test_auto_refresh_covers_active_folder_pages(self) -> None:
        script = self.scripts["main"]
        for marker in (
            'activeViewName === "messages"',
            'activeViewName === "agents"',
            'activeViewName === "launch-prompt"',
            'activeViewName === "pending-sprints"',
            'activeViewName === "cycles"',
            'activeViewName === "screenshots"',
            'activeViewName === "evidence"',
            "refreshAttachmentFolderChoices()",
            "refreshProjectSprints()",
            "refreshPendingSprints()",
            "refreshScreenshotFolders()",
            "refreshEvidenceFolders()",
        ):
            self.assertIn(marker, script)

        refresh_source = re.search(
            r"async function refresh\(\) \{(.*?)\n    \}",
            script,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(refresh_source)
        source = refresh_source.group(1)
        self.assertLess(
            source.index('activeViewName === "agents"'),
            source.index("refreshProjectSprints()"),
        )

    @unittest.skipUnless(shutil.which("node"), "Node.js is required for JS syntax check")
    def test_rendered_javascript_passes_node_syntax_check(self) -> None:
        node = shutil.which("node")
        for name, script in self.scripts.items():
            with self.subTest(page=name):
                result = subprocess.run(
                    [node, "--check", "-"],
                    input=script,
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                )
                self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
