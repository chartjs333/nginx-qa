import asyncio
import json
import tempfile
import unittest
from pathlib import Path

from fastapi import Request

import main


def request_for_port(port: int = 8025) -> Request:
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": "/api/v1/sequential-sprint-prompt",
        "raw_path": b"/api/v1/sequential-sprint-prompt",
        "query_string": b"",
        "root_path": "",
        "headers": [(b"host", f"testserver:{port}".encode("ascii"))],
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", port),
    }
    return Request(scope)


class SequentialPromptUiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.html = main.render_index_v2()

    def test_launch_prompt_tab_and_controls_have_unique_markers(self) -> None:
        self.assertEqual(
            self.html.count('class="page-tab" data-view="launch-prompt"'),
            1,
        )
        self.assertEqual(
            self.html.count(
                'class="view launch-prompt-view" data-view="launch-prompt"'
            ),
            1,
        )
        for element_id in (
            "launchPromptProjectName",
            "launchPromptGitAddress",
            "launchPromptEndpointMode",
            "launchPromptWhoamiUrl",
            "launchPromptDirectoryTemplate",
            "launchPromptId",
            "launchPromptFilePath",
            "launchPromptLatestResponsePath",
            "launchPromptText",
            "copyLaunchPromptButton",
            "copyLaunchPromptPathButton",
            "copyLaunchResponsePathButton",
            "refreshLaunchPromptButton",
            "saveLaunchPromptDirectoryButton",
            "launchPromptStatus",
        ):
            self.assertEqual(self.html.count(f'id="{element_id}"'), 1)

    def test_ui_requests_selected_context_and_supports_public_or_local_url(self) -> None:
        for marker in (
            "/api/v1/sequential-sprint-prompt?${params.toString()}",
            "git_context_key: gitContextKey",
            'endpoint_mode: launchPromptEndpointModeEl.value || "public"',
            'if (view === "launch-prompt")',
            "copyTextToClipboard(prompt)",
            "/api/v1/sequential-sprint-prompt/settings",
            "data.latest_response_file_path",
        ):
            self.assertIn(marker, self.html)


class SequentialPromptApiTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.temp_path = Path(self.temp_dir.name)
        self.original_git_config_path = main.git_config_path
        self.original_history_path = main.history_path
        self.original_git_config_lock = main.git_config_lock
        self.original_sequential_prompt_storage_lock = (
            main.sequential_prompt_storage_lock
        )
        main.git_config_path = self.temp_path / "port_git_map.json"
        main.history_path = self.temp_path / "conversation_log.jsonl"
        main.git_config_lock = asyncio.Lock()
        main.sequential_prompt_storage_lock = asyncio.Lock()
        main.git_config_path.write_text(
            json.dumps(
                {
                    "8025": {
                        "git_address": "https://github.com/acme/omega.git",
                        "project_name": "Omega",
                        "git_context_key": "github.com/acme/omega",
                    }
                }
            ),
            encoding="utf-8",
        )
        settings_path = main.sequential_prompt_settings_path()
        settings_path.parent.mkdir(parents=True, exist_ok=True)
        settings_path.write_text(
            json.dumps(
                {
                    "directory_template": str(
                        self.temp_path / "saved-prompts" / "{repository}"
                    )
                }
            ),
            encoding="utf-8",
        )

    def tearDown(self) -> None:
        main.git_config_path = self.original_git_config_path
        main.history_path = self.original_history_path
        main.git_config_lock = self.original_git_config_lock
        main.sequential_prompt_storage_lock = (
            self.original_sequential_prompt_storage_lock
        )
        self.temp_dir.cleanup()

    def write_tunnel_url(self, value: str) -> None:
        runtime_path = main.runtime_state_directory()
        runtime_path.mkdir(parents=True, exist_ok=True)
        (runtime_path / "cloudflared-quick-tunnel.url").write_text(
            value,
            encoding="utf-8",
        )

    async def test_public_prompt_substitutes_project_repository_and_tunnel(self) -> None:
        self.write_tunnel_url("https://fresh-tunnel.trycloudflare.com\n")

        result = await main.get_sequential_sprint_prompt(
            request_for_port(),
            "github.com/acme/omega",
            "public",
        )

        self.assertEqual(result["endpoint_mode"], "public")
        self.assertEqual(result["project"]["project_name"], "Omega")
        self.assertEqual(
            result["selected_whoami_url"],
            "https://fresh-tunnel.trycloudflare.com/api/v1/agents/whoami",
        )
        self.assertIn("проекте Omega", result["prompt"])
        self.assertIn("https://github.com/acme/omega.git", result["prompt"])
        self.assertIn(result["selected_whoami_url"], result["prompt"])
        self.assertIn(f'Prompt ID: {result["prompt_id"]}', result["prompt"])
        self.assertIn(result["prompt_file_path"], result["prompt"])
        stored_prompt = Path(result["prompt_file_path"]).read_text(encoding="utf-8")
        self.assertEqual(stored_prompt, result["prompt"])
        self.assertEqual(
            Path(result["latest_prompt_file_path"]).read_text(encoding="utf-8"),
            result["prompt"],
        )
        self.assertEqual(Path(result["prompt_directory"]).name, "omega")
        self.assertIn(
            "Выполняйте этот цикл при каждом переходе графа",
            result["prompt"],
        )
        self.assertNotIn("{{PROJECT_NAME}}", result["prompt"])
        self.assertNotIn("YOUR_PUBLIC_HOST", result["prompt"])
        self.assertNotIn("http://localhost:8025", result["prompt"])

    async def test_local_prompt_uses_request_port(self) -> None:
        self.write_tunnel_url("https://fresh-tunnel.trycloudflare.com")

        result = await main.get_sequential_sprint_prompt(
            request_for_port(8123),
            "github.com/acme/omega",
            "local",
        )

        self.assertEqual(result["endpoint_mode"], "local")
        self.assertEqual(
            result["selected_whoami_url"],
            "http://localhost:8123/api/v1/agents/whoami",
        )
        self.assertIn(result["selected_whoami_url"], result["prompt"])

    async def test_graph_response_is_saved_with_id_and_recovery_path_first(self) -> None:
        response = {
            "answer": "Полный ответ перехода",
            "project_id": "9001",
            "project": {
                "project_name": "Omega",
                "git_address": "https://github.com/acme/omega.git",
                "git_context_key": "github.com/acme/omega",
            },
            "agent": {"name": "Reviewer 1"},
            "active_task": {"id": "task-1"},
        }

        stored = await main.attach_sequential_graph_response_storage(
            response,
            response_kind="transition-response",
        )

        self.assertEqual(
            list(stored)[:4],
            [
                "response_id",
                "response_file_path",
                "latest_response_file_path",
                "full_response_instructions",
            ],
        )
        response_file = Path(stored["response_file_path"])
        self.assertTrue(response_file.is_file())
        self.assertEqual(
            json.loads(response_file.read_text(encoding="utf-8")),
            stored,
        )
        self.assertEqual(
            json.loads(
                Path(stored["latest_response_file_path"]).read_text(
                    encoding="utf-8"
                )
            ),
            stored,
        )

    def test_directory_template_supports_repository_project_and_context(self) -> None:
        directory = main.resolve_sequential_prompt_directory(
            str(
                self.temp_path
                / "{repository}"
                / "{project}"
                / "{git_context_key}"
            ),
            project_name="Omega Project",
            git_address="https://github.com/acme/omega.git",
            git_context_key="github.com/acme/omega",
        )

        self.assertEqual(
            directory,
            self.temp_path
            / "omega"
            / "Omega-Project"
            / "github.com_acme_omega",
        )

    def test_webhook_url_is_reduced_to_safe_public_base(self) -> None:
        self.assertEqual(
            main.normalize_public_nginx_qa_base_url(
                "https://public.example.net/api/v1/telegram/agents/sequential"
            ),
            "https://public.example.net",
        )
        self.assertEqual(
            main.normalize_public_nginx_qa_base_url(
                "https://user:secret@public.example.net/api/v1/agents/whoami"
            ),
            "",
        )


if __name__ == "__main__":
    unittest.main()
