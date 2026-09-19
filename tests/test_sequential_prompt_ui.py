import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException, Request

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
            "launchPromptAgentLatestFileTemplate",
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
            "data.agent_latest_file_template",
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
        self.agent_latest_file_template = str(
            self.temp_path
            / "agent-latest"
            / "{repository}_{agent_phone}-latest.prompt"
        )
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
                    ),
                    "agent_latest_file_template": (
                        self.agent_latest_file_template
                    ),
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
        self.assertEqual(
            result["agent_latest_file_template"],
            self.agent_latest_file_template,
        )
        self.assertEqual(
            result["agent_latest_file_hint"],
            str(
                self.temp_path
                / "agent-latest"
                / "omega_{agent_phone}-latest.prompt"
            ),
        )
        self.assertIn(result["agent_latest_file_hint"], result["prompt"])
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
            "agent": {"name": "Reviewer 1", "phone": "4102"},
            "active_task": {"id": "task-1"},
        }

        stored = await main.attach_sequential_graph_response_storage(
            response,
            response_kind="transition-response",
        )

        self.assertEqual(
            list(stored)[:6],
            [
                "response_id",
                "latest_agent_prompt_file_path",
                "agent_prompt_file_paths",
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
        expected_agent_file = (
            self.temp_path / "agent-latest" / "omega_4102-latest.prompt"
        )
        self.assertEqual(
            stored["latest_agent_prompt_file_path"],
            str(expected_agent_file),
        )
        self.assertEqual(
            stored["agent_prompt_file_paths"],
            {"4102": str(expected_agent_file)},
        )
        self.assertEqual(
            json.loads(expected_agent_file.read_text(encoding="utf-8")),
            stored,
        )

    async def test_transition_uses_caller_phone_and_terminal_response_still_persists(
        self,
    ) -> None:
        transition = {
            "answer": "Результат принят; теперь работает Reviewer 2",
            "project": {
                "project_name": "Omega",
                "git_address": "https://github.com/acme/omega.git",
                "git_context_key": "github.com/acme/omega",
            },
            "agent": {"name": "Reviewer 2", "phone": "4103"},
            "active_task": {"id": "review-2"},
        }

        stored_transition = await main.attach_sequential_graph_response_storage(
            transition,
            response_kind="transition-response",
            request_agent_phone="4102",
        )

        caller_file = (
            self.temp_path / "agent-latest" / "omega_4102-latest.prompt"
        )
        next_agent_file = (
            self.temp_path / "agent-latest" / "omega_4103-latest.prompt"
        )
        self.assertEqual(
            stored_transition["latest_agent_prompt_file_path"],
            str(caller_file),
        )
        self.assertEqual(
            stored_transition["agent_prompt_file_paths"],
            {"4102": str(caller_file)},
        )
        self.assertFalse(next_agent_file.exists())
        self.assertEqual(
            json.loads(caller_file.read_text(encoding="utf-8")),
            stored_transition,
        )

        terminal = {
            "answer": "Спринт завершён",
            "project": transition["project"],
            "agent": None,
            "all_completed": True,
        }
        stored_terminal = await main.attach_sequential_graph_response_storage(
            terminal,
            response_kind="transition-response",
            request_agent_phone="4102",
        )

        self.assertEqual(
            stored_terminal["latest_agent_prompt_file_path"],
            str(caller_file),
        )
        self.assertEqual(
            json.loads(caller_file.read_text(encoding="utf-8")),
            stored_terminal,
        )

    async def test_agent_latest_files_overwrite_independently_with_full_utf8_json(
        self,
    ) -> None:
        project = {
            "project_name": "Omega",
            "git_address": "https://github.com/acme/omega.git",
            "git_context_key": "github.com/acme/omega",
        }
        large_utf8_result = ("Полный результат 🚀 — данные ревью\n" * 6000).rstrip()
        first = await main.attach_sequential_graph_response_storage(
            {
                "answer": large_utf8_result,
                "project": project,
                "agent": {"name": "Агент Один", "phone": "4201"},
            },
            response_kind="identity-response",
        )
        first_agent_file = Path(first["latest_agent_prompt_file_path"])
        first_archive_file = Path(first["response_file_path"])
        self.assertEqual(
            json.loads(first_agent_file.read_text(encoding="utf-8")),
            first,
        )

        with patch.object(main.os, "replace", wraps=main.os.replace) as replace:
            second = await main.attach_sequential_graph_response_storage(
                {
                    "answer": "Второй полный ответ — заменяет latest",
                    "project": project,
                    "agent": {"name": "Агент Один", "phone": "4201"},
                },
                response_kind="identity-response",
            )
        self.assertTrue(
            any(
                Path(call.args[1]) == first_agent_file
                for call in replace.call_args_list
            ),
            "stable per-agent file must be replaced atomically",
        )
        self.assertEqual(
            second["latest_agent_prompt_file_path"],
            str(first_agent_file),
        )
        self.assertNotEqual(first["response_id"], second["response_id"])
        self.assertEqual(
            json.loads(first_agent_file.read_text(encoding="utf-8")),
            second,
        )
        self.assertEqual(
            json.loads(first_archive_file.read_text(encoding="utf-8")),
            first,
        )

        third = await main.attach_sequential_graph_response_storage(
            {
                "answer": "Независимый ответ второго агента",
                "project": project,
                "agent": {"name": "Агент Два", "phone": "4202"},
            },
            response_kind="identity-response",
        )
        second_agent_file = Path(third["latest_agent_prompt_file_path"])
        self.assertNotEqual(first_agent_file, second_agent_file)
        self.assertEqual(
            json.loads(first_agent_file.read_text(encoding="utf-8")),
            second,
        )
        self.assertEqual(
            json.loads(second_agent_file.read_text(encoding="utf-8")),
            third,
        )
        self.assertEqual(list(self.temp_path.rglob("*.tmp")), [])

    def test_agent_latest_file_settings_migrate_and_preserve_custom_template(
        self,
    ) -> None:
        settings_path = main.sequential_prompt_settings_path()
        legacy_directory = str(self.temp_path / "legacy" / "{repository}")
        settings_path.write_text(
            json.dumps({"directory_template": legacy_directory}),
            encoding="utf-8",
        )

        migrated = main.read_sequential_prompt_settings_file()

        self.assertEqual(migrated["directory_template"], legacy_directory)
        self.assertEqual(
            migrated["agent_latest_file_template"],
            r"D:\Prompt\{repository}_{agent_phone}-latest.prompt",
        )
        self.assertEqual(
            main.DEFAULT_SEQUENTIAL_AGENT_LATEST_FILE_TEMPLATE,
            r"D:\Prompt\{repository}_{agent_phone}-latest.prompt",
        )

        custom_agent_template = str(
            self.temp_path
            / "custom-latest"
            / "{project}-{agent_phone}-latest.prompt"
        )
        main.write_sequential_prompt_settings_file(
            legacy_directory,
            custom_agent_template,
        )
        new_directory = str(self.temp_path / "new" / "{repository}")

        saved = main.write_sequential_prompt_settings_file(new_directory)

        self.assertEqual(saved["directory_template"], new_directory)
        self.assertEqual(
            saved["agent_latest_file_template"],
            custom_agent_template,
        )
        self.assertEqual(
            main.read_sequential_prompt_settings_file()[
                "agent_latest_file_template"
            ],
            custom_agent_template,
        )

    def test_agent_latest_file_template_resolves_exact_configurable_name(self) -> None:
        resolved = main.resolve_sequential_agent_latest_file(
            self.agent_latest_file_template,
            project_name="Omega Project",
            git_address="https://github.com/acme/omega.git",
            git_context_key="github.com/acme/omega",
            agent_phone="+49 123/45",
        )

        self.assertEqual(
            resolved,
            self.temp_path
            / "agent-latest"
            / "omega_+49-123_45-latest.prompt",
        )
        with self.assertRaisesRegex(Exception, r"must contain \{agent_phone\}"):
            main.validate_sequential_agent_latest_file_template(
                str(self.temp_path / "{repository}-latest.prompt")
            )

    def test_agent_latest_file_template_rejects_unsafe_or_wrong_shape(self) -> None:
        invalid_templates = (
            r"relative\{repository}_{agent_phone}-latest.prompt",
            r"\\server\share\{repository}_{agent_phone}-latest.prompt",
            str(
                self.temp_path
                / ".."
                / "{repository}_{agent_phone}-latest.prompt"
            ),
            str(
                self.temp_path
                / "{agent_phone}"
                / "{repository}-latest.prompt"
            ),
            str(
                self.temp_path
                / "{repository}_{agent_phone}-latest.txt"
            ),
        )

        for file_template in invalid_templates:
            with self.subTest(file_template=file_template):
                with self.assertRaises(HTTPException):
                    main.validate_sequential_agent_latest_file_template(
                        file_template
                    )

    async def test_agent_latest_survives_both_archive_write_failures(self) -> None:
        real_write = main.write_prompt_text_if_changed

        def fail_archive_writes(target_path: Path, prompt: str) -> None:
            if (
                target_path.name == "latest-response.json"
                or target_path.name.startswith("identity-response-")
            ):
                raise OSError(f"archive unavailable: {target_path.name}")
            real_write(target_path, prompt)

        with patch.object(
            main,
            "write_prompt_text_if_changed",
            side_effect=fail_archive_writes,
        ):
            stored = await main.attach_sequential_graph_response_storage(
                {
                    "answer": "Полный ответ остаётся доступен агенту",
                    "project": {
                        "project_name": "Omega",
                        "git_address": "https://github.com/acme/omega.git",
                        "git_context_key": "github.com/acme/omega",
                    },
                    "agent": {"name": "Agent", "phone": "4301"},
                },
                response_kind="identity-response",
            )

        self.assertEqual(
            set(stored["response_storage_errors"]),
            {"response_archive", "project_latest"},
        )
        self.assertFalse(Path(stored["response_file_path"]).exists())
        self.assertFalse(Path(stored["latest_response_file_path"]).exists())
        agent_latest = Path(stored["latest_agent_prompt_file_path"])
        self.assertTrue(agent_latest.is_file())
        self.assertEqual(
            json.loads(agent_latest.read_text(encoding="utf-8")),
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
