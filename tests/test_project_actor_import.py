import asyncio
import json
import os
import tempfile
import unittest
import urllib.parse
from collections import deque
from pathlib import Path
from unittest.mock import patch

import main


async def asgi_request(
    target: str,
    *,
    method: str = "GET",
    payload: object | None = None,
    headers: list[tuple[bytes, bytes]] | None = None,
    host: str = "testserver:8025",
    scheme: str = "http",
    client_host: str = "127.0.0.1",
) -> tuple[int, object]:
    parsed = urllib.parse.urlsplit(target)
    body = json.dumps(payload).encode("utf-8") if payload is not None else b""
    delivered = False
    messages: list[dict[str, object]] = []

    async def receive() -> dict[str, object]:
        nonlocal delivered
        if delivered:
            return {"type": "http.disconnect"}
        delivered = True
        return {"type": "http.request", "body": body, "more_body": False}

    async def send(message: dict[str, object]) -> None:
        messages.append(message)

    request_headers = [(b"host", host.encode("ascii"))]
    if payload is not None:
        request_headers.append((b"content-type", b"application/json"))
    request_headers.extend(headers or [])
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "scheme": scheme,
        "path": parsed.path,
        "raw_path": parsed.path.encode("ascii"),
        "query_string": parsed.query.encode("ascii"),
        "root_path": "",
        "headers": request_headers,
        "client": (client_host, 12345),
        "server": (host.partition(":")[0], 8025),
    }
    await main.app(scope, receive, send)
    response_start = next(
        message for message in messages if message["type"] == "http.response.start"
    )
    response_body = b"".join(
        message.get("body", b"")
        for message in messages
        if message["type"] == "http.response.body"
    )
    decoded: object = json.loads(response_body) if response_body else None
    return int(response_start["status"]), decoded


class ProjectActorImportTests(unittest.IsolatedAsyncioTestCase):
    PROJECT_PHONE = "9008"
    PROJECT_CONTEXT = "github.com/example/actor-import"
    TELEGRAM_ENV_NAMES = (
        "TELEGRAM_BOT_TOKEN",
        "TELEGRAM_WEBHOOK_SECRET",
        "TELEGRAM_WEBHOOK_URL",
        "TELEGRAM_WEBHOOK_AUTO_REGISTER",
        "TELEGRAM_DROP_PENDING_UPDATES",
        "TELEGRAM_ALLOWED_CHAT_IDS",
        "TELEGRAM_ALLOWED_USER_IDS",
        "TELEGRAM_HISTORY_CHAT_ID",
        "TELEGRAM_HISTORY_MESSAGE_THREAD_ID",
    )

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        temp_path = Path(self.temp_dir.name)
        self.temp_path = temp_path
        self.sequential_agent_latest_file_template = str(
            temp_path
            / "agent-latest"
            / "{repository}_{agent_phone}-latest.prompt"
        )
        self.original_values = {
            "git_config_path": main.git_config_path,
            "agents_path": main.agents_path,
            "history_path": main.history_path,
            "sprint_history_path": main.sprint_history_path,
            "pending_sprints_path": main.pending_sprints_path,
            "git_config_lock": main.git_config_lock,
            "agents_lock": main.agents_lock,
            "history_lock": main.history_lock,
            "sprint_history_lock": main.sprint_history_lock,
            "pending_sprints_lock": main.pending_sprints_lock,
            "group_task_submission_lock": main.group_task_submission_lock,
            "sequential_prompt_storage_lock": main.sequential_prompt_storage_lock,
            "project_state_patch_cache": main.project_state_patch_cache,
            "queues": main.queues,
            "locks": main.locks,
        }
        main.git_config_path = temp_path / "port_git_map.json"
        main.agents_path = temp_path / "agents.json"
        main.history_path = temp_path / "conversation_log.jsonl"
        main.sprint_history_path = temp_path / "project_sprints.json"
        main.pending_sprints_path = temp_path / "pending_project_sprints.json"
        main.git_config_lock = asyncio.Lock()
        main.agents_lock = asyncio.Lock()
        main.history_lock = asyncio.Lock()
        main.sprint_history_lock = asyncio.Lock()
        main.pending_sprints_lock = asyncio.Lock()
        main.group_task_submission_lock = asyncio.Lock()
        main.sequential_prompt_storage_lock = asyncio.Lock()
        main.project_state_patch_cache = {}
        main.queues = {name: deque() for name in main.QUEUE_DEFINITIONS}
        main.locks = {name: asyncio.Lock() for name in main.QUEUE_DEFINITIONS}
        self.original_telegram_env = {
            name: os.environ.get(name)
            for name in self.TELEGRAM_ENV_NAMES
        }
        for name in self.TELEGRAM_ENV_NAMES:
            os.environ.pop(name, None)
        main.write_sequential_prompt_settings_file(
            str(temp_path / "prompts" / "{repository}"),
            self.sequential_agent_latest_file_template,
        )
        self.write_project()
        self.write_agents()

    def tearDown(self) -> None:
        for name, value in self.original_values.items():
            setattr(main, name, value)
        for name, value in self.original_telegram_env.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        self.temp_dir.cleanup()

    def write_project(self) -> None:
        project = {
            "project_name": "Actor Import Project",
            "git_address": "https://github.com/example/actor-import.git",
            "git_context_key": self.PROJECT_CONTEXT,
            "project_phone": self.PROJECT_PHONE,
            "groups": [],
            "group_relationships": [],
            "customer_reporting": {},
        }
        config = {
            main.PROJECTS_KEY: {self.PROJECT_CONTEXT: project},
            main.PHONE_GIT_CONTEXTS_KEY: {
                self.PROJECT_PHONE: {
                    **project,
                    "phone": self.PROJECT_PHONE,
                }
            },
        }
        main.git_config_path.write_text(
            json.dumps(config, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def write_agents(self) -> None:
        agents = [
            {
                "id": "old-project-actor",
                "name": "Old Project Actor",
                "phone": "2010",
                "profile": "Old profile",
                "parameters": {
                    "git_context_key": self.PROJECT_CONTEXT,
                    "project_phone": self.PROJECT_PHONE,
                },
            },
            {
                "id": "outside-actor",
                "name": "Outside Actor",
                "phone": "2011",
                "profile": "Must be preserved",
                "parameters": {"git_context_key": "github.com/example/outside"},
            },
        ]
        main.agents_path.write_text(
            json.dumps({"agents": agents}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def register_project(
        self,
        *,
        context_key: str,
        project_phone: str,
        git_address: str,
        project_name: str,
    ) -> None:
        project = {
            "project_name": project_name,
            "git_address": git_address,
            "git_context_key": context_key,
            "project_phone": project_phone,
            "groups": [],
            "group_relationships": [],
            "customer_reporting": {},
        }
        config = main.read_git_config_file()
        config.setdefault(main.PROJECTS_KEY, {})[context_key] = project
        config.setdefault(main.PHONE_GIT_CONTEXTS_KEY, {})[project_phone] = {
            **project,
            "phone": project_phone,
        }
        main.write_git_config_file(config)

    async def start_staged_sprint(
        self,
        staged: dict[str, object],
        *,
        project_phone: str | None = None,
    ) -> dict[str, object]:
        pending = staged.get("pending_sprint")
        self.assertIsInstance(pending, dict)
        assert isinstance(pending, dict)
        sprint_id = str(pending.get("id") or "")
        self.assertTrue(sprint_id)
        status_code, body = await asgi_request(
            (
                f"/api/v1/projects/{project_phone or self.PROJECT_PHONE}"
                f"/pending-sprints/{urllib.parse.quote(sprint_id, safe='')}/start"
            ),
            method="POST",
        )
        self.assertEqual(status_code, 200, body)
        self.assertIsInstance(body, dict)
        assert isinstance(body, dict)
        raw_result = body.get("result") or body.get("import_result") or body
        self.assertIsInstance(raw_result, dict)
        assert isinstance(raw_result, dict)
        return raw_result

    async def test_overwrite_import_replaces_project_actors_and_queues_tasks(self) -> None:
        payload = {
            "project_id": self.PROJECT_PHONE,
            "git_address": "https://github.com/example/actor-import.git",
            "actors": {
                "overwrite": True,
                "items": [
                    {
                        "id": "new-analyst",
                        "name": "New Analyst",
                        "phone": "2021",
                        "profile": "Analyze tasks.",
                        "tasks": [
                            "Prepare requirements.",
                            {
                                "task_id": "QA-1",
                                "queue": "tester-all",
                                "message": "Verify the requirements.",
                            },
                        ],
                    }
                ],
            }
        }
        status_code, body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/actors/import",
            method="POST",
            payload=payload,
        )

        self.assertEqual(status_code, 201)
        self.assertIsInstance(body, dict)
        assert isinstance(body, dict)
        self.assertEqual(body["removed_actor_count"], 1)
        self.assertEqual(body["imported_actor_count"], 1)
        self.assertEqual(body["queued_task_count"], 2)
        imported = body["imported_agents"][0]
        self.assertEqual(imported["name"], "New Analyst")
        self.assertEqual(len(imported["tasks"]), 2)
        self.assertEqual(
            imported["parameters"]["git_context_key"],
            self.PROJECT_CONTEXT,
        )

        stored = main.read_agents_file()
        self.assertEqual(
            {agent["id"] for agent in stored},
            {main.PROJECT_MANAGER_AGENT_ID, "outside-actor", "new-analyst"},
        )
        self.assertEqual(len(main.queues["worker-all"]), 1)
        self.assertEqual(len(main.queues["tester-all"]), 1)

        wrong_project_item = main.make_queue_item(
            "Message from another Git project.",
            {
                "conversation_phone": self.PROJECT_PHONE,
                "from_phone": "2998",
                "to_phone": "2021",
                "phone_channel": "worker-all",
                "git_context_key": "github.com/example/other-project",
                "git_address": "https://github.com/example/other-project.git",
            },
        )
        main.queues["worker-all"].appendleft(wrong_project_item)

        poll_status, poll_body = await asgi_request(
            f"/worker/all/{self.PROJECT_PHONE}?to_phone=2021"
        )
        self.assertEqual(poll_status, 200)
        self.assertIsInstance(poll_body, dict)
        assert isinstance(poll_body, dict)
        self.assertEqual(poll_body["message"], "Prepare requirements.")
        self.assertEqual(poll_body["metadata"]["to_agent_id"], "new-analyst")
        self.assertEqual(
            poll_body["metadata"]["to_agent_git_branch"],
            "agent/new-analyst",
        )
        self.assertEqual(
            poll_body["metadata"]["to_agent_profile_endpoint"],
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/2021",
        )
        self.assertEqual(len(main.queues["worker-all"]), 1)
        self.assertEqual(
            main.queue_item_message(main.queues["worker-all"][0]),
            "Message from another Git project.",
        )
        main.queues["worker-all"].popleft()

        second_status, second_body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/actors/import",
            method="POST",
            payload={
                "project_id": self.PROJECT_PHONE,
                "git_address": "https://github.com/example/actor-import.git",
                "actors": {
                    "overwrite": True,
                    "items": [
                        {
                            "id": "new-developer",
                            "name": "New Developer",
                            "tasks": ["Implement the approved requirements."],
                        }
                    ],
                }
            },
        )
        self.assertEqual(second_status, 201)
        self.assertIsInstance(second_body, dict)
        assert isinstance(second_body, dict)
        self.assertEqual(second_body["removed_actor_count"], 1)
        self.assertEqual(second_body["removed_task_count"], 1)
        self.assertEqual(second_body["imported_agents"][0]["phone"], "2000")
        self.assertEqual(len(main.queues["tester-all"]), 0)
        self.assertEqual(len(main.queues["worker-all"]), 1)

    async def test_new_import_archives_previous_sprint_state_and_downloads_it(self) -> None:
        first_payload = {
            "project_id": self.PROJECT_PHONE,
            "git_address": "https://github.com/example/actor-import.git",
            "sprint": {"id": "sprint-one", "title": "Sprint One"},
            "agents": {
                "overwrite": True,
                "items": [
                    {
                        "id": "sprint-agent",
                        "name": "Sprint Agent",
                        "phone": "2026",
                        "git_branch": "agent/sprint-agent",
                        "tasks": [
                            {"task_id": "S1-1", "message": "Finish sprint one."}
                        ],
                    }
                ],
            },
        }
        first_status, first_body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/import",
            method="POST",
            payload=first_payload,
            headers=[
                (
                    b"x-nginx-qa-sprint-filename",
                    urllib.parse.quote("delta_sprint_01.json").encode("ascii"),
                )
            ],
        )
        self.assertEqual(first_status, 201)
        self.assertIsInstance(first_body, dict)
        assert isinstance(first_body, dict)
        self.assertEqual(first_body["sprint"]["title"], "Sprint One")

        delivered_status, delivered_body = await asgi_request(
            f"/worker/all/{self.PROJECT_PHONE}?to_phone=2026"
        )
        self.assertEqual(delivered_status, 200)
        self.assertIsInstance(delivered_body, dict)

        for message, commit in (
            ("Work at commit A.", "a" * 40),
            ("More work at commit A.", "a" * 40),
            ("Work at commit B.", "b" * 40),
        ):
            await main.append_history(
                "agent_progress",
                "worker-all",
                message,
                {
                    "sender": "Sprint Agent",
                    "receiver": "Project Manager",
                    "from_phone": "2026",
                    "to_phone": main.PROJECT_MANAGER_PHONE,
                    "from_agent_id": "sprint-agent",
                },
                git_context={
                    "project_name": "Actor Import Project",
                    "git_context_key": self.PROJECT_CONTEXT,
                    "git_address": "https://github.com/example/actor-import.git",
                    "git_commit": commit,
                    "git_commit_short": commit[:12],
                },
            )

        second_payload = {
            "project_id": self.PROJECT_PHONE,
            "git_address": "https://github.com/example/actor-import.git",
            "sprint": {"id": "sprint-two", "title": "Sprint Two"},
            "agents": {
                "overwrite": True,
                "items": [
                    {
                        "id": "sprint-agent",
                        "name": "Sprint Agent",
                        "phone": "2026",
                        "git_branch": "agent/sprint-agent",
                        "tasks": [
                            {"task_id": "S2-1", "message": "Start sprint two."}
                        ],
                    }
                ],
            },
        }
        patch_calls: list[tuple[str, str, str | None]] = []
        original_resolve_git_patch = main.resolve_git_patch

        def fake_resolve_git_patch(
            git_address: str,
            to_commit: str,
            from_commit: str | None = None,
        ) -> dict[str, str]:
            patch_calls.append((git_address, to_commit, from_commit))
            return {
                "source": "test_git",
                "patch": f"diff --git a/file.txt b/file.txt\n+{to_commit[:8]}",
            }

        main.resolve_git_patch = fake_resolve_git_patch
        try:
            second_status, second_body = await asgi_request(
                f"/api/v1/projects/{self.PROJECT_PHONE}/agents/import",
                method="POST",
                payload=second_payload,
                headers=[
                    (
                        b"x-nginx-qa-sprint-filename",
                        urllib.parse.quote("delta_sprint_02.json").encode("ascii"),
                    )
                ],
            )
        finally:
            main.resolve_git_patch = original_resolve_git_patch
        self.assertEqual(second_status, 201)
        self.assertIsInstance(second_body, dict)
        assert isinstance(second_body, dict)
        self.assertEqual(second_body["sprint"]["title"], "Sprint Two")

        list_status, list_body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/sprints"
        )
        self.assertEqual(list_status, 200)
        self.assertIsInstance(list_body, dict)
        assert isinstance(list_body, dict)
        self.assertEqual(list_body["sprint_count"], 3)
        sprint_one = next(
            sprint for sprint in list_body["sprints"] if sprint["title"] == "Sprint One"
        )
        sprint_two = next(
            sprint for sprint in list_body["sprints"] if sprint["title"] == "Sprint Two"
        )
        self.assertEqual(sprint_one["status"], "archived")
        self.assertEqual(sprint_two["status"], "current")
        self.assertEqual(sprint_one["source_filename"], "delta_sprint_01.json")
        self.assertEqual(sprint_one["code_patch_count"], 1)
        self.assertEqual(sprint_one["code_patch_error_count"], 0)
        self.assertEqual(len(patch_calls), 1, patch_calls)

        download_status, archive = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/sprints/{sprint_one['id']}/download"
        )
        self.assertEqual(download_status, 200)
        self.assertIsInstance(archive, dict)
        assert isinstance(archive, dict)
        self.assertEqual(archive["archive_type"], "nginx-qa-project-sprint")
        self.assertEqual(archive["import_payload"]["sprint"]["id"], "sprint-one")
        self.assertEqual(archive["code_history"]["patch_count"], 1)
        patch_entries = [
            item
            for item in archive["code_history"]["timeline"]
            if item["type"] == "patch"
        ]
        self.assertEqual(len(patch_entries), 1)
        self.assertEqual(patch_entries[0]["from_commit"]["full"], "a" * 40)
        self.assertEqual(patch_entries[0]["to_commit"]["full"], "b" * 40)
        self.assertIn("diff --git", patch_entries[0]["patch"])
        self.assertTrue(
            any(
                record.get("message") == "Finish sprint one."
                for record in archive["runtime_state"]["recent_activity"]
            )
        )

    async def test_sprint_history_is_isolated_by_project(self) -> None:
        second_context = "github.com/example/second-project"
        second_phone = "9009"
        config = main.read_git_config_file()
        second_project = {
            "project_name": "Second Project",
            "git_address": "https://github.com/example/second-project.git",
            "git_context_key": second_context,
            "project_phone": second_phone,
            "groups": [],
            "group_relationships": [],
            "customer_reporting": {},
        }
        config[main.PROJECTS_KEY][second_context] = second_project
        config[main.PHONE_GIT_CONTEXTS_KEY][second_phone] = {
            **second_project,
            "phone": second_phone,
        }
        main.write_git_config_file(config)

        for project_phone, git_address, title, agent_id, agent_name, agent_phone in (
            (
                self.PROJECT_PHONE,
                "https://github.com/example/actor-import.git",
                "Primary Sprint",
                "primary-sprint-agent",
                "Primary Sprint Agent",
                "2021",
            ),
            (
                second_phone,
                second_project["git_address"],
                "Second Sprint",
                "second-sprint-agent",
                "Second Sprint Agent",
                "2022",
            ),
        ):
            status_code, _ = await asgi_request(
                f"/api/v1/projects/{project_phone}/agents/import",
                method="POST",
                payload={
                    "project_id": project_phone,
                    "git_address": git_address,
                    "sprint": {"title": title},
                    "agents": {
                        "overwrite": True,
                        "items": [
                            {
                                "id": agent_id,
                                "name": agent_name,
                                "phone": agent_phone,
                                "tasks": [{"message": f"Task for {title}"}],
                            }
                        ],
                    },
                },
            )
            self.assertEqual(status_code, 201)

        primary_status, primary = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/sprints"
        )
        second_status, second = await asgi_request(
            f"/api/v1/projects/{second_phone}/sprints"
        )
        self.assertEqual(primary_status, 200)
        self.assertEqual(second_status, 200)
        self.assertIsInstance(primary, dict)
        self.assertIsInstance(second, dict)
        assert isinstance(primary, dict) and isinstance(second, dict)
        primary_titles = {sprint["title"] for sprint in primary["sprints"]}
        second_titles = {sprint["title"] for sprint in second["sprints"]}
        self.assertIn("Primary Sprint", primary_titles)
        self.assertNotIn("Second Sprint", primary_titles)
        self.assertEqual(second_titles, {"Second Sprint"})

    async def test_parallel_project_queues_and_cleanup_are_isolated(self) -> None:
        second_context = "github.com/example/second-project"
        second_phone = "9009"
        second_project = {
            "project_name": "Second Project",
            "git_address": "https://github.com/example/second-project.git",
            "git_context_key": second_context,
            "project_phone": second_phone,
            "groups": [],
            "group_relationships": [],
            "customer_reporting": {},
        }
        config = main.read_git_config_file()
        config[main.PROJECTS_KEY][second_context] = second_project
        config[main.PHONE_GIT_CONTEXTS_KEY][second_phone] = {
            **second_project,
            "phone": second_phone,
        }
        main.write_git_config_file(config)

        async def post_message(project_phone: str, message: str) -> tuple[int, object]:
            return await asgi_request(
                f"/worker/all/{project_phone}",
                method="POST",
                payload={
                    "message": message,
                    "sender": "Project Manager",
                    "receiver": "Shared logical recipient",
                    "from_phone": main.PROJECT_MANAGER_PHONE,
                    "to_phone": "2999",
                },
            )

        post_results = await asyncio.gather(
            post_message(self.PROJECT_PHONE, "Primary project message."),
            post_message(second_phone, "Second project message."),
        )
        self.assertEqual([result[0] for result in post_results], [201, 201])
        self.assertEqual(len(main.queues["worker-all"]), 2)

        primary_result, second_result = await asyncio.gather(
            asgi_request(
                f"/worker/all/{self.PROJECT_PHONE}?to_phone=2999"
            ),
            asgi_request(f"/worker/all/{second_phone}?to_phone=2999"),
        )
        self.assertEqual(primary_result[0], 200)
        self.assertEqual(second_result[0], 200)
        self.assertEqual(primary_result[1]["message"], "Primary project message.")
        self.assertEqual(second_result[1]["message"], "Second project message.")
        self.assertEqual(len(main.queues["worker-all"]), 0)

        await asyncio.gather(
            post_message(self.PROJECT_PHONE, "Primary cleanup candidate."),
            post_message(second_phone, "Second cleanup survivor."),
        )
        removed = await main.remove_project_actor_queue_items(
            {
                "project_phone": self.PROJECT_PHONE,
                "git_context_key": self.PROJECT_CONTEXT,
                "git_address": "https://github.com/example/actor-import.git",
            },
            set(),
            {"2999"},
            action="test_project_scoped_cleanup",
        )
        self.assertEqual(len(removed), 1)
        self.assertEqual(len(main.queues["worker-all"]), 1)
        remaining = main.queues["worker-all"][0]
        self.assertEqual(
            main.queue_item_message(remaining),
            "Second cleanup survivor.",
        )
        self.assertEqual(
            main.queue_item_metadata(remaining)["git_context_key"],
            second_context,
        )

    async def test_sequential_projects_dequeue_their_own_tasks_concurrently(self) -> None:
        second_context = "github.com/example/second-project"
        second_phone = "9009"
        second_project = {
            "project_name": "Second Project",
            "git_address": "https://github.com/example/second-project.git",
            "git_context_key": second_context,
            "project_phone": second_phone,
            "groups": [],
            "group_relationships": [],
            "customer_reporting": {},
        }
        config = main.read_git_config_file()
        config[main.PROJECTS_KEY][second_context] = second_project
        config[main.PHONE_GIT_CONTEXTS_KEY][second_phone] = {
            **second_project,
            "phone": second_phone,
        }
        main.write_git_config_file(config)

        imports = await asyncio.gather(
            asgi_request(
                f"/api/v1/projects/{self.PROJECT_PHONE}/agents/import",
                method="POST",
                payload={
                    "project_id": self.PROJECT_PHONE,
                    "git_address": "https://github.com/example/actor-import.git",
                    "agents": {
                        "overwrite": True,
                        "assignment_mode": "sequential",
                        "items": [
                            {
                                "id": "primary-sequential-agent",
                                "name": "Primary Sequential Agent",
                                "phone": "2021",
                                "tasks": ["Primary sequential task."],
                            }
                        ],
                    },
                },
            ),
            asgi_request(
                f"/api/v1/projects/{second_phone}/agents/import",
                method="POST",
                payload={
                    "project_id": second_phone,
                    "git_address": second_project["git_address"],
                    "agents": {
                        "overwrite": True,
                        "assignment_mode": "sequential",
                        "items": [
                            {
                                "id": "second-sequential-agent",
                                "name": "Second Sequential Agent",
                                "phone": "2022",
                                "tasks": ["Second sequential task."],
                            }
                        ],
                    },
                },
            ),
        )
        self.assertEqual([result[0] for result in imports], [201, 201])
        self.assertEqual(len(main.queues["worker-all"]), 2)

        primary_identity, second_identity = await asyncio.gather(
            asgi_request(
                "/api/v1/agents/whoami/repository",
                method="POST",
                payload={
                    "git_address": "https://github.com/example/actor-import.git"
                },
            ),
            asgi_request(
                "/api/v1/agents/whoami/repository",
                method="POST",
                payload={"git_address": second_project["git_address"]},
            ),
        )
        self.assertEqual(primary_identity[0], 200)
        self.assertEqual(second_identity[0], 200)
        self.assertEqual(
            primary_identity[1]["agent"]["id"],
            "primary-sequential-agent",
        )
        self.assertEqual(
            second_identity[1]["agent"]["id"],
            "second-sequential-agent",
        )
        self.assertEqual(primary_identity[1]["project_phone"], self.PROJECT_PHONE)
        self.assertEqual(second_identity[1]["project_phone"], second_phone)
        self.assertIn(
            "Primary sequential task.",
            primary_identity[1]["active_task"]["message"],
        )
        self.assertIn(
            "Second sequential task.",
            second_identity[1]["active_task"]["message"],
        )
        self.assertEqual(len(main.queues["worker-all"]), 0)

    async def test_import_route_rejects_json_for_another_project(self) -> None:
        status_code, body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/import",
            method="POST",
            payload={
                "project_id": "9009",
                "agents": {
                    "overwrite": True,
                    "items": [{"name": "Wrong Project Agent"}],
                },
            },
        )
        self.assertEqual(status_code, 409)
        self.assertIsInstance(body, dict)
        assert isinstance(body, dict)
        self.assertEqual(body["detail"]["error"], "project_reference_mismatch")
        self.assertFalse(main.sprint_history_path.exists())
        self.assertIn("old-project-actor", {agent["id"] for agent in main.read_agents_file()})

    async def test_import_route_requires_a_json_project_reference(self) -> None:
        agents_before = main.agents_path.read_text(encoding="utf-8")
        status_code, body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/import",
            method="POST",
            payload={
                "agents": {
                    "overwrite": True,
                    "items": [{"name": "Unscoped Agent"}],
                },
            },
        )

        self.assertEqual(status_code, 400)
        self.assertIsInstance(body, dict)
        assert isinstance(body, dict)
        self.assertEqual(body["detail"]["error"], "project_reference_required")
        self.assertEqual(main.agents_path.read_text(encoding="utf-8"), agents_before)
        self.assertFalse(main.sprint_history_path.exists())
        self.assertTrue(all(not queue for queue in main.queues.values()))

    async def test_import_route_rejects_mixed_git_project_references_atomically(self) -> None:
        agents_before = main.agents_path.read_text(encoding="utf-8")
        queued = main.make_queue_item(
            "Existing Delta task.",
            {
                "conversation_phone": self.PROJECT_PHONE,
                "from_phone": main.PROJECT_MANAGER_PHONE,
                "to_phone": "2010",
                "to_agent_id": "old-project-actor",
                "phone_channel": "worker-all",
                "git_context_key": self.PROJECT_CONTEXT,
                "git_address": "https://github.com/example/actor-import.git",
            },
        )
        main.queues["worker-all"].append(queued)

        status_code, body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/import",
            method="POST",
            payload={
                "project_id": self.PROJECT_PHONE,
                "git_address": "https://github.com/example/actor-import.git",
                "agents": {
                    "git_address": "https://github.com/example/other-project.git",
                    "overwrite": True,
                    "items": [{"name": "Wrong Repository Agent"}],
                },
            },
        )

        self.assertEqual(status_code, 409)
        self.assertIsInstance(body, dict)
        assert isinstance(body, dict)
        self.assertEqual(body["detail"]["error"], "project_reference_mismatch")
        self.assertEqual(body["detail"]["json_field"], "agents.git_address")
        self.assertEqual(main.agents_path.read_text(encoding="utf-8"), agents_before)
        self.assertEqual(len(main.queues["worker-all"]), 1)
        self.assertEqual(
            main.queue_item_message(main.queues["worker-all"][0]),
            "Existing Delta task.",
        )
        self.assertFalse(main.sprint_history_path.exists())

    async def test_delete_all_removes_project_actors_and_pending_tasks_only(self) -> None:
        import_status, _ = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/actors/import",
            method="POST",
            payload={
                "project_id": self.PROJECT_PHONE,
                "git_address": "https://github.com/example/actor-import.git",
                "actors": {
                    "overwrite": True,
                    "items": [
                        {
                            "name": "Temporary Actor",
                            "phone": "2025",
                            "tasks": ["Temporary task"],
                        }
                    ],
                }
            },
        )
        self.assertEqual(import_status, 201)

        delete_status, delete_body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/actors?include_managed=true",
            method="DELETE",
        )
        self.assertEqual(delete_status, 200)
        self.assertIsInstance(delete_body, dict)
        assert isinstance(delete_body, dict)
        self.assertEqual(delete_body["deleted_actor_count"], 1)
        self.assertEqual(delete_body["removed_task_count"], 1)
        self.assertEqual(len(main.queues["worker-all"]), 0)
        self.assertEqual(
            {agent["id"] for agent in main.read_agents_file()},
            {main.PROJECT_MANAGER_AGENT_ID, "outside-actor"},
        )

        get_status, get_body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/actors"
        )
        self.assertEqual(get_status, 200)
        self.assertIsInstance(get_body, dict)
        assert isinstance(get_body, dict)
        self.assertEqual(get_body["actor_count"], 0)

    async def test_delete_all_can_archive_groups_and_remove_managed_actors(self) -> None:
        config = json.loads(main.git_config_path.read_text(encoding="utf-8"))
        project = config[main.PROJECTS_KEY][self.PROJECT_CONTEXT]
        project["groups"] = [
            {
                "group_id": "group-managed-test",
                "group_key": "managed-test",
                "template_id": "backend_dev_team_v1",
                "status": "active",
                "revision": 1,
                "agents": [
                    {
                        "role": "backend_developer",
                        "agent_id": "managed-actor",
                        "agent_phone": "4022",
                    }
                ],
            }
        ]
        config[main.PHONE_GIT_CONTEXTS_KEY]["4022"] = {
            "project_name": project["project_name"],
            "git_address": project["git_address"],
            "git_context_key": self.PROJECT_CONTEXT,
            "phone": "4022",
            "project_phone": self.PROJECT_PHONE,
            "managed_by": "group_api",
            "agent_id": "managed-actor",
        }
        main.git_config_path.write_text(
            json.dumps(config, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        stored = json.loads(main.agents_path.read_text(encoding="utf-8"))["agents"]
        stored.append(
            {
                "id": "managed-actor",
                "name": "Managed Actor",
                "phone": "4022",
                "profile": "Managed by the group API",
                "parameters": {
                    "managed_by": "group_api",
                    "group_ids": "group-managed-test",
                    "git_context_key": self.PROJECT_CONTEXT,
                    "project_phone": self.PROJECT_PHONE,
                },
            }
        )
        main.agents_path.write_text(
            json.dumps({"agents": stored}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        delete_status, delete_body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/actors?include_managed=true",
            method="DELETE",
        )
        self.assertEqual(delete_status, 200)
        self.assertIsInstance(delete_body, dict)
        assert isinstance(delete_body, dict)
        self.assertEqual(delete_body["deleted_actor_count"], 2)
        updated_config = json.loads(main.git_config_path.read_text(encoding="utf-8"))
        self.assertNotIn("4022", updated_config[main.PHONE_GIT_CONTEXTS_KEY])
        updated_group = updated_config[main.PROJECTS_KEY][self.PROJECT_CONTEXT]["groups"][0]
        self.assertEqual(updated_group["status"], "archived")
        self.assertEqual(updated_group["revision"], 2)
        self.assertNotIn(
            "managed-actor",
            {agent["id"] for agent in main.read_agents_file()},
        )

    async def test_invalid_import_is_atomic(self) -> None:
        agents_before = main.agents_path.read_text(encoding="utf-8")
        status_code, body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/actors/import",
            method="POST",
            payload={
                "project_id": self.PROJECT_PHONE,
                "git_address": "https://github.com/example/actor-import.git",
                "actors": {
                    "overwrite": True,
                    "items": [
                        {
                            "name": "Broken Actor",
                            "tasks": [
                                {"queue": "unknown", "message": "Do not queue"}
                            ],
                        }
                    ],
                }
            },
        )
        self.assertEqual(status_code, 400)
        self.assertIsInstance(body, dict)
        self.assertEqual(main.agents_path.read_text(encoding="utf-8"), agents_before)
        self.assertTrue(all(not queue for queue in main.queues.values()))

    async def test_agent_profiles_include_project_communication_and_git_branches(self) -> None:
        payload = {
            "project_id": self.PROJECT_PHONE,
            "git_address": "https://github.com/example/actor-import.git",
            "agents": {
                "overwrite": True,
                "items": [
                    {
                        "id": "programmer-a",
                        "name": "Programmer A",
                        "phone": "2101",
                        "git_branch": "agent/programmer-a-contracts",
                        "profile": "Implement API contracts.",
                        "tasks": [],
                    },
                    {
                        "id": "programmer-b",
                        "name": "Programmer B",
                        "phone": "2102",
                        "profile": "Implement the controller.",
                        "tasks": [],
                    },
                ],
            }
        }
        status_code, body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/import",
            method="POST",
            payload=payload,
        )

        self.assertEqual(status_code, 201)
        self.assertIsInstance(body, dict)
        assert isinstance(body, dict)
        imported = {agent["phone"]: agent for agent in body["imported_agents"]}
        first = imported["2101"]
        second = imported["2102"]

        self.assertEqual(first["git_branch"], "agent/programmer-a-contracts")
        self.assertEqual(second["git_branch"], "agent/programmer-b")
        self.assertEqual(
            first["parameters"]["git_branch"],
            "agent/programmer-a-contracts",
        )
        self.assertEqual(
            first["parameters"]["profile_endpoint"],
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/2101",
        )
        self.assertTrue(first["profile"].startswith("Implement API contracts."))
        self.assertEqual(
            first["profile"].count(main.AGENT_COMMUNICATION_BLOCK_START),
            1,
        )
        for expected in (
            f"GET /worker/all/{self.PROJECT_PHONE}?to_phone=2101",
            f"POST /worker/all/{self.PROJECT_PHONE}",
            "Programmer B: phone=2102, id=programmer-b, git_branch=agent/programmer-b",
            "Работайте, коммитьте и отправляйте изменения только в эту ветку.",
        ):
            self.assertIn(expected, first["profile"])
        self.assertIn("Programmer A: phone=2101", second["profile"])

        profile_status, profile_body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/2101"
        )
        self.assertEqual(profile_status, 200)
        self.assertIsInstance(profile_body, dict)
        assert isinstance(profile_body, dict)
        self.assertEqual(profile_body["agent"]["id"], "programmer-a")
        self.assertEqual(profile_body["git_branch"], "agent/programmer-a-contracts")
        self.assertEqual(profile_body["profile"], first["profile"])

        send_status, _ = await asgi_request(
            f"/worker/all/{self.PROJECT_PHONE}",
            method="POST",
            payload={
                "from_phone": "2101",
                "to_phone": "2102",
                "sender": "Programmer A",
                "receiver": "Programmer B",
                "message": "The API contract is ready.",
            },
        )
        self.assertEqual(send_status, 201)
        receive_status, receive_body = await asgi_request(
            f"/worker/all/{self.PROJECT_PHONE}?to_phone=2102"
        )
        self.assertEqual(receive_status, 200)
        self.assertIsInstance(receive_body, dict)
        assert isinstance(receive_body, dict)
        self.assertEqual(receive_body["from_phone"], "2101")
        self.assertEqual(receive_body["message"], "The API contract is ready.")

        repeated_payload = json.loads(json.dumps(payload))
        repeated_payload["agents"]["items"][0]["profile"] = first["profile"]
        repeat_status, repeat_body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/import",
            method="POST",
            payload=repeated_payload,
        )
        self.assertEqual(repeat_status, 201)
        self.assertIsInstance(repeat_body, dict)
        assert isinstance(repeat_body, dict)
        repeated_first = next(
            agent
            for agent in repeat_body["imported_agents"]
            if agent["phone"] == "2101"
        )
        self.assertEqual(
            repeated_first["profile"].count(main.AGENT_COMMUNICATION_BLOCK_START),
            1,
        )

    async def test_duplicate_agent_git_branch_is_rejected(self) -> None:
        agents_before = main.agents_path.read_text(encoding="utf-8")
        status_code, body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/import",
            method="POST",
            payload={
                "project_id": self.PROJECT_PHONE,
                "git_address": "https://github.com/example/actor-import.git",
                "agents": {
                    "overwrite": True,
                    "items": [
                        {"name": "Agent One", "git_branch": "agent/shared"},
                        {"name": "Agent Two", "git_branch": "agent/shared"},
                    ],
                }
            },
        )
        self.assertEqual(status_code, 409)
        self.assertIsInstance(body, dict)
        self.assertEqual(main.agents_path.read_text(encoding="utf-8"), agents_before)

    async def test_whoami_marks_agent_alive_and_returns_full_work_history(self) -> None:
        import_status, import_body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/import",
            method="POST",
            payload={
                "project_id": self.PROJECT_PHONE,
                "git_address": "https://github.com/example/actor-import.git",
                "agents": {
                    "overwrite": True,
                    "items": [
                        {
                            "id": "dynamic-agent",
                            "name": "Dynamic Agent",
                            "phone": "2111",
                            "git_branch": "agent/dynamic-agent",
                            "profile": "Perform dynamically assigned work.",
                            "tasks": [
                                {
                                    "task_id": "DYN-1",
                                    "queue": "worker-all",
                                    "message": "Complete the dynamic task.",
                                }
                            ],
                        },
                        {
                            "id": "peer-agent",
                            "name": "Peer Agent",
                            "phone": "2112",
                            "git_branch": "agent/peer-agent",
                            "tasks": [],
                        },
                    ],
                }
            },
        )
        self.assertEqual(import_status, 201)
        self.assertIsInstance(import_body, dict)
        assert isinstance(import_body, dict)
        imported_agent = next(
            agent
            for agent in import_body["imported_agents"]
            if agent["id"] == "dynamic-agent"
        )
        self.assertIn(
            f"POST /api/v1/projects/{self.PROJECT_PHONE}/agents/2111/whoami",
            imported_agent["profile"],
        )
        self.assertEqual(
            imported_agent["parameters"]["whoami_endpoint"],
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/2111/whoami",
        )
        self.assertTrue(imported_agent["parameters"]["created_at"])

        whoami_status, whoami_body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/2111/whoami",
            method="POST",
            payload={"message": "Кто я?"},
        )
        self.assertEqual(whoami_status, 200)
        self.assertIsInstance(whoami_body, dict)
        assert isinstance(whoami_body, dict)
        self.assertIn("Dynamic Agent", whoami_body["answer"])
        self.assertIn("Вы отмечены как живой агент", whoami_body["answer"])
        self.assertEqual(whoami_body["agent"]["status"], "active")
        self.assertTrue(whoami_body["presence"]["is_alive"])
        self.assertEqual(whoami_body["presence"]["status"], "alive")
        self.assertEqual(whoami_body["presence"]["heartbeat_count"], 1)
        self.assertEqual(len(whoami_body["assigned_tasks"]), 1)
        self.assertEqual(whoami_body["work_summary"]["assigned_task_count"], 1)
        self.assertGreaterEqual(whoami_body["work_summary"]["history_event_count"], 2)
        self.assertIn(
            "queued_to_worker_all",
            whoami_body["work_summary"]["event_counts"],
        )
        self.assertIn(
            "agent_identity_heartbeat",
            whoami_body["work_summary"]["event_counts"],
        )
        self.assertEqual(
            whoami_body["work_history"][0]["metadata"]["task_id"],
            "DYN-1",
        )
        self.assertEqual(
            {agent["id"] for agent in whoami_body["project_agents"]},
            {"dynamic-agent", "peer-agent"},
        )

        stored_agent = next(
            agent
            for agent in main.read_agents_file()
            if agent["id"] == "dynamic-agent"
        )
        self.assertEqual(stored_agent["parameters"]["presence_status"], "alive")
        self.assertEqual(stored_agent["parameters"]["heartbeat_count"], "1")
        first_seen_at = stored_agent["parameters"]["first_seen_at"]

        second_status, second_body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/2111/whoami",
            method="POST",
            payload={"message": "Who am I?"},
        )
        self.assertEqual(second_status, 200)
        self.assertIsInstance(second_body, dict)
        assert isinstance(second_body, dict)
        self.assertEqual(second_body["presence"]["heartbeat_count"], 2)
        self.assertEqual(
            second_body["agent"]["parameters"]["first_seen_at"],
            first_seen_at,
        )
        self.assertGreater(
            second_body["work_summary"]["history_event_count"],
            whoami_body["work_summary"]["history_event_count"],
        )

        list_status, list_body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents"
        )
        self.assertEqual(list_status, 200)
        self.assertIsInstance(list_body, dict)
        assert isinstance(list_body, dict)
        listed_agent = next(
            agent for agent in list_body["agents"] if agent["id"] == "dynamic-agent"
        )
        self.assertTrue(listed_agent["presence"]["is_alive"])

        unknown_status, _ = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/2999/whoami",
            method="POST",
            payload={"message": "Кто я?"},
        )
        self.assertEqual(unknown_status, 404)

    async def test_whoami_reminds_after_one_hour_without_outgoing_messages(self) -> None:
        import_status, _ = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/import",
            method="POST",
            payload={
                "project_id": self.PROJECT_PHONE,
                "agents": {
                    "overwrite": True,
                    "items": [
                        {
                            "id": "quiet-agent",
                            "name": "Quiet Agent",
                            "phone": "2188",
                            "git_branch": "agent/quiet",
                            "tasks": [],
                        },
                        {
                            "id": "quiet-peer",
                            "name": "Quiet Peer",
                            "phone": "2189",
                            "git_branch": "agent/quiet-peer",
                            "tasks": [],
                        },
                    ],
                },
            },
        )
        self.assertEqual(import_status, 201)
        agents = main.read_agents_file()
        quiet_agent = next(agent for agent in agents if agent["id"] == "quiet-agent")
        quiet_agent["parameters"]["created_at"] = "2020-01-01T00:00:00+00:00"
        main.write_agents_file(agents)

        quiet_status, quiet_body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/2188/whoami",
            method="POST",
            payload={"message": "Кто я?"},
        )
        self.assertEqual(quiet_status, 200)
        self.assertIsInstance(quiet_body, dict)
        assert isinstance(quiet_body, dict)
        self.assertTrue(quiet_body["communication_reminder"]["required"])
        self.assertIn("более часа не было исходящих сообщений", quiet_body["answer"])

        await main.append_history(
            "queued_to_worker_all",
            "worker-all",
            "Свежий отчёт о прогрессе.",
            {
                "sender": "Quiet Agent",
                "receiver": "Quiet Peer",
                "from_phone": "2188",
                "to_phone": "2189",
                "from_agent_id": "quiet-agent",
                "to_agent_id": "quiet-peer",
                "project_phone": self.PROJECT_PHONE,
            },
            8025,
            {
                "project_phone": self.PROJECT_PHONE,
                "git_context_key": self.PROJECT_CONTEXT,
                "git_address": "https://github.com/example/actor-import.git",
            },
        )
        active_status, active_body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/2188/whoami",
            method="POST",
            payload={"message": "Кто я?"},
        )
        self.assertEqual(active_status, 200)
        self.assertIsInstance(active_body, dict)
        assert isinstance(active_body, dict)
        self.assertFalse(active_body["communication_reminder"]["required"])
        self.assertNotIn("более часа не было исходящих сообщений", active_body["answer"])

    async def test_project_history_forwarding_setting_is_persistent_and_isolated(self) -> None:
        os.environ["TELEGRAM_BOT_TOKEN"] = "test-token"
        os.environ["TELEGRAM_HISTORY_CHAT_ID"] = "-100555"

        initial_status, initial_body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/telegram-history-forwarding"
        )
        self.assertEqual(initial_status, 200)
        self.assertIsInstance(initial_body, dict)
        assert isinstance(initial_body, dict)
        self.assertFalse(initial_body["enabled"])
        self.assertTrue(initial_body["destination_configured"])

        enabled_status, enabled_body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/telegram-history-forwarding",
            method="PUT",
            payload={"enabled": True},
        )
        self.assertEqual(enabled_status, 200)
        self.assertIsInstance(enabled_body, dict)
        assert isinstance(enabled_body, dict)
        self.assertTrue(enabled_body["enabled"])

        config = main.read_git_config_file()
        second_context = "github.com/example/other-project"
        second_project = {
            "project_name": "Other Project",
            "git_address": "https://github.com/example/other-project.git",
            "git_context_key": second_context,
            "project_phone": "9009",
            "groups": [],
        }
        config[main.PROJECTS_KEY][second_context] = second_project
        config[main.PHONE_GIT_CONTEXTS_KEY]["9009"] = {
            **second_project,
            "phone": "9009",
        }
        main.write_git_config_file(config)

        telegram_calls: list[tuple[str, str, dict[str, object]]] = []

        def fake_telegram_api(
            token: str,
            method: str,
            data: dict[str, object],
        ) -> dict[str, object]:
            telegram_calls.append((token, method, data))
            return {"ok": True}

        with patch.object(main, "telegram_api_json", side_effect=fake_telegram_api):
            await main.append_history(
                "queued_to_worker_all",
                "worker-all",
                "Сообщение проекта Actor Import.",
                {
                    "sender": "Agent A",
                    "receiver": "Agent B",
                    "project_phone": self.PROJECT_PHONE,
                },
                8025,
                {
                    "project_phone": self.PROJECT_PHONE,
                    "git_context_key": self.PROJECT_CONTEXT,
                },
            )
            await main.append_history(
                "queued_to_worker_all",
                "worker-all",
                "Сообщение другого проекта.",
                {
                    "sender": "Other A",
                    "receiver": "Other B",
                    "project_phone": "9009",
                },
                8025,
                {
                    "project_phone": "9009",
                    "git_context_key": second_context,
                },
            )

        self.assertEqual(len(telegram_calls), 1)
        token, method, data = telegram_calls[0]
        self.assertEqual(token, "test-token")
        self.assertEqual(method, "sendMessage")
        self.assertEqual(data["chat_id"], "-100555")
        self.assertIn("Actor Import Project", str(data["text"]))
        self.assertIn("Сообщение проекта Actor Import", str(data["text"]))

        stored = main.read_git_config_file()
        self.assertTrue(
            stored[main.PROJECTS_KEY][self.PROJECT_CONTEXT][
                main.TELEGRAM_HISTORY_FORWARDING_KEY
            ]["enabled"]
        )
        self.assertNotIn(
            main.TELEGRAM_HISTORY_FORWARDING_KEY,
            stored[main.PROJECTS_KEY][second_context],
        )

    async def test_sequential_runtime_changes_identity_from_queue_graph(self) -> None:
        import_status, import_body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/import",
            method="POST",
            payload={
                "project_id": self.PROJECT_PHONE,
                "git_address": "https://github.com/example/actor-import.git",
                "agents": {
                    "overwrite": True,
                    "assignment_mode": "sequential",
                    "items": [
                        {
                            "id": "sequential-role-a",
                            "name": "Sequential Role A",
                            "phone": "2121",
                            "git_branch": "agent/sequential-a",
                            "profile": "Analyze the first graph node.",
                            "tasks": [
                                {
                                    "task_id": "SEQ-A",
                                    "queue": "worker-all",
                                    "message": "Complete sequential task A.",
                                }
                            ],
                        },
                        {
                            "id": "sequential-role-b",
                            "name": "Sequential Role B",
                            "phone": "2122",
                            "git_branch": "agent/sequential-b",
                            "profile": "Implement the task received from role A.",
                            "tasks": [],
                        },
                    ],
                }
            },
        )
        self.assertEqual(import_status, 201)
        self.assertIsInstance(import_body, dict)
        assert isinstance(import_body, dict)
        self.assertEqual(import_body["assignment_mode"], "sequential")
        self.assertEqual(import_body["assignment"]["strategy"], "queue_graph")
        self.assertEqual(import_body["queued_task_count"], 1)
        self.assertEqual(import_body["queued_queue_item_count"], 1)
        self.assertEqual(len(main.queues["worker-all"]), 1)

        question_status, question_body = await asgi_request(
            "/api/v1/agents/whoami",
            method="POST",
            payload={"message": "Кто я?"},
        )
        self.assertEqual(question_status, 200)
        self.assertIsInstance(question_body, dict)
        assert isinstance(question_body, dict)
        self.assertEqual(question_body["status"], "repository_required")
        self.assertEqual(
            question_body["reply_url"],
            "http://testserver:8025/api/v1/agents/whoami/repository",
        )

        first_status, first_body = await asgi_request(
            "/api/v1/agents/whoami/repository",
            method="POST",
            payload={
                "git_address": "git@github.com:example/actor-import.git",
            },
        )
        self.assertEqual(first_status, 200)
        self.assertIsInstance(first_body, dict)
        assert isinstance(first_body, dict)
        self.assertEqual(first_body["assignment_strategy"], "queue_graph")
        self.assertEqual(first_body["agent"]["id"], "sequential-role-a")
        self.assertEqual(first_body["git_branch"], "agent/sequential-a")
        self.assertEqual(first_body["graph_position"]["queue"], "worker-all")
        first_agent_prompt = (
            self.temp_path
            / "agent-latest"
            / "actor-import_2121-latest.prompt"
        )
        self.assertEqual(
            first_body["latest_agent_prompt_file_path"],
            str(first_agent_prompt),
        )
        self.assertEqual(
            json.loads(first_agent_prompt.read_text(encoding="utf-8")),
            first_body,
        )
        self.assertEqual(len(first_body["team"]), 2)
        self.assertIn("send_endpoints", first_body["communication"])
        self.assertFalse(first_body["identity_reused"])
        self.assertFalse(first_body["communication_reminder"]["required"])
        self.assertTrue(first_body["execution_authorized"])
        self.assertFalse(first_body["requires_additional_confirmation"])
        self.assertEqual(
            first_body["next_identity_request"]["url"],
            "http://testserver:8025/api/v1/agents/whoami",
        )
        self.assertTrue(first_body["next_identity_request"]["follow_reply_url"])
        self.assertEqual(
            first_body["next_identity_request"]["reply"]["url"],
            "http://testserver:8025/api/v1/agents/whoami/repository",
        )
        self.assertIn("activity_with_patches", first_body["project_state"])
        self.assertGreaterEqual(
            first_body["project_state"]["code_patch_summary"]["activity_count"],
            1,
        )
        self.assertTrue(
            any(
                entry.get("type") == "activity"
                and "Complete sequential task A"
                in str(entry.get("activity", {}).get("message") or "")
                for entry in first_body["project_state"]["activity_with_patches"]
            )
        )
        self.assertIn(
            "project_state.activity_with_patches",
            first_body["communication"]["instructions"],
        )
        self.assertEqual(len(main.queues["worker-all"]), 0)

        config = main.read_git_config_file()
        config[main.PROJECTS_KEY][self.PROJECT_CONTEXT][
            main.PROJECT_AGENT_ASSIGNMENT_KEY
        ]["current_started_at"] = "2020-01-01T00:00:00+00:00"
        main.write_git_config_file(config)

        repeated_first_status, repeated_first_body = await asgi_request(
            "/api/v1/agents/whoami/repository",
            method="POST",
            payload={
                "git_address": "https://github.com/example/actor-import.git",
            },
        )
        self.assertEqual(repeated_first_status, 200)
        self.assertIsInstance(repeated_first_body, dict)
        assert isinstance(repeated_first_body, dict)
        self.assertTrue(repeated_first_body["identity_reused"])
        self.assertTrue(repeated_first_body["communication_reminder"]["required"])
        self.assertIn(
            "более часа не было исходящих сообщений",
            repeated_first_body["answer"],
        )
        self.assertEqual(
            repeated_first_body["active_task"]["id"],
            first_body["active_task"]["id"],
        )
        self.assertEqual(
            repeated_first_body["agent"]["id"],
            "sequential-role-a",
        )
        self.assertEqual(len(main.queues["worker-all"]), 0)

        handoff_status, _ = await asgi_request(
            f"/tester/all/{self.PROJECT_PHONE}",
            method="POST",
            payload={
                "from_phone": "2121",
                "to_phone": "2122",
                "sender": "Sequential Role A",
                "receiver": "Sequential Role B",
                "message": "Implement the graph handoff from role A.",
            },
        )
        self.assertEqual(handoff_status, 201)

        second_status, second_body = await asgi_request(
            "/api/v1/agents/whoami/repository",
            method="POST",
            payload={
                "git_address": "https://github.com/example/actor-import.git",
            },
        )
        self.assertEqual(second_status, 200)
        self.assertIsInstance(second_body, dict)
        assert isinstance(second_body, dict)
        self.assertEqual(second_body["agent"]["id"], "sequential-role-b")
        self.assertEqual(
            second_body["active_task"]["message"],
            "Implement the graph handoff from role A.",
        )
        self.assertEqual(second_body["graph_position"]["queue"], "tester-all")

        project_status, project_body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents"
        )
        self.assertEqual(project_status, 200)
        self.assertIsInstance(project_body, dict)
        assert isinstance(project_body, dict)
        self.assertEqual(
            project_body["assignment"]["current_agent_id"],
            "sequential-role-b",
        )
        self.assertEqual(
            project_body["assignment"]["strategy"],
            "queue_graph",
        )

        fixed_phone_status, fixed_phone_body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/2122/whoami",
            method="POST",
            payload={"message": "Кто я?"},
        )
        self.assertEqual(fixed_phone_status, 409)
        self.assertIsInstance(fixed_phone_body, dict)
        assert isinstance(fixed_phone_body, dict)
        self.assertEqual(
            fixed_phone_body["detail"]["error"],
            "use_sequential_queue_graph_identity",
        )

        repeated_second_status, repeated_second_body = await asgi_request(
            "/api/v1/agents/whoami/repository",
            method="POST",
            payload={
                "git_address": "https://github.com/example/actor-import.git",
            },
        )
        self.assertEqual(repeated_second_status, 200)
        self.assertIsInstance(repeated_second_body, dict)
        assert isinstance(repeated_second_body, dict)
        self.assertTrue(repeated_second_body["identity_reused"])
        self.assertEqual(
            repeated_second_body["active_task"]["id"],
            second_body["active_task"]["id"],
        )
        self.assertEqual(
            repeated_second_body["agent"]["id"],
            "sequential-role-b",
        )

    def test_sequential_history_inserts_each_commit_patch_once(self) -> None:
        commits = {
            "one": "1" * 40,
            "two": "2" * 40,
            "three": "3" * 40,
        }
        records = [
            {
                "id": f"record-{index}",
                "timestamp": f"2026-09-18T10:0{index}:00+00:00",
                "event": "message",
                "queue": "worker-all",
                "message": message,
                "metadata": {
                    "project_name": "Patch Project",
                    "git_commit": commits[commit_name],
                    "git_commit_short": commits[commit_name][:12],
                    "git_branch": f"agent/{commit_name}",
                    "sender": "Agent A",
                    "receiver": "Agent B",
                },
            }
            for index, (commit_name, message) in enumerate(
                [
                    ("one", "Message at commit one."),
                    ("one", "Second message at commit one."),
                    ("two", "First message at commit two."),
                    ("two", "Second message at commit two."),
                    ("three", "Message at commit three."),
                ]
            )
        ]
        patch_calls: list[tuple[str, str]] = []

        def fake_resolve_git_patch(
            git_address: str,
            to_commit: str,
            from_commit: str | None = None,
        ) -> dict[str, str]:
            self.assertEqual(
                git_address,
                "https://github.com/example/actor-import.git",
            )
            assert from_commit is not None
            patch_calls.append((from_commit, to_commit))
            return {
                "source": "test",
                "patch": f"diff {from_commit[:4]}..{to_commit[:4]}",
            }

        with patch.object(main, "resolve_git_patch", fake_resolve_git_patch):
            result = main.history_with_patches_context(
                records,
                "https://github.com/example/actor-import.git",
            )

        self.assertEqual(
            patch_calls,
            [
                (commits["one"], commits["two"]),
                (commits["two"], commits["three"]),
            ],
        )
        self.assertEqual(result["record_count"], 5)
        self.assertEqual(result["patch_count"], 2)
        self.assertEqual(result["patch_error_count"], 0)
        self.assertIn("From branch: agent/one", result["text"])
        self.assertIn("To branch: agent/two", result["text"])
        self.assertEqual(result["patches"][0]["from_branch"], "agent/one")
        self.assertEqual(result["patches"][0]["to_branch"], "agent/two")
        self.assertEqual(
            [entry["type"] for entry in result["timeline"]],
            ["activity", "activity", "patch", "activity", "activity", "patch", "activity"],
        )
        self.assertLess(
            result["text"].index("diff 1111..2222"),
            result["text"].index("First message at commit two."),
        )
        self.assertLess(
            result["text"].index("diff 2222..3333"),
            result["text"].index("Message at commit three."),
        )

    async def _test_legacy_sequential_whoami_moves_to_next_role_without_parallel_work(self) -> None:
        import_status, import_body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/import",
            method="POST",
            payload={
                "project_id": self.PROJECT_PHONE,
                "git_address": "https://github.com/example/actor-import.git",
                "agents": {
                    "overwrite": True,
                    "assignment_mode": "sequential",
                    "items": [
                        {
                            "id": "sequential-role-a",
                            "name": "Sequential Role A",
                            "phone": "2121",
                            "git_branch": "agent/sequential-a",
                            "profile": "Complete role A before role B starts.",
                            "tasks": [
                                {
                                    "task_id": "SEQ-A",
                                    "queue": "worker-all",
                                    "message": "Complete sequential task A.",
                                }
                            ],
                        },
                        {
                            "id": "sequential-role-b",
                            "name": "Sequential Role B",
                            "phone": "2122",
                            "git_branch": "agent/sequential-b",
                            "profile": "Start only after role A is complete.",
                            "tasks": [
                                {
                                    "task_id": "SEQ-B",
                                    "queue": "tester-all",
                                    "message": "Complete sequential task B.",
                                }
                            ],
                        },
                    ],
                }
            },
        )
        self.assertEqual(import_status, 201)
        self.assertIsInstance(import_body, dict)
        assert isinstance(import_body, dict)
        self.assertEqual(import_body["assignment_mode"], "sequential")
        self.assertEqual(import_body["queued_task_count"], 0)
        self.assertEqual(import_body["deferred_task_count"], 2)
        self.assertTrue(all(not queue for queue in main.queues.values()))
        self.assertIn(
            "одновременно активна только одна роль",
            import_body["imported_agents"][0]["profile"],
        )
        self.assertEqual(
            import_body["imported_agents"][0]["parameters"]["assignment_order"],
            "1",
        )

        first_status, first_body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/2121/whoami",
            method="POST",
            payload={"message": "Кто я?"},
        )
        self.assertEqual(first_status, 200)
        self.assertIsInstance(first_body, dict)
        assert isinstance(first_body, dict)
        self.assertEqual(first_body["assignment_mode"], "sequential")
        self.assertFalse(first_body["all_completed"])
        self.assertTrue(first_body["newly_assigned"])
        self.assertEqual(first_body["agent"]["id"], "sequential-role-a")
        self.assertEqual(first_body["assigned_tasks"][0]["task_id"], "SEQ-A")
        self.assertEqual(len(first_body["queued_tasks"]), 1)
        self.assertEqual(
            first_body["sequential_poll_endpoint"],
            f"/worker/all/{self.PROJECT_PHONE}?to_phone={self.PROJECT_PHONE}",
        )
        self.assertEqual(
            first_body["queued_tasks"][0]["delivery_phone"],
            self.PROJECT_PHONE,
        )
        self.assertEqual(len(main.queues["worker-all"]), 1)
        self.assertEqual(len(main.queues["tester-all"]), 0)

        heartbeat_status, heartbeat_body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/2121/whoami",
            method="POST",
            payload={"message": "Кто я?"},
        )
        self.assertEqual(heartbeat_status, 200)
        self.assertIsInstance(heartbeat_body, dict)
        assert isinstance(heartbeat_body, dict)
        self.assertFalse(heartbeat_body["newly_assigned"])
        self.assertEqual(heartbeat_body["agent"]["id"], "sequential-role-a")
        self.assertEqual(len(main.queues["worker-all"]), 1)

        parallel_status, parallel_body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/2122/whoami",
            method="POST",
            payload={"message": "Кто я?"},
        )
        self.assertEqual(parallel_status, 409)
        self.assertIsInstance(parallel_body, dict)
        assert isinstance(parallel_body, dict)
        self.assertEqual(
            parallel_body["detail"]["error"],
            "sequential_assignment_in_progress",
        )

        second_status, second_body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/2121/whoami",
            method="POST",
            payload={
                "message": "Задание выполнено. Кто я?",
                "completed": True,
            },
        )
        self.assertEqual(second_status, 200)
        self.assertIsInstance(second_body, dict)
        assert isinstance(second_body, dict)
        self.assertTrue(second_body["newly_assigned"])
        self.assertEqual(second_body["agent"]["id"], "sequential-role-b")
        self.assertEqual(second_body["completed_assignment"]["agent_id"], "sequential-role-a")
        self.assertEqual(
            second_body["next_whoami_endpoint"],
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/2122/whoami",
        )
        self.assertEqual(len(second_body["removed_pending_tasks"]), 1)
        self.assertEqual(len(main.queues["worker-all"]), 1)
        self.assertEqual(len(main.queues["tester-all"]), 0)
        next_node = main.queues["worker-all"][0]
        self.assertIn("Сейчас вы агент Sequential Role B", main.queue_item_message(next_node))
        self.assertEqual(
            main.queue_item_metadata(next_node)["graph_node_index"],
            2,
        )
        self.assertEqual(
            main.queue_item_metadata(next_node)["tasks"][0]["task_id"],
            "SEQ-B",
        )

        done_status, done_body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/2122/whoami",
            method="POST",
            payload={"message": "Role completed. Who am I?", "status": "DONE"},
        )
        self.assertEqual(done_status, 200)
        self.assertIsInstance(done_body, dict)
        assert isinstance(done_body, dict)
        self.assertTrue(done_body["all_completed"])
        self.assertIsNone(done_body["agent"])
        self.assertEqual(done_body["assignment"]["status"], "completed")
        self.assertEqual(len(done_body["completed_assignments"]), 2)
        self.assertTrue(all(not queue for queue in main.queues.values()))

        project_status, project_body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents"
        )
        self.assertEqual(project_status, 200)
        self.assertIsInstance(project_body, dict)
        assert isinstance(project_body, dict)
        self.assertEqual(project_body["assignment_mode"], "sequential")
        self.assertEqual(project_body["assignment"]["status"], "completed")

    async def test_explicit_sequential_graph_uses_common_identity_queue(self) -> None:
        import_status, import_body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/import",
            method="POST",
            payload={
                "project_id": self.PROJECT_PHONE,
                "git_address": "https://github.com/example/actor-import.git",
                "agents": {"overwrite": True},
                "execution": {
                    "mode": "sequential",
                    "start_node": "analysis",
                    "required_approvals": 2,
                    "max_rework_cycles": 2,
                    "reviewers": [
                        {
                            "id": "reviewer-one",
                            "name": "Reviewer One",
                            "phone": "2151",
                        },
                        {
                            "id": "reviewer-two",
                            "name": "Reviewer Two",
                            "phone": "2152",
                        },
                    ],
                },
                "nodes": [
                    {
                        "id": "analysis",
                        "agent": {
                            "id": "graph-analyst",
                            "name": "Graph Analyst",
                            "phone": "2153",
                            "git_branch": "agent/graph-analyst",
                            "profile": "Analyze the graph task.",
                        },
                        "tasks": ["Analyze the sprint request."],
                        "transitions": {"DONE": "finished"},
                    },
                    {
                        "id": "finished",
                        "type": "terminal",
                        "status": "DONE",
                        "message": "Sprint graph completed.",
                    },
                ],
            },
        )
        self.assertEqual(import_status, 201)
        self.assertIsInstance(import_body, dict)
        assert isinstance(import_body, dict)
        self.assertEqual(
            import_body["assignment"]["strategy"],
            "conditional_graph",
        )

        node_status, node_body = await asgi_request(
            "/api/v1/agents/whoami/repository",
            method="POST",
            payload={
                "git_address": "https://github.com/example/actor-import.git",
            },
        )
        self.assertEqual(node_status, 200)
        self.assertIsInstance(node_body, dict)
        assert isinstance(node_body, dict)
        self.assertEqual(node_body["agent"]["id"], "graph-analyst")
        graph_agent_prompt = (
            self.temp_path
            / "agent-latest"
            / "actor-import_2153-latest.prompt"
        )
        reviewer_one_prompt = (
            self.temp_path
            / "agent-latest"
            / "actor-import_2151-latest.prompt"
        )
        self.assertEqual(
            node_body["latest_agent_prompt_file_path"],
            str(graph_agent_prompt),
        )
        self.assertEqual(
            json.loads(graph_agent_prompt.read_text(encoding="utf-8")),
            node_body,
        )
        node_assignment_id = node_body["active_task"]["metadata"]["assignment_id"]

        from_commit = "a" * 40
        to_commit = "b" * 40
        patch_calls: list[tuple[str, str, str | None]] = []

        def fake_resolve_git_patch(
            git_address: str,
            target_commit: str,
            base_commit: str | None = None,
        ) -> dict[str, str]:
            patch_calls.append((git_address, target_commit, base_commit))
            return {
                "source": "test_git",
                "patch": "diff --git a/analysis.txt b/analysis.txt\n+finished",
            }

        with patch.object(main, "resolve_git_patch", fake_resolve_git_patch):
            first_review_status, first_review = await asgi_request(
                f"/api/v1/projects/{self.PROJECT_PHONE}/agents/2153/whoami",
                method="POST",
                payload={
                    "assignment_id": node_assignment_id,
                    "status": "DONE",
                    "result": "Analysis finished.",
                    "from_commit": from_commit,
                    "git_commit": to_commit,
                    "git_branch": "agent/graph-analyst",
                },
            )
        self.assertEqual(first_review_status, 200)
        self.assertIsInstance(first_review, dict)
        assert isinstance(first_review, dict)
        self.assertEqual(first_review["agent"]["id"], "reviewer-one")
        self.assertEqual(
            first_review["latest_agent_prompt_file_path"],
            str(graph_agent_prompt),
        )
        self.assertEqual(
            first_review["agent_prompt_file_paths"],
            {"2153": str(graph_agent_prompt)},
        )
        self.assertFalse(reviewer_one_prompt.exists())
        self.assertEqual(
            json.loads(graph_agent_prompt.read_text(encoding="utf-8")),
            first_review,
        )
        self.assertEqual(first_review["assignment"]["phase"], "review")
        self.assertTrue(first_review["identity_request_required"])
        self.assertEqual(
            first_review["next_identity_request"]["url"],
            "http://testserver:8025/api/v1/agents/whoami",
        )
        self.assertEqual(
            patch_calls,
            [
                (
                    "https://github.com/example/actor-import.git",
                    to_commit,
                    from_commit,
                )
            ],
        )
        review_context = first_review["assignment"]["pending_transition"][
            "review_context"
        ]
        self.assertEqual(review_context["patch_count"], 1)
        self.assertIn("diff --git", review_context["text"])
        self.assertEqual(
            review_context["submission"]["git_branch"],
            "agent/graph-analyst",
        )
        transition_patch = review_context["patches"][0]
        self.assertEqual(
            transition_patch["from_commit"]["branch"],
            "agent/graph-analyst",
        )
        self.assertEqual(
            transition_patch["to_commit"]["branch"],
            "agent/graph-analyst",
        )
        self.assertIn("From branch: agent/graph-analyst", review_context["text"])
        self.assertEqual(len(main.queues["worker-all"]), 1, first_review)
        self.assertIn(
            "diff --git a/analysis.txt b/analysis.txt",
            main.queue_item_message(main.queues["worker-all"][0]),
        )

        reviewer_status, reviewer_body = await asgi_request(
            "/api/v1/agents/whoami/repository",
            method="POST",
            payload={
                "git_address": "https://github.com/example/actor-import.git",
            },
        )
        self.assertEqual(reviewer_status, 200)
        self.assertIsInstance(reviewer_body, dict)
        assert isinstance(reviewer_body, dict)
        self.assertEqual(reviewer_body["agent"]["id"], "reviewer-one")
        self.assertEqual(
            reviewer_body["latest_agent_prompt_file_path"],
            str(reviewer_one_prompt),
        )
        self.assertEqual(
            json.loads(reviewer_one_prompt.read_text(encoding="utf-8")),
            reviewer_body,
        )
        self.assertEqual(
            reviewer_body["next_identity_request"]["url"],
            "http://testserver:8025/api/v1/agents/whoami",
        )
        self.assertTrue(
            reviewer_body["active_task"]["metadata"]["next_identity_request"][
                "required_for_every_graph_transition"
            ]
        )
        self.assertIn(
            "POST /api/v1/agents/whoami",
            reviewer_body["active_task"]["message"],
        )
        self.assertEqual(
            reviewer_body["active_task"]["message"].count(
                "[PATCH BETWEEN COMMITS]"
            ),
            1,
        )
        queued_context = reviewer_body["active_task"]["metadata"][
            "pending_transition"
        ]["review_context"]
        self.assertNotIn("text", queued_context)
        self.assertEqual(queued_context["patch_count"], 1)

        reviewer_one_status, reviewer_one_result = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/2151/whoami",
            method="POST",
            payload={
                "assignment_id": reviewer_body["active_task"]["metadata"][
                    "assignment_id"
                ],
                "status": "APPROVE",
            },
        )
        self.assertEqual(reviewer_one_status, 200)
        self.assertIsInstance(reviewer_one_result, dict)
        assert isinstance(reviewer_one_result, dict)
        self.assertTrue(reviewer_one_result["identity_request_required"])
        self.assertEqual(
            reviewer_one_result["next_identity_request"]["url"],
            "http://testserver:8025/api/v1/agents/whoami",
        )

        reviewer_two_status, reviewer_two_body = await asgi_request(
            "/api/v1/agents/whoami/repository",
            method="POST",
            payload={
                "git_address": "https://github.com/example/actor-import.git",
            },
        )
        self.assertEqual(reviewer_two_status, 200)
        self.assertIsInstance(reviewer_two_body, dict)
        assert isinstance(reviewer_two_body, dict)
        self.assertEqual(reviewer_two_body["agent"]["id"], "reviewer-two")
        self.assertEqual(
            reviewer_two_body["active_task"]["message"].count(
                "[PATCH BETWEEN COMMITS]"
            ),
            1,
        )
        self.assertIn(
            "diff --git a/analysis.txt b/analysis.txt",
            reviewer_two_body["active_task"]["message"],
        )

    async def test_sequential_graph_reuses_logical_agent_across_nodes(self) -> None:
        payload = {
            "git_address": "https://github.com/example/actor-import.git",
            "agents": {"overwrite": True},
            "execution": {
                "mode": "sequential",
                "start_node": "coordinate",
                "required_approvals": 2,
                "reviewers": [
                    {
                        "id": "reuse-reviewer-one",
                        "name": "Reuse Reviewer One",
                        "phone": "2181",
                    },
                    {
                        "id": "reuse-reviewer-two",
                        "name": "Reuse Reviewer Two",
                        "phone": "2182",
                    },
                ],
            },
            "nodes": [
                {
                    "id": "coordinate",
                    "agent": {
                        "id": "coordinator-first-role",
                        "name": "Shared Coordinator",
                        "phone": "2183",
                        "git_branch": "agent/coordinator-first",
                    },
                    "tasks": [{"task_id": "COORD-1", "message": "Plan the fix."}],
                    "transitions": {"DONE": "record-merge"},
                },
                {
                    "id": "record-merge",
                    "agent": {
                        "id": "coordinator-second-role",
                        "name": "Shared Coordinator",
                        "phone": "2183",
                        "git_branch": "agent/coordinator-second",
                    },
                    "tasks": [
                        {"task_id": "COORD-2", "message": "Record the merge."}
                    ],
                    "transitions": {"DONE": "finished"},
                },
                {"id": "finished", "type": "terminal", "status": "DONE"},
            ],
        }

        import_status, imported = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/import",
            method="POST",
            payload=payload,
        )

        self.assertEqual(import_status, 201, imported)
        self.assertIsInstance(imported, dict)
        assert isinstance(imported, dict)
        first_role = next(
            agent
            for agent in imported["imported_agents"]
            if agent["id"] == "coordinator-first-role"
        )
        second_role = next(
            agent
            for agent in imported["imported_agents"]
            if agent["id"] == "coordinator-second-role"
        )
        self.assertEqual(first_role["name"], "Shared Coordinator")
        self.assertEqual(first_role["phone"], "2183")
        self.assertNotEqual(second_role["name"], first_role["name"])
        self.assertNotEqual(second_role["phone"], first_role["phone"])
        for role in (first_role, second_role):
            self.assertEqual(
                role["parameters"]["workflow_logical_agent_name"],
                "Shared Coordinator",
            )
            self.assertEqual(
                role["parameters"]["workflow_logical_agent_phone"],
                "2183",
            )
            self.assertEqual(
                role["parameters"]["workflow_identity_group"],
                "coordinator-first-role",
            )
        workflow_nodes = imported["assignment"]["workflow"]["nodes"]
        self.assertEqual(
            [node["agent_name"] for node in workflow_nodes],
            ["Shared Coordinator", "Shared Coordinator"],
        )

        work_status, first_review = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/2183/whoami",
            method="POST",
            payload={
                "assignment_id": imported["assignment"]["current_assignment_id"],
                "status": "DONE",
                "result": "Coordination complete.",
            },
        )
        self.assertEqual(work_status, 200, first_review)
        self.assertIsInstance(first_review, dict)
        assert isinstance(first_review, dict)
        approval_one_status, second_review = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/2181/whoami",
            method="POST",
            payload={
                "assignment_id": first_review["current_assignment_id"],
                "status": "APPROVE",
            },
        )
        self.assertEqual(approval_one_status, 200, second_review)
        self.assertIsInstance(second_review, dict)
        assert isinstance(second_review, dict)
        approval_two_status, next_node = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/2182/whoami",
            method="POST",
            payload={
                "assignment_id": second_review["current_assignment_id"],
                "status": "APPROVE",
            },
        )
        self.assertEqual(approval_two_status, 200, next_node)
        self.assertIsInstance(next_node, dict)
        assert isinstance(next_node, dict)
        self.assertEqual(next_node["agent"]["id"], "coordinator-second-role")
        self.assertEqual(next_node["agent"]["phone"], second_role["phone"])
        self.assertEqual(next_node["agent"]["git_branch"], "agent/coordinator-second")
        self.assertEqual(
            main.queue_item_metadata(main.queues["worker-all"][0])["tasks"][0][
                "task_id"
            ],
            "COORD-2",
        )

    async def test_sequential_telegram_url_stages_then_starts_first_graph_node(self) -> None:
        agents_before = main.agents_path.read_text(encoding="utf-8")
        status_code, body = await asgi_request(
            "/api/v1/telegram/agents/sequential",
            method="POST",
            payload={
                "git_address": "https://github.com/example/actor-import.git",
                "assignment_mode": "parallel",
                "agents": {
                    "overwrite": True,
                    "items": [
                        {
                            "id": "url-sequential-a",
                            "name": "URL Sequential A",
                            "phone": "2131",
                            "tasks": [
                                {
                                    "task_id": "URL-SEQ-A",
                                    "queue": "tester-all",
                                    "message": "Run the first graph node.",
                                }
                            ],
                        },
                        {
                            "id": "url-sequential-b",
                            "name": "URL Sequential B",
                            "phone": "2132",
                            "tasks": ["Run the second graph node."],
                        },
                    ],
                },
            },
        )

        self.assertEqual(status_code, 200)
        self.assertIsInstance(body, dict)
        assert isinstance(body, dict)
        self.assertTrue(body["staged"])
        self.assertEqual(body["pending_sprint"]["assignment_mode"], "sequential")
        self.assertEqual(main.agents_path.read_text(encoding="utf-8"), agents_before)
        self.assertTrue(all(not queue for queue in main.queues.values()))
        self.assertFalse(main.sprint_history_path.exists())
        self.assertTrue(main.pending_sprints_path.exists())

        pending_id = body["pending_sprint"]["id"]
        list_status, pending_list = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints"
        )
        self.assertEqual(list_status, 200)
        self.assertIsInstance(pending_list, dict)
        assert isinstance(pending_list, dict)
        self.assertEqual(
            [sprint["id"] for sprint in pending_list["pending_sprints"]],
            [pending_id],
        )
        detail_status, pending_detail = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/{pending_id}"
        )
        self.assertEqual(detail_status, 200)
        self.assertIsInstance(pending_detail, dict)
        assert isinstance(pending_detail, dict)
        detail_record = pending_detail.get("pending_sprint") or pending_detail
        self.assertEqual(detail_record["id"], pending_id)

        activated = await self.start_staged_sprint(body)
        self.assertEqual(activated["assignment_mode"], "sequential")
        self.assertEqual(activated["active_agent"]["id"], "url-sequential-a")
        self.assertEqual(activated["queued_task_count"], 1)
        self.assertEqual(activated["queued_queue_item_count"], 1)
        self.assertEqual(activated["deferred_task_count"], 1)
        self.assertEqual(
            activated["sequential_poll_endpoint"],
            f"/worker/all/{self.PROJECT_PHONE}?to_phone={self.PROJECT_PHONE}",
        )
        self.assertEqual(len(main.queues["worker-all"]), 1)
        self.assertEqual(len(main.queues["tester-all"]), 0)
        after_start_status, after_start = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints"
        )
        self.assertEqual(after_start_status, 200)
        self.assertIsInstance(after_start, dict)
        assert isinstance(after_start, dict)
        self.assertEqual(after_start["pending_count"], 0)
        self.assertEqual(after_start["pending_sprints"], [])

        poll_status, node = await asgi_request(activated["sequential_poll_endpoint"])
        self.assertEqual(poll_status, 200)
        self.assertIsInstance(node, dict)
        assert isinstance(node, dict)
        self.assertIn("Сейчас вы агент URL Sequential A", node["message"])
        self.assertEqual(node["to_phone"], self.PROJECT_PHONE)
        self.assertEqual(node["metadata"]["graph_node_index"], 1)
        self.assertEqual(node["metadata"]["graph_node_count"], 2)
        self.assertEqual(node["metadata"]["agent"]["id"], "url-sequential-a")
        self.assertEqual(node["metadata"]["tasks"][0]["task_id"], "URL-SEQ-A")

    async def test_conditional_graph_requires_two_reviews_for_every_transition(self) -> None:
        payload = {
            "git_address": "https://github.com/example/actor-import.git",
            "agents": {"overwrite": True},
            "execution": {
                "mode": "sequential",
                "start_node": "development",
                "max_rework_cycles": 3,
                "required_approvals": 2,
                "reviewers": [
                    {
                        "id": "reviewer-one",
                        "name": "Architecture Reviewer",
                        "phone": "2153",
                        "profile": "Review correctness and project architecture.",
                    },
                    {
                        "id": "reviewer-two",
                        "name": "Quality Reviewer",
                        "phone": "2154",
                        "profile": "Review tests, evidence, and regressions.",
                    },
                ],
            },
            "nodes": [
                {
                    "id": "development",
                    "agent": {
                        "id": "developer",
                        "name": "Programmer",
                        "phone": "2151",
                        "profile": "Implement the requested change.",
                    },
                    "task": "Implement and test the change.",
                    "transitions": {"DONE": "verification"},
                },
                {
                    "id": "verification",
                    "agent": {
                        "id": "tester",
                        "name": "Tester",
                        "phone": "2152",
                        "profile": "Verify the implementation independently.",
                    },
                    "task": "Run acceptance and regression tests.",
                    "transitions": {
                        "PASS": "completed",
                        "FAIL": "development",
                    },
                },
                {
                    "id": "completed",
                    "type": "terminal",
                    "status": "DONE",
                    "message": "Implementation accepted.",
                },
            ],
        }
        import_status, import_body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/import",
            method="POST",
            payload=payload,
        )
        self.assertEqual(import_status, 201)
        self.assertIsInstance(import_body, dict)
        assert isinstance(import_body, dict)
        self.assertEqual(import_body["imported_agent_count"], 4)
        self.assertEqual(import_body["assignment"]["strategy"], "conditional_graph")
        self.assertEqual(import_body["active_agent"]["id"], "developer")
        reviewer_one = next(
            agent
            for agent in import_body["imported_agents"]
            if agent["id"] == "reviewer-one"
        )
        self.assertIn("Полный граф проекта", reviewer_one["profile"])
        self.assertIn(payload["git_address"], reviewer_one["profile"])
        self.assertEqual(
            reviewer_one["parameters"]["git_context_key"],
            self.PROJECT_CONTEXT,
        )
        state_status, project_state = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/state.json"
        )
        self.assertEqual(state_status, 200)
        self.assertIsInstance(project_state, dict)
        assert isinstance(project_state, dict)
        self.assertEqual(len(project_state["agents"]), 4)
        self.assertEqual(
            project_state["execution"]["workflow"]["reviewer_agent_ids"],
            ["reviewer-one", "reviewer-two"],
        )

        first_status, first_body = await asgi_request(
            "/api/v1/agents/whoami/repository",
            method="POST",
            payload={"git_address": payload["git_address"]},
        )
        self.assertEqual(first_status, 200)
        self.assertIsInstance(first_body, dict)
        assert isinstance(first_body, dict)
        self.assertEqual(first_body["agent"]["id"], "developer")
        self.assertEqual(first_body["assignment_strategy"], "conditional_graph")
        developer_assignment_id = first_body["active_task"]["metadata"]["assignment_id"]

        review_one_status, review_one = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/2151/whoami",
            method="POST",
            payload={
                "assignment_id": developer_assignment_id,
                "status": "DONE",
                "result": "Implemented the feature; unit tests pass.",
            },
        )
        self.assertEqual(review_one_status, 200)
        self.assertIsInstance(review_one, dict)
        assert isinstance(review_one, dict)
        self.assertEqual(review_one["phase"], "review")
        self.assertEqual(review_one["agent"]["id"], "reviewer-one")
        self.assertEqual(
            review_one["pending_transition"]["target_node_id"],
            "verification",
        )
        self.assertEqual(len(main.queues["worker-all"]), 1, review_one)

        reviewer_card_status, reviewer_card = await asgi_request(
            "/api/v1/agents/whoami/repository",
            method="POST",
            payload={"git_address": payload["git_address"]},
        )
        self.assertEqual(reviewer_card_status, 200)
        self.assertIsInstance(reviewer_card, dict)
        assert isinstance(reviewer_card, dict)
        self.assertEqual(reviewer_card["agent"]["id"], "reviewer-one")
        self.assertEqual(reviewer_card["phase"], "review")
        self.assertIn("Implemented the feature", reviewer_card["active_task"]["message"])
        self.assertIn("Полный граф проекта", reviewer_card["active_task"]["message"])
        self.assertEqual(reviewer_card["project"]["git_address"], payload["git_address"])

        review_two_status, review_two = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/2153/whoami",
            method="POST",
            payload={
                "assignment_id": reviewer_card["active_task"]["metadata"]["assignment_id"],
                "status": "APPROVE",
                "feedback": "Architecture is consistent.",
            },
        )
        self.assertEqual(review_two_status, 200)
        self.assertIsInstance(review_two, dict)
        assert isinstance(review_two, dict)
        self.assertEqual(review_two["agent"]["id"], "reviewer-two")
        self.assertEqual(len(review_two["pending_transition"]["reviews"]), 1)

        reviewer_two_status, reviewer_two_card = await asgi_request(
            "/api/v1/agents/whoami/repository",
            method="POST",
            payload={"git_address": payload["git_address"]},
        )
        self.assertEqual(reviewer_two_status, 200)
        self.assertIsInstance(reviewer_two_card, dict)
        assert isinstance(reviewer_two_card, dict)
        self.assertEqual(reviewer_two_card["agent"]["id"], "reviewer-two")

        tester_status, tester = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/2154/whoami",
            method="POST",
            payload={
                "assignment_id": reviewer_two_card["active_task"]["metadata"]["assignment_id"],
                "status": "APPROVE",
                "feedback": "Tests and evidence are sufficient.",
            },
        )
        self.assertEqual(tester_status, 200)
        self.assertIsInstance(tester, dict)
        assert isinstance(tester, dict)
        self.assertEqual(tester["phase"], "node")
        self.assertEqual(tester["agent"]["id"], "tester")
        self.assertEqual(len(tester["transition_applied"]["reviews"]), 2)

        tester_card_status, tester_card = await asgi_request(
            "/api/v1/agents/whoami/repository",
            method="POST",
            payload={"git_address": payload["git_address"]},
        )
        self.assertEqual(tester_card_status, 200)
        self.assertIsInstance(tester_card, dict)
        assert isinstance(tester_card, dict)
        self.assertEqual(tester_card["agent"]["id"], "tester")

        fail_review_status, fail_review = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/2152/whoami",
            method="POST",
            payload={
                "assignment_id": tester_card["active_task"]["metadata"]["assignment_id"],
                "status": "FAIL",
                "result": "Regression test failed.",
                "feedback": "Fix the duplicate notification.",
            },
        )
        self.assertEqual(fail_review_status, 200)
        self.assertIsInstance(fail_review, dict)
        assert isinstance(fail_review, dict)
        self.assertEqual(fail_review["agent"]["id"], "reviewer-one")
        self.assertEqual(
            fail_review["pending_transition"]["target_node_id"],
            "development",
        )

        first_fail_approval_status, first_fail_approval = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/2153/whoami",
            method="POST",
            payload={
                "assignment_id": fail_review["current_assignment_id"],
                "status": "APPROVE",
                "feedback": "The failure report is reproducible.",
            },
        )
        self.assertEqual(first_fail_approval_status, 200)
        self.assertIsInstance(first_fail_approval, dict)
        assert isinstance(first_fail_approval, dict)
        second_fail_approval_status, rework = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/2154/whoami",
            method="POST",
            payload={
                "assignment_id": first_fail_approval["current_assignment_id"],
                "status": "APPROVE",
                "feedback": "Returning the issue for rework is justified.",
            },
        )
        self.assertEqual(second_fail_approval_status, 200)
        self.assertIsInstance(rework, dict)
        assert isinstance(rework, dict)
        self.assertEqual(rework["agent"]["id"], "developer")
        self.assertEqual(rework["assignment"]["rework_cycle_count"], 1)
        self.assertIn(
            "Fix the duplicate notification",
            main.queue_item_message(main.queues["worker-all"][0]),
        )

        development_done_status, development_done = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/2151/whoami",
            method="POST",
            payload={
                "assignment_id": rework["current_assignment_id"],
                "status": "DONE",
                "result": "Duplicate notification fixed and regression test added.",
            },
        )
        self.assertEqual(development_done_status, 200)
        self.assertIsInstance(development_done, dict)
        assert isinstance(development_done, dict)
        approval_a_status, approval_a = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/2153/whoami",
            method="POST",
            payload={
                "assignment_id": development_done["current_assignment_id"],
                "status": "APPROVE",
            },
        )
        self.assertEqual(approval_a_status, 200)
        self.assertIsInstance(approval_a, dict)
        assert isinstance(approval_a, dict)
        approval_b_status, verification_again = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/2154/whoami",
            method="POST",
            payload={
                "assignment_id": approval_a["current_assignment_id"],
                "status": "APPROVE",
            },
        )
        self.assertEqual(approval_b_status, 200)
        self.assertIsInstance(verification_again, dict)
        assert isinstance(verification_again, dict)
        self.assertEqual(verification_again["agent"]["id"], "tester")

        pass_status, pass_review = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/2152/whoami",
            method="POST",
            payload={
                "assignment_id": verification_again["current_assignment_id"],
                "status": "PASS",
                "result": "Acceptance and regression tests pass.",
            },
        )
        self.assertEqual(pass_status, 200)
        self.assertIsInstance(pass_review, dict)
        assert isinstance(pass_review, dict)
        terminal_review_a_status, terminal_review_a = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/2153/whoami",
            method="POST",
            payload={
                "assignment_id": pass_review["current_assignment_id"],
                "status": "APPROVE",
            },
        )
        self.assertEqual(terminal_review_a_status, 200)
        self.assertIsInstance(terminal_review_a, dict)
        assert isinstance(terminal_review_a, dict)
        completed_status, completed = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/2154/whoami",
            method="POST",
            payload={
                "assignment_id": terminal_review_a["current_assignment_id"],
                "status": "APPROVE",
            },
        )
        self.assertEqual(completed_status, 200)
        self.assertIsInstance(completed, dict)
        assert isinstance(completed, dict)
        self.assertTrue(completed["all_completed"])
        self.assertFalse(completed["blocked"])
        self.assertEqual(completed["terminal_node"]["id"], "completed")

    async def test_graph_reject_requires_feedback_and_returns_source_node(self) -> None:
        payload = {
            "project_id": self.PROJECT_PHONE,
            "git_address": "https://github.com/example/actor-import.git",
            "agents": {"overwrite": True},
            "execution": {
                "mode": "sequential",
                "start_node": "work",
                "max_rework_cycles": 2,
                "reviewers": [
                    {"id": "review-a", "name": "Reviewer A", "phone": "2161"},
                    {"id": "review-b", "name": "Reviewer B", "phone": "2162"},
                ],
            },
            "nodes": [
                {
                    "id": "work",
                    "agent": {"id": "worker", "name": "Worker", "phone": "2163"},
                    "task": "Prepare the result.",
                    "transitions": {"DONE": "finished"},
                },
                {"id": "finished", "type": "terminal", "status": "DONE"},
            ],
        }
        import_status, imported = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/import",
            method="POST",
            payload=payload,
        )
        self.assertEqual(import_status, 201)
        self.assertIsInstance(imported, dict)
        assert isinstance(imported, dict)
        worker_assignment = imported["assignment"]["current_assignment_id"]
        review_status, review = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/2163/whoami",
            method="POST",
            payload={
                "assignment_id": worker_assignment,
                "status": "DONE",
                "result": "Draft result.",
            },
        )
        self.assertEqual(review_status, 200)
        self.assertIsInstance(review, dict)
        assert isinstance(review, dict)
        reject_without_feedback, _ = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/2161/whoami",
            method="POST",
            payload={
                "assignment_id": review["current_assignment_id"],
                "status": "REJECT",
            },
        )
        self.assertEqual(reject_without_feedback, 400)
        reject_status, rejected = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/2161/whoami",
            method="POST",
            payload={
                "assignment_id": review["current_assignment_id"],
                "status": "REJECT",
                "feedback": "Add reproducible verification evidence.",
            },
        )
        self.assertEqual(reject_status, 200)
        self.assertIsInstance(rejected, dict)
        assert isinstance(rejected, dict)
        self.assertEqual(rejected["agent"]["id"], "worker")
        self.assertEqual(rejected["assignment"]["rework_cycle_count"], 1)
        self.assertIn(
            "Add reproducible verification evidence",
            main.queue_item_message(main.queues["worker-all"][0]),
        )

    async def test_graph_blocks_after_rework_limit(self) -> None:
        payload = {
            "project_id": self.PROJECT_PHONE,
            "git_address": "https://github.com/example/actor-import.git",
            "agents": {"overwrite": True},
            "execution": {
                "mode": "sequential",
                "start_node": "work",
                "max_rework_cycles": 0,
                "reviewers": [
                    {"id": "limit-review-a", "name": "Limit Review A", "phone": "2171"},
                    {"id": "limit-review-b", "name": "Limit Review B", "phone": "2172"},
                ],
            },
            "nodes": [
                {
                    "id": "work",
                    "agent": {
                        "id": "limit-worker",
                        "name": "Limit Worker",
                        "phone": "2173",
                    },
                    "task": "Prepare the result.",
                    "transitions": {"DONE": "finished"},
                },
                {"id": "finished", "type": "terminal"},
            ],
        }
        import_status, imported = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/import",
            method="POST",
            payload=payload,
        )
        self.assertEqual(import_status, 201)
        self.assertIsInstance(imported, dict)
        assert isinstance(imported, dict)
        review_status, review = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/2173/whoami",
            method="POST",
            payload={
                "assignment_id": imported["assignment"]["current_assignment_id"],
                "status": "DONE",
                "result": "Result requiring review.",
            },
        )
        self.assertEqual(review_status, 200)
        self.assertIsInstance(review, dict)
        assert isinstance(review, dict)
        blocked_status, blocked = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/2171/whoami",
            method="POST",
            payload={
                "assignment_id": review["current_assignment_id"],
                "status": "REJECT",
                "feedback": "The evidence is incomplete.",
            },
        )
        self.assertEqual(blocked_status, 200)
        self.assertIsInstance(blocked, dict)
        assert isinstance(blocked, dict)
        self.assertTrue(blocked["all_completed"])
        self.assertTrue(blocked["blocked"])
        self.assertEqual(blocked["assignment"]["status"], "blocked")
        self.assertEqual(
            blocked["assignment"]["blocked_reason"],
            "max_rework_cycles_exceeded",
        )

    async def test_parallel_telegram_url_never_switches_roles(self) -> None:
        agents_before = main.agents_path.read_text(encoding="utf-8")
        status_code, body = await asgi_request(
            "/api/v1/telegram/agents/parallel",
            method="POST",
            payload={
                "git_address": "https://github.com/example/actor-import.git",
                "assignment_mode": "sequential",
                "agents": {
                    "overwrite": True,
                    "items": [
                        {
                            "id": "url-parallel-a",
                            "name": "URL Parallel A",
                            "phone": "2141",
                            "tasks": ["Run parallel task A."],
                        },
                        {
                            "id": "url-parallel-b",
                            "name": "URL Parallel B",
                            "phone": "2142",
                            "tasks": ["Run parallel task B."],
                        },
                    ],
                },
            },
        )

        self.assertEqual(status_code, 200)
        self.assertIsInstance(body, dict)
        assert isinstance(body, dict)
        self.assertTrue(body["staged"])
        self.assertEqual(body["pending_sprint"]["assignment_mode"], "parallel")
        self.assertEqual(main.agents_path.read_text(encoding="utf-8"), agents_before)
        self.assertTrue(all(not queue for queue in main.queues.values()))

        activated = await self.start_staged_sprint(body)
        self.assertEqual(activated["assignment_mode"], "parallel")
        self.assertEqual(activated["queued_task_count"], 2)
        self.assertEqual(activated["deferred_task_count"], 0)
        self.assertIsNone(activated["sequential_poll_endpoint"])
        queued_phones = {
            main.queue_item_metadata(item)["to_phone"]
            for item in main.queues["worker-all"]
        }
        self.assertEqual(queued_phones, {"2141", "2142"})

        whoami_status, whoami_body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/2142/whoami",
            method="POST",
            payload={"message": "Кто я?", "completed": True},
        )
        self.assertEqual(whoami_status, 200)
        self.assertIsInstance(whoami_body, dict)
        assert isinstance(whoami_body, dict)
        self.assertEqual(whoami_body["agent"]["id"], "url-parallel-b")
        self.assertNotIn("assignment_mode", whoami_body)

        switch_status, switch_body = await asgi_request(
            "/api/v1/agents/whoami/repository",
            method="POST",
            payload={
                "git_address": "https://github.com/example/actor-import.git",
            },
        )
        self.assertEqual(switch_status, 409)
        self.assertIsInstance(switch_body, dict)
        assert isinstance(switch_body, dict)
        self.assertEqual(
            switch_body["detail"]["error"],
            "project_not_sequential",
        )

    async def test_conditional_graph_initializes_two_reviewers_before_work(self) -> None:
        graph_json = {
            "git_address": "https://github.com/example/actor-import.git",
            "agents": {"overwrite": True},
            "execution": {
                "mode": "sequential",
                "initialize_reviewers": True,
                "start_node": "build",
                "required_approvals": 2,
                "reviewers": [
                    {
                        "id": "reviewer-one",
                        "name": "Reviewer One",
                        "phone": "2101",
                        "profile": "Review the whole project and product requirements.",
                    },
                    {
                        "id": "reviewer-two",
                        "name": "Reviewer Two",
                        "phone": "2102",
                        "profile": "Review the whole project and technical quality.",
                    },
                ],
            },
            "nodes": [
                {
                    "id": "build",
                    "agent": {
                        "id": "builder",
                        "name": "Builder",
                        "phone": "2201",
                        "profile": "Build and verify the requested change.",
                    },
                    "tasks": [
                        {
                            "task_id": "BUILD-1",
                            "queue": "worker-all",
                            "message": "Implement the project change.",
                        }
                    ],
                    "transitions": {"DONE": "finished"},
                },
                {
                    "id": "finished",
                    "type": "terminal",
                    "status": "DONE",
                    "message": "Project work is complete.",
                },
            ],
        }
        import_status, imported = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/import",
            method="POST",
            payload=graph_json,
        )
        self.assertEqual(import_status, 201)
        self.assertIsInstance(imported, dict)
        assert isinstance(imported, dict)
        self.assertEqual(imported["assignment"]["strategy"], "conditional_graph")
        self.assertEqual(imported["queued_queue_item_count"], 3)

        identities: list[dict[str, object]] = []
        for _ in range(3):
            status_code, body = await asgi_request(
                "/api/v1/agents/whoami/repository",
                method="POST",
                payload={
                    "git_address": "https://github.com/example/actor-import.git"
                },
            )
            self.assertEqual(status_code, 200)
            self.assertIsInstance(body, dict)
            assert isinstance(body, dict)
            identities.append(body)

        self.assertEqual(
            [identity["agent"]["id"] for identity in identities],
            ["reviewer-one", "reviewer-two", "builder"],
        )
        self.assertEqual(
            [identity["identity_kind"] for identity in identities],
            ["reviewer_bootstrap", "reviewer_bootstrap", "graph_node"],
        )
        self.assertTrue(identities[0]["identity_persistent"])
        self.assertTrue(identities[1]["identity_persistent"])
        self.assertFalse(identities[2]["identity_persistent"])
        reviewer_state = identities[1]["project_state"]
        self.assertEqual(len(reviewer_state["agents"]), 3)
        self.assertEqual(
            reviewer_state["workflow"]["start_node_id"],
            "build",
        )
        self.assertEqual(
            len(reviewer_state["execution"]["reviewer_initializations"]),
            2,
        )
        self.assertGreaterEqual(len(reviewer_state["recent_activity"]), 1)

        builder_identity = identities[2]
        work_status, work_result = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/2201/whoami",
            method="POST",
            payload={
                "assignment_id": builder_identity["active_task"]["metadata"][
                    "assignment_id"
                ],
                "status": "DONE",
                "result": "Implemented the change and all tests passed.",
            },
        )
        self.assertEqual(work_status, 200)
        self.assertIsInstance(work_result, dict)
        assert isinstance(work_result, dict)
        self.assertEqual(work_result["phase"], "review")
        self.assertEqual(work_result["agent"]["id"], "reviewer-one")
        self.assertEqual(
            work_result["project_state"]["execution"]["pending_transition"][
                "result"
            ],
            "Implemented the change and all tests passed.",
        )

        first_review_status, first_review = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/2101/whoami",
            method="POST",
            payload={
                "assignment_id": work_result["current_assignment_id"],
                "status": "APPROVE",
                "feedback": "Product requirements are satisfied.",
            },
        )
        self.assertEqual(first_review_status, 200)
        self.assertIsInstance(first_review, dict)
        assert isinstance(first_review, dict)
        self.assertEqual(first_review["agent"]["id"], "reviewer-two")

        second_review_status, second_review = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/2102/whoami",
            method="POST",
            payload={
                "assignment_id": first_review["current_assignment_id"],
                "status": "APPROVE",
                "feedback": "Technical verification passed.",
            },
        )
        self.assertEqual(second_review_status, 200)
        self.assertIsInstance(second_review, dict)
        assert isinstance(second_review, dict)
        self.assertTrue(second_review["all_completed"])
        self.assertEqual(
            second_review["project_state"]["execution"]["status"],
            "completed",
        )
        self.assertEqual(
            len(
                second_review["project_state"]["execution"]["last_transition"][
                    "reviews"
                ]
            ),
            2,
        )

        state_status, state = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/state.json"
        )
        self.assertEqual(state_status, 200)
        self.assertIsInstance(state, dict)
        assert isinstance(state, dict)
        self.assertEqual(state["execution"]["status"], "completed")
        self.assertGreaterEqual(len(state["recent_activity"]), 1)

    async def test_telegram_text_json_imports_actors_and_tasks(self) -> None:
        agents_before = main.agents_path.read_text(encoding="utf-8")
        actor_json = {
            "project_id": self.PROJECT_PHONE,
            "actors": {
                "overwrite": True,
                "items": [
                    {
                        "name": "Telegram Actor",
                        "phone": "2030",
                        "tasks": ["Task received from Telegram."],
                    }
                ],
            },
        }
        status_code, body = await asgi_request(
            "/api/v1/telegram/actors",
            method="POST",
            payload={
                "update_id": 1,
                "message": {
                    "message_id": 2,
                    "chat": {"id": 3},
                    "text": "```json\n"
                    + json.dumps(actor_json, ensure_ascii=False)
                    + "\n```",
                },
            },
        )
        self.assertEqual(status_code, 200)
        self.assertIsInstance(body, dict)
        assert isinstance(body, dict)
        self.assertTrue(body["ok"])
        self.assertTrue(body["staged"])
        self.assertEqual(body["pending_sprint"]["source"], "telegram")
        self.assertEqual(main.agents_path.read_text(encoding="utf-8"), agents_before)
        self.assertTrue(all(not queue for queue in main.queues.values()))

        activated = await self.start_staged_sprint(body)
        self.assertEqual(activated["source"], "telegram")
        self.assertEqual(activated["imported_actor_count"], 1)
        self.assertEqual(activated["queued_task_count"], 1)

    async def test_telegram_agents_json_resolves_project_from_git_address(self) -> None:
        agent_json = {
            "git_address": "git@github.com:example/actor-import.git",
            "agents": {
                "overwrite": True,
                "items": [
                    {
                        "id": "repository-agent",
                        "name": "Repository Agent",
                        "tasks": ["Task routed by repository."],
                    }
                ],
            },
        }
        status_code, body = await asgi_request(
            "/api/v1/telegram/agents",
            method="POST",
            payload={
                "update_id": 11,
                "message": {
                    "message_id": 12,
                    "chat": {"id": 13},
                    "text": json.dumps(agent_json),
                },
            },
        )

        self.assertEqual(status_code, 200)
        self.assertIsInstance(body, dict)
        assert isinstance(body, dict)
        self.assertTrue(body["staged"])
        self.assertEqual(body["project_phone"], self.PROJECT_PHONE)
        self.assertEqual(body["git_context_key"], self.PROJECT_CONTEXT)
        activated = await self.start_staged_sprint(body)
        self.assertEqual(activated["project_phone"], self.PROJECT_PHONE)
        self.assertEqual(activated["project"]["git_context_key"], self.PROJECT_CONTEXT)
        self.assertEqual(activated["imported_agent_count"], 1)
        self.assertEqual(activated["queued_task_count"], 1)

    async def test_pending_telegram_sprint_survives_storage_reload(self) -> None:
        status_code, staged = await asgi_request(
            "/api/v1/telegram/agents",
            method="POST",
            payload={
                "update_id": 12001,
                "message": {
                    "message_id": 81,
                    "chat": {"id": 73},
                    "text": json.dumps(
                        {
                            "git_address": "https://github.com/example/actor-import.git",
                            "sprint": {"id": "persistent-pending", "title": "Persistent Pending"},
                            "agents": {
                                "overwrite": True,
                                "items": [
                                    {
                                        "id": "persistent-pending-agent",
                                        "name": "Persistent Pending Agent",
                                        "phone": "2061",
                                        "tasks": ["Start only after confirmation."],
                                    }
                                ],
                            },
                        }
                    ),
                },
            },
        )
        self.assertEqual(status_code, 200)
        self.assertIsInstance(staged, dict)
        assert isinstance(staged, dict)
        sprint_id = staged["pending_sprint"]["id"]
        self.assertTrue(main.pending_sprints_path.exists())
        self.assertIn(
            sprint_id,
            main.pending_sprints_path.read_text(encoding="utf-8"),
        )

        # Model a process restart: no in-memory object may be required to list
        # or open the staged sprint; the file remains the source of truth.
        main.pending_sprints_lock = asyncio.Lock()
        list_status, pending_list = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints"
        )
        detail_status, detail = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/{sprint_id}"
        )
        self.assertEqual(list_status, 200)
        self.assertEqual(detail_status, 200)
        self.assertIsInstance(pending_list, dict)
        self.assertIsInstance(detail, dict)
        assert isinstance(pending_list, dict) and isinstance(detail, dict)
        self.assertEqual(
            [item["id"] for item in pending_list["pending_sprints"]],
            [sprint_id],
        )
        detail_record = detail.get("pending_sprint") or detail
        self.assertEqual(detail_record["id"], sprint_id)

    async def test_pending_telegram_sprints_are_isolated_by_project(self) -> None:
        second_context = "github.com/example/pending-second-project"
        second_phone = "9009"
        second_project = {
            "project_name": "Pending Second Project",
            "git_address": "https://github.com/example/pending-second-project.git",
            "git_context_key": second_context,
            "project_phone": second_phone,
            "groups": [],
            "group_relationships": [],
            "customer_reporting": {},
        }
        config = main.read_git_config_file()
        config[main.PROJECTS_KEY][second_context] = second_project
        config[main.PHONE_GIT_CONTEXTS_KEY][second_phone] = {
            **second_project,
            "phone": second_phone,
        }
        main.write_git_config_file(config)

        staged_results: list[dict[str, object]] = []
        for update_id, git_address, title, actor_id, phone in (
            (
                12101,
                "https://github.com/example/actor-import.git",
                "Primary Pending Sprint",
                "primary-pending-agent",
                "2062",
            ),
            (
                12102,
                second_project["git_address"],
                "Second Pending Sprint",
                "second-pending-agent",
                "2063",
            ),
        ):
            stage_status, stage_body = await asgi_request(
                "/api/v1/telegram/agents",
                method="POST",
                payload={
                    "update_id": update_id,
                    "message": {
                        "message_id": update_id,
                        "chat": {"id": 74},
                        "text": json.dumps(
                            {
                                "git_address": git_address,
                                "sprint": {"title": title},
                                "agents": {
                                    "overwrite": True,
                                    "items": [
                                        {
                                            "id": actor_id,
                                            "name": title + " Agent",
                                            "phone": phone,
                                            "tasks": [title + " task"],
                                        }
                                    ],
                                },
                            }
                        ),
                    },
                },
            )
            self.assertEqual(stage_status, 200)
            self.assertIsInstance(stage_body, dict)
            assert isinstance(stage_body, dict)
            staged_results.append(stage_body)

        primary_id = staged_results[0]["pending_sprint"]["id"]
        second_id = staged_results[1]["pending_sprint"]["id"]
        primary_status, primary = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints"
        )
        second_status, second = await asgi_request(
            f"/api/v1/projects/{second_phone}/pending-sprints"
        )
        self.assertEqual(primary_status, 200)
        self.assertEqual(second_status, 200)
        self.assertIsInstance(primary, dict)
        self.assertIsInstance(second, dict)
        assert isinstance(primary, dict) and isinstance(second, dict)
        self.assertEqual(
            {item["id"] for item in primary["pending_sprints"]},
            {primary_id},
        )
        self.assertEqual(
            {item["id"] for item in second["pending_sprints"]},
            {second_id},
        )
        foreign_status, _ = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/{second_id}"
        )
        self.assertEqual(foreign_status, 404)

    async def test_telegram_update_id_is_deduplicated_without_mutation(self) -> None:
        update = {
            "update_id": 12201,
            "message": {
                "message_id": 91,
                "chat": {"id": 75},
                "text": json.dumps(
                    {
                        "git_address": "https://github.com/example/actor-import.git",
                        "sprint": {"title": "Deduplicated Pending Sprint"},
                        "agents": {
                            "overwrite": True,
                            "items": [
                                {
                                    "id": "deduplicated-pending-agent",
                                    "name": "Deduplicated Pending Agent",
                                    "phone": "2064",
                                    "tasks": ["Queue exactly once after start."],
                                }
                            ],
                        },
                    }
                ),
            },
        }
        agents_before = main.agents_path.read_text(encoding="utf-8")
        first_status, first = await asgi_request(
            "/api/v1/telegram/agents", method="POST", payload=update
        )
        second_status, second = await asgi_request(
            "/api/v1/telegram/agents", method="POST", payload=update
        )
        self.assertEqual(first_status, 200)
        self.assertEqual(second_status, 200)
        self.assertIsInstance(first, dict)
        self.assertIsInstance(second, dict)
        assert isinstance(first, dict) and isinstance(second, dict)
        self.assertEqual(
            first["pending_sprint"]["id"],
            second["pending_sprint"]["id"],
        )
        self.assertEqual(main.agents_path.read_text(encoding="utf-8"), agents_before)
        self.assertTrue(all(not queue for queue in main.queues.values()))
        list_status, pending_list = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints"
        )
        self.assertEqual(list_status, 200)
        self.assertIsInstance(pending_list, dict)
        assert isinstance(pending_list, dict)
        self.assertEqual(len(pending_list["pending_sprints"]), 1)

    async def test_telegram_update_id_cannot_move_to_another_project(self) -> None:
        second_context = "github.com/example/update-id-second"
        second_phone = "9009"
        second_git_address = "https://github.com/example/update-id-second.git"
        self.register_project(
            context_key=second_context,
            project_phone=second_phone,
            git_address=second_git_address,
            project_name="Update ID Second",
        )
        agents_before = main.agents_path.read_text(encoding="utf-8")
        shared_update_id = 12202
        first_status, first = await asgi_request(
            "/api/v1/telegram/agents",
            method="POST",
            payload={
                "update_id": shared_update_id,
                "git_address": "https://github.com/example/actor-import.git",
                "agents": {
                    "overwrite": True,
                    "items": [
                        {
                            "id": "global-update-primary-agent",
                            "name": "Global Update Primary Agent",
                            "phone": "2068",
                            "tasks": [],
                        }
                    ],
                },
            },
        )
        self.assertEqual(first_status, 200)
        self.assertIsInstance(first, dict)
        assert isinstance(first, dict)

        second_status, second = await asgi_request(
            "/api/v1/telegram/agents",
            method="POST",
            payload={
                "update_id": shared_update_id,
                "git_address": second_git_address,
                "agents": {
                    "overwrite": True,
                    "items": [
                        {
                            "id": "global-update-second-agent",
                            "name": "Global Update Second Agent",
                            "phone": "2069",
                            "tasks": [],
                        }
                    ],
                },
            },
        )
        self.assertEqual(second_status, 409, second)
        self.assertIsInstance(second, dict)
        assert isinstance(second, dict)
        self.assertEqual(
            second["detail"]["error"],
            "telegram_update_project_conflict",
        )
        self.assertEqual(main.agents_path.read_text(encoding="utf-8"), agents_before)
        self.assertTrue(all(not queue for queue in main.queues.values()))

        storage = main.read_pending_sprints_file()["projects"]
        primary_records = storage[self.PROJECT_CONTEXT]["sprints"]
        second_records = storage.get(second_context, {}).get("sprints", [])
        self.assertEqual(len(primary_records), 1)
        self.assertEqual(primary_records[0]["id"], first["pending_sprint"]["id"])
        self.assertEqual(second_records, [])

    async def test_pending_sprint_cannot_be_started_twice(self) -> None:
        stage_status, staged = await asgi_request(
            "/api/v1/telegram/agents/parallel",
            method="POST",
            payload={
                "git_address": "https://github.com/example/actor-import.git",
                "sprint": {"title": "Start Once"},
                "agents": {
                    "overwrite": True,
                    "items": [
                        {
                            "id": "start-once-agent",
                            "name": "Start Once Agent",
                            "phone": "2065",
                            "tasks": ["This task must be queued once."],
                        }
                    ],
                },
            },
        )
        self.assertEqual(stage_status, 200)
        self.assertIsInstance(staged, dict)
        assert isinstance(staged, dict)
        await self.start_staged_sprint(staged)
        queue_sizes = {name: len(queue) for name, queue in main.queues.items()}
        sprint_id = staged["pending_sprint"]["id"]

        repeated_status, repeated = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/{sprint_id}/start",
            method="POST",
        )
        self.assertEqual(repeated_status, 409, repeated)
        self.assertEqual(
            {name: len(queue) for name, queue in main.queues.items()},
            queue_sizes,
        )

    async def test_failed_pending_activation_remains_pending(self) -> None:
        agents_before = main.agents_path.read_text(encoding="utf-8")
        stage_status, staged = await asgi_request(
            "/api/v1/telegram/agents/parallel",
            method="POST",
            payload={
                "git_address": "https://github.com/example/actor-import.git",
                "sprint": {"title": "Retry After Conflict"},
                "agents": {
                    "overwrite": True,
                    "items": [
                        {
                            "id": "conflicting-pending-agent",
                            "name": "Conflicting Pending Agent",
                            "phone": "2066",
                            "tasks": ["Wait until the conflict is resolved."],
                        }
                    ],
                },
            },
        )
        self.assertEqual(stage_status, 200)
        self.assertIsInstance(staged, dict)
        assert isinstance(staged, dict)
        agents_data = main.read_agents_file()
        agents_data.append(
            {
                "id": "outside-phone-conflict",
                "name": "Outside Phone Conflict",
                "phone": "2066",
                "profile": "Belongs to another project.",
                "parameters": {"git_context_key": "github.com/example/outside-conflict"},
            }
        )
        main.agents_path.write_text(
            json.dumps({"agents": agents_data}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        state_before_start = main.agents_path.read_text(encoding="utf-8")
        sprint_id = staged["pending_sprint"]["id"]

        start_status, _ = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/{sprint_id}/start",
            method="POST",
        )
        self.assertEqual(start_status, 409)
        self.assertEqual(main.agents_path.read_text(encoding="utf-8"), state_before_start)
        self.assertNotEqual(state_before_start, agents_before)
        self.assertTrue(all(not queue for queue in main.queues.values()))

        list_status, pending_list = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints"
        )
        self.assertEqual(list_status, 200)
        self.assertIsInstance(pending_list, dict)
        assert isinstance(pending_list, dict)
        record = next(
            item
            for item in pending_list["pending_sprints"]
            if item["id"] == sprint_id
        )
        self.assertEqual(record["status"], "pending")

    async def test_telegram_stage_reply_contains_public_pending_sprint_url(self) -> None:
        os.environ["TELEGRAM_BOT_TOKEN"] = "test-token"
        telegram_calls: list[tuple[str, str, dict[str, object]]] = []

        def fake_telegram_api(
            token: str,
            method: str,
            data: dict[str, object],
        ) -> dict[str, object]:
            telegram_calls.append((token, method, data))
            return {"ok": True, "result": {}}

        with patch.object(
            main,
            "cloudflared_public_base_url",
            return_value="https://pending.example.test",
        ), patch.object(main, "telegram_api_json", side_effect=fake_telegram_api):
            status_code, body = await asgi_request(
                "/api/v1/telegram/agents",
                method="POST",
                payload={
                    "update_id": 12301,
                    "message": {
                        "message_id": 101,
                        "message_thread_id": 17,
                        "chat": {"id": 76},
                        "text": json.dumps(
                            {
                                "git_address": "https://github.com/example/actor-import.git",
                                "sprint": {"title": "Public Pending Sprint"},
                                "agents": {
                                    "overwrite": True,
                                    "items": [
                                        {
                                            "id": "public-pending-agent",
                                            "name": "Public Pending Agent",
                                            "phone": "2067",
                                            "tasks": [],
                                        }
                                    ],
                                },
                            }
                        ),
                    },
                },
            )

        self.assertEqual(status_code, 200)
        self.assertIsInstance(body, dict)
        assert isinstance(body, dict)
        self.assertTrue(body["staged"])
        public_url = body["pending_sprints_url"]
        parsed = urllib.parse.urlsplit(public_url)
        query = urllib.parse.parse_qs(parsed.query)
        self.assertEqual(parsed.scheme, "https")
        self.assertEqual(parsed.netloc, "pending.example.test")
        self.assertEqual(query["view"], ["pending-sprints"])
        self.assertEqual(query["project_phone"], [self.PROJECT_PHONE])
        self.assertEqual(query["sprint_id"], [body["pending_sprint"]["id"]])
        self.assertEqual(len(telegram_calls), 1)
        token, method, request_data = telegram_calls[0]
        self.assertEqual(token, "test-token")
        self.assertEqual(method, "sendMessage")
        self.assertEqual(request_data["chat_id"], 76)
        self.assertIn(public_url, str(request_data["text"]))

    async def test_public_pending_sprint_api_requires_project_scoped_token(self) -> None:
        second_context = "github.com/example/pending-token-second"
        second_phone = "9009"
        second_git_address = "https://github.com/example/pending-token-second.git"
        self.register_project(
            context_key=second_context,
            project_phone=second_phone,
            git_address=second_git_address,
            project_name="Pending Token Second",
        )

        async def stage(
            project_phone: str,
            git_address: str,
            actor_id: str,
            actor_phone: str,
        ) -> dict[str, object]:
            with patch.object(
                main,
                "cloudflared_public_base_url",
                return_value="https://pending.example.test",
            ):
                stage_status, stage_body = await asgi_request(
                    "/api/v1/telegram/agents/parallel",
                    method="POST",
                    payload={
                        "project_phone": project_phone,
                        "git_address": git_address,
                        "sprint": {"title": f"Pending token {project_phone}"},
                        "agents": {
                            "overwrite": True,
                            "items": [
                                {
                                    "id": actor_id,
                                    "name": f"Pending Token Agent {project_phone}",
                                    "phone": actor_phone,
                                    "tasks": ["Start through the protected public API."],
                                }
                            ],
                        },
                    },
                )
            self.assertEqual(stage_status, 200)
            self.assertIsInstance(stage_body, dict)
            assert isinstance(stage_body, dict)
            return stage_body

        primary = await stage(
            self.PROJECT_PHONE,
            "https://github.com/example/actor-import.git",
            "primary-token-agent",
            "2071",
        )
        second = await stage(
            second_phone,
            second_git_address,
            "second-token-agent",
            "2072",
        )
        primary_query = urllib.parse.parse_qs(
            urllib.parse.urlsplit(primary["pending_sprints_url"]).query
        )
        second_query = urllib.parse.parse_qs(
            urllib.parse.urlsplit(second["pending_sprints_url"]).query
        )
        primary_token = primary_query["pending_token"][0]
        second_token = second_query["pending_token"][0]
        self.assertTrue(primary_token)
        self.assertTrue(second_token)
        self.assertNotEqual(primary_token, second_token)
        self.assertEqual(primary_query["project_phone"], [self.PROJECT_PHONE])
        self.assertEqual(
            primary_query["sprint_id"],
            [primary["pending_sprint"]["id"]],
        )

        primary_id = primary["pending_sprint"]["id"]
        list_url = f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints"
        detail_url = f"{list_url}/{primary_id}"
        start_url = f"{detail_url}/start"
        public_request = {
            "host": "pending.example.test",
            "scheme": "https",
        }
        for target, method in (
            (list_url, "GET"),
            (detail_url, "GET"),
            (start_url, "POST"),
        ):
            denied_status, denied = await asgi_request(
                target,
                method=method,
                **public_request,
            )
            self.assertEqual(denied_status, 403, denied)
            self.assertIsInstance(denied, dict)
            assert isinstance(denied, dict)
            self.assertEqual(
                denied["detail"]["error"],
                "pending_sprints_access_denied",
            )

        wrong_status, wrong = await asgi_request(
            list_url,
            headers=[(b"x-pending-sprints-token", b"wrong-token")],
            **public_request,
        )
        self.assertEqual(wrong_status, 403, wrong)

        spoofed_local_status, spoofed_local = await asgi_request(
            list_url,
            host="localhost:8025",
            client_host="192.0.2.10",
        )
        self.assertEqual(spoofed_local_status, 403, spoofed_local)
        self.assertEqual(
            spoofed_local["detail"]["error"],
            "pending_sprints_access_denied",
        )

        token_headers = [
            (b"x-pending-sprints-token", primary_token.encode("ascii"))
        ]
        local_list_status, _ = await asgi_request(list_url)
        local_detail_status, _ = await asgi_request(detail_url)
        public_list_status, _ = await asgi_request(
            list_url,
            headers=token_headers,
            **public_request,
        )
        public_detail_status, _ = await asgi_request(
            detail_url,
            headers=token_headers,
            **public_request,
        )
        self.assertEqual(local_list_status, 200)
        self.assertEqual(local_detail_status, 200)
        self.assertEqual(public_list_status, 200)
        self.assertEqual(public_detail_status, 200)

        foreign_status, foreign = await asgi_request(
            f"/api/v1/projects/{second_phone}/pending-sprints",
            headers=token_headers,
            **public_request,
        )
        self.assertEqual(foreign_status, 403, foreign)
        second_token_status, _ = await asgi_request(
            f"/api/v1/projects/{second_phone}/pending-sprints",
            headers=[
                (b"x-pending-sprints-token", second_token.encode("ascii"))
            ],
            **public_request,
        )
        self.assertEqual(second_token_status, 200)

        start_status, started = await asgi_request(
            start_url,
            method="POST",
            headers=token_headers,
            **public_request,
        )
        self.assertEqual(start_status, 200, started)
        self.assertEqual(len(main.queues["worker-all"]), 1)

    async def test_pending_sprint_rejects_start_after_repository_changes(self) -> None:
        agents_before = main.agents_path.read_text(encoding="utf-8")
        stage_status, staged = await asgi_request(
            "/api/v1/telegram/agents/parallel",
            method="POST",
            payload={
                "git_address": "https://github.com/example/actor-import.git",
                "sprint": {"title": "Repository Identity"},
                "agents": {
                    "overwrite": True,
                    "items": [
                        {
                            "id": "repository-identity-agent",
                            "name": "Repository Identity Agent",
                            "phone": "2073",
                            "tasks": ["Do not start after repository drift."],
                        }
                    ],
                },
            },
        )
        self.assertEqual(stage_status, 200)
        self.assertIsInstance(staged, dict)
        assert isinstance(staged, dict)
        pending = staged["pending_sprint"]
        self.assertEqual(
            pending["git_address"],
            "https://github.com/example/actor-import.git",
        )
        self.assertEqual(
            pending["repository_key"],
            "github.com/example/actor-import",
        )

        moved_address = "https://github.com/example/repository-moved.git"
        moved_context = "github.com/example/repository-moved"
        config = main.read_git_config_file()
        moved_project = {
            **config[main.PROJECTS_KEY].pop(self.PROJECT_CONTEXT),
            "git_address": moved_address,
            "git_context_key": moved_context,
        }
        config[main.PROJECTS_KEY][moved_context] = moved_project
        config[main.PHONE_GIT_CONTEXTS_KEY][self.PROJECT_PHONE] = {
            **moved_project,
            "phone": self.PROJECT_PHONE,
        }
        main.write_git_config_file(config)

        sprint_id = pending["id"]
        start_status, start_body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/{sprint_id}/start",
            method="POST",
        )
        self.assertEqual(start_status, 409, start_body)
        self.assertIsInstance(start_body, dict)
        assert isinstance(start_body, dict)
        self.assertEqual(
            start_body["detail"]["error"],
            "pending_sprint_repository_changed",
        )
        self.assertEqual(main.agents_path.read_text(encoding="utf-8"), agents_before)
        self.assertTrue(all(not queue for queue in main.queues.values()))
        self.assertFalse(main.sprint_history_path.exists())
        stored = main.read_pending_sprints_file()["projects"][self.PROJECT_CONTEXT][
            "sprints"
        ][0]
        self.assertEqual(stored["status"], "pending")
        self.assertEqual(stored["activation_attempts"], 0)

    async def test_parallel_telegram_endpoint_rejects_sequential_graph(self) -> None:
        agents_before = main.agents_path.read_text(encoding="utf-8")
        status_code, body = await asgi_request(
            "/api/v1/telegram/agents/parallel",
            method="POST",
            payload={
                "git_address": "https://github.com/example/actor-import.git",
                "agents": {"overwrite": True},
                "execution": {
                    "mode": "sequential",
                    "start_node": "build",
                    "required_approvals": 2,
                    "reviewers": [
                        {
                            "id": "mode-reviewer-one",
                            "name": "Mode Reviewer One",
                            "phone": "2074",
                        },
                        {
                            "id": "mode-reviewer-two",
                            "name": "Mode Reviewer Two",
                            "phone": "2075",
                        },
                    ],
                },
                "nodes": [
                    {
                        "id": "build",
                        "agent": {
                            "id": "mode-builder",
                            "name": "Mode Builder",
                            "phone": "2076",
                        },
                        "tasks": ["This graph requires sequential execution."],
                        "transitions": {"DONE": "finished"},
                    },
                    {"id": "finished", "type": "terminal", "status": "DONE"},
                ],
            },
        )
        self.assertEqual(status_code, 400, body)
        self.assertIsInstance(body, dict)
        assert isinstance(body, dict)
        self.assertEqual(
            body["detail"]["error"],
            "telegram_assignment_mode_conflict",
        )
        self.assertEqual(main.agents_path.read_text(encoding="utf-8"), agents_before)
        self.assertTrue(all(not queue for queue in main.queues.values()))
        self.assertFalse(main.pending_sprints_path.exists())

    async def test_concurrent_pending_sprint_start_has_single_winner(self) -> None:
        stage_status, staged = await asgi_request(
            "/api/v1/telegram/agents/parallel",
            method="POST",
            payload={
                "git_address": "https://github.com/example/actor-import.git",
                "sprint": {"title": "Concurrent Start"},
                "agents": {
                    "overwrite": True,
                    "items": [
                        {
                            "id": "concurrent-start-agent",
                            "name": "Concurrent Start Agent",
                            "phone": "2077",
                            "tasks": ["Queue exactly once."],
                        }
                    ],
                },
            },
        )
        self.assertEqual(stage_status, 200)
        self.assertIsInstance(staged, dict)
        assert isinstance(staged, dict)
        sprint_id = staged["pending_sprint"]["id"]
        start_url = (
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/"
            f"{sprint_id}/start"
        )

        first, second = await asyncio.gather(
            asgi_request(start_url, method="POST"),
            asgi_request(start_url, method="POST"),
        )
        self.assertEqual(sorted((first[0], second[0])), [200, 409])
        winner = first[1] if first[0] == 200 else second[1]
        loser = first[1] if first[0] == 409 else second[1]
        self.assertIsInstance(winner, dict)
        self.assertIsInstance(loser, dict)
        assert isinstance(winner, dict) and isinstance(loser, dict)
        self.assertTrue(winner["started"])
        self.assertIn(
            loser["detail"]["error"],
            {
                "pending_sprint_activation_in_progress",
                "pending_sprint_already_started",
            },
        )
        self.assertEqual(len(main.queues["worker-all"]), 1)
        self.assertEqual(
            main.queue_item_message(main.queues["worker-all"][0]),
            "Queue exactly once.",
        )

    async def test_another_pending_sprint_cannot_start_during_project_activation(
        self,
    ) -> None:
        agents_before = main.agents_path.read_text(encoding="utf-8")

        async def stage(
            sprint_title: str,
            actor_id: str,
            actor_phone: str,
        ) -> dict[str, object]:
            status_code, body = await asgi_request(
                "/api/v1/telegram/agents/parallel",
                method="POST",
                payload={
                    "git_address": "https://github.com/example/actor-import.git",
                    "sprint": {"title": sprint_title},
                    "agents": {
                        "overwrite": True,
                        "items": [
                            {
                                "id": actor_id,
                                "name": sprint_title + " Agent",
                                "phone": actor_phone,
                                "tasks": [sprint_title + " task"],
                            }
                        ],
                    },
                },
            )
            self.assertEqual(status_code, 200)
            self.assertIsInstance(body, dict)
            assert isinstance(body, dict)
            return body

        first = await stage("Held Project Start", "held-project-agent", "2079")
        second = await stage("Blocked Project Start", "blocked-project-agent", "2080")
        first_id = first["pending_sprint"]["id"]
        second_id = second["pending_sprint"]["id"]
        first_claim = main.update_pending_sprint_activation_file(
            self.PROJECT_CONTEXT,
            first_id,
            action="claim",
        )
        self.assertEqual(first_claim["status"], "activating")
        self.assertTrue(first_claim["activation_attempt_id"])

        blocked_status, blocked = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/{second_id}/start",
            method="POST",
        )
        self.assertEqual(blocked_status, 409, blocked)
        self.assertIsInstance(blocked, dict)
        assert isinstance(blocked, dict)
        self.assertEqual(
            blocked["detail"]["error"],
            "project_sprint_activation_in_progress",
        )
        self.assertEqual(blocked["detail"]["pending_sprint_id"], first_id)
        self.assertEqual(main.agents_path.read_text(encoding="utf-8"), agents_before)
        self.assertTrue(all(not queue for queue in main.queues.values()))
        self.assertFalse(main.sprint_history_path.exists())

        storage = main.read_pending_sprints_file()
        records = {
            item["id"]: item
            for item in storage["projects"][self.PROJECT_CONTEXT]["sprints"]
        }
        self.assertEqual(records[first_id]["status"], "activating")
        self.assertEqual(records[second_id]["status"], "pending")
        self.assertEqual(records[second_id]["activation_attempts"], 0)

    async def test_stale_pending_activation_can_be_reclaimed_with_new_attempt(self) -> None:
        stage_status, staged = await asgi_request(
            "/api/v1/telegram/agents/parallel",
            method="POST",
            payload={
                "git_address": "https://github.com/example/actor-import.git",
                "sprint": {"title": "Recover Stale Activation"},
                "agents": {
                    "overwrite": True,
                    "items": [
                        {
                            "id": "stale-activation-agent",
                            "name": "Stale Activation Agent",
                            "phone": "2078",
                            "tasks": ["Recover and queue this once."],
                        }
                    ],
                },
            },
        )
        self.assertEqual(stage_status, 200)
        self.assertIsInstance(staged, dict)
        assert isinstance(staged, dict)
        sprint_id = staged["pending_sprint"]["id"]
        first_claim = main.update_pending_sprint_activation_file(
            self.PROJECT_CONTEXT,
            sprint_id,
            action="claim",
        )
        first_attempt_id = first_claim["activation_attempt_id"]
        self.assertTrue(first_attempt_id)

        storage = main.read_pending_sprints_file()
        stored = next(
            item
            for item in storage["projects"][self.PROJECT_CONTEXT]["sprints"]
            if item["id"] == sprint_id
        )
        stored["activating_at"] = "2000-01-01T00:00:00+00:00"
        stored["retry_at"] = "2000-01-01T00:01:00+00:00"
        main.write_pending_sprints_file(storage)

        detail_status, detail = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/{sprint_id}"
        )
        self.assertEqual(detail_status, 200)
        self.assertIsInstance(detail, dict)
        assert isinstance(detail, dict)
        summary = detail["pending_sprint"]
        self.assertEqual(summary["status"], "activating")
        self.assertTrue(summary["startable"])
        self.assertTrue(summary["retry_at"])

        retry_status, retried = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/{sprint_id}/start",
            method="POST",
        )
        self.assertEqual(retry_status, 200, retried)
        self.assertIsInstance(retried, dict)
        assert isinstance(retried, dict)
        second_attempt_id = retried["claimed_sprint"]["activation_attempt_id"]
        self.assertTrue(second_attempt_id)
        self.assertNotEqual(first_attempt_id, second_attempt_id)
        self.assertEqual(retried["claimed_sprint"]["activation_attempts"], 2)
        self.assertEqual(len(main.queues["worker-all"]), 1)

    async def test_stale_activation_reconciles_completed_import_without_duplicates(self) -> None:
        payload = {
            "git_address": "https://github.com/example/actor-import.git",
            "sprint": {"title": "Reconcile Completed Import"},
            "agents": {
                "overwrite": True,
                "items": [
                    {
                        "id": "reconciled-activation-agent",
                        "name": "Reconciled Activation Agent",
                        "phone": "2081",
                        "tasks": ["Queue this task exactly once."],
                    }
                ],
            },
        }
        stage_status, staged = await asgi_request(
            "/api/v1/telegram/agents/parallel",
            method="POST",
            payload=payload,
        )
        self.assertEqual(stage_status, 200)
        self.assertIsInstance(staged, dict)
        assert isinstance(staged, dict)
        sprint_id = staged["pending_sprint"]["id"]

        first_claim = main.update_pending_sprint_activation_file(
            self.PROJECT_CONTEXT,
            sprint_id,
            action="claim",
        )
        self.assertEqual(first_claim["status"], "activating")
        imported = await main.import_project_actors_data(
            self.PROJECT_PHONE,
            payload,
            source="telegram",
            source_filename="telegram-message.json",
            activate_sequential=False,
            expected_git_context_key=self.PROJECT_CONTEXT,
            expected_repository_key="github.com/example/actor-import",
            pending_sprint_id=sprint_id,
        )
        imported_sprint_id = imported["sprint"]["id"]
        self.assertEqual(len(main.queues["worker-all"]), 1)
        original_queue_item_id = main.queue_item_id(main.queues["worker-all"][0])

        storage = main.read_pending_sprints_file()
        stored = next(
            item
            for item in storage["projects"][self.PROJECT_CONTEXT]["sprints"]
            if item["id"] == sprint_id
        )
        stored["activating_at"] = "2000-01-01T00:00:00+00:00"
        stored["retry_at"] = "2000-01-01T00:01:00+00:00"
        main.write_pending_sprints_file(storage)

        retry_status, retried = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/{sprint_id}/start",
            method="POST",
        )
        self.assertEqual(retry_status, 200, retried)
        self.assertIsInstance(retried, dict)
        assert isinstance(retried, dict)
        self.assertTrue(retried["reconciled"])
        self.assertEqual(retried["sprint"]["id"], imported_sprint_id)
        self.assertEqual(len(main.queues["worker-all"]), 1)
        self.assertEqual(
            main.queue_item_id(main.queues["worker-all"][0]),
            original_queue_item_id,
        )

        list_status, pending_list = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints"
        )
        self.assertEqual(list_status, 200)
        self.assertIsInstance(pending_list, dict)
        assert isinstance(pending_list, dict)
        self.assertEqual(pending_list["pending_count"], 0)

    async def test_reclaimed_activation_waits_for_inflight_import_marker(self) -> None:
        payload = {
            "git_address": "https://github.com/example/actor-import.git",
            "sprint": {"title": "Inflight Reconciliation"},
            "agents": {
                "overwrite": True,
                "items": [
                    {
                        "id": "inflight-reconciliation-agent",
                        "name": "Inflight Reconciliation Agent",
                        "phone": "2082",
                        "tasks": ["Do not duplicate this inflight task."],
                    }
                ],
            },
        }
        stage_status, staged = await asgi_request(
            "/api/v1/telegram/agents/parallel",
            method="POST",
            payload=payload,
        )
        self.assertEqual(stage_status, 200)
        self.assertIsInstance(staged, dict)
        assert isinstance(staged, dict)
        sprint_id = staged["pending_sprint"]["id"]
        main.update_pending_sprint_activation_file(
            self.PROJECT_CONTEXT,
            sprint_id,
            action="claim",
        )
        storage = main.read_pending_sprints_file()
        stored = next(
            item
            for item in storage["projects"][self.PROJECT_CONTEXT]["sprints"]
            if item["id"] == sprint_id
        )
        stored["activating_at"] = "2000-01-01T00:00:00+00:00"
        stored["retry_at"] = "2000-01-01T00:01:00+00:00"
        main.write_pending_sprints_file(storage)

        reached_history_write = asyncio.Event()
        release_history_write = asyncio.Event()
        original_record_import = main.record_project_sprint_import

        async def delayed_record_import(**kwargs: object) -> dict[str, object]:
            reached_history_write.set()
            await release_history_write.wait()
            return await original_record_import(**kwargs)

        with patch.object(
            main,
            "record_project_sprint_import",
            new=delayed_record_import,
        ):
            first_import_task = asyncio.create_task(
                main.import_project_actors_data(
                    self.PROJECT_PHONE,
                    payload,
                    source="telegram",
                    source_filename="telegram-message.json",
                    activate_sequential=False,
                    expected_git_context_key=self.PROJECT_CONTEXT,
                    expected_repository_key="github.com/example/actor-import",
                    pending_sprint_id=sprint_id,
                )
            )
            await asyncio.wait_for(reached_history_write.wait(), timeout=2)
            self.assertEqual(len(main.queues["worker-all"]), 1)
            original_queue_item_id = main.queue_item_id(
                main.queues["worker-all"][0]
            )
            retry_task = asyncio.create_task(
                asgi_request(
                    f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/"
                    f"{sprint_id}/start",
                    method="POST",
                )
            )
            try:
                await asyncio.sleep(0.05)
                self.assertFalse(retry_task.done())
            finally:
                release_history_write.set()

            imported, retry_response = await asyncio.gather(
                first_import_task,
                retry_task,
            )

        retry_status, retried = retry_response
        self.assertEqual(retry_status, 200, retried)
        self.assertIsInstance(retried, dict)
        assert isinstance(retried, dict)
        self.assertTrue(retried["reconciled"])
        self.assertEqual(retried["claimed_sprint"]["activation_attempts"], 2)
        self.assertEqual(retried["sprint"]["id"], imported["sprint"]["id"])
        self.assertEqual(len(main.queues["worker-all"]), 1)
        self.assertEqual(
            main.queue_item_id(main.queues["worker-all"][0]),
            original_queue_item_id,
        )
        history = main.read_sprint_history_file()
        matching_records = [
            item
            for item in history["projects"][self.PROJECT_CONTEXT]["sprints"]
            if item.get("pending_sprint_id") == sprint_id
        ]
        self.assertEqual(len(matching_records), 1)

    async def test_telegram_unknown_git_repository_does_not_mutate_agents(self) -> None:
        status_code, body = await asgi_request(
            "/api/v1/telegram/agents",
            method="POST",
            payload={
                "git_address": "https://github.com/example/not-registered.git",
                "agents": {
                    "overwrite": True,
                    "items": [{"name": "Must Not Be Imported", "tasks": []}],
                },
            },
        )

        self.assertEqual(status_code, 404)
        self.assertIsInstance(body, dict)
        assert isinstance(body, dict)
        self.assertEqual(body["detail"]["error"], "project_not_found")
        agent_ids = {agent["id"] for agent in main.read_agents_file()}
        self.assertIn("old-project-actor", agent_ids)
        self.assertNotIn("must-not-be-imported", agent_ids)

    async def test_telegram_validation_error_is_replied_to_and_acknowledged(self) -> None:
        actor_json = {
            "git_address": "https://github.com/example/not-registered.git",
            "agents": {
                "overwrite": True,
                "items": [{"name": "Must Not Be Imported", "tasks": []}],
            },
        }
        update = {
            "update_id": 123456,
            "message": {
                "message_id": 77,
                "chat": {"id": 99},
                "text": json.dumps(actor_json),
            },
        }

        with patch.object(main, "send_telegram_import_error_reply") as send_error:
            status_code, body = await asgi_request(
                "/api/v1/telegram/agents",
                method="POST",
                payload=update,
            )

        self.assertEqual(status_code, 200)
        self.assertIsInstance(body, dict)
        assert isinstance(body, dict)
        self.assertFalse(body["ok"])
        self.assertTrue(body["accepted"])
        self.assertEqual(body["status_code"], 404)
        self.assertEqual(body["error"]["error"], "project_not_found")
        send_error.assert_called_once()
        agent_ids = {agent["id"] for agent in main.read_agents_file()}
        self.assertIn("old-project-actor", agent_ids)
        self.assertNotIn("must-not-be-imported", agent_ids)

    async def test_telegram_ambiguous_repository_uses_git_context_key(self) -> None:
        git_address = "https://github.com/example/shared-import.git"
        backend_context = "github.com/example/shared-import#backend"
        frontend_context = "github.com/example/shared-import#frontend"

        def project(context_key: str, phone: str) -> dict[str, object]:
            return {
                "project_name": context_key.rsplit("#", 1)[-1].title(),
                "git_address": git_address,
                "git_context_key": context_key,
                "project_phone": phone,
                "groups": [],
                "group_relationships": [],
                "customer_reporting": {},
            }

        backend = project(backend_context, self.PROJECT_PHONE)
        frontend = project(frontend_context, "9009")
        config = {
            main.PROJECTS_KEY: {
                backend_context: backend,
                frontend_context: frontend,
            },
            main.PHONE_GIT_CONTEXTS_KEY: {
                self.PROJECT_PHONE: {**backend, "phone": self.PROJECT_PHONE},
                "9009": {**frontend, "phone": "9009"},
            },
        }
        main.git_config_path.write_text(
            json.dumps(config, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        import_section = {
            "overwrite": True,
            "items": [{"id": "frontend-agent", "name": "Frontend Agent"}],
        }

        ambiguous_status, ambiguous_body = await asgi_request(
            "/api/v1/telegram/agents",
            method="POST",
            payload={"git_address": git_address, "agents": import_section},
        )
        self.assertEqual(ambiguous_status, 409)
        self.assertIsInstance(ambiguous_body, dict)
        assert isinstance(ambiguous_body, dict)
        self.assertEqual(ambiguous_body["detail"]["error"], "ambiguous_project")

        selected_status, selected_body = await asgi_request(
            "/api/v1/telegram/agents",
            method="POST",
            payload={
                "git_address": git_address,
                "git_context_key": frontend_context,
                "agents": import_section,
            },
        )
        self.assertEqual(selected_status, 200)
        self.assertIsInstance(selected_body, dict)
        assert isinstance(selected_body, dict)
        self.assertTrue(selected_body["staged"])
        self.assertEqual(selected_body["project_phone"], "9009")
        self.assertEqual(selected_body["git_context_key"], frontend_context)

    async def test_telegram_webhook_enforces_secret_and_chat_allowlist(self) -> None:
        os.environ["TELEGRAM_WEBHOOK_SECRET"] = "test-secret"
        os.environ["TELEGRAM_ALLOWED_CHAT_IDS"] = "99,-100123"
        actor_json = {
            "project_id": self.PROJECT_PHONE,
            "actors": {
                "overwrite": True,
                "items": [{"name": "Allowed Telegram Actor", "tasks": []}],
            },
        }

        missing_secret_status, _ = await asgi_request(
            "/api/v1/telegram/actors",
            method="POST",
            payload={
                "message": {
                    "chat": {"id": 99},
                    "text": json.dumps(actor_json),
                }
            },
        )
        self.assertEqual(missing_secret_status, 403)

        denied_chat_status, _ = await asgi_request(
            "/api/v1/telegram/actors",
            method="POST",
            payload={
                "message": {
                    "chat": {"id": 3},
                    "text": json.dumps(actor_json),
                }
            },
            headers=[(b"x-telegram-bot-api-secret-token", b"test-secret")],
        )
        self.assertEqual(denied_chat_status, 403)

        allowed_status, allowed_body = await asgi_request(
            "/api/v1/telegram/actors",
            method="POST",
            payload={
                "message": {
                    "chat": {"id": 99},
                    "text": json.dumps(actor_json),
                }
            },
            headers=[(b"x-telegram-bot-api-secret-token", b"test-secret")],
        )
        self.assertEqual(allowed_status, 200)
        self.assertIsInstance(allowed_body, dict)
        assert isinstance(allowed_body, dict)
        self.assertTrue(allowed_body["staged"])
        activated = await self.start_staged_sprint(allowed_body)
        self.assertEqual(activated["imported_actor_count"], 1)

    def test_ui_exposes_actor_import_and_bulk_delete_controls(self) -> None:
        html = main.render_index_v2()
        for marker in (
            'id="projectActorsJsonFile"',
            'id="importProjectActorsButton"',
            'id="deleteAllProjectActorsButton"',
            'id="projectSprints"',
            'id="refreshProjectSprintsButton"',
            'id="projectTelegramHistoryForwarding"',
            'class="page-tab" data-view="pending-sprints"',
            'class="view pending-sprints-view" data-view="pending-sprints"',
            'id="pendingSprintsProjectSelect"',
            'id="pendingSprintsProjectSummary"',
            'id="refreshPendingSprintsButton"',
            'id="pendingSprintsStatus"',
            'id="pendingSprints"',
            'id="pendingSprintPreviewTitle"',
            'id="pendingSprintJsonPreview"',
            "async function importProjectActorsFromJson()",
            "async function deleteAllProjectActors()",
            "async function refreshProjectSprints()",
            "async function updateTelegramHistoryForwarding()",
            "pendingSprintsViewIsActive",
            "renderPendingSprintsProjectOptions",
            "renderPendingSprints",
            "refreshPendingSprints",
            "loadPendingSprintPreview",
            "startPendingSprint",
            "applyPendingSprintsDeepLink",
            "syncPendingSprintsUrl",
            'data-action="preview-pending-sprint"',
            'data-action="start-pending-sprint"',
            "download-project-sprint",
            "agents.overwrite: true",
            "/agents/import",
        ):
            self.assertIn(marker, html)


if __name__ == "__main__":
    unittest.main()
