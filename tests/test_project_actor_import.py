import asyncio
import json
import os
import tempfile
import unittest
import urllib.parse
from collections import deque
from pathlib import Path

import main


async def asgi_request(
    target: str,
    *,
    method: str = "GET",
    payload: object | None = None,
    headers: list[tuple[bytes, bytes]] | None = None,
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

    request_headers = [(b"host", b"testserver:8025")]
    if payload is not None:
        request_headers.append((b"content-type", b"application/json"))
    request_headers.extend(headers or [])
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": parsed.path,
        "raw_path": parsed.path.encode("ascii"),
        "query_string": parsed.query.encode("ascii"),
        "root_path": "",
        "headers": request_headers,
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", 8025),
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
    )

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        temp_path = Path(self.temp_dir.name)
        self.original_values = {
            "git_config_path": main.git_config_path,
            "agents_path": main.agents_path,
            "history_path": main.history_path,
            "git_config_lock": main.git_config_lock,
            "agents_lock": main.agents_lock,
            "history_lock": main.history_lock,
            "group_task_submission_lock": main.group_task_submission_lock,
            "queues": main.queues,
            "locks": main.locks,
        }
        main.git_config_path = temp_path / "port_git_map.json"
        main.agents_path = temp_path / "agents.json"
        main.history_path = temp_path / "conversation_log.jsonl"
        main.git_config_lock = asyncio.Lock()
        main.agents_lock = asyncio.Lock()
        main.history_lock = asyncio.Lock()
        main.group_task_submission_lock = asyncio.Lock()
        main.queues = {name: deque() for name in main.QUEUE_DEFINITIONS}
        main.locks = {name: asyncio.Lock() for name in main.QUEUE_DEFINITIONS}
        self.original_telegram_env = {
            name: os.environ.get(name)
            for name in self.TELEGRAM_ENV_NAMES
        }
        for name in self.TELEGRAM_ENV_NAMES:
            os.environ.pop(name, None)
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

    async def test_overwrite_import_replaces_project_actors_and_queues_tasks(self) -> None:
        payload = {
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

        second_status, second_body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/actors/import",
            method="POST",
            payload={
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

    async def test_delete_all_removes_project_actors_and_pending_tasks_only(self) -> None:
        import_status, _ = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/actors/import",
            method="POST",
            payload={
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

    async def test_telegram_text_json_imports_actors_and_tasks(self) -> None:
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
        self.assertEqual(body["source"], "telegram")
        self.assertEqual(body["imported_actor_count"], 1)
        self.assertEqual(body["queued_task_count"], 1)

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
        self.assertEqual(allowed_body["imported_actor_count"], 1)

    def test_ui_exposes_actor_import_and_bulk_delete_controls(self) -> None:
        html = main.render_index_v2()
        for marker in (
            'id="projectActorsJsonFile"',
            'id="importProjectActorsButton"',
            'id="deleteAllProjectActorsButton"',
            "async function importProjectActorsFromJson()",
            "async function deleteAllProjectActors()",
            "agents.overwrite: true",
            "/agents/import",
        ):
            self.assertIn(marker, html)


if __name__ == "__main__":
    unittest.main()
