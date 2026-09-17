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

    async def test_sequential_runtime_changes_identity_from_queue_graph(self) -> None:
        import_status, import_body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/import",
            method="POST",
            payload={
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
        self.assertEqual(len(first_body["team"]), 2)
        self.assertIn("send_endpoints", first_body["communication"])
        self.assertFalse(first_body["identity_reused"])
        self.assertTrue(first_body["execution_authorized"])
        self.assertFalse(first_body["requires_additional_confirmation"])
        self.assertEqual(len(main.queues["worker-all"]), 0)

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

    async def _test_legacy_sequential_whoami_moves_to_next_role_without_parallel_work(self) -> None:
        import_status, import_body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/import",
            method="POST",
            payload={
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
        node_assignment_id = node_body["active_task"]["metadata"]["assignment_id"]

        first_review_status, first_review = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/2153/whoami",
            method="POST",
            payload={
                "assignment_id": node_assignment_id,
                "status": "DONE",
                "result": "Analysis finished.",
            },
        )
        self.assertEqual(first_review_status, 200)
        self.assertIsInstance(first_review, dict)
        assert isinstance(first_review, dict)
        self.assertEqual(first_review["agent"]["id"], "reviewer-one")
        self.assertEqual(first_review["assignment"]["phase"], "review")
        self.assertEqual(len(main.queues["worker-all"]), 1, first_review)

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

    async def test_sequential_telegram_url_starts_first_graph_node(self) -> None:
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
        self.assertEqual(body["assignment_mode"], "sequential")
        self.assertEqual(body["active_agent"]["id"], "url-sequential-a")
        self.assertEqual(body["queued_task_count"], 1)
        self.assertEqual(body["queued_queue_item_count"], 1)
        self.assertEqual(body["deferred_task_count"], 1)
        self.assertEqual(
            body["sequential_poll_endpoint"],
            f"/worker/all/{self.PROJECT_PHONE}?to_phone={self.PROJECT_PHONE}",
        )
        self.assertEqual(len(main.queues["worker-all"]), 1)
        self.assertEqual(len(main.queues["tester-all"]), 0)

        poll_status, node = await asgi_request(body["sequential_poll_endpoint"])
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
        self.assertEqual(body["assignment_mode"], "parallel")
        self.assertEqual(body["queued_task_count"], 2)
        self.assertEqual(body["deferred_task_count"], 0)
        self.assertIsNone(body["sequential_poll_endpoint"])
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
        self.assertEqual(body["project_phone"], self.PROJECT_PHONE)
        self.assertEqual(body["project"]["git_context_key"], self.PROJECT_CONTEXT)
        self.assertEqual(body["imported_agent_count"], 1)
        self.assertEqual(body["queued_task_count"], 1)

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
        self.assertEqual(selected_body["project_phone"], "9009")
        self.assertEqual(
            selected_body["project"]["git_context_key"],
            frontend_context,
        )

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
