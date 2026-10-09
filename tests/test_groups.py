import asyncio
import json
import tempfile
import unittest
import urllib.parse
from collections import deque
from copy import deepcopy
from pathlib import Path
from typing import Any

import main


async def asgi_request(
    path: str,
    *,
    method: str = "GET",
    payload: object | None = None,
) -> tuple[int, object]:
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

    headers = [(b"host", b"testserver:8025")]
    if payload is not None:
        headers.append((b"content-type", b"application/json"))
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("ascii"),
        "query_string": b"",
        "root_path": "",
        "headers": headers,
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


class GroupsApiTests(unittest.IsolatedAsyncioTestCase):
    PROJECT_PHONE = "9000"
    PROJECT_CONTEXT = "github.com/example/groups-api"
    TEMPLATE_ID = "backend_dev_team_v1"

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        temp_path = Path(self.temp_dir.name)
        self.original_values = {
            "git_config_path": main.git_config_path,
            "agents_path": main.agents_path,
            "group_templates_path": main.group_templates_path,
            "history_path": main.history_path,
            "git_config_lock": main.git_config_lock,
            "agents_lock": main.agents_lock,
            "history_lock": main.history_lock,
            "group_task_submission_lock": main.group_task_submission_lock,
            "queues": main.queues,
            "locks": main.locks,
            "resolve_git_reference": main.resolve_git_reference,
        }
        main.git_config_path = temp_path / "port_git_map.json"
        main.agents_path = temp_path / "agents.json"
        main.group_templates_path = temp_path / "group_templates.json"
        main.history_path = temp_path / "conversation_log.jsonl"
        main.git_config_lock = asyncio.Lock()
        main.agents_lock = asyncio.Lock()
        main.history_lock = asyncio.Lock()
        main.group_task_submission_lock = asyncio.Lock()
        main.queues = {name: deque() for name in main.QUEUE_DEFINITIONS}
        main.locks = {name: asyncio.Lock() for name in main.QUEUE_DEFINITIONS}
        main.resolve_git_reference = lambda _address: {
            "git_commit": "a" * 40,
            "git_commit_short": "a" * 12,
        }

        self.write_registry()
        self.write_project()
        self.write_agents([])

    def tearDown(self) -> None:
        for name, value in self.original_values.items():
            setattr(main, name, value)
        self.temp_dir.cleanup()

    def write_registry(self) -> None:
        registry = {
            "schema_version": 1,
            "agent_specs": {
                "analytic_v2": {
                    "name": "System Analyst",
                    "profile": "Ты системный аналитик тестовой группы.",
                    "parameters": {"role": "system_analyst"},
                    "status": "active",
                },
                "programmer_backend_v1": {
                    "name": "Backend Developer",
                    "profile": "Ты backend-разработчик тестовой группы.",
                    "parameters": {"role": "backend_developer"},
                    "status": "active",
                },
                "qa_tester_v1": {
                    "name": "QA Engineer",
                    "profile": "Ты QA-инженер тестовой группы.",
                    "parameters": {"role": "qa_engineer"},
                    "status": "active",
                },
            },
            "group_templates": {
                self.TEMPLATE_ID: {
                    "template_id": self.TEMPLATE_ID,
                    "name": "Backend Development Team",
                    "description": "Test backend group",
                    "agent_templates": [
                        {
                            "role": "system_analyst",
                            "spec": "analytic_v2",
                            "is_entrypoint": True,
                        },
                        {
                            "role": "backend_developer",
                            "spec": "programmer_backend_v1",
                        },
                        {"role": "qa_engineer", "spec": "qa_tester_v1"},
                    ],
                    "internal_connections": [
                        {
                            "id": "analyst-to-backend",
                            "from": "system_analyst",
                            "to": "backend_developer",
                            "channel_type": "queue",
                            "queue": "worker-all",
                            "event": "spec_approved",
                        },
                        {
                            "id": "backend-to-qa",
                            "from": "backend_developer",
                            "to": "qa_engineer",
                            "channel_type": "queue",
                            "queue": "tester-all",
                            "event": "code_ready",
                        },
                    ],
                    "reporting_rule": {
                        "report_from": "system_analyst",
                        "report_to": "project_manager",
                        "trigger": "all_tasks_completed",
                        "output_queue": "worker-all",
                    },
                }
            },
            "group_topologies": {},
        }
        main.group_templates_path.write_text(
            json.dumps(registry, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    def write_project(self) -> None:
        project = {
            "project_name": "Groups API Project",
            "git_address": "https://github.com/example/groups-api.git",
            "git_context_key": self.PROJECT_CONTEXT,
            "project_phone": self.PROJECT_PHONE,
            "groups": [],
            "group_relationships": [],
            "customer_reporting": {},
        }
        mapping = {
            **project,
            "phone": self.PROJECT_PHONE,
        }
        main.git_config_path.write_text(
            json.dumps(
                {
                    main.PROJECTS_KEY: {self.PROJECT_CONTEXT: project},
                    main.PHONE_GIT_CONTEXTS_KEY: {self.PROJECT_PHONE: mapping},
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

    def write_agents(self, agents: list[dict[str, object]]) -> None:
        main.agents_path.write_text(
            json.dumps({"agents": agents}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def stored_agents(self) -> list[dict[str, Any]]:
        data = json.loads(main.agents_path.read_text(encoding="utf-8"))
        return data["agents"]

    def stored_groups(self) -> list[dict[str, Any]]:
        data = json.loads(main.git_config_path.read_text(encoding="utf-8"))
        return data[main.PROJECTS_KEY][self.PROJECT_CONTEXT]["groups"]

    def create_payload(
        self,
        *,
        group_key: str = "auth-module-team",
        group_name: str = "Auth Module Team",
        roles: list[str] | None = None,
    ) -> dict[str, object]:
        selected_roles = (
            ["system_analyst", "backend_developer", "qa_engineer"]
            if roles is None
            else roles
        )
        return {
            "group_key": group_key,
            "template_id": self.TEMPLATE_ID,
            "group_name": group_name,
            "task_template": {
                "id": "auth-delivery-v1",
                "agents": selected_roles,
            },
            # Compatibility rule: an empty custom_connections list means that
            # the registry template connections remain active.
            "custom_connections": [],
        }

    @staticmethod
    def group_from_response(body: object) -> dict[str, Any]:
        if not isinstance(body, dict):
            raise AssertionError(f"Expected JSON object, got {body!r}")
        group = body.get("group", body)
        if not isinstance(group, dict):
            raise AssertionError(f"Expected group object, got {body!r}")
        return group

    @staticmethod
    def role_bindings(group: dict[str, Any]) -> dict[str, tuple[str, str]]:
        return {
            str(agent["role"]): (
                str(agent["agent_id"]),
                str(agent["agent_phone"]),
            )
            for agent in group["agents"]
        }

    async def create_group(
        self, payload: dict[str, object] | None = None
    ) -> tuple[int, dict[str, Any], dict[str, Any]]:
        status_code, body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/groups",
            method="POST",
            payload=payload or self.create_payload(),
        )
        if not isinstance(body, dict):
            raise AssertionError(f"Expected JSON object, got {body!r}")
        return status_code, body, self.group_from_response(body)

    async def test_group_templates_are_discoverable(self) -> None:
        status_code, body = await asgi_request("/api/v1/group-templates")
        detail_status, detail_body = await asgi_request(
            f"/api/v1/group-templates/{self.TEMPLATE_ID}"
        )

        self.assertEqual(status_code, 200)
        self.assertIsInstance(body, dict)
        assert isinstance(body, dict)
        raw_templates = body.get("group_templates", body.get("templates"))
        if isinstance(raw_templates, dict):
            templates = list(raw_templates.values())
        else:
            templates = raw_templates
        self.assertIsInstance(templates, list)
        template = next(
            item for item in templates if item.get("template_id") == self.TEMPLATE_ID
        )
        self.assertEqual(
            [agent["role"] for agent in template["agent_templates"]],
            ["system_analyst", "backend_developer", "qa_engineer"],
        )
        self.assertEqual(detail_status, 200)
        self.assertIsInstance(detail_body, dict)
        assert isinstance(detail_body, dict)
        self.assertEqual(detail_body["template"]["template_id"], self.TEMPLATE_ID)

    async def test_create_is_idempotent_and_does_not_duplicate_agents(self) -> None:
        first_status, first_body, first_group = await self.create_group()
        agents_after_first = deepcopy(self.stored_agents())
        second_status, second_body, second_group = await self.create_group()

        self.assertIn(first_status, {200, 201})
        self.assertIn(second_status, {200, 201})
        self.assertTrue(first_body["created"])
        self.assertFalse(second_body["created"])
        self.assertEqual(first_group["group_id"], second_group["group_id"])
        self.assertEqual(
            self.role_bindings(first_group), self.role_bindings(second_group)
        )
        self.assertEqual(agents_after_first, self.stored_agents())
        self.assertEqual(len(self.stored_groups()), 1)
        bindings = self.role_bindings(first_group)
        self.assertEqual(set(bindings), {
            "system_analyst",
            "backend_developer",
            "qa_engineer",
        })
        self.assertEqual(len({agent_id for agent_id, _ in bindings.values()}), 3)
        self.assertEqual(len({phone for _, phone in bindings.values()}), 3)

    async def test_same_group_key_with_changed_roles_is_a_conflict(self) -> None:
        await self.create_group()
        agents_before = deepcopy(self.stored_agents())
        groups_before = deepcopy(self.stored_groups())
        changed = self.create_payload(roles=["system_analyst", "backend_developer"])

        status_code, _body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/groups",
            method="POST",
            payload=changed,
        )

        self.assertEqual(status_code, 409)
        self.assertEqual(self.stored_agents(), agents_before)
        self.assertEqual(self.stored_groups(), groups_before)

    async def test_selected_roles_are_validated_before_any_write(self) -> None:
        invalid_role_sets = [
            [],
            ["system_analyst", "system_analyst"],
            ["system_analyst", "unknown_role"],
        ]
        for index, roles in enumerate(invalid_role_sets):
            with self.subTest(roles=roles):
                payload = self.create_payload(group_key=f"invalid-{index}")
                payload["task_template"]["agents"] = roles  # type: ignore[index]
                status_code, _body = await asgi_request(
                    f"/api/v1/projects/{self.PROJECT_PHONE}/groups",
                    method="POST",
                    payload=payload,
                )
                self.assertEqual(status_code, 400)
                self.assertEqual(self.stored_groups(), [])
                self.assertEqual(self.stored_agents(), [])

    async def test_enabled_roles_alias_selects_the_same_template_agents(self) -> None:
        payload = self.create_payload()
        selected = payload["task_template"].pop("agents")  # type: ignore[union-attr]
        payload["enabled_roles"] = selected

        status_code, _body, group = await self.create_group(payload)

        self.assertIn(status_code, {200, 201})
        self.assertEqual(
            set(self.role_bindings(group)),
            {"system_analyst", "backend_developer", "qa_engineer"},
        )

    async def test_custom_connections_empty_uses_template_but_connections_empty_is_explicit(self) -> None:
        _status, _body, inherited = await self.create_group()
        self.assertEqual(
            {connection["id"] for connection in inherited["connections"]},
            {"analyst-to-backend", "backend-to-qa"},
        )
        agents_after_first = deepcopy(self.stored_agents())

        explicit = self.create_payload(
            group_key="isolated-backend-team",
            group_name="Isolated Backend Team",
            roles=["system_analyst"],
        )
        explicit.pop("custom_connections")
        explicit["connections"] = []
        _status, _body, isolated = await self.create_group(explicit)

        self.assertEqual(isolated["connections"], [])
        # The entrypoint spec is shared project-wide, so the second group only
        # adds membership and does not allocate another physical agent.
        self.assertEqual(len(self.stored_agents()), len(agents_after_first))
        self.assertEqual(len(self.stored_groups()), 2)

    async def test_get_list_detail_and_project_id_is_canonical_phone(self) -> None:
        _status, _body, created = await self.create_group()
        group_id = created["group_id"]

        list_status, list_body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/groups"
        )
        detail_status, detail_body = await asgi_request(
            f"/api/v1/groups/{group_id}"
        )
        invalid_status, _invalid_body = await asgi_request(
            "/api/v1/projects/not-a-project-phone/groups"
        )
        encoded_context = urllib.parse.quote(self.PROJECT_CONTEXT, safe="")
        context_status, _context_body = await asgi_request(
            f"/api/v1/projects/{encoded_context}/groups"
        )

        self.assertEqual(list_status, 200)
        self.assertIsInstance(list_body, dict)
        assert isinstance(list_body, dict)
        self.assertEqual(
            [group["group_id"] for group in list_body["groups"]], [group_id]
        )
        self.assertEqual(detail_status, 200)
        self.assertEqual(self.group_from_response(detail_body)["group_id"], group_id)
        self.assertIn(invalid_status, {400, 404})
        self.assertIn(context_status, {400, 404})

    async def test_put_reconciles_without_recreating_agents_and_checks_revision(self) -> None:
        _status, _body, created = await self.create_group()
        original_bindings = self.role_bindings(created)
        original_agent_count = len(self.stored_agents())
        update_payload = {
            "expected_revision": created["revision"],
            "group_name": "Auth Module Delivery Team",
            "task_template": {
                "id": "auth-delivery-v1",
                "agents": [
                    "system_analyst",
                    "backend_developer",
                    "qa_engineer",
                ],
            },
            "custom_connections": [],
            "status": "active",
        }

        update_status, update_body = await asgi_request(
            f"/api/v1/groups/{created['group_id']}",
            method="PUT",
            payload=update_payload,
        )
        updated = self.group_from_response(update_body)

        self.assertEqual(update_status, 200)
        self.assertEqual(updated["group_name"], "Auth Module Delivery Team")
        self.assertGreater(updated["revision"], created["revision"])
        self.assertEqual(self.role_bindings(updated), original_bindings)
        self.assertEqual(len(self.stored_agents()), original_agent_count)

        stale_status, _stale_body = await asgi_request(
            f"/api/v1/groups/{created['group_id']}",
            method="PUT",
            payload=update_payload,
        )
        self.assertEqual(stale_status, 409)

    async def test_put_can_add_and_remove_roles_without_deleting_global_agents(self) -> None:
        initial_payload = self.create_payload(
            roles=["system_analyst", "backend_developer"]
        )
        _status, _body, initial = await self.create_group(initial_payload)
        initial_bindings = self.role_bindings(initial)
        agent_count_before_add = len(self.stored_agents())
        template_connections = [
            {
                "id": "analyst-to-backend",
                "from": "system_analyst",
                "to": "backend_developer",
                "queue": "worker-all",
                "event": "spec_approved",
            },
            {
                "id": "backend-to-qa",
                "from": "backend_developer",
                "to": "qa_engineer",
                "queue": "tester-all",
                "event": "code_ready",
            },
        ]
        add_payload = {
            "expected_revision": initial["revision"],
            "task_template": {
                "id": "auth-delivery-v1",
                "agents": [
                    "system_analyst",
                    "backend_developer",
                    "qa_engineer",
                ],
            },
            "connections": template_connections,
        }

        add_status, add_body = await asgi_request(
            f"/api/v1/groups/{initial['group_id']}",
            method="PUT",
            payload=add_payload,
        )
        expanded = self.group_from_response(add_body)

        self.assertEqual(add_status, 200)
        expanded_bindings = self.role_bindings(expanded)
        self.assertEqual(
            expanded_bindings["system_analyst"],
            initial_bindings["system_analyst"],
        )
        self.assertEqual(
            expanded_bindings["backend_developer"],
            initial_bindings["backend_developer"],
        )
        self.assertIn("qa_engineer", expanded_bindings)
        self.assertEqual(len(self.stored_agents()), agent_count_before_add + 1)

        remove_payload = {
            "expected_revision": expanded["revision"],
            "task_template": {
                "id": "auth-delivery-v1",
                "agents": ["system_analyst", "backend_developer"],
            },
            "connections": [template_connections[0]],
        }
        remove_status, remove_body = await asgi_request(
            f"/api/v1/groups/{initial['group_id']}",
            method="PUT",
            payload=remove_payload,
        )
        reduced = self.group_from_response(remove_body)

        self.assertEqual(remove_status, 200)
        self.assertEqual(
            set(self.role_bindings(reduced)),
            {"system_analyst", "backend_developer"},
        )
        # Removing membership must not delete the reusable project agent.
        self.assertEqual(len(self.stored_agents()), agent_count_before_add + 1)

    async def test_delete_soft_archives_group_and_blocks_new_tasks(self) -> None:
        _status, _body, created = await self.create_group()
        group_id = created["group_id"]

        delete_status, delete_body = await asgi_request(
            f"/api/v1/groups/{group_id}", method="DELETE"
        )
        archived = self.group_from_response(delete_body)
        repeated_status, repeated_body = await asgi_request(
            f"/api/v1/groups/{group_id}", method="DELETE"
        )
        task_status, _task_body = await asgi_request(
            f"/api/v1/groups/{group_id}/tasks",
            method="POST",
            payload={"message": "This task must not be queued."},
        )

        self.assertEqual(delete_status, 200)
        self.assertEqual(repeated_status, 200)
        self.assertEqual(archived["status"], "archived")
        self.assertEqual(
            self.group_from_response(repeated_body)["status"], "archived"
        )
        self.assertEqual(task_status, 409)

    async def test_group_task_is_idempotent_and_delivered_only_to_entrypoint(self) -> None:
        _status, _body, group = await self.create_group()
        entrypoint = next(agent for agent in group["agents"] if agent["is_entrypoint"])
        payload = {
            "message": "Prepare an implementation specification.",
            "task_template": {
                "id": "task-specification-v1",
                "agents": [
                    "system_analyst",
                    "backend_developer",
                    "qa_engineer",
                ],
            },
            "request_id": "external-task-1",
        }

        first_status, first_body = await asgi_request(
            f"/api/v1/groups/{group['group_id']}/tasks",
            method="POST",
            payload=payload,
        )
        second_status, second_body = await asgi_request(
            f"/api/v1/groups/{group['group_id']}/tasks",
            method="POST",
            payload=payload,
        )
        conflict_status, _conflict_body = await asgi_request(
            f"/api/v1/groups/{group['group_id']}/tasks",
            method="POST",
            payload={**payload, "message": "A different task with the same request id."},
        )

        self.assertIn(first_status, {200, 201})
        self.assertIn(second_status, {200, 201})
        self.assertEqual(first_body["task_id"], second_body["task_id"])
        self.assertEqual(first_body["to_agent_id"], entrypoint["agent_id"])
        self.assertEqual(conflict_status, 409)

        poll_status, poll_body = await asgi_request(
            f"/api/v1/groups/{group['group_id']}/agents/{entrypoint['agent_id']}/tasks"
        )
        delivered_retry_status, delivered_retry_body = await asgi_request(
            f"/api/v1/groups/{group['group_id']}/tasks",
            method="POST",
            payload=payload,
        )
        empty_status, _empty_body = await asgi_request(
            f"/api/v1/groups/{group['group_id']}/agents/{entrypoint['agent_id']}/tasks"
        )
        self.assertEqual(poll_status, 200)
        self.assertEqual(poll_body["message"], payload["message"])
        self.assertIn(delivered_retry_status, {200, 201})
        self.assertEqual(delivered_retry_body["task_id"], first_body["task_id"])
        self.assertEqual(empty_status, 404)

    async def test_empty_project_cycle_list_is_empty(self) -> None:
        status_code, body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/cycles"
        )

        self.assertEqual(status_code, 200)
        self.assertIsInstance(body, dict)
        assert isinstance(body, dict)
        self.assertEqual(body["project_id"], self.PROJECT_PHONE)
        self.assertEqual(body["project_phone"], self.PROJECT_PHONE)
        self.assertEqual(body["cycle_count"], 0)
        self.assertEqual(body["cycles"], [])

    async def test_external_task_creates_stable_cycle_history_and_graph(self) -> None:
        _status, _body, group = await self.create_group()
        entrypoint = next(
            binding for binding in group["agents"] if binding["is_entrypoint"]
        )
        task_endpoint = f"/api/v1/groups/{group['group_id']}/tasks"
        payload = {
            "message": "Start an auditable development cycle.",
            "cycle_title": "Auditable cycle",
            "request_id": "cycle-root-1",
        }

        first_status, first_body = await asgi_request(
            task_endpoint,
            method="POST",
            payload=payload,
        )
        retry_status, retry_body = await asgi_request(
            task_endpoint,
            method="POST",
            payload=payload,
        )

        self.assertEqual(first_status, 201)
        self.assertEqual(retry_status, 201)
        self.assertIsInstance(first_body, dict)
        self.assertIsInstance(retry_body, dict)
        assert isinstance(first_body, dict)
        assert isinstance(retry_body, dict)
        cycle_id = first_body["cycle_id"]
        self.assertTrue(cycle_id.startswith("cycle-"))
        self.assertEqual(retry_body["cycle_id"], cycle_id)
        self.assertEqual(retry_body["task_id"], first_body["task_id"])
        self.assertEqual(
            retry_body["task_node_id"],
            first_body["task_node_id"],
        )
        self.assertTrue(retry_body["deduplicated"])

        list_status, list_body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/cycles"
        )
        history_status, history_body = await asgi_request(
            f"/api/v1/cycles/{cycle_id}/history"
        )
        graph_status, graph_body = await asgi_request(
            f"/api/v1/cycles/{cycle_id}/graph"
        )

        self.assertEqual(list_status, 200)
        self.assertEqual(history_status, 200)
        self.assertEqual(graph_status, 200)
        self.assertIsInstance(list_body, dict)
        self.assertIsInstance(history_body, dict)
        self.assertIsInstance(graph_body, dict)
        assert isinstance(list_body, dict)
        assert isinstance(history_body, dict)
        assert isinstance(graph_body, dict)
        self.assertEqual(list_body["cycle_count"], 1)
        cycle = list_body["cycles"][0]
        self.assertEqual(cycle["cycle_id"], cycle_id)
        self.assertEqual(cycle["title"], payload["cycle_title"])
        self.assertEqual(cycle["status"], "queued")
        self.assertEqual(cycle["root_task_id"], first_body["task_id"])
        self.assertEqual(cycle["root_group_id"], group["group_id"])
        self.assertEqual(cycle["task_count"], 1)
        self.assertEqual(
            [event["event_type"] for event in history_body["events"]],
            ["CYCLE_STARTED", "GROUP_DEPLOYED", "MESSAGE_QUEUED"],
        )
        self.assertEqual(
            [event["sequence"] for event in history_body["events"]],
            [1, 2, 3],
        )
        self.assertEqual(graph_body["cycle"]["cycle_id"], cycle_id)
        self.assertEqual(len(graph_body["nodes"]["tasks"]), 1)
        self.assertEqual(len(graph_body["edges"]["communications"]), 1)

        poll_status, poll_body = await asgi_request(
            f"/api/v1/groups/{group['group_id']}/agents/"
            f"{entrypoint['agent_id']}/tasks"
        )
        started_status, started_body = await asgi_request(
            f"/api/v1/cycles/{cycle_id}/history"
        )
        updated_list_status, updated_list_body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/cycles"
        )

        self.assertEqual(poll_status, 200)
        self.assertEqual(started_status, 200)
        self.assertEqual(updated_list_status, 200)
        self.assertIsInstance(poll_body, dict)
        self.assertIsInstance(started_body, dict)
        self.assertIsInstance(updated_list_body, dict)
        assert isinstance(poll_body, dict)
        assert isinstance(started_body, dict)
        assert isinstance(updated_list_body, dict)
        self.assertEqual(poll_body["cycle_id"], cycle_id)
        self.assertEqual(poll_body["task_id"], first_body["task_id"])
        self.assertEqual(poll_body["task_node_id"], first_body["task_node_id"])
        self.assertEqual(
            [event["event_type"] for event in started_body["events"]],
            [
                "CYCLE_STARTED",
                "GROUP_DEPLOYED",
                "MESSAGE_QUEUED",
                "TASK_STARTED",
            ],
        )
        self.assertEqual(updated_list_body["cycles"][0]["status"], "in_progress")

    async def test_cycle_handoff_creates_parent_child_lineage(self) -> None:
        _status, _body, group = await self.create_group()
        entrypoint = next(
            binding for binding in group["agents"] if binding["is_entrypoint"]
        )
        root_status, root_body = await asgi_request(
            f"/api/v1/groups/{group['group_id']}/tasks",
            method="POST",
            payload={
                "message": "Analyse the requested change.",
                "request_id": "lineage-root-1",
            },
        )
        self.assertEqual(root_status, 201)
        self.assertIsInstance(root_body, dict)
        assert isinstance(root_body, dict)
        poll_status, _poll_body = await asgi_request(
            f"/api/v1/groups/{group['group_id']}/agents/"
            f"{entrypoint['agent_id']}/tasks"
        )
        self.assertEqual(poll_status, 200)
        connection = next(
            item
            for item in group["connections"]
            if item["id"] == "analyst-to-backend"
        )

        missing_parent_status, _missing_parent_body = await asgi_request(
            f"/api/v1/groups/{group['group_id']}/connections/"
            f"{connection['id']}/tasks",
            method="POST",
            payload={
                "message": "Do not detach this handoff from its parent.",
                "from_agent_id": connection["from_agent_id"],
                "cycle_id": root_body["cycle_id"],
                "request_id": "lineage-missing-parent-1",
            },
        )
        wrong_parent_status, _wrong_parent_body = await asgi_request(
            f"/api/v1/groups/{group['group_id']}/connections/"
            f"{connection['id']}/tasks",
            method="POST",
            payload={
                "message": "Do not attach this handoff to an unknown task.",
                "from_agent_id": connection["from_agent_id"],
                "cycle_id": root_body["cycle_id"],
                "parent_task_id": "unknown-parent-task",
                "request_id": "lineage-wrong-parent-1",
            },
        )
        self.assertEqual(missing_parent_status, 400)
        self.assertEqual(wrong_parent_status, 409)
        detached_parent_status, _detached_parent_body = await asgi_request(
            f"/api/v1/groups/{group['group_id']}/connections/"
            f"{connection['id']}/tasks",
            method="POST",
            payload={
                "message": "Do not accept a parent without its cycle.",
                "from_agent_id": connection["from_agent_id"],
                "parent_task_node_id": root_body["task_node_id"],
                "request_id": "lineage-detached-parent-1",
            },
        )
        self.assertEqual(detached_parent_status, 400)

        handoff_status, handoff_body = await asgi_request(
            f"/api/v1/groups/{group['group_id']}/connections/"
            f"{connection['id']}/tasks",
            method="POST",
            payload={
                "message": "Implement the approved analysis.",
                "from_agent_id": connection["from_agent_id"],
                "cycle_id": root_body["cycle_id"],
                "parent_task_id": root_body["task_id"],
                "request_id": "lineage-handoff-1",
            },
        )

        self.assertEqual(handoff_status, 201)
        self.assertIsInstance(handoff_body, dict)
        assert isinstance(handoff_body, dict)
        self.assertEqual(handoff_body["cycle_id"], root_body["cycle_id"])
        self.assertEqual(handoff_body["parent_task_id"], root_body["task_id"])
        self.assertNotEqual(handoff_body["task_node_id"], root_body["task_node_id"])

        history_status, history_body = await asgi_request(
            f"/api/v1/cycles/{root_body['cycle_id']}/history"
        )
        graph_status, graph_body = await asgi_request(
            f"/api/v1/cycles/{root_body['cycle_id']}/graph"
        )
        list_status, list_body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/cycles"
        )

        self.assertEqual(history_status, 200)
        self.assertEqual(graph_status, 200)
        self.assertEqual(list_status, 200)
        self.assertIsInstance(history_body, dict)
        self.assertIsInstance(graph_body, dict)
        self.assertIsInstance(list_body, dict)
        assert isinstance(history_body, dict)
        assert isinstance(graph_body, dict)
        assert isinstance(list_body, dict)
        event_types = [event["event_type"] for event in history_body["events"]]
        self.assertEqual(event_types.count("HANDOFF_TRIGGERED"), 1)
        self.assertEqual(event_types.count("MESSAGE_QUEUED"), 2)
        handoff_event = next(
            event
            for event in history_body["events"]
            if event["event_type"] == "HANDOFF_TRIGGERED"
        )
        self.assertEqual(handoff_event["parent_task_id"], root_body["task_id"])
        self.assertEqual(handoff_event["task_id"], handoff_body["task_id"])
        self.assertEqual(handoff_event["connection_id"], connection["id"])

        task_nodes = graph_body["nodes"]["tasks"]
        lineage = graph_body["edges"]["task_lineage"]
        self.assertEqual(len(task_nodes), 2)
        self.assertEqual(len(lineage), 1)
        self.assertEqual(lineage[0]["from"], root_body["task_node_id"])
        self.assertEqual(lineage[0]["to"], handoff_body["task_node_id"])
        self.assertEqual(lineage[0]["connection_id"], connection["id"])
        self.assertEqual(list_body["cycles"][0]["task_count"], 2)
        self.assertEqual(list_body["cycles"][0]["handoff_count"], 1)

    async def test_cycle_lifecycle_events_are_idempotent_and_completion_blocks_work(self) -> None:
        _status, _body, group = await self.create_group()
        entrypoint = next(
            binding for binding in group["agents"] if binding["is_entrypoint"]
        )
        root_status, root_body = await asgi_request(
            f"/api/v1/groups/{group['group_id']}/tasks",
            method="POST",
            payload={
                "message": "Produce evidence and a final report.",
                "request_id": "lifecycle-root-1",
            },
        )
        self.assertEqual(root_status, 201)
        self.assertIsInstance(root_body, dict)
        assert isinstance(root_body, dict)
        poll_status, _poll_body = await asgi_request(
            f"/api/v1/groups/{group['group_id']}/agents/"
            f"{entrypoint['agent_id']}/tasks"
        )
        self.assertEqual(poll_status, 200)
        event_endpoint = f"/api/v1/cycles/{root_body['cycle_id']}/events"
        artifact_payload = {
            "event_type": "ARTIFACT_CREATED",
            "group_id": group["group_id"],
            "from_agent_id": entrypoint["agent_id"],
            "task_id": root_body["task_id"],
            "artifact": {
                "artifact_id": "artifact-auth-evidence-1",
                "kind": "test-evidence",
                "path": "evidence/auth-result.json",
            },
            "request_id": "artifact-event-1",
        }
        report_payload = {
            "event_type": "GROUP_REPORT_SUBMITTED",
            "group_id": group["group_id"],
            "from_agent_id": entrypoint["agent_id"],
            "task_id": root_body["task_id"],
            "report": {
                "report_id": "auth-report-1",
                "status": "ready",
            },
            "request_id": "report-event-1",
        }
        completion_payload = {
            "event_type": "CYCLE_COMPLETED",
            "decision": {"status": "accepted"},
            "request_id": "completion-event-1",
        }

        pm_artifact_status, _pm_artifact_body = await asgi_request(
            event_endpoint,
            method="POST",
            payload={
                **artifact_payload,
                "from_agent_id": main.PROJECT_MANAGER_AGENT_ID,
                "request_id": "pm-must-not-create-artifact-1",
            },
        )
        self.assertEqual(pm_artifact_status, 403)

        artifact_first = await asgi_request(
            event_endpoint,
            method="POST",
            payload=artifact_payload,
        )
        artifact_retry = await asgi_request(
            event_endpoint,
            method="POST",
            payload=artifact_payload,
        )
        cross_type_conflict = await asgi_request(
            event_endpoint,
            method="POST",
            payload={
                **report_payload,
                "request_id": artifact_payload["request_id"],
            },
        )
        report_first = await asgi_request(
            event_endpoint,
            method="POST",
            payload=report_payload,
        )
        report_retry = await asgi_request(
            event_endpoint,
            method="POST",
            payload=report_payload,
        )
        completion_first = await asgi_request(
            event_endpoint,
            method="POST",
            payload=completion_payload,
        )
        completion_retry = await asgi_request(
            event_endpoint,
            method="POST",
            payload=completion_payload,
        )

        for response in (
            artifact_first,
            artifact_retry,
            report_first,
            report_retry,
            completion_first,
            completion_retry,
        ):
            self.assertEqual(response[0], 201)
            self.assertIsInstance(response[1], dict)
        self.assertTrue(artifact_first[1]["created"])
        self.assertFalse(artifact_retry[1]["created"])
        self.assertTrue(artifact_retry[1]["deduplicated"])
        self.assertEqual(
            artifact_first[1]["event"]["event_id"],
            artifact_retry[1]["event"]["event_id"],
        )
        protected_delete_status, _protected_delete_body = await asgi_request(
            f"/history/{artifact_first[1]['event']['event_id']}",
            method="DELETE",
        )
        self.assertEqual(protected_delete_status, 403)
        self.assertEqual(cross_type_conflict[0], 409)
        self.assertTrue(report_first[1]["created"])
        self.assertFalse(report_retry[1]["created"])
        self.assertTrue(report_retry[1]["deduplicated"])
        self.assertTrue(completion_first[1]["created"])
        self.assertFalse(completion_retry[1]["created"])
        self.assertTrue(completion_retry[1]["deduplicated"])

        history_status, history_body = await asgi_request(
            f"/api/v1/cycles/{root_body['cycle_id']}/history"
        )
        self.assertEqual(history_status, 200)
        self.assertIsInstance(history_body, dict)
        assert isinstance(history_body, dict)
        event_types = [event["event_type"] for event in history_body["events"]]
        self.assertEqual(event_types.count("ARTIFACT_CREATED"), 1)
        self.assertEqual(event_types.count("GROUP_REPORT_SUBMITTED"), 1)
        self.assertEqual(event_types.count("CYCLE_COMPLETED"), 1)
        self.assertEqual(history_body["cycle"]["artifact_count"], 1)
        self.assertEqual(history_body["cycle"]["report_count"], 1)
        self.assertEqual(history_body["cycle"]["task_count"], 1)
        self.assertEqual(history_body["cycle"]["status"], "completed")
        self.assertIsNotNone(history_body["cycle"]["completed_at"])
        graph_status, graph_body = await asgi_request(
            f"/api/v1/cycles/{root_body['cycle_id']}/graph"
        )
        self.assertEqual(graph_status, 200)
        self.assertIsInstance(graph_body, dict)
        assert isinstance(graph_body, dict)
        self.assertEqual(len(graph_body["nodes"]["tasks"]), 1)

        blocked_task_status, blocked_task_body = await asgi_request(
            f"/api/v1/groups/{group['group_id']}/tasks",
            method="POST",
            payload={
                "message": "This work must not reopen the completed cycle.",
                "cycle_id": root_body["cycle_id"],
                "request_id": "completed-cycle-task-1",
            },
        )
        connection = next(
            item
            for item in group["connections"]
            if item["id"] == "analyst-to-backend"
        )
        blocked_handoff_status, blocked_handoff_body = await asgi_request(
            f"/api/v1/groups/{group['group_id']}/connections/"
            f"{connection['id']}/tasks",
            method="POST",
            payload={
                "message": "This handoff must also be rejected.",
                "from_agent_id": connection["from_agent_id"],
                "cycle_id": root_body["cycle_id"],
                "parent_task_id": root_body["task_id"],
                "request_id": "completed-cycle-handoff-1",
            },
        )
        blocked_event_status, blocked_event_body = await asgi_request(
            event_endpoint,
            method="POST",
            payload={
                **artifact_payload,
                "artifact": {
                    "artifact_id": "artifact-too-late",
                    "kind": "test-evidence",
                },
                "request_id": "artifact-event-too-late",
            },
        )
        self.assertEqual(blocked_task_status, 409)
        self.assertEqual(blocked_handoff_status, 409)
        self.assertEqual(blocked_event_status, 409)
        self.assertEqual(blocked_task_body["detail"]["error"], "cycle_completed")
        self.assertEqual(
            blocked_handoff_body["detail"]["error"],
            "cycle_completed",
        )
        self.assertEqual(blocked_event_body["detail"]["error"], "cycle_completed")

    async def test_unknown_cycle_history_graph_and_events_return_404(self) -> None:
        cycle_id = "cycle-does-not-exist"
        history_status, history_body = await asgi_request(
            f"/api/v1/cycles/{cycle_id}/history"
        )
        graph_status, graph_body = await asgi_request(
            f"/api/v1/cycles/{cycle_id}/graph"
        )
        event_status, event_body = await asgi_request(
            f"/api/v1/cycles/{cycle_id}/events",
            method="POST",
            payload={
                "event_type": "CYCLE_COMPLETED",
                "decision": {"status": "accepted"},
                "request_id": "unknown-cycle-completion",
            },
        )

        for status_code, body in (
            (history_status, history_body),
            (graph_status, graph_body),
            (event_status, event_body),
        ):
            self.assertEqual(status_code, 404)
            self.assertIsInstance(body, dict)
            assert isinstance(body, dict)
            self.assertEqual(body["detail"]["error"], "cycle_not_found")
            self.assertEqual(body["detail"]["cycle_id"], cycle_id)

    async def test_cycle_completion_cancels_pending_queue_items(self) -> None:
        _status, _body, group = await self.create_group()
        entrypoint = next(
            binding for binding in group["agents"] if binding["is_entrypoint"]
        )
        root_status, root_body = await asgi_request(
            f"/api/v1/groups/{group['group_id']}/tasks",
            method="POST",
            payload={
                "message": "Cancel this pending work when the cycle closes.",
                "request_id": "pending-completion-root-1",
            },
        )
        self.assertEqual(root_status, 201)
        self.assertIsInstance(root_body, dict)
        assert isinstance(root_body, dict)

        completion_status, completion_body = await asgi_request(
            f"/api/v1/cycles/{root_body['cycle_id']}/events",
            method="POST",
            payload={
                "event_type": "CYCLE_COMPLETED",
                "decision": {"status": "cancelled_by_customer"},
                "request_id": "pending-completion-event-1",
            },
        )
        detail_status, detail_body = await asgi_request(
            f"/api/v1/groups/{group['group_id']}"
        )
        poll_status, _poll_body = await asgi_request(
            f"/api/v1/groups/{group['group_id']}/agents/"
            f"{entrypoint['agent_id']}/tasks"
        )

        self.assertEqual(completion_status, 201)
        self.assertIsInstance(completion_body, dict)
        assert isinstance(completion_body, dict)
        self.assertEqual(completion_body["cancelled_queue_task_count"], 1)
        retry_status, retry_body = await asgi_request(
            f"/api/v1/cycles/{root_body['cycle_id']}/events",
            method="POST",
            payload={
                "event_type": "CYCLE_COMPLETED",
                "decision": {"status": "cancelled_by_customer"},
                "request_id": "pending-completion-event-1",
            },
        )
        self.assertEqual(retry_status, 201)
        self.assertIsInstance(retry_body, dict)
        assert isinstance(retry_body, dict)
        self.assertEqual(retry_body["cancelled_queue_task_count"], 1)
        self.assertEqual(retry_body["newly_cancelled_queue_task_count"], 0)
        self.assertEqual(detail_status, 200)
        self.assertIsInstance(detail_body, dict)
        assert isinstance(detail_body, dict)
        self.assertEqual(detail_body["queue_size"], 0)
        self.assertEqual(poll_status, 404)
        history_status, history_body = await asgi_request(
            f"/api/v1/cycles/{root_body['cycle_id']}/history"
        )
        self.assertEqual(history_status, 200)
        self.assertIsInstance(history_body, dict)
        assert isinstance(history_body, dict)
        self.assertEqual(history_body["cycle"]["cancelled_task_count"], 1)
        self.assertIn(
            "MESSAGE_REMOVED",
            [event["event_type"] for event in history_body["events"]],
        )

    async def test_conflicting_request_id_keeps_exactly_one_queued_task(self) -> None:
        _status, _body, group = await self.create_group()
        entrypoint = next(agent for agent in group["agents"] if agent["is_entrypoint"])
        endpoint = f"/api/v1/groups/{group['group_id']}/tasks"
        payload = {
            "message": "Queue the original task.",
            "request_id": "single-queue-regression",
        }

        first_status, first_body = await asgi_request(
            endpoint,
            method="POST",
            payload=payload,
        )
        conflict_status, _conflict_body = await asgi_request(
            endpoint,
            method="POST",
            payload={**payload, "message": "Do not replace the original task."},
        )
        detail_status, detail_body = await asgi_request(
            f"/api/v1/groups/{group['group_id']}"
        )

        self.assertEqual(first_status, 201)
        self.assertEqual(conflict_status, 409)
        self.assertEqual(detail_status, 200)
        self.assertIsInstance(first_body, dict)
        self.assertIsInstance(detail_body, dict)
        assert isinstance(first_body, dict)
        assert isinstance(detail_body, dict)
        self.assertEqual(detail_body["queue_size"], 1)
        self.assertEqual(len(detail_body["queue_tasks"]), 1)
        self.assertEqual(
            detail_body["queue_tasks"][0]["task_id"],
            first_body["task_id"],
        )

        poll_path = (
            f"/api/v1/groups/{group['group_id']}/agents/"
            f"{entrypoint['agent_id']}/tasks"
        )
        poll_status, poll_body = await asgi_request(poll_path)
        empty_status, _empty_body = await asgi_request(poll_path)
        self.assertEqual(poll_status, 200)
        self.assertIsInstance(poll_body, dict)
        assert isinstance(poll_body, dict)
        self.assertEqual(poll_body["message"], payload["message"])
        self.assertEqual(empty_status, 404)

    async def test_post_agents_cannot_delete_or_mutate_group_managed_agents(self) -> None:
        await self.create_group()
        agents_before = deepcopy(self.stored_agents())
        managed_before = [
            agent
            for agent in agents_before
            if agent.get("parameters", {}).get("managed_by") == "group_api"
        ]
        self.assertEqual(len(managed_before), 3)

        omission_status, omission_body = await asgi_request(
            "/agents",
            method="POST",
            payload={"agents": []},
        )

        self.assertEqual(omission_status, 201)
        self.assertIsInstance(omission_body, dict)
        assert isinstance(omission_body, dict)
        self.assertEqual(omission_body["agents"], agents_before)
        self.assertEqual(self.stored_agents(), agents_before)

        tampered = deepcopy(agents_before)
        managed_index = next(
            index
            for index, agent in enumerate(tampered)
            if agent.get("parameters", {}).get("managed_by") == "group_api"
        )
        tampered[managed_index]["profile"] = (
            "A caller must not overwrite this profile."
        )
        mutation_status, mutation_body = await asgi_request(
            "/agents",
            method="POST",
            payload={"agents": tampered},
        )

        self.assertEqual(mutation_status, 409)
        self.assertIsInstance(mutation_body, dict)
        assert isinstance(mutation_body, dict)
        self.assertEqual(
            mutation_body["detail"]["error"],
            "group_managed_agent_is_immutable",
        )
        self.assertEqual(self.stored_agents(), agents_before)

    async def test_group_managed_agent_phone_mapping_cannot_be_deleted(self) -> None:
        _status, _body, group = await self.create_group()
        agent_phone = group["agents"][0]["agent_phone"]
        config_before = main.git_config_path.read_text(encoding="utf-8")

        delete_status, delete_body = await asgi_request(
            f"/git-config/phone/{agent_phone}",
            method="DELETE",
        )

        self.assertEqual(delete_status, 409)
        self.assertIsInstance(delete_body, dict)
        assert isinstance(delete_body, dict)
        self.assertEqual(
            delete_body["detail"]["error"],
            "group_managed_phone_is_immutable",
        )
        self.assertEqual(
            main.git_config_path.read_text(encoding="utf-8"),
            config_before,
        )

    async def test_project_manager_returns_groups_and_full_group_agents(self) -> None:
        _status, _body, group = await self.create_group()
        expected_agents = {
            agent["id"]: main.agent_with_presence(agent)
            for agent in deepcopy(self.stored_agents())
            if agent.get("parameters", {}).get("managed_by") == "group_api"
        }

        pm_status, pm_body = await asgi_request(
            "/project-manager/0001",
            method="POST",
            payload={
                "git_address": "https://github.com/example/groups-api.git",
            },
        )

        self.assertEqual(pm_status, 200)
        self.assertIsInstance(pm_body, dict)
        assert isinstance(pm_body, dict)
        self.assertFalse(pm_body["created"])
        self.assertFalse(pm_body["phone_assigned"])
        self.assertEqual(pm_body["project_manager_phone"], "0001")
        self.assertEqual(pm_body["project_phone"], self.PROJECT_PHONE)
        self.assertEqual(pm_body["group_count"], 1)
        self.assertEqual(pm_body["cycle_count"], 0)
        self.assertEqual(
            [item["group_id"] for item in pm_body["project"]["groups"]],
            [group["group_id"]],
        )

        returned_agents = {
            agent["id"]: agent for agent in pm_body["agents"]
        }
        self.assertEqual(pm_body["agent_count"], len(expected_agents))
        self.assertEqual(returned_agents, expected_agents)
        self.assertEqual(
            set(returned_agents),
            {binding["agent_id"] for binding in group["agents"]},
        )

    async def test_duplicate_legacy_project_phones_do_not_block_group_agents(self) -> None:
        self.write_agents(
            [
                {
                    "id": "legacy-a",
                    "name": "Legacy A",
                    "phone": "9001",
                    "profile": "Legacy project-scoped agent A",
                    "parameters": {"git_context_key": self.PROJECT_CONTEXT},
                },
                {
                    "id": "legacy-b",
                    "name": "Legacy B",
                    "phone": "9001",
                    "profile": "Legacy project-scoped agent B",
                    "parameters": {"git_context_key": self.PROJECT_CONTEXT},
                },
            ]
        )

        status_code, _body, group = await self.create_group()

        self.assertIn(status_code, {200, 201})
        self.assertEqual(len(group["agents"]), 3)
        self.assertTrue(
            all(
                4000 <= int(binding["agent_phone"]) <= 8999
                for binding in group["agents"]
            )
        )

    async def test_rejected_task_does_not_reconcile_paused_group(self) -> None:
        payload = self.create_payload(roles=["system_analyst"])
        _status, _body, group = await self.create_group(payload)
        pause_status, pause_body = await asgi_request(
            f"/api/v1/groups/{group['group_id']}",
            method="PUT",
            payload={
                "expected_revision": group["revision"],
                "status": "paused",
            },
        )
        self.assertEqual(pause_status, 200)
        paused = self.group_from_response(pause_body)
        config_before = main.git_config_path.read_text(encoding="utf-8")
        agents_before = main.agents_path.read_text(encoding="utf-8")

        task_status, _task_body = await asgi_request(
            f"/api/v1/groups/{group['group_id']}/tasks",
            method="POST",
            payload={
                "message": "Must be rejected without adding roles.",
                "request_id": "paused-task",
                "task_template": {
                    "id": "paused-task-v1",
                    "agents": [
                        "system_analyst",
                        "backend_developer",
                        "qa_engineer",
                    ],
                },
            },
        )

        self.assertEqual(task_status, 409)
        self.assertEqual(
            main.git_config_path.read_text(encoding="utf-8"),
            config_before,
        )
        self.assertEqual(main.agents_path.read_text(encoding="utf-8"), agents_before)
        self.assertEqual(paused["status"], "paused")

    async def test_pending_request_id_is_requeued_after_queue_memory_restart(self) -> None:
        _status, _body, group = await self.create_group()
        entrypoint = next(
            binding for binding in group["agents"] if binding["is_entrypoint"]
        )
        endpoint = f"/api/v1/groups/{group['group_id']}/tasks"
        payload = {
            "message": "Restore this pending task after restart.",
            "request_id": "restart-pending-task",
        }
        first_status, _first_body = await asgi_request(
            endpoint,
            method="POST",
            payload=payload,
        )
        self.assertEqual(first_status, 201)
        for queue in main.queues.values():
            queue.clear()

        retry_status, retry_body = await asgi_request(
            endpoint,
            method="POST",
            payload=payload,
        )
        poll_status, poll_body = await asgi_request(
            f"/api/v1/groups/{group['group_id']}/agents/{entrypoint['agent_id']}/tasks"
        )

        self.assertEqual(retry_status, 201)
        self.assertIsInstance(retry_body, dict)
        assert isinstance(retry_body, dict)
        self.assertFalse(retry_body["deduplicated"])
        self.assertEqual(poll_status, 200)
        self.assertEqual(poll_body["message"], payload["message"])

    async def test_task_commit_finishes_before_group_archive(self) -> None:
        _status, _body, group = await self.create_group()
        endpoint = f"/api/v1/groups/{group['group_id']}/tasks"
        entered_enqueue = asyncio.Event()
        release_enqueue = asyncio.Event()
        original_enqueue_phone_channel = main.enqueue_phone_channel

        async def blocked_enqueue(*args: Any, **kwargs: Any) -> dict[str, Any]:
            entered_enqueue.set()
            await release_enqueue.wait()
            return await original_enqueue_phone_channel(*args, **kwargs)

        main.enqueue_phone_channel = blocked_enqueue
        try:
            post_future = asyncio.create_task(
                asgi_request(
                    endpoint,
                    method="POST",
                    payload={"message": "Commit before archive."},
                )
            )
            await entered_enqueue.wait()
            delete_future = asyncio.create_task(
                asgi_request(
                    f"/api/v1/groups/{group['group_id']}",
                    method="DELETE",
                )
            )
            await asyncio.sleep(0.05)
            self.assertFalse(delete_future.done())
            release_enqueue.set()
            post_result, delete_result = await asyncio.gather(
                post_future,
                delete_future,
            )
        finally:
            main.enqueue_phone_channel = original_enqueue_phone_channel

        self.assertEqual(post_result[0], 201)
        self.assertEqual(delete_result[0], 200)
        self.assertEqual(
            self.group_from_response(delete_result[1])["status"],
            "archived",
        )

    async def test_delivery_history_commits_before_request_id_retry(self) -> None:
        _status, _body, group = await self.create_group()
        entrypoint = next(
            binding for binding in group["agents"] if binding["is_entrypoint"]
        )
        endpoint = f"/api/v1/groups/{group['group_id']}/tasks"
        payload = {
            "message": "Deliver exactly once across a retry race.",
            "request_id": "delivery-race-task",
        }
        first_status, _first_body = await asgi_request(
            endpoint,
            method="POST",
            payload=payload,
        )
        self.assertEqual(first_status, 201)

        entered_history = asyncio.Event()
        release_history = asyncio.Event()
        original_append_history = main.append_history

        async def blocked_history(
            event: str,
            *args: Any,
            **kwargs: Any,
        ) -> dict[str, Any]:
            if event.startswith("delivered_to_"):
                entered_history.set()
                await release_history.wait()
            return await original_append_history(event, *args, **kwargs)

        main.append_history = blocked_history
        poll_path = (
            f"/api/v1/groups/{group['group_id']}/agents/"
            f"{entrypoint['agent_id']}/tasks"
        )
        try:
            poll_future = asyncio.create_task(asgi_request(poll_path))
            await entered_history.wait()
            retry_future = asyncio.create_task(
                asgi_request(endpoint, method="POST", payload=payload)
            )
            await asyncio.sleep(0.05)
            self.assertFalse(retry_future.done())
            release_history.set()
            poll_result, retry_result = await asyncio.gather(
                poll_future,
                retry_future,
            )
        finally:
            main.append_history = original_append_history

        self.assertEqual(poll_result[0], 200)
        self.assertEqual(retry_result[0], 201)
        self.assertIsInstance(retry_result[1], dict)
        assert isinstance(retry_result[1], dict)
        self.assertTrue(retry_result[1]["deduplicated"])
        self.assertEqual(retry_result[1]["status"], "delivered")
        empty_status, _empty_body = await asgi_request(poll_path)
        self.assertEqual(empty_status, 404)

    async def test_explicit_empty_task_template_is_stable_after_put(self) -> None:
        _status, _body, group = await self.create_group()
        first_status, first_body = await asgi_request(
            f"/api/v1/groups/{group['group_id']}",
            method="PUT",
            payload={
                "expected_revision": group["revision"],
                "task_template": {},
            },
        )
        first_update = self.group_from_response(first_body)
        self.assertEqual(first_status, 200)
        self.assertEqual(first_update["task_template"], {})
        self.assertGreater(first_update["revision"], group["revision"])

        second_status, second_body = await asgi_request(
            f"/api/v1/groups/{group['group_id']}",
            method="PUT",
            payload={"expected_revision": first_update["revision"]},
        )
        second_update = self.group_from_response(second_body)
        self.assertEqual(second_status, 200)
        self.assertFalse(second_body["updated"])
        self.assertEqual(second_update["revision"], first_update["revision"])
        self.assertEqual(second_update["task_template"], {})

    async def test_create_retry_with_changed_task_template_is_conflict(self) -> None:
        await self.create_group()
        config_before = main.git_config_path.read_text(encoding="utf-8")
        changed = self.create_payload()
        changed["task_template"]["id"] = "another-task-template"  # type: ignore[index]

        status_code, _body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/groups",
            method="POST",
            payload=changed,
        )

        self.assertEqual(status_code, 409)
        self.assertEqual(
            main.git_config_path.read_text(encoding="utf-8"),
            config_before,
        )

    async def test_cross_group_topology_is_live_and_rejects_paused_target(self) -> None:
        registry = json.loads(main.group_templates_path.read_text(encoding="utf-8"))
        registry["agent_specs"]["project_coordinator_v1"] = {
            "name": "Project Coordinator",
            "profile": "Coordinate project groups.",
            "parameters": {"role": "project_coordinator"},
            "status": "active",
        }
        registry["group_templates"]["project_coordination_v1"] = {
            "template_id": "project_coordination_v1",
            "name": "Project Coordination",
            "description": "Coordinates test groups",
            "agent_templates": [
                {
                    "role": "project_coordinator",
                    "spec": "project_coordinator_v1",
                    "is_entrypoint": True,
                }
            ],
            "internal_connections": [],
            "reporting_rule": {
                "report_from": "project_coordinator",
                "report_to": "customer",
                "output_queue": "worker-all",
            },
        }
        registry["group_topologies"] = {
            "delivery": {
                "topology_id": "delivery",
                "groups": ["project_coordination_v1", self.TEMPLATE_ID],
                "connections": [
                    {
                        "id": "coordinator-to-backend",
                        "from_group": "project_coordination_v1",
                        "from_role": "project_coordinator",
                        "to_group": self.TEMPLATE_ID,
                        "to_role": "system_analyst",
                        "queue": "worker-all",
                        "event": "backend_task_assigned",
                    }
                ],
                "customer_reporting": {
                    "reporter_group": "project_coordination_v1",
                    "reporter_role": "project_coordinator",
                    "recipient": "customer",
                },
            }
        }
        main.group_templates_path.write_text(
            json.dumps(registry, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        _status, _body, backend_group = await self.create_group()
        coordination_payload = {
            "group_key": "coordination",
            "template_id": "project_coordination_v1",
            "group_name": "Coordination",
            "task_template": {
                "id": "coordination-v1",
                "agents": ["project_coordinator"],
            },
            "connections": [],
        }
        _coord_status, _coord_body, coordination_group = await self.create_group(
            coordination_payload
        )
        list_status, list_body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/groups"
        )
        self.assertEqual(list_status, 200)
        self.assertIsInstance(list_body, dict)
        assert isinstance(list_body, dict)
        relationship = list_body["group_relationships"][0]
        self.assertEqual(relationship["event"], "backend_task_assigned")

        registry["group_topologies"]["delivery"]["connections"][0][
            "event"
        ] = "backend_task_reassigned"
        main.group_templates_path.write_text(
            json.dumps(registry, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        _fresh_status, fresh_body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/groups"
        )
        assert isinstance(fresh_body, dict)
        fresh_relationship = fresh_body["group_relationships"][0]
        self.assertEqual(fresh_relationship["event"], "backend_task_reassigned")

        coordinator = coordination_group["agents"][0]
        post_status, _post_body = await asgi_request(
            (
                f"/api/v1/groups/{coordination_group['group_id']}/connections/"
                f"{fresh_relationship['id']}/tasks"
            ),
            method="POST",
            payload={
                "message": "Implement backend work.",
                "from_agent_id": coordinator["agent_id"],
                "request_id": "cross-group-task-1",
            },
        )
        backend_entrypoint = next(
            binding
            for binding in backend_group["agents"]
            if binding["is_entrypoint"]
        )
        poll_status, _poll_body = await asgi_request(
            (
                f"/api/v1/groups/{backend_group['group_id']}/agents/"
                f"{backend_entrypoint['agent_id']}/tasks"
            )
        )
        self.assertEqual(post_status, 201)
        self.assertEqual(poll_status, 200)

        pause_status, pause_body = await asgi_request(
            f"/api/v1/groups/{backend_group['group_id']}",
            method="PUT",
            payload={
                "expected_revision": backend_group["revision"],
                "status": "paused",
            },
        )
        self.assertEqual(pause_status, 200)
        self.assertEqual(self.group_from_response(pause_body)["status"], "paused")
        rejected_status, _rejected_body = await asgi_request(
            (
                f"/api/v1/groups/{coordination_group['group_id']}/connections/"
                f"{fresh_relationship['id']}/tasks"
            ),
            method="POST",
            payload={
                "message": "Must not reach a paused target.",
                "from_agent_id": coordinator["agent_id"],
                "request_id": "cross-group-task-2",
            },
        )
        self.assertIn(rejected_status, {404, 409})

    async def test_connection_task_enforces_declared_sender_and_recipient(self) -> None:
        _status, _body, group = await self.create_group()
        connection = next(
            item for item in group["connections"] if item["id"] == "analyst-to-backend"
        )
        payload = {
            "message": "Implement the approved specification.",
            "from_agent_id": connection["from_agent_id"],
            "request_id": "edge-task-1",
        }

        post_status, post_body = await asgi_request(
            (
                f"/api/v1/groups/{group['group_id']}/connections/"
                f"{connection['id']}/tasks"
            ),
            method="POST",
            payload=payload,
        )
        retry_status, retry_body = await asgi_request(
            (
                f"/api/v1/groups/{group['group_id']}/connections/"
                f"{connection['id']}/tasks"
            ),
            method="POST",
            payload=payload,
        )
        conflict_status, _conflict_body = await asgi_request(
            (
                f"/api/v1/groups/{group['group_id']}/connections/"
                f"{connection['id']}/tasks"
            ),
            method="POST",
            payload={**payload, "message": "Conflicting edge task."},
        )
        self.assertIn(post_status, {200, 201})
        self.assertIn(retry_status, {200, 201})
        self.assertEqual(retry_body["task_id"], post_body["task_id"])
        self.assertEqual(conflict_status, 409)
        self.assertEqual(post_body["from_agent_id"], connection["from_agent_id"])
        self.assertEqual(post_body["to_agent_id"], connection["to_agent_id"])

        poll_status, poll_body = await asgi_request(
            (
                f"/api/v1/groups/{group['group_id']}/agents/"
                f"{connection['to_agent_id']}/tasks"
            )
        )
        self.assertEqual(poll_status, 200)
        self.assertEqual(poll_body["message"], payload["message"])
        duplicate_poll_status, _duplicate_poll_body = await asgi_request(
            (
                f"/api/v1/groups/{group['group_id']}/agents/"
                f"{connection['to_agent_id']}/tasks"
            )
        )
        self.assertEqual(duplicate_poll_status, 404)

        spoofed = {**payload, "from_agent_id": connection["to_agent_id"]}
        spoofed_status, _spoofed_body = await asgi_request(
            (
                f"/api/v1/groups/{group['group_id']}/connections/"
                f"{connection['id']}/tasks"
            ),
            method="POST",
            payload=spoofed,
        )
        missing_status, _missing_body = await asgi_request(
            f"/api/v1/groups/{group['group_id']}/connections/missing-edge/tasks",
            method="POST",
            payload=payload,
        )
        outsider_status, _outsider_body = await asgi_request(
            f"/api/v1/groups/{group['group_id']}/agents/not-a-member/tasks"
        )
        self.assertIn(spoofed_status, {400, 403, 409})
        self.assertEqual(missing_status, 404)
        self.assertEqual(outsider_status, 404)

    async def test_concurrent_identical_creation_has_one_winner(self) -> None:
        path = f"/api/v1/projects/{self.PROJECT_PHONE}/groups"
        payload = self.create_payload()

        first, second = await asyncio.gather(
            asgi_request(path, method="POST", payload=payload),
            asgi_request(path, method="POST", payload=payload),
        )

        bodies = [response[1] for response in (first, second)]
        self.assertEqual(
            sorted(bool(body["created"]) for body in bodies), [False, True]
        )
        groups = [self.group_from_response(body) for body in bodies]
        self.assertEqual(groups[0]["group_id"], groups[1]["group_id"])
        self.assertEqual(self.role_bindings(groups[0]), self.role_bindings(groups[1]))
        self.assertEqual(len(self.stored_groups()), 1)


if __name__ == "__main__":
    unittest.main()
