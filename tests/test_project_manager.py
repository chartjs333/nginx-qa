import asyncio
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

from fastapi import HTTPException
from fastapi import Request

import main


def json_request(payload: object, path: str = "/project-manager/0001") -> Request:
    body = json.dumps(payload).encode("utf-8")
    delivered = False

    async def receive() -> dict[str, object]:
        nonlocal delivered
        if delivered:
            return {"type": "http.disconnect"}
        delivered = True
        return {"type": "http.request", "body": body, "more_body": False}

    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("ascii"),
        "query_string": b"",
        "root_path": "",
        "headers": [(b"content-type", b"application/json"), (b"host", b"testserver:8025")],
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", 8025),
    }
    return Request(scope, receive)


async def asgi_request(
    path: str,
    *,
    method: str = "POST",
    payload: object | None = None,
    content_type: str = "application/json",
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
        "headers": [
            (b"content-type", content_type.encode("ascii")),
            (b"host", b"testserver:8025"),
        ],
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", 8025),
    }
    await main.app(scope, receive, send)
    response_start = next(message for message in messages if message["type"] == "http.response.start")
    response_body = b"".join(
        message.get("body", b"")
        for message in messages
        if message["type"] == "http.response.body"
    )
    decoded_body: object = json.loads(response_body) if response_body else None
    return int(response_start["status"]), decoded_body


class ProjectManagerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        temp_path = Path(self.temp_dir.name)
        self.original_git_config_path = main.git_config_path
        self.original_agents_path = main.agents_path
        self.original_git_config_lock = main.git_config_lock
        self.original_agents_lock = main.agents_lock
        main.git_config_path = temp_path / "port_git_map.json"
        main.agents_path = temp_path / "agents.json"
        main.git_config_lock = asyncio.Lock()
        main.agents_lock = asyncio.Lock()

    def tearDown(self) -> None:
        main.git_config_path = self.original_git_config_path
        main.agents_path = self.original_agents_path
        main.git_config_lock = self.original_git_config_lock
        main.agents_lock = self.original_agents_lock
        self.temp_dir.cleanup()

    def write_config(self, config: dict[str, object]) -> None:
        main.git_config_path.write_text(
            json.dumps(config, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def write_agents(self, agents: list[dict[str, object]]) -> None:
        main.agents_path.write_text(
            json.dumps({"agents": agents}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    async def resolve(self, payload: dict[str, object]) -> dict[str, object]:
        return await main.post_project_manager_0001(json_request(payload))

    async def test_new_project_is_persisted_once_for_equivalent_git_addresses(self) -> None:
        existing_port_entry = {
            "project_name": "Existing",
            "git_address": "https://github.com/example/existing.git",
            "git_context_key": "github.com/example/existing",
            "updated_at": "2026-01-01T00:00:00+00:00",
        }
        self.write_config({"8025": existing_port_entry})

        first = await self.resolve(
            {"git_address": "HTTPS://GitHub.com/Example/Example-Project.GIT/"}
        )
        second = await self.resolve(
            {"git_address": "git@github.com:example/example-project.git"}
        )
        third = await self.resolve(
            {"git_address": "ssh://git@github.com:22/example/example-project.git"}
        )

        self.assertTrue(first["created"])
        self.assertFalse(second["created"])
        self.assertFalse(third["created"])
        self.assertTrue(first["phone_assigned"])
        self.assertFalse(second["phone_assigned"])
        self.assertFalse(third["phone_assigned"])
        self.assertEqual(first["project_phone"], second["project_phone"])
        self.assertEqual(first["project_phone"], third["project_phone"])
        self.assertIn(first["project_phone"], first["project"]["phones"])
        self.assertNotEqual(first["project_phone"], main.PROJECT_MANAGER_PHONE)
        self.assertEqual(
            first["project"]["git_context_key"],
            "github.com/example/example-project",
        )
        self.assertEqual(first["agents"], [])
        self.assertEqual(second["agents"], [])
        config = json.loads(main.git_config_path.read_text(encoding="utf-8"))
        self.assertEqual(config["8025"], existing_port_entry)
        self.assertEqual(len(config[main.PROJECTS_KEY]), 1)
        self.assertEqual(
            config[main.PROJECTS_KEY]["github.com/example/example-project"]["project_phone"],
            first["project_phone"],
        )
        self.assertEqual(len(config[main.PHONE_GIT_CONTEXTS_KEY]), 1)
        self.assertEqual(
            config[main.PHONE_GIT_CONTEXTS_KEY][first["project_phone"]]["git_context_key"],
            "github.com/example/example-project",
        )

    async def test_existing_project_gets_one_free_phone_without_recreation(self) -> None:
        context_key = "github.com/example/legacy"
        legacy_entry = {
            "project_name": "Legacy",
            "git_address": "https://github.com/example/legacy.git",
            "git_context_key": context_key,
            "created_at": "2025-01-02T03:04:05+00:00",
            "updated_at": "2025-01-02T03:04:05+00:00",
        }
        other_context_key = "github.com/example/other"
        self.write_config(
            {
                main.PROJECTS_KEY: {
                    context_key: legacy_entry,
                    other_context_key: {
                        "project_name": "Other",
                        "git_address": "https://github.com/example/other.git",
                        "git_context_key": other_context_key,
                    },
                },
                main.PHONE_GIT_CONTEXTS_KEY: {
                    "9001": {
                        "project_name": "Other",
                        "git_address": "https://github.com/example/other.git",
                        "git_context_key": other_context_key,
                        "phone": "9001",
                    }
                },
            }
        )
        self.write_agents(
            [
                {
                    "id": "occupied-9000",
                    "name": "Occupied 9000",
                    "phone": "9000",
                    "profile": "",
                },
                {
                    "id": "occupied-9002",
                    "name": "Occupied 9002",
                    "phone": "9002",
                    "profile": "",
                },
            ]
        )

        first = await self.resolve(
            {"git_address": "git@github.com:example/legacy.git"}
        )
        second = await self.resolve(
            {"git_address": "https://github.com/example/legacy.git"}
        )

        self.assertFalse(first["created"])
        self.assertFalse(second["created"])
        self.assertTrue(first["phone_assigned"])
        self.assertFalse(second["phone_assigned"])
        self.assertEqual(first["project_phone"], "9003")
        self.assertEqual(second["project_phone"], "9003")
        self.assertIn("9003", second["project"]["phones"])
        config = json.loads(main.git_config_path.read_text(encoding="utf-8"))
        self.assertEqual(
            config[main.PROJECTS_KEY][context_key],
            {**legacy_entry, "project_phone": "9003"},
        )
        self.assertEqual(len(config[main.PROJECTS_KEY]), 2)
        self.assertEqual(
            config[main.PHONE_GIT_CONTEXTS_KEY]["9003"]["git_context_key"],
            context_key,
        )

    async def test_self_hosted_paths_keep_case_and_generic_scp_agents_match(self) -> None:
        upper_context = "git.example/Org/Repo"
        self.write_config(
            {
                main.PROJECTS_KEY: {
                    upper_context: {
                        "project_name": "Upper Repo",
                        "git_address": "https://git.example/Org/Repo.git",
                        "git_context_key": upper_context,
                    }
                }
            }
        )
        self.write_agents(
            [
                {
                    "id": "generic-scp",
                    "name": "Generic SCP",
                    "phone": "9301",
                    "profile": "",
                    "parameters": {
                        "git_context_key": "git@git.example:Org/Repo.git"
                    },
                }
            ]
        )

        upper = await self.resolve(
            {"git_address": "ssh://git@git.example:22/Org/Repo.git"}
        )
        lower = await self.resolve(
            {"git_address": "https://git.example/org/repo.git"}
        )

        self.assertFalse(upper["created"])
        self.assertEqual(upper["project"]["git_context_key"], upper_context)
        self.assertEqual([agent["id"] for agent in upper["agents"]], ["generic-scp"])
        self.assertTrue(lower["created"])
        self.assertEqual(lower["project"]["git_context_key"], "git.example/org/repo")
        config = json.loads(main.git_config_path.read_text(encoding="utf-8"))
        self.assertEqual(len(config[main.PROJECTS_KEY]), 2)

    async def test_project_agents_follow_include_exclude_and_phone_mapping_rules(self) -> None:
        context_key = "github.com/example/repo"
        self.write_config(
            {
                main.PROJECTS_KEY: {
                    context_key: {
                        "project_name": "Repo",
                        "git_address": "https://github.com/example/repo.git",
                        "git_context_key": context_key,
                    }
                },
                main.PHONE_GIT_CONTEXTS_KEY: {
                    "9101": {
                        "project_name": "Repo",
                        "git_address": "https://github.com/example/repo.git",
                        "git_context_key": context_key,
                        "phone": "9101",
                    }
                },
            }
        )
        self.write_agents(
            [
                {
                    "id": "phone-mapped",
                    "name": "Phone mapped",
                    "phone": "9101",
                    "profile": "secret profile",
                    "parameters": {"login_password": "must-not-leak"},
                    "template_source": "prompts/phone-mapped.md",
                    "status": "busy",
                },
                {
                    "id": "explicit",
                    "name": "Explicit",
                    "phone": "9102",
                    "profile": "secret profile",
                    "parameters": {"git_context_key": context_key},
                },
                {
                    "id": "list",
                    "name": "List",
                    "phone": "9103",
                    "profile": "secret profile",
                    "parameters": {"git_context_keys": f"other; {context_key}"},
                },
                {
                    "id": "excluded",
                    "name": "Excluded",
                    "phone": "9104",
                    "profile": "secret profile",
                    "parameters": {
                        "git_context_key": context_key,
                        "git_context_excluded_keys": context_key,
                    },
                },
                {
                    "id": "unrelated",
                    "name": "Unrelated",
                    "phone": "9105",
                    "profile": "secret profile",
                    "parameters": {"git_context_key": "github.com/elsewhere/repo"},
                },
            ]
        )

        response = await self.resolve(
            {"git_address": "https://github.com/example/repo.git"}
        )

        self.assertFalse(response["created"])
        self.assertEqual(response["project_phone"], "9000")
        self.assertTrue(response["phone_assigned"])
        self.assertEqual(response["project"]["project_phone"], "9000")
        self.assertEqual(response["agent_count"], 3)
        self.assertEqual(
            {agent["id"] for agent in response["agents"]},
            {"phone-mapped", "explicit", "list"},
        )
        agents_by_id = {agent["id"]: agent for agent in response["agents"]}
        self.assertEqual(
            agents_by_id["phone-mapped"]["profile"],
            "secret profile",
        )
        self.assertEqual(
            agents_by_id["phone-mapped"]["parameters"]["login_password"],
            "must-not-leak",
        )
        self.assertEqual(
            agents_by_id["phone-mapped"]["template_source"],
            "prompts/phone-mapped.md",
        )
        self.assertEqual(agents_by_id["phone-mapped"]["status"], "busy")
        self.assertEqual(agents_by_id["explicit"]["status"], "active")

    async def test_ambiguous_repository_requires_context_key(self) -> None:
        git_address = "https://github.com/example/multi.git"
        self.write_config(
            {
                main.PROJECTS_KEY: {
                    "github.com/example/multi#backend": {
                        "project_name": "Backend",
                        "git_address": git_address,
                        "git_context_key": "github.com/example/multi#backend",
                    },
                    "github.com/example/multi#frontend": {
                        "project_name": "Frontend",
                        "git_address": git_address,
                        "git_context_key": "github.com/example/multi#frontend",
                    },
                }
            }
        )

        with self.assertRaises(HTTPException) as caught:
            await self.resolve({"git_address": git_address})
        self.assertEqual(caught.exception.status_code, 409)
        self.assertEqual(caught.exception.detail["error"], "ambiguous_project")
        self.assertEqual(len(caught.exception.detail["candidates"]), 2)

        selected = await self.resolve(
            {
                "git_address": git_address,
                "git_context_key": "github.com/example/multi#frontend",
            }
        )
        self.assertFalse(selected["created"])
        self.assertEqual(
            selected["project"]["git_context_key"],
            "github.com/example/multi#frontend",
        )

        with self.assertRaises(HTTPException) as typo_caught:
            await self.resolve(
                {
                    "git_address": git_address,
                    "git_context_key": "github.com/example/multi#fronted",
                }
            )
        self.assertEqual(typo_caught.exception.status_code, 409)
        self.assertEqual(
            typo_caught.exception.detail["error"],
            "project_context_not_found",
        )
        config = json.loads(main.git_config_path.read_text(encoding="utf-8"))
        self.assertEqual(len(config[main.PROJECTS_KEY]), 2)

    async def test_project_manager_phone_is_restored_and_cannot_be_reassigned(self) -> None:
        agents = main.read_agents_file()
        self.assertEqual(agents[0]["id"], main.PROJECT_MANAGER_AGENT_ID)
        self.assertEqual(agents[0]["phone"], "0001")

        saved = await main.save_agents([])
        self.assertEqual(len(saved), 1)
        self.assertEqual(saved[0]["phone"], "0001")

        with self.assertRaises(HTTPException) as caught:
            await main.save_agents(
                [
                    {
                        "id": "intruder",
                        "name": "Intruder",
                        "phone": "0001",
                        "profile": "",
                    }
                ]
            )
        self.assertEqual(caught.exception.status_code, 409)

        with self.assertRaises(HTTPException) as caught:
            await main.save_git_address(
                8025,
                "https://github.com/example/repo.git",
                phone="0001",
            )
        self.assertEqual(caught.exception.status_code, 409)
        self.assertFalse(main.git_config_path.exists())

        context_key = "github.com/example/reserved"
        self.write_config(
            {
                main.PROJECTS_KEY: {
                    context_key: {
                        "project_name": "Reserved",
                        "git_address": "https://github.com/example/reserved.git",
                        "git_context_key": context_key,
                    }
                },
                main.PHONE_GIT_CONTEXTS_KEY: {
                    "0001": {
                        "project_name": "Reserved",
                        "git_address": "https://github.com/example/reserved.git",
                        "git_context_key": context_key,
                        "phone": "0001",
                    }
                },
            }
        )
        response = await self.resolve(
            {"git_address": "https://github.com/example/reserved.git"}
        )
        self.assertNotIn("0001", response["project"]["phones"])

    async def test_response_sanitizes_credentials_from_legacy_git_config(self) -> None:
        context_key = "github.com/example/private"
        self.write_config(
            {
                main.PROJECTS_KEY: {
                    context_key: {
                        "project_name": "Private",
                        "git_address": (
                            "https://token:secret@github.com/example/private.git"
                            "?access_token=also-secret"
                        ),
                        "git_context_key": context_key,
                    }
                }
            }
        )

        response = await self.resolve(
            {"git_address": "https://github.com/example/private.git"}
        )

        response_text = json.dumps(response)
        self.assertNotIn("token", response_text)
        self.assertNotIn("secret", response_text)
        self.assertEqual(
            response["project"]["git_address"],
            "https://github.com/example/private.git",
        )

        invalid_context_key = "github.com/example/healed"
        self.write_config(
            {
                main.PROJECTS_KEY: {
                    invalid_context_key: {
                        "project_name": "Invalid legacy",
                        "git_address": "not-a-url token=raw-secret",
                        "git_context_key": invalid_context_key,
                    }
                }
            }
        )
        healed = await self.resolve(
            {"git_address": "https://github.com/example/healed.git"}
        )
        self.assertTrue(healed["created"])
        self.assertNotIn("raw-secret", json.dumps(healed))
        self.assertEqual(
            healed["project"]["git_address"],
            "https://github.com/example/healed.git",
        )

    async def test_cross_repository_context_key_is_rejected_or_ignored(self) -> None:
        with self.assertRaises(HTTPException) as caught:
            await main.save_git_address(
                8025,
                "https://github.com/example/repo-a.git",
                phone="9101",
                git_context_key="github.com/example/repo-b",
            )
        self.assertEqual(caught.exception.status_code, 400)
        self.assertFalse(main.git_config_path.exists())

        mismatched_entry = {
            "project_name": "Wrong legacy mapping",
            "git_address": "https://github.com/example/repo-a.git",
            "git_context_key": "github.com/example/repo-b",
        }
        self.write_config({"8025": mismatched_entry})
        response = await self.resolve(
            {"git_address": "https://github.com/example/repo-a.git"}
        )
        self.assertTrue(response["created"])
        self.assertEqual(
            response["project"]["git_context_key"],
            "github.com/example/repo-a",
        )
        config = json.loads(main.git_config_path.read_text(encoding="utf-8"))
        self.assertEqual(config["8025"], mismatched_entry)

        correct_registry_entry = {
            "project_name": "Correct B",
            "git_address": "https://github.com/example/repo-b.git",
            "git_context_key": "github.com/example/repo-b",
        }
        self.write_config(
            {
                "8025": mismatched_entry,
                main.PROJECTS_KEY: {
                    "github.com/example/repo-b": correct_registry_entry,
                },
            }
        )
        resolved_b = await self.resolve(
            {"git_address": "https://github.com/example/repo-b.git"}
        )
        self.assertFalse(resolved_b["created"])
        self.assertEqual(resolved_b["project"]["project_name"], "Correct B")
        self.assertEqual(
            resolved_b["project"]["git_address"],
            "https://github.com/example/repo-b.git",
        )

    async def test_concurrent_first_requests_create_one_project(self) -> None:
        payload = {"git_address": "https://github.com/example/concurrent.git"}
        first, second = await asyncio.gather(self.resolve(payload), self.resolve(payload))

        self.assertEqual(sorted([first["created"], second["created"]]), [False, True])
        self.assertEqual(first["project_phone"], second["project_phone"])
        self.assertEqual(
            sorted([first["phone_assigned"], second["phone_assigned"]]),
            [False, True],
        )
        config = json.loads(main.git_config_path.read_text(encoding="utf-8"))
        self.assertEqual(len(config[main.PROJECTS_KEY]), 1)
        self.assertEqual(len(config[main.PHONE_GIT_CONTEXTS_KEY]), 1)

    async def test_exhausted_project_phone_range_does_not_partially_create_project(self) -> None:
        self.write_config(
            {
                main.PHONE_GIT_CONTEXTS_KEY: {
                    f"{phone:04d}": {"occupied": True}
                    for phone in range(main.PROJECT_PHONE_MIN, main.PROJECT_PHONE_MAX + 1)
                }
            }
        )

        with self.assertRaises(HTTPException) as caught:
            await self.resolve(
                {"git_address": "https://github.com/example/no-phone-left.git"}
            )

        self.assertEqual(caught.exception.status_code, 409)
        config = json.loads(main.git_config_path.read_text(encoding="utf-8"))
        self.assertNotIn(main.PROJECTS_KEY, config)

    async def test_canonical_project_phone_cannot_be_reassigned_or_deleted(self) -> None:
        response = await self.resolve(
            {"git_address": "https://github.com/example/canonical.git"}
        )
        project_phone = response["project_phone"]

        with self.assertRaises(HTTPException) as reassign_caught:
            await main.save_git_address(
                8025,
                "https://github.com/example/different.git",
                phone=project_phone,
            )
        self.assertEqual(reassign_caught.exception.status_code, 409)

        with self.assertRaises(HTTPException) as delete_caught:
            await main.delete_git_context_phone(project_phone)
        self.assertEqual(delete_caught.exception.status_code, 409)

        repeated = await self.resolve(
            {"git_address": "git@github.com:example/canonical.git"}
        )
        self.assertFalse(repeated["created"])
        self.assertFalse(repeated["phone_assigned"])
        self.assertEqual(repeated["project_phone"], project_phone)

    async def test_canonical_phone_conflict_with_legacy_mapping_is_not_overwritten(self) -> None:
        context_key = "github.com/example/canonical-a"
        conflicting_mapping = {
            "project_name": "Legacy B",
            "git_address": "https://github.com/example/legacy-b.git",
            "phone": "9000",
        }
        self.write_config(
            {
                main.PROJECTS_KEY: {
                    context_key: {
                        "project_name": "Canonical A",
                        "git_address": "https://github.com/example/canonical-a.git",
                        "git_context_key": context_key,
                        "project_phone": "9000",
                    }
                },
                main.PHONE_GIT_CONTEXTS_KEY: {"9000": conflicting_mapping},
            }
        )

        with self.assertRaises(HTTPException) as caught:
            await self.resolve(
                {"git_address": "https://github.com/example/canonical-a.git"}
            )

        self.assertEqual(caught.exception.status_code, 409)
        self.assertEqual(caught.exception.detail["error"], "project_phone_conflict")
        config = json.loads(main.git_config_path.read_text(encoding="utf-8"))
        self.assertEqual(
            config[main.PHONE_GIT_CONTEXTS_KEY]["9000"],
            conflicting_mapping,
        )

    async def test_registry_project_phone_wins_over_secondary_context_entries(self) -> None:
        context_key = "github.com/example/source-of-truth"
        base_entry = {
            "project_name": "Source of truth",
            "git_address": "https://github.com/example/source-of-truth.git",
            "git_context_key": context_key,
        }
        self.write_config(
            {
                main.PROJECTS_KEY: {
                    context_key: {**base_entry, "project_phone": "9000"}
                },
                main.PHONE_GIT_CONTEXTS_KEY: {
                    "9000": {
                        **base_entry,
                        "phone": "9000",
                        "project_phone": "9000",
                    },
                    "9001": {
                        **base_entry,
                        "phone": "9001",
                        "project_phone": "9001",
                    },
                },
            }
        )

        response = await self.resolve(
            {"git_address": "https://github.com/example/source-of-truth.git"}
        )

        self.assertFalse(response["created"])
        self.assertFalse(response["phone_assigned"])
        self.assertEqual(response["project_phone"], "9000")
        config = json.loads(main.git_config_path.read_text(encoding="utf-8"))
        self.assertEqual(
            config[main.PROJECTS_KEY][context_key]["project_phone"],
            "9000",
        )

    async def test_project_resolution_does_not_contact_git_remote(self) -> None:
        original_resolver = main.resolve_git_reference

        def forbidden_resolver(_git_address: str) -> dict[str, object]:
            raise AssertionError("Project Manager must not contact the Git remote")

        main.resolve_git_reference = forbidden_resolver
        try:
            response = await self.resolve(
                {"git_address": "https://github.com/example/no-network.git"}
            )
        finally:
            main.resolve_git_reference = original_resolver

        self.assertTrue(response["created"])

    def test_interprocess_transactions_do_not_lose_projects(self) -> None:
        worker_source = """
import sys
from pathlib import Path
import main

main.git_config_path = Path(sys.argv[1])
main.agents_path = Path(sys.argv[2])
Path(sys.argv[3]).write_text("ready", encoding="utf-8")
created, project, project_phone, phone_assigned = main.resolve_or_create_project_transaction(sys.argv[4], sys.argv[5])
print(f"{created}:{project_phone}:{phone_assigned}:{project['git_context_key']}")
"""
        temp_path = Path(self.temp_dir.name)
        marker_one = temp_path / "worker-one.ready"
        marker_two = temp_path / "worker-two.ready"
        urls_and_keys = [
            ("https://github.com/example/process-one.git", "github.com/example/process-one"),
            ("https://github.com/example/process-two.git", "github.com/example/process-two"),
        ]
        environment = dict(os.environ)
        environment["PYTHONUTF8"] = "1"
        processes: list[subprocess.Popen[str]] = []

        with main.git_config_file_lock():
            for marker, (git_address, context_key) in zip(
                (marker_one, marker_two),
                urls_and_keys,
            ):
                processes.append(
                    subprocess.Popen(
                        [
                            sys.executable,
                            "-c",
                            worker_source,
                            str(main.git_config_path),
                            str(main.agents_path),
                            str(marker),
                            git_address,
                            context_key,
                        ],
                        cwd=Path(main.__file__).resolve().parent,
                        env=environment,
                        text=True,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                    )
                )

            deadline = time.monotonic() + 15
            while not (marker_one.exists() and marker_two.exists()):
                if time.monotonic() >= deadline:
                    self.fail("Worker processes did not reach the Git config transaction")
                time.sleep(0.05)
            self.assertTrue(all(process.poll() is None for process in processes))

        for process in processes:
            stdout, stderr = process.communicate(timeout=15)
            self.assertEqual(process.returncode, 0, msg=stderr or stdout)

        config = json.loads(main.git_config_path.read_text(encoding="utf-8"))
        self.assertEqual(
            set(config[main.PROJECTS_KEY]),
            {context_key for _, context_key in urls_and_keys},
        )
        self.assertEqual(len(config[main.PHONE_GIT_CONTEXTS_KEY]), 2)
        self.assertEqual(
            len(set(config[main.PHONE_GIT_CONTEXTS_KEY])),
            2,
        )

        agent_worker_source = """
import sys
from pathlib import Path
import main

main.agents_path = Path(sys.argv[1])
Path(sys.argv[2]).write_text("ready", encoding="utf-8")
agent = main.create_empty_agent_transaction("http://testserver:8025")
print(agent["phone"])
"""
        agent_markers = [
            temp_path / "agent-worker-one.ready",
            temp_path / "agent-worker-two.ready",
        ]
        agent_processes: list[subprocess.Popen[str]] = []
        with main.agents_file_lock():
            for marker in agent_markers:
                agent_processes.append(
                    subprocess.Popen(
                        [
                            sys.executable,
                            "-c",
                            agent_worker_source,
                            str(main.agents_path),
                            str(marker),
                        ],
                        cwd=Path(main.__file__).resolve().parent,
                        env=environment,
                        text=True,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                    )
                )

            deadline = time.monotonic() + 15
            while not all(marker.exists() for marker in agent_markers):
                if time.monotonic() >= deadline:
                    self.fail("Worker processes did not reach the agents transaction")
                time.sleep(0.05)
            self.assertTrue(all(process.poll() is None for process in agent_processes))

        created_phones: set[str] = set()
        for process in agent_processes:
            stdout, stderr = process.communicate(timeout=15)
            self.assertEqual(process.returncode, 0, msg=stderr or stdout)
            created_phones.add(stdout.strip())
        self.assertEqual(len(created_phones), 2)
        empty_agents = [
            agent
            for agent in main.read_agents_file()
            if agent.get("status") == "empty"
        ]
        self.assertEqual(len(empty_agents), 2)

    async def test_invalid_address_and_wrong_project_manager_route_contract(self) -> None:
        with self.assertRaises(HTTPException) as caught:
            await self.resolve({"git_address": "https://token@github.com/example/repo.git"})
        self.assertEqual(caught.exception.status_code, 400)
        with self.assertRaises(HTTPException) as caught:
            await self.resolve(
                {"git_address": "ssh://git:secret@github.com/example/repo.git"}
            )
        self.assertEqual(caught.exception.status_code, 400)
        with self.assertRaises(HTTPException) as caught:
            await self.resolve({"git_address": r"D:\repos\repo#one"})
        self.assertEqual(caught.exception.status_code, 400)

        project_manager_routes = {
            route.path
            for route in main.app.routes
            if route.path.startswith("/project-manager/")
        }
        self.assertEqual(project_manager_routes, {"/project-manager/0001"})

        not_found_status, _ = await asgi_request(
            "/project-manager/0002",
            payload={"git_address": "https://github.com/example/repo.git"},
        )
        method_status, _ = await asgi_request(
            "/project-manager/0001",
            method="GET",
        )
        content_type_status, _ = await asgi_request(
            "/project-manager/0001",
            payload={"git_address": "https://github.com/example/repo.git"},
            content_type="text/plain",
        )
        self.assertEqual(not_found_status, 404)
        self.assertEqual(method_status, 405)
        self.assertEqual(content_type_status, 400)

        success_status, success_body = await asgi_request(
            "/project-manager/0001",
            payload={"git_address": "https://github.com/example/http-success.git"},
        )
        self.assertEqual(success_status, 200)
        self.assertEqual(success_body["project_manager_phone"], "0001")
        self.assertTrue(success_body["created"])
        self.assertTrue(success_body["phone_assigned"])
        self.assertIn(
            success_body["project_phone"],
            success_body["project"]["phones"],
        )


if __name__ == "__main__":
    unittest.main()
