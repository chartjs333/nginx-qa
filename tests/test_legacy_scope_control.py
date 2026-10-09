import asyncio
import hashlib
import json
import os
import secrets
import subprocess
import tempfile
import unittest
import urllib.parse
from collections import deque
from copy import deepcopy
from pathlib import Path
from unittest.mock import patch

import main
from nginx_qa.legacy_scope_control import (
    LegacyScopeControlError,
    manifest_scope_delta,
)
from nginx_qa.scope_control_prestart import (
    CompatibilityError,
    validate_scope_control_compatibility,
)


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

    request_headers = [(b"host", b"testserver:18025")]
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
        "client": ("127.0.0.1", 18025),
        "server": ("testserver", 18025),
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


class LegacyScopeControlTests(unittest.IsolatedAsyncioTestCase):
    PROJECT_PHONE = "9008"
    PROJECT_CONTEXT = "github.com/example/scope-control"
    GIT_ADDRESS = "https://github.com/example/scope-control.git"
    BRANCH = "agent/scope-amendment"
    MANIFEST_PATH = "orchestration/sprints/scope-control.json"
    AMENDMENT_PATH = "orchestration/sprints/scope-amendment.json"
    AMENDMENT_ID = "DELTA-SCOPE-CONTROL-TEST"
    COORDINATOR_PHONE = "2750"
    FORMAL_PHONE = "2753"
    REVIEWER_ONE_PHONE = "2791"
    REVIEWER_TWO_PHONE = "2792"
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
        self.temp_path = Path(self.temp_dir.name)
        self.repo_path = self.temp_path / "source-repository"
        self.sequential_agent_latest_file_template = str(
            self.temp_path
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
        main.git_config_path = self.temp_path / "port_git_map.json"
        main.agents_path = self.temp_path / "agents.json"
        main.history_path = self.temp_path / "conversation_log.jsonl"
        main.sprint_history_path = self.temp_path / "project_sprints.json"
        main.pending_sprints_path = self.temp_path / "pending_project_sprints.json"
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

        self.admin_token = secrets.token_urlsafe(32)
        self.role_tokens = {
            self.COORDINATOR_PHONE: secrets.token_urlsafe(32),
            self.FORMAL_PHONE: secrets.token_urlsafe(32),
            self.REVIEWER_ONE_PHONE: secrets.token_urlsafe(32),
            self.REVIEWER_TWO_PHONE: secrets.token_urlsafe(32),
        }
        self.scope_env_names = [
            "NGINX_QA_SCOPE_CONTROL_ADMIN_TOKEN",
            "NGINX_QA_SCOPE_CONTROL_REPOSITORY_MAP",
            "NGINX_QA_MANAGED_ROOT",
            *[
                f"NGINX_QA_SCOPE_CONTROL_ROLE_TOKEN_{phone}"
                for phone in self.role_tokens
            ],
        ]
        all_env_names = (*self.TELEGRAM_ENV_NAMES, *self.scope_env_names)
        self.original_env = {name: os.environ.get(name) for name in all_env_names}
        for name in all_env_names:
            os.environ.pop(name, None)
        os.environ["NGINX_QA_SCOPE_CONTROL_ADMIN_TOKEN"] = self.admin_token
        for phone, token in self.role_tokens.items():
            os.environ[f"NGINX_QA_SCOPE_CONTROL_ROLE_TOKEN_{phone}"] = token

        main.write_sequential_prompt_settings_file(
            str(self.temp_path / "prompts" / "{repository}"),
            self.sequential_agent_latest_file_template,
        )
        self._write_project()
        main.agents_path.write_text('{"agents": []}', encoding="utf-8")
        self.base_manifest = self._base_manifest()
        self._init_source_repository()

    def tearDown(self) -> None:
        for name, value in self.original_values.items():
            setattr(main, name, value)
        for name, value in self.original_env.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        self.temp_dir.cleanup()

    def _write_project(self) -> None:
        project = {
            "project_name": "Scope Control Test Project",
            "git_address": self.GIT_ADDRESS,
            "git_context_key": self.PROJECT_CONTEXT,
            "project_phone": self.PROJECT_PHONE,
            "groups": [],
            "group_relationships": [],
            "customer_reporting": {},
        }
        config = {
            main.PROJECTS_KEY: {self.PROJECT_CONTEXT: project},
            main.PHONE_GIT_CONTEXTS_KEY: {
                self.PROJECT_PHONE: {**project, "phone": self.PROJECT_PHONE}
            },
        }
        main.git_config_path.write_text(
            json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    def _base_manifest(self) -> dict[str, object]:
        return {
            "sprint": {"id": "scope-control-sprint", "title": "Scope control"},
            "git_address": self.GIT_ADDRESS,
            "agents": {"overwrite": True},
            "execution": {
                "mode": "sequential",
                "start_node": "continuity-coordinator",
                "max_rework_cycles": 20,
                "required_approvals": 2,
                "reviewers": [
                    {
                        "id": "reviewer-one",
                        "name": "Reviewer One",
                        "phone": self.REVIEWER_ONE_PHONE,
                        "git_branch": "review/scope-one",
                        "profile": "ISSUED REVIEWER ONE PROFILE",
                    },
                    {
                        "id": "reviewer-two",
                        "name": "Reviewer Two",
                        "phone": self.REVIEWER_TWO_PHONE,
                        "git_branch": "review/scope-two",
                        "profile": "ISSUED REVIEWER TWO PROFILE",
                    },
                ],
            },
            "nodes": [
                {
                    "id": "continuity-coordinator",
                    "agent": {
                        "id": "continuity-coordinator",
                        "name": "Continuity Coordinator",
                        "phone": self.COORDINATOR_PHONE,
                        "git_branch": "agent/scope-coordinator",
                        "profile": "ISSUED COORDINATOR PROFILE: do not start proof work.",
                    },
                    "tasks": [
                        {
                            "task_id": "COORDINATE",
                            "queue": "worker-all",
                            "message": "ISSUED COORDINATOR TASK: do not start R2.3/R3.",
                        }
                    ],
                    "transitions": {"RESUME_FORMAL": "formal-linkage"},
                },
                {
                    "id": "formal-linkage",
                    "agent": {
                        "id": "formal-linkage",
                        "name": "Formal Linkage",
                        "phone": self.FORMAL_PHONE,
                        "git_branch": "agent/scope-formal",
                        "profile": "ISSUED FORMAL PROFILE: use the old scope.",
                    },
                    "tasks": [
                        {
                            "task_id": "FORMAL",
                            "queue": "worker-all",
                            "message": "ISSUED FORMAL TASK: evaluate the old scope.",
                        }
                    ],
                    "transitions": {"NO_GO": "continuity-coordinator"},
                },
            ],
        }

    def _git(self, *arguments: str) -> str:
        return self._git_at(self.repo_path, *arguments)

    @staticmethod
    def _git_at(repo_path: Path, *arguments: str) -> str:
        completed = subprocess.run(
            ["git", "-C", str(repo_path), *arguments],
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        return completed.stdout.strip()

    @staticmethod
    def _json_bytes(value: object) -> bytes:
        return (
            json.dumps(value, ensure_ascii=False, indent=2) + "\n"
        ).encode("utf-8")

    def _write_repo_json(self, relative_path: str, value: object) -> bytes:
        data = self._json_bytes(value)
        path = self.repo_path / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        return data

    @staticmethod
    def _sha256(data: bytes) -> str:
        return hashlib.sha256(data).hexdigest()

    def _init_source_repository(self) -> None:
        self.repo_path.mkdir(parents=True)
        subprocess.run(
            ["git", "init", "--quiet", "--initial-branch", self.BRANCH, str(self.repo_path)],
            check=True,
            capture_output=True,
        )
        self._git("config", "user.email", "scope-control-tests@example.invalid")
        self._git("config", "user.name", "Scope Control Tests")
        self._git("remote", "add", "origin", self.GIT_ADDRESS)
        base_bytes = self._write_repo_json(self.MANIFEST_PATH, self.base_manifest)
        self.base_manifest_sha256 = self._sha256(base_bytes)
        self._git("add", self.MANIFEST_PATH)
        self._git("commit", "--quiet", "-m", "base scope")
        self.base_commit = self._git("rev-parse", "HEAD")

    def _admin_headers(self, token: str | None = None) -> list[tuple[bytes, bytes]]:
        return [
            (
                b"x-nginx-qa-scope-control-token",
                (self.admin_token if token is None else token).encode("utf-8"),
            )
        ]

    def _role_headers(self, phone: str) -> list[tuple[bytes, bytes]]:
        return [
            (b"x-nginx-qa-scope-token", self.role_tokens[phone].encode("utf-8"))
        ]

    async def _import_sprint(self) -> dict[str, object]:
        status_code, response = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/import",
            method="POST",
            payload=self.base_manifest,
        )
        self.assertEqual(status_code, 201, response)
        self.assertIsInstance(response, dict)
        assert isinstance(response, dict)
        self.sprint_id = str(response["sprint"]["id"])
        return response

    async def _claim(self, agent_id: str) -> dict[str, object]:
        phone_by_agent = {
            "continuity-coordinator": self.COORDINATOR_PHONE,
            "formal-linkage": self.FORMAL_PHONE,
            "reviewer-one": self.REVIEWER_ONE_PHONE,
            "reviewer-two": self.REVIEWER_TWO_PHONE,
        }
        status_code, response = await asgi_request(
            "/api/v1/agents/whoami/repository",
            method="POST",
            payload={"git_address": self.GIT_ADDRESS},
            headers=self._role_headers(phone_by_agent[agent_id]),
        )
        self.assertEqual(status_code, 200, response)
        self.assertIsInstance(response, dict)
        assert isinstance(response, dict)
        self.assertEqual(response["agent"]["id"], agent_id, response)
        return response

    @staticmethod
    def _assignment_id(identity: dict[str, object]) -> str:
        return str(identity["active_task"]["metadata"]["assignment_id"])

    async def _submit_issued(
        self,
        phone: str,
        assignment_id: str,
        result_status: str,
        *,
        result: str = "",
        feedback: str = "",
    ) -> dict[str, object]:
        payload: dict[str, object] = {
            "assignment_id": assignment_id,
            "status": result_status,
        }
        if result:
            payload["result"] = result
        if feedback:
            payload["feedback"] = feedback
        status_code, response = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/{phone}/whoami",
            method="POST",
            payload=payload,
        )
        self.assertEqual(status_code, 200, response)
        self.assertIsInstance(response, dict)
        assert isinstance(response, dict)
        return response

    async def _pre_amendment_round_trip(self) -> tuple[list[dict[str, object]], dict[str, object]]:
        completed: list[dict[str, object]] = []
        coordinator = await self._claim("continuity-coordinator")
        await self._submit_issued(
            self.COORDINATOR_PHONE,
            self._assignment_id(coordinator),
            "RESUME_FORMAL",
            result="Historical coordinator handoff.",
        )
        reviewer_one = await self._claim("reviewer-one")
        completed.append(reviewer_one)
        await self._submit_issued(
            self.REVIEWER_ONE_PHONE,
            self._assignment_id(reviewer_one),
            "APPROVE",
            feedback="Historical reviewer one approval.",
        )
        reviewer_two = await self._claim("reviewer-two")
        completed.append(reviewer_two)
        await self._submit_issued(
            self.REVIEWER_TWO_PHONE,
            self._assignment_id(reviewer_two),
            "APPROVE",
            feedback="Historical reviewer two approval.",
        )
        formal = await self._claim("formal-linkage")
        completed.insert(0, coordinator)
        completed.append(formal)
        await self._submit_issued(
            self.FORMAL_PHONE,
            self._assignment_id(formal),
            "NO_GO",
            result="Historical formal NO_GO.",
        )
        reviewer_one_back = await self._claim("reviewer-one")
        completed.append(reviewer_one_back)
        await self._submit_issued(
            self.REVIEWER_ONE_PHONE,
            self._assignment_id(reviewer_one_back),
            "APPROVE",
            feedback="Historical return approval one.",
        )
        reviewer_two_back = await self._claim("reviewer-two")
        completed.append(reviewer_two_back)
        await self._submit_issued(
            self.REVIEWER_TWO_PHONE,
            self._assignment_id(reviewer_two_back),
            "APPROVE",
            feedback="Historical return approval two.",
        )
        current = await self._claim("continuity-coordinator")
        return completed, current

    def _create_amendment_commit(self, assignment_id: str) -> None:
        target_manifest = deepcopy(self.base_manifest)
        nodes = target_manifest["nodes"]
        nodes[0]["agent"]["profile"] = (
            "EFFECTIVE COORDINATOR PROFILE: recovery proof-only scope is authorized."
        )
        nodes[0]["tasks"][0]["message"] = (
            "EFFECTIVE COORDINATOR TASK: perform only the authorized recovery proof."
        )
        nodes[1]["agent"]["profile"] = (
            "EFFECTIVE FORMAL PROFILE: assess only the recovery proof."
        )
        nodes[1]["tasks"][0]["message"] = (
            "EFFECTIVE FORMAL TASK: evaluate the authorized proof-only evidence."
        )
        reviewers = target_manifest["execution"]["reviewers"]
        reviewers[0]["profile"] = (
            "EFFECTIVE REVIEWER ONE PROFILE: review the proof-only amendment."
        )
        reviewers[1]["profile"] = (
            "EFFECTIVE REVIEWER TWO PROFILE: review the proof-only amendment."
        )
        amendment = {
            "task_id": self.AMENDMENT_ID,
            "kind": "USER_AUTHORIZED_SCOPE_AMENDMENT",
            "authorization": {"scope_authorized": True},
            "preserved_assignment_id": assignment_id,
            "from_commit": self.base_commit,
        }
        target_bytes = self._write_repo_json(self.MANIFEST_PATH, target_manifest)
        amendment_bytes = self._write_repo_json(self.AMENDMENT_PATH, amendment)
        self.target_manifest_sha256 = self._sha256(target_bytes)
        self.amendment_sha256 = self._sha256(amendment_bytes)
        self._git("add", self.MANIFEST_PATH, self.AMENDMENT_PATH)
        self._git("commit", "--quiet", "-m", "authorized scope amendment")
        self.target_commit = self._git("rev-parse", "HEAD")
        self.target_manifest = target_manifest

    def _create_additional_amendment_commit(
        self, assignment_id: str
    ) -> dict[str, str]:
        branch = "agent/scope-amendment-two"
        amendment_id = "DELTA-SCOPE-CONTROL-TEST-TWO"
        self._git("switch", "--quiet", "-c", branch, self.base_commit)
        target_bytes = self._write_repo_json(self.MANIFEST_PATH, self.target_manifest)
        amendment_bytes = self._write_repo_json(
            self.AMENDMENT_PATH,
            {
                "task_id": amendment_id,
                "kind": "USER_AUTHORIZED_SCOPE_AMENDMENT",
                "authorization": {"scope_authorized": True},
                "preserved_assignment_id": assignment_id,
                "from_commit": self.base_commit,
            },
        )
        self._git("add", self.MANIFEST_PATH, self.AMENDMENT_PATH)
        self._git("commit", "--quiet", "-m", "second authorized amendment")
        return {
            "amendment_id": amendment_id,
            "branch": branch,
            "target_commit": self._git("rev-parse", "HEAD"),
            "target_manifest_sha256": self._sha256(target_bytes),
            "amendment_sha256": self._sha256(amendment_bytes),
        }

    async def _preflight(self) -> dict[str, object]:
        status_code, response = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/sprints/{self.sprint_id}/scope-amendments/preflight",
            headers=self._admin_headers(),
        )
        self.assertEqual(status_code, 200, response)
        self.assertIsInstance(response, dict)
        assert isinstance(response, dict)
        self.assertTrue(response["applicable"])
        return response

    def _amendment_payload(
        self, preflight: dict[str, object], *, idempotency_key: str = "scope-test-apply-1"
    ) -> dict[str, object]:
        return {
            "schema_version": 1,
            "amendment_id": self.AMENDMENT_ID,
            "expected_execution_revision": preflight["execution_revision"],
            "expected_assignment_id": preflight["assignment_id"],
            "expected_node_id": preflight["node_id"],
            "expected_phase": preflight["phase"],
            "expected_issued_task_sha256": preflight["issued_task_sha256"],
            "expected_issued_message_sha256": preflight["issued_message_sha256"],
            "idempotency_key": idempotency_key,
            "source": {
                "repository_key": self.PROJECT_CONTEXT,
                "base_commit": self.base_commit,
                "target_commit": self.target_commit,
                "target_ref": f"refs/heads/{self.BRANCH}",
                "manifest": {
                    "path": self.MANIFEST_PATH,
                    "base_sha256": self.base_manifest_sha256,
                    "target_sha256": self.target_manifest_sha256,
                },
                "amendment": {
                    "path": self.AMENDMENT_PATH,
                    "sha256": self.amendment_sha256,
                },
            },
            "targets": {
                "active_assignment": True,
                "future_node_ids": ["continuity-coordinator", "formal-linkage"],
                "reviewer_agent_ids": ["reviewer-one", "reviewer-two"],
            },
        }

    async def _apply(self, payload: dict[str, object], headers=None) -> tuple[int, object]:
        with patch.object(main, "local_repo_for_remote", return_value=self.repo_path):
            return await asgi_request(
                f"/api/v1/projects/{self.PROJECT_PHONE}/sprints/{self.sprint_id}/scope-amendments",
                method="POST",
                payload=payload,
                headers=self._admin_headers() if headers is None else headers,
            )

    async def _scope_and_ack(
        self, assignment_id: str, phone: str
    ) -> dict[str, object]:
        headers = self._role_headers(phone)
        status_code, scope = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/assignments/{assignment_id}/effective-scope",
            headers=headers,
        )
        self.assertEqual(status_code, 200, scope)
        self.assertIsInstance(scope, dict)
        assert isinstance(scope, dict)
        self._assert_scope_integrity(scope)
        ack_status, ack = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/assignments/{assignment_id}/effective-scope/ack",
            method="POST",
            payload={"schema_version": 1, "scope_context": scope["scope_context"]},
            headers=headers,
        )
        self.assertEqual(ack_status, 200, ack)
        self.assertIsInstance(ack, dict)
        assert isinstance(ack, dict)
        self.assertEqual(ack["assignment_id"], assignment_id)
        return scope

    async def _submit_effective(
        self,
        phone: str,
        assignment_id: str,
        result_status: str,
        scope_context: dict[str, object],
        *,
        result: str = "",
        feedback: str = "",
    ) -> dict[str, object]:
        payload: dict[str, object] = {
            "assignment_id": assignment_id,
            "status": result_status,
            "scope_context": scope_context,
        }
        if result:
            payload["result"] = result
        if feedback:
            payload["feedback"] = feedback
        status_code, response = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/{phone}/whoami",
            method="POST",
            payload=payload,
            headers=self._role_headers(phone),
        )
        self.assertEqual(status_code, 200, response)
        self.assertIsInstance(response, dict)
        assert isinstance(response, dict)
        return response

    def _stored_assignment(self) -> dict[str, object]:
        config = json.loads(main.git_config_path.read_text(encoding="utf-8"))
        return config[main.PROJECTS_KEY][self.PROJECT_CONTEXT][
            main.PROJECT_AGENT_ASSIGNMENT_KEY
        ]

    @staticmethod
    def _canonical(value: object) -> bytes:
        return json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")

    def _assert_scope_integrity(self, scope: dict[str, object]) -> None:
        effective_core = scope["effective_core"]
        digest = hashlib.sha256(self._canonical(effective_core)).hexdigest()
        integrity = scope["effective_scope_integrity"]
        self.assertEqual(integrity["algorithm"], "SHA-256")
        self.assertEqual(
            integrity["canonicalization"], "json-sort-keys-utf8-no-whitespace"
        )
        self.assertEqual(integrity["input"], "effective_core")
        self.assertIs(integrity["projected_active_task_in_hash"], False)
        self.assertEqual(integrity["sha256"], digest)
        self.assertEqual(scope["scope_context"]["effective_scope_sha256"], digest)
        self.assertEqual(scope["effective"]["effective_scope_sha256"], digest)

    def _queue_snapshot(self) -> dict[str, list[dict[str, object]]]:
        return {
            name: [main.queue_item_snapshot(item) for item in queue]
            for name, queue in main.queues.items()
        }

    def test_manifest_scope_allowlist_rejects_graph_identity_and_policy_changes(
        self,
    ) -> None:
        targets = {
            "future_node_ids": ["continuity-coordinator", "formal-linkage"],
            "reviewer_agent_ids": ["reviewer-one", "reviewer-two"],
        }
        allowed = deepcopy(self.base_manifest)
        allowed["nodes"][0]["agent"]["profile"] = "Amended coordinator profile"
        allowed["nodes"][0]["tasks"][0]["message"] = "Amended task message"
        allowed["execution"]["reviewers"][0]["profile"] = "Amended reviewer profile"
        delta = manifest_scope_delta(self.base_manifest, allowed, targets)
        self.assertEqual(
            delta["node_overrides"]["continuity-coordinator"]["profile"],
            "Amended coordinator profile",
        )

        forbidden_changes = [
            (("nodes",), [*allowed["nodes"], {"id": "new-node", "type": "terminal"}]),
            (("nodes",), list(reversed(allowed["nodes"]))),
            (("nodes", 0, "id"), "renamed-coordinator"),
            (("nodes", 0, "transitions"), {"RESUME_FORMAL": "continuity-coordinator"}),
            (("execution", "start_node"), "formal-linkage"),
            (("execution", "required_approvals"), 1),
            (("execution", "max_rework_cycles"), 99),
            (
                ("execution", "reviewers"),
                list(reversed(allowed["execution"]["reviewers"])),
            ),
            (("nodes", 0, "agent", "id"), "different-agent"),
            (("nodes", 0, "agent", "phone"), "9999"),
            (("nodes", 0, "agent", "git_branch"), "agent/different-branch"),
            (("execution", "reviewers", 0, "id"), "different-reviewer"),
            (("execution", "reviewers", 0, "phone"), "9998"),
            (("execution", "reviewers", 0, "git_branch"), "review/different-branch"),
            (("nodes", 0, "tasks", 0, "task_id"), "DIFFERENT-TASK"),
            (("nodes", 0, "tasks", 0, "queue"), "different-queue"),
            (("nodes", 0, "tasks", 0, "metadata"), {"bypass_review": True}),
        ]
        for path, value in forbidden_changes:
            with self.subTest(path=path, value=value):
                target = deepcopy(allowed)
                parent = target
                for key in path[:-1]:
                    parent = parent[key]
                parent[path[-1]] = deepcopy(value)
                with self.assertRaises(LegacyScopeControlError) as caught:
                    manifest_scope_delta(self.base_manifest, target, targets)
                self.assertEqual(caught.exception.code, "SCOPE_GRAPH_CHANGE_FORBIDDEN")

    def test_repository_map_resolves_non_adjacent_delta_checkout_and_fails_closed(
        self,
    ) -> None:
        app_release = (
            self.temp_path / "application" / "releases" / "nginx-qa-scope-control"
        )
        delta_checkout = (
            self.temp_path / "canonical-projects" / "chartjs333" / "delta"
        )
        app_release.mkdir(parents=True)
        delta_checkout.mkdir(parents=True)

        subprocess.run(
            [
                "git",
                "init",
                "--quiet",
                "--initial-branch",
                "release/18025",
                str(app_release),
            ],
            check=True,
            capture_output=True,
        )
        self._git_at(app_release, "config", "user.email", "app@example.invalid")
        self._git_at(app_release, "config", "user.name", "Application Release")
        self._git_at(
            app_release,
            "remote",
            "add",
            "origin",
            "https://github.com/chartjs333/nginx-qa.git",
        )
        (app_release / "README.md").write_text("release checkout\n", encoding="utf-8")
        self._git_at(app_release, "add", "README.md")
        self._git_at(app_release, "commit", "--quiet", "-m", "release checkout")

        subprocess.run(
            [
                "git",
                "init",
                "--quiet",
                "--initial-branch",
                "agent/delta-scope",
                str(delta_checkout),
            ],
            check=True,
            capture_output=True,
        )
        self._git_at(delta_checkout, "config", "user.email", "delta@example.invalid")
        self._git_at(delta_checkout, "config", "user.name", "Delta Scope Source")
        delta_address = "https://github.com/chartjs333/delta.git"
        delta_key = "github.com/chartjs333/delta"
        self._git_at(delta_checkout, "remote", "add", "origin", delta_address)

        base_manifest = deepcopy(self.base_manifest)
        base_manifest["git_address"] = delta_address
        target_manifest = deepcopy(base_manifest)
        target_manifest["nodes"][0]["agent"]["profile"] = (
            "EFFECTIVE DELTA COORDINATOR PROFILE"
        )
        target_manifest["nodes"][0]["tasks"][0]["message"] = (
            "EFFECTIVE DELTA COORDINATOR TASK"
        )
        manifest_path = delta_checkout / self.MANIFEST_PATH
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        base_bytes = self._json_bytes(base_manifest)
        manifest_path.write_bytes(base_bytes)
        self._git_at(delta_checkout, "add", self.MANIFEST_PATH)
        self._git_at(delta_checkout, "commit", "--quiet", "-m", "Delta base scope")
        base_commit = self._git_at(delta_checkout, "rev-parse", "HEAD")

        assignment_id = "delta-map-assignment"
        amendment_id = "DELTA-MAPPED-SCOPE"
        amendment = {
            "task_id": amendment_id,
            "kind": "USER_AUTHORIZED_SCOPE_AMENDMENT",
            "authorization": {"scope_authorized": True},
            "preserved_assignment_id": assignment_id,
            "from_commit": base_commit,
        }
        target_bytes = self._json_bytes(target_manifest)
        amendment_bytes = self._json_bytes(amendment)
        manifest_path.write_bytes(target_bytes)
        amendment_path = delta_checkout / self.AMENDMENT_PATH
        amendment_path.parent.mkdir(parents=True, exist_ok=True)
        amendment_path.write_bytes(amendment_bytes)
        self._git_at(
            delta_checkout, "add", self.MANIFEST_PATH, self.AMENDMENT_PATH
        )
        self._git_at(
            delta_checkout, "commit", "--quiet", "-m", "Delta scope amendment"
        )
        target_commit = self._git_at(delta_checkout, "rev-parse", "HEAD")

        request_payload = {
            "schema_version": 1,
            "amendment_id": amendment_id,
            "expected_execution_revision": 1,
            "expected_assignment_id": assignment_id,
            "expected_node_id": "continuity-coordinator",
            "expected_phase": "node",
            "expected_issued_task_sha256": "0" * 64,
            "expected_issued_message_sha256": "0" * 64,
            "idempotency_key": "delta-map-test",
            "source": {
                "repository_key": delta_key,
                "base_commit": base_commit,
                "target_commit": target_commit,
                "target_ref": "refs/heads/agent/delta-scope",
                "manifest": {
                    "path": self.MANIFEST_PATH,
                    "base_sha256": self._sha256(base_bytes),
                    "target_sha256": self._sha256(target_bytes),
                },
                "amendment": {
                    "path": self.AMENDMENT_PATH,
                    "sha256": self._sha256(amendment_bytes),
                },
            },
            "targets": {
                "active_assignment": True,
                "future_node_ids": ["continuity-coordinator", "formal-linkage"],
                "reviewer_agent_ids": ["reviewer-one", "reviewer-two"],
            },
        }
        map_env = "NGINX_QA_SCOPE_CONTROL_REPOSITORY_MAP"
        encoded_map = json.dumps({delta_key: str(delta_checkout.resolve())})

        with patch.object(main, "base_dir", app_release):
            with patch.dict(os.environ, {map_env: encoded_map}, clear=False):
                verified = main.verify_legacy_scope_source(
                    delta_address, request_payload
                )
            self.assertEqual(
                Path(verified["repo_path"]), delta_checkout.resolve()
            )
            self.assertEqual(
                verified["repository_resolution"], "operator_repository_map"
            )
            self.assertEqual(verified["source"]["target_commit"], target_commit)
            self.assertEqual(
                verified["amendment_document"]["preserved_assignment_id"],
                assignment_id,
            )

            os.environ.pop(map_env, None)
            with self.assertRaises(main.LegacyScopeControlError) as missing_map:
                main.verify_legacy_scope_source(delta_address, request_payload)
            self.assertEqual(missing_map.exception.code, "SCOPE_SOURCE_UNAVAILABLE")

            wrong_map = json.dumps({delta_key: str(app_release.resolve())})
            with patch.dict(os.environ, {map_env: wrong_map}, clear=False):
                with self.assertRaises(main.LegacyScopeControlError) as mismatched_map:
                    main.verify_legacy_scope_source(delta_address, request_payload)
            self.assertEqual(
                mismatched_map.exception.code, "SCOPE_REPOSITORY_MISMATCH"
            )

    async def test_bound_actor_drift_fails_closed_after_ack(self) -> None:
        await self._import_sprint()
        current = await self._claim("continuity-coordinator")
        assignment_id = self._assignment_id(current)
        self._create_amendment_commit(assignment_id)
        payload = self._amendment_payload(await self._preflight())
        status_code, response = await self._apply(payload)
        self.assertEqual(status_code, 200, response)
        await self._scope_and_ack(assignment_id, self.COORDINATOR_PHONE)

        scope_url = (
            f"/api/v1/projects/{self.PROJECT_PHONE}/assignments/"
            f"{assignment_id}/effective-scope"
        )
        original_agents = main.agents_path.read_bytes()
        try:
            agents_document = json.loads(original_agents)
            coordinator = next(
                agent
                for agent in agents_document["agents"]
                if agent["id"] == "continuity-coordinator"
            )
            coordinator["profile"] = "UNTRUSTED PROFILE DRIFT"
            main.agents_path.write_text(
                json.dumps(agents_document, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            status_code, response = await asgi_request(
                scope_url, headers=self._role_headers(self.COORDINATOR_PHONE)
            )
            self.assertIn(status_code, {409, 503}, response)
            self.assertTrue(
                response["detail"]["error"].startswith("SCOPE_RUNTIME_"), response
            )

            main.agents_path.write_bytes(original_agents)
            agents_document = json.loads(original_agents)
            coordinator = next(
                agent
                for agent in agents_document["agents"]
                if agent["id"] == "continuity-coordinator"
            )
            coordinator["phone"] = "999999"
            main.agents_path.write_text(
                json.dumps(agents_document, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            status_code, response = await asgi_request(
                scope_url, headers=self._role_headers(self.COORDINATOR_PHONE)
            )
            self.assertIn(status_code, {409, 503}, response)
            self.assertTrue(
                response["detail"]["error"].startswith("SCOPE_RUNTIME_"), response
            )
        finally:
            main.agents_path.write_bytes(original_agents)

    async def test_offline_prestart_validator_accepts_clean_transitions_and_rejects_corruption(
        self,
    ) -> None:
        clean_pre_state = json.loads(main.git_config_path.read_text(encoding="utf-8"))
        preflight_result = validate_scope_control_compatibility(
            clean_pre_state,
            required_project=self.PROJECT_PHONE,
            require_scope_control="absent",
        )
        self.assertTrue(preflight_result["compatible"])
        self.assertEqual(preflight_result["controlled_project_count"], 0)
        with self.assertRaises(CompatibilityError):
            validate_scope_control_compatibility(
                clean_pre_state,
                required_project=self.PROJECT_PHONE,
                require_scope_control="present",
            )

        await self._import_sprint()
        current = await self._claim("continuity-coordinator")
        assignment_id = self._assignment_id(current)
        self._create_amendment_commit(assignment_id)
        payload = self._amendment_payload(await self._preflight())
        status_code, response = await self._apply(payload)
        self.assertEqual(status_code, 200, response)
        await self._scope_and_ack(assignment_id, self.COORDINATOR_PHONE)

        compatible_post_state_bytes = main.git_config_path.read_bytes()
        compatible_post_state = json.loads(compatible_post_state_bytes)
        postflight_result = validate_scope_control_compatibility(
            compatible_post_state,
            required_project=self.PROJECT_PHONE,
            require_scope_control="present",
        )
        self.assertTrue(postflight_result["compatible"])
        self.assertEqual(postflight_result["controlled_project_count"], 1)
        self.assertEqual(postflight_result["assignment_binding_count"], 1)
        self.assertEqual(postflight_result["acknowledgement_count"], 1)

        effective_snapshot_tamper = deepcopy(compatible_post_state)
        tampered_binding = effective_snapshot_tamper[main.PROJECTS_KEY][
            self.PROJECT_CONTEXT
        ][main.PROJECT_AGENT_ASSIGNMENT_KEY]["scope_control"][
            "assignment_bindings"
        ][assignment_id]
        original_binding = compatible_post_state[main.PROJECTS_KEY][
            self.PROJECT_CONTEXT
        ][main.PROJECT_AGENT_ASSIGNMENT_KEY]["scope_control"][
            "assignment_bindings"
        ][assignment_id]
        tampered_binding["effective"]["profile"] = (
            "TAMPERED EFFECTIVE PROFILE OUTSIDE HASHED CORE"
        )
        self.assertEqual(
            tampered_binding["effective_core"], original_binding["effective_core"]
        )
        self.assertEqual(
            tampered_binding["effective_scope_sha256"],
            original_binding["effective_scope_sha256"],
        )
        self.assertEqual(
            tampered_binding["scope_context"], original_binding["scope_context"]
        )
        with self.assertRaises(CompatibilityError):
            validate_scope_control_compatibility(
                effective_snapshot_tamper,
                required_project=self.PROJECT_PHONE,
                require_scope_control="present",
            )

        try:
            main.git_config_path.write_text(
                json.dumps(
                    effective_snapshot_tamper,
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            status_code, response = await asgi_request(
                f"/api/v1/projects/{self.PROJECT_PHONE}/assignments/"
                f"{assignment_id}/effective-scope",
                headers=self._role_headers(self.COORDINATOR_PHONE),
            )
            self.assertEqual(status_code, 503, response)
            self.assertEqual(response["detail"]["error"], "SCOPE_BINDING_INVALID")
        finally:
            main.git_config_path.write_bytes(compatible_post_state_bytes)

        projected_message_tamper = deepcopy(compatible_post_state)
        tampered_binding = projected_message_tamper[main.PROJECTS_KEY][
            self.PROJECT_CONTEXT
        ][main.PROJECT_AGENT_ASSIGNMENT_KEY]["scope_control"][
            "assignment_bindings"
        ][assignment_id]
        tampered_binding["effective"]["active_task"]["message"] = (
            "TAMPERED PROJECTED MESSAGE OUTSIDE HASHED CORE"
        )
        self.assertEqual(
            tampered_binding["effective_core"], original_binding["effective_core"]
        )
        self.assertEqual(
            tampered_binding["effective_scope_sha256"],
            original_binding["effective_scope_sha256"],
        )
        self.assertEqual(
            tampered_binding["scope_context"], original_binding["scope_context"]
        )
        with self.assertRaises(CompatibilityError):
            validate_scope_control_compatibility(
                projected_message_tamper,
                required_project=self.PROJECT_PHONE,
                require_scope_control="present",
            )
        try:
            main.git_config_path.write_text(
                json.dumps(projected_message_tamper, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            status_code, response = await asgi_request(
                f"/api/v1/projects/{self.PROJECT_PHONE}/assignments/"
                f"{assignment_id}/effective-scope",
                headers=self._role_headers(self.COORDINATOR_PHONE),
            )
            self.assertEqual(status_code, 503, response)
            self.assertEqual(response["detail"]["error"], "SCOPE_BINDING_INVALID")
        finally:
            main.git_config_path.write_bytes(compatible_post_state_bytes)

        corrupted = deepcopy(compatible_post_state)
        project = corrupted[main.PROJECTS_KEY][self.PROJECT_CONTEXT]
        bindings = project[main.PROJECT_AGENT_ASSIGNMENT_KEY]["scope_control"][
            "assignment_bindings"
        ]
        bindings[assignment_id]["effective_core"]["profile"] = (
            "CORRUPTED EFFECTIVE CORE"
        )
        with self.assertRaises(CompatibilityError):
            validate_scope_control_compatibility(
                corrupted,
                required_project=self.PROJECT_PHONE,
                require_scope_control="present",
            )
        with self.assertRaises(CompatibilityError):
            validate_scope_control_compatibility(
                compatible_post_state,
                required_project=self.PROJECT_PHONE,
                require_scope_control="absent",
            )

    async def test_apply_auth_cas_source_hash_and_idempotency(self) -> None:
        await self._import_sprint()
        current = await self._claim("continuity-coordinator")
        assignment_id = self._assignment_id(current)
        self._create_amendment_commit(assignment_id)
        preflight_url = (
            f"/api/v1/projects/{self.PROJECT_PHONE}/sprints/{self.sprint_id}/"
            "scope-amendments/preflight"
        )
        before_preflight = deepcopy(self._stored_assignment())
        status_code, response = await asgi_request(preflight_url)
        self.assertEqual(status_code, 403, response)
        self.assertEqual(response["detail"]["error"], "SCOPE_CONTROL_FORBIDDEN")
        status_code, response = await asgi_request(
            preflight_url,
            headers=[(b"x-nginx-qa-scope-control-token", b"wrong-token")],
        )
        self.assertEqual(status_code, 403, response)
        self.assertEqual(response["detail"]["error"], "SCOPE_CONTROL_FORBIDDEN")
        self.assertEqual(
            self._canonical(self._stored_assignment()),
            self._canonical(before_preflight),
        )
        preflight = await self._preflight()
        payload = self._amendment_payload(preflight)
        before = deepcopy(self._stored_assignment())

        status_code, response = await self._apply(payload, headers=[])
        self.assertEqual(status_code, 403, response)
        self.assertEqual(response["detail"]["error"], "SCOPE_CONTROL_FORBIDDEN")
        status_code, response = await self._apply(
            payload,
            headers=[(b"x-nginx-qa-scope-control-token", b"wrong-token")],
        )
        self.assertEqual(status_code, 403, response)
        self.assertEqual(self._canonical(self._stored_assignment()), self._canonical(before))

        stale = deepcopy(payload)
        stale["expected_execution_revision"] = int(
            stale["expected_execution_revision"]
        ) + 1
        status_code, response = await self._apply(stale)
        self.assertEqual(status_code, 409, response)
        self.assertEqual(
            response["detail"]["error"], "SCOPE_EXECUTION_REVISION_CONFLICT"
        )
        self.assertEqual(self._canonical(self._stored_assignment()), self._canonical(before))

        bad_hash = deepcopy(payload)
        bad_hash["source"]["amendment"]["sha256"] = "0" * 64
        status_code, response = await self._apply(bad_hash)
        self.assertEqual(status_code, 409, response)
        self.assertEqual(response["detail"]["error"], "SCOPE_SOURCE_HASH_MISMATCH")
        self.assertEqual(self._canonical(self._stored_assignment()), self._canonical(before))

        reviewer_two_env = (
            f"NGINX_QA_SCOPE_CONTROL_ROLE_TOKEN_{self.REVIEWER_TWO_PHONE}"
        )
        reviewer_two_token = os.environ[reviewer_two_env]
        try:
            os.environ[reviewer_two_env] = self.role_tokens[self.REVIEWER_ONE_PHONE]
            status_code, response = await self._apply(payload)
            self.assertEqual(status_code, 503, response)
            self.assertEqual(
                response["detail"]["error"], "SCOPE_CREDENTIALS_NOT_DISTINCT"
            )
            self.assertEqual(
                self._canonical(self._stored_assignment()), self._canonical(before)
            )
        finally:
            os.environ[reviewer_two_env] = reviewer_two_token

        status_code, applied = await self._apply(payload)
        self.assertEqual(status_code, 200, applied)
        self.assertIsInstance(applied, dict)
        assert isinstance(applied, dict)
        self.assertFalse(applied["deduplicated"])
        self.assertEqual(applied["assignment_id"], assignment_id)
        self.assertTrue(applied["assignment_preserved"])
        self.assertFalse(applied["graph_advanced"])
        self.assertFalse(applied["queue_changed"])
        self.assertTrue(applied["audit_history_recorded"])
        self.assertTrue(applied["audit_history_created"])
        once = deepcopy(self._stored_assignment())
        history_once = main.history_path.read_bytes()

        status_code, replay = await self._apply(payload)
        self.assertEqual(status_code, 200, replay)
        self.assertIsInstance(replay, dict)
        assert isinstance(replay, dict)
        self.assertTrue(replay["deduplicated"])
        self.assertTrue(replay["audit_history_recorded"])
        self.assertFalse(replay["audit_history_created"])
        self.assertEqual(replay["effective_revision"], applied["effective_revision"])
        self.assertEqual(self._canonical(self._stored_assignment()), self._canonical(once))
        self.assertEqual(main.history_path.read_bytes(), history_once)

        conflicting = deepcopy(payload)
        conflicting["expected_execution_revision"] = int(
            conflicting["expected_execution_revision"]
        ) + 2
        status_code, response = await self._apply(conflicting)
        self.assertEqual(status_code, 409, response)
        self.assertEqual(response["detail"]["error"], "SCOPE_IDEMPOTENCY_CONFLICT")

        second_source = self._create_additional_amendment_commit(assignment_id)
        second = deepcopy(payload)
        second["amendment_id"] = second_source["amendment_id"]
        second["idempotency_key"] = "scope-test-apply-2"
        second["expected_execution_revision"] = applied["execution_revision"]
        second["source"]["target_commit"] = second_source["target_commit"]
        second["source"]["target_ref"] = (
            f"refs/heads/{second_source['branch']}"
        )
        second["source"]["manifest"]["target_sha256"] = second_source[
            "target_manifest_sha256"
        ]
        second["source"]["amendment"]["sha256"] = second_source[
            "amendment_sha256"
        ]
        status_code, response = await self._apply(second)
        self.assertEqual(status_code, 409, response)
        self.assertEqual(
            response["detail"]["error"], "SCOPE_ADDITIONAL_AMENDMENT_UNSUPPORTED"
        )
        self.assertEqual(self._canonical(self._stored_assignment()), self._canonical(once))
        self.assertEqual(main.history_path.read_bytes(), history_once)

        status_code, replay_after_rejection = await self._apply(payload)
        self.assertEqual(status_code, 200, replay_after_rejection)
        self.assertTrue(replay_after_rejection["deduplicated"])
        self.assertEqual(
            replay_after_rejection["effective_revision"],
            applied["effective_revision"],
        )

        # A crash can occur after the state transaction commits but before its
        # separate JSONL audit receipt is durable.  An exact idempotent retry
        # must repair that receipt once without changing runtime state.
        history_records = [
            json.loads(line)
            for line in main.history_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        without_scope_receipt = [
            record
            for record in history_records
            if not (
                record.get("event") == "legacy_scope_amendment_applied"
                and record.get("metadata", {}).get("amendment_id")
                == self.AMENDMENT_ID
            )
        ]
        self.assertEqual(len(history_records) - len(without_scope_receipt), 1)
        main.history_path.write_text(
            "".join(
                json.dumps(record, ensure_ascii=False) + "\n"
                for record in without_scope_receipt
            ),
            encoding="utf-8",
        )
        state_before_audit_repair = deepcopy(self._stored_assignment())
        status_code, repaired_replay = await self._apply(payload)
        self.assertEqual(status_code, 200, repaired_replay)
        self.assertTrue(repaired_replay["deduplicated"])
        self.assertTrue(repaired_replay["audit_history_created"])
        self.assertEqual(
            self._canonical(self._stored_assignment()),
            self._canonical(state_before_audit_repair),
        )
        repaired_history = main.history_path.read_bytes()
        status_code, stable_replay = await self._apply(payload)
        self.assertEqual(status_code, 200, stable_replay)
        self.assertFalse(stable_replay["audit_history_created"])
        self.assertEqual(main.history_path.read_bytes(), repaired_history)

    async def test_effective_scope_requires_role_token_exact_context_and_ack(self) -> None:
        await self._import_sprint()
        current = await self._claim("continuity-coordinator")
        assignment_id = self._assignment_id(current)
        self._create_amendment_commit(assignment_id)
        payload = self._amendment_payload(await self._preflight())
        status_code, response = await self._apply(payload)
        self.assertEqual(status_code, 200, response)

        scope_url = (
            f"/api/v1/projects/{self.PROJECT_PHONE}/assignments/"
            f"{assignment_id}/effective-scope"
        )
        status_code, response = await asgi_request(scope_url)
        self.assertEqual(status_code, 403, response)
        status_code, response = await asgi_request(
            scope_url, headers=[(b"x-nginx-qa-scope-token", b"wrong-token")]
        )
        self.assertEqual(status_code, 403, response)

        status_code, scope = await asgi_request(
            scope_url, headers=self._role_headers(self.COORDINATOR_PHONE)
        )
        self.assertEqual(status_code, 200, scope)
        self.assertIsInstance(scope, dict)
        assert isinstance(scope, dict)
        self.assertEqual(scope["precedence"]["authoritative"], "effective")
        self.assertIn("do not start R2.3/R3", scope["issued"]["active_task"]["message"])
        self.assertIn("authorized recovery proof", scope["effective"]["active_task"]["message"])
        self.assertFalse(scope["acknowledged"])

        submit_url = (
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/"
            f"{self.COORDINATOR_PHONE}/whoami"
        )
        base_submit = {
            "assignment_id": assignment_id,
            "status": "RESUME_FORMAL",
            "result": "Must not transition before a valid ACK.",
        }
        status_code, response = await asgi_request(
            submit_url,
            method="POST",
            payload=base_submit,
            headers=self._role_headers(self.COORDINATOR_PHONE),
        )
        self.assertEqual(status_code, 428, response)
        self.assertEqual(response["detail"]["error"], "SCOPE_ACK_REQUIRED")

        tampered_context = deepcopy(scope["scope_context"])
        tampered_context["effective_revision"] += 1
        ack_url = f"{scope_url}/ack"
        status_code, response = await asgi_request(
            ack_url,
            method="POST",
            payload={"schema_version": 1, "scope_context": tampered_context},
            headers=self._role_headers(self.COORDINATOR_PHONE),
        )
        self.assertEqual(status_code, 409, response)
        self.assertEqual(response["detail"]["error"], "SCOPE_CONTEXT_MISMATCH")

        status_code, ack = await asgi_request(
            ack_url,
            method="POST",
            payload={"schema_version": 1, "scope_context": scope["scope_context"]},
            headers=self._role_headers(self.COORDINATOR_PHONE),
        )
        self.assertEqual(status_code, 200, ack)
        self.assertFalse(ack["deduplicated"])
        status_code, replay = await asgi_request(
            ack_url,
            method="POST",
            payload={"schema_version": 1, "scope_context": scope["scope_context"]},
            headers=self._role_headers(self.COORDINATOR_PHONE),
        )
        self.assertEqual(status_code, 200, replay)
        self.assertTrue(replay["deduplicated"])

        bad_submit = {**base_submit, "scope_context": tampered_context}
        status_code, response = await asgi_request(
            submit_url,
            method="POST",
            payload=bad_submit,
            headers=self._role_headers(self.COORDINATOR_PHONE),
        )
        self.assertEqual(status_code, 409, response)
        self.assertEqual(response["detail"]["error"], "SCOPE_CONTEXT_MISMATCH")
        self.assertEqual(self._stored_assignment()["current_assignment_id"], assignment_id)

        for secret_value in (self.admin_token, *self.role_tokens.values()):
            secret_bytes = secret_value.encode("utf-8")
            for path in self.temp_path.rglob("*"):
                if path.is_file():
                    self.assertNotIn(secret_bytes, path.read_bytes(), str(path))

    async def test_end_to_end_amendment_survives_reviewed_round_trip(self) -> None:
        await self._import_sprint()
        historical_cards, current = await self._pre_amendment_round_trip()
        current_id = self._assignment_id(current)
        stored_before = deepcopy(self._stored_assignment())
        old_assignments = {
            item["assignment_id"]: deepcopy(item)
            for item in stored_before["assignments"][:-1]
        }
        self.assertEqual(len(old_assignments), 6)
        current_before = deepcopy(stored_before["assignments"][-1])
        active_task_before = deepcopy(stored_before["active_task"])
        workflow_before = deepcopy(stored_before["workflow"])
        queues_before = self._queue_snapshot()
        history_before = main.history_path.read_bytes()
        sprint_archive_before = main.sprint_history_path.read_bytes()

        self._create_amendment_commit(current_id)
        payload = self._amendment_payload(await self._preflight())
        status_code, applied = await self._apply(payload)
        self.assertEqual(status_code, 200, applied)
        stored_after_apply = self._stored_assignment()
        self.assertEqual(stored_after_apply["current_assignment_id"], current_id)
        self.assertEqual(
            next(
                item
                for item in stored_after_apply["assignments"]
                if item["assignment_id"] == current_id
            ),
            current_before,
        )
        self.assertEqual(stored_after_apply["active_task"], active_task_before)
        self.assertEqual(stored_after_apply["workflow"], workflow_before)
        self.assertEqual(self._queue_snapshot(), queues_before)
        self.assertEqual(main.sprint_history_path.read_bytes(), sprint_archive_before)
        for assignment_id, historical in old_assignments.items():
            actual = next(
                item
                for item in stored_after_apply["assignments"]
                if item["assignment_id"] == assignment_id
            )
            self.assertEqual(actual, historical)

        coordinator_scope = await self._scope_and_ack(
            current_id, self.COORDINATOR_PHONE
        )
        self.assertEqual(coordinator_scope["scope_context"]["occurrence"], 2)
        self.assertIn(
            "EFFECTIVE COORDINATOR PROFILE",
            coordinator_scope["effective"]["profile"],
        )
        self.assertIn(
            "ISSUED COORDINATOR PROFILE", coordinator_scope["issued"]["profile"]
        )
        handoff = await self._submit_effective(
            self.COORDINATOR_PHONE,
            current_id,
            "RESUME_FORMAL",
            coordinator_scope["scope_context"],
            result="Effective proof-only coordinator handoff.",
        )

        reviewer_one_preclaim_id = str(handoff["current_assignment_id"])
        preclaim_state = deepcopy(self._stored_assignment())
        preclaim_queues = self._queue_snapshot()
        preclaim_binding = preclaim_state["scope_control"]["assignment_bindings"][
            reviewer_one_preclaim_id
        ]
        preclaim_context = deepcopy(preclaim_binding["scope_context"])
        preclaim_scope_url = (
            f"/api/v1/projects/{self.PROJECT_PHONE}/assignments/"
            f"{reviewer_one_preclaim_id}/effective-scope"
        )
        governed_queue_items = [
            item
            for item in preclaim_queues["worker-all"]
            if item["metadata"].get("assignment_id") == reviewer_one_preclaim_id
        ]
        self.assertEqual(len(governed_queue_items), 1)
        governed_queue_item_id = governed_queue_items[0]["id"]

        status_code, response = await asgi_request(
            f"/worker/all/{self.PROJECT_PHONE}?to_phone={self.PROJECT_PHONE}"
        )
        self.assertEqual(status_code, 409, response)
        self.assertEqual(
            response["detail"]["error"],
            "SCOPE_CONTROLLED_IDENTITY_REQUIRES_WHOAMI",
        )
        self.assertEqual(
            self._canonical(self._stored_assignment()), self._canonical(preclaim_state)
        )
        self.assertEqual(self._queue_snapshot(), preclaim_queues)

        status_code, response = await asgi_request(
            f"/queues/worker-all/{governed_queue_item_id}", method="DELETE"
        )
        self.assertEqual(status_code, 409, response)
        self.assertEqual(
            response["detail"]["error"],
            "SCOPE_CONTROLLED_QUEUE_MUTATION_FORBIDDEN",
        )
        self.assertEqual(
            self._canonical(self._stored_assignment()), self._canonical(preclaim_state)
        )
        self.assertEqual(self._queue_snapshot(), preclaim_queues)

        status_code, response = await asgi_request(
            preclaim_scope_url, headers=self._role_headers(self.REVIEWER_ONE_PHONE)
        )
        self.assertEqual(status_code, 409, response)
        self.assertEqual(
            response["detail"]["error"], "SCOPE_ASSIGNMENT_NOT_DELIVERED"
        )
        status_code, response = await asgi_request(
            f"{preclaim_scope_url}/ack",
            method="POST",
            payload={"schema_version": 1, "scope_context": preclaim_context},
            headers=self._role_headers(self.REVIEWER_ONE_PHONE),
        )
        self.assertEqual(status_code, 409, response)
        self.assertEqual(
            response["detail"]["error"], "SCOPE_ASSIGNMENT_NOT_DELIVERED"
        )
        status_code, response = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/agents/"
            f"{self.REVIEWER_ONE_PHONE}/whoami",
            method="POST",
            payload={
                "assignment_id": reviewer_one_preclaim_id,
                "status": "APPROVE",
                "feedback": "This must not bypass ordinary queue delivery.",
                "scope_context": preclaim_context,
            },
            headers=self._role_headers(self.REVIEWER_ONE_PHONE),
        )
        self.assertEqual(status_code, 409, response)
        self.assertEqual(
            response["detail"]["error"], "SCOPE_ASSIGNMENT_NOT_DELIVERED"
        )
        self.assertEqual(
            self._canonical(self._stored_assignment()), self._canonical(preclaim_state)
        )
        self.assertEqual(self._queue_snapshot(), preclaim_queues)

        reviewer_one = await self._claim("reviewer-one")
        reviewer_one_id = self._assignment_id(reviewer_one)
        self.assertEqual(reviewer_one_id, reviewer_one_preclaim_id)
        reviewer_one_scope = await self._scope_and_ack(
            reviewer_one_id, self.REVIEWER_ONE_PHONE
        )
        self.assertEqual(
            reviewer_one_scope["scope_context"]["source_assignment_id"], current_id
        )
        self.assertIn(
            "EFFECTIVE REVIEWER ONE PROFILE",
            reviewer_one_scope["effective"]["profile"],
        )
        await self._submit_effective(
            self.REVIEWER_ONE_PHONE,
            reviewer_one_id,
            "APPROVE",
            reviewer_one_scope["scope_context"],
            feedback="Effective proof-only review one approves.",
        )

        reviewer_two = await self._claim("reviewer-two")
        reviewer_two_id = self._assignment_id(reviewer_two)
        reviewer_two_scope = await self._scope_and_ack(
            reviewer_two_id, self.REVIEWER_TWO_PHONE
        )
        self.assertEqual(
            reviewer_two_scope["scope_context"]["source_assignment_id"], current_id
        )
        self.assertIn(
            "EFFECTIVE REVIEWER TWO PROFILE",
            reviewer_two_scope["effective"]["profile"],
        )
        await self._submit_effective(
            self.REVIEWER_TWO_PHONE,
            reviewer_two_id,
            "APPROVE",
            reviewer_two_scope["scope_context"],
            feedback="Effective proof-only review two approves.",
        )

        formal = await self._claim("formal-linkage")
        formal_id = self._assignment_id(formal)
        formal_scope = await self._scope_and_ack(formal_id, self.FORMAL_PHONE)
        self.assertEqual(formal_scope["scope_context"]["occurrence"], 2)
        self.assertIn("EFFECTIVE FORMAL PROFILE", formal_scope["effective"]["profile"])
        self.assertIn("ISSUED FORMAL PROFILE", formal_scope["issued"]["profile"])
        await self._submit_effective(
            self.FORMAL_PHONE,
            formal_id,
            "NO_GO",
            formal_scope["scope_context"],
            result="Effective formal assessment remains NO_GO.",
        )

        reviewer_one_back = await self._claim("reviewer-one")
        reviewer_one_back_id = self._assignment_id(reviewer_one_back)
        reviewer_one_back_scope = await self._scope_and_ack(
            reviewer_one_back_id, self.REVIEWER_ONE_PHONE
        )
        self.assertEqual(
            reviewer_one_back_scope["scope_context"]["source_assignment_id"],
            formal_id,
        )
        self.assertEqual(
            reviewer_one_back_scope["scope_context"]["amendment_id"],
            self.AMENDMENT_ID,
        )
        self.assertIn(
            "EFFECTIVE REVIEWER ONE PROFILE",
            reviewer_one_back_scope["effective"]["profile"],
        )
        await self._submit_effective(
            self.REVIEWER_ONE_PHONE,
            reviewer_one_back_id,
            "APPROVE",
            reviewer_one_back_scope["scope_context"],
            feedback="Effective return review one approves.",
        )

        reviewer_two_back = await self._claim("reviewer-two")
        reviewer_two_back_id = self._assignment_id(reviewer_two_back)
        reviewer_two_back_scope = await self._scope_and_ack(
            reviewer_two_back_id, self.REVIEWER_TWO_PHONE
        )
        self.assertEqual(
            reviewer_two_back_scope["scope_context"]["source_assignment_id"],
            formal_id,
        )
        self.assertEqual(
            reviewer_two_back_scope["scope_context"]["amendment_id"],
            self.AMENDMENT_ID,
        )
        self.assertIn(
            "EFFECTIVE REVIEWER TWO PROFILE",
            reviewer_two_back_scope["effective"]["profile"],
        )
        await self._submit_effective(
            self.REVIEWER_TWO_PHONE,
            reviewer_two_back_id,
            "APPROVE",
            reviewer_two_back_scope["scope_context"],
            feedback="Effective return review two approves.",
        )

        returned = await self._claim("continuity-coordinator")
        returned_id = self._assignment_id(returned)
        self.assertNotEqual(returned_id, current_id)
        returned_scope = await self._scope_and_ack(
            returned_id, self.COORDINATOR_PHONE
        )
        self.assertEqual(returned_scope["scope_context"]["occurrence"], 3)
        self.assertIn(
            "EFFECTIVE COORDINATOR TASK",
            returned_scope["effective"]["active_task"]["message"],
        )
        self.assertIn(
            "ISSUED COORDINATOR TASK",
            returned_scope["issued"]["tasks"][0]["message"],
        )

        final = self._stored_assignment()
        for assignment_id, historical in old_assignments.items():
            actual = next(
                item
                for item in final["assignments"]
                if item["assignment_id"] == assignment_id
            )
            self.assertEqual(actual, historical)
        current_after = next(
            item for item in final["assignments"] if item["assignment_id"] == current_id
        )
        for key in (
            "assignment_id",
            "agent_id",
            "agent_phone",
            "node_id",
            "task_id",
            "task_message",
            "assigned_at",
        ):
            self.assertEqual(current_after.get(key), current_before.get(key), key)
        self.assertEqual(final["workflow"], workflow_before)
        self.assertEqual(final["workflow"]["required_approvals"], 2)
        self.assertEqual(len(final["assignments"]), 13)
        continuation_ids = [
            reviewer_one_id,
            reviewer_two_id,
            formal_id,
            reviewer_one_back_id,
            reviewer_two_back_id,
            returned_id,
        ]
        self.assertEqual(len(set(continuation_ids)), len(continuation_ids))
        self.assertTrue(set(continuation_ids).isdisjoint({*old_assignments, current_id}))
        self.assertEqual(
            [item["assignment_id"] for item in final["assignments"][7:]],
            continuation_ids,
        )
        self.assertEqual(len(final["scope_control"]["assignment_bindings"]), 7)
        self.assertEqual(len(final["scope_control"]["acknowledgements"]), 7)
        self.assertTrue(main.history_path.read_bytes().startswith(history_before))
        self.assertEqual(main.sprint_history_path.read_bytes(), sprint_archive_before)

        new_source_assignments = [
            item
            for item in final["assignments"]
            if item["assignment_id"] in {current_id, formal_id}
        ]
        self.assertEqual(len(new_source_assignments), 2)
        for source in new_source_assignments:
            self.assertEqual(len(source["reviews"]), 2)
            self.assertEqual(
                [review["decision"] for review in source["reviews"]],
                ["APPROVE", "APPROVE"],
            )
            self.assertTrue(
                all(review["amendment_id"] == self.AMENDMENT_ID for review in source["reviews"])
            )

        state_status, state = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/state.json"
        )
        self.assertEqual(state_status, 200, state)
        self.assertEqual(
            state["execution"]["issued_active_task"]["message"],
            final["active_task"]["message"],
        )
        self.assertIn(
            "EFFECTIVE COORDINATOR TASK",
            state["execution"]["active_task"]["message"],
        )


if __name__ == "__main__":
    unittest.main()
