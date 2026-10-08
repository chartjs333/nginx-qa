import asyncio
import base64
import hashlib
import json
import os
import tempfile
import unittest
import urllib.parse
from collections import deque
from pathlib import Path
from unittest.mock import AsyncMock, patch

import main
from nginx_qa import inbound_proposals
from nginx_qa.managed_import import ManagedImportError, ManagedStartResult


async def asgi_request(
    target: str,
    *,
    method: str = "GET",
    payload: object | None = None,
    raw_body: bytes | None = None,
    headers: list[tuple[bytes, bytes]] | None = None,
    host: str = "testserver:8025",
    client_host: str = "127.0.0.1",
) -> tuple[int, dict[str, str], object]:
    parsed = urllib.parse.urlsplit(target)
    if raw_body is None:
        body = json.dumps(payload).encode("utf-8") if payload is not None else b""
    else:
        body = raw_body
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
    if payload is not None or raw_body is not None:
        request_headers.append((b"content-type", b"application/json; charset=utf-8"))
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
    response_headers = {
        key.decode("latin-1").lower(): value.decode("latin-1")
        for key, value in response_start.get("headers", [])
    }
    decoded: object = json.loads(response_body) if response_body else None
    return int(response_start["status"]), response_headers, decoded


class InboundProposalApiTests(unittest.IsolatedAsyncioTestCase):
    PROJECT_PHONE = "9008"
    PROJECT_CONTEXT = "github.com/example/inbound-proposals"
    STATUS_RESOURCE_KEYS = {
        "schema_version",
        "proposal_id",
        "pending_sprint_id",
        "project_id",
        "revision",
        "proposal_status",
        "activation_state",
        "source_metadata",
        "summary",
        "validation",
        "regenerate_requested",
        "created_at",
        "updated_at",
        "started_sprint_id",
    }
    TOKEN = base64.urlsafe_b64encode(b"A" * 32).decode("ascii").rstrip("=")
    OTHER_TOKEN = base64.urlsafe_b64encode(b"B" * 32).decode("ascii").rstrip("=")
    UNRELATED_TOKEN = (
        base64.urlsafe_b64encode(b"C" * 32).decode("ascii").rstrip("=")
    )

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.temp_path = Path(self.temp_dir.name)
        self.original_values = {
            "git_config_path": main.git_config_path,
            "pending_sprints_path": main.pending_sprints_path,
            "git_config_lock": main.git_config_lock,
            "pending_sprints_lock": main.pending_sprints_lock,
            "queues": main.queues,
            "locks": main.locks,
        }
        self.original_registry = os.environ.get(
            main.INBOUND_PRODUCER_REGISTRY_ENV
        )
        main.git_config_path = self.temp_path / "port_git_map.json"
        main.pending_sprints_path = self.temp_path / "pending_project_sprints.json"
        main.git_config_lock = asyncio.Lock()
        main.pending_sprints_lock = asyncio.Lock()
        main.queues = {name: deque() for name in main.QUEUE_DEFINITIONS}
        main.locks = {name: asyncio.Lock() for name in main.QUEUE_DEFINITIONS}
        self.registry_acl_patcher = patch(
            "nginx_qa.inbound_proposals._registry_acl_is_protected",
            return_value=True,
        )
        self.registry_acl_patcher.start()
        self.write_project()
        self.write_registry()

    def tearDown(self) -> None:
        self.registry_acl_patcher.stop()
        for name, value in self.original_values.items():
            setattr(main, name, value)
        if self.original_registry is None:
            os.environ.pop(main.INBOUND_PRODUCER_REGISTRY_ENV, None)
        else:
            os.environ[main.INBOUND_PRODUCER_REGISTRY_ENV] = self.original_registry
        self.temp_dir.cleanup()

    def write_project(self) -> None:
        project = {
            "project_name": "Inbound Proposal Project",
            "git_address": "https://github.com/example/inbound-proposals.git",
            "git_context_key": self.PROJECT_CONTEXT,
            "project_phone": self.PROJECT_PHONE,
            "groups": [],
            "group_relationships": [],
            "customer_reporting": {},
        }
        main.git_config_path.write_text(
            json.dumps(
                {
                    main.PROJECTS_KEY: {self.PROJECT_CONTEXT: project},
                    main.PHONE_GIT_CONTEXTS_KEY: {
                        self.PROJECT_PHONE: {**project, "phone": self.PROJECT_PHONE}
                    },
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

    def write_registry(self) -> None:
        registry_path = self.temp_path / "producer-registry.json"
        self.registry_path = registry_path
        registry_path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "producers": [
                        {
                            "producer_id": "inbound-hub",
                            "token_sha256": hashlib.sha256(
                                self.TOKEN.encode("ascii")
                            ).hexdigest(),
                            "project_ids": [self.PROJECT_PHONE],
                            "actions": [
                                "create",
                                "read",
                                "preview",
                                "comment",
                                "reject",
                            ],
                        },
                        {
                            "producer_id": "other-hub",
                            "token_sha256": hashlib.sha256(
                                self.OTHER_TOKEN.encode("ascii")
                            ).hexdigest(),
                            "project_ids": [self.PROJECT_PHONE],
                            "actions": [
                                "create",
                                "read",
                                "preview",
                                "comment",
                                "reject",
                            ],
                        },
                    ],
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        os.environ[main.INBOUND_PRODUCER_REGISTRY_ENV] = str(registry_path)

    def auth_headers(
        self,
        *,
        token: str | None = None,
        correlation_id: str = "corr-test-1",
    ) -> list[tuple[bytes, bytes]]:
        return [
            (b"authorization", f"Bearer {token or self.TOKEN}".encode("ascii")),
            (b"x-correlation-id", correlation_id.encode("ascii")),
        ]

    def proposal_payload(
        self,
        *,
        proposal_id: str = "proposal-1",
        idempotency_key: str = "create:proposal-1:v1",
        summary: str = "Implement the inbound proposal API.",
        source_type: str = "email",
    ) -> dict[str, object]:
        return {
            "schema_version": 1,
            "proposal_id": proposal_id,
            "idempotency_key": idempotency_key,
            "source": {
                "source_type": source_type,
                "conversation_id": "conversation-1",
                "message_id": "message-1",
                "sender_label": "Release coordinator",
                "title": "Inbound proposal",
                "observed_at": "2026-10-08T08:30:00Z",
            },
            "summary": summary,
            "candidate": {
                "kind": "legacy_json",
                "payload": {
                    "sprint_type": "legacy_v1",
                    "actors": [],
                },
            },
        }

    def managed_proposal_payload(
        self,
        *,
        proposal_id: str = "proposal-managed-1",
        create_idempotency_key: str = "create:proposal-managed-1:v1",
        activation_idempotency_key: str = "activate:proposal-managed-1:v1",
    ) -> dict[str, object]:
        payload = self.proposal_payload(
            proposal_id=proposal_id,
            idempotency_key=create_idempotency_key,
            summary="Start the pinned managed Git sprint after Preview.",
            source_type="api",
        )
        payload["candidate"] = {
            "kind": "managed_git",
            "request": {
                "repository_id": "main",
                "ref": "refs/heads/inbound-proposal",
                "manifest_path": "orchestration/inbound-proposal.json",
                "idempotency_key": activation_idempotency_key,
            },
        }
        return payload

    async def preview(
        self,
        pending_id: str,
        *,
        expected_revision: int = 0,
        idempotency_key: str = "preview:proposal-managed-1:v1",
        correlation_id: str = "corr-preview-1",
    ) -> tuple[int, dict[str, str], object]:
        return await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/"
            f"{pending_id}/preview",
            method="POST",
            payload={
                "schema_version": 1,
                "action": "preview",
                "expected_revision": expected_revision,
                "idempotency_key": idempotency_key,
            },
            headers=self.auth_headers(correlation_id=correlation_id),
        )

    async def create(
        self,
        payload: dict[str, object] | None = None,
        *,
        correlation_id: str = "corr-create-1",
        token: str | None = None,
    ) -> tuple[int, dict[str, str], object]:
        return await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints",
            method="POST",
            payload=payload or self.proposal_payload(),
            headers=self.auth_headers(
                token=token,
                correlation_id=correlation_id,
            ),
        )

    async def read_status(
        self,
        pending_id: str,
        *,
        correlation_id: str,
        token: str | None = None,
        headers: list[tuple[bytes, bytes]] | None = None,
    ) -> tuple[int, dict[str, str], object]:
        return await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/"
            f"{pending_id}/status",
            headers=(
                headers
                if headers is not None
                else self.auth_headers(
                    token=token,
                    correlation_id=correlation_id,
                )
            ),
        )

    def assert_status_response(
        self,
        response_headers: dict[str, str],
        response: object,
        *,
        correlation_id: str,
        proposal_status: str,
        activation_state: str,
        pending_id: str,
    ) -> dict[str, object]:
        self.assertIsInstance(response, dict)
        assert isinstance(response, dict)
        self.assertEqual(set(response), {"schema_version", "correlation_id", "status"})
        self.assertEqual(response["schema_version"], 1)
        self.assertEqual(response["correlation_id"], correlation_id)
        self.assertEqual(response_headers["x-correlation-id"], correlation_id)
        resource = response["status"]
        self.assertIsInstance(resource, dict)
        assert isinstance(resource, dict)
        self.assertEqual(set(resource), self.STATUS_RESOURCE_KEYS)
        self.assertEqual(resource["pending_sprint_id"], pending_id)
        self.assertEqual(resource["project_id"], self.PROJECT_PHONE)
        self.assertEqual(resource["proposal_status"], proposal_status)
        self.assertEqual(resource["activation_state"], activation_state)
        for forbidden in (
            "candidate",
            "comments",
            "submitted_by",
            "import_payload",
            "activation_attempt_id",
            "telegram_update_id",
        ):
            self.assertNotIn(forbidden, resource)
        self.assertEqual(
            main.managed_schema_errors(
                response,
                "inbound-pending-proposal-status-response-v1.schema.json",
                issue_code="TEST_STATUS_SCHEMA_INVALID",
            ),
            [],
        )
        return resource

    def test_status_openapi_is_bearer_only_local_and_normalized(self) -> None:
        specification = main.app.openapi()
        path = (
            "/api/v1/projects/{project_id}/pending-sprints/"
            "{pending_sprint_id}/status"
        )
        self.assertIn(path, specification["paths"])
        self.assertNotIn(
            "/api/v1/projects/{project_id}/pending-sprints/{sprint_id}/status",
            specification["paths"],
        )
        operation = specification["paths"][path]["get"]
        path_parameters = {
            parameter["name"]: parameter
            for parameter in operation.get("parameters", [])
            if parameter.get("in") == "path"
        }
        self.assertEqual(set(path_parameters), {"project_id", "pending_sprint_id"})
        self.assertTrue(all(item.get("required") is True for item in path_parameters.values()))
        self.assertNotIn("requestBody", operation)

        security_schemes = specification["components"]["securitySchemes"]
        bearer_schemes = {
            name
            for name, definition in security_schemes.items()
            if str(definition.get("type") or "").casefold() == "http"
            and str(definition.get("scheme") or "").casefold() == "bearer"
        }
        self.assertTrue(bearer_schemes)
        operation_security = operation.get("security")
        self.assertIsInstance(operation_security, list)
        self.assertTrue(operation_security)
        self.assertNotIn({}, operation_security)
        self.assertTrue(
            any(bearer_schemes.intersection(requirement) for requirement in operation_security)
        )

        expected_responses = {
            "200",
            "400",
            "401",
            "403",
            "404",
            "500",
            "503",
            "default",
        }
        responses = operation["responses"]
        self.assertEqual(set(responses), expected_responses)
        self.assertNotIn("422", responses)
        self.assertEqual(
            responses["default"]["description"],
            "Normalized inbound proposal error",
        )

        def resolve_local_reference(reference: str) -> object:
            self.assertTrue(reference.startswith("#/"), reference)
            current: object = specification
            for raw_part in reference[2:].split("/"):
                part = raw_part.replace("~1", "/").replace("~0", "~")
                self.assertIsInstance(current, dict)
                assert isinstance(current, dict)
                self.assertIn(part, current)
                current = current[part]
            return current

        visited_references: set[str] = set()

        def assert_local_schema(node: object) -> None:
            if isinstance(node, list):
                for item in node:
                    assert_local_schema(item)
                return
            if not isinstance(node, dict):
                return
            reference = node.get("$ref")
            if isinstance(reference, str):
                self.assertFalse(reference.startswith(("http://", "https://")))
                target = resolve_local_reference(reference)
                if reference not in visited_references:
                    visited_references.add(reference)
                    assert_local_schema(target)
            for key, value in node.items():
                if key != "$ref":
                    assert_local_schema(value)

        for response_code in sorted(expected_responses):
            response = responses[response_code]
            correlation_header = response["headers"]["X-Correlation-ID"]
            self.assertEqual(correlation_header["schema"]["type"], "string")
            response_schema = response["content"]["application/json"]["schema"]
            self.assertIsInstance(response_schema, dict)
            assert_local_schema(response_schema)

        authenticate_header = responses["401"]["headers"]["WWW-Authenticate"]
        self.assertEqual(authenticate_header["schema"]["type"], "string")
        self.assertIn("Bearer", json.dumps(authenticate_header))

    async def test_create_replay_and_read_are_durable_without_activation(self) -> None:
        with patch.object(
            main,
            "update_pending_sprint_activation_file",
            side_effect=AssertionError("create must not claim activation"),
        ), patch.object(
            main,
            "import_project_actors_data",
            side_effect=AssertionError("create must not invoke an executor"),
        ):
            status_code, headers, body = await self.create()
        self.assertEqual(status_code, 201, body)
        self.assertEqual(headers["x-correlation-id"], "corr-create-1")
        self.assertIsInstance(body, dict)
        assert isinstance(body, dict)
        proposal = body["proposal"]
        self.assertFalse(body["deduplicated"])
        self.assertEqual(proposal["proposal_status"], "created")
        self.assertEqual(proposal["activation_state"], "not_started")
        self.assertEqual(proposal["source_metadata"]["source_type"], "email")
        pending_id = proposal["pending_sprint_id"]

        stored = main.read_pending_sprints_file()
        records = stored["projects"][self.PROJECT_CONTEXT]["sprints"]
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["activation_attempts"], 0)
        self.assertEqual(records[0]["source"], "inbound-proposal")
        self.assertNotIn("source_metadata", records[0]["import_payload"])
        bytes_before_replay = main.pending_sprints_path.read_bytes()

        replay_status, replay_headers, replay = await self.create(
            correlation_id="corr-create-replay"
        )
        self.assertEqual(replay_status, 200, replay)
        self.assertEqual(replay_headers["x-correlation-id"], "corr-create-replay")
        self.assertTrue(replay["deduplicated"])
        self.assertEqual(replay["proposal"]["pending_sprint_id"], pending_id)
        self.assertEqual(main.pending_sprints_path.read_bytes(), bytes_before_replay)

        detail_status, detail_headers, detail = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/{pending_id}",
            headers=self.auth_headers(correlation_id="corr-read-1"),
        )
        self.assertEqual(detail_status, 200, detail)
        self.assertEqual(detail_headers["x-correlation-id"], "corr-read-1")
        self.assertEqual(
            detail["pending_sprint"]["proposal"]["source_metadata"]["message_id"],
            "message-1",
        )
        self.assertEqual(
            detail["pending_sprint"]["proposal"]["proposal_status"],
            "created",
        )

    async def test_bearer_status_read_is_transport_neutral_for_every_lifecycle(
        self,
    ) -> None:
        observed_statuses: list[str] = []

        async def assert_read(
            pending_id: str,
            *,
            correlation_id: str,
            proposal_status: str,
            activation_state: str,
            validation_status: str,
            started_sprint_id: str | None = None,
        ) -> dict[str, object]:
            bytes_before = main.pending_sprints_path.read_bytes()
            status_code, response_headers, response = await self.read_status(
                pending_id,
                correlation_id=correlation_id,
            )
            self.assertEqual(status_code, 200, response)
            self.assertEqual(main.pending_sprints_path.read_bytes(), bytes_before)
            resource = self.assert_status_response(
                response_headers,
                response,
                correlation_id=correlation_id,
                proposal_status=proposal_status,
                activation_state=activation_state,
                pending_id=pending_id,
            )
            self.assertEqual(resource["validation"]["status"], validation_status)
            self.assertEqual(resource["started_sprint_id"], started_sprint_id)
            observed_statuses.append(str(resource["proposal_status"]))
            return resource

        with patch.object(
            main,
            "telegram_api_json",
            side_effect=AssertionError("status read used Telegram transport"),
        ) as telegram_api, patch.object(
            main,
            "send_telegram_import_reply",
            side_effect=AssertionError("status read sent a Telegram reply"),
        ) as telegram_reply, patch.object(
            main,
            "send_telegram_import_error_reply",
            side_effect=AssertionError("status read sent a Telegram error"),
        ) as telegram_error:
            started_payload = self.managed_proposal_payload(
                proposal_id="proposal-status-started",
                create_idempotency_key="create:proposal-status-started:v1",
                activation_idempotency_key="activate:proposal-status-started:v1",
            )
            create_status, _, created = await self.create(
                started_payload,
                correlation_id="corr-status-created-create",
            )
            self.assertEqual(create_status, 201, created)
            started_pending_id = created["proposal"]["pending_sprint_id"]
            await assert_read(
                started_pending_id,
                correlation_id="corr-status-created-read",
                proposal_status="created",
                activation_state="not_started",
                validation_status="not_run",
            )

            preview_status, _, preview = await self.preview(
                started_pending_id,
                idempotency_key="preview:proposal-status-started:v1",
                correlation_id="corr-status-ready-preview",
            )
            self.assertEqual(preview_status, 200, preview)
            await assert_read(
                started_pending_id,
                correlation_id="corr-status-ready-read",
                proposal_status="ready",
                activation_state="not_started",
                validation_status="valid",
            )

            activated_sprint_id = "msv1-" + ("d" * 64)
            managed_result = ManagedStartResult(
                {
                    "sprint_id": activated_sprint_id,
                    "status": "active",
                    "phase": "ACTIVATE",
                    "deduplicated": False,
                },
                201,
            )
            with patch.object(
                main,
                "execute_managed_project_sprint_start",
                new=AsyncMock(return_value=managed_result),
            ):
                start_status, _, started = await asgi_request(
                    f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/"
                    f"{started_pending_id}/start",
                    method="POST",
                )
            self.assertEqual(start_status, 200, started)
            await assert_read(
                started_pending_id,
                correlation_id="corr-status-started-read",
                proposal_status="started",
                activation_state="started",
                validation_status="valid",
                started_sprint_id=activated_sprint_id,
            )

            rejected_payload = self.proposal_payload(
                proposal_id="proposal-status-rejected",
                idempotency_key="create:proposal-status-rejected:v1",
            )
            rejected_create_status, _, rejected_create = await self.create(
                rejected_payload,
                correlation_id="corr-status-rejected-create",
            )
            self.assertEqual(rejected_create_status, 201, rejected_create)
            rejected_pending_id = rejected_create["proposal"]["pending_sprint_id"]
            reject_status, _, rejected = await asgi_request(
                f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/"
                f"{rejected_pending_id}/reject",
                method="POST",
                payload={
                    "schema_version": 1,
                    "action": "reject",
                    "expected_revision": 0,
                    "idempotency_key": "reject:proposal-status-rejected:v1",
                    "comment": "Reject this proposal without activation.",
                },
                headers=self.auth_headers(
                    correlation_id="corr-status-rejected-action"
                ),
            )
            self.assertEqual(reject_status, 200, rejected)
            await assert_read(
                rejected_pending_id,
                correlation_id="corr-status-rejected-read",
                proposal_status="rejected",
                activation_state="not_started",
                validation_status="not_run",
            )

            failed_payload = self.managed_proposal_payload(
                proposal_id="proposal-status-failed",
                create_idempotency_key="create:proposal-status-failed:v1",
                activation_idempotency_key="activate:proposal-status-failed:v1",
            )
            failed_create_status, _, failed_create = await self.create(
                failed_payload,
                correlation_id="corr-status-failed-create",
            )
            self.assertEqual(failed_create_status, 201, failed_create)
            failed_pending_id = failed_create["proposal"]["pending_sprint_id"]
            failed_preview_status, _, failed_preview = await self.preview(
                failed_pending_id,
                idempotency_key="preview:proposal-status-failed:v1",
                correlation_id="corr-status-failed-preview",
            )
            self.assertEqual(failed_preview_status, 200, failed_preview)
            activation_error = ManagedImportError(
                "SPRINT_PREFLIGHT_FAILED",
                409,
                "managed-status-failure",
                phase="VALIDATE",
                issues=[
                    {
                        "code": "MANIFEST_SCHEMA_INVALID",
                        "path": "manifest",
                        "message": "Managed manifest validation failed",
                    }
                ],
            )
            with patch.object(
                main,
                "execute_managed_project_sprint_start",
                new=AsyncMock(side_effect=activation_error),
            ):
                failed_start_status, _, failed_start = await asgi_request(
                    f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/"
                    f"{failed_pending_id}/start",
                    method="POST",
                )
            self.assertEqual(failed_start_status, 409, failed_start)
            await assert_read(
                failed_pending_id,
                correlation_id="corr-status-failed-read",
                proposal_status="failed",
                activation_state="failed",
                validation_status="valid",
            )

        self.assertEqual(
            observed_statuses,
            ["created", "ready", "started", "rejected", "failed"],
        )
        telegram_api.assert_not_called()
        telegram_reply.assert_not_called()
        telegram_error.assert_not_called()

    async def test_status_read_requires_bearer_ownership_and_a_proposal(
        self,
    ) -> None:
        create_status, _, created = await self.create(
            self.proposal_payload(
                proposal_id="proposal-status-access",
                idempotency_key="create:proposal-status-access:v1",
            ),
            correlation_id="corr-status-access-create",
        )
        self.assertEqual(create_status, 201, created)
        pending_id = created["proposal"]["pending_sprint_id"]
        legacy = main.stage_project_sprint_file(
            context_key=self.PROJECT_CONTEXT,
            project_phone=self.PROJECT_PHONE,
            project_name="Inbound Proposal Project",
            git_address="https://github.com/example/inbound-proposals.git",
            repository_key="github.com/example/inbound-proposals",
            payload={"sprint": {"title": "Telegram pending without proposal"}},
            source="telegram",
            source_filename="telegram.json",
            assignment_mode="parallel",
            agent_count=0,
            task_count=0,
            telegram_update_id="telegram-status-null-1",
        )
        local_detail_status, _, local_detail = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/{legacy['id']}"
        )
        self.assertEqual(local_detail_status, 200, local_detail)
        self.assertEqual(local_detail["pending_sprint"]["source"], "telegram")
        self.assertIsNone(local_detail["pending_sprint"]["proposal"])
        storage_before = main.pending_sprints_path.read_bytes()

        with patch.object(
            main,
            "telegram_api_json",
            side_effect=AssertionError("status denial used Telegram transport"),
        ) as telegram_api, patch.object(
            main,
            "send_telegram_import_reply",
            side_effect=AssertionError("status denial sent a Telegram reply"),
        ) as telegram_reply, patch.object(
            main,
            "send_telegram_import_error_reply",
            side_effect=AssertionError("status denial sent a Telegram error"),
        ) as telegram_error:
            missing_status, missing_headers, missing = await self.read_status(
                pending_id,
                correlation_id="unused",
                headers=[(b"x-correlation-id", b"corr-status-missing-bearer")],
            )
            self.assertEqual(missing_status, 401, missing)
            self.assertEqual(missing["detail"]["error"], "PROPOSAL_AUTH_REQUIRED")
            self.assertEqual(
                missing["detail"]["correlation_id"],
                "corr-status-missing-bearer",
            )
            self.assertEqual(
                missing_headers["x-correlation-id"],
                "corr-status-missing-bearer",
            )
            self.assertEqual(missing_headers["www-authenticate"], "Bearer")

            foreign_status, _, foreign = await self.read_status(
                pending_id,
                correlation_id="corr-status-foreign-owner",
                token=self.OTHER_TOKEN,
            )
            self.assertEqual(foreign_status, 404, foreign)
            self.assertEqual(foreign["detail"]["error"], "PROPOSAL_NOT_FOUND")

            legacy_status, _, legacy_response = await self.read_status(
                str(legacy["id"]),
                correlation_id="corr-status-legacy-null",
            )
            self.assertEqual(legacy_status, 404, legacy_response)
            self.assertEqual(
                legacy_response["detail"]["error"],
                "PROPOSAL_NOT_FOUND",
            )

        self.assertEqual(main.pending_sprints_path.read_bytes(), storage_before)
        local_again_status, _, local_again = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/{legacy['id']}"
        )
        self.assertEqual(local_again_status, 200, local_again)
        self.assertEqual(local_again["pending_sprint"]["source"], "telegram")
        self.assertIsNone(local_again["pending_sprint"]["proposal"])
        telegram_api.assert_not_called()
        telegram_reply.assert_not_called()
        telegram_error.assert_not_called()

    async def test_status_read_forbidden_and_invalid_correlation_are_normalized(
        self,
    ) -> None:
        create_status, _, created = await self.create(
            self.proposal_payload(
                proposal_id="proposal-status-normalized-errors",
                idempotency_key="create:proposal-status-normalized-errors:v1",
            ),
            correlation_id="corr-status-normalized-create",
        )
        self.assertEqual(create_status, 201, created)
        pending_id = created["proposal"]["pending_sprint_id"]

        no_read_token = base64.urlsafe_b64encode(b"D" * 32).decode("ascii").rstrip("=")
        registry = json.loads(self.registry_path.read_text(encoding="utf-8"))
        registry["producers"].append(
            {
                "producer_id": "status-without-read",
                "token_sha256": hashlib.sha256(
                    no_read_token.encode("ascii")
                ).hexdigest(),
                "project_ids": [self.PROJECT_PHONE],
                "actions": ["create"],
            }
        )
        self.registry_path.write_text(
            json.dumps(registry, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        bytes_before = main.pending_sprints_path.read_bytes()

        forbidden_status, forbidden_headers, forbidden = await self.read_status(
            pending_id,
            correlation_id="corr-status-forbidden",
            token=no_read_token,
        )
        self.assertEqual(forbidden_status, 403, forbidden)
        self.assertEqual(forbidden["detail"]["error"], "PROPOSAL_FORBIDDEN")
        self.assertEqual(
            forbidden["detail"]["correlation_id"],
            "corr-status-forbidden",
        )
        self.assertEqual(
            forbidden_headers["x-correlation-id"],
            "corr-status-forbidden",
        )

        invalid_correlation = "invalid correlation must not echo"
        invalid_status, invalid_headers, invalid = await self.read_status(
            pending_id,
            correlation_id="unused",
            headers=[
                (b"authorization", f"Bearer {self.TOKEN}".encode("ascii")),
                (b"x-correlation-id", invalid_correlation.encode("ascii")),
            ],
        )
        self.assertEqual(invalid_status, 400, invalid)
        self.assertEqual(invalid["detail"]["error"], "PROPOSAL_REQUEST_INVALID")
        self.assertEqual(invalid["detail"]["field"], "X-Correlation-ID")
        generated_correlation = invalid["detail"]["correlation_id"]
        self.assertTrue(generated_correlation)
        self.assertNotEqual(generated_correlation, invalid_correlation)
        self.assertEqual(
            invalid_headers["x-correlation-id"],
            generated_correlation,
        )
        self.assertNotIn(invalid_correlation, json.dumps(invalid))
        self.assertNotIn(invalid_correlation, json.dumps(invalid_headers))
        self.assertEqual(main.pending_sprints_path.read_bytes(), bytes_before)

    async def test_status_projects_pre_upgrade_legacy_activation_without_mutation(
        self,
    ) -> None:
        async def create_ready(suffix: str) -> str:
            create_status, _, created = await self.create(
                self.proposal_payload(
                    proposal_id=f"proposal-stale-legacy-{suffix}",
                    idempotency_key=f"create:proposal-stale-legacy-{suffix}:v1",
                ),
                correlation_id=f"corr-stale-legacy-{suffix}-create",
            )
            self.assertEqual(create_status, 201, created)
            pending_id = created["proposal"]["pending_sprint_id"]
            preview_status, _, preview = await self.preview(
                pending_id,
                idempotency_key=f"preview:proposal-stale-legacy-{suffix}:v1",
                correlation_id=f"corr-stale-legacy-{suffix}-preview",
            )
            self.assertEqual(preview_status, 200, preview)
            self.assertEqual(preview["proposal"]["proposal_status"], "ready")
            self.assertEqual(preview["proposal"]["activation_state"], "not_started")
            self.assertEqual(preview["proposal"]["revision"], 1)
            return str(pending_id)

        activated_id = await create_ready("activated")
        activating_id = await create_ready("activating")
        failed_id = await create_ready("failed")
        activated_at = "2026-10-08T10:01:00+00:00"
        activating_at = "2026-10-08T10:02:00+00:00"
        failed_at = "2026-10-08T10:03:00+00:00"
        activated_sprint_id = "legacy-pre-upgrade-started"

        storage = main.read_pending_sprints_file()
        records = storage["projects"][self.PROJECT_CONTEXT]["sprints"]
        records_by_id = {str(record["id"]): record for record in records}
        raw_proposals = {
            pending_id: json.loads(
                json.dumps(records_by_id[pending_id]["proposal"], ensure_ascii=False)
            )
            for pending_id in (activated_id, activating_id, failed_id)
        }
        records_by_id[activated_id].update(
            {
                "status": "activated",
                "activation_attempts": 1,
                "activation_attempt_id": None,
                "activating_at": None,
                "activated_at": activated_at,
                "activated_sprint_id": activated_sprint_id,
                "last_activation_failed_at": None,
                "last_activation_error": None,
                "import_payload": None,
            }
        )
        records_by_id[activating_id].update(
            {
                "status": "activating",
                "activation_attempts": 1,
                "activation_attempt_id": "pre-upgrade-activation-attempt",
                "activating_at": activating_at,
                "activated_at": None,
                "activated_sprint_id": None,
                "last_activation_failed_at": None,
                "last_activation_error": None,
            }
        )
        records_by_id[failed_id].update(
            {
                "status": "pending",
                "activation_attempts": 1,
                "activation_attempt_id": None,
                "activating_at": None,
                "activated_at": None,
                "activated_sprint_id": None,
                "last_activation_failed_at": failed_at,
                "last_activation_error": {
                    "error": "legacy_import_failed",
                    "message": "Pre-upgrade legacy activation failed.",
                },
            }
        )
        main.write_pending_sprints_file(storage)
        bytes_before = main.pending_sprints_path.read_bytes()

        expected = (
            (
                activated_id,
                "started",
                "started",
                3,
                activated_at,
                activated_sprint_id,
            ),
            (activating_id, "ready", "starting", 2, activating_at, None),
            (failed_id, "failed", "failed", 3, failed_at, None),
        )
        for (
            pending_id,
            proposal_status,
            activation_state,
            revision,
            updated_at,
            started_sprint_id,
        ) in expected:
            correlation_id = f"corr-stale-projection-{proposal_status}"
            status_code, response_headers, response = await self.read_status(
                pending_id,
                correlation_id=correlation_id,
            )
            self.assertEqual(status_code, 200, response)
            projected = self.assert_status_response(
                response_headers,
                response,
                correlation_id=correlation_id,
                proposal_status=proposal_status,
                activation_state=activation_state,
                pending_id=pending_id,
            )
            self.assertEqual(projected["revision"], revision)
            self.assertEqual(projected["updated_at"], updated_at)
            self.assertEqual(projected["started_sprint_id"], started_sprint_id)
            self.assertEqual(projected["validation"]["status"], "valid")

        self.assertEqual(main.pending_sprints_path.read_bytes(), bytes_before)
        persisted = main.read_pending_sprints_file()["projects"][self.PROJECT_CONTEXT][
            "sprints"
        ]
        persisted_by_id = {str(record["id"]): record for record in persisted}
        for pending_id, raw_proposal in raw_proposals.items():
            self.assertEqual(persisted_by_id[pending_id]["proposal"], raw_proposal)
            self.assertEqual(raw_proposal["proposal_status"], "ready")
            self.assertEqual(raw_proposal["activation_state"], "not_started")
            self.assertEqual(raw_proposal["revision"], 1)

        detail_status, _, failed_detail = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/{failed_id}"
        )
        self.assertEqual(detail_status, 200, failed_detail)
        self.assertTrue(failed_detail["pending_sprint"]["startable"])
        self.assertEqual(main.pending_sprints_path.read_bytes(), bytes_before)

    async def test_managed_preview_is_advisory_idempotent_and_nonactivating(
        self,
    ) -> None:
        create_status, _, created = await self.create(
            self.managed_proposal_payload(),
            correlation_id="corr-managed-create",
        )
        self.assertEqual(create_status, 201, created)
        pending_id = created["proposal"]["pending_sprint_id"]
        with patch.object(
            main,
            "execute_managed_project_sprint_start",
            new=AsyncMock(side_effect=AssertionError("Preview invoked managed Start")),
        ), patch.object(
            main,
            "import_project_actors_data",
            side_effect=AssertionError("Preview invoked the legacy importer"),
        ):
            preview_status, preview_headers, preview = await self.preview(pending_id)
        self.assertEqual(preview_status, 200, preview)
        self.assertEqual(preview_headers["x-correlation-id"], "corr-preview-1")
        proposal = preview["proposal"]
        self.assertEqual(proposal["proposal_status"], "ready")
        self.assertEqual(proposal["activation_state"], "not_started")
        self.assertEqual(proposal["validation"]["status"], "valid")
        self.assertEqual(proposal["revision"], 1)
        self.assertEqual(
            main.managed_schema_errors(
                preview,
                "inbound-pending-proposal-response-v1.schema.json",
                issue_code="TEST_SCHEMA_INVALID",
            ),
            [],
        )
        stored = main.read_pending_sprints_file()["projects"][self.PROJECT_CONTEXT][
            "sprints"
        ][0]
        self.assertEqual(stored["status"], "pending")
        self.assertEqual(stored["activation_attempts"], 0)
        self.assertTrue(main.pending_sprint_summary(stored)["startable"])
        before_replay = main.pending_sprints_path.read_bytes()
        replay_status, _, replay = await self.preview(
            pending_id,
            correlation_id="corr-preview-replay",
        )
        self.assertEqual(replay_status, 200, replay)
        self.assertTrue(replay["deduplicated"])
        self.assertEqual(replay["proposal"]["revision"], 1)
        self.assertEqual(main.pending_sprints_path.read_bytes(), before_replay)
        self.assertTrue(all(not queue for queue in main.queues.values()))

    async def test_managed_preview_records_sanitized_invalid_candidate(self) -> None:
        create_status, _, created = await self.create(
            self.managed_proposal_payload(
                proposal_id="proposal-managed-invalid-preview",
                create_idempotency_key="create:managed-invalid-preview:v1",
                activation_idempotency_key="activate:managed-invalid-preview:v1",
            ),
            correlation_id="corr-managed-invalid-create",
        )
        self.assertEqual(create_status, 201, created)
        pending_id = created["proposal"]["pending_sprint_id"]
        storage = main.read_pending_sprints_file()
        record = storage["projects"][self.PROJECT_CONTEXT]["sprints"][0]
        record["proposal"]["candidate"]["request"]["ref"] = "invalid ref"
        main.write_pending_sprints_file(storage)
        with patch.object(
            main,
            "execute_managed_project_sprint_start",
            new=AsyncMock(side_effect=AssertionError("Preview invoked managed Start")),
        ):
            preview_status, _, preview = await self.preview(
                pending_id,
                idempotency_key="preview:managed-invalid:v1",
                correlation_id="corr-managed-invalid-preview",
            )
        self.assertEqual(preview_status, 200, preview)
        proposal = preview["proposal"]
        self.assertEqual(proposal["proposal_status"], "failed")
        self.assertEqual(proposal["activation_state"], "not_started")
        self.assertEqual(proposal["validation"]["status"], "invalid")
        self.assertEqual(
            proposal["validation"]["issues"][0]["code"],
            "MANAGED_CANDIDATE_INVALID",
        )
        self.assertNotIn(
            "invalid ref",
            json.dumps(proposal["validation"]),
        )
        self.assertEqual(record.get("activation_attempts"), 0)

    async def test_managed_start_is_fenced_once_and_preserves_concurrent_comment(
        self,
    ) -> None:
        create_status, _, created = await self.create(
            self.managed_proposal_payload(),
            correlation_id="corr-managed-start-create",
        )
        self.assertEqual(create_status, 201, created)
        pending_id = created["proposal"]["pending_sprint_id"]
        preview_status, _, preview = await self.preview(pending_id)
        self.assertEqual(preview_status, 200, preview)
        entered = asyncio.Event()
        release = asyncio.Event()
        managed_calls = []

        async def fake_managed_start(project_id, start_request, correlation_id):
            managed_calls.append((project_id, start_request, correlation_id))
            entered.set()
            await release.wait()
            return ManagedStartResult(
                {
                    "sprint_id": "msv1-" + ("a" * 64),
                    "status": "active",
                    "phase": "ACTIVATE",
                    "deduplicated": False,
                },
                201,
            )

        start_url = (
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/"
            f"{pending_id}/start"
        )
        with patch.object(
            main,
            "execute_managed_project_sprint_start",
            new=fake_managed_start,
        ), patch.object(
            main,
            "import_project_actors_data",
            side_effect=AssertionError("Managed proposal invoked legacy import"),
        ):
            first_task = asyncio.create_task(
                asgi_request(start_url, method="POST")
            )
            await asyncio.wait_for(entered.wait(), timeout=5)
            second_status, _, second = await asgi_request(start_url, method="POST")
            self.assertEqual(second_status, 409, second)
            self.assertEqual(second["detail"]["error"], "PROPOSAL_STATE_CONFLICT")
            regenerate_status, _, regenerate = await asgi_request(
                f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/"
                f"{pending_id}/regenerate-request",
                method="POST",
                payload={
                    "schema_version": 1,
                    "action": "request_regeneration",
                    "expected_revision": 2,
                    "idempotency_key": "regenerate:during-managed-start:v1",
                    "comment": "Regenerate after the active attempt settles.",
                },
            )
            self.assertEqual(regenerate_status, 409, regenerate)
            self.assertEqual(
                regenerate["detail"]["error"],
                "PROPOSAL_STATE_CONFLICT",
            )
            comment_status, _, commented = await asgi_request(
                f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/"
                f"{pending_id}/comments",
                method="POST",
                payload={
                    "schema_version": 1,
                    "action": "comment",
                    "expected_revision": 2,
                    "idempotency_key": "comment:during-managed-start:v1",
                    "comment": "Keep this comment across managed settlement.",
                },
                headers=self.auth_headers(
                    correlation_id="corr-comment-during-managed-start"
                ),
            )
            self.assertEqual(comment_status, 200, commented)
            release.set()
            first_status, _, first = await first_task
        self.assertEqual(first_status, 200, first)
        self.assertTrue(first["started"])
        self.assertTrue(first["managed"])
        self.assertEqual(len(managed_calls), 1)
        project_id, start_request, _ = managed_calls[0]
        self.assertEqual(project_id, self.PROJECT_PHONE)
        self.assertEqual(start_request.repository_id, "main")
        self.assertEqual(start_request.ref, "refs/heads/inbound-proposal")
        self.assertEqual(
            start_request.manifest_path,
            "orchestration/inbound-proposal.json",
        )
        self.assertEqual(
            start_request.idempotency_key,
            "activate:proposal-managed-1:v1",
        )
        proposal = first["pending_sprint"]["proposal"]
        self.assertEqual(proposal["proposal_status"], "started")
        self.assertEqual(proposal["activation_state"], "started")
        self.assertEqual(proposal["revision"], 4)
        self.assertEqual(len(proposal["comments"]), 1)
        self.assertEqual(first["pending_sprint"]["activation_attempts"], 1)

    async def test_managed_preflight_failure_is_sanitized_and_not_retryable(
        self,
    ) -> None:
        create_status, _, created = await self.create(
            self.managed_proposal_payload(
                proposal_id="proposal-managed-failure",
                create_idempotency_key="create:managed-failure:v1",
                activation_idempotency_key="activate:managed-failure:v1",
            ),
            correlation_id="corr-managed-failure-create",
        )
        self.assertEqual(create_status, 201, created)
        pending_id = created["proposal"]["pending_sprint_id"]
        preview_status, _, _ = await self.preview(
            pending_id,
            idempotency_key="preview:managed-failure:v1",
        )
        self.assertEqual(preview_status, 200)
        error = ManagedImportError(
            "SPRINT_PREFLIGHT_FAILED",
            409,
            "managed-preflight-correlation",
            phase="VALIDATE",
            issues=[
                {
                    "code": "MANIFEST_SCHEMA_INVALID",
                    "path": "manifest",
                    "message": "Managed manifest validation failed",
                }
            ],
            evidence={"stderr": "credential-canary-must-not-persist"},
        )
        runner = AsyncMock(side_effect=error)
        start_url = (
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/"
            f"{pending_id}/start"
        )
        with patch.object(
            main,
            "execute_managed_project_sprint_start",
            new=runner,
        ), patch.object(
            main,
            "import_project_actors_data",
            side_effect=AssertionError("Managed proposal invoked legacy import"),
        ):
            failed_status, _, failed = await asgi_request(
                start_url,
                method="POST",
            )
        self.assertEqual(failed_status, 409, failed)
        self.assertEqual(failed["detail"]["error"], "SPRINT_PREFLIGHT_FAILED")
        bytes_after_failure = main.pending_sprints_path.read_bytes()
        retry_status, _, retry = await asgi_request(start_url, method="POST")
        self.assertEqual(retry_status, 409, retry)
        self.assertEqual(retry, failed)
        self.assertEqual(runner.await_count, 1)
        self.assertEqual(main.pending_sprints_path.read_bytes(), bytes_after_failure)
        stored_text = main.pending_sprints_path.read_text(encoding="utf-8")
        self.assertNotIn("credential-canary-must-not-persist", stored_text)
        record = main.read_pending_sprints_file()["projects"][self.PROJECT_CONTEXT][
            "sprints"
        ][0]
        self.assertEqual(record["status"], "pending")
        self.assertIsNone(record["activation_attempt_id"])
        self.assertEqual(record["proposal"]["proposal_status"], "failed")
        self.assertEqual(record["proposal"]["activation_state"], "failed")
        self.assertEqual(record["proposal"]["validation"]["status"], "valid")
        self.assertTrue(all(not queue for queue in main.queues.values()))

    async def test_managed_nonterminal_receipt_remains_ambiguous_until_recovery(
        self,
    ) -> None:
        create_status, _, created = await self.create(
            self.managed_proposal_payload(
                proposal_id="proposal-managed-ambiguous",
                create_idempotency_key="create:managed-ambiguous:v1",
                activation_idempotency_key="activate:managed-ambiguous:v1",
            ),
            correlation_id="corr-managed-ambiguous-create",
        )
        self.assertEqual(create_status, 201, created)
        pending_id = created["proposal"]["pending_sprint_id"]
        preview_status, _, _ = await self.preview(
            pending_id,
            idempotency_key="preview:managed-ambiguous:v1",
        )
        self.assertEqual(preview_status, 200)
        error = ManagedImportError(
            "SPRINT_PREFLIGHT_FAILED",
            503,
            "managed-ambiguous-correlation",
            phase="VALIDATE",
        )
        error.managed_receipt_status = "VALIDATING"
        runner = AsyncMock(side_effect=error)
        start_url = (
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/"
            f"{pending_id}/start"
        )
        with patch.object(
            main,
            "execute_managed_project_sprint_start",
            new=runner,
        ):
            ambiguous_status, _, ambiguous = await asgi_request(
                start_url,
                method="POST",
            )
            live_retry_status, _, live_retry = await asgi_request(
                start_url,
                method="POST",
            )
        self.assertEqual(ambiguous_status, 503, ambiguous)
        self.assertEqual(live_retry_status, 409, live_retry)
        self.assertEqual(
            live_retry["detail"]["error"],
            "PROPOSAL_STATE_CONFLICT",
        )
        self.assertEqual(runner.await_count, 1)
        record = main.read_pending_sprints_file()["projects"][self.PROJECT_CONTEXT][
            "sprints"
        ][0]
        self.assertEqual(record["status"], "activating")
        self.assertTrue(record["activation_attempt_id"])
        self.assertEqual(record["proposal"]["proposal_status"], "ready")
        self.assertEqual(record["proposal"]["activation_state"], "starting")

    async def test_managed_503_without_receipt_remains_ambiguous_until_recovery(
        self,
    ) -> None:
        create_status, _, created = await self.create(
            self.managed_proposal_payload(
                proposal_id="proposal-managed-unavailable",
                create_idempotency_key="create:managed-unavailable:v1",
                activation_idempotency_key="activate:managed-unavailable:v1",
            ),
            correlation_id="corr-managed-unavailable-create",
        )
        self.assertEqual(create_status, 201, created)
        pending_id = created["proposal"]["pending_sprint_id"]
        preview_status, _, _ = await self.preview(
            pending_id,
            idempotency_key="preview:managed-unavailable:v1",
        )
        self.assertEqual(preview_status, 200)
        error = ManagedImportError(
            "SPRINT_PREFLIGHT_FAILED",
            503,
            "managed-unavailable-correlation",
            phase="VALIDATE",
        )
        runner = AsyncMock(side_effect=error)
        start_url = (
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/"
            f"{pending_id}/start"
        )
        with patch.object(
            main,
            "execute_managed_project_sprint_start",
            new=runner,
        ):
            unavailable_status, _, unavailable = await asgi_request(
                start_url,
                method="POST",
            )
            live_retry_status, _, live_retry = await asgi_request(
                start_url,
                method="POST",
            )
        self.assertEqual(unavailable_status, 503, unavailable)
        self.assertEqual(live_retry_status, 409, live_retry)
        self.assertEqual(
            live_retry["detail"]["error"],
            "PROPOSAL_STATE_CONFLICT",
        )
        self.assertEqual(runner.await_count, 1)
        record = main.read_pending_sprints_file()["projects"][self.PROJECT_CONTEXT][
            "sprints"
        ][0]
        self.assertEqual(record["status"], "activating")
        self.assertTrue(record["activation_attempt_id"])
        self.assertEqual(record["proposal"]["proposal_status"], "ready")
        self.assertEqual(record["proposal"]["activation_state"], "starting")

    async def test_managed_runner_classifies_only_matching_terminal_receipts(
        self,
    ) -> None:
        request_payload = self.managed_proposal_payload()["candidate"]["request"]
        start_request = main.validate_start_request(
            request_payload,
            "runner-terminal-race",
        )
        expected_fingerprint = start_request.request_fingerprint(self.PROJECT_PHONE)
        project_entry = {
            "project_phone": self.PROJECT_PHONE,
            "git_address": "https://github.com/example/inbound-proposals.git",
        }

        class Store:
            def __init__(self, receipt):
                self.receipt = receipt

            def lookup(self, project_id, idempotency_key):
                return self.receipt

        class TerminalRaceImporter:
            def __init__(self, receipt, terminal_result):
                self.store = Store(receipt)
                self.terminal_result = terminal_result
                self.calls = 0

            def start(self, project_id, request):
                self.calls += 1
                if self.calls == 1:
                    raise ManagedImportError(
                        "PROJECT_ACTIVATION_IN_PROGRESS",
                        409,
                        "competing-owner",
                        phase="VALIDATE",
                    )
                if isinstance(self.terminal_result, Exception):
                    raise self.terminal_result
                return self.terminal_result

        success_importer = TerminalRaceImporter(
            {
                "status": "SUCCEEDED",
                "request_fingerprint": expected_fingerprint,
            },
            None,
        )
        common_patches = (
            patch.object(main, "read_git_config", AsyncMock(return_value={})),
            patch.object(
                main,
                "project_for_group_api",
                return_value=("key", self.PROJECT_CONTEXT, project_entry, project_entry),
            ),
            patch.object(main, "managed_repository_registry_for_project", return_value={}),
            patch.object(main, "load_managed_runtime_config", return_value={}),
        )
        with common_patches[0], common_patches[1], common_patches[2], common_patches[3], patch.object(
            main,
            "managed_sprint_importer_factory",
            return_value=success_importer,
        ):
            with self.assertRaises(ManagedImportError) as succeeded:
                await main.execute_managed_project_sprint_start(
                    self.PROJECT_PHONE,
                    start_request,
                    "runner-terminal-race",
                )
        self.assertEqual(succeeded.exception.code, "PROJECT_ACTIVATION_IN_PROGRESS")
        self.assertEqual(succeeded.exception.managed_receipt_status, "SUCCEEDED")
        self.assertEqual(success_importer.calls, 1)

        stored_failure = ManagedImportError(
            "SPRINT_PREFLIGHT_FAILED",
            409,
            "stored-terminal-failure",
            phase="VALIDATE",
        )
        failed_importer = TerminalRaceImporter(
            {
                "status": "FAILED",
                "request_fingerprint": expected_fingerprint,
                "error": stored_failure.envelope,
                "http_status": stored_failure.http_status,
            },
            None,
        )
        with patch.object(
            main, "read_git_config", AsyncMock(return_value={})
        ), patch.object(
            main,
            "project_for_group_api",
            return_value=("key", self.PROJECT_CONTEXT, project_entry, project_entry),
        ), patch.object(
            main, "managed_repository_registry_for_project", return_value={}
        ), patch.object(
            main, "load_managed_runtime_config", return_value={}
        ), patch.object(
            main,
            "managed_sprint_importer_factory",
            return_value=failed_importer,
        ):
            with self.assertRaises(ManagedImportError) as raised:
                await main.execute_managed_project_sprint_start(
                    self.PROJECT_PHONE,
                    start_request,
                    "runner-terminal-failure",
                )
        self.assertEqual(raised.exception.code, "SPRINT_PREFLIGHT_FAILED")
        self.assertEqual(raised.exception.managed_receipt_status, "FAILED")
        self.assertEqual(failed_importer.calls, 1)

        mismatch_importer = TerminalRaceImporter(
            {
                "status": "SUCCEEDED",
                "request_fingerprint": "0" * 64,
            },
            None,
        )
        with patch.object(
            main, "read_git_config", AsyncMock(return_value={})
        ), patch.object(
            main,
            "project_for_group_api",
            return_value=("key", self.PROJECT_CONTEXT, project_entry, project_entry),
        ), patch.object(
            main, "managed_repository_registry_for_project", return_value={}
        ), patch.object(
            main, "load_managed_runtime_config", return_value={}
        ), patch.object(
            main,
            "managed_sprint_importer_factory",
            return_value=mismatch_importer,
        ):
            with self.assertRaises(ManagedImportError) as mismatch:
                await main.execute_managed_project_sprint_start(
                    self.PROJECT_PHONE,
                    start_request,
                    "runner-mismatched-terminal",
                )
        self.assertEqual(mismatch.exception.code, "PROJECT_ACTIVATION_IN_PROGRESS")
        self.assertIsNone(mismatch.exception.managed_receipt_status)
        self.assertEqual(mismatch_importer.calls, 1)

    async def test_managed_lost_completion_replays_frozen_request(self) -> None:
        create_status, _, created = await self.create(
            self.managed_proposal_payload(
                proposal_id="proposal-managed-recovery",
                create_idempotency_key="create:managed-recovery:v1",
                activation_idempotency_key="activate:managed-recovery:v1",
            ),
            correlation_id="corr-managed-recovery-create",
        )
        self.assertEqual(create_status, 201, created)
        pending_id = created["proposal"]["pending_sprint_id"]
        preview_status, _, _ = await self.preview(
            pending_id,
            idempotency_key="preview:managed-recovery:v1",
        )
        self.assertEqual(preview_status, 200)
        sprint_id = "msv1-" + ("b" * 64)
        frozen_requests = []

        async def fake_managed_start(project_id, start_request, correlation_id):
            frozen_requests.append(
                (
                    start_request.repository_id,
                    start_request.ref,
                    start_request.manifest_path,
                    start_request.idempotency_key,
                )
            )
            return ManagedStartResult(
                {
                    "sprint_id": sprint_id,
                    "status": "active",
                    "phase": "ACTIVATE",
                    "deduplicated": len(frozen_requests) > 1,
                },
                200 if len(frozen_requests) > 1 else 201,
            )

        real_settle = main.settle_managed_pending_proposal_file
        fail_completion_once = True

        def flaky_settle(**kwargs):
            nonlocal fail_completion_once
            if kwargs.get("action") == "complete" and fail_completion_once:
                fail_completion_once = False
                raise RuntimeError("simulated lost proposal completion")
            return real_settle(**kwargs)

        start_url = (
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/"
            f"{pending_id}/start"
        )
        with patch.object(
            main,
            "execute_managed_project_sprint_start",
            new=fake_managed_start,
        ), patch.object(
            main,
            "settle_managed_pending_proposal_file",
            new=flaky_settle,
        ):
            first_status, _, first = await asgi_request(start_url, method="POST")
            self.assertEqual(first_status, 500, first)
            storage = main.read_pending_sprints_file()
            record = storage["projects"][self.PROJECT_CONTEXT]["sprints"][0]
            first_attempt_id = record["activation_attempt_id"]
            candidate_sha256 = record["managed_activation"]["candidate_sha256"]
            self.assertEqual(record["proposal"]["activation_state"], "starting")
            record["activating_at"] = "2000-01-01T00:00:00+00:00"
            main.write_pending_sprints_file(storage)
            second_status, _, second = await asgi_request(start_url, method="POST")
        self.assertEqual(second_status, 200, second)
        self.assertTrue(second["started"])
        self.assertTrue(second["reconciled"])
        self.assertEqual(len(frozen_requests), 2)
        self.assertEqual(frozen_requests[0], frozen_requests[1])
        self.assertEqual(
            frozen_requests[0][3],
            "activate:managed-recovery:v1",
        )
        self.assertEqual(second["pending_sprint"]["activated_sprint_id"], sprint_id)
        self.assertEqual(second["pending_sprint"]["activation_attempts"], 2)
        with self.assertRaises(main.HTTPException):
            real_settle(
                context_key=self.PROJECT_CONTEXT,
                project_phone=self.PROJECT_PHONE,
                sprint_id=pending_id,
                activation_attempt_id=first_attempt_id,
                candidate_sha256=candidate_sha256,
                action="complete",
                activated_sprint_id=sprint_id,
            )

    async def test_concurrent_duplicate_create_has_one_record(self) -> None:
        results = await asyncio.gather(
            *(
                self.create(correlation_id=f"corr-concurrent-{index}")
                for index in range(4)
            )
        )
        self.assertEqual(sorted(item[0] for item in results), [200, 200, 200, 201])
        pending_ids = {
            item[2]["proposal"]["pending_sprint_id"]
            for item in results
        }
        self.assertEqual(len(pending_ids), 1)
        records = main.read_pending_sprints_file()["projects"][
            self.PROJECT_CONTEXT
        ]["sprints"]
        self.assertEqual(len(records), 1)

    async def test_secondary_alias_and_action_revision_conflicts_are_stable(self) -> None:
        create_status, _, created = await self.create()
        self.assertEqual(create_status, 201, created)
        pending_id = created["proposal"]["pending_sprint_id"]
        alias = self.proposal_payload(idempotency_key="create:proposal-1:alias")
        alias_status, _, alias_body = await self.create(
            alias,
            correlation_id="corr-alias",
        )
        self.assertEqual(alias_status, 200, alias_body)
        self.assertTrue(alias_body["deduplicated"])
        self.assertEqual(alias_body["proposal"]["pending_sprint_id"], pending_id)
        self.assertEqual(alias_body["proposal"]["revision"], 0)

        proposal_conflict = self.proposal_payload(
            idempotency_key="create:proposal-1:different",
            summary="Different proposal semantics",
        )
        conflict_status, _, conflict = await self.create(
            proposal_conflict,
            correlation_id="corr-proposal-conflict",
        )
        self.assertEqual(conflict_status, 409, conflict)
        self.assertEqual(conflict["detail"]["error"], "PROPOSAL_ID_CONFLICT")

        comment_url = (
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/"
            f"{pending_id}/comments"
        )
        first_action = {
            "schema_version": 1,
            "action": "comment",
            "expected_revision": 0,
            "idempotency_key": "comment:revision:v1",
            "comment": "First comment.",
        }
        first_status, _, first = await asgi_request(
            comment_url,
            method="POST",
            payload=first_action,
            headers=self.auth_headers(correlation_id="corr-first-action"),
        )
        self.assertEqual(first_status, 200, first)
        changed_replay = {**first_action, "comment": "Changed replay."}
        changed_status, _, changed = await asgi_request(
            comment_url,
            method="POST",
            payload=changed_replay,
            headers=self.auth_headers(correlation_id="corr-changed-action"),
        )
        self.assertEqual(changed_status, 409, changed)
        self.assertEqual(
            changed["detail"]["error"],
            "PROPOSAL_IDEMPOTENCY_CONFLICT",
        )
        stale_action = {
            **first_action,
            "idempotency_key": "comment:revision:stale",
        }
        stale_status, _, stale = await asgi_request(
            comment_url,
            method="POST",
            payload=stale_action,
            headers=self.auth_headers(correlation_id="corr-stale-action"),
        )
        self.assertEqual(stale_status, 409, stale)
        self.assertEqual(stale["detail"]["error"], "PROPOSAL_REVISION_CONFLICT")
        self.assertEqual(stale["detail"]["expected_revision"], 0)
        self.assertEqual(stale["detail"]["actual_revision"], 1)

    async def test_comment_reject_replay_and_start_guard_do_not_activate(self) -> None:
        create_status, _, created = await self.create()
        self.assertEqual(create_status, 201, created)
        pending_id = created["proposal"]["pending_sprint_id"]
        comment = {
            "schema_version": 1,
            "action": "comment",
            "expected_revision": 0,
            "idempotency_key": "comment:proposal-1:v1",
            "comment": "Reviewed by the producer.",
        }
        comment_url = (
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/"
            f"{pending_id}/comments"
        )
        comment_status, _, commented = await asgi_request(
            comment_url,
            method="POST",
            payload=comment,
            headers=self.auth_headers(correlation_id="corr-comment-1"),
        )
        self.assertEqual(comment_status, 200, commented)
        self.assertFalse(commented["deduplicated"])
        self.assertEqual(commented["proposal"]["revision"], 1)
        self.assertEqual(
            commented["proposal"]["comments"][0]["actor"],
            {"actor_type": "producer", "actor_id": "producer:inbound-hub"},
        )
        after_comment = main.pending_sprints_path.read_bytes()
        replay_status, _, replay = await asgi_request(
            comment_url,
            method="POST",
            payload=comment,
            headers=self.auth_headers(correlation_id="corr-comment-replay"),
        )
        self.assertEqual(replay_status, 200, replay)
        self.assertTrue(replay["deduplicated"])
        self.assertEqual(replay["proposal"]["revision"], 1)
        self.assertEqual(main.pending_sprints_path.read_bytes(), after_comment)

        reject = {
            "schema_version": 1,
            "action": "reject",
            "expected_revision": 1,
            "idempotency_key": "reject:proposal-1:v1",
            "comment": "The proposal must be revised.",
        }
        reject_status, _, rejected = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/"
            f"{pending_id}/reject",
            method="POST",
            payload=reject,
            headers=self.auth_headers(correlation_id="corr-reject-1"),
        )
        self.assertEqual(reject_status, 200, rejected)
        self.assertEqual(rejected["proposal"]["proposal_status"], "rejected")
        self.assertEqual(rejected["proposal"]["revision"], 2)
        before_start = main.pending_sprints_path.read_bytes()
        with patch.object(
            main,
            "update_pending_sprint_activation_file",
            side_effect=AssertionError("rejected proposal reached activation"),
        ):
            start_status, _, start_body = await asgi_request(
                f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/"
                f"{pending_id}/start",
                method="POST",
            )
        self.assertEqual(start_status, 409, start_body)
        self.assertEqual(start_body["detail"]["error"], "PROPOSAL_STATE_CONFLICT")
        self.assertEqual(main.pending_sprints_path.read_bytes(), before_start)
        self.assertTrue(all(not queue for queue in main.queues.values()))

    async def test_operator_regeneration_marker_is_idempotent_and_nonactivating(
        self,
    ) -> None:
        create_status, _, created = await self.create()
        self.assertEqual(create_status, 201, created)
        pending_id = created["proposal"]["pending_sprint_id"]
        project = main.read_pending_sprints_file()["projects"][
            self.PROJECT_CONTEXT
        ]
        capability = project["access_token"]
        payload = {
            "schema_version": 1,
            "action": "request_regeneration",
            "expected_revision": 0,
            "idempotency_key": "regenerate:operator:proposal-1:v1",
            "comment": "Regenerate the source proposal with corrected metadata.",
        }
        url = (
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/"
            f"{pending_id}/regenerate-request"
        )
        headers = [
            (
                main.PENDING_SPRINT_TOKEN_HEADER.lower().encode("ascii"),
                capability.encode("ascii"),
            ),
            (b"x-correlation-id", b"corr-operator-regenerate"),
        ]
        with patch.object(
            main,
            "update_pending_sprint_activation_file",
            side_effect=AssertionError("regeneration marker activated a sprint"),
        ), patch.object(
            main,
            "execute_managed_project_sprint_start",
            new=AsyncMock(
                side_effect=AssertionError("regeneration marker invoked managed Start")
            ),
        ):
            status_code, _, response = await asgi_request(
                url,
                method="POST",
                payload=payload,
                headers=headers,
                host="pending.example.test",
                client_host="203.0.113.10",
            )
        self.assertEqual(status_code, 200, response)
        self.assertFalse(response["deduplicated"])
        proposal = response["proposal"]
        self.assertTrue(proposal["regenerate_requested"])
        self.assertEqual(proposal["revision"], 1)
        self.assertEqual(proposal["proposal_status"], "created")
        self.assertEqual(proposal["activation_state"], "not_started")
        self.assertEqual(proposal["validation"]["status"], "not_run")
        self.assertIsNone(proposal["started_sprint_id"])
        self.assertEqual(len(proposal["comments"]), 1)
        self.assertEqual(
            proposal["comments"][0]["actor"],
            {
                "actor_type": "operator",
                "actor_id": f"pending-token:{self.PROJECT_PHONE}",
            },
        )
        stored = main.read_pending_sprints_file()["projects"][
            self.PROJECT_CONTEXT
        ]["sprints"][0]
        self.assertEqual(stored["status"], "pending")
        self.assertEqual(stored["activation_attempts"], 0)
        self.assertIsNone(stored["activation_attempt_id"])
        bytes_after_first = main.pending_sprints_path.read_bytes()

        replay_status, _, replay = await asgi_request(
            url,
            method="POST",
            payload=payload,
            headers=[
                headers[0],
                (b"x-correlation-id", b"corr-operator-regenerate-replay"),
            ],
            host="pending.example.test",
            client_host="203.0.113.10",
        )
        self.assertEqual(replay_status, 200, replay)
        self.assertTrue(replay["deduplicated"])
        self.assertEqual(replay["proposal"]["revision"], 1)
        self.assertEqual(len(replay["proposal"]["comments"]), 1)
        self.assertEqual(main.pending_sprints_path.read_bytes(), bytes_after_first)
        self.assertTrue(all(not queue for queue in main.queues.values()))

    async def test_conflicts_invalid_payload_secrets_and_ownership_are_stable(self) -> None:
        create_status, _, created = await self.create()
        self.assertEqual(create_status, 201, created)
        pending_id = created["proposal"]["pending_sprint_id"]
        bytes_before = main.pending_sprints_path.read_bytes()

        changed = self.proposal_payload(summary="Changed semantic request")
        conflict_status, _, conflict = await self.create(changed)
        self.assertEqual(conflict_status, 409, conflict)
        self.assertEqual(
            conflict["detail"]["error"],
            "PROPOSAL_IDEMPOTENCY_CONFLICT",
        )

        invalid = self.proposal_payload(proposal_id="proposal-invalid")
        invalid.pop("source")
        invalid_status, _, invalid_body = await self.create(
            invalid,
            correlation_id="corr-invalid",
        )
        self.assertEqual(invalid_status, 400, invalid_body)
        self.assertEqual(invalid_body["detail"]["error"], "PROPOSAL_REQUEST_INVALID")

        secret = self.proposal_payload(
            proposal_id="proposal-secret",
            idempotency_key="create:proposal-secret:v1",
            summary="Bearer secret-value-that-must-never-be-persisted",
        )
        secret_status, _, secret_body = await self.create(
            secret,
            correlation_id="corr-secret",
        )
        self.assertEqual(secret_status, 400, secret_body)
        self.assertNotIn("secret-value", json.dumps(secret_body))

        configured_secret = self.proposal_payload(
            proposal_id="proposal-configured-secret",
            idempotency_key="create:proposal-configured-secret:v1",
            summary="configured-secret-canary",
        )
        with patch.dict(
            os.environ,
            {"TELEGRAM_WEBHOOK_SECRET": "configured-secret-canary"},
            clear=False,
        ):
            configured_status, _, configured_body = await self.create(
                configured_secret,
                correlation_id="corr-configured-secret",
            )
        self.assertEqual(configured_status, 400, configured_body)
        self.assertNotIn("configured-secret-canary", json.dumps(configured_body))

        short_auth = self.proposal_payload(
            proposal_id="proposal-short-auth",
            idempotency_key="create:proposal-short-auth:v1",
            summary="Basic abc",
        )
        short_status, _, _ = await self.create(
            short_auth,
            correlation_id="corr-short-auth",
        )
        self.assertEqual(short_status, 400)
        self.assertEqual(main.pending_sprints_path.read_bytes(), bytes_before)

        foreign_status, _, foreign = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/{pending_id}",
            headers=self.auth_headers(
                token=self.OTHER_TOKEN,
                correlation_id="corr-foreign",
            ),
        )
        self.assertEqual(foreign_status, 404, foreign)
        self.assertEqual(foreign["detail"]["error"], "PROPOSAL_NOT_FOUND")

        no_auth_status, no_auth_headers, no_auth = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints",
            method="POST",
            payload=self.proposal_payload(),
            headers=[(b"x-correlation-id", b"corr-no-auth")],
        )
        self.assertEqual(no_auth_status, 401, no_auth)
        self.assertEqual(no_auth["detail"]["error"], "PROPOSAL_AUTH_REQUIRED")
        self.assertEqual(no_auth_headers["www-authenticate"], "Bearer")

    async def test_credentials_are_rejected_from_payload_and_alternate_transports(
        self,
    ) -> None:
        bytes_before = (
            main.pending_sprints_path.read_bytes()
            if main.pending_sprints_path.exists()
            else None
        )
        candidate_secret = self.proposal_payload(
            proposal_id="proposal-candidate-secret",
            idempotency_key="create:candidate-secret:v1",
        )
        candidate_secret["candidate"]["payload"]["credential"] = self.TOKEN
        status_code, _, response = await self.create(
            candidate_secret,
            correlation_id="corr-candidate-secret",
        )
        self.assertEqual(status_code, 400, response)
        self.assertNotIn(self.TOKEN, json.dumps(response))

        key_secret = self.proposal_payload(
            proposal_id="proposal-key-secret",
            idempotency_key="create:key-secret:v1",
        )
        key_secret["candidate"]["payload"][self.TOKEN] = "credential-key"
        status_code, _, response = await self.create(
            key_secret,
            correlation_id="corr-key-secret",
        )
        self.assertEqual(status_code, 400, response)
        self.assertNotIn(self.TOKEN, json.dumps(response))

        idempotency_secret = self.proposal_payload(
            proposal_id="proposal-idempotency-secret",
            idempotency_key=self.TOKEN,
        )
        status_code, _, response = await self.create(
            idempotency_secret,
            correlation_id="corr-idempotency-secret",
        )
        self.assertEqual(status_code, 400, response)
        self.assertNotIn(self.TOKEN, json.dumps(response))

        credential_url = self.proposal_payload(
            proposal_id="proposal-credential-url",
            idempotency_key="create:credential-url:v1",
            summary="See https://example.test/path?pending_token=url-canary-value",
        )
        status_code, _, response = await self.create(
            credential_url,
            correlation_id="corr-credential-url",
        )
        self.assertEqual(status_code, 400, response)
        self.assertNotIn("url-canary-value", json.dumps(response))
        for index, parameter_name in enumerate(
            ("token", "password", "X-Amz-Credential"),
            start=1,
        ):
            broad_url = self.proposal_payload(
                proposal_id=f"proposal-credential-url-{index}",
                idempotency_key=f"create:credential-url-{index}:v1",
                summary=(
                    "See https://example.test/path?"
                    f"{parameter_name}=broad-url-canary-{index}"
                ),
            )
            status_code, _, response = await self.create(
                broad_url,
                correlation_id=f"corr-credential-url-{index}",
            )
            self.assertEqual(status_code, 400, response)
            self.assertNotIn(f"broad-url-canary-{index}", json.dumps(response))

        managed_secret = self.proposal_payload(
            proposal_id="proposal-managed-secret",
            idempotency_key="create:managed-secret:v1",
        )
        managed_secret["candidate"] = {
            "kind": "managed_git",
            "request": {
                "repository_id": "main",
                "ref": "refs/heads/inbound-proposal",
                "manifest_path": "orchestration/inbound-proposal.json",
                "idempotency_key": "Bearer candidate-secret-canary",
            },
        }
        status_code, _, response = await self.create(
            managed_secret,
            correlation_id="corr-managed-secret",
        )
        self.assertEqual(status_code, 400, response)
        self.assertNotIn("candidate-secret-canary", json.dumps(response))
        if bytes_before is None:
            self.assertFalse(main.pending_sprints_path.exists())
        else:
            self.assertEqual(main.pending_sprints_path.read_bytes(), bytes_before)

        query_status, query_headers, query_body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints"
            f"?access_token={self.TOKEN}",
            headers=[(b"x-correlation-id", b"corr-query-token")],
        )
        self.assertEqual(query_status, 401, query_body)
        self.assertEqual(query_headers["www-authenticate"], "Bearer")
        opaque_query_status, _, opaque_query = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints"
            f"?foo={self.TOKEN}",
            headers=[(b"x-correlation-id", b"corr-opaque-query-token")],
        )
        self.assertEqual(opaque_query_status, 401, opaque_query)
        invalid_correlation_status, _, invalid_correlation = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints"
            f"?access_token={self.TOKEN}",
            headers=[(b"x-correlation-id", b"invalid correlation")],
        )
        self.assertEqual(invalid_correlation_status, 400, invalid_correlation)
        self.assertEqual(
            invalid_correlation["detail"]["error"],
            "PROPOSAL_REQUEST_INVALID",
        )

        cookie_status, _, cookie_body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints",
            headers=[
                (b"cookie", f"producer_token={self.TOKEN}".encode("ascii")),
                (b"x-correlation-id", b"corr-cookie-token"),
            ],
        )
        self.assertEqual(cookie_status, 401, cookie_body)
        self.assertEqual(cookie_body["detail"]["error"], "PROPOSAL_AUTH_REQUIRED")
        opaque_cookie_status, _, opaque_cookie = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints",
            headers=[
                (b"cookie", f"foo={self.TOKEN}".encode("ascii")),
                (b"x-correlation-id", b"corr-opaque-cookie-token"),
            ],
        )
        self.assertEqual(opaque_cookie_status, 401, opaque_cookie)
        mixed_status, _, mixed_body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints",
            headers=[
                *self.auth_headers(correlation_id="corr-mixed-auth"),
                (
                    main.PENDING_SPRINT_TOKEN_HEADER.lower().encode("ascii"),
                    b"unsupported-second-capability",
                ),
            ],
        )
        self.assertEqual(mixed_status, 401, mixed_body)
        self.assertEqual(mixed_body["detail"]["error"], "PROPOSAL_AUTH_REQUIRED")

    async def test_registered_producer_token_cannot_cross_persist(self) -> None:
        bytes_before = (
            main.pending_sprints_path.read_bytes()
            if main.pending_sprints_path.exists()
            else None
        )
        cross_producer = self.proposal_payload(
            proposal_id="proposal-cross-producer-token",
            idempotency_key="create:cross-producer-token:v1",
            summary=f"Credential canary {self.OTHER_TOKEN}",
        )
        status_code, _, response = await self.create(
            cross_producer,
            correlation_id="corr-cross-producer-token",
        )
        self.assertEqual(status_code, 400, response)
        self.assertEqual(response["detail"]["error"], "PROPOSAL_REQUEST_INVALID")
        self.assertNotIn(self.OTHER_TOKEN, json.dumps(response))
        if bytes_before is None:
            self.assertFalse(main.pending_sprints_path.exists())
        else:
            self.assertEqual(main.pending_sprints_path.read_bytes(), bytes_before)

        create_status, _, created = await self.create(
            correlation_id="corr-create-for-cross-token-comment"
        )
        self.assertEqual(create_status, 201, created)
        pending_id = created["proposal"]["pending_sprint_id"]
        stored_before_comment = main.pending_sprints_path.read_bytes()
        comment_status, _, comment_response = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/"
            f"{pending_id}/comments",
            method="POST",
            payload={
                "schema_version": 1,
                "action": "comment",
                "expected_revision": 0,
                "idempotency_key": "comment:cross-producer-token:v1",
                "comment": f"Credential canary {self.OTHER_TOKEN}",
            },
            headers=self.auth_headers(correlation_id="corr-cross-token-comment"),
        )
        self.assertEqual(comment_status, 400, comment_response)
        self.assertNotIn(self.OTHER_TOKEN, json.dumps(comment_response))
        self.assertEqual(
            main.pending_sprints_path.read_bytes(), stored_before_comment
        )
        self.assertNotIn(
            self.OTHER_TOKEN,
            main.pending_sprints_path.read_text(encoding="utf-8"),
        )

    async def test_registered_token_lazy_local_scan_fails_closed(self) -> None:
        create_status, _, created = await self.create(
            correlation_id="corr-create-for-local-token-scan"
        )
        self.assertEqual(create_status, 201, created)
        pending_id = created["proposal"]["pending_sprint_id"]
        comment_url = (
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/"
            f"{pending_id}/comments"
        )
        storage_before = main.pending_sprints_path.read_bytes()
        exact_token_action = {
            "schema_version": 1,
            "action": "comment",
            "expected_revision": 0,
            "idempotency_key": "comment:local-cross-token:v1",
            "comment": self.OTHER_TOKEN,
        }
        exact_status, _, exact_response = await asgi_request(
            comment_url,
            method="POST",
            payload=exact_token_action,
            headers=[(b"x-correlation-id", b"corr-local-cross-token")],
        )
        self.assertEqual(exact_status, 400, exact_response)
        self.assertEqual(
            exact_response["detail"]["error"], "PROPOSAL_REQUEST_INVALID"
        )
        self.assertNotIn(self.OTHER_TOKEN, json.dumps(exact_response))
        self.assertEqual(main.pending_sprints_path.read_bytes(), storage_before)

        with patch.object(
            main,
            "configured_producer_registry",
            side_effect=RuntimeError("registry unavailable canary"),
        ):
            unavailable_status, unavailable_headers, unavailable_response = (
                await asgi_request(
                    comment_url,
                    method="POST",
                    payload={
                        **exact_token_action,
                        "idempotency_key": "comment:local-registry-unavailable:v1",
                        "comment": "Ordinary local operator comment.",
                    },
                    headers=[
                        (b"x-correlation-id", b"corr-local-registry-unavailable")
                    ],
                )
            )
        self.assertEqual(unavailable_status, 503, unavailable_response)
        self.assertEqual(
            unavailable_response["detail"]["error"],
            "PROPOSAL_SERVICE_UNAVAILABLE",
        )
        self.assertTrue(unavailable_response["detail"]["retryable"])
        self.assertEqual(
            unavailable_response["detail"]["correlation_id"],
            "corr-local-registry-unavailable",
        )
        self.assertEqual(
            unavailable_headers["x-correlation-id"],
            "corr-local-registry-unavailable",
        )
        self.assertEqual(main.pending_sprints_path.read_bytes(), storage_before)

    async def test_documented_minimal_legacy_candidate_is_accepted(self) -> None:
        payload = self.proposal_payload(
            proposal_id="proposal-documented-minimal-legacy",
            idempotency_key="create:documented-minimal-legacy:v1",
        )
        self.assertEqual(
            payload["candidate"]["payload"],
            {"sprint_type": "legacy_v1", "actors": []},
        )
        status_code, _, response = await self.create(
            payload,
            correlation_id="corr-documented-minimal-legacy",
        )
        self.assertEqual(status_code, 201, response)
        record = main.read_pending_sprints_file()["projects"][
            self.PROJECT_CONTEXT
        ]["sprints"][0]
        self.assertEqual(record["assignment_mode"], "parallel")
        self.assertEqual(record["agent_count"], 0)
        self.assertEqual(record["task_count"], 0)

    async def test_minimal_legacy_proposal_start_tracks_failure_retry_and_completion(
        self,
    ) -> None:
        payload = self.proposal_payload(
            proposal_id="proposal-minimal-legacy-start",
            idempotency_key="create:proposal-minimal-legacy-start:v1",
        )
        self.assertEqual(
            payload["candidate"]["payload"],
            {"sprint_type": "legacy_v1", "actors": []},
        )
        create_status, _, created = await self.create(
            payload,
            correlation_id="corr-minimal-legacy-start-create",
        )
        self.assertEqual(create_status, 201, created)
        pending_id = created["proposal"]["pending_sprint_id"]
        preview_status, _, preview = await self.preview(
            pending_id,
            idempotency_key="preview:minimal-legacy-start:v1",
            correlation_id="corr-minimal-legacy-start-preview",
        )
        self.assertEqual(preview_status, 200, preview)
        self.assertEqual(preview["proposal"]["proposal_status"], "ready")
        self.assertEqual(preview["proposal"]["activation_state"], "not_started")
        self.assertEqual(preview["proposal"]["revision"], 1)

        observed_during_import: list[dict[str, object]] = []
        importer_payloads: list[dict[str, object]] = []

        async def observe_starting(correlation_id: str) -> None:
            status_code, response_headers, response = await self.read_status(
                pending_id,
                correlation_id=correlation_id,
            )
            self.assertEqual(status_code, 200, response)
            observed_during_import.append(
                self.assert_status_response(
                    response_headers,
                    response,
                    correlation_id=correlation_id,
                    proposal_status="ready",
                    activation_state="starting",
                    pending_id=pending_id,
                )
            )

        async def failing_import(
            project_phone: str,
            import_payload: dict[str, object],
            **kwargs: object,
        ) -> dict[str, object]:
            self.assertEqual(project_phone, self.PROJECT_PHONE)
            self.assertEqual(kwargs["source"], "inbound-proposal")
            importer_payloads.append(
                json.loads(json.dumps(import_payload, ensure_ascii=False))
            )
            await observe_starting("corr-minimal-legacy-starting-failed")
            raise main.HTTPException(
                status_code=409,
                detail={
                    "error": "legacy_import_failed",
                    "message": "Synthetic legacy importer failure.",
                },
            )

        start_url = (
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/"
            f"{pending_id}/start"
        )
        with patch.object(
            main,
            "import_project_actors_data",
            new=failing_import,
        ):
            failed_status, _, failed = await asgi_request(start_url, method="POST")
        self.assertEqual(failed_status, 409, failed)
        self.assertEqual(failed["detail"]["error"], "legacy_import_failed")

        failed_read_status, failed_headers, failed_read = await self.read_status(
            pending_id,
            correlation_id="corr-minimal-legacy-failed-read",
        )
        self.assertEqual(failed_read_status, 200, failed_read)
        failed_resource = self.assert_status_response(
            failed_headers,
            failed_read,
            correlation_id="corr-minimal-legacy-failed-read",
            proposal_status="failed",
            activation_state="failed",
            pending_id=pending_id,
        )
        self.assertEqual(failed_resource["validation"]["status"], "valid")
        self.assertEqual(failed_resource["revision"], 3)
        failed_detail_status, _, failed_detail = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/{pending_id}"
        )
        self.assertEqual(failed_detail_status, 200, failed_detail)
        self.assertTrue(failed_detail["pending_sprint"]["startable"])
        self.assertIsNotNone(failed_detail["import_payload"])

        activated_sprint_id = "legacy-started-from-proposal"

        async def successful_import(
            project_phone: str,
            import_payload: dict[str, object],
            **kwargs: object,
        ) -> dict[str, object]:
            self.assertEqual(project_phone, self.PROJECT_PHONE)
            self.assertTrue(kwargs["reconcile_existing_pending_sprint"])
            importer_payloads.append(
                json.loads(json.dumps(import_payload, ensure_ascii=False))
            )
            await observe_starting("corr-minimal-legacy-starting-retry")
            return {
                "source": "inbound-proposal",
                "sprint": {
                    "id": activated_sprint_id,
                    "status": "current",
                    "imported_at": "2026-10-08T09:00:00Z",
                },
            }

        with patch.object(
            main,
            "project_sprint_for_pending_id_file",
            return_value=None,
        ), patch.object(
            main,
            "import_project_actors_data",
            new=successful_import,
        ):
            retry_status, _, retried = await asgi_request(start_url, method="POST")
        self.assertEqual(retry_status, 200, retried)
        self.assertTrue(retried["started"])
        self.assertEqual(
            retried["claimed_sprint"]["proposal"]["proposal_status"],
            "ready",
        )
        self.assertEqual(
            retried["claimed_sprint"]["proposal"]["activation_state"],
            "starting",
        )
        self.assertEqual(
            retried["pending_sprint"]["proposal"]["proposal_status"],
            "started",
        )
        self.assertEqual(
            retried["pending_sprint"]["proposal"]["activation_state"],
            "started",
        )

        started_read_status, started_headers, started_read = await self.read_status(
            pending_id,
            correlation_id="corr-minimal-legacy-started-read",
        )
        self.assertEqual(started_read_status, 200, started_read)
        started_resource = self.assert_status_response(
            started_headers,
            started_read,
            correlation_id="corr-minimal-legacy-started-read",
            proposal_status="started",
            activation_state="started",
            pending_id=pending_id,
        )
        self.assertEqual(started_resource["started_sprint_id"], activated_sprint_id)
        self.assertEqual(started_resource["revision"], 5)
        self.assertEqual(
            [resource["revision"] for resource in observed_during_import],
            [2, 4],
        )
        expected_import_payload = {
            "sprint_type": "legacy_v1",
            "actors": [],
            "assignment_mode": "parallel",
            "project_id": self.PROJECT_PHONE,
        }
        self.assertEqual(importer_payloads, [expected_import_payload] * 2)
        terminal_detail_status, _, terminal_detail = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/{pending_id}"
        )
        self.assertEqual(terminal_detail_status, 200, terminal_detail)
        self.assertIsNone(terminal_detail["import_payload"])
        self.assertEqual(
            terminal_detail["pending_sprint"]["proposal"]["candidate"]["payload"],
            {"sprint_type": "legacy_v1", "actors": []},
        )

    async def test_legacy_candidate_semantics_and_summary_metadata_are_validated(
        self,
    ) -> None:
        invalid = self.proposal_payload(
            proposal_id="proposal-invalid-legacy-candidate",
            idempotency_key="create:invalid-legacy-candidate:v1",
        )
        invalid["candidate"]["payload"] = {
            "sprint_type": "legacy_v1",
            "agents": "not-an-object",
        }
        invalid_status, _, invalid_response = await self.create(
            invalid,
            correlation_id="corr-invalid-legacy-candidate",
        )
        self.assertEqual(invalid_status, 400, invalid_response)
        self.assertEqual(
            invalid_response["detail"]["error"], "PROPOSAL_REQUEST_INVALID"
        )
        self.assertFalse(main.pending_sprints_path.exists())

        valid = self.proposal_payload(
            proposal_id="proposal-sequential-metadata",
            idempotency_key="create:sequential-metadata:v1",
        )
        valid["candidate"]["payload"] = {
            "sprint_type": "legacy_v1",
            "agents": {
                "overwrite": True,
                "assignment_mode": "sequential",
                "items": [
                    {
                        "id": "first-agent",
                        "name": "First Agent",
                        "phone": "2101",
                        "tasks": ["First task."],
                    },
                    {
                        "id": "second-agent",
                        "name": "Second Agent",
                        "phone": "2102",
                        "tasks": ["Second task."],
                    },
                ],
            },
        }
        with patch.object(
            main,
            "update_pending_sprint_activation_file",
            side_effect=AssertionError("proposal validation must not activate"),
        ), patch.object(
            main,
            "import_project_actors_data",
            side_effect=AssertionError("proposal validation must not import"),
        ):
            create_status, _, created = await self.create(
                valid,
                correlation_id="corr-sequential-metadata",
            )
        self.assertEqual(create_status, 201, created)
        pending_id = created["proposal"]["pending_sprint_id"]
        record = main.read_pending_sprints_file()["projects"][
            self.PROJECT_CONTEXT
        ]["sprints"][0]
        self.assertEqual(record["assignment_mode"], "sequential")
        self.assertEqual(record["agent_count"], 2)
        self.assertEqual(record["task_count"], 2)

        detail_status, _, detail = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/{pending_id}",
            headers=self.auth_headers(correlation_id="corr-sequential-detail"),
        )
        self.assertEqual(detail_status, 200, detail)
        self.assertEqual(detail["pending_sprint"]["assignment_mode"], "sequential")
        self.assertEqual(detail["pending_sprint"]["agent_count"], 2)
        self.assertEqual(detail["pending_sprint"]["task_count"], 2)
        self.assertTrue(all(not queue for queue in main.queues.values()))

    async def test_deep_link_capability_cannot_be_persisted_as_comment(self) -> None:
        create_status, _, created = await self.create()
        self.assertEqual(create_status, 201, created)
        pending_id = created["proposal"]["pending_sprint_id"]
        main.stage_project_sprint_file(
            context_key="github.com/example/other-proposals",
            project_phone="9009",
            project_name="Other Proposal Project",
            git_address="https://github.com/example/other-proposals.git",
            repository_key="github.com/example/other-proposals",
            payload={"sprint": {"title": "Other pending sprint"}},
            source="telegram",
            source_filename="other.json",
            assignment_mode="parallel",
            agent_count=0,
            task_count=0,
        )
        projects = main.read_pending_sprints_file()["projects"]
        project = projects[self.PROJECT_CONTEXT]
        capability = project["access_token"]
        other_capability = projects[
            "github.com/example/other-proposals"
        ]["access_token"]
        bytes_before = main.pending_sprints_path.read_bytes()
        cross_project_create = self.proposal_payload(
            proposal_id="proposal-cross-project-capability",
            idempotency_key="create:cross-project-capability:v1",
            summary=other_capability,
        )
        cross_project_status, _, cross_project_body = await self.create(
            cross_project_create,
            correlation_id="corr-cross-project-capability",
        )
        self.assertEqual(cross_project_status, 400, cross_project_body)
        self.assertNotIn(other_capability, json.dumps(cross_project_body))
        capability_create = self.proposal_payload(
            proposal_id="proposal-project-capability",
            idempotency_key="create:project-capability:v1",
            summary=capability,
        )
        capability_create_status, _, capability_create_body = await self.create(
            capability_create,
            correlation_id="corr-project-capability-create",
        )
        self.assertEqual(capability_create_status, 400, capability_create_body)
        self.assertNotIn(capability, json.dumps(capability_create_body))
        compatible_status, _, compatible_body = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/"
            f"{pending_id}?foo={self.UNRELATED_TOKEN}",
            headers=[
                (
                    main.PENDING_SPRINT_TOKEN_HEADER.lower().encode("ascii"),
                    capability.encode("ascii"),
                ),
                (
                    b"cookie",
                    f"unrelated_session={self.UNRELATED_TOKEN}".encode("ascii"),
                ),
            ],
            host="pending.example.test",
            client_host="203.0.113.10",
        )
        self.assertEqual(compatible_status, 200, compatible_body)
        status_code, _, response = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/"
            f"{pending_id}/comments",
            method="POST",
            payload={
                "schema_version": 1,
                "action": "comment",
                "expected_revision": 0,
                "idempotency_key": "comment:capability-canary:v1",
                "comment": capability,
            },
            headers=[
                (
                    main.PENDING_SPRINT_TOKEN_HEADER.lower().encode("ascii"),
                    capability.encode("ascii"),
                ),
                (b"x-correlation-id", b"corr-capability-canary"),
            ],
            client_host="203.0.113.10",
        )
        self.assertEqual(status_code, 400, response)
        self.assertNotIn(capability, json.dumps(response))
        self.assertEqual(main.pending_sprints_path.read_bytes(), bytes_before)
        action_payload = {
            "schema_version": 1,
            "action": "comment",
            "expected_revision": 0,
            "idempotency_key": "comment:stored-capability:v1",
            "comment": capability,
        }
        local_status, _, local_response = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/"
            f"{pending_id}/comments",
            method="POST",
            payload=action_payload,
            headers=[(b"x-correlation-id", b"corr-local-capability")],
        )
        self.assertEqual(local_status, 400, local_response)
        self.assertNotIn(capability, json.dumps(local_response))
        cross_action_status, _, cross_action_response = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/"
            f"{pending_id}/comments",
            method="POST",
            payload={
                **action_payload,
                "idempotency_key": "comment:cross-project-capability:v1",
                "comment": other_capability,
            },
            headers=[(b"x-correlation-id", b"corr-cross-action-capability")],
        )
        self.assertEqual(cross_action_status, 400, cross_action_response)
        self.assertNotIn(other_capability, json.dumps(cross_action_response))
        producer_status, _, producer_response = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/"
            f"{pending_id}/comments",
            method="POST",
            payload={
                **action_payload,
                "idempotency_key": "comment:producer-capability:v1",
            },
            headers=self.auth_headers(correlation_id="corr-producer-capability"),
        )
        self.assertEqual(producer_status, 400, producer_response)
        self.assertNotIn(capability, json.dumps(producer_response))
        self.assertEqual(main.pending_sprints_path.read_bytes(), bytes_before)

    async def test_long_producer_actor_still_matches_response_schema(self) -> None:
        registry = json.loads(self.registry_path.read_text(encoding="utf-8"))
        registry["producers"][0]["producer_id"] = "p" * 200
        self.registry_path.write_text(json.dumps(registry), encoding="utf-8")
        create_status, _, created = await self.create()
        self.assertEqual(create_status, 201, created)
        pending_id = created["proposal"]["pending_sprint_id"]
        comment_status, _, commented = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/"
            f"{pending_id}/comments",
            method="POST",
            payload={
                "schema_version": 1,
                "action": "comment",
                "expected_revision": 0,
                "idempotency_key": "comment:long-producer:v1",
                "comment": "Schema boundary check.",
            },
            headers=self.auth_headers(correlation_id="corr-long-producer"),
        )
        self.assertEqual(comment_status, 200, commented)
        self.assertEqual(
            main.managed_schema_errors(
                commented,
                "inbound-pending-proposal-response-v1.schema.json",
                issue_code="TEST_SCHEMA_INVALID",
            ),
            [],
        )

    def test_bearer_token_must_be_canonical_base64url(self) -> None:
        invalid_token = "A" * 45
        principal = main.ProducerPrincipal(
            producer_id="invalid-token-fixture",
            token_sha256=hashlib.sha256(invalid_token.encode("ascii")).hexdigest(),
            project_ids=frozenset({self.PROJECT_PHONE}),
            actions=frozenset({"read"}),
        )
        self.assertIsNone(
            main.authenticate_bearer([f"Bearer {invalid_token}"], [principal])
        )

    def test_windows_acl_model_rejects_untrusted_owner_and_replacement(self) -> None:
        current_sid = "S-1-5-21-current"
        read_rule = {
            "sid": "S-1-5-32-545",
            "rights": 1,
            "type": "Allow",
            "propagation": "None",
        }
        protected = {
            "current_sid": current_sid,
            "items": [
                {"owner": current_sid, "access": [read_rule]},
                {"owner": current_sid, "access": [read_rule]},
                {"owner": "S-1-5-18", "access": [read_rule]},
            ],
        }
        self.assertTrue(
            inbound_proposals._windows_registry_acl_is_protected(protected)
        )
        attacker_owned = json.loads(json.dumps(protected))
        attacker_owned["items"][0]["owner"] = "S-1-5-21-attacker"
        self.assertFalse(
            inbound_proposals._windows_registry_acl_is_protected(attacker_owned)
        )
        replaceable_parent = json.loads(json.dumps(protected))
        replaceable_parent["items"][1]["access"].append(
            {
                "sid": "S-1-5-21-attacker",
                "rights": 64,
                "type": "Allow",
                "propagation": "None",
            }
        )
        self.assertFalse(
            inbound_proposals._windows_registry_acl_is_protected(
                replaceable_parent
            )
        )

    async def test_legacy_pending_read_is_additive_and_registry_absence_isolated(self) -> None:
        legacy = main.stage_project_sprint_file(
            context_key=self.PROJECT_CONTEXT,
            project_phone=self.PROJECT_PHONE,
            project_name="Inbound Proposal Project",
            git_address="https://github.com/example/inbound-proposals.git",
            repository_key="github.com/example/inbound-proposals",
            payload={"sprint": {"title": "Telegram pending"}},
            source="telegram",
            source_filename="telegram.json",
            assignment_mode="parallel",
            agent_count=0,
            task_count=0,
            telegram_update_id="telegram-update-1",
        )
        before_read = main.pending_sprints_path.read_bytes()
        local_status, _, local = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/"
            f"{legacy['id']}"
        )
        self.assertEqual(local_status, 200, local)
        self.assertEqual(local["pending_sprint"]["source"], "telegram")
        self.assertIsNone(local["pending_sprint"]["proposal"])
        self.assertEqual(main.pending_sprints_path.read_bytes(), before_read)

        os.environ.pop(main.INBOUND_PRODUCER_REGISTRY_ENV, None)
        disabled_status, _, disabled = await self.create()
        self.assertEqual(disabled_status, 503, disabled)
        self.assertEqual(
            disabled["detail"]["error"],
            "PROPOSAL_SERVICE_UNAVAILABLE",
        )
        disabled_alternate_status, _, disabled_alternate = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints"
            f"?access_token={self.TOKEN}",
            headers=[(b"x-correlation-id", b"corr-disabled-alternate")],
        )
        self.assertEqual(disabled_alternate_status, 503, disabled_alternate)
        self.assertEqual(
            disabled_alternate["detail"]["error"],
            "PROPOSAL_SERVICE_UNAVAILABLE",
        )
        local_again_status, _, _ = await asgi_request(
            f"/api/v1/projects/{self.PROJECT_PHONE}/pending-sprints/"
            f"{legacy['id']}"
        )
        self.assertEqual(local_again_status, 200)

    def test_startup_registry_validation_fails_closed_on_unsafe_acl(self) -> None:
        with patch(
            "nginx_qa.inbound_proposals._registry_acl_is_protected",
            return_value=False,
        ):
            with self.assertRaisesRegex(RuntimeError, "ACL is unsafe"):
                main.configured_producer_registry(
                    known_project_ids={self.PROJECT_PHONE}
                )
            with self.assertRaisesRegex(RuntimeError, "ACL is unsafe"):
                main.configured_producer_registry()


if __name__ == "__main__":
    unittest.main()
