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
from unittest.mock import patch

import main
from nginx_qa import inbound_proposals


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
                            "actions": ["create", "read", "comment", "reject"],
                        },
                        {
                            "producer_id": "other-hub",
                            "token_sha256": hashlib.sha256(
                                self.OTHER_TOKEN.encode("ascii")
                            ).hexdigest(),
                            "project_ids": [self.PROJECT_PHONE],
                            "actions": ["create", "read", "comment", "reject"],
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
                    "actors": {},
                },
            },
        }

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
