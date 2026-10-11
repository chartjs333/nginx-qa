import asyncio
import json
import threading
from types import SimpleNamespace
import unittest

from fastapi import FastAPI, HTTPException

from nginx_qa.prompt_archive_api import (
    _HydrationAdmission,
    register_prompt_archive_api,
)
from nginx_qa.prompt_archive_metrics import PromptArchiveMetrics
from nginx_qa.prompt_archive_storage import PromptArchiveStorageError


PROJECT_ID = "9000"
SPRINT_ID = "msv1-" + "1" * 64
ARCHIVE_ID = "par1-" + "2" * 64
RESPONSE_ID = "parresp1-" + "3" * 64
AGENT_PHONE = "2974"
CONTENT = b'{"hello":"archive"}'


class FakeArchiveRuntime:
    def __init__(self, project_id=PROJECT_ID):
        self.project_id = project_id
        self.lookup_calls = []
        self.legacy_lookup_calls = []
        self.manifest_calls = []
        self.hydrate_calls = []
        self.lookup_error = None
        self.hydrate_error = None
        self.hydrate_started = None
        self.hydrate_release = None
        self.hydrate_exited = None
        self.slow_hydration_chunks = 0
        self.hydrated_chunks = 0
        self.read = SimpleNamespace(
            archive_id=ARCHIVE_ID,
            archive_format="incremental_transition_v1",
            scope={
                "project_id": PROJECT_ID,
                "sprint_id": SPRINT_ID,
                "response_id": RESPONSE_ID,
                "response_kind": "transition-response",
                "occurrence_kind": "transition",
                "occurrence_id": "transition-1",
                "transition_id": "transition-1",
            },
            integrity={
                "semantics": "native-byte-equality",
                "hydrated_media_type": "application/json",
                "hydrated_size": len(CONTENT),
                "hydrated_sha256": "4" * 64,
            },
            created_at="2026-10-11T12:00:00Z",
        )
        self.manifest = {
            "schema_version": 1,
            "kind": "prompt_archive_manifest",
            "archive_id": ARCHIVE_ID,
            "scope": dict(self.read.scope),
        }

    def lookup_archive_metadata(self, *, sprint_id, archive_id, cancellation=None):
        self.lookup_calls.append((sprint_id, archive_id))
        if cancellation is not None:
            cancellation.check()
        if self.lookup_error is not None:
            raise self.lookup_error
        return self.read

    def lookup_archive(self, *, sprint_id, archive_id, cancellation=None):
        self.legacy_lookup_calls.append((sprint_id, archive_id))
        raise AssertionError("HTTP metadata resources used non-shallow lookup")

    def read_manifest(self, read):
        self.manifest_calls.append(read)
        return dict(self.manifest)

    def hydrate_content(self, read, *, cancellation=None):
        self.hydrate_calls.append(read)
        if self.hydrate_started is not None:
            self.hydrate_started.set()
        try:
            for _index in range(self.slow_hydration_chunks):
                if cancellation is not None:
                    cancellation.check()
                self.hydrated_chunks += 1
                threading.Event().wait(0.005)
            if self.hydrate_release is not None:
                while not self.hydrate_release.wait(timeout=0.005):
                    if cancellation is not None:
                        cancellation.check()
            if self.hydrate_error is not None:
                raise self.hydrate_error
            if cancellation is not None:
                cancellation.check()
            return CONTENT
        finally:
            if self.hydrate_exited is not None:
                self.hydrate_exited.set()


class FakeRegistry:
    def __init__(self, runtime):
        self.runtime = runtime
        self.calls = []

    def for_project(self, project_id):
        self.calls.append(project_id)
        return self.runtime


class FakeManagedContinuity:
    def __init__(self):
        self.calls = []
        self.authorized = True
        self.error = None

    def authorize_prompt_archive_read(
        self, project_id, sprint_id, agent_phone, supplied_role_token
    ):
        self.calls.append(
            (project_id, sprint_id, agent_phone, supplied_role_token)
        )
        if self.error is not None:
            raise self.error
        if not self.authorized or supplied_role_token != "scope-token":
            return None
        return {
            "project_id": project_id,
            "sprint_id": sprint_id,
            "assignment_id": "assignment-1",
            "assignment_kind": "assignment",
            "agent_phone": agent_phone,
            "authorization": {
                "auth_model_version": "prompt-archive-authorization-v1",
                "principal_kind": "managed_agent",
            },
        }


class PromptArchiveApiTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.runtime = FakeArchiveRuntime()
        self.registry = FakeRegistry(self.runtime)
        self.managed = FakeManagedContinuity()

        def operator(request):
            if request.headers.get("x-test-operator") != "allowed":
                raise HTTPException(401, {"error": "hidden"})
            return "operator"

        self.host = SimpleNamespace(
            app=FastAPI(),
            prompt_archive_runtime=self.registry,
            prompt_archive_runtime_metrics=PromptArchiveMetrics(),
            managed_continuity_runtime=self.managed,
            scope_api_operator=operator,
            pending_sprints_request_is_local=lambda request: (
                request.headers.get("x-test-local") == "yes"
            ),
        )
        register_prompt_archive_api(self.host)

    async def request(
        self,
        path,
        *,
        headers=None,
        query_string=b"",
        disconnect_event=None,
        send_started=None,
        send_release=None,
        message_sink=None,
    ):
        messages = message_sink if message_sink is not None else []
        inbound = asyncio.Queue()
        inbound.put_nowait(
            {"type": "http.request", "body": b"", "more_body": False}
        )

        async def receive():
            return await inbound.get()

        async def forward_disconnect():
            await disconnect_event.wait()
            inbound.put_nowait({"type": "http.disconnect"})

        async def send(message):
            messages.append(message)
            if (
                message["type"] == "http.response.body"
                and send_started is not None
            ):
                send_started.set()
                if send_release is not None:
                    await send_release.wait()

        raw_headers = [(b"host", b"127.0.0.1:18032")]
        raw_headers.extend(
            (str(key).lower().encode(), str(value).encode())
            for key, value in (headers or {}).items()
        )
        scope = {
            "type": "http",
            "asgi": {"version": "3.0"},
            "method": "GET",
            "path": path,
            "raw_path": path.encode(),
            "query_string": query_string,
            "root_path": "",
            "scheme": "http",
            "http_version": "1.1",
            "headers": raw_headers,
            "server": ("127.0.0.1", 18032),
            "client": ("127.0.0.1", 35000),
        }
        forwarder = (
            asyncio.create_task(forward_disconnect())
            if disconnect_event is not None
            else None
        )
        try:
            await self.host.app(scope, receive, send)
        finally:
            if forwarder is not None:
                forwarder.cancel()
                await asyncio.gather(forwarder, return_exceptions=True)
        start = next(item for item in messages if item["type"] == "http.response.start")
        body = b"".join(
            item.get("body", b"")
            for item in messages
            if item["type"] == "http.response.body"
        )
        response_headers = {
            key.decode().casefold(): value.decode()
            for key, value in start["headers"]
        }
        return start["status"], body, response_headers

    @property
    def operator_base(self):
        return (
            f"/api/v1/projects/{PROJECT_ID}/sprints/{SPRINT_ID}"
            f"/prompt-archives/{ARCHIVE_ID}"
        )

    @property
    def managed_base(self):
        return (
            f"/api/v1/projects/{PROJECT_ID}/agents/{AGENT_PHONE}/sprints/"
            f"{SPRINT_ID}/prompt-archives/{ARCHIVE_ID}"
        )

    def operator_headers(self):
        return {"X-Test-Local": "yes", "X-Test-Operator": "allowed"}

    async def test_operator_authentication_and_loopback_precede_lookup(self):
        for headers in ({}, {"X-Test-Local": "yes", "X-Test-Operator": "no"}):
            status, body, response_headers = await self.request(
                self.operator_base, headers=headers
            )
            self.assertEqual(status, 404)
            result = json.loads(body)
            self.assertEqual(
                result["detail"]["error"], "PROMPT_ARCHIVE_NOT_FOUND"
            )
            self.assertFalse(result["detail"]["retryable"])
            self.assertNotIn("hidden", body.decode())
            self.assertEqual(response_headers["cache-control"], "private, no-store")
        self.assertEqual(self.registry.calls, [])

        status, body, headers = await self.request(
            self.operator_base, headers=self.operator_headers()
        )
        self.assertEqual(status, 200)
        descriptor = json.loads(body)
        self.assertEqual(descriptor["route_context"], "local_operator")
        self.assertIsNone(descriptor["agent_phone"])
        self.assertEqual(descriptor["scope"]["project_id"], PROJECT_ID)
        self.assertEqual(descriptor["descriptor_url"], self.operator_base)
        self.assertEqual(descriptor["manifest_url"], self.operator_base + "/manifest")
        self.assertEqual(descriptor["content_url"], self.operator_base + "/content")
        self.assertEqual(descriptor["correlation_id"], headers["x-correlation-id"])
        self.assertEqual(self.registry.calls, [PROJECT_ID])

    async def test_managed_authentication_precedes_lookup_and_binds_phone_scope(self):
        status, body, _ = await self.request(self.managed_base)
        self.assertEqual(status, 404)
        self.assertEqual(json.loads(body)["detail"]["error"], "PROMPT_ARCHIVE_NOT_FOUND")
        self.assertEqual(self.registry.calls, [])
        self.assertEqual(self.managed.calls[-1][-1], "")

        status, body, headers = await self.request(
            self.managed_base,
            headers={"X-Nginx-QA-Scope-Token": "scope-token"},
        )
        self.assertEqual(status, 200)
        descriptor = json.loads(body)
        self.assertEqual(descriptor["route_context"], "managed_agent")
        self.assertEqual(descriptor["agent_phone"], AGENT_PHONE)
        self.assertEqual(descriptor["descriptor_url"], self.managed_base)
        self.assertEqual(descriptor["correlation_id"], headers["x-correlation-id"])
        self.assertEqual(
            self.managed.calls[-1],
            (PROJECT_ID, SPRINT_ID, AGENT_PHONE, "scope-token"),
        )
        self.assertEqual(self.registry.calls, [PROJECT_ID])

    async def test_managed_runtime_request_resolver_is_supported(self):
        self.host.managed_continuity_runtime = None
        resolver_calls = []

        def resolver():
            resolver_calls.append(True)
            return self.managed

        self.host.managed_continuity_runtime_for_requests = resolver
        status, body, _ = await self.request(
            self.managed_base,
            headers={"X-Nginx-QA-Scope-Token": "scope-token"},
        )
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body)["agent_phone"], AGENT_PHONE)
        self.assertEqual(resolver_calls, [True])

    async def test_manifest_and_content_are_verified_before_exact_response(self):
        status, body, headers = await self.request(
            self.operator_base + "/manifest", headers=self.operator_headers()
        )
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body), self.runtime.manifest)
        self.assertEqual(self.runtime.manifest_calls, [self.runtime.read])
        self.assertEqual(len(self.runtime.lookup_calls), 1)
        self.assertEqual(self.runtime.legacy_lookup_calls, [])
        self.assertEqual(headers["content-type"], "application/json")
        self.assertEqual(headers["x-content-type-options"], "nosniff")

        status, body, headers = await self.request(
            self.operator_base + "/content", headers=self.operator_headers()
        )
        self.assertEqual(status, 200)
        self.assertEqual(body, CONTENT)
        self.assertEqual(self.runtime.hydrate_calls, [self.runtime.read])
        self.assertEqual(len(self.runtime.lookup_calls), 2)
        self.assertEqual(self.runtime.legacy_lookup_calls, [])
        self.assertEqual(headers["content-type"], "application/json")
        self.assertEqual(
            headers["content-disposition"],
            'attachment; filename="prompt-archive.json"',
        )
        self.assertEqual(headers["x-content-type-options"], "nosniff")
        self.assertEqual(headers["cache-control"], "private, no-store")
        successful_metrics = self.host.prompt_archive_runtime_metrics.snapshot()
        self.assertEqual(successful_metrics.hydration_attempt_count, 1)
        self.assertEqual(successful_metrics.hydration_success_count, 1)
        self.assertEqual(successful_metrics.hydration_failure_count, 0)
        self.assertGreaterEqual(successful_metrics.hydration_latency_ns_total, 0)

        self.runtime.hydrate_error = PromptArchiveStorageError(
            "PROMPT_ARCHIVE_BLOB_CORRUPT"
        )
        status, body, headers = await self.request(
            self.operator_base + "/content", headers=self.operator_headers()
        )
        self.assertEqual(status, 409)
        self.assertNotIn(CONTENT, body)
        self.assertEqual(
            json.loads(body)["detail"]["error"], "PROMPT_ARCHIVE_BLOB_CORRUPT"
        )
        self.assertNotIn("content-disposition", headers)
        failed_metrics = self.host.prompt_archive_runtime_metrics.snapshot()
        self.assertEqual(failed_metrics.hydration_attempt_count, 2)
        self.assertEqual(failed_metrics.hydration_success_count, 1)
        self.assertEqual(failed_metrics.hydration_failure_count, 1)
        self.assertEqual(failed_metrics.corruption_failures, 1)

    async def test_range_and_query_are_stable_400_after_auth_without_lookup(self):
        for headers, query in (
            ({**self.operator_headers(), "Range": "bytes=0-1"}, b""),
            (self.operator_headers(), b"path=secret"),
        ):
            before = len(self.registry.calls)
            status, body, _ = await self.request(
                self.operator_base + "/content",
                headers=headers,
                query_string=query,
            )
            self.assertEqual(status, 400)
            self.assertEqual(
                json.loads(body)["detail"]["error"],
                "PROMPT_ARCHIVE_REQUEST_INVALID",
            )
            self.assertEqual(len(self.registry.calls), before)

    async def test_disconnect_stops_slow_multichunk_worker_and_releases_all_budgets(self):
        self.runtime.hydrate_started = threading.Event()
        self.runtime.hydrate_exited = threading.Event()
        self.runtime.slow_hydration_chunks = 10_000
        disconnect = asyncio.Event()
        messages = []
        operation = asyncio.create_task(
            self.request(
                self.operator_base + "/content",
                headers=self.operator_headers(),
                disconnect_event=disconnect,
                message_sink=messages,
            )
        )
        for _ in range(100):
            if self.runtime.hydrate_started.is_set():
                break
            await asyncio.sleep(0.01)
        self.assertTrue(self.runtime.hydrate_started.is_set())
        admission = self.host.app.state.prompt_archive_hydration_admission
        self.assertEqual(admission._active_global, 0)
        self.assertEqual(admission._reserved_bytes, 0)
        self.assertEqual(admission._working_global, 1)

        disconnect.set()
        try:
            disconnected_result = await asyncio.wait_for(operation, timeout=2)
        except asyncio.CancelledError:
            pass
        else:
            self.fail(f"disconnect returned a response: {disconnected_result!r}")
        self.assertFalse(
            any(message["type"] == "http.response.start" for message in messages)
        )
        self.assertEqual(len(admission._waiters), 0)
        self.assertEqual(admission._active_global, 0)
        self.assertEqual(admission._reserved_bytes, 0)
        self.assertTrue(self.runtime.hydrate_exited.is_set())
        self.assertLess(self.runtime.hydrated_chunks, self.runtime.slow_hydration_chunks)
        self.assertEqual(admission._working_global, 0)
        self.assertEqual(admission._working_bytes, 0)

    async def test_hydration_timeout_joins_worker_without_partial_headers_or_leaks(self):
        self.runtime.hydrate_started = threading.Event()
        self.runtime.hydrate_exited = threading.Event()
        self.runtime.slow_hydration_chunks = 10_000
        admission = self.host.app.state.prompt_archive_hydration_admission
        admission.wall_seconds = 0.03

        status, body, headers = await asyncio.wait_for(
            self.request(
                self.operator_base + "/content",
                headers=self.operator_headers(),
            ),
            timeout=2,
        )

        self.assertEqual(status, 503)
        self.assertEqual(
            json.loads(body)["detail"]["error"],
            "PROMPT_ARCHIVE_UNAVAILABLE",
        )
        self.assertNotIn("content-disposition", headers)
        self.assertTrue(self.runtime.hydrate_started.is_set())
        self.assertTrue(self.runtime.hydrate_exited.is_set())
        self.assertLess(self.runtime.hydrated_chunks, self.runtime.slow_hydration_chunks)
        self.assertEqual(len(admission._waiters), 0)
        self.assertEqual(admission._active_global, 0)
        self.assertEqual(admission._reserved_bytes, 0)
        self.assertEqual(admission._working_global, 0)
        self.assertEqual(admission._working_bytes, 0)

    async def test_hydrated_bytes_remain_accounted_until_body_send_completes(self):
        send_started = asyncio.Event()
        send_release = asyncio.Event()
        operation = asyncio.create_task(
            self.request(
                self.operator_base + "/content",
                headers=self.operator_headers(),
                send_started=send_started,
                send_release=send_release,
            )
        )
        await asyncio.wait_for(send_started.wait(), timeout=2)
        admission = self.host.app.state.prompt_archive_hydration_admission
        self.assertEqual(admission._active_global, 0)
        self.assertEqual(admission._reserved_bytes, 0)
        self.assertEqual(admission._working_global, 1)
        self.assertGreater(admission._working_bytes, 0)

        send_release.set()
        status, body, _headers = await asyncio.wait_for(operation, timeout=2)
        self.assertEqual(status, 200)
        self.assertEqual(body, CONTENT)
        self.assertEqual(admission._working_global, 0)
        self.assertEqual(admission._working_bytes, 0)

    async def test_send_cancellation_releases_hydrated_byte_accounting(self):
        send_started = asyncio.Event()
        send_release = asyncio.Event()
        operation = asyncio.create_task(
            self.request(
                self.operator_base + "/content",
                headers=self.operator_headers(),
                send_started=send_started,
                send_release=send_release,
            )
        )
        await asyncio.wait_for(send_started.wait(), timeout=2)
        admission = self.host.app.state.prompt_archive_hydration_admission
        self.assertEqual(admission._working_global, 1)
        self.assertGreater(admission._working_bytes, 0)

        operation.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await operation
        self.assertEqual(admission._active_global, 0)
        self.assertEqual(admission._reserved_bytes, 0)
        self.assertEqual(admission._working_global, 0)
        self.assertEqual(admission._working_bytes, 0)

    async def test_storage_error_mapping_is_closed_and_busy_has_retry_after(self):
        cases = {
            "PROMPT_ARCHIVE_REQUEST_INVALID": (400, False),
            "PROMPT_ARCHIVE_NOT_FOUND": (404, False),
            "PROMPT_ARCHIVE_VERSION_UNSUPPORTED": (409, False),
            "PROMPT_ARCHIVE_REFERENCE_INVALID": (409, False),
            "PROMPT_ARCHIVE_BLOB_MISSING": (409, False),
            "PROMPT_ARCHIVE_BLOB_CORRUPT": (409, False),
            "PROMPT_ARCHIVE_INTEGRITY_MISMATCH": (409, False),
            "PROMPT_ARCHIVE_GENERATION_CONFLICT": (409, True),
            "PROMPT_ARCHIVE_BUSY": (429, True),
            "PROMPT_ARCHIVE_LIMIT_EXCEEDED": (413, False),
            "PROMPT_ARCHIVE_UNAVAILABLE": (503, True),
        }
        for code, (expected_status, retryable) in cases.items():
            with self.subTest(code=code):
                self.runtime.lookup_error = PromptArchiveStorageError(code)
                status, body, headers = await self.request(
                    self.operator_base, headers=self.operator_headers()
                )
                result = json.loads(body)["detail"]
                self.assertEqual(status, expected_status)
                self.assertEqual(result["error"], code)
                self.assertEqual(result["retryable"], retryable)
                self.assertEqual(result["message"], "Prompt archive is unavailable for this request.")
                self.assertEqual(result["correlation_id"], headers["x-correlation-id"])
                self.assertEqual(headers.get("retry-after"), "1" if code == "PROMPT_ARCHIVE_BUSY" else None)
        self.runtime.lookup_error = None

    async def test_cross_scope_runtime_result_is_generic_not_found(self):
        self.runtime.read.scope = {**self.runtime.read.scope, "sprint_id": "other-sprint"}
        status, body, _ = await self.request(
            self.operator_base, headers=self.operator_headers()
        )
        self.assertEqual(status, 404)
        self.assertEqual(json.loads(body)["detail"]["error"], "PROMPT_ARCHIVE_NOT_FOUND")

    async def test_openapi_declares_six_routes_security_bounds_and_no_auth_leaks(self):
        document = self.host.app.openapi()
        expected_paths = {
            "/api/v1/projects/{project_id}/sprints/{sprint_id}/prompt-archives/{archive_id}",
            "/api/v1/projects/{project_id}/sprints/{sprint_id}/prompt-archives/{archive_id}/manifest",
            "/api/v1/projects/{project_id}/sprints/{sprint_id}/prompt-archives/{archive_id}/content",
            "/api/v1/projects/{project_id}/agents/{agent_phone}/sprints/{sprint_id}/prompt-archives/{archive_id}",
            "/api/v1/projects/{project_id}/agents/{agent_phone}/sprints/{sprint_id}/prompt-archives/{archive_id}/manifest",
            "/api/v1/projects/{project_id}/agents/{agent_phone}/sprints/{sprint_id}/prompt-archives/{archive_id}/content",
        }
        self.assertEqual(set(document["paths"]), expected_paths)
        for path in expected_paths:
            operation = document["paths"][path]["get"]
            self.assertEqual(
                set(operation["responses"]),
                {"200", "400", "404", "409", "413", "429", "503"},
            )
            self.assertNotIn("401", operation["responses"])
            self.assertNotIn("403", operation["responses"])
            self.assertNotIn("422", operation["responses"])
            if "/agents/" in path:
                self.assertEqual(
                    operation["security"],
                    [{"PromptArchiveManagedScopeToken": []}],
                )
            else:
                self.assertEqual(
                    operation["security"],
                    [
                        {"PromptArchiveOperatorAdminToken": []},
                        {
                            "PromptArchiveOperatorSession": [],
                            "PromptArchiveOperatorCsrf": [],
                        },
                    ],
                )
        schemes = document["components"]["securitySchemes"]
        self.assertEqual(
            schemes["PromptArchiveOperatorAdminToken"],
            {
                "type": "apiKey",
                "in": "header",
                "name": "X-Nginx-QA-Scope-Control-Token",
                "description": "Protected local operator administrative token.",
            },
        )
        self.assertEqual(
            schemes["PromptArchiveOperatorSession"]["in"], "cookie"
        )
        self.assertEqual(
            schemes["PromptArchiveOperatorCsrf"]["name"], "X-Nginx-QA-CSRF"
        )
        content = document["paths"][self.operator_base.replace(PROJECT_ID, "{project_id}").replace(SPRINT_ID, "{sprint_id}").replace(ARCHIVE_ID, "{archive_id}") + "/content"]["get"]["responses"]["200"]
        self.assertIn("Content-Disposition", content["headers"])
        self.assertIn("X-Content-Type-Options", content["headers"])
        self.assertIn("Cache-Control", content["headers"])
        self.assertEqual(
            content["content"]["application/json"]["schema"]["type"],
            "string",
        )
        self.assertEqual(
            content["content"]["application/json"]["schema"]["format"],
            "binary",
        )
        profile = document["paths"][
            self.operator_base.replace(PROJECT_ID, "{project_id}")
            .replace(SPRINT_ID, "{sprint_id}")
            .replace(ARCHIVE_ID, "{archive_id}")
            + "/content"
        ]["get"]["x-nginx-qa-prompt-archive-admission"]
        self.assertEqual(profile, self.host.app.state.prompt_archive_hydration_profile)
        self.assertEqual(profile["global_concurrent_hydrations"], 4)
        self.assertEqual(profile["per_project_concurrent_hydrations"], 2)
        self.assertEqual(profile["queued_hydration_requests"], 16)
        self.assertEqual(profile["aggregate_hydration_spool_bytes"], 1024 ** 3)
        self.assertEqual(profile["queue_wait_wall_seconds"], 120.0)
        self.assertEqual(profile["hydration_wall_seconds"], 120.0)
        self.assertEqual(
            profile["order"], "authenticate-admit-lookup-verify-respond"
        )


class HydrationAdmissionTests(unittest.IsolatedAsyncioTestCase):
    async def test_fifo_queue_is_bounded_and_releases_project_capacity(self):
        admission = _HydrationAdmission(
            global_limit=1,
            project_limit=1,
            queue_limit=1,
            spool_bytes=100,
            wall_seconds=1,
        )
        first = await admission.acquire("project-a", 40)
        queued = asyncio.create_task(admission.acquire("project-b", 40))
        await asyncio.sleep(0)
        with self.assertRaises(PromptArchiveStorageError) as raised:
            await admission.acquire("project-c", 1)
        self.assertEqual(raised.exception.code, "PROMPT_ARCHIVE_BUSY")
        await first.release()
        second = await asyncio.wait_for(queued, timeout=1)
        await second.release()
        self.assertEqual(admission._active_global, 0)
        self.assertEqual(admission._reserved_bytes, 0)

    async def test_spool_overflow_is_rejected_before_queue_or_allocation(self):
        admission = _HydrationAdmission(spool_bytes=8)
        with self.assertRaises(PromptArchiveStorageError) as raised:
            await admission.acquire("project-a", 9)
        self.assertEqual(raised.exception.code, "PROMPT_ARCHIVE_BUSY")
        self.assertEqual(len(admission._waiters), 0)

    async def test_queued_wait_has_wall_timeout_and_cleans_every_counter(self):
        admission = _HydrationAdmission(
            global_limit=1,
            project_limit=1,
            queue_limit=1,
            spool_bytes=100,
            wall_seconds=1,
        )
        first = await admission.acquire("project-a", 40)
        with self.assertRaises(PromptArchiveStorageError) as raised:
            await admission.acquire("project-b", 40, wait_seconds=0.01)
        self.assertEqual(raised.exception.code, "PROMPT_ARCHIVE_BUSY")
        self.assertEqual(len(admission._waiters), 0)
        self.assertEqual(admission._active_global, 1)
        self.assertEqual(admission._reserved_bytes, 40)
        await first.release()
        self.assertEqual(admission._active_global, 0)
        self.assertEqual(admission._reserved_bytes, 0)

    async def test_queued_cancellation_removes_waiter_without_capacity_leak(self):
        admission = _HydrationAdmission(
            global_limit=1,
            project_limit=1,
            queue_limit=1,
            spool_bytes=100,
            wall_seconds=1,
        )
        first = await admission.acquire("project-a", 40)
        queued = asyncio.create_task(
            admission.acquire("project-b", 40, wait_seconds=1)
        )
        await asyncio.sleep(0)
        self.assertEqual(len(admission._waiters), 1)
        queued.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await queued
        self.assertEqual(len(admission._waiters), 0)
        self.assertEqual(admission._active_global, 1)
        self.assertEqual(admission._reserved_bytes, 40)
        await first.release()
        self.assertEqual(admission._active_global, 0)
        self.assertEqual(admission._reserved_bytes, 0)

    async def test_running_work_is_separate_and_still_bounds_new_admission(self):
        admission = _HydrationAdmission(
            global_limit=1,
            project_limit=1,
            queue_limit=0,
            spool_bytes=100,
            wall_seconds=1,
        )
        request_lease = await admission.acquire("project-a", 40)
        work_lease = await request_lease.begin_work()
        self.assertEqual(admission._active_global, 0)
        self.assertEqual(admission._reserved_bytes, 0)
        self.assertEqual(admission._working_global, 1)
        self.assertEqual(admission._working_bytes, 40)
        with self.assertRaises(PromptArchiveStorageError) as raised:
            await admission.acquire("project-b", 1, wait_seconds=0.01)
        self.assertEqual(raised.exception.code, "PROMPT_ARCHIVE_BUSY")
        await work_lease.release()
        self.assertEqual(admission._working_global, 0)
        self.assertEqual(admission._working_bytes, 0)


if __name__ == "__main__":
    unittest.main()
