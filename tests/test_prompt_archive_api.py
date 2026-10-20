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
STATE_DOCUMENT = {
    "schema_version": 1,
    "kind": "prompt_archive_state_view",
    "media_type": "application/vnd.nginx-qa.prompt-archive-state-view-v1+json",
    "scope": {
        "project_id": PROJECT_ID,
        "sprint_id": SPRINT_ID,
        "root_archive_id": ARCHIVE_ID,
        "head_archive_id": ARCHIVE_ID,
        "head_response_id": RESPONSE_ID,
        "occurrence_count": 1,
    },
    "integrity": {
        "algorithm": "sha256",
        "chain_sha256": "5" * 64,
        "aggregate_hydrated_bytes": len(CONTENT),
    },
    "occurrences": [],
    "state": {"current": {}, "history": []},
}
STATE_CONTENT = json.dumps(
    STATE_DOCUMENT,
    ensure_ascii=False,
    allow_nan=False,
    sort_keys=True,
    separators=(",", ":"),
).encode("utf-8")


class DeterministicFloorClock:
    def __init__(self):
        self.value = 0.0
        self.sleeps = []

    def monotonic(self):
        return self.value

    async def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.value += seconds

    def advance(self, seconds):
        self.value += seconds


class FakeArchiveRuntime:
    def __init__(self, project_id=PROJECT_ID):
        self.project_id = project_id
        self.lookup_calls = []
        self.legacy_lookup_calls = []
        self.manifest_calls = []
        self.hydrate_calls = []
        self.state_calls = []
        self.lookup_error = None
        self.hydrate_error = None
        self.state_error = None
        self.state_hydrate_started = None
        self.state_hydrate_exited = None
        self.slow_state_hydration_chunks = 0
        self.state_hydrated_chunks = 0
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

    def hydrate_state_view(
        self,
        *,
        sprint_id,
        archive_id,
        max_output_bytes=None,
        cancellation=None,
    ):
        self.state_calls.append((sprint_id, archive_id, max_output_bytes))
        if self.state_hydrate_started is not None:
            self.state_hydrate_started.set()
        try:
            for _index in range(self.slow_state_hydration_chunks):
                if cancellation is not None:
                    cancellation.check()
                self.state_hydrated_chunks += 1
                threading.Event().wait(0.005)
            if self.state_error is not None:
                raise self.state_error
            if cancellation is not None:
                cancellation.check()
            return STATE_CONTENT
        finally:
            if self.state_hydrate_exited is not None:
                self.state_hydrate_exited.set()


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
        self.assertEqual(descriptor["state_url"], self.operator_base + "/state")
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
        self.assertEqual(descriptor["state_url"], self.managed_base + "/state")
        self.assertEqual(descriptor["correlation_id"], headers["x-correlation-id"])
        self.assertEqual(
            self.managed.calls[-1],
            (PROJECT_ID, SPRINT_ID, AGENT_PHONE, "scope-token"),
        )
        self.assertEqual(self.registry.calls, [PROJECT_ID])

    async def test_managed_not_found_paths_share_deterministic_floor_without_unauthorized_lookup(self):
        clock = DeterministicFloorClock()
        self.host = SimpleNamespace(
            app=FastAPI(),
            prompt_archive_runtime=self.registry,
            prompt_archive_runtime_metrics=PromptArchiveMetrics(),
            managed_continuity_runtime=self.managed,
            scope_api_operator=lambda _request: "operator",
            pending_sprints_request_is_local=lambda _request: True,
            prompt_archive_managed_not_found_floor_seconds=1.0,
            prompt_archive_managed_not_found_monotonic=clock.monotonic,
            prompt_archive_managed_not_found_sleep=clock.sleep,
        )
        register_prompt_archive_api(self.host)

        normalized_details = []
        elapsed = []

        async def request_not_found(*, headers=None):
            started = clock.monotonic()
            status, body, response_headers = await self.request(
                self.managed_base,
                headers=headers,
            )
            elapsed.append(clock.monotonic() - started)
            self.assertEqual(status, 404)
            detail = json.loads(body)["detail"]
            self.assertEqual(
                detail["correlation_id"], response_headers["x-correlation-id"]
            )
            normalized_details.append(
                {
                    key: value
                    for key, value in detail.items()
                    if key != "correlation_id"
                }
            )
            self.assertNotIn("scope-token", body.decode())
            self.assertNotIn("timing-secret-canary", body.decode())

        # Missing and invalid tokens, plus a stale binding decision, must stop
        # before the archive registry or locator is touched.
        await request_not_found()
        await request_not_found(
            headers={"X-Nginx-QA-Scope-Token": "timing-secret-canary"}
        )
        self.managed.authorized = False
        await request_not_found(
            headers={"X-Nginx-QA-Scope-Token": "scope-token"}
        )
        self.managed.authorized = True

        # A cross-scope authorization snapshot is also denied before lookup.
        exact_authorize = self.managed.authorize_prompt_archive_read

        def cross_scope_authorize(*args, **kwargs):
            snapshot = exact_authorize(*args, **kwargs)
            assert snapshot is not None
            return {**snapshot, "sprint_id": "msv1-cross-scope"}

        self.managed.authorize_prompt_archive_read = cross_scope_authorize
        await request_not_found(
            headers={"X-Nginx-QA-Scope-Token": "scope-token"}
        )
        self.managed.authorize_prompt_archive_read = exact_authorize

        self.assertEqual(self.registry.calls, [])
        self.assertEqual(self.runtime.lookup_calls, [])

        # Only the authorized absent case may reach the bounded locator lookup.
        # Simulate lookup work consuming part of the floor without sleeping in
        # real time; the response must wait only the exact remainder.
        exact_lookup = self.runtime.lookup_archive_metadata

        def absent_lookup(**kwargs):
            clock.advance(0.4)
            return exact_lookup(**kwargs)

        self.runtime.lookup_archive_metadata = absent_lookup
        self.runtime.lookup_error = PromptArchiveStorageError(
            "PROMPT_ARCHIVE_NOT_FOUND"
        )
        await request_not_found(
            headers={"X-Nginx-QA-Scope-Token": "scope-token"}
        )

        self.assertEqual(self.registry.calls, [PROJECT_ID])
        self.assertEqual(
            self.runtime.lookup_calls,
            [(SPRINT_ID, ARCHIVE_ID)],
        )
        self.assertTrue(
            all(item == normalized_details[0] for item in normalized_details)
        )
        for measured in elapsed:
            self.assertAlmostEqual(measured, 1.0)
        self.assertEqual(len(clock.sleeps), 5)
        for measured, expected in zip(
            clock.sleeps,
            [1.0, 1.0, 1.0, 1.0, 0.6],
            strict=True,
        ):
            self.assertAlmostEqual(measured, expected)
        self.assertEqual(
            self.host.app.state.prompt_archive_managed_not_found_floor_seconds,
            1.0,
        )

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

    async def test_state_view_is_authorized_bounded_verified_json_attachment(self):
        status, body, _ = await self.request(self.managed_base + "/state")
        self.assertEqual(status, 404)
        self.assertEqual(json.loads(body)["detail"]["error"], "PROMPT_ARCHIVE_NOT_FOUND")
        self.assertEqual(self.registry.calls, [])
        self.assertEqual(self.runtime.state_calls, [])

        status, body, headers = await self.request(
            self.operator_base + "/state", headers=self.operator_headers()
        )
        self.assertEqual(status, 200)
        self.assertEqual(body, STATE_CONTENT)
        self.assertEqual(json.loads(body), STATE_DOCUMENT)
        self.assertEqual(
            self.runtime.state_calls,
            [(SPRINT_ID, ARCHIVE_ID, 256 * 1024 * 1024)],
        )
        self.assertEqual(headers["content-type"], "application/json")
        self.assertEqual(
            headers["content-disposition"],
            'attachment; filename="prompt-archive-state.json"',
        )
        self.assertEqual(headers["cache-control"], "private, no-store")
        self.assertEqual(headers["x-content-type-options"], "nosniff")

        status, body, _ = await self.request(
            self.managed_base + "/state",
            headers={"X-Nginx-QA-Scope-Token": "scope-token"},
        )
        self.assertEqual(status, 200)
        self.assertEqual(body, STATE_CONTENT)
        self.assertEqual(
            self.managed.calls[-1],
            (PROJECT_ID, SPRINT_ID, AGENT_PHONE, "scope-token"),
        )

        # A non-canonical runtime result fails before any attachment headers or
        # body bytes from that result can be committed.
        original = self.runtime.hydrate_state_view
        self.runtime.hydrate_state_view = lambda **_kwargs: (
            json.dumps(STATE_DOCUMENT, indent=2).encode("utf-8")
        )
        try:
            status, body, headers = await self.request(
                self.operator_base + "/state", headers=self.operator_headers()
            )
        finally:
            self.runtime.hydrate_state_view = original
        self.assertEqual(status, 409)
        self.assertEqual(
            json.loads(body)["detail"]["error"],
            "PROMPT_ARCHIVE_INTEGRITY_MISMATCH",
        )
        self.assertNotIn("content-disposition", headers)

        cross_scope = json.loads(json.dumps(STATE_DOCUMENT))
        cross_scope["scope"]["sprint_id"] = "sprint-other"
        cross_scope_bytes = json.dumps(
            cross_scope,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        self.runtime.hydrate_state_view = lambda **_kwargs: cross_scope_bytes
        try:
            status, body, headers = await self.request(
                self.managed_base + "/state",
                headers={"X-Nginx-QA-Scope-Token": "scope-token"},
            )
        finally:
            self.runtime.hydrate_state_view = original
        self.assertEqual(status, 404)
        self.assertEqual(
            json.loads(body)["detail"]["error"],
            "PROMPT_ARCHIVE_NOT_FOUND",
        )
        self.assertNotIn("content-disposition", headers)

    async def test_state_view_timeout_cooperatively_stops_worker_without_headers_or_leaks(self):
        self.runtime.state_hydrate_started = threading.Event()
        self.runtime.state_hydrate_exited = threading.Event()
        self.runtime.slow_state_hydration_chunks = 10_000
        admission = self.host.app.state.prompt_archive_hydration_admission
        admission.wall_seconds = 0.03

        status, body, headers = await asyncio.wait_for(
            self.request(
                self.operator_base + "/state",
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
        self.assertTrue(self.runtime.state_hydrate_started.is_set())
        self.assertTrue(self.runtime.state_hydrate_exited.is_set())
        self.assertLess(
            self.runtime.state_hydrated_chunks,
            self.runtime.slow_state_hydration_chunks,
        )
        self.assertEqual(len(admission._waiters), 0)
        self.assertEqual(admission._active_global, 0)
        self.assertEqual(admission._reserved_bytes, 0)
        self.assertEqual(admission._working_global, 0)
        self.assertEqual(admission._working_bytes, 0)

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

    async def test_openapi_declares_eight_routes_security_bounds_and_no_auth_leaks(self):
        document = self.host.app.openapi()
        expected_paths = {
            "/api/v1/projects/{project_id}/sprints/{sprint_id}/prompt-archives/{archive_id}",
            "/api/v1/projects/{project_id}/sprints/{sprint_id}/prompt-archives/{archive_id}/manifest",
            "/api/v1/projects/{project_id}/sprints/{sprint_id}/prompt-archives/{archive_id}/content",
            "/api/v1/projects/{project_id}/sprints/{sprint_id}/prompt-archives/{archive_id}/state",
            "/api/v1/projects/{project_id}/agents/{agent_phone}/sprints/{sprint_id}/prompt-archives/{archive_id}",
            "/api/v1/projects/{project_id}/agents/{agent_phone}/sprints/{sprint_id}/prompt-archives/{archive_id}/manifest",
            "/api/v1/projects/{project_id}/agents/{agent_phone}/sprints/{sprint_id}/prompt-archives/{archive_id}/content",
            "/api/v1/projects/{project_id}/agents/{agent_phone}/sprints/{sprint_id}/prompt-archives/{archive_id}/state",
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
        content_schema = content["content"]["application/json"]["schema"]
        self.assertEqual(len(content_schema["oneOf"]), 2)
        self.assertEqual(
            content_schema["oneOf"][0]["properties"]["kind"]["const"],
            "prompt_archive_occurrence_document",
        )
        compact_in_content = content_schema["oneOf"][0]["properties"][
            "execution_response"
        ]["allOf"][0]
        descriptor_in_compact = compact_in_content["properties"]["prompt_archive"]
        self.assertEqual(descriptor_in_compact["additionalProperties"], False)
        self.assertTrue(
            {
                "descriptor_url",
                "manifest_url",
                "content_url",
                "state_url",
                "route_context",
                "scope",
                "integrity",
                "created_at",
            }.issubset(descriptor_in_compact["required"])
        )
        self.assertEqual(content_schema["oneOf"][1]["type"], "object")
        self.assertEqual(
            content_schema["oneOf"][1]["not"],
            {"required": ["kind"]},
        )
        state = document["paths"][
            self.operator_base.replace(PROJECT_ID, "{project_id}")
            .replace(SPRINT_ID, "{sprint_id}")
            .replace(ARCHIVE_ID, "{archive_id}")
            + "/state"
        ]["get"]["responses"]["200"]
        self.assertEqual(
            state["content"]["application/json"]["schema"]["type"],
            "object",
        )
        self.assertEqual(
            state["content"]["application/json"]["schema"]["properties"][
                "kind"
            ]["const"],
            "prompt_archive_state_view",
        )
        self.assertEqual(
            state["headers"]["Content-Disposition"]["schema"]["const"],
            'attachment; filename="prompt-archive-state.json"',
        )
        profile = document["paths"][
            self.operator_base.replace(PROJECT_ID, "{project_id}")
            .replace(SPRINT_ID, "{sprint_id}")
            .replace(ARCHIVE_ID, "{archive_id}")
            + "/content"
        ]["get"]["x-nginx-qa-prompt-archive-admission"]
        state_profile = document["paths"][
            self.operator_base.replace(PROJECT_ID, "{project_id}")
            .replace(SPRINT_ID, "{sprint_id}")
            .replace(ARCHIVE_ID, "{archive_id}")
            + "/state"
        ]["get"]["x-nginx-qa-prompt-archive-admission"]
        self.assertEqual(state_profile, profile)
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
