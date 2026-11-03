"""Real scope routes with synthetic in-memory hosts; no main, files or sockets.

No production environment, credential, runtime, store or lifespan is loaded.
Blocking controls deliberately fail if projection runs on the test event loop.
"""
import asyncio
from copy import deepcopy
import json
import threading
from types import SimpleNamespace
import unittest
from unittest import mock
from urllib.parse import urlsplit

from fastapi import FastAPI, Request

from nginx_qa import execution_observability as projection
from nginx_qa import scope_api, scope_workflow
from nginx_qa.legacy_scope_control import LegacyScopeControlError
from tests.test_execution_observability import managed_fixture


PROJECT = "synthetic-read-project"
SPRINT = "synthetic-read-sprint"
BASE = f"/api/v1/projects/{PROJECT}/sprints/{SPRINT}/observability"


def fixture():
    snapshot = managed_fixture()
    snapshot.update(project_id=PROJECT, sprint_id=SPRINT)
    state = snapshot["execution"]
    state["sprint_id"] = SPRINT
    state["assignments"][0].update(status="active", agent_phone="synthetic-role")
    state["scope_workflow"] = {"requests": [{
        "request_id": "synthetic-request", "assignment_id": "work-1",
        "base_scope_revision": 0, "status": "pending", "created_at": "2026-01-01T00:00:00Z",
    }]}
    ledger = {}
    projection.append_execution_checkpoint(ledger, snapshot, recorded_at="2026-01-01T00:00:00Z")
    state["execution_audit"] = ledger
    return state


class SyntheticHost:
    def __init__(self, state=None):
        self.state = deepcopy(state if state is not None else fixture())
        self.lookups = self.reads = 0
        self.lock = threading.Lock()
        self.app = FastAPI()
        self.managed_continuity_runtime = SimpleNamespace(
            store=SimpleNamespace(runtime_states_for_project=self.lookup),
            scope_snapshot=self.snapshot,
        )
        scope_api.register_scope_api(self)

        @self.app.get("/synthetic-heartbeat")
        async def heartbeat():
            return {"alive": True}

    def lookup(self, project_id):
        assert project_id == PROJECT
        with self.lock:
            self.lookups += 1
            return [deepcopy(self.state)]

    def snapshot(self, project_id, sprint_id):
        assert (project_id, sprint_id) == (PROJECT, SPRINT)
        with self.lock:
            self.reads += 1
            return deepcopy(self.state)

    def set_revision(self, revision):
        with self.lock:
            self.state["revision"] = revision


async def request(app, target=BASE, *, headers=()):
    """Exercise FastAPI routing and actual JSON serialization without TCP."""
    parsed = urlsplit(target)
    messages = []

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        messages.append(message)

    await app({
        "type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1",
        "method": "GET", "scheme": "http", "path": parsed.path,
        "raw_path": parsed.path.encode(), "query_string": parsed.query.encode(),
        "root_path": "", "headers": [(b"host", b"testserver"), *headers],
        "server": ("testserver", 80), "client": ("127.0.0.1", 1),
    }, receive, send)
    response = next(message for message in messages if message["type"] == "http.response.start")
    payload = b"".join(message.get("body", b"") for message in messages
                       if message["type"] == "http.response.body")
    return response["status"], json.loads(payload), dict(response["headers"])


class BlockedProjection:
    def __init__(self):
        self.loop = asyncio.get_running_loop()
        self.loop_thread = threading.get_ident()
        self.started = asyncio.Event()
        self.release = threading.Event()
        self.lock = threading.Lock()
        self.active = self.maximum = self.calls = 0
        self.original = scope_api.project_execution

    def __call__(self, *args, **kwargs):
        if threading.get_ident() == self.loop_thread:
            raise AssertionError("Projection blocked the event-loop thread")
        with self.lock:
            self.calls += 1
            self.active += 1
            self.maximum = max(self.maximum, self.active)
        self.loop.call_soon_threadsafe(self.started.set)
        try:
            if not self.release.wait(5):
                raise AssertionError("Test did not release synthetic projection")
            return self.original(*args, **kwargs)
        finally:
            with self.lock:
                self.active -= 1


class ExecutionReadSchedulingTests(unittest.IsolatedAsyncioTestCase):
    async def settle(self):
        # Give runnable clients a chance to reach their gate; no wall-time sleep.
        for _ in range(10):
            await asyncio.sleep(0)

    async def test_http_projection_parity_and_repeated_cursor(self):
        host = SyntheticHost()
        original = deepcopy(host.state)
        expected = projection.project_execution(
            {"project_id": PROJECT, "sprint_id": SPRINT, "execution": original},
            scope_requests=scope_workflow.request_list(original)["requests"],
            checkpoints=original["execution_audit"]["checkpoints"],
        )
        status, result, _ = await request(host.app)
        self.assertEqual(status, 200)
        self.assertEqual(result, expected)
        status, repeated, _ = await request(host.app, BASE + "?cursor=" + result["cursor"])
        self.assertEqual(status, 200)
        self.assertTrue(repeated["unchanged"])
        self.assertEqual({**repeated, "unchanged": False}, result)
        self.assertEqual(host.state, original)
        self.assertEqual(host.reads, 2)

    async def test_notification_only_change_rebuilds_cursor_at_same_revision(self):
        host = SyntheticHost()
        notification = {"request_id": "synthetic-request", "notification_id": "n-1", "status": "pending"}
        events = []
        host.app.state.decision_notifications = SimpleNamespace(store=SimpleNamespace(
            list_for=lambda *args: [deepcopy(notification)], events=lambda nid: deepcopy(events)))
        _, first, _ = await request(host.app)
        notification["status"] = "delivered"
        events.append({"event_id": "delivery-1", "kind": "delivered"})
        status, second, _ = await request(host.app, BASE + "?cursor=" + first["cursor"])
        self.assertEqual(status, 200)
        self.assertEqual(first["execution"]["revision"], second["execution"]["revision"])
        self.assertNotEqual(first["cursor"], second["cursor"])
        self.assertFalse(second["unchanged"])
        self.assertEqual(second["scope_requests"][0]["notification"]["status"], "delivered")
        self.assertEqual(second["notification_events"], events)
        _, third, _ = await request(host.app, BASE + "?cursor=" + second["cursor"])
        self.assertTrue(third["unchanged"])

    async def test_caches_are_per_app_even_for_identical_namespace_and_content(self):
        caches = []
        original = scope_api.HistoricalProjectionCache

        def capture_cache(*args, **kwargs):
            cache = original(*args, **kwargs)
            caches.append(cache)
            return cache

        with mock.patch.object(scope_api, "HistoricalProjectionCache", side_effect=capture_cache):
            first, second = SyntheticHost(), SyntheticHost()
        await request(first.app)
        await request(first.app)
        self.assertEqual(len(caches), 2)
        self.assertGreater(caches[0].info()["hits"], 0)
        self.assertEqual(caches[1].info()["entries"], 0)
        status, _, _ = await request(second.app)
        self.assertEqual(status, 200)
        self.assertEqual(caches[1].info()["hits"], 0)
        self.assertGreater(caches[1].info()["misses"], 0)

    async def test_warm_cache_never_hides_same_label_tamper_and_recovers_after_error(self):
        host = SyntheticHost()
        _, before, _ = await request(host.app)
        saved = deepcopy(host.state)
        point = host.state["execution_audit"]["checkpoints"][0]
        point["snapshot"]["execution"]["revision"] += 100
        status, error, _ = await request(host.app, BASE + "?cursor=" + before["cursor"])
        self.assertEqual(status, 409)
        self.assertEqual(error, {"detail": {"error": "HISTORICAL_SNAPSHOT_UNAVAILABLE"}})
        host.state = saved
        status, recovered, _ = await request(host.app, BASE + "?cursor=" + before["cursor"])
        self.assertEqual(status, 200)
        self.assertTrue(recovered["unchanged"])

    async def test_admission_precedes_snapshot_and_queued_reader_observes_new_state(self):
        host, blocked = SyntheticHost(), BlockedProjection()
        tasks = []
        with mock.patch.object(scope_api, "project_execution", side_effect=blocked):
            try:
                tasks.append(asyncio.create_task(request(host.app)))
                await asyncio.wait_for(blocked.started.wait(), 2)
                tasks.append(asyncio.create_task(request(host.app)))
                await self.settle()
                self.assertEqual((host.lookups, host.reads, blocked.calls), (1, 1, 1))
                host.set_revision(17)
                blocked.release.set()
                first, second = await asyncio.wait_for(asyncio.gather(*tasks), 3)
                self.assertNotEqual(first[1]["execution"]["revision"], second[1]["execution"]["revision"])
                self.assertEqual(second[1]["execution"]["revision"], 17)
                self.assertEqual((host.reads, blocked.maximum), (2, 1))
            finally:
                blocked.release.set()
                await asyncio.gather(*tasks, return_exceptions=True)

    async def test_cancelled_waiter_never_captures_snapshot(self):
        host, blocked = SyntheticHost(), BlockedProjection()
        tasks = []
        with mock.patch.object(scope_api, "project_execution", side_effect=blocked):
            try:
                tasks.append(asyncio.create_task(request(host.app)))
                await asyncio.wait_for(blocked.started.wait(), 2)
                waiter = asyncio.create_task(request(host.app))
                tasks.append(waiter)
                await self.settle()
                waiter.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await waiter
                self.assertEqual((host.lookups, host.reads), (1, 1))
                blocked.release.set()
                self.assertEqual((await asyncio.wait_for(tasks[0], 3))[0], 200)
                self.assertEqual(host.reads, 1)
            finally:
                blocked.release.set()
                await asyncio.gather(*tasks, return_exceptions=True)

    async def test_cancelled_thread_waiter_retains_admission_until_actual_completion(self):
        host, blocked = SyntheticHost(), BlockedProjection()
        tasks = []
        with mock.patch.object(scope_api, "project_execution", side_effect=blocked):
            try:
                first = asyncio.create_task(request(host.app))
                tasks.append(first)
                await asyncio.wait_for(blocked.started.wait(), 2)
                first.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await first
                for _ in range(5):
                    waiter = asyncio.create_task(request(host.app))
                    tasks.append(waiter)
                    await self.settle()
                    waiter.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await waiter
                surviving = asyncio.create_task(request(host.app))
                tasks.append(surviving)
                await self.settle()
                self.assertEqual((host.lookups, host.reads, blocked.calls), (1, 1, 1))
                host.set_revision(21)
                blocked.release.set()
                status, result, _ = await asyncio.wait_for(surviving, 3)
                self.assertEqual(status, 200)
                self.assertEqual(result["execution"]["revision"], 21)
                self.assertEqual((host.reads, blocked.maximum), (2, 1))
            finally:
                blocked.release.set()
                await asyncio.gather(*tasks, return_exceptions=True)

    async def test_worker_exception_releases_admission_for_next_request(self):
        host = SyntheticHost()
        with mock.patch.object(scope_api, "project_execution", side_effect=RuntimeError("synthetic projection failure")):
            with self.assertRaisesRegex(RuntimeError, "synthetic projection failure"):
                await request(host.app)
        status, _, _ = await asyncio.wait_for(request(host.app), 3)
        self.assertEqual(status, 200)
        self.assertEqual(host.reads, 2)

    async def test_cancelled_client_worker_failure_is_retrieved_and_gate_recovers(self):
        host, blocked = SyntheticHost(), BlockedProjection()
        loop = asyncio.get_running_loop()
        previous_handler = loop.get_exception_handler()
        unhandled = []
        loop.set_exception_handler(lambda current_loop, context: unhandled.append(context))
        tasks = []

        def fails_once(*args, **kwargs):
            result = blocked(*args, **kwargs)
            if blocked.calls == 1:
                raise RuntimeError("Synthetic abandoned-worker failure")
            return result

        with mock.patch.object(scope_api, "project_execution", side_effect=fails_once):
            try:
                first = asyncio.create_task(request(host.app))
                tasks.append(first)
                await asyncio.wait_for(blocked.started.wait(), 2)
                first.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await first
                next_client = asyncio.create_task(request(host.app))
                tasks.append(next_client)
                await self.settle()
                self.assertEqual(host.reads, 1)
                blocked.release.set()
                self.assertEqual((await asyncio.wait_for(next_client, 3))[0], 200)
                await self.settle()
                self.assertEqual(unhandled, [])
                self.assertEqual(blocked.maximum, 1)
            finally:
                blocked.release.set()
                await asyncio.gather(*tasks, return_exceptions=True)
                loop.set_exception_handler(previous_handler)

    async def test_response_serialization_runs_off_event_loop(self):
        host = SyntheticHost()
        loop_thread = threading.get_ident()
        rendering_threads = []
        original = scope_api.JSONResponse

        def render(*args, **kwargs):
            rendering_threads.append(threading.get_ident())
            return original(*args, **kwargs)

        with mock.patch.object(scope_api, "JSONResponse", side_effect=render):
            status, _, _ = await request(host.app)
        self.assertEqual(status, 200)
        self.assertEqual(len(rendering_threads), 1)
        self.assertNotEqual(rendering_threads[0], loop_thread)

    async def test_heartbeat_and_session_read_remain_responsive_during_projection(self):
        host, blocked = SyntheticHost(), BlockedProjection()
        tasks = []
        with mock.patch.object(scope_api, "project_execution", side_effect=blocked):
            try:
                tasks.append(asyncio.create_task(request(host.app)))
                await asyncio.wait_for(blocked.started.wait(), 2)
                for _ in range(3):
                    status, heartbeat, _ = await asyncio.wait_for(
                        request(host.app, "/synthetic-heartbeat"), 1)
                    self.assertEqual((status, heartbeat), (200, {"alive": True}))
                status, session, headers = await asyncio.wait_for(
                    request(host.app, "/api/v1/operator/session"), 1)
                self.assertEqual(status, 200)
                self.assertFalse(session["authenticated"])
                self.assertIsNone(session["csrf_token"])
                self.assertEqual(headers[b"cache-control"], b"no-store")
                self.assertFalse(tasks[0].done())
                blocked.release.set()
                self.assertEqual((await asyncio.wait_for(tasks[0], 3))[0], 200)
            finally:
                blocked.release.set()
                await asyncio.gather(*tasks, return_exceptions=True)

    async def test_warm_observability_does_not_bypass_protected_role_check(self):
        host = SyntheticHost()
        await request(host.app)
        private = Request({"type": "http", "method": "GET", "path": "/synthetic",
                           "headers": [(scope_api.ROLE_TOKEN_HEADER.lower().encode(), b"wrong-role")]})
        denied = LegacyScopeControlError("SCOPE_CONTROL_FORBIDDEN", "Synthetic role denied", status_code=403)
        with mock.patch.object(scope_api, "verify_role_token", side_effect=denied) as verify:
            with self.assertRaises(scope_api.HTTPException) as caught:
                await host.scope_api_managed_effective(PROJECT, "work-1", private)
        self.assertEqual(caught.exception.status_code, 403)
        verify.assert_called_once_with("synthetic-role", "wrong-role")


class ExecutionReadLifecycleTests(unittest.TestCase):
    def test_same_registered_app_supports_contended_reads_on_successive_loops(self):
        host = SyntheticHost()

        async def lifecycle():
            blocked = BlockedProjection()
            tasks = []
            reads_before = host.reads
            with mock.patch.object(scope_api, "project_execution", side_effect=blocked):
                try:
                    tasks.append(asyncio.create_task(request(host.app)))
                    await asyncio.wait_for(blocked.started.wait(), 2)
                    tasks.append(asyncio.create_task(request(host.app)))
                    for _ in range(10):
                        await asyncio.sleep(0)
                    self.assertEqual(host.reads, reads_before + 1)
                    blocked.release.set()
                    results = await asyncio.wait_for(asyncio.gather(*tasks), 3)
                    self.assertEqual([result[0] for result in results], [200, 200])
                    self.assertEqual(blocked.maximum, 1)
                    return results[0][1]
                finally:
                    blocked.release.set()
                    await asyncio.gather(*tasks, return_exceptions=True)

        first = asyncio.run(lifecycle())
        second = asyncio.run(lifecycle())
        self.assertEqual(first, second)
        self.assertEqual(host.reads, 4)

    def test_active_gate_rejects_cross_loop_entry_without_starting_operation(self):
        gate = scope_api.ExecutionReadGate()
        entered_cross_loop_operation = []

        async def lifecycle():
            entered, release = asyncio.Event(), asyncio.Event()

            async def held():
                entered.set()
                await release.wait()
                return "original-owner"

            def forbidden_operation():
                entered_cross_loop_operation.append(True)
                return asyncio.sleep(0)

            first = asyncio.create_task(gate.run(held))
            try:
                await asyncio.wait_for(entered.wait(), 2)
                with self.assertRaisesRegex(RuntimeError, "active on another event loop"):
                    await asyncio.to_thread(
                        lambda: asyncio.run(gate.run(forbidden_operation)))
                self.assertEqual(entered_cross_loop_operation, [])
                self.assertFalse(first.done())
            finally:
                release.set()
                self.assertEqual(await first, "original-owner")

        asyncio.run(lifecycle())
        self.assertEqual(asyncio.run(gate.run(lambda: asyncio.sleep(0, result="next-owner"))),
                         "next-owner")

    def test_cancelled_worker_and_waiter_finish_before_idle_loop_rebind(self):
        gate = scope_api.ExecutionReadGate()

        async def cancelled_lifecycle():
            loop = asyncio.get_running_loop()
            entered = asyncio.Event()
            release = threading.Event()
            tasks = []

            def held_worker():
                loop.call_soon_threadsafe(entered.set)
                if not release.wait(5):
                    raise AssertionError("Lifecycle test did not release worker")
                return "finished-worker"

            try:
                caller = asyncio.create_task(gate.run(lambda: asyncio.to_thread(held_worker)))
                tasks.append(caller)
                await asyncio.wait_for(entered.wait(), 2)
                caller.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await caller
                waiter = asyncio.create_task(gate.run(lambda: asyncio.sleep(0)))
                tasks.append(waiter)
                await asyncio.sleep(0)
                waiter.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await waiter
                survivor = asyncio.create_task(gate.run(lambda: asyncio.sleep(0, result="drained")))
                tasks.append(survivor)
                await asyncio.sleep(0)
                self.assertFalse(survivor.done())
                release.set()
                self.assertEqual(await asyncio.wait_for(survivor, 3), "drained")
            finally:
                release.set()
                await asyncio.gather(*tasks, return_exceptions=True)

        asyncio.run(cancelled_lifecycle())
        self.assertEqual(asyncio.run(gate.run(lambda: asyncio.sleep(0, result="new-loop"))),
                         "new-loop")


if __name__ == "__main__":
    unittest.main()
