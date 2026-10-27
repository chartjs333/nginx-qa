"""Synthetic scope-probe scheduling regressions; never import the server host.

The host double models the existing async writer gates and a contended file
lock.  It fails immediately at the file-lock boundary instead of consuming the
real lock's timeout.  Scope projection is a stub: these tests exercise routing,
scheduling and authentication, not replace the v2 history-validation tests.
"""

import asyncio
from contextlib import contextmanager
from copy import deepcopy
import json
import os
from types import SimpleNamespace
import unittest
from unittest import mock

from fastapi import FastAPI, HTTPException, Request

from nginx_qa import scope_api
from nginx_qa.legacy_scope_control import LegacyScopeControlError


PHONE = "9917"
TOKEN = "synthetic-scope-test-token-" + "x" * 40
TOKEN_ENV = "NGINX_QA_SCOPE_CONTROL_ROLE_TOKEN_" + PHONE
ASSIGNMENT = "synthetic-assignment"
PROJECT = "synthetic-project"


def execution(mode="sequential"):
    state = {
        "mode": mode,
        "sprint_id": "synthetic-sprint",
        "revision": 7,
        "assignments": [{
            "assignment_id": ASSIGNMENT,
            "agent_phone": PHONE,
            "agent_id": "synthetic-role",
            "status": "active",
            "delivery_status": "delivered",
            "issued_task": {"task_id": "synthetic-task"},
        }],
    }
    if mode == "managed":
        state["sprint_type"] = "managed_workspace_v1"
    return state


def request(token=TOKEN, payload=None):
    encoded = json.dumps(payload or {}).encode("utf-8")

    async def receive():
        return {"type": "http.request", "body": encoded, "more_body": False}

    headers = [] if token is None else [
        (scope_api.ROLE_TOKEN_HEADER.lower().encode("ascii"), token.encode("ascii"))
    ]
    return Request({"type": "http", "method": "POST", "path": "/synthetic",
                    "headers": headers}, receive)


class SyntheticHost:
    PROJECT_AGENT_ASSIGNMENT_KEY = "execution"
    PROJECTS_KEY = "projects"

    def __init__(self, state, managed_states=()):
        self.app = FastAPI()
        self.loop = asyncio.get_running_loop()
        self.git_config_lock = asyncio.Lock()
        self.agents_lock = asyncio.Lock()
        self.group_task_submission_lock = asyncio.Lock()
        self.gate_entered = asyncio.Event()
        self.file_lock_attempted = asyncio.Event()
        self.writer_entered = asyncio.Event()
        self.release_writer = asyncio.Event()
        self.writer_active = False
        self.gate_calls = self.dispatches = self.file_lock_calls = self.reads = 0
        self.writes = []
        self.config = {"projects": {"synthetic": {"execution": deepcopy(state)}}}
        self.parse_strict_json_object = json.loads
        self.utc_now = lambda: "2026-01-01T00:00:00Z"
        self.managed_continuity_runtime = None
        if managed_states:
            self.managed_continuity_runtime = SimpleNamespace(
                store=SimpleNamespace(runtime_states_for_project=mock.Mock(
                    return_value=deepcopy(list(managed_states)))))
        scope_api.register_scope_api(self)

    async def run_group_write_transaction(self, transaction, *args):
        # Same gate ordering as main.run_group_write_transaction, without main.
        self.gate_calls += 1
        self.gate_entered.set()
        async with self.git_config_lock:
            async with self.agents_lock:
                self.dispatches += 1
                return await asyncio.to_thread(transaction, *args)

    async def hold_writer(self):
        async with self.git_config_lock:
            async with self.agents_lock:
                self.writer_active = True
                self.writer_entered.set()
                try:
                    await self.release_writer.wait()
                finally:
                    self.writer_active = False

    @contextmanager
    def git_config_file_lock(self):
        self.file_lock_calls += 1
        self.loop.call_soon_threadsafe(self.file_lock_attempted.set)
        if self.writer_active:
            raise TimeoutError("Synthetic file-lock contention: async gate bypassed")
        yield

    def read_git_config_file(self):
        self.reads += 1
        return deepcopy(self.config)

    def project_for_group_api(self, config, project_id):
        if project_id != PROJECT:
            raise AssertionError("Unexpected synthetic project")
        return "synthetic", None, config["projects"]["synthetic"], None

    def write_git_config_file(self, config):
        self.writes.append(deepcopy(config))
        self.config = deepcopy(config)


class ScopeProbeSerializationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        # This process sees synthetic credentials only, including known_secret_values.
        environment = mock.patch.dict(os.environ, {TOKEN_ENV: TOKEN}, clear=True)
        environment.start()
        self.addCleanup(environment.stop)
        self.projection = mock.patch.object(scope_api.adapters, "effective_scope_snapshot",
            return_value={"requires_scope_ack": True,
                          "instruction_precedence": "synthetic-effective-first",
                          "scope_context": {"synthetic": "context"}, "mutated": False})
        self.snapshot = self.projection.start()
        self.addCleanup(self.projection.stop)

    async def probe(self, host, **kwargs):
        return await host.scope_api_managed_effective(
            PROJECT, ASSIGNMENT, kwargs.pop("request", request()), **kwargs)

    async def wait_for_probe_boundary(self, host):
        waiters = [asyncio.create_task(host.gate_entered.wait()),
                   asyncio.create_task(host.file_lock_attempted.wait())]
        try:
            done, _ = await asyncio.wait(waiters, timeout=2,
                                         return_when=asyncio.FIRST_COMPLETED)
            self.assertTrue(done, "Probe reached neither scheduling boundary")
        finally:
            for waiter in waiters:
                waiter.cancel()
            await asyncio.gather(*waiters, return_exceptions=True)

    async def test_probe_waits_for_writer_before_executor_or_file_lock(self):
        host = SyntheticHost(execution())
        writer = asyncio.create_task(host.hold_writer())
        await host.writer_entered.wait()
        probe = asyncio.create_task(self.probe(host))
        try:
            await self.wait_for_probe_boundary(host)
            self.assertTrue(host.gate_entered.is_set(),
                            "Legacy probe bypassed the host async writer gate")
            self.assertFalse(probe.done())
            self.assertEqual((host.dispatches, host.file_lock_calls, host.reads), (0, 0, 0))
            host.release_writer.set()
            self.assertIsNone(await asyncio.wait_for(probe, timeout=2))
            self.assertEqual((host.gate_calls, host.dispatches, host.reads), (1, 1, 1))
            self.assertEqual(host.writes, [])
        finally:
            host.release_writer.set()
            await asyncio.gather(writer, probe, return_exceptions=True)

    async def test_cancelled_waiter_never_dispatches_or_touches_registry(self):
        host = SyntheticHost(execution())
        writer = asyncio.create_task(host.hold_writer())
        await host.writer_entered.wait()
        probe = asyncio.create_task(self.probe(host))
        try:
            await self.wait_for_probe_boundary(host)
            self.assertTrue(host.gate_entered.is_set(),
                            "Cancellation must occur before executor/file-lock dispatch")
            probe.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await probe
            host.release_writer.set()
            await writer
            self.assertEqual((host.dispatches, host.file_lock_calls, host.reads), (0, 0, 0))
            self.assertEqual(host.writes, [])
        finally:
            host.release_writer.set()
            await asyncio.gather(writer, probe, return_exceptions=True)

    async def test_sequential_probe_returns_none_without_projecting_or_writing(self):
        host = SyntheticHost(execution())
        self.assertIsNone(await self.probe(host))
        self.assertEqual((host.gate_calls, host.dispatches, host.reads), (1, 1, 1))
        self.snapshot.assert_not_called()
        self.assertEqual(host.writes, [])

    async def test_parallel_probe_preserves_projection_and_response(self):
        state = execution("parallel")
        host = SyntheticHost(state)
        response = await self.probe(host)
        self.snapshot.assert_called_once_with(state, ASSIGNMENT)
        self.assertEqual(response, {
            "requires_scope_ack": True, "instruction_precedence": "synthetic-effective-first",
            "scope_context": {"synthetic": "context"}, "mutated": False,
            "project_id": PROJECT, "assignment_id": ASSIGNMENT, "acknowledged": False,
            "precedence": {"authoritative": "effective", "historical": "issued",
                           "rule": "synthetic-effective-first"},
        })
        self.assertEqual((host.gate_calls, host.dispatches, host.reads), (1, 1, 1))
        self.assertEqual(host.writes, [])

    async def test_managed_assignment_bypasses_legacy_gate_and_registry(self):
        state = execution("managed")
        host = SyntheticHost(execution(), managed_states=[state])
        response = await self.probe(host)
        self.assertEqual(response["assignment_id"], ASSIGNMENT)
        self.snapshot.assert_called_once_with(state, ASSIGNMENT)
        host.managed_continuity_runtime.store.runtime_states_for_project.assert_called_once_with(PROJECT)
        self.assertEqual((host.gate_calls, host.dispatches, host.file_lock_calls, host.reads),
                         (0, 0, 0, 0))
        self.assertEqual(host.writes, [])

    async def test_managed_and_parallel_tokens_remain_fail_closed(self):
        for mode in ("managed", "parallel"):
            for supplied, configured, expected_status, error in (
                (None, TOKEN, 403, "SCOPE_CONTROL_FORBIDDEN"),
                ("incorrect", TOKEN, 403, "SCOPE_CONTROL_FORBIDDEN"),
                (TOKEN, "", 503, "SCOPE_CONTROL_CREDENTIAL_UNAVAILABLE"),
            ):
                with self.subTest(mode=mode, supplied=supplied, configured=bool(configured)):
                    state = execution(mode)
                    host = SyntheticHost(state, managed_states=[state] if mode == "managed" else [])
                    with mock.patch.dict(os.environ, {TOKEN_ENV: configured}):
                        with self.assertRaises(HTTPException) as caught:
                            await self.probe(host, request=request(token=supplied))
                    self.assertEqual(caught.exception.status_code, expected_status)
                    self.assertEqual(caught.exception.detail, {"error": error})
                    self.assertEqual(host.writes, [])
        self.snapshot.assert_not_called()

    async def test_parallel_ack_revalidates_role_inside_ordinary_transaction(self):
        state = execution("parallel")
        changed = {**deepcopy(state), "revision": 8}
        context = {"synthetic": "exact-context"}
        response = {"acknowledged": True, "graph_advanced": False}
        host = SyntheticHost(state)
        with mock.patch.object(scope_api.adapters, "acknowledge_scope",
                               return_value=(changed, response)) as acknowledge:
            with mock.patch.object(scope_api, "verify_role_token",
                                   wraps=scope_api.verify_role_token) as verify:
                result = await self.probe(host, acknowledge=True,
                    request=request(payload={"schema_version": 1, "scope_context": context}))
        self.assertEqual(result, response)
        acknowledge.assert_called_once_with(state, ASSIGNMENT, context, "2026-01-01T00:00:00Z")
        self.assertEqual(verify.call_args_list, [mock.call(PHONE, TOKEN), mock.call(PHONE, TOKEN)])
        self.assertEqual((host.gate_calls, host.dispatches, host.file_lock_calls, host.reads),
                         (1, 1, 2, 2))
        self.assertEqual(len(host.writes), 1)
        self.assertEqual(host.config["projects"]["synthetic"]["execution"], changed)
        self.snapshot.assert_not_called()

    async def test_managed_ack_keeps_runtime_mutation_routing(self):
        state = execution("managed")
        host = SyntheticHost(execution(), managed_states=[state])
        context = {"synthetic": "exact-context"}
        response = {"acknowledged": True, "graph_advanced": False}
        host.managed_continuity_runtime.scope_mutate = mock.Mock(
            side_effect=lambda project, sprint, callback: callback(deepcopy(state)))
        with mock.patch.object(scope_api.adapters, "acknowledge_scope",
                               return_value=(state, response)) as acknowledge:
            result = await self.probe(host, acknowledge=True,
                request=request(payload={"schema_version": 1, "scope_context": context}))
        self.assertEqual(result, response)
        acknowledge.assert_called_once_with(state, ASSIGNMENT, context, "2026-01-01T00:00:00Z")
        self.assertEqual(host.managed_continuity_runtime.scope_mutate.call_args.args[:2],
                         (PROJECT, "synthetic-sprint"))
        self.assertEqual((host.gate_calls, host.file_lock_calls, host.reads), (0, 0, 0))
        self.assertEqual(host.writes, [])

    async def test_parallel_ack_invalid_body_and_stale_context_do_not_write(self):
        for payload, expected_status, code in (
            ({"schema_version": 2, "scope_context": {}}, 400, "SCOPE_ACK_INVALID"),
            ({"schema_version": 1, "scope_context": {"stale": True}},
             409, "SCOPE_CONTEXT_STALE"),
        ):
            with self.subTest(code=code):
                host = SyntheticHost(execution("parallel"))
                with mock.patch.object(scope_api.adapters, "acknowledge_scope",
                    side_effect=LegacyScopeControlError(
                        "SCOPE_CONTEXT_STALE", "Synthetic stale context", status_code=409)) as acknowledge:
                    with self.assertRaises(HTTPException) as caught:
                        await self.probe(host, acknowledge=True, request=request(payload=payload))
                self.assertEqual(caught.exception.status_code, expected_status)
                self.assertEqual(caught.exception.detail, {"error": code})
                self.assertEqual(acknowledge.call_count, int(code == "SCOPE_CONTEXT_STALE"))
                self.assertEqual(host.writes, [])
        self.snapshot.assert_not_called()


if __name__ == "__main__":
    unittest.main()
