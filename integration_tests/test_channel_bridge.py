"""nginx runtime against an independent HTTPS 1.1 CONTRACT SIMULATOR.

The stock Hub HTTP surface is tested separately in test_hub_public_http.py.
No Hub imports, source path injection, shared databases or in-process HTTP clients.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from integration_tests.http_support import (
    Server,
    certificate,
    client,
    free_port,
    restore_runtime,
)
from nginx_qa.channel_api import router
from nginx_qa.channel_bridge import ChannelBridge, HubClient
from nginx_qa.channel_models import (
    BINDING,
    Binding,
    CreateRequest,
    canonical,
    signature,
)
from nginx_qa.git_provider import ManagedGitProvider
from nginx_qa.legacy_scope_control import ROLE_TOKEN_HEADER
from nginx_qa.managed_continuity import ManagedContinuityError
from tests import test_managed_continuity as managed_fixture

KEY = b"t" * 32
ROLE = "agent-role-fixture-" * 3
INTROSPECTION = "introspection-fixture-" * 3
OPERATIONS = [
    "send_message",
    "request_data",
    "request_observation",
    "request_live_session",
]
CALLBACK = "/api/v1/integrations/inbound-hub/channel-events"
INTRO = "/api/v1/integrations/inbound-hub/agent-introspection"


@pytest.fixture
def system(tmp_path, monkeypatch):
    nginx_dir, peer_dir = tmp_path / "nginx-owned", tmp_path / "peer-owned"
    nginx_dir.mkdir()
    peer_dir.mkdir()
    fixture = managed_fixture.ManagedContinuityTests()
    original = managed_fixture.TransactionalSprintImporter

    def native_importer(*args, **kwargs):
        kwargs["git_provider"] = ManagedGitProvider(nginx_dir / "git", {})
        return original(*args, **kwargs)

    monkeypatch.setattr(managed_fixture, "TransactionalSprintImporter", native_importer)
    fixture.setUp()
    monkeypatch.setenv("NGINX_QA_SCOPE_CONTROL_ROLE_TOKEN_2861", ROLE)
    policy = {
        "schema_version": 1,
        "introspection_token_sha256": hashlib.sha256(
            INTROSPECTION.encode()
        ).hexdigest(),
        "introspection_projects": ["project-id"],
        "projects": {
            "project-id": {
                "operations": OPERATIONS,
                "agents": {
                    "builder": {
                        "profile": "neurogeneticist",
                        "expires_at": time.time() + 10000,
                        "channels": {"human": OPERATIONS, "human2": OPERATIONS},
                    }
                },
            }
        },
    }
    cert, key = certificate(nginx_dir)
    peer_cert, peer_key = certificate(peer_dir)
    app = FastAPI()
    app.state.callback_unavailable = False

    @app.middleware("http")
    async def callback_outage(request: Request, call_next):
        if request.url.path == CALLBACK and app.state.callback_unavailable:
            return JSONResponse({"test_outage": True}, status_code=503)
        return await call_next(request)

    app.include_router(router)
    server = Server(app, cert, key)
    peer_port = free_port()
    peer_url = f"https://127.0.0.1:{peer_port}"
    peer_client = client(peer_url, peer_cert)
    offset = [0.0]
    clock = lambda: time.time() + offset[0]
    hub = HubClient(peer_url, KEY, peer_client, clock=clock)
    bridge = ChannelBridge(
        fixture.runtime, hub, KEY, lambda: deepcopy(policy), clock=clock
    )
    app.state.channel_bridge = bridge
    nginx = client(server.url, cert)
    config = {
        "key": KEY.hex(),
        "token": INTROSPECTION,
        "introspection": server.url + INTRO,
        "callback": server.url + CALLBACK,
        "ca": cert.read_text(),
        "port": peer_port,
        "cert": str(peer_cert),
        "tls_key": str(peer_key),
    }
    process = subprocess.Popen(
        [sys.executable, str(Path(__file__).with_name("http_peer.py"))],
        env={**os.environ, "HTTP_PEER_CONFIG": json.dumps(config)},
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    s = SimpleNamespace(
        fixture=fixture,
        policy=policy,
        bridge=bridge,
        hub=hub,
        nginx=nginx,
        peer=peer_client,
        server=server,
        offset=offset,
        clock=clock,
        app=app,
        cert=cert,
        key=key,
    )
    try:
        deadline = time.monotonic() + 10
        while True:
            assert process.poll() is None, "contract simulator failed to start"
            try:
                if peer_client.get("/health").status_code == 200:
                    break
            except httpx.HTTPError:
                pass
            assert time.monotonic() < deadline
            time.sleep(0.03)
        yield s
    finally:
        peer_client.close()
        process.terminate()
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=3)
        s.nginx.close()
        s.server.close()
        fixture.tearDown()


def req(s, ident="r1", **updates):
    value = {
        "schema_version": 1,
        "request_id": ident,
        "idempotency_key": ident,
        "correlation_id": "correlation-" + ident,
        "project_id": "project-id",
        "sprint_id": s.fixture.initial_state["sprint_id"],
        "assignment_id": "assignment-1",
        "agent_id": "builder",
        "channel_id": "human",
        "operation": "request_observation",
        "payload": {"text": "Observe", "sample_id": "sample"},
        "created_at": s.clock(),
        "deadline": s.clock() + 3600,
        "experiment_id": "experiment",
        "blocking": False,
    }
    value.update(updates)
    return CreateRequest.model_validate(value)


def bind(r):
    return Binding(**{k: getattr(r, k) for k in BINDING})


def base(r):
    return f"/api/v1/projects/{r.project_id}/sprints/{r.sprint_id}/agents/{r.agent_id}"


def post(s, r, token=ROLE):
    return s.nginx.post(
        base(r) + "/channel-requests",
        json=r.model_dump(),
        headers={ROLE_TOKEN_HEADER: token},
    )


def status(s, r, token=ROLE):
    result = s.nginx.get(
        f"/api/v1/projects/{r.project_id}/sprints/{r.sprint_id}/channel-requests/{r.request_id}",
        params={"agent_id": r.agent_id, "assignment_id": r.assignment_id},
        headers={ROLE_TOKEN_HEADER: token},
    )
    assert result.status_code == 200, result.text
    return result.json()


def change(s, r, action):
    return s.nginx.post(
        base(r) + f"/channel-requests/{r.request_id}/{action}",
        params={"assignment_id": r.assignment_id},
        headers={ROLE_TOKEN_HEADER: ROLE},
    )


def event(r, ident="event-1", kind="response_received"):
    digest = hashlib.sha256(b"Independent HTTP observation").hexdigest()
    value = {
        "schema_version": 1,
        "event_id": ident,
        "request_id": r.request_id,
        "correlation_id": r.correlation_id,
        "channel_id": r.channel_id,
        "event_type": kind,
        "occurred_at": time.time(),
        "received_at": time.time(),
        "content_ref": "artifact://sha256/" + digest
        if kind == "response_received"
        else None,
        "sha256": digest if kind == "response_received" else None,
        "source_metadata": {
            **bind(r).model_dump(),
            "experiment_id": r.experiment_id,
            "checkpoint_id": r.checkpoint_id,
            "sample_id": r.payload.sample_id,
            "instrument": None,
            "operator": "researcher",
        },
    }
    return value


def callback(s, value, *, stamp=None, key=KEY):
    data = canonical(value)
    stamp = str(time.time()) if stamp is None else stamp
    return s.nginx.post(
        CALLBACK,
        content=data,
        headers={"X-Timestamp": stamp, "X-Signature": signature(key, stamp, data)},
    )


def respond(s, r):
    response = s.peer.post("/__test/control", json={"event": event(r), "pump": True})
    assert response.status_code == 200, response.text
    assert response.json()["pending_events"] == 0


def test_http_roundtrip_wait_ack_and_review_gates(system):
    s = system
    r = req(s, blocking=True)
    assert post(s, r).status_code == 202
    with pytest.raises(ManagedContinuityError, match="WAITING_FOR_EXTERNAL_INPUT"):
        s.fixture.submit_result()
    s.bridge.tick()
    respond(s, r)
    value = status(s, r)
    assert value["status"] == "response_received"
    assert value["origin"]["assignment_kind"] == "assignment"
    assert (
        value["events"][0]["event"]["source_metadata"]["experiment_id"] == "experiment"
    )
    artifact = s.nginx.get(
        base(r) + "/channel-requests/r1/artifact",
        params={"assignment_id": r.assignment_id},
        headers={ROLE_TOKEN_HEADER: ROLE},
    )
    assert artifact.content == b"Independent HTTP observation"
    with pytest.raises(ManagedContinuityError, match="WAITING_FOR_EXTERNAL_INPUT"):
        s.fixture.submit_result()
    assert change(s, r, "ack").status_code == 200
    s.fixture.submit_result()
    state = s.fixture.store.runtime_state(r.project_id, r.sprint_id)
    assert state["assignments"][0]["status"] == "reviews_pending"
    assert len(state["review_assignments"]) == 2
    for index, review in enumerate(state["review_assignments"]):
        s.fixture.runtime.submit_if_managed(
            r.project_id,
            review["reviewer_phone"],
            canonical({"assignment_id": review["assignment_id"], "status": "APPROVE"}),
            "review",
        )
        current = s.fixture.store.runtime_state(r.project_id, r.sprint_id)
        assert current["status"] == ("active" if index == 0 else "completed")


def test_restart_after_lost_receipt_and_delayed_callback(system):
    s = system
    s.offset[0] = -120
    r = req(s)
    assert post(s, r).status_code == 202
    s.offset[0] = 0
    s.peer.post("/__test/control", json={"modes": {"lose_ack": True}})
    s.bridge.tick()
    assert status(s, r)["status"] == "pending"
    s.server.close()
    # Reopen all nginx runtime/store objects, not just the HTTP client.
    runtime = restore_runtime(s.fixture.store.database_path, s.fixture.initial_state)
    s.bridge = ChannelBridge(runtime, s.hub, KEY, lambda: s.policy)
    s.app.state.channel_bridge = s.bridge
    with s.bridge.db() as db:  # nginx-owned scheduler clock, no peer state access
        db.execute("UPDATE channel_requests SET due=0")
    pending = s.peer.post(
        "/__test/control", json={"event": event(r), "pump": True}
    ).json()
    assert pending["pending_events"] == 1
    s.server = Server(s.app, s.cert, s.key, port=s.server.port)
    s.bridge.tick()
    result = s.peer.post("/__test/control", json={"pump": True}).json()
    assert result == {"requests": 1, "pending_events": 0}
    assert status(s, r)["status"] == "response_received"


def test_multiple_channels_concurrent_requests_are_deduped(system):
    s = system
    r = req(s)
    with ThreadPoolExecutor(max_workers=4) as pool:
        responses = list(pool.map(lambda _: post(s, r), range(12)))
    assert all(x.status_code == 202 for x in responses)
    second = req(s, "r2", channel_id="human2")
    assert post(s, second).status_code == 202
    with ThreadPoolExecutor(max_workers=3) as pool:
        list(pool.map(lambda _: s.bridge.tick(), range(3)))
    assert s.peer.post("/__test/control", json={}).json()["requests"] == 2
    respond(s, r)
    response = s.peer.post(
        "/__test/control", json={"event": event(second, "event-2"), "pump": True}
    )
    assert response.status_code == 200
    assert status(s, r)["status"] == status(s, second)["status"] == "response_received"


@pytest.mark.parametrize("field", BINDING)
def test_callback_scope_isolation(system, field):
    s = system
    r = req(s)
    post(s, r)
    value = event(r)
    value["source_metadata"][field] = "foreign"
    assert callback(s, value).status_code == 409
    assert status(s, r)["status"] == "pending"


@pytest.mark.parametrize(
    "kind", ["forged", "stale", "duplicate", "conflict", "out_of_order"]
)
def test_callback_signature_replay_and_order(system, kind):
    s = system
    r = req(s)
    post(s, r)
    value = event(r)
    if kind == "forged":
        assert callback(s, value, key=b"wrong" * 8).status_code == 403
    elif kind == "stale":
        assert callback(s, value, stamp=str(time.time() - 121)).status_code == 403
    else:
        assert callback(s, value).status_code == 200
        if kind == "duplicate":
            assert callback(s, value).json()["duplicate"] is True
        elif kind == "conflict":
            value["source_metadata"]["operator"] = "other"
            assert callback(s, value).status_code == 409
        else:
            assert (
                callback(s, event(r, "accepted-late", "accepted")).json()["disposition"]
                == "late"
            )
        assert status(s, r)["status"] == "response_received"


@pytest.mark.parametrize("reason", ["local_revoke", "peer_deny", "cancel", "timeout"])
def test_revocation_and_terminal_fences(system, reason):
    s = system
    r = req(s)
    post(s, r)
    if reason == "local_revoke":
        s.policy["projects"]["project-id"]["agents"]["builder"]["revoked"] = True
    elif reason == "peer_deny":
        s.peer.post("/__test/control", json={"modes": {"deny": True}})
    elif reason == "cancel":
        assert change(s, r, "cancel").status_code == 200
    else:
        s.offset[0] = 4000
    s.bridge.tick()
    s.offset[0] = 0
    s.policy["projects"]["project-id"]["agents"]["builder"]["revoked"] = False
    expected = {
        "local_revoke": "rejected",
        "peer_deny": "rejected",
        "cancel": "cancelled",
        "timeout": "expired",
    }[reason]
    assert status(s, r)["status"] == expected
    assert callback(s, event(r)).json()["disposition"] == "late"
    assert status(s, r)["status"] == expected


def test_parallel_assignment_is_not_blocked_by_external_wait(system):
    s = system
    s.fixture.configure_parallel_active_sibling()
    r = req(s, blocking=True)
    post(s, r)
    state = s.fixture.store.runtime_state(r.project_id, r.sprint_id)
    sibling = next(
        a for a in state["assignments"] if a["assignment_id"] == "assignment-parallel"
    )
    workspace = next(
        w for w in state["workspaces"] if w["workspace_id"] == sibling["workspace_id"]
    )
    payload = s.fixture.result_request()
    payload.update(
        assignment_id=sibling["assignment_id"], git_branch=workspace["assigned_branch"]
    )
    result = s.fixture.runtime.submit_if_managed(
        "project-id", sibling["agent_phone"], canonical(payload), "parallel"
    )
    assert result.http_status == 200
    with pytest.raises(ManagedContinuityError, match="WAITING_FOR_EXTERNAL_INPUT"):
        s.fixture.submit_result()


def test_introspection_and_agent_auth_fail_closed(system):
    s = system
    r = req(s)
    assert post(s, r, token="wrong").status_code == 403
    assert (
        s.nginx.post(
            INTRO, json=bind(r).model_dump(), headers={"Authorization": "Bearer wrong"}
        ).status_code
        == 403
    )
    for field in BINDING:
        wrong = bind(r).model_dump()
        wrong[field] = "other"
        response = s.nginx.post(
            INTRO, json=wrong, headers={"Authorization": "Bearer " + INTROSPECTION}
        )
        assert response.status_code == 403 or response.json()["active"] is False
    found = s.nginx.get(
        base(r) + "/channels",
        params={"assignment_id": r.assignment_id},
        headers={ROLE_TOKEN_HEADER: ROLE},
    )
    assert found.status_code == 200
    assert len(found.json()["channels"]) == 2
    s.policy["projects"]["project-id"]["agents"]["builder"]["channels"] = {}
    assert post(s, r).status_code == 403


def test_wrong_http_receipt_leaves_outbox_pending(system):
    s = system
    r = req(s)
    post(s, r)
    s.peer.post("/__test/control", json={"modes": {"wrong_ack": True}})
    s.bridge.tick()
    assert status(s, r)["status"] == "pending"
    with s.bridge.db() as db:
        assert db.execute("SELECT sent FROM channel_requests").fetchone()[0] == 0


def test_two_agents_mailboxes_and_origin_are_isolated(system, monkeypatch):
    s = system
    s.fixture.configure_parallel_active_sibling()
    token = "second-role-fixture-" * 3
    monkeypatch.setenv("NGINX_QA_SCOPE_CONTROL_ROLE_TOKEN_2862", token)
    s.policy["projects"]["project-id"]["agents"]["linter"] = deepcopy(
        s.policy["projects"]["project-id"]["agents"]["builder"]
    )
    first = req(s)
    second = req(s, "r2", agent_id="linter", assignment_id="assignment-parallel")
    assert post(s, first).status_code == 202
    assert post(s, second, token).status_code == 202
    s.bridge.tick()
    respond(s, first)
    s.peer.post(
        "/__test/control", json={"event": event(second, "event-2"), "pump": True}
    )
    assert status(s, second, token)["request"]["agent_id"] == "linter"
    assert status(s, first)["request"]["agent_id"] == "builder"
    wrong = s.nginx.get(
        f"/api/v1/projects/{first.project_id}/sprints/{first.sprint_id}/channel-requests/{first.request_id}",
        params={"assignment_id": second.assignment_id, "agent_id": second.agent_id},
        headers={ROLE_TOKEN_HEADER: token},
    )
    assert wrong.status_code == 404


def test_reviewer_wait_preserves_review_gate(system, monkeypatch):
    s = system
    s.fixture.submit_result()
    state = s.fixture.store.runtime_state(
        "project-id", s.fixture.initial_state["sprint_id"]
    )
    review = state["review_assignments"][0]
    agent = review["reviewer_id"]
    token = "review-role-fixture-" * 3
    monkeypatch.setenv(
        "NGINX_QA_SCOPE_CONTROL_ROLE_TOKEN_" + review["reviewer_phone"], token
    )
    s.policy["projects"]["project-id"]["agents"][agent] = deepcopy(
        s.policy["projects"]["project-id"]["agents"]["builder"]
    )
    r = req(s, assignment_id=review["assignment_id"], agent_id=agent, blocking=True)
    assert post(s, r, token).status_code == 202
    s.bridge.tick()
    respond(s, r)
    assert status(s, r, token)["origin"]["assignment_kind"] == "review"
    payload = canonical({"assignment_id": review["assignment_id"], "status": "APPROVE"})
    with pytest.raises(ManagedContinuityError, match="WAITING_FOR_EXTERNAL_INPUT"):
        s.fixture.runtime.submit_if_managed(
            "project-id", review["reviewer_phone"], payload, "review"
        )
    s.bridge.change(bind(r), token, r.request_id, "ack")
    s.fixture.runtime.submit_if_managed(
        "project-id", review["reviewer_phone"], payload, "review"
    )
    state = s.fixture.store.runtime_state(r.project_id, r.sprint_id)
    assert len(state["reviews"]) == 1 and state["status"] == "active"


def test_callback_http_non_ascii_and_duplicate_headers_fail_closed(system):
    s = system
    headers = [
        (b"X-Timestamp", str(time.time()).encode()),
        (b"X-Signature", b"\xff" * 64),
    ]
    assert s.nginx.post(CALLBACK, content=b"{}", headers=headers).status_code == 403
    headers = [
        ("X-Timestamp", str(time.time())),
        ("X-Timestamp", str(time.time())),
        ("X-Signature", "a" * 64),
    ]
    assert s.nginx.post(CALLBACK, content=b"{}", headers=headers).status_code == 403
