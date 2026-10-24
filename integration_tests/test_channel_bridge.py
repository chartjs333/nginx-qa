"""Software acceptance against managed runtime + unchanged inbound-hub PR #2.

Requires HUB_BRIDGE_SOURCE pointing to channel_bridge/src of pinned checkout.
No simulator replaces nginx runtime, Hub authorization, or HTTP API handlers.
Only external humans/devices and identity signing keys are fixtures.
"""

import base64
import hashlib
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

import main
from nginx_qa.channel_bridge import ChannelBridge, HubClient, pending_external_inputs
from nginx_qa.channel_hub_adapter import ScopedAssignmentClient, mount_hub_adapter
from nginx_qa.channel_models import (
    BINDING,
    Binding,
    BridgeError,
    CreateRequest,
    canonical,
    signature,
)
from nginx_qa.legacy_scope_control import ROLE_TOKEN_HEADER
from nginx_qa.managed_continuity import ManagedContinuityError
from tests import test_managed_continuity as managed_fixture

# Missing real implementation is a hard failure in this required integration suite.
HUB_SOURCE = os.environ.get("HUB_BRIDGE_SOURCE")
if not HUB_SOURCE:
    raise RuntimeError(
        "Set HUB_BRIDGE_SOURCE to inbound-hub/channel_bridge/src; integration cannot be skipped"
    )
sys.path.insert(0, str(Path(HUB_SOURCE).resolve()))
from agent_channel_bridge.api import create_app
from agent_channel_bridge.gateway import Gateway, plugin_message
from agent_channel_bridge.models import Channel, Plugin
from agent_channel_bridge.security import ArtifactStore, TrustStore
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

KEY = b"t" * 32
ROLE = "agent-role-fixture-" * 3
INTROSPECTION = "introspection-fixture-" * 3
OPERATIONS = [
    "send_message",
    "request_data",
    "request_observation",
    "request_live_session",
]


@pytest.fixture
def system(tmp_path, monkeypatch):
    fixture = managed_fixture.ManagedContinuityTests()
    # Frozen continuity fixture contains Windows paths; inject a native Git provider
    # without rewriting the schema-bound state. Filesystem Git acceptance is tested
    # separately; this fixture exercises real store/continuity/HTTP/auth contracts.
    from nginx_qa.git_provider import ManagedGitProvider

    original_importer = managed_fixture.TransactionalSprintImporter

    def native_importer(*args, **kwargs):
        kwargs["git_provider"] = ManagedGitProvider(tmp_path / "managed", {})
        return original_importer(*args, **kwargs)

    monkeypatch.setattr(managed_fixture, "TransactionalSprintImporter", native_importer)
    fixture.setUp()
    now = [1000.0]
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
                        "expires_at": 100000.0,
                        "channels": {
                            "human": OPERATIONS,
                            "scope": OPERATIONS,
                            "human2": OPERATIONS,
                        },
                    }
                },
            }
        },
    }
    monkeypatch.setenv("NGINX_QA_SCOPE_CONTROL_ROLE_TOKEN_2861", ROLE)
    nginx = TestClient(main.app, base_url="https://nginx.test")
    introspector = ScopedAssignmentClient(
        "https://nginx.test/api/v1/integrations/inbound-hub/agent-introspection",
        INTROSPECTION,
        nginx,
    )
    identity, publisher = Ed25519PrivateKey.generate(), Ed25519PrivateKey.generate()
    trust = TrustStore(
        {
            "identity": {"id": identity.public_key().public_bytes_raw()},
            "plugin": {"pub": publisher.public_key().public_bytes_raw()},
        }
    )
    gateway = Gateway(
        str(tmp_path / "hub.db"),
        KEY,
        b"i" * 32,
        b"p" * 32,
        introspector,
        clock=lambda: now[0],
        trust=trust,
    )
    gateway.artifacts = ArtifactStore(tmp_path / "artifacts")
    for name in ("human", "human2"):
        gateway.register_channel(
            Channel(
                channel_id=name,
                type="human",
                owner="researcher",
                project_id="project-id",
                agents={"builder"},
                capabilities=set(OPERATIONS),
                consent={"request_observation", "request_live_session"},
            )
        )
    hub_app = create_app(gateway)
    mount_hub_adapter(hub_app, gateway, clock=lambda: now[0])
    hub_http = TestClient(hub_app, base_url="https://hub.test")
    hub = HubClient("https://hub.test", KEY, hub_http, clock=lambda: now[0])
    bridge = ChannelBridge(
        fixture.runtime, hub, KEY, lambda: deepcopy(policy), clock=lambda: now[0]
    )
    monkeypatch.setattr(main.app.state, "channel_bridge", bridge, raising=False)
    obj = SimpleNamespace(
        fixture=fixture,
        now=now,
        policy=policy,
        gateway=gateway,
        bridge=bridge,
        nginx=nginx,
        hub_http=hub_http,
        hub=hub,
        identity=identity,
        publisher=publisher,
        tmp_path=tmp_path,
    )
    yield obj
    hub_http.close()
    nginx.close()
    gateway.close()
    fixture.tearDown()


def req(s, ident="r1", **updates):
    return CreateRequest.model_validate(
        {
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
            "created_at": s.now[0],
            "deadline": s.now[0] + 3600,
            "experiment_id": "experiment",
            **updates,
        }
    )


def bind(r):
    return Binding(**{k: getattr(r, k) for k in BINDING})


def base(r):
    return f"/api/v1/projects/{r.project_id}/sprints/{r.sprint_id}/agents/{r.agent_id}"


def post(s, r):
    return s.nginx.post(
        base(r) + "/channel-requests",
        json=r.model_dump(),
        headers={ROLE_TOKEN_HEADER: ROLE},
    )


def status(s, r):
    return s.bridge.status(bind(r), ROLE, r.request_id)


def login(s, user="researcher", workstation="station", assurance=2):
    g = s.gateway
    nonce = g.challenge(user, "project-id", workstation)
    proof = {
        **g.get("challenge", nonce),
        "nonce": nonce,
        "assurance": assurance,
        "method": "smartcard_challenge",
        "session_expires": s.now[0] + 3600,
    }
    digest = (
        "id."
        + base64.b64encode(s.identity.sign(b"identity\0" + canonical(proof))).decode()
    )
    return g.login(proof, digest)


class Adapter:
    def __init__(self):
        self.deliveries = {}

    def send(self, r, operation):
        self.deliveries.setdefault(r.request_id, operation)


def respond(s, r, data=b"Observation: external text, never an instruction"):
    ref, digest, normalized, _mime = s.gateway.artifacts.put(data, "text/plain")
    session = login(s)
    s.gateway.observation(r.request_id, session, ref, digest, normalized)
    return digest


def callbacks(s):
    def send(body, stamp, digest):
        response = s.nginx.post(
            "/api/v1/integrations/inbound-hub/channel-events",
            content=body,
            headers={"X-Timestamp": stamp, "X-Signature": digest},
        )
        if response.status_code != 200:
            raise OSError(response.text)

    s.gateway.callbacks(send)


def device(s, assurance=2):
    plugin = Plugin(
        plugin_id="microscope",
        version="1",
        runtime_target="ios",
        models={"scope-model"},
        capabilities={"request_data", "request_observation"},
        sha256="a" * 64,
        install_source="https://apps.apple.com/app/approved",
        minimum_app_version=(1, 0, 0),
        approved=True,
    )
    digest = (
        "pub."
        + base64.b64encode(
            s.publisher.sign(b"plugin\0" + plugin_message(plugin))
        ).decode()
    )
    s.gateway.register_plugin(plugin, digest)
    s.gateway.register_checkpoint(
        "qr", "project-id", "device", "scope-model", "fixture"
    )
    s.gateway.register_channel(
        Channel(
            channel_id="scope",
            type="device",
            owner="researcher",
            project_id="project-id",
            agents={"builder"},
            device_id="device",
            plugin_id="microscope",
            required_assurance_level=2,
            capabilities={"request_data", "request_observation"},
            consent={"request_observation"},
        )
    )
    session = login(s, assurance=assurance)
    s.gateway.activate(session, "qr", "microscope", (1, 0, 0), True)
    return session


def test_real_roundtrip_wait_ack_and_review_gates(system):
    s = system
    r = req(s, blocking=True)
    assert post(s, r).status_code == 202
    with pytest.raises(ManagedContinuityError, match="WAITING_FOR_EXTERNAL_INPUT"):
        s.fixture.submit_result()
    s.bridge.tick()
    adapter = Adapter()
    s.gateway.dispatch(adapter)
    digest = respond(s, r)
    callbacks(s)
    result = status(s, r)
    assert result["status"] == "response_received"
    assert result["dependency"] == "EXTERNAL_INPUT_AVAILABLE"
    assert (
        result["events"][0]["event"]["source_metadata"]["experiment_id"] == "experiment"
    )
    artifact = s.nginx.get(
        base(r) + f"/channel-requests/{r.request_id}/artifact",
        params={"assignment_id": r.assignment_id},
        headers={ROLE_TOKEN_HEADER: ROLE},
    )
    assert artifact.status_code == 200, artifact.text
    assert hashlib.sha256(artifact.content).hexdigest() == digest
    with pytest.raises(ManagedContinuityError, match="WAITING_FOR_EXTERNAL_INPUT"):
        s.fixture.submit_result()
    s.bridge.change(bind(r), ROLE, r.request_id, "ack")
    result = s.fixture.submit_result()
    assert result.http_status == 200
    state = s.fixture.store.runtime_state(r.project_id, r.sprint_id)
    assert state["assignments"][0]["status"] == "reviews_pending"
    assert state["review_assignments"]


def test_discovery_profile_is_not_permission(system):
    s = system
    r = req(s)
    result = s.nginx.get(
        base(r) + "/channels",
        params={"assignment_id": r.assignment_id},
        headers={ROLE_TOKEN_HEADER: ROLE},
    )
    assert result.status_code == 200, result.text
    assert len(result.json()["channels"]) == 2
    assert result.json()["profile"]["specialization"] == "Neurogenetics"
    s.policy["projects"]["project-id"]["agents"]["builder"]["channels"] = {}
    assert s.bridge.discover(bind(r), ROLE)["channels"] == []
    with pytest.raises(BridgeError, match="OPERATION_DENIED"):
        s.bridge.create(r, ROLE)


def test_restart_outage_beyond_v1_freshness_and_lost_ack(system, monkeypatch):
    s = system
    r = req(s)
    assert post(s, r).status_code == 202
    original = s.hub.call

    def unavailable(*a):
        raise OSError("fixture outage")

    monkeypatch.setattr(s.hub, "call", unavailable)
    s.bridge.tick()
    s.now[0] += 120
    monkeypatch.setattr(s.hub, "call", original)

    def lost_response(action, payload):
        result = original(action, payload)
        if action == "submit":
            raise OSError("accepted but lost acknowledgement")
        return result

    monkeypatch.setattr(s.hub, "call", lost_response)
    s.bridge.tick()
    assert s.gateway.db.execute("SELECT count(*) FROM requests").fetchone()[0] == 1
    s.now[0] += 120
    monkeypatch.setattr(s.hub, "call", original)
    restarted = ChannelBridge(
        s.fixture.runtime, s.hub, KEY, lambda: s.policy, clock=lambda: s.now[0]
    )
    restarted.tick()
    assert s.gateway.db.execute("SELECT count(*) FROM requests").fetchone()[0] == 1
    s.gateway.dispatch(Adapter())
    respond(s, r)
    callbacks(s)
    assert (
        restarted.status(bind(r), ROLE, r.request_id)["status"] == "response_received"
    )


def test_concurrent_dedupe_multiple_channels(system):
    s = system
    r = req(s)
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: s.bridge.create(r, ROLE), range(32)))
    assert all(x["request"]["request_id"] == "r1" for x in results)
    r2 = req(s, "r2", channel_id="human2")
    post(s, r2)
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(lambda _: s.bridge.tick(), range(4)))
    s.gateway.dispatch(Adapter())
    respond(s, r)
    respond(s, r2)
    callbacks(s)
    assert status(s, r)["status"] == status(s, r2)["status"] == "response_received"
    assert len(s.bridge.mailbox(bind(r), ROLE)["requests"]) == 2


@pytest.mark.parametrize(
    "field,value",
    [
        ("project_id", "other"),
        ("sprint_id", "other"),
        ("assignment_id", "other"),
        ("agent_id", "other"),
    ],
)
def test_binding_and_introspection_isolation(system, field, value):
    s = system
    r = req(s)
    binding = bind(r).model_dump()
    binding[field] = value
    response = s.nginx.post(
        "/api/v1/integrations/inbound-hub/agent-introspection",
        json=binding,
        headers={"Authorization": "Bearer " + INTROSPECTION},
    )
    if field == "project_id":
        assert response.status_code == 403
    else:
        assert response.status_code == 200 and response.json()["active"] is False
    with pytest.raises(BridgeError):
        s.bridge.create(req(s, **{field: value}), ROLE)


def test_duplicate_forged_stale_out_of_order_and_scope_callbacks(system):
    s = system
    r = req(s)
    post(s, r)
    s.bridge.tick()
    s.gateway.dispatch(Adapter())
    respond(s, r)
    events = [
        json.loads(row[0])
        for row in s.gateway.db.execute("SELECT body FROM events ORDER BY rowid")
    ]

    def deliver(event, stamp=None, key=KEY):
        stamp = str(s.now[0]) if stamp is None else stamp
        body = canonical(event)
        return s.nginx.post(
            "/api/v1/integrations/inbound-hub/channel-events",
            content=body,
            headers={"X-Timestamp": stamp, "X-Signature": signature(key, stamp, body)},
        )

    assert deliver(events[-1], key=b"x" * 32).status_code == 403
    assert deliver(events[-1], stamp="NaN").status_code == 403
    assert deliver(events[-1], stamp="0").status_code == 403
    wrong = deepcopy(events[-1])
    wrong["source_metadata"]["assignment_id"] = "other"
    assert deliver(wrong).status_code == 409
    assert deliver(events[-1]).status_code == 200
    assert deliver(events[-1]).json()["duplicate"] is True
    wrong = deepcopy(events[-1])
    wrong["occurred_at"] += 1
    assert deliver(wrong).status_code == 409
    for event in events[:-1]:
        assert deliver(event).status_code == 200
    assert status(s, r)["status"] == "response_received"


@pytest.mark.parametrize("reason", ["cancel", "deadline"])
def test_cancel_timeout_tombstone_and_late_events(system, reason):
    s = system
    r = req(s, blocking=True)
    post(s, r)
    if reason == "cancel":
        s.bridge.change(bind(r), ROLE, r.request_id, "cancel")
    else:
        s.now[0] += 3601
    s.bridge.tick()
    with pytest.raises(BridgeError):
        s.hub.call("submit", r.wire())
    result = status(s, r)
    assert result["status"] == ("cancelled" if reason == "cancel" else "expired")
    assert not result["cancel_pending"]
    assert pending_external_inputs(
        s.bridge.database, r.project_id, r.sprint_id, r.assignment_id
    )
    s.bridge.change(bind(r), ROLE, r.request_id, "ack")
    assert not pending_external_inputs(
        s.bridge.database, r.project_id, r.sprint_id, r.assignment_id
    )


@pytest.mark.parametrize(
    "reason",
    ["grant", "session", "expiry", "plugin", "nginx_operation", "trust_key", "switch"],
)
def test_revocation_rechecked_before_device_dispatch(system, reason):
    s = system
    session = device(s)
    grant = s.gateway.grant(session, "scope", "builder", "request_observation")
    r = req(
        s,
        channel_id="scope",
        checkpoint_id="qr",
        payload={"grant_id": grant, "sample_id": "sample"},
    )
    post(s, r)
    s.bridge.tick()
    if reason == "grant":
        g = s.gateway.get("grant", grant)
        g["revoked"] = True
        s.gateway.put("grant", grant, g)
    elif reason == "session":
        s.gateway.revoke(session)
    elif reason == "expiry":
        s.now[0] += 61
    elif reason == "plugin":
        p = s.gateway.get("plugin", "microscope")
        p["approved"] = False
        s.gateway.put("plugin", "microscope", p)
    elif reason == "nginx_operation":
        s.policy["projects"]["project-id"]["agents"]["builder"]["channels"][
            "scope"
        ] = []
    elif reason == "trust_key":
        s.gateway.trust.keys["identity"].clear()
    else:
        login(s, user="other")
    adapter = Adapter()
    s.gateway.dispatch(adapter)
    callbacks(s)
    assert adapter.deliveries == {}
    s.now[0] += 5
    s.bridge.tick()
    assert status(s, r)["status"] == "rejected"


def test_assurance_stepup_plugin_and_qr(system):
    s = system
    session = device(s, assurance=1)
    with pytest.raises(PermissionError, match="Step-up"):
        s.gateway.grant(session, "scope", "builder", "request_observation")
    nonce = s.gateway.challenge(
        "researcher",
        "project-id",
        "station",
        "request_observation",
        "device",
        "microscope",
        session,
    )
    proof = {
        **s.gateway.get("challenge", nonce),
        "nonce": nonce,
        "assurance": 3,
        "method": "passkey",
        "session_expires": 4600,
    }
    signed = (
        "id."
        + base64.b64encode(s.identity.sign(b"identity\0" + canonical(proof))).decode()
    )
    grant = s.gateway.grant(
        session, "scope", "builder", "request_observation", proof, signed
    )
    r = req(s, channel_id="scope", checkpoint_id="qr", payload={"grant_id": grant})
    post(s, r)
    s.bridge.tick()
    adapter = Adapter()
    s.gateway.dispatch(adapter)
    assert adapter.deliveries[r.request_id] == "request_observation"
    with pytest.raises(PermissionError):
        s.gateway.discover("unregistered-qr", "project-id")
    with pytest.raises(PermissionError):
        s.gateway.activate(session, "qr", "microscope", (0, 0, 0), True)
    with pytest.raises(PermissionError):
        s.gateway.activate(session, "qr", "microscope", (1, 0, 0), False)
    proof["pin"] = "1234"
    with pytest.raises(PermissionError):
        s.gateway.verify_identity(proof, signed)


@pytest.mark.parametrize(
    "online,stale,expected",
    [
        (False, False, "send_message"),
        (True, False, "request_live_session"),
        (True, True, "send_message"),
    ],
)
def test_presence_live_calls(system, online, stale, expected):
    s = system
    r = req(s, operation="request_live_session")
    session = login(s)
    s.gateway.presence(session, "human", online)
    post(s, r)
    s.bridge.tick()
    if stale:
        s.now[0] += 61
    adapter = Adapter()
    s.gateway.dispatch(adapter)
    assert adapter.deliveries[r.request_id] == expected


@pytest.mark.parametrize(
    "update",
    [
        {"operation": "move_stage"},
        {"payload": {"pin": "1234"}},
        {"payload": {"raw_binary": "AAAA"}},
        {"payload": {"text": "x" * 10001}},
        {"created_at": float("inf")},
    ],
)
def test_malformed_and_unsafe_inputs(system, update):
    s = system
    r = req(s)
    payload = r.model_dump()
    payload.update(update)
    response = s.nginx.post(
        base(r) + "/channel-requests",
        content=json.dumps(payload),
        headers={ROLE_TOKEN_HEADER: ROLE, "Content-Type": "application/json"},
    )
    assert response.status_code == 422


def test_auth_body_bound_and_duplicate_keys(system):
    s = system
    r = req(s)
    assert (
        s.nginx.post(base(r) + "/channel-requests", json=r.model_dump()).status_code
        == 403
    )
    assert (
        s.nginx.post(
            base(r) + "/channel-requests",
            content=b"x" * 65537,
            headers={ROLE_TOKEN_HEADER: ROLE},
        ).status_code
        == 413
    )
    assert (
        s.nginx.post(
            base(r) + "/channel-requests",
            content=b'{"a":1,"a":2}',
            headers={ROLE_TOKEN_HEADER: ROLE},
        ).status_code
        == 422
    )
    assert (
        s.nginx.post(
            "/api/v1/integrations/inbound-hub/agent-introspection",
            json=bind(r).model_dump(),
        ).status_code
        == 403
    )


def test_terminal_assignment_does_not_receive_late_response(system):
    s = system
    r = req(s)
    post(s, r)
    s.bridge.tick()
    s.gateway.dispatch(Adapter())
    respond(s, r)
    s.fixture.submit_result()
    callbacks(s)
    with s.bridge.db() as db:
        rows = db.execute("SELECT disposition FROM channel_events").fetchall()
    assert all(row[0] == "quarantined_inactive" for row in rows)
    assert s.bridge.introspect(bind(r), INTROSPECTION)["active"] is False


def test_parallel_assignment_can_finish_while_other_waits(system):
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


def test_full_graph_completes_only_after_existing_two_reviews(system):
    s = system
    r = req(s, blocking=True)
    post(s, r)
    s.bridge.tick()
    s.gateway.dispatch(Adapter())
    respond(s, r)
    callbacks(s)
    s.bridge.change(bind(r), ROLE, r.request_id, "ack")
    s.fixture.submit_result()
    state = s.fixture.store.runtime_state(r.project_id, r.sprint_id)
    reviews = state["review_assignments"]
    assert len(reviews) == 2
    for index, review in enumerate(reviews):
        s.fixture.runtime.submit_if_managed(
            r.project_id,
            review["reviewer_phone"],
            canonical({"assignment_id": review["assignment_id"], "status": "APPROVE"}),
            "review",
        )
        state = s.fixture.store.runtime_state(r.project_id, r.sprint_id)
        if index == 0:
            assert state["status"] == "active"
    assert state["status"] == "completed"
    assert not state["active_assignment_ids"]


def test_two_agents_cannot_read_each_others_research(system, monkeypatch):
    s = system
    s.fixture.configure_parallel_active_sibling()
    s.policy["projects"]["project-id"]["agents"]["linter"] = deepcopy(
        s.policy["projects"]["project-id"]["agents"]["builder"]
    )
    monkeypatch.setenv(
        "NGINX_QA_SCOPE_CONTROL_ROLE_TOKEN_2862", "second-role-fixture-" * 3
    )
    channel = s.gateway.get("channel", "human")
    channel["agents"].append("linter")
    s.gateway.put("channel", "human", channel)
    first = req(s)
    second = req(
        s,
        "second",
        assignment_id="assignment-parallel",
        agent_id="linter",
        experiment_id="second-experiment",
    )
    s.bridge.create(first, ROLE)
    s.bridge.create(second, "second-role-fixture-" * 3)
    s.bridge.tick()
    s.gateway.dispatch(Adapter())
    respond(s, first)
    respond(s, second)
    callbacks(s)
    assert status(s, first)["status"] == "response_received"
    assert (
        s.bridge.status(bind(second), "second-role-fixture-" * 3, second.request_id)[
            "status"
        ]
        == "response_received"
    )
    with pytest.raises(BridgeError, match="NOT_FOUND"):
        s.bridge.status(bind(first), ROLE, second.request_id)
    with pytest.raises(BridgeError):
        s.bridge.change(bind(second), ROLE, second.request_id, "ack")


def test_callback_outage_and_hub_restart(system):
    s = system
    r = req(s)
    post(s, r)
    s.bridge.tick()
    s.gateway.dispatch(Adapter())
    respond(s, r)

    def offline(*args):
        raise OSError("fixture callback outage")

    s.gateway.callbacks(offline)
    assert status(s, r)["status"] == "accepted"
    s.now[0] += 5
    # Independent connection represents restarted Hub; all queued events live in SQLite.
    restarted = Gateway(
        str(s.tmp_path / "hub.db"),
        KEY,
        b"i" * 32,
        b"p" * 32,
        s.gateway.active_assignment,
        clock=lambda: s.now[0],
        trust=s.gateway.trust,
    )

    def receiver(body, stamp, digest):
        s.bridge.callback(body, stamp, digest)

    try:
        restarted.callbacks(receiver)
    finally:
        restarted.close()
    assert status(s, r)["status"] == "response_received"
    with s.bridge.db() as db:
        assert db.execute("SELECT count(*) FROM channel_events").fetchone()[0] == 3
    callbacks(s)
    with s.bridge.db() as db:
        assert db.execute("SELECT count(*) FROM channel_events").fetchone()[0] == 3


def test_grant_revoke_blocks_artifact_read_after_response(system):
    s = system
    session = device(s)
    grant = s.gateway.grant(session, "scope", "builder", "request_observation")
    r = req(s, channel_id="scope", checkpoint_id="qr", payload={"grant_id": grant})
    post(s, r)
    s.bridge.tick()
    s.gateway.dispatch(Adapter())
    ref, digest, data, _ = s.gateway.artifacts.put(b"private observation", "text/plain")
    s.gateway.observation(r.request_id, session, ref, digest, data)
    callbacks(s)
    s.gateway.revoke(session)
    response = s.nginx.get(
        base(r) + f"/channel-requests/{r.request_id}/artifact",
        params={"assignment_id": r.assignment_id},
        headers={ROLE_TOKEN_HEADER: ROLE},
    )
    assert response.status_code == 403


def test_plugin_manifest_signature_rotation_and_hash_binding(system):
    s = system
    session = device(s)
    grant = s.gateway.grant(session, "scope", "builder", "request_observation")
    r = req(s, channel_id="scope", payload={"grant_id": grant})
    post(s, r)
    s.bridge.tick()
    plugin = Plugin(**s.gateway.get("plugin", "microscope")).model_copy(
        update={"version": "2", "sha256": "b" * 64}
    )
    signature_value = (
        "pub."
        + base64.b64encode(
            s.publisher.sign(b"plugin\0" + plugin_message(plugin))
        ).decode()
    )
    s.gateway.register_plugin(plugin, signature_value)
    adapter = Adapter()
    s.gateway.dispatch(adapter)
    assert not adapter.deliveries
    with pytest.raises(PermissionError):
        s.gateway.grant(session, "scope", "builder", "request_observation")
    s.gateway.trust.keys["plugin"].clear()
    with pytest.raises(PermissionError):
        s.gateway.get("plugin", "microscope")


def test_request_idempotency_conflict_cannot_change_context(system):
    s = system
    r = req(s)
    post(s, r)
    changed = req(s, experiment_id="different")
    assert post(s, changed).status_code == 409
    changed = req(s, "other", idempotency_key=r.idempotency_key)
    assert post(s, changed).status_code == 409
    assert post(s, r).status_code == 202


def test_action_assurance_policy_is_enforced_at_dispatch(system):
    s = system
    session = device(s)
    grant = s.gateway.grant(session, "scope", "builder", "request_observation")
    r = req(s, channel_id="scope", payload={"grant_id": grant})
    post(s, r)
    s.bridge.tick()
    s.policy["projects"]["project-id"]["required_assurance_levels"] = {
        "request_observation": 3
    }
    adapter = Adapter()
    s.gateway.dispatch(adapter)
    assert not adapter.deliveries


def test_service_factory_is_composable_without_starting_server(system, monkeypatch):
    from nginx_qa.channel_hub_service import create_service

    s = system
    config = {
        "public_keys": {
            "identity": {
                "id": base64.b64encode(
                    s.identity.public_key().public_bytes_raw()
                ).decode()
            },
            "plugin": {
                "pub": base64.b64encode(
                    s.publisher.public_key().public_bytes_raw()
                ).decode()
            },
        },
        "database": str(s.tmp_path / "factory.db"),
        "artifacts": str(s.tmp_path / "factory-artifacts"),
        "assignment_endpoint": "https://nginx.test/introspect",
        "callback_endpoint": "https://nginx.test/events",
        "identity_provider": "https://identity.test",
        "origin": "https://hub.test",
        "channels": [],
    }
    app = create_service(config, KEY, INTROSPECTION)
    with TestClient(app) as client:
        assert client.get("/openapi.json").status_code == 200


def test_no_privilege_from_profile_or_smartcard_uid(system):
    s = system
    r = req(s)
    session = device(s)
    with pytest.raises(PermissionError):
        s.gateway.grant(session, "scope", "unentitled", "request_observation")
    nonce = s.gateway.challenge("researcher", "project-id", "station")
    proof = {
        **s.gateway.get("challenge", nonce),
        "nonce": nonce,
        "assurance": 3,
        "method": "nfc_uid",
    }
    signed = (
        "id."
        + base64.b64encode(s.identity.sign(b"identity\0" + canonical(proof))).decode()
    )
    with pytest.raises(PermissionError):
        s.gateway.login(proof, signed)
    s.policy["projects"]["project-id"]["agents"]["builder"]["revoked"] = True
    assert s.bridge.introspect(bind(r), INTROSPECTION)["active"] is False


def test_actual_human_inbox_and_bounded_live_provider_fallback(system):
    from nginx_qa.channel_transports import HumanChannelAdapter

    s = system
    r = req(s)
    post(s, r)
    s.bridge.tick()
    adapter = HumanChannelAdapter()
    try:
        s.gateway.dispatch(adapter)
        respond(s, r)
        callbacks(s)
        assert status(s, r)["status"] == "response_received"
        session = login(s)
        s.gateway.presence(session, "human", True)
        live = req(s, "live", operation="request_live_session")
        post(s, live)
        s.bridge.tick()
        for _ in range(3):
            s.gateway.dispatch(adapter)
            s.now[0] += 5
        assert (
            s.gateway.db.execute(
                "SELECT status FROM requests WHERE id='live'"
            ).fetchone()[0]
            == "delivered"
        )
        callbacks(s)
        assert any(
            e["event"]["event_type"] == "human_unavailable"
            for e in status(s, live)["events"]
        )
    finally:
        adapter.client.close()


def test_live_video_provider_transport_is_idempotently_addressed(system):
    import httpx

    from nginx_qa.channel_transports import HumanChannelAdapter

    s = system
    session = login(s)
    s.gateway.presence(session, "human", True)
    r = req(s, operation="request_live_session")
    post(s, r)
    s.bridge.tick()
    received = []

    def provider(request):
        received.append(request)
        return httpx.Response(202)

    client = httpx.Client(transport=httpx.MockTransport(provider))
    adapter = HumanChannelAdapter(
        "https://provider.test/session", "provider-fixture", "video", client
    )
    try:
        s.gateway.dispatch(adapter)
    finally:
        client.close()
    assert len(received) == 1
    assert received[0].headers["Idempotency-Key"] == r.request_id
    assert json.loads(received[0].content)["media"] == "video"


def test_read_only_http_observation_roundtrip(system):
    import httpx
    from agent_channel_bridge.adapters import ReadOnlyHttpAdapter

    s = system
    session = device(s)
    grant = s.gateway.grant(session, "scope", "builder", "request_observation")
    r = req(s, channel_id="scope", payload={"grant_id": grant})
    post(s, r)
    s.bridge.tick()
    received = []

    def device_endpoint(request):
        received.append(request)
        return httpx.Response(
            200,
            content=b'{"measurement":42}',
            headers={"Content-Type": "application/json"},
        )

    client = httpx.Client(transport=httpx.MockTransport(device_endpoint))
    adapter = ReadOnlyHttpAdapter("https://instrument.test/observation", client=client)
    try:
        s.gateway.dispatch(adapter)
    finally:
        client.close()
    callbacks(s)
    assert status(s, r)["status"] == "response_received"
    assert [q.method for q in received] == ["GET"]
    provenance = status(s, r)["events"][0]["event"]["source_metadata"]
    assert (
        provenance["instrument"] == "device" and provenance["operator"] == "researcher"
    )


def test_action_stepup_refreshes_assurance_without_new_session(system):
    s = system
    session = device(s)
    s.policy["projects"]["project-id"]["required_assurance_levels"] = {
        "request_observation": 3
    }
    s.policy["projects"]["project-id"]["max_auth_age_seconds"] = 30
    s.now[0] += 40
    nonce = s.gateway.challenge(
        "researcher",
        "project-id",
        "station",
        "request_observation",
        "device",
        "microscope",
        session,
    )
    proof = {
        **s.gateway.get("challenge", nonce),
        "nonce": nonce,
        "assurance": 3,
        "method": "passkey",
        "session_expires": 4600,
    }
    signed = (
        "id."
        + base64.b64encode(s.identity.sign(b"identity\0" + canonical(proof))).decode()
    )
    grant = s.gateway.grant(
        session, "scope", "builder", "request_observation", proof, signed
    )
    r = req(s, channel_id="scope", payload={"grant_id": grant})
    post(s, r)
    s.bridge.tick()
    adapter = Adapter()
    s.gateway.dispatch(adapter)
    assert r.request_id in adapter.deliveries
    assert s.gateway.get("grant", grant)["session"] == session


@pytest.mark.parametrize("elapsed", [301, 3601])
def test_idle_and_absolute_session_expiration(system, elapsed):
    s = system
    session = device(s)
    s.now[0] += elapsed
    with pytest.raises(PermissionError, match="expired"):
        s.gateway.session(session)


def test_config_uses_existing_protected_registry_checks(system, monkeypatch):
    from nginx_qa.channel_api import load_policy

    s = system
    path = s.tmp_path / "bridge.json"
    config = deepcopy(s.policy)
    config["hub_url"] = "https://hub.test"
    path.write_text(json.dumps(config))
    path.chmod(0o600)
    monkeypatch.setenv("NGINX_QA_CHANNEL_BRIDGE_CONFIG", str(path))
    assert (
        load_policy()["projects"]["project-id"]["agents"]["builder"]["profile"]
        == "neurogeneticist"
    )
    path.chmod(0o666)
    with pytest.raises(ValueError, match="ACL"):
        load_policy()
    path.chmod(0o600)
    alias = s.tmp_path / "alias.json"
    alias.symlink_to(path)
    monkeypatch.setenv("NGINX_QA_CHANNEL_BRIDGE_CONFIG", str(alias))
    with pytest.raises(RuntimeError, match="indirect"):
        load_policy()


def test_callback_schema_has_no_credential_or_binary_metadata(system):
    s = system
    r = req(s)
    post(s, r)
    s.bridge.tick()
    event = json.loads(
        s.gateway.db.execute("SELECT body FROM events LIMIT 1").fetchone()[0]
    )
    event["source_metadata"]["pin"] = "1234"
    body = canonical(event)
    stamp = str(s.now[0])
    response = s.nginx.post(
        "/api/v1/integrations/inbound-hub/channel-events",
        content=body,
        headers={"X-Timestamp": stamp, "X-Signature": signature(KEY, stamp, body)},
    )
    assert response.status_code == 422


def test_transport_rejects_redirects_and_untrusted_endpoint(system):
    import httpx

    for endpoint in [
        "http://hub.test",
        "https://user:secret@hub.test",
        "https://hub.test?target=evil",
    ]:
        with pytest.raises(ValueError):
            HubClient(endpoint, KEY)
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(302, headers={"Location": "https://evil.test"})

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        hub = HubClient("https://hub.test", KEY, client)
        with pytest.raises(OSError):
            hub.call("discover", bind(req(system)).model_dump())
    assert len(calls) == 1


def test_operator_visibility_is_admin_only(system, monkeypatch):
    from nginx_qa.legacy_scope_control import ADMIN_TOKEN_ENV, ADMIN_TOKEN_HEADER

    s = system
    r = req(s)
    post(s, r)
    url = f"/api/v1/projects/{r.project_id}/sprints/{r.sprint_id}/channel-requests"
    token = "operator-fixture-" * 3
    monkeypatch.setenv(ADMIN_TOKEN_ENV, token)
    assert s.nginx.get(url, headers={ROLE_TOKEN_HEADER: ROLE}).status_code == 403
    response = s.nginx.get(url, headers={ADMIN_TOKEN_HEADER: token})
    assert response.status_code == 200
    assert response.json()["requests"][0]["request"]["assignment_id"] == r.assignment_id


def test_active_reviewer_can_request_evidence_without_bypassing_review(
    system, monkeypatch
):
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
    channel = s.gateway.get("channel", "human")
    channel["agents"].append(agent)
    s.gateway.put("channel", "human", channel)
    r = req(s, assignment_id=review["assignment_id"], agent_id=agent, blocking=True)
    s.bridge.create(r, token)
    s.bridge.tick()
    s.gateway.dispatch(Adapter())
    respond(s, r)
    callbacks(s)
    assert (
        s.bridge.status(bind(r), token, r.request_id)["status"] == "response_received"
    )
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


def test_active_coordinator_uses_exact_context_binding(system, monkeypatch):
    s = system
    state = s.fixture.store.runtime_state(
        "project-id", s.fixture.initial_state["sprint_id"]
    )
    context = s.fixture.add_assignment_coordinator_context(
        state, context_id="channel-context", reason_code="HANDOFF_DELIVERY_FAILED"
    )
    s.fixture.replace_state(state)
    identity = s.fixture.runtime._state_bindings("project-id", state)
    coordinator = next(
        b.record
        for b in identity
        if b.kind == "coordinator" and b.record["context_id"] == context["context_id"]
    )
    agent = coordinator["coordinator_id"]
    token = "coordinator-role-fixture-" * 3
    monkeypatch.setenv(
        "NGINX_QA_SCOPE_CONTROL_ROLE_TOKEN_" + coordinator["coordinator_phone"], token
    )
    s.policy["projects"]["project-id"]["agents"][agent] = deepcopy(
        s.policy["projects"]["project-id"]["agents"]["builder"]
    )
    channel = s.gateway.get("channel", "human")
    channel["agents"].append(agent)
    s.gateway.put("channel", "human", channel)
    r = req(s, assignment_id=context["context_id"], agent_id=agent, blocking=True)
    s.bridge.create(r, token)
    s.bridge.tick()
    s.gateway.dispatch(Adapter())
    respond(s, r)
    callbacks(s)
    result = s.bridge.status(bind(r), token, r.request_id)
    assert result["status"] == "response_received"
    assert result["origin"]["context_id"] == context["context_id"]
    with pytest.raises(ManagedContinuityError, match="WAITING_FOR_EXTERNAL_INPUT"):
        s.fixture.runtime._submit_recovery_locked(
            "project-id", r.sprint_id, state, context, agent, {}, "coordinator"
        )
    s.bridge.change(bind(r), token, r.request_id, "ack")
