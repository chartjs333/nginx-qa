"""Unmodified, packaged Hub PR #2, exercised exclusively through public HTTP.

Set HUB_HTTP_WHEEL to the pinned wheel. It is installed into Hub's private test
process, never imported by nginx or this test runner. Missing prerequisite fails.
"""

import base64
import json
import os
import shutil
import subprocess
import sys
import time
import zipfile
from pathlib import Path
from types import SimpleNamespace

import certifi
import httpx
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from fastapi import FastAPI, Request, Response

from integration_tests import test_channel_bridge as bridge_tests
from integration_tests.http_support import Server, certificate, client, free_port
from integration_tests.test_channel_bridge import (
    CALLBACK,
    INTRO,
    INTROSPECTION,
    KEY,
    bind,
    callback,
    post,
    req,
    status,
)
from nginx_qa.channel_bridge import HubClient
from nginx_qa.channel_models import BridgeError, canonical, signature

system = bridge_tests.system


@pytest.fixture
def real_hub(system, tmp_path):
    wheel = os.environ.get("HUB_HTTP_WHEEL")
    if not wheel or not Path(wheel).is_file():
        pytest.fail(
            "HUB_HTTP_WHEEL must identify the pinned unmodified Hub distribution; not a skip"
        )
    s = system
    home = tmp_path / "independent-hub"
    home.mkdir()
    site = home / "installed"
    site.mkdir()
    with zipfile.ZipFile(wheel) as archive:
        archive.extractall(site)
    # Hub's httpx introspector ignores environment trust settings. Give only the
    # isolated Hub interpreter a test CA bundle; no application source is changed.
    shutil.copytree(Path(certifi.__file__).parent, site / "certifi")
    bundle = site / "certifi" / "cacert.pem"
    bundle.write_bytes(bundle.read_bytes() + s.cert.read_bytes())
    cert, tls_key = certificate(home)
    origin_port, hub_port = free_port(), free_port()
    origin = f"https://127.0.0.1:{origin_port}"
    backend = f"http://127.0.0.1:{hub_port}"
    proxy = FastAPI(openapi_url=None, docs_url=None, redoc_url=None)

    @proxy.api_route("/{path:path}", methods=["GET", "POST"])
    async def forward(path: str, request: Request):
        headers = {
            k: v
            for k, v in request.headers.items()
            if k.lower() not in ("host", "content-length", "connection")
        }
        async with httpx.AsyncClient(
            trust_env=False, follow_redirects=False
        ) as outbound:
            response = await outbound.request(
                request.method,
                backend + "/" + path,
                content=await request.body(),
                headers=headers,
            )
        headers = {
            k: v
            for k, v in response.headers.items()
            if k.lower()
            not in (
                "content-length",
                "transfer-encoding",
                "connection",
                "content-encoding",
            )
        }
        return Response(
            response.content, status_code=response.status_code, headers=headers
        )

    server = Server(proxy, cert, tls_key, port=origin_port)
    identity = Ed25519PrivateKey.generate()
    config = {
        "database": str(home / "own.sqlite3"),
        "artifacts": str(home / "own-artifacts"),
        "public_keys": {
            "identity": {
                "id": base64.b64encode(
                    identity.public_key().public_bytes_raw()
                ).decode()
            },
            "plugin": {},
        },
        "assignment_endpoint": s.server.url + INTRO,
        "callback_endpoint": s.server.url + CALLBACK,
        "identity_provider": "https://idp.invalid",
        "origin": origin,
        "port": hub_port,
        "channels": [
            {
                "channel_id": "human",
                "type": "human",
                "owner": "researcher",
                "project_id": "project-id",
                "agents": ["builder"],
                "capabilities": ["send_message", "request_observation"],
                "consent": ["request_observation"],
            }
        ],
    }
    config_path = home / "config.json"
    config_path.write_text(json.dumps(config))
    environment = {
        **{
            k: v
            for k, v in os.environ.items()
            if not k.lower().endswith("_proxy")
            and k not in ("SSL_CERT_FILE", "SSL_CERT_DIR")
        },
        "PYTHONPATH": str(site),
        "BRIDGE_CONFIG": str(config_path),
        "BRIDGE_TRANSPORT_KEY": base64.b64encode(KEY).decode(),
        "BRIDGE_ASSIGNMENT_TOKEN": INTROSPECTION,
    }
    log = (home / "process.log").open("wb")

    def start():
        process = subprocess.Popen(
            [sys.executable, "-m", "agent_channel_bridge"],
            cwd=home,
            env=environment,
            stdout=log,
            stderr=log,
        )
        with httpx.Client(trust_env=False) as probe:
            deadline = time.monotonic() + 10
            while True:
                assert process.poll() is None, (home / "process.log").read_text()
                try:
                    if probe.get(backend + "/health").status_code == 200:
                        break
                except httpx.HTTPError:
                    pass
                assert time.monotonic() < deadline
                time.sleep(0.05)
        return process

    def stop(process):
        process.terminate()
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=3)

    browser = client(origin, cert)
    obj = SimpleNamespace(
        http=browser, origin=origin, process=start(), sign=identity, cert=cert
    )

    def restart():
        stop(obj.process)
        obj.process = start()

    obj.restart = restart
    try:
        yield obj
    finally:
        browser.close()
        stop(obj.process)
        server.close()
        log.close()


def signed(hub, r, *, key=KEY):
    data = canonical(r.wire())
    stamp = str(time.time())
    return hub.http.post(
        "/v1/channel-requests",
        content=data,
        headers={"X-Timestamp": stamp, "X-Signature": signature(key, stamp, data)},
    )


def login(hub):
    hub.http.headers["Origin"] = hub.origin
    challenge = hub.http.post(
        "/mobile/challenge",
        json={"user": "researcher", "project": "project-id", "workstation": "station"},
    )
    assert challenge.status_code == 200, challenge.text
    value = challenge.json()
    proof = {
        **value["binding"],
        "nonce": value["nonce"],
        "assurance": 2,
        "method": "smartcard_challenge",
        "session_expires": time.time() + 3600,
    }
    digest = (
        "id."
        + base64.b64encode(hub.sign.sign(b"identity\0" + canonical(proof))).decode()
    )
    result = hub.http.post("/mobile/login", json={"proof": proof, "signature": digest})
    assert result.status_code == 200, result.text
    hub.http.headers["X-CSRF-Token"] = result.json()["csrf"]


def wait_status(s, r, expected):
    deadline = time.monotonic() + 10
    while True:
        value = status(s, r)
        if value["status"] == expected:
            return value
        assert time.monotonic() < deadline, value
        time.sleep(0.1)


def test_stock_hub_v1_signed_request_mobile_response_and_callback(system, real_hub):
    s, hub = system, real_hub
    r = req(s, operation="send_message", blocking=True)
    assert post(s, r).status_code == 202
    assert signed(hub, r).json()["request_id"] == r.request_id
    wait_status(s, r, "delivered")
    login(hub)
    observation = hub.http.post(
        "/mobile/requests/r1/observation",
        content=b"Actual independent Hub observation",
        headers={"Content-Type": "text/plain"},
    )
    assert observation.status_code == 200, observation.text
    value = wait_status(s, r, "response_received")
    received = next(
        x["event"]
        for x in value["events"]
        if x["event"]["event_type"] == "response_received"
    )
    assert received["source_metadata"]["assignment_id"] == r.assignment_id
    assert received["sha256"] == observation.json()["sha256"]
    # Replay the actual event using a fresh transport signature.
    assert callback(s, received).json()["duplicate"] is True
    download = hub.http.get("/mobile/requests/r1/artifacts/" + received["sha256"])
    assert download.content == b"Actual independent Hub observation"


def test_stock_hub_signature_binding_and_session_revocation(system, real_hub):
    s, hub = system, real_hub
    r = req(s, operation="send_message")
    post(s, r)
    assert signed(hub, r, key=b"x" * 32).status_code == 403
    foreign = r.model_copy(update={"agent_id": "other"})
    assert signed(hub, foreign).status_code == 403
    assert signed(hub, r).status_code == 200
    wait_status(s, r, "delivered")
    login(hub)
    assert hub.http.post("/mobile/logout").status_code == 200
    assert hub.http.post(
        "/mobile/requests/r1/observation",
        content=b"after revoke",
        headers={"Content-Type": "text/plain"},
    ).status_code in (401, 403)


def test_stock_hub_process_restart_and_v1_duplicate(system, real_hub):
    s, hub = system, real_hub
    r = req(s, operation="send_message")
    post(s, r)
    s.app.state.callback_unavailable = True
    assert signed(hub, r).status_code == 200
    # nginx's callback endpoint is unavailable while introspection stays live.
    # Restart the actual Hub executable; only its own process reopens its database.
    hub.restart()
    assert signed(hub, r).status_code == 200
    assert status(s, r)["status"] == "pending"
    login(hub)
    inbox = hub.http.get("/mobile/requests")
    assert inbox.status_code == 200
    assert [x["request_id"] for x in inbox.json()].count("r1") == 1
    s.app.state.callback_unavailable = False
    wait_status(s, r, "delivered")
    assert len({item["event"]["event_id"] for item in status(s, r)["events"]}) == 2


def test_stock_hub_does_not_implement_11_or_old_request_recovery(system, real_hub):
    s, hub = system, real_hub
    schema = hub.http.get("/openapi.json").json()
    assert "/v1/channel-requests" in schema["paths"]
    assert "/v1/nginx-qa-bridge" not in schema["paths"]
    adapter = HubClient(hub.origin, KEY, hub.http)
    with pytest.raises(BridgeError, match="CHANNEL_HUB_CONTRACT_UNAVAILABLE"):
        adapter.call("discover", bind(req(s)).model_dump())
    old = req(
        s,
        created_at=time.time() - 120,
        deadline=time.time() + 3600,
        operation="send_message",
    )
    assert signed(hub, old).status_code == 403


def test_stock_v1_does_not_consume_operation_entitlements(system, real_hub):
    """Expected incompatibility: active-only v1 introspection ignores channel scopes."""
    s, hub = system, real_hub
    r = req(s, operation="send_message")
    assert post(s, r).status_code == 202
    s.policy["projects"]["project-id"]["agents"]["builder"]["channels"]["human"] = [
        "request_observation"
    ]
    introspection = s.nginx.post(
        INTRO,
        json=bind(r).model_dump(),
        headers={"Authorization": "Bearer " + INTROSPECTION},
    ).json()
    assert introspection["active"] is True
    assert "send_message" not in introspection["channels"]["human"]
    # This is evidence of a Hub v1 contract gap, NOT a successful authorization gate.
    assert signed(hub, r).status_code == 200
