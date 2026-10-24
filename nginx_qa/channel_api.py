"""Opt-in FastAPI surface and lifecycle for managed agent channel communication."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import os

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import ValidationError

from .channel_bridge import ChannelBridge, HubClient
from .channel_models import (
    Binding,
    BridgeError,
    BridgePolicy,
    CreateRequest,
    strict_json,
)
from .legacy_scope_control import (
    ADMIN_TOKEN_HEADER,
    ROLE_TOKEN_HEADER,
    LegacyScopeControlError,
    verify_admin_token,
)

CONFIG_ENV = "NGINX_QA_CHANNEL_BRIDGE_CONFIG"
router = APIRouter(tags=["Agent Channel Bridge"])


def load_policy():
    from .inbound_proposals import (
        _assert_registry_path_safe,
        _registry_acl_is_protected,
    )

    path = _assert_registry_path_safe(os.environ[CONFIG_ENV])
    if not _registry_acl_is_protected(path):
        raise ValueError("Bridge config ACL is unsafe")
    with path.open("rb") as stream:
        raw = stream.read(1024 * 1024 + 1)
    if len(raw) > 1024 * 1024:
        raise ValueError("Bridge config exceeds bound")
    policy = strict_json(raw)
    if policy.get("schema_version") != 1:
        raise ValueError("Unsupported bridge configuration")
    return BridgePolicy.model_validate(policy).model_dump()


def service(request: Request) -> ChannelBridge:
    bridge = getattr(request.app.state, "channel_bridge", None)
    if bridge is None:
        raise HTTPException(503, {"error": "CHANNEL_BRIDGE_DISABLED"})
    return bridge


async def body(request: Request):
    data = bytearray()
    async for chunk in request.stream():
        if len(data) + len(chunk) > 65536:
            raise HTTPException(413, {"error": "CHANNEL_BODY_TOO_LARGE"})
        data.extend(chunk)
    return bytes(data)


async def invoke(fn, *args):
    try:
        return await asyncio.to_thread(fn, *args)
    except BridgeError as exc:
        raise HTTPException(exc.status, {"error": exc.code}) from None
    except (ValidationError, ValueError, KeyError):
        raise HTTPException(422, {"error": "CHANNEL_CONTRACT_INVALID"}) from None
    except OSError:
        raise HTTPException(503, {"error": "CHANNEL_HUB_UNAVAILABLE"}) from None


def binding(project_id, sprint_id, agent_id, assignment_id):
    try:
        return Binding(
            project_id=project_id,
            sprint_id=sprint_id,
            agent_id=agent_id,
            assignment_id=assignment_id,
        )
    except ValidationError:
        raise HTTPException(422, {"error": "CHANNEL_BINDING_INVALID"}) from None


BASE = "/api/v1/projects/{project_id}/sprints/{sprint_id}/agents/{agent_id}"


@router.get(BASE + "/channels")
async def channels(
    project_id: str, sprint_id: str, agent_id: str, assignment_id: str, request: Request
):
    return await invoke(
        service(request).discover,
        binding(project_id, sprint_id, agent_id, assignment_id),
        request.headers.get(ROLE_TOKEN_HEADER, ""),
    )


@router.post(BASE + "/channel-requests", status_code=202)
async def create(project_id: str, sprint_id: str, agent_id: str, request: Request):
    bridge = service(request)
    try:
        payload = CreateRequest.model_validate(strict_json(await body(request)))
    except (ValueError, ValidationError):
        raise HTTPException(422, {"error": "CHANNEL_CONTRACT_INVALID"}) from None
    if (payload.project_id, payload.sprint_id, payload.agent_id) != (
        project_id,
        sprint_id,
        agent_id,
    ):
        raise HTTPException(409, {"error": "CHANNEL_PATH_SCOPE_MISMATCH"})
    return await invoke(
        bridge.create, payload, request.headers.get(ROLE_TOKEN_HEADER, "")
    )


@router.get(BASE + "/channel-requests")
async def mailbox(
    project_id: str,
    sprint_id: str,
    agent_id: str,
    assignment_id: str,
    request: Request,
    after: str = "",
):
    return await invoke(
        service(request).mailbox,
        binding(project_id, sprint_id, agent_id, assignment_id),
        request.headers.get(ROLE_TOKEN_HEADER, ""),
        after,
    )


@router.get(
    "/api/v1/projects/{project_id}/sprints/{sprint_id}/channel-requests/{request_id}"
)
async def status(
    project_id: str,
    sprint_id: str,
    request_id: str,
    agent_id: str,
    assignment_id: str,
    request: Request,
):
    return await invoke(
        service(request).status,
        binding(project_id, sprint_id, agent_id, assignment_id),
        request.headers.get(ROLE_TOKEN_HEADER, ""),
        request_id,
    )


@router.post(BASE + "/channel-requests/{request_id}/{action}")
async def change(
    project_id: str,
    sprint_id: str,
    agent_id: str,
    request_id: str,
    action: str,
    assignment_id: str,
    request: Request,
):
    if action not in ("cancel", "ack"):
        raise HTTPException(404, {"error": "CHANNEL_ACTION_UNKNOWN"})
    return await invoke(
        service(request).change,
        binding(project_id, sprint_id, agent_id, assignment_id),
        request.headers.get(ROLE_TOKEN_HEADER, ""),
        request_id,
        action,
    )


@router.post("/api/v1/integrations/inbound-hub/agent-introspection")
async def introspect(request: Request):
    bridge = service(request)
    try:
        payload = Binding.model_validate(strict_json(await body(request)))
    except (ValidationError, ValueError):
        raise HTTPException(422, {"error": "CHANNEL_CONTRACT_INVALID"}) from None
    values = request.headers.getlist("authorization")
    if len(values) != 1 or not values[0].startswith("Bearer "):
        raise HTTPException(403, {"error": "CHANNEL_INTROSPECTION_DENIED"})
    return await invoke(bridge.introspect, payload, values[0][7:])


@router.post("/api/v1/integrations/inbound-hub/channel-events")
async def callback(request: Request):
    bridge = service(request)
    if any(
        len(request.headers.getlist(k)) != 1 for k in ("x-timestamp", "x-signature")
    ):
        raise HTTPException(403, {"error": "CHANNEL_SIGNATURE_INVALID"})
    return await invoke(
        bridge.callback,
        await body(request),
        request.headers["x-timestamp"],
        request.headers["x-signature"],
    )


@router.get(BASE + "/channel-requests/{request_id}/artifact")
async def artifact(
    project_id: str,
    sprint_id: str,
    agent_id: str,
    request_id: str,
    assignment_id: str,
    request: Request,
):
    bridge = service(request)
    owner = binding(project_id, sprint_id, agent_id, assignment_id)
    token = request.headers.get(ROLE_TOKEN_HEADER, "")

    def download():
        status = bridge.status(owner, token, request_id)
        events = [
            e["event"]
            for e in status["events"]
            if e["disposition"] == "applied"
            and e["event"]["event_type"] == "response_received"
        ]
        if status["status"] != "response_received" or len(events) != 1:
            raise BridgeError("CHANNEL_ARTIFACT_UNAVAILABLE", 409)
        digest = events[0]["sha256"]
        envelope = CreateRequest.model_validate(status["request"])
        data = bytearray()
        total = None
        while total is None or len(data) < total:
            bridge.authorize_agent(owner, token)
            bridge.allowed(envelope)
            part = bridge.hub.call(
                "artifact",
                {"request": envelope.wire(), "digest": digest, "offset": len(data)},
            )
            if (
                part.get("sha256") != digest
                or part.get("offset") != len(data)
                or type(part.get("size")) is not int
                or not 0 < part["size"] <= 5_000_000
                or (total is not None and total != part["size"])
            ):
                raise BridgeError("CHANNEL_ARTIFACT_INTEGRITY", 502)
            total = part["size"]
            chunk = base64.b64decode(part["data"], validate=True)
            if not chunk or len(chunk) > 32768 or len(data) + len(chunk) > total:
                raise BridgeError("CHANNEL_ARTIFACT_INTEGRITY", 502)
            data.extend(chunk)
        if hashlib.sha256(data).hexdigest() != digest:
            raise BridgeError("CHANNEL_ARTIFACT_INTEGRITY", 502)
        return bytes(data)

    result = await invoke(download)
    return Response(
        result,
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": 'attachment; filename="external-observation.bin"',
            "X-Content-Type-Options": "nosniff",
            "Cache-Control": "no-store",
        },
    )


async def start(app, runtime):
    if not os.environ.get(CONFIG_ENV):
        return
    if runtime is None:
        raise RuntimeError("Channel bridge requires managed runtime")
    policy = load_policy()
    key = base64.b64decode(os.environ["NGINX_QA_CHANNEL_TRANSPORT_KEY"], validate=True)
    hub = HubClient(policy["hub_url"], key)
    bridge = ChannelBridge(runtime, hub, key, load_policy)
    app.state.channel_bridge = bridge
    app.state.channel_stop = asyncio.Event()
    app.state.channel_health = {"worker": "starting"}

    async def worker():
        while not app.state.channel_stop.is_set():
            try:
                await asyncio.to_thread(bridge.tick)
                app.state.channel_health = {"worker": "running"}
            except Exception:  # noqa: BLE001 - worker remains alive, health records failure
                # No credential-bearing exception text in health/log responses.
                app.state.channel_health = {"worker": "error"}
            try:
                await asyncio.wait_for(app.state.channel_stop.wait(), 1)
            except TimeoutError:
                pass

    app.state.channel_task = asyncio.create_task(worker())


async def stop(app):
    task = getattr(app.state, "channel_task", None)
    if task is not None:
        app.state.channel_stop.set()
        await task
        app.state.channel_bridge.hub.close()
        app.state.channel_task = None
        app.state.channel_bridge = None


@router.get("/api/v1/projects/{project_id}/sprints/{sprint_id}/channel-requests")
async def operator_requests(
    project_id: str, sprint_id: str, request: Request, after: str = ""
):
    bridge = service(request)
    try:
        verify_admin_token(request.headers.get(ADMIN_TOKEN_HEADER))
    except LegacyScopeControlError as exc:
        raise HTTPException(exc.status_code, {"error": exc.code}) from None

    def read():
        with bridge.db() as db:
            rows = db.execute(
                "SELECT * FROM channel_requests WHERE project_id=? AND sprint_id=? AND id>? ORDER BY id LIMIT 100",
                (project_id, sprint_id, after),
            ).fetchall()
            # Operator sees provenance/status, never credential-bearing HTTP headers.
            return {
                "requests": [bridge.view(row) for row in rows],
                "worker": getattr(
                    request.app.state, "channel_health", {"worker": "not_started"}
                ),
                "next_cursor": rows[-1]["id"] if len(rows) == 100 else None,
            }

    return await invoke(read)
