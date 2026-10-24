"""Opt-in v1.1 interoperability adapter, owned by nginx-qa.

Mount alongside inbound-hub PR #2's unmodified create_app(gateway). This module
imports Hub models only when mounted. It does not copy/replace Hub authorization,
mobile identity verification, adapters, dispatch or callback machinery.
"""

from __future__ import annotations

import base64
import hashlib
import json
import time
from typing import Any

from fastapi import HTTPException, Request

from .channel_models import (
    BINDING,
    Binding,
    BridgeError,
    canonical,
    signature,
    strict_json,
    verify_signature,
)


def mount_hub_adapter(app, gateway, *, clock=time.time):
    from agent_channel_bridge.models import Request as HubRequest

    if not isinstance(gateway.active_assignment, ScopedAssignmentClient):
        raise TypeError("Adapter requires operation-scoped live introspection")
    gateway.active_assignment.gateway = gateway
    with gateway.lock, gateway.db:
        gateway.db.executescript("""
        CREATE TABLE IF NOT EXISTS nginx_bridge_receipts(
            id TEXT PRIMARY KEY, project TEXT NOT NULL, dedupe TEXT NOT NULL,
            fingerprint TEXT NOT NULL, body TEXT NOT NULL, cancelled INTEGER NOT NULL DEFAULT 0,
            UNIQUE(project,dedupe));
        """)

    def matching(request, stored):
        return (
            all(getattr(request, k) == getattr(stored, k) for k in BINDING)
            and request.channel_id == stored.channel_id
        )

    def authorize(request):
        channel = gateway.authorize(request)
        # For protected equipment, the actual Gateway performs session, researcher,
        # plugin/activation/version/hash/assurance/revocation checks. Human asynchronous
        # messaging remains possible without an online session, as required by v1.
        if channel.type == "device" and request.operation not in (
            "request_data",
            "request_observation",
        ):
            raise PermissionError("Instrument actuation is excluded")
        return channel

    def execute(action: str, payload: dict[str, Any]):
        if action == "discover":
            binding = Binding.model_validate(payload)
            probe = HubRequest(
                **binding.model_dump(),
                request_id="discovery",
                idempotency_key="discovery",
                correlation_id="discovery",
                channel_id="discovery",
                operation="send_message",
                created_at=clock(),
            )
            if not gateway.active_assignment.base(probe):
                raise PermissionError("Inactive assignment")
            channels = []
            for (raw,) in gateway.db.execute(
                "SELECT value FROM objects WHERE kind='channel'"
            ):
                value = json.loads(raw)
                if (
                    value["project_id"] == binding.project_id
                    and binding.agent_id in value["agents"]
                ):
                    value.pop("agents", None)
                    try:
                        presence = gateway.get("presence", value["channel_id"])
                    except PermissionError:
                        presence = {"online": False, "at": 0}
                    value["availability"] = (
                        "online"
                        if presence["online"] and 0 <= clock() - presence["at"] <= 60
                        else "offline"
                    )
                    value["grant_required"] = value["type"] != "human"
                    if value["plugin_id"]:
                        plugin = gateway.get("plugin", value["plugin_id"])
                        value["plugin"] = {
                            k: plugin[k]
                            for k in (
                                "plugin_id",
                                "version",
                                "runtime_target",
                                "models",
                                "capabilities",
                                "sha256",
                                "minimum_app_version",
                                "approved",
                            )
                        }
                    channels.append(value)
            return {"channels": channels}
        if action == "artifact":
            if set(payload) != {"request", "digest", "offset"}:
                raise ValueError("Invalid artifact request")
            request = HubRequest.model_validate(payload["request"])
        else:
            request = HubRequest.model_validate(payload)
        raw = gateway.db.execute(
            "SELECT body,status FROM requests WHERE id=?", (request.request_id,)
        ).fetchone()
        if raw and not matching(request, HubRequest.model_validate_json(raw[0])):
            raise PermissionError("Request scope mismatch")
        if action == "cancel":
            # Persistent tombstone also fences a submit already in flight. Cancellation
            # can only suppress work; it does not require a still-valid equipment grant.
            body = canonical(request.model_dump()).decode()
            fingerprint = hashlib.sha256(body.encode()).hexdigest()
            receipt = gateway.db.execute(
                "SELECT fingerprint FROM nginx_bridge_receipts WHERE id=?",
                (request.request_id,),
            ).fetchone()
            if receipt and receipt[0] != fingerprint:
                raise ValueError("Cancellation conflict")
            with gateway.db:
                gateway.db.execute(
                    "INSERT OR IGNORE INTO nginx_bridge_receipts VALUES(?,?,?,?,?,1)",
                    (
                        request.request_id,
                        request.project_id,
                        request.idempotency_key,
                        fingerprint,
                        body,
                    ),
                )
                gateway.db.execute(
                    "UPDATE nginx_bridge_receipts SET cancelled=1 WHERE id=?",
                    (request.request_id,),
                )
            if raw:
                gateway.cancel(request.request_id, request.project_id)
            return {"status": "cancelled", "remote_effects_reversible": False}
        authorize(request)
        if action == "validate":
            return {"authorized": True}
        if action == "status":
            return {"status": raw[1] if raw else "unknown"}
        if action == "artifact":
            digest, offset = payload["digest"], payload["offset"]
            if (
                not raw
                or raw[1] != "response_received"
                or type(offset) is not int
                or not 0 <= offset < 5_000_000
            ):
                raise ValueError("Artifact unavailable")
            events = gateway.db.execute(
                "SELECT body FROM events WHERE json_extract(body,'$.request_id')=?",
                (request.request_id,),
            ).fetchall()
            if not any(
                json.loads(e[0]).get("sha256") == digest
                and json.loads(e[0]).get("event_type") == "response_received"
                for e in events
            ):
                raise PermissionError("Unbound artifact")
            data = gateway.artifacts.read(digest)
            if len(data) > 5_000_000:
                raise ValueError("Artifact exceeds bound")
            return {
                "sha256": digest,
                "size": len(data),
                "offset": offset,
                "data": base64.b64encode(data[offset : offset + 32768]).decode(),
            }
        if action != "submit":
            raise ValueError("Unknown bridge action")
        if request.deadline is None or request.deadline <= clock():
            raise PermissionError("Expired request")
        body = canonical(request.model_dump()).decode()
        fingerprint = hashlib.sha256(body.encode()).hexdigest()
        receipt = gateway.db.execute(
            "SELECT fingerprint,cancelled FROM nginx_bridge_receipts WHERE id=? OR (project=? AND dedupe=?)",
            (request.request_id, request.project_id, request.idempotency_key),
        ).fetchall()
        if receipt and (len(receipt) != 1 or receipt[0][0] != fingerprint):
            raise ValueError("Idempotency conflict")
        if receipt and receipt[0][1]:
            raise PermissionError("Cancelled request")
        if raw:
            # Recovery after Gateway commits but HTTP response / adapter acknowledgement is lost.
            stored = HubRequest.model_validate_json(raw[0]).model_dump()
            expected = request.model_dump()
            stored.pop("created_at")
            expected.pop("created_at")
            if stored != expected:
                raise ValueError("Idempotency conflict")
            return {"request_id": request.request_id}
        if not receipt:
            with gateway.db:
                gateway.db.execute(
                    "INSERT INTO nginx_bridge_receipts VALUES(?,?,?,?,?,0)",
                    (
                        request.request_id,
                        request.project_id,
                        request.idempotency_key,
                        fingerprint,
                        body,
                    ),
                )
        # Version 1.1 retains the original envelope durably in receipts, and assigns the
        # v1 transport creation time at first delivery. Retrying never changes stored v1 bytes.
        fresh = request.model_copy(update={"created_at": clock()})
        wire = fresh.model_dump_json().encode()
        stamp = str(clock())
        ident = gateway.intake(
            wire, stamp, signature(gateway.transport_key, stamp, wire)
        )
        return {"request_id": ident}

    @app.post("/v1/nginx-qa-bridge")
    async def bridge(request: Request):
        body = bytearray()
        async for chunk in request.stream():
            if len(body) + len(chunk) > 65536:
                raise HTTPException(413, "Request too large")
            body.extend(chunk)
        try:
            verify_signature(
                gateway.transport_key,
                request.headers.get("x-timestamp", ""),
                bytes(body),
                request.headers.get("x-signature", ""),
                clock(),
            )
            envelope = strict_json(bytes(body))
            if (
                set(envelope) != {"schema_version", "action", "payload"}
                or envelope["schema_version"] != "1.1"
            ):
                raise ValueError("Unsupported contract")
            # Gateway's lock also excludes its dispatch worker and mobile mutations.
            # Run in thread: active_assignment may synchronously call the nginx ASGI server.
            import asyncio

            def locked():
                with gateway.lock:
                    return execute(envelope["action"], envelope["payload"])

            result = await asyncio.to_thread(locked)
            return {"schema_version": "1.1", **result}
        except (PermissionError, BridgeError):
            raise HTTPException(403, "Channel authorization denied") from None
        except (ValueError, KeyError, TypeError):
            raise HTTPException(422, "Invalid channel contract or conflict") from None


class ScopedAssignmentClient:
    """Preserve Hub's exact binding introspection and add nginx operation entitlement.

    Use this callable for Gateway.active_assignment when mounting the 1.1 adapter;
    it rechecks nginx policy at *each* Gateway authorize, including fallback/retry.
    """

    def __init__(self, endpoint, token, client=None):
        from agent_channel_bridge.introspection import ActiveAssignmentClient

        self.base = ActiveAssignmentClient(endpoint, token, client=client)
        self.gateway = None

    def __call__(self, request):
        import httpx

        binding = {k: getattr(request, k) for k in BINDING}
        try:
            response = self.base.client.post(
                self.base.endpoint,
                json=binding,
                headers={"Authorization": "Bearer " + self.base.token},
                follow_redirects=False,
            )
            response.raise_for_status()
            value = response.json()
            if value.get("active") is not True or any(
                value.get(k) != v for k, v in binding.items()
            ):
                return False
            actions = value.get("channels", {}).get(request.channel_id, [])
            required = value.get("required_assurance_levels", {}).get(
                request.operation, 1
            )
            if type(required) is not int or not 1 <= required <= 3:
                return False
            if required > 1:
                if self.gateway is None:
                    return False
                grant = self.gateway.get("grant", request.payload.get("grant_id", ""))
                session = self.gateway.session(grant["session"])
                channel = self.gateway.get("channel", request.channel_id)
                authenticated_at = session["authenticated_at"]
                for (raw,) in self.gateway.db.execute(
                    "SELECT value FROM objects JOIN used_proofs ON objects.id=used_proofs.id WHERE kind='challenge'"
                ):
                    challenge = json.loads(raw)
                    if all(
                        challenge.get(k) == v
                        for k, v in {
                            "session_id": grant["session"],
                            "action": request.operation,
                            "device": channel.get("device_id"),
                            "plugin": channel.get("plugin_id"),
                            "user": session["user"],
                            "project": request.project_id,
                            "workstation": session["workstation"],
                        }.items()
                    ):
                        # Challenge lifetime is exactly 60 seconds in pinned Hub v1.
                        # Creation time is a conservative lower bound on proof verification time.
                        authenticated_at = max(
                            authenticated_at, challenge["expires"] - 60
                        )
                if (
                    grant["revoked"]
                    or grant["expires"] <= self.gateway.clock()
                    or grant["agent"] != request.agent_id
                    or grant["channel"] != request.channel_id
                    or grant["action"] != request.operation
                    or grant["assurance"] < required
                    or session["project"] != request.project_id
                    or session["user"] != channel["owner"]
                    or self.gateway.clock() - authenticated_at
                    > value.get("max_auth_age_seconds", 300)
                ):
                    return False
            # Live attempts may fall back to asynchronous messaging inside Gateway.
            return request.operation in actions and (
                request.operation != "request_live_session" or "send_message" in actions
            )
        except (
            httpx.HTTPError,
            ValueError,
            AttributeError,
            TypeError,
            KeyError,
            PermissionError,
        ):
            return False
