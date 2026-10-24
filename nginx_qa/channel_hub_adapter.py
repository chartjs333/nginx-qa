"""HTTP-only client for the versioned remote Hub adapter 1.1.

No Hub package, gateway object, state directory or database belongs in nginx-qa.
The remote service must implement this contract; stock Hub PR #2 does not.
"""

from __future__ import annotations

import time
from typing import Any
from urllib.parse import urlsplit

import httpx

from .channel_models import BridgeError, canonical, signature, strict_json


class HubClient:
    """Signed, bounded, fixed-origin transport for the documented 1.1 adapter."""

    def __init__(self, endpoint: str, key: bytes, client=None, clock=time.time):
        url = urlsplit(endpoint)
        if (
            url.scheme != "https"
            or not url.hostname
            or url.username
            or url.password
            or url.query
            or url.fragment
            or len(key) < 32
        ):
            raise ValueError("configured HTTPS origin and 32-byte key required")
        self.endpoint, self.key, self.clock = endpoint.rstrip("/"), key, clock
        self.client = client or httpx.Client(
            timeout=10, trust_env=False, follow_redirects=False
        )

    def call(self, action: str, payload: dict[str, Any]) -> dict[str, Any]:
        body = canonical(
            {"schema_version": "1.1", "action": action, "payload": payload}
        )
        timestamp = str(self.clock())
        try:
            with self.client.stream(
                "POST",
                self.endpoint + "/v1/nginx-qa-bridge",
                content=body,
                headers={
                    "Content-Type": "application/json",
                    "X-Timestamp": timestamp,
                    "X-Signature": signature(self.key, timestamp, body),
                },
                follow_redirects=False,
            ) as response:
                raw = bytearray()
                for chunk in response.iter_bytes():
                    if len(raw) + len(chunk) > 65536:
                        raise OSError("Hub response exceeds bound")
                    raw.extend(chunk)
                if response.status_code in (404, 405, 501):
                    raise BridgeError("CHANNEL_HUB_CONTRACT_UNAVAILABLE", 502)
                if response.status_code in (401, 403):
                    raise BridgeError("CHANNEL_HUB_DENIED")
                if response.status_code in (409, 422):
                    raise BridgeError("CHANNEL_HUB_CONFLICT", 409)
                if not response.is_success:
                    raise OSError("Hub unavailable")
                value = strict_json(bytes(raw))
                if value.get("schema_version") != "1.1":
                    raise OSError("Unsupported Hub adapter version")
                validate_response(action, payload, value)
                return value
        except (httpx.HTTPError, ValueError) as exc:
            raise OSError("Hub unavailable or invalid response") from exc

    def close(self):
        self.client.close()


def validate_response(action: str, payload: dict, value: dict) -> None:
    """A 2xx envelope alone is not a receipt for the original operation."""
    if action == "submit":
        valid = type(value.get("request_id")) is str and value[
            "request_id"
        ] == payload.get("request_id")
    elif action == "cancel":
        valid = (
            value.get("status") == "cancelled"
            and value.get("remote_effects_reversible") is False
        )
    elif action == "validate":
        if value.get("authorized") is False:
            raise BridgeError("CHANNEL_HUB_DENIED")
        valid = value.get("authorized") is True
    elif action == "status":
        valid = type(value.get("status")) is str and value["status"] in {
            "unknown",
            "queued",
            "accepted",
            "delivered",
            "human_unavailable",
            "response_received",
            "rejected",
            "failed",
            "expired",
            "cancelled",
        }
    elif action == "discover":
        channels = value.get("channels")
        valid = isinstance(channels, list) and all(
            isinstance(item, dict)
            and all(
                type(item.get(k)) is str and item[k]
                for k in ("channel_id", "project_id")
            )
            and isinstance(item.get("capabilities"), list)
            and all(type(operation) is str for operation in item["capabilities"])
            for item in channels
        )
    elif action == "artifact":
        valid = (
            all(k in value for k in ("sha256", "offset", "size", "data"))
            and type(value.get("data")) is str
        )
    else:
        raise ValueError("Unsupported bridge action")
    if not valid:
        raise ValueError("Invalid Hub adapter response")
