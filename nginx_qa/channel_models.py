"""Versioned channel contract, compatible with inbound-hub PR #2 wire models."""

from __future__ import annotations

import hashlib
import hmac
import json
import math
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

Operation = Literal[
    "send_message", "request_data", "request_observation", "request_live_session"
]
EventType = Literal[
    "accepted",
    "rejected",
    "queued",
    "delivered",
    "human_unavailable",
    "response_received",
    "failed",
    "expired",
    "cancelled",
]
TERMINAL = frozenset(
    {"response_received", "rejected", "failed", "expired", "cancelled"}
)
BINDING = ("project_id", "sprint_id", "assignment_id", "agent_id")


class BridgeError(Exception):
    def __init__(self, code: str, status: int = 403):
        self.code, self.status = code, status
        super().__init__(code)


class WireModel(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False, strict=True)


class Binding(WireModel):
    project_id: str = Field(min_length=1, max_length=256)
    sprint_id: str = Field(min_length=1, max_length=256)
    assignment_id: str = Field(min_length=1, max_length=256)
    agent_id: str = Field(min_length=1, max_length=256)


class Payload(WireModel):
    text: str = Field(default="", max_length=10000)
    grant_id: str | None = Field(default=None, min_length=1, max_length=128)
    sample_id: str | None = Field(default=None, min_length=1, max_length=256)


class CreateRequest(Binding):
    schema_version: Literal[1] = 1
    request_id: str = Field(min_length=1, max_length=128)
    idempotency_key: str = Field(min_length=1, max_length=128)
    correlation_id: str = Field(min_length=1, max_length=256)
    channel_id: str = Field(min_length=1, max_length=256)
    operation: Operation
    payload: Payload = Field(default_factory=Payload)
    created_at: float
    deadline: float
    experiment_id: str | None = Field(default=None, max_length=256)
    checkpoint_id: str | None = Field(default=None, max_length=256)
    # nginx-only dependency declaration; never placed in the Hub v1 envelope.
    blocking: bool = False

    @model_validator(mode="after")
    def bounded_lifetime(self):
        if not self.created_at < self.deadline <= self.created_at + 86400:
            raise ValueError("deadline must be within 24 hours of creation")
        return self

    def wire(self) -> dict[str, Any]:
        value = self.model_dump(exclude={"blocking"})
        value["payload"] = self.payload.model_dump(exclude_none=True)
        return value


class Provenance(Binding):
    experiment_id: str | None = Field(default=None, max_length=256)
    checkpoint_id: str | None = Field(default=None, max_length=256)
    sample_id: str | None = Field(default=None, max_length=256)
    instrument: str | None = Field(default=None, max_length=256)
    operator: str = Field(min_length=1, max_length=256)


class Event(WireModel):
    schema_version: Literal[1] = 1
    event_id: str = Field(min_length=1, max_length=128)
    request_id: str = Field(min_length=1, max_length=128)
    correlation_id: str = Field(min_length=1, max_length=256)
    channel_id: str = Field(min_length=1, max_length=256)
    event_type: EventType
    occurred_at: float
    received_at: float
    content_ref: str | None = Field(default=None, max_length=256)
    sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    source_metadata: Provenance

    @model_validator(mode="after")
    def artifact(self):
        if self.event_type == "response_received":
            if (
                not self.sha256
                or self.content_ref != "artifact://sha256/" + self.sha256
            ):
                raise ValueError("immutable content-addressed artifact required")
        elif self.content_ref is not None or self.sha256 is not None:
            raise ValueError("artifact only allowed on response")
        return self


def canonical(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()


def signature(key: bytes, timestamp: str, body: bytes) -> str:
    return hmac.new(key, timestamp.encode() + b"." + body, hashlib.sha256).hexdigest()


def verify_signature(
    key: bytes, timestamp: str, body: bytes, digest: str, now: float
) -> None:
    try:
        instant = float(timestamp)
    except (ValueError, TypeError):
        raise BridgeError("CHANNEL_SIGNATURE_INVALID") from None
    if (
        len(body) > 65536
        or not math.isfinite(instant)
        or abs(instant - now) > 60
        or not hmac.compare_digest(signature(key, timestamp, body), digest)
    ):
        raise BridgeError("CHANNEL_SIGNATURE_INVALID")


def strict_json(body: bytes) -> dict[str, Any]:
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate key")
            result[key] = value
        return result

    value = json.loads(
        body,
        object_pairs_hook=pairs,
        parse_constant=lambda _: (_ for _ in ()).throw(ValueError("nonfinite")),
    )
    if not isinstance(value, dict):
        raise ValueError("object required")  # noqa: TRY004 - malformed wire JSON
    return value


class AgentPolicy(WireModel):
    profile: Literal["neurogeneticist", "clinician", "bioinformatician"] | None = None
    expires_at: float
    revoked: bool = False
    channels: dict[str, list[Operation]]


class ProjectPolicy(WireModel):
    operations: list[Operation]
    agents: dict[str, AgentPolicy]
    required_assurance_levels: dict[Operation, int] = Field(default_factory=dict)
    max_auth_age_seconds: int = Field(default=300, ge=1, le=300)

    @model_validator(mode="after")
    def assurance_levels(self):
        if any(
            type(v) is not int or not 1 <= v <= 3
            for v in self.required_assurance_levels.values()
        ):
            raise ValueError("assurance levels must be 1..3")
        return self


class BridgePolicy(WireModel):
    schema_version: Literal[1] = 1
    hub_url: str
    introspection_token_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    introspection_projects: list[str]
    projects: dict[str, ProjectPolicy]
