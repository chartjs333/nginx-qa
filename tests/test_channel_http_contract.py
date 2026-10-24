"""Transport contract regressions; never imports or opens the Hub implementation."""

import ast
from pathlib import Path

import httpx
import pytest

from nginx_qa.channel_bridge import HubClient


@pytest.mark.parametrize(
    "action,payload,response",
    [
        (
            "submit",
            {"request_id": "original"},
            {"schema_version": "1.1", "request_id": "another"},
        ),
        ("submit", {"request_id": "original"}, {"schema_version": "1.1"}),
        ("cancel", {}, {"schema_version": "1.1", "status": "queued"}),
        ("status", {}, {"schema_version": "1.1", "status": "invented"}),
        ("discover", {}, {"schema_version": "1.1", "channels": "invalid"}),
    ],
)
def test_malformed_remote_success_is_not_an_ack(action, payload, response):
    client = httpx.Client(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json=response))
    )
    hub = HubClient("https://hub.invalid", b"k" * 32, client)
    try:
        with pytest.raises(OSError):
            hub.call(action, payload)
    finally:
        hub.close()


def test_runtime_has_no_hub_internal_imports():
    root = Path(__file__).parents[1]
    violations = []
    for path in (root / "nginx_qa").glob("channel*.py"):
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.ImportFrom) and (node.module or "").startswith(
                "agent_channel_bridge"
            ):
                violations.append(f"{path.name}:{node.lineno}")
            if isinstance(node, ast.Import):
                violations.extend(
                    f"{path.name}:{node.lineno}"
                    for item in node.names
                    if item.name.startswith("agent_channel_bridge")
                )
    assert not violations, violations


def test_non_ascii_signature_is_rejected_without_server_error():
    from nginx_qa.channel_models import BridgeError, verify_signature

    with pytest.raises(BridgeError, match="CHANNEL_SIGNATURE_INVALID"):
        verify_signature(b"k" * 32, "1000", b"{}", "\u00e9" * 64, 1000)


@pytest.mark.parametrize(
    "action,response",
    [
        ("status", {"schema_version": "1.1", "status": []}),
        ("discover", {"schema_version": "1.1", "channels": [{"channel_id": "human"}]}),
        ("validate", {"schema_version": "1.1"}),
    ],
)
def test_malformed_response_types_are_retryable_transport_errors(action, response):
    hub = HubClient(
        "https://hub.invalid",
        b"k" * 32,
        httpx.Client(
            transport=httpx.MockTransport(lambda _: httpx.Response(200, json=response))
        ),
    )
    with pytest.raises(OSError):
        hub.call(action, {})
    hub.close()


def test_negative_validation_never_authorizes():
    from nginx_qa.channel_models import BridgeError

    hub = HubClient(
        "https://hub.invalid",
        b"k" * 32,
        httpx.Client(
            transport=httpx.MockTransport(
                lambda _: httpx.Response(
                    200, json={"schema_version": "1.1", "authorized": False}
                )
            )
        ),
    )
    with pytest.raises(BridgeError, match="CHANNEL_HUB_DENIED"):
        hub.call("validate", {})
    hub.close()


@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(302, headers={"location": "https://other.invalid"}),
        httpx.Response(200, content=b"x" * 65537),
        httpx.Response(200, content=b'{"schema_version":"1.1","schema_version":"1.1"}'),
    ],
)
def test_no_redirect_or_unbounded_or_ambiguous_response(response):
    calls = []

    def send(request):
        calls.append(str(request.url))
        return response

    hub = HubClient(
        "https://hub.invalid",
        b"k" * 32,
        httpx.Client(transport=httpx.MockTransport(send)),
    )
    with pytest.raises(OSError):
        hub.call("discover", {})
    assert calls == ["https://hub.invalid/v1/nginx-qa-bridge"]
    hub.close()


def test_retired_composition_cannot_start_hub():
    from nginx_qa.channel_hub_service import main

    with pytest.raises(SystemExit, match="HTTP/HTTPS only"):
        main()
