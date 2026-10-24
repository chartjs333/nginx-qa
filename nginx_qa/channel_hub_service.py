"""Composition root for unmodified Hub PR #2 plus nginx's explicit 1.1 adapter.

Run only in an explicitly configured isolated/operator-owned Hub process. Importing
this module does not start a server, touch state, or import optional Hub packages.
"""

from __future__ import annotations

import asyncio
import base64
import os
from contextlib import asynccontextmanager
from pathlib import Path

import httpx

from .channel_hub_adapter import ScopedAssignmentClient, mount_hub_adapter
from .channel_models import strict_json


def create_service(config, transport_key, assignment_token):
    from agent_channel_bridge.adapters import (
        ChannelRouter,
        ReadOnlyHttpAdapter,
    )
    from agent_channel_bridge.api import create_app
    from agent_channel_bridge.gateway import Gateway
    from agent_channel_bridge.models import Channel, Plugin
    from agent_channel_bridge.security import ArtifactStore, TrustStore
    from agent_channel_bridge.transport import CallbackClient

    keys = {
        purpose: {
            ident: base64.b64decode(value, validate=True)
            for ident, value in items.items()
        }
        for purpose, items in config["public_keys"].items()
    }
    introspection = ScopedAssignmentClient(
        config["assignment_endpoint"], assignment_token
    )
    gateway = Gateway(
        config["database"],
        transport_key,
        os.urandom(32),
        os.urandom(32),
        introspection,
        trust=TrustStore(keys),
    )
    artifacts = ArtifactStore(Path(config["artifacts"]))
    gateway.artifacts = artifacts
    adapters = {}
    for item in config["channels"]:
        channel = Channel(**item)
        gateway.register_channel(channel)
        if channel.type == "human":
            from .channel_transports import HumanChannelAdapter

            adapters[channel.channel_id] = HumanChannelAdapter()
    for item in config.get("live_session_endpoints", []):
        from .channel_transports import HumanChannelAdapter

        channel = gateway.get("channel", item["channel_id"])
        if channel["type"] != "human":
            raise ValueError("Live provider requires a human channel")
        adapters[item["channel_id"]].client.close()
        token = os.environ[item["token_env"]] if item.get("token_env") else None
        adapters[item["channel_id"]] = HumanChannelAdapter(
            item["endpoint"], token, item.get("media", "audio")
        )
    for item in config.get("read_only_endpoints", []):
        token = os.environ[item["token_env"]] if item.get("token_env") else None
        adapters[item["channel_id"]] = ReadOnlyHttpAdapter(item["endpoint"], token)
    for item in config.get("plugins", []):
        gateway.register_plugin(Plugin(**item["manifest"]), item["signature"])
    for item in config.get("checkpoints", []):
        gateway.register_checkpoint(**item)
    callback = CallbackClient(
        config["callback_endpoint"],
        httpx.Client(timeout=10, trust_env=False, follow_redirects=False),
    )
    app = create_app(
        gateway,
        artifacts=artifacts,
        idp_url=config["identity_provider"],
        origin=config["origin"],
    )
    mount_hub_adapter(app, gateway)
    router = ChannelRouter(adapters)
    stop = asyncio.Event()

    async def worker():
        while not stop.is_set():
            try:
                await asyncio.to_thread(gateway.dispatch, router)
                await asyncio.to_thread(gateway.callbacks, callback)
                app.state.channel_health = "running"
            except Exception:  # noqa: BLE001 - fail closed and expose worker health
                app.state.channel_health = "error"
            try:
                await asyncio.wait_for(stop.wait(), 1)
            except TimeoutError:
                pass

    @asynccontextmanager
    async def lifespan(_app):
        task = asyncio.create_task(worker())
        try:
            yield
        finally:
            stop.set()
            await task
            callback.close()
            introspection.base.client.close()
            for adapter in adapters.values():
                client = getattr(adapter, "client", None)
                if client is not None:
                    client.close()
            gateway.close()

    app.router.lifespan_context = lifespan
    return app


def main():
    import uvicorn

    os.umask(0o077)
    from .inbound_proposals import (
        _assert_registry_path_safe,
        _registry_acl_is_protected,
    )

    path = _assert_registry_path_safe(os.environ["BRIDGE_CONFIG"])
    if not _registry_acl_is_protected(path):
        raise ValueError("protected configuration required")
    with path.open("rb") as stream:
        raw = stream.read(1024 * 1024 + 1)
    if len(raw) > 1024 * 1024:
        raise ValueError("Hub config exceeds bound")
    config = strict_json(raw)
    app = create_service(
        config,
        base64.b64decode(os.environ["BRIDGE_TRANSPORT_KEY"], validate=True),
        os.environ["BRIDGE_ASSIGNMENT_TOKEN"],
    )
    uvicorn.run(app, host="127.0.0.1", port=config.get("port", 8081))


if __name__ == "__main__":
    main()
