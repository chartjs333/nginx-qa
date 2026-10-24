"""Contract simulator for the missing remote 1.1 API. NOT Inbound Hub.

Runs as a separate HTTPS process; only this process can inspect its SQLite state.
Its test-control API is loopback-only and never shipped as a runtime service.
"""

import base64
import hashlib
import hmac
import json
import os
import sqlite3
import ssl
import tempfile
import threading
import time
from pathlib import Path

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException, Request


def main():
    config = json.loads(os.environ["HTTP_PEER_CONFIG"])
    private = tempfile.TemporaryDirectory(prefix="http-contract-peer-")
    db = sqlite3.connect(Path(private.name) / "own.sqlite3", check_same_thread=False)
    db.execute("CREATE TABLE requests(id TEXT PRIMARY KEY, body TEXT, status TEXT)")
    db.execute(
        "CREATE TABLE events(id TEXT PRIMARY KEY, body TEXT, delivered INTEGER DEFAULT 0)"
    )
    lock = threading.RLock()
    key = bytes.fromhex(config["key"])
    outbound = httpx.Client(
        verify=ssl.create_default_context(cadata=config["ca"]),
        trust_env=False,
        timeout=5,
    )
    app = FastAPI()
    modes = {}

    def sign(stamp, body):
        return hmac.new(key, stamp.encode() + b"." + body, hashlib.sha256).hexdigest()

    def pump():
        for ident, body in db.execute(
            "SELECT id,body FROM events WHERE delivered=0"
        ).fetchall():
            stamp = str(time.time())
            try:
                r = outbound.post(
                    config["callback"],
                    content=body.encode(),
                    headers={
                        "X-Timestamp": stamp,
                        "X-Signature": sign(stamp, body.encode()),
                    },
                )
                if r.status_code == 200:
                    db.execute("UPDATE events SET delivered=1 WHERE id=?", (ident,))
                    db.commit()
            except httpx.HTTPError:
                pass

    @app.get("/health")
    def health():
        return {"mode": "contract_simulator"}

    @app.post("/__test/control")
    def control(value: dict):
        with lock:
            modes.update(value.get("modes", {}))
            if "event" in value:
                event = value["event"]
                db.execute(
                    "INSERT OR IGNORE INTO events(id,body) VALUES(?,?)",
                    (event["event_id"], json.dumps(event)),
                )
                db.execute(
                    "UPDATE requests SET status=? WHERE id=?",
                    (event["event_type"], event["request_id"]),
                )
                db.commit()
            if value.get("pump"):
                pump()
            return {
                "requests": db.execute("SELECT count(*) FROM requests").fetchone()[0],
                "pending_events": db.execute(
                    "SELECT count(*) FROM events WHERE delivered=0"
                ).fetchone()[0],
            }

    @app.post("/v1/nginx-qa-bridge")
    async def bridge(request: Request):
        body = await request.body()
        stamp = request.headers.get("x-timestamp", "")
        try:
            valid = abs(float(stamp) - time.time()) <= 60
        except ValueError:
            valid = False
        if not valid or not hmac.compare_digest(
            sign(stamp, body), request.headers.get("x-signature", "")
        ):
            raise HTTPException(403)
        envelope = json.loads(body)
        if envelope.get("schema_version") != "1.1":
            raise HTTPException(422)
        action, value = envelope["action"], envelope["payload"]
        # Use a thread because introspection is a separate service HTTP roundtrip.
        import asyncio

        return await asyncio.to_thread(execute, action, value)

    def execute(action, value):
        with lock:
            if modes.get("missing_contract"):
                raise HTTPException(404)
            if modes.get("unavailable"):
                raise HTTPException(503)
            if modes.get("deny"):
                raise HTTPException(403)
            request = value["request"] if action == "artifact" else value
            binding = {
                k: request[k]
                for k in ("project_id", "sprint_id", "assignment_id", "agent_id")
            }
            if action != "cancel":
                auth = outbound.post(
                    config["introspection"],
                    json=binding,
                    headers={"Authorization": "Bearer " + config["token"]},
                )
                allowed = auth.json()
                if (
                    auth.status_code != 200
                    or allowed.get("active") is not True
                    or any(allowed.get(k) != v for k, v in binding.items())
                ):
                    raise HTTPException(403)
                if action != "discover" and request["operation"] not in allowed.get(
                    "channels", {}
                ).get(request["channel_id"], []):
                    raise HTTPException(403)
            result = {"schema_version": "1.1"}
            if action == "discover":
                result["channels"] = [
                    {
                        "channel_id": k,
                        "project_id": binding["project_id"],
                        "type": "human",
                        "capabilities": v,
                    }
                    for k, v in allowed["channels"].items()
                ]
            elif action == "submit":
                old = db.execute(
                    "SELECT body FROM requests WHERE id=?", (request["request_id"],)
                ).fetchone()
                encoded = json.dumps(request, sort_keys=True)
                if old and old[0] != encoded:
                    raise HTTPException(409)
                db.execute(
                    "INSERT OR IGNORE INTO requests VALUES(?,?,?)",
                    (request["request_id"], encoded, "accepted"),
                )
                db.commit()
                if modes.pop("lose_ack", False):
                    raise HTTPException(503)
                result["request_id"] = (
                    "wrong" if modes.get("wrong_ack") else request["request_id"]
                )
            elif action == "cancel":
                db.execute(
                    'UPDATE requests SET status="cancelled" WHERE id=?',
                    (request["request_id"],),
                )
                db.commit()
                result.update(status="cancelled", remote_effects_reversible=False)
            elif action == "status":
                row = db.execute(
                    "SELECT status FROM requests WHERE id=?", (request["request_id"],)
                ).fetchone()
                result["status"] = row[0] if row else "unknown"
            elif action == "artifact":
                data = b"Independent HTTP observation"
                offset = value["offset"]
                result.update(
                    sha256=hashlib.sha256(data).hexdigest(),
                    offset=offset,
                    size=len(data),
                    data=base64.b64encode(data[offset : offset + 32768]).decode(),
                )
            elif action == "validate":
                result["authorized"] = True
            else:
                raise HTTPException(422)
            return result

    try:
        uvicorn.run(
            app,
            host="127.0.0.1",
            port=config["port"],
            ssl_certfile=config["cert"],
            ssl_keyfile=config["tls_key"],
            log_level="error",
        )
    finally:
        db.close()
        private.cleanup()


if __name__ == "__main__":
    main()
