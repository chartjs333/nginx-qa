"""Real main.app browser harness on a random loopback port and temporary stores.

This executable helper is test-only. It reuses the normal HTTP/import/source
fixtures, no lifespan services, no existing repositories, tokens or state files.
Test-only pairing/verification endpoints must never be installed in production.
"""
import asyncio
from copy import deepcopy
import hashlib
import json
import os
import socket

from fastapi import Request
import uvicorn

import main
from nginx_qa.scope_control_migrate import migrate_document
from nginx_qa.legacy_scope_control import canonical_json_sha256
from tests.test_legacy_scope_control import LegacyScopeControlTests, asgi_request


async def run():
    fixture = LegacyScopeControlTests()
    fixture.setUp()
    original_runtime = main.managed_continuity_runtime
    main.managed_continuity_runtime = None
    browser_requests = []
    try:
        await fixture._import_sprint()
        _, current = await fixture._pre_amendment_round_trip()
        assignment_id = fixture._assignment_id(current)
        fixture._create_amendment_commit(assignment_id)
        os.environ["NGINX_QA_SCOPE_CONTROL_REPOSITORY_MAP"] = json.dumps({fixture.PROJECT_CONTEXT: str(fixture.repo_path)})
        status, result = await fixture._apply(fixture._amendment_payload(await fixture._preflight()))
        fixture.assertEqual(status, 200, result)
        await fixture._scope_and_ack(assignment_id, fixture.COORDINATOR_PHONE)
        document = json.loads(main.git_config_path.read_text(encoding="utf-8"))
        migrated, _ = migrate_document(document, migrated_at=main.utc_now(), source_sha256=canonical_json_sha256(document))
        main.git_config_path.write_text(json.dumps(migrated), encoding="utf-8")
        baseline = deepcopy(fixture._stored_assignment())
        queues = fixture._queue_snapshot()
        history_hash = hashlib.sha256(main.history_path.read_bytes()).hexdigest()
        base = f"/api/v1/projects/{fixture.PROJECT_PHONE}/sprints/{fixture.sprint_id}"
        status, created = await asgi_request(base + "/scope-requests", method="POST", headers=fixture._role_headers(fixture.COORDINATOR_PHONE), payload={
            "assignment_id": assignment_id, "expected_execution_revision": baseline["revision"], "expected_scope_revision": 1,
            "idempotency_key": "isolated-browser-scope-request", "reason": "Isolated real API browser integration", "dependencies": ["Reference proof compatibility"],
            "proposal": {"instructions": "Authorize isolated reference proof work", "retained_restrictions": ["No production integration", "No review waiver"], "node_ids": ["continuity-coordinator", "formal-linkage"], "reviewer_ids": ["reviewer-one", "reviewer-two"]},
            "source": {"repository_key": fixture.PROJECT_CONTEXT, "commit": fixture.target_commit, "path": fixture.AMENDMENT_PATH, "sha256": fixture.amendment_sha256},
        })
        fixture.assertEqual(status, 200, created)

        @main.app.post("/__test__/authorize")
        async def authorize(request: Request):
            payload = await request.json()
            status, value = await asgi_request("/api/v1/operator/session/authorize", method="POST", headers=fixture._admin_headers(), payload={"pairing_code": payload.get("pairing_code")})
            return {"http_status": status, "authorized": value.get("authorized", False), "credentials_returned": False}

        @main.app.get("/__test__/verify")
        async def verify():
            before_read = main.git_config_path.read_bytes()
            status, effective = await asgi_request(f"/api/v1/projects/{fixture.PROJECT_PHONE}/assignments/{assignment_id}/effective-scope", headers=fixture._role_headers(fixture.COORDINATOR_PHONE))
            state = fixture._stored_assignment()
            compared = ("assignments", "current_assignment_id", "current_node_id", "phase", "workflow", "pending_transition", "visit_counts", "active_task")
            return {"isolated": True, "effective_http_status": status,
                    "effective_revision": (effective.get("scope_context") or {}).get("effective_revision"),
                    "effective_profile": effective.get("effective_core", {}).get("profile"),
                    "assignment_preserved": state["current_assignment_id"] == assignment_id,
                    "execution_fields_preserved": all(state.get(key) == baseline.get(key) for key in compared),
                    "prior_assignments_preserved": state["assignments"] == baseline["assignments"],
                    "old_scope_preserved": state["scope_control"]["amendments"][0] == baseline["scope_control"]["amendments"][0],
                    "old_ack_preserved": state["scope_control"]["acknowledgements"] == baseline["scope_control"]["acknowledgements"],
                    "queues_preserved": fixture._queue_snapshot() == queues,
                    "history_preserved": hashlib.sha256(main.history_path.read_bytes()).hexdigest() == history_hash,
                    "get_effective_is_read_only": before_read == main.git_config_path.read_bytes(),
                    "browser_execution_mutations": [item for item in browser_requests if item["method"] != "GET" and any(value in item["path"] for value in ("/whoami", "/ack", "/work", "/handoff"))],
                    "browser_requests": deepcopy(browser_requests)}

        class Recorder:
            async def __call__(self, scope, receive, send):
                if scope["type"] == "http":
                    browser_requests.append({"method": scope["method"], "path": scope["path"]})
                await main.app(scope, receive, send)

        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(128)
        port = listener.getsockname()[1]
        print(json.dumps({"isolated_ui_integration": f"http://127.0.0.1:{port}/execution?project_id={fixture.PROJECT_PHONE}&sprint_id={fixture.sprint_id}", "port": port, "project_id": fixture.PROJECT_PHONE, "sprint_id": fixture.sprint_id, "assignment_id": assignment_id, "request_id": created["request_id"]}), flush=True)
        config = uvicorn.Config(Recorder(), host="127.0.0.1", port=port, lifespan="off", log_level="error", access_log=False)
        await uvicorn.Server(config).serve(sockets=[listener])
    finally:
        main.managed_continuity_runtime = original_runtime
        fixture.tearDown()


if __name__ == "__main__":
    asyncio.run(run())
