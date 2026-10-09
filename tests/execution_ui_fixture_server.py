"""Isolated browser fixture server; never imports main or accesses live files.

Run ``python -m tests.execution_ui_fixture_server`` and use its ephemeral loopback
port. This exercises the real projection and UI, but deliberately NOT deployment
or production authentication. Backend mutation semantics have separate tests.
"""
from copy import deepcopy
from difflib import unified_diff
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from nginx_qa.execution_observability import append_execution_checkpoint, project_execution
from tests.test_execution_observability import delta_fixture, managed_fixture

ASSETS = Path(__file__).resolve().parents[1] / "nginx_qa" / "static"
SNAPSHOTS = {"9000": delta_fixture(), "9011": managed_fixture()}
LEDGERS = {key: {} for key in SNAPSHOTS}
REQUEST = {"request_id": "fixture-scope-request", "assignment_id": SNAPSHOTS["9000"]["execution"]["current_assignment_id"], "base_scope_revision": 1,
           "status": "pending", "reason": "Isolated UI fixture: permit reference-only proof", "dependencies": ["Reference provenance"],
           "proposal": {"instructions": "Reference-only proof and source-bound tests", "retained_restrictions": ["No production integration", "No review waiver"], "node_ids": ["continuity-coordinator", "formal-linkage"], "reviewer_ids": ["review-1", "review-2"]},
           "source": {"commit": "fixture-commit", "path": "fixture.json", "sha256": "fixture-hash"}, "created_at": "2026-10-05T11:00:00Z"}
SNAPSHOTS["9000"]["scope_requests"] = [REQUEST]
REQUESTS = []
VALIDATION = None
AUTHENTICATED = True
FAIL_READ = False
for key, snapshot in SNAPSHOTS.items():
    append_execution_checkpoint(LEDGERS[key], snapshot, recorded_at="2026-10-05T11:00:00Z")


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def respond(self, value, code=200, content_type="application/json; charset=utf-8"):
        data = json.dumps(value, ensure_ascii=False).encode() if "json" in content_type else value
        self.send_response(code); self.send_header("Content-Type", content_type); self.send_header("Content-Length", str(len(data))); self.end_headers(); self.wfile.write(data)

    def do_GET(self):
        global AUTHENTICATED, FAIL_READ
        parsed = urlsplit(self.path); path = parsed.path
        REQUESTS.append({"method": "GET", "path": path})
        if path == "/execution":
            return self.respond((ASSETS / "execution.html").read_bytes(), content_type="text/html; charset=utf-8")
        if path.startswith("/execution/assets/") and path.rsplit("/", 1)[-1] in {"execution.js", "execution.css"}:
            name = path.rsplit("/", 1)[-1]
            return self.respond((ASSETS / name).read_bytes(), content_type="application/javascript; charset=utf-8" if name.endswith("js") else "text/css; charset=utf-8")
        if path == "/api/v1/operator/session":
            return self.respond({"authenticated": AUTHENTICATED, "csrf_token": "fixture-not-a-credential" if AUTHENTICATED else None, "pairing_code": None if AUTHENTICATED else "FIXTURE"})
        if path == "/api/v1/execution-catalog":
            return self.respond({"projects": [{"project_id": key, "name": "ISOLATED FIXTURE · " + ("Delta" if key == "9000" else "Managed parallel"), "sprints": [{"sprint_id": item["sprint_id"]}]} for key, item in SNAPSHOTS.items()]})
        if path.endswith("/observability"):
            if FAIL_READ:
                return self.respond({"detail": {"error": "FIXTURE_DISCONNECTED"}}, 503)
            key = path.split("/")[4]; query = parse_qs(parsed.query)
            return self.respond(project_execution(SNAPSHOTS[key], checkpoints=LEDGERS[key]["checkpoints"], at_checkpoint=query.get("at_checkpoint", [None])[0], cursor=query.get("cursor", [None])[0]))
        if path == "/__test__/requests":
            return self.respond(REQUESTS)
        return self.respond({"error": "fixture route not found"}, 404)

    def do_POST(self):
        global VALIDATION, AUTHENTICATED, FAIL_READ
        path = urlsplit(self.path).path
        data = json.loads(self.rfile.read(int(self.headers.get("Content-Length", "0"))) or b"{}")
        REQUESTS.append({"method": "POST", "path": path})
        if path == "/__test__/control":
            if "authenticated" in data: AUTHENTICATED = bool(data["authenticated"])
            if "fail_read" in data: FAIL_READ = bool(data["fail_read"])
            if data.get("advance"):
                SNAPSHOTS["9000"]["execution"]["revision"] += 1
                append_execution_checkpoint(LEDGERS["9000"], SNAPSHOTS["9000"], recorded_at="2026-10-05T12:00:00Z")
            return self.respond({"fixture_controlled": True})
        if not AUTHENTICATED:
            return self.respond({"detail": {"error": "OPERATOR_SESSION_REQUIRED"}}, 401)
        if path.endswith("/validate"):
            before = {"profile": "Proof-only", "tasks": []}; after = {"profile": data["proposal"]["instructions"], "restrictions": data["proposal"]["retained_restrictions"]}
            VALIDATION = {"validation_id": "fixture-validation", "proposal": deepcopy(data["proposal"]), "before_effective": before, "after_effective": after, "execution_revision": SNAPSHOTS["9000"]["execution"]["revision"], "scope_revision": 1,
                          "diff": "\n".join(unified_diff(json.dumps(before, indent=2).splitlines(), json.dumps(after, indent=2).splitlines(), fromfile="current", tofile="validated"))}
            return self.respond(VALIDATION)
        if path.endswith("/decisions"):
            if data.get("expected_execution_revision") != SNAPSHOTS["9000"]["execution"]["revision"]:
                return self.respond({"detail": {"error": "SCOPE_EXECUTION_REVISION_CONFLICT"}}, 409)
            REQUEST["status"] = "rejected" if data["action"] == "reject" else "applied"
            REQUEST["decision"] = {"decision_id": "fixture-decision", "action": data["action"], "decided_at": "2026-10-05T11:10:00Z"}
            if data["action"] != "reject":
                state = SNAPSHOTS["9000"]["execution"]; control = state["scope_control"]; binding = control["assignment_bindings"][state["current_assignment_id"]]
                binding["effective_revision"] = 2; binding["scope_context"]["effective_revision"] = 2; binding["effective_core"] = deepcopy(VALIDATION["after_effective"])
                control["amendments"].append({"amendment_id": "fixture-2", "effective_revision": 2, "applied_at": "2026-10-05T11:10:00Z"})
                # Break fixture's intentionally shared first-context reference.
                control["acknowledgements"][state["current_assignment_id"]]["scope_context"] = {"effective_revision": 1}
            append_execution_checkpoint(LEDGERS["9000"], SNAPSHOTS["9000"], recorded_at="2026-10-05T11:10:00Z")
            return self.respond({"status": REQUEST["status"]})
        return self.respond({"error": "fixture mutation not supported"}, 404)


if __name__ == "__main__":
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    print("ISOLATED_UI_FIXTURE http://127.0.0.1:" + str(server.server_port) + "/execution", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.server_close()
