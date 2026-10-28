#!/usr/bin/env python3
"""Operator-only pilot launcher for NGINX QA managed_workspace_v1.

Default DRY RUN: no HTTP, no Git mutation. --execute posts to a loopback-only
qualified staging server. Never runs the full/night sprint.
"""
import argparse
import json
import re
import sys
from urllib import request, error, parse

GIT_ADDRESS = "https://github.com/chartjs333/nginx-qa.git"
CONTEXT_KEY = "github.com/chartjs333/nginx-qa#madoev-eeg-pilot-v1"
REF = "refs/heads/project/madoev-eeg-lab"
MANIFEST = "projects/madoev-eeg/orchestration/pilot_managed_sprint_v1.json"
IDEMPOTENCY_KEY = "madoev-eeg-pilot-ar04-20261028-v1"

def post_json(base, path, payload):
    req = request.Request(
        base + path,
        data=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    opener = request.build_opener(request.ProxyHandler({}), NoRedirect())
    try:
        with opener.open(req, timeout=30) as resp:
            body = resp.read(1048577)
            if len(body) > 1048576:
                raise RuntimeError("Oversized server response")
            if resp.geturl() != base + path:
                raise RuntimeError("Unexpected redirect")
            return resp.status, json.loads(body)
    except error.HTTPError as exc:
        detail = exc.read(2048).decode("utf-8", errors="replace")
        raise RuntimeError("NGINX QA HTTP %s: %s" % (exc.code, detail)) from exc

class NoRedirect(request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:18025")
    parser.add_argument("--execute", action="store_true",
                        help="Actually register the isolated project and start ONE pilot task")
    args = parser.parse_args()
    parsed = parse.urlsplit(args.base_url)
    if (parsed.scheme != "http" or parsed.hostname != "127.0.0.1"
        or parsed.port != 18025 or parsed.path not in ("", "/")
        or parsed.username or parsed.password or parsed.query or parsed.fragment):
        parser.error("Only exact staging loopback http://127.0.0.1:18025 is allowed")
    base = "http://127.0.0.1:18025"
    project_payload = {
        "git_address": GIT_ADDRESS,
        "git_context_key": CONTEXT_KEY,
        "project_name": "Madoev EEG isolated pilot",
    }
    start_payload = {
        "repository_id": "main",
        "ref": REF,
        "manifest_path": MANIFEST,
        "idempotency_key": IDEMPOTENCY_KEY,
    }
    if not args.execute:
        print(json.dumps({
            "status": "DRY_RUN_NO_HTTP",
            "server": base,
            "project_registration": {"path": "/project-manager/0001", "payload": project_payload},
            "sprint_start_path_template": "/api/v1/projects/{project_phone}/sprints/start-from-git",
            "sprint_start_payload": start_payload,
            "note": "To execute on an independently configured Windows staging host, pass --execute. No full/night run."
        }, ensure_ascii=False, indent=2))
        return 0
    status, response = post_json(base, "/project-manager/0001", project_payload)
    phone = response.get("project_phone")
    actual_key = (response.get("project") or {}).get("git_context_key")
    if status != 200 or not isinstance(phone, str) or not re.fullmatch(r"9[0-9]{3}", phone) or actual_key != CONTEXT_KEY:
        raise RuntimeError("Isolated project identity not confirmed; refusing sprint start")
    path = "/api/v1/projects/%s/sprints/start-from-git" % phone
    start_status, sprint = post_json(base, path, start_payload)
    if start_status not in (200, 201) or sprint.get("status") != "active":
        raise RuntimeError("NGINX QA did not confirm active pilot")
    print(json.dumps({
        "status": "PILOT_STARTED",
        "project_phone": phone,
        "sprint_id": sprint.get("sprint_id"),
        "start_http_status": start_status,
        "deduplicated": sprint.get("deduplicated"),
        "initial_assignment_ids": sprint.get("initial_assignment_ids"),
        "note": "Only AR-04 pilot was requested. Completion, reviewer approvals and evidence still require verification."
    }, ensure_ascii=False, indent=2))
    return 0

if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (RuntimeError, OSError, ValueError) as exc:
        print("Pilot not confirmed:", exc, file=sys.stderr)
        raise SystemExit(2)
