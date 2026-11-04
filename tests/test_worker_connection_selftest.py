"""Worker wire qualification. All HTTP peers here are protocol fixtures.

These tests make no claim that a real Managed Sprint completed. Native self-test
start/stop is qualified separately through the registered native importer.
"""
import http.server
import json
import threading
import unittest

from nginx_qa.setup_worker_adapter import BoundedExistingClientHTTPAdapter
from nginx_qa.telegram_launcher_execution import ExecutorUnavailable

TOKEN = "wire-fixture-token-0123456789"
DESCRIPTOR = {"protocol": "nginx-qa-managed-client-v1", "operation_id": "operation-fixture",
              "project_id": "9010", "sprint_id": "msv1-" + "1" * 64, "agent_phones": ["7101", "7191", "7192"]}


class ExistingWorkerWireContract(unittest.TestCase):
    def setUp(self):
        owner = self
        self.corrupt = False
        self.observed = []
        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                if self.headers.get("Authorization") != "Bearer " + TOKEN:
                    self.send_response(401)
                    self.end_headers()
                    return
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                owner.observed.append((self.path, body))
                action = self.path.rsplit("/", 1)[1]
                result = {**body, "client_session_id": "client-fixture",
                          "status": {"prepare": "READY", "start": "ACCEPTED", "stop": "STOPPED"}[action]}
                if owner.corrupt:
                    result["project_id"] = "different-project"
                raw = json.dumps(result).encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)
            def log_message(self, *args):
                pass
        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.adapter = BoundedExistingClientHTTPAdapter(
            f"http://127.0.0.1:{self.server.server_port}",
            "env:NGINX_QA_LAUNCHER_CONTROL_FIXTURE", lambda reference: TOKEN)

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()

    def test_exact_ready_accepted_stopped_receipts_and_owned_session(self):
        ready = self.adapter.prepare(DESCRIPTOR)
        self.assertEqual(ready, {**DESCRIPTOR, "status": "READY", "client_session_id": "client-fixture"})
        accepted = self.adapter.start(DESCRIPTOR, ["assignment-native"], ready)
        self.assertEqual(accepted, {**DESCRIPTOR, "initial_assignment_ids": ["assignment-native"],
                                   "status": "ACCEPTED", "client_session_id": "client-fixture"})
        stopped = self.adapter.stop(DESCRIPTOR, ready)
        self.assertEqual(stopped, {**DESCRIPTOR, "status": "STOPPED", "client_session_id": "client-fixture"})
        self.assertEqual(self.observed[1][1]["client_session_id"], ready["client_session_id"])
        self.assertEqual(self.observed[2][1]["client_session_id"], ready["client_session_id"])

    def test_changed_native_binding_receipt_is_rejected(self):
        self.corrupt = True
        with self.assertRaises(ExecutorUnavailable):
            self.adapter.prepare(DESCRIPTOR)

    def test_rejected_auth_never_yields_execution_receipt(self):
        self.adapter.secret_resolver = lambda reference: "different-token-0123456789"
        with self.assertRaises(ExecutorUnavailable):
            self.adapter.prepare(DESCRIPTOR)
        self.assertEqual(self.observed, [])


if __name__ == "__main__":
    unittest.main()
