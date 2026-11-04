r"""Isolated security-boundary tests: fake sockets, fake key, fixture CIM data.

Run: C:\Python312\python.exe -I -B test_authorize_wizard_browser.py -v
No server lifecycle action, real credential read, or real HTTP request occurs.
"""

import base64
from contextlib import ExitStack
import copy
import http.client
import importlib.util
import io
import json
from pathlib import Path
import runpy
import subprocess
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch


SOURCE = Path(__file__).with_name("authorize_wizard_browser.py")
spec = importlib.util.spec_from_file_location("authorize_wizard_browser", SOURCE)
helper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helper)

ROOT_PID, MIDDLE_PID, SERVER_PID, CLIENT_PORT = 101, 202, 303, 54321
FAKE_KEY = "fixture-only-token-never-a-real-secret-12345"


def snapshot():
    processes = [
        dict(pid=ROOT_PID, parent_pid=9, name="powershell.exe",
             executable=helper.POWERSHELL,
             command_line='"' + helper.LAUNCH_ARGUMENTS[0] + '" ' + ' '.join(helper.LAUNCH_ARGUMENTS[1:]),
             created="2026-11-04T10:00:00.0000000Z"),
        dict(pid=MIDDLE_PID, parent_pid=ROOT_PID, name="python.exe",
             executable=r"C:\nqt37\v\Scripts\python.exe", command_line="fixture middle",
             created="2026-11-04T10:00:01.0000000Z"),
        dict(pid=SERVER_PID, parent_pid=MIDDLE_PID, name="python.exe",
             executable=r"C:\Python312\python.exe", command_line="fixture server",
             created="2026-11-04T10:00:02.0000000Z"),
    ]
    return dict(schema=1, before=processes, after=copy.deepcopy(processes), tcp=[
        dict(local_address="127.0.0.1", local_port=18038, remote_address="0.0.0.0",
             remote_port=0, state=2, pid=SERVER_PID),
        dict(local_address="127.0.0.1", local_port=18038, remote_address="127.0.0.1",
             remote_port=CLIENT_PORT, state=5, pid=SERVER_PID),
    ])


class FakeSocket:
    def __init__(self, status=200, body=b'{"authorized":true}'):
        self.sent = []
        self.closed = False
        self.local = (helper.HOST, CLIENT_PORT)
        self.peer = (helper.HOST, helper.PORT)
        self.response = (f"HTTP/1.1 {status} Fixture\r\nContent-Length: {len(body)}\r\n"
                         "Location: http://127.0.0.1:19999/steal\r\n\r\n").encode() + body

    def getsockname(self):
        if self.closed:
            raise OSError("fixture closed")
        return self.local

    def getpeername(self):
        if self.closed:
            raise OSError("fixture closed")
        return self.peer

    def setsockopt(self, *args):
        pass

    def sendall(self, data):
        if self.closed:
            raise OSError("fixture closed")
        self.sent.append(bytes(data))

    def makefile(self, mode):
        return io.BytesIO(self.response)

    def close(self):
        self.closed = True


class AuthorizationBoundaryTests(unittest.TestCase):
    def setUp(self):
        # Even an accidental call to the real loader cannot read a disk secret.
        forbidden_secret = Mock()
        forbidden_secret.read_text.side_effect = AssertionError("Real key reads forbidden in tests")
        self.secret_guard = patch.object(helper, "SECRET_PATH", forbidden_secret)
        self.secret_guard.start()
        self.addCleanup(self.secret_guard.stop)

    def environment(self, stack, data=None, sock=None, receipt=ROOT_PID):
        sock = sock or FakeSocket()
        dial = stack.enter_context(patch.object(http.client.socket, "create_connection", return_value=sock))
        stack.enter_context(patch.object(helper, "verify_static_owner"))
        stack.enter_context(patch.object(helper, "read_launcher_pid", return_value=receipt))
        collect = stack.enter_context(patch.object(helper, "collect_ownership", return_value=data))
        key = stack.enter_context(patch.object(helper, "load_admin_key", return_value=FAKE_KEY))
        return sock, dial, collect, key

    def test_import_is_inert(self):
        with patch("builtins.input") as prompt, patch.object(Path, "read_text") as read_text, \
                patch.object(Path, "read_bytes") as read_bytes, patch.object(subprocess, "run") as process, \
                patch.object(http.client.socket, "create_connection") as connect:
            runpy.run_path(str(SOURCE), run_name="fixture_import")
        for mock in (prompt, read_text, read_bytes, process, connect):
            mock.assert_not_called()

    def test_owned_recipient_loads_key_after_proof_and_posts_on_identical_socket(self):
        with ExitStack() as stack:
            sock, dial, collect, key = self.environment(stack, snapshot())

            def collect_after_connect():
                dial.assert_called_once_with((helper.HOST, helper.PORT), 15, None)
                self.assertEqual(sock.sent, [])
                key.assert_not_called()
                return snapshot()

            def key_after_proof():
                collect.assert_called_once()
                self.assertEqual(sock.sent, [])
                self.assertFalse(sock.closed)
                return FAKE_KEY

            collect.side_effect = collect_after_connect
            key.side_effect = key_after_proof
            helper.authorize("abcdef1234")
            dial.assert_called_once()
            key.assert_called_once()
            wire = b"".join(sock.sent)
            self.assertEqual(wire.count(b"POST /api/v1/operator/session/authorize HTTP/1.1"), 1)
            self.assertIn(b"X-Nginx-QA-Scope-Control-Token: " + FAKE_KEY.encode(), wire)
            self.assertIn(b'"pairing_code": "ABCDEF1234"', wire)
            self.assertIn(b'"operator_id": "madoev"', wire)
            self.assertTrue(sock.closed)

    def test_absent_forged_unowned_and_malformed_evidence_never_loads_key(self):
        cases = {}
        for name in ("empty_tcp", "no_listener", "no_tuple", "wrong_tuple_port", "wrong_tuple_address",
                     "wrong_local_address", "listener_pid_mismatch", "unowned_recipient", "forged_root_image",
                     "forged_command_suffix", "forged_command_prefix", "wrong_commit", "wrong_profile",
                     "missing_root", "null_root_name", "parent_pid_reused", "root_pid_reused_during_probe",
                     "child_pid_reused_during_probe", "missing_parent", "cyclic_parent", "duplicate_process",
                     "duplicate_tuple", "malformed_tcp", "missing_timestamp", "naive_timestamp"):
            data = snapshot()
            if name == "empty_tcp": data["tcp"] = []
            elif name == "no_listener": data["tcp"] = data["tcp"][1:]
            elif name == "no_tuple": data["tcp"] = data["tcp"][:1]
            elif name == "wrong_tuple_port": data["tcp"][1]["remote_port"] += 1
            elif name == "wrong_tuple_address": data["tcp"][1]["remote_address"] = "127.0.0.2"
            elif name == "wrong_local_address": data["tcp"][0]["local_address"] = "0.0.0.0"
            elif name == "listener_pid_mismatch": data["tcp"][0]["pid"] = MIDDLE_PID
            elif name == "unowned_recipient": data["before"][2]["parent_pid"] = 999
            elif name == "forged_root_image": data["before"][0]["executable"] = r"C:\fake\powershell.exe"
            elif name == "forged_command_suffix": data["before"][0]["command_line"] += " -Other attacker"
            elif name == "forged_command_prefix": data["before"][0]["command_line"] = "echo " + data["before"][0]["command_line"]
            elif name == "wrong_commit": data["before"][0]["command_line"] = data["before"][0]["command_line"].replace(helper.COMMIT, "0" * 40)
            elif name == "wrong_profile": data["before"][0]["command_line"] = data["before"][0]["command_line"].replace("setup-diagnostics-wizard-v1", "other")
            elif name == "missing_root": data["before"] = data["before"][1:]
            elif name == "null_root_name": data["before"][0]["name"] = None
            elif name == "parent_pid_reused": data["before"][1]["created"] = "2026-11-04T10:01:00Z"
            elif name == "root_pid_reused_during_probe": data["after"][0]["created"] = "2026-11-04T10:02:00Z"
            elif name == "child_pid_reused_during_probe": data["after"][2]["created"] = "2026-11-04T10:02:00Z"
            elif name == "missing_parent": data["before"].pop(1)
            elif name == "cyclic_parent": data["before"][1]["parent_pid"] = SERVER_PID
            elif name == "duplicate_process": data["before"].append(copy.deepcopy(data["before"][1]))
            elif name == "duplicate_tuple": data["tcp"].append(copy.deepcopy(data["tcp"][1]))
            elif name == "malformed_tcp": data["tcp"][1]["pid"] = "303"
            elif name == "missing_timestamp": del data["before"][1]["created"]
            elif name == "naive_timestamp": data["before"][1]["created"] = "2026-11-04T10:00:01"
            if "during_probe" not in name:
                data["after"] = copy.deepcopy(data["before"])
            cases[name] = data
        cases.update(null_response=None, list_response=[], missing_fields={}, wrong_schema={"schema": True})
        for name, data in cases.items():
            with self.subTest(name=name), ExitStack() as stack:
                sock, dial, collect, key = self.environment(stack, data)
                with self.assertRaises(helper.AuthorizationError):
                    helper.authorize("ABCDEF1234")
                key.assert_not_called()
                self.assertEqual(sock.sent, [])
                self.assertTrue(sock.closed)
                dial.assert_called_once()

    def test_receipt_root_mismatch_or_change_prevents_key_read(self):
        for change in (False, True):
            with self.subTest(change=change), ExitStack() as stack:
                sock, _, _, key = self.environment(stack, snapshot(), receipt=999 if not change else ROOT_PID)
                if change:
                    stack.enter_context(patch.object(helper, "read_launcher_pid", side_effect=[ROOT_PID, 999]))
                with self.assertRaises(helper.AuthorizationError):
                    helper.authorize("ABCDEF1234")
                key.assert_not_called()
                self.assertEqual(sock.sent, [])

    def test_tcp_connect_failure_prevents_probe_and_key_load(self):
        with ExitStack() as stack:
            _, dial, collect, key = self.environment(stack, snapshot())
            dial.side_effect = ConnectionRefusedError("fixture absent listener")
            with self.assertRaises(ConnectionRefusedError):
                helper.authorize("ABCDEF1234")
            dial.assert_called_once()
            collect.assert_not_called()
            key.assert_not_called()

    def test_closed_replaced_or_retargeted_socket_cannot_reconnect(self):
        for change in ("none", "replacement", "closed", "local_changed", "peer_changed"):
            with self.subTest(change=change):
                original, replacement = FakeSocket(), FakeSocket()
                with patch.object(http.client.socket, "create_connection", return_value=original) as dial:
                    connection = helper.BoundHTTPConnection()
                    connection.connect()
                    connection.mark_proven()
                    if change == "none": connection.sock = None
                    elif change == "replacement": connection.sock = replacement
                    elif change == "closed": original.close()
                    elif change == "local_changed": original.local = (helper.HOST, CLIENT_PORT + 1)
                    else: original.peer = (helper.HOST, helper.PORT + 1)
                    with self.assertRaises(helper.AuthorizationError):
                        connection.request("POST", "/fixture", headers={"Fixture-Key": FAKE_KEY})
                    with self.assertRaises(helper.AuthorizationError):
                        connection.connect()
                    self.assertEqual(original.sent + replacement.sent, [])
                    dial.assert_called_once()
                    connection.close()

    def test_no_http_bytes_before_proof(self):
        sock = FakeSocket()
        with patch.object(http.client.socket, "create_connection", return_value=sock):
            connection = helper.BoundHTTPConnection()
            connection.connect()
            with self.assertRaises(helper.AuthorizationError):
                connection.request("GET", "/fixture")
            self.assertEqual(sock.sent, [])
            connection.close()

    def test_disconnect_during_key_load_does_not_retry(self):
        with ExitStack() as stack:
            sock, dial, _, key = self.environment(stack, snapshot())
            key.side_effect = lambda: (sock.close(), FAKE_KEY)[1]
            with self.assertRaises(helper.AuthorizationError):
                helper.authorize("ABCDEF1234")
            self.assertEqual(sock.sent, [])
            dial.assert_called_once()

    def test_redirect_and_bad_response_are_not_followed(self):
        for status, body in ((302, b'{"authorized":true}'), (200, b'[]'), (200, b'not-json')):
            with self.subTest(status=status, body=body), ExitStack() as stack:
                sock, dial, _, _ = self.environment(stack, snapshot(), FakeSocket(status, body))
                with self.assertRaises(helper.AuthorizationError):
                    helper.authorize("ABCDEF1234")
                dial.assert_called_once()
                self.assertEqual(b"".join(sock.sent).count(b"POST "), 1)

    def test_collector_uses_fixed_trusted_program_and_rejects_bad_output(self):
        good = SimpleNamespace(returncode=0, stdout=json.dumps(snapshot()).encode(), stderr=b"")
        with patch.object(subprocess, "run", return_value=good) as run:
            self.assertEqual(helper.collect_ownership(), snapshot())
            args, kwargs = run.call_args
            self.assertEqual(args[0][0], helper.POWERSHELL)
            self.assertEqual(base64.b64decode(args[0][-1]).decode("utf-16le"), helper.CIM_SCRIPT)
            self.assertEqual(kwargs["timeout"], 20)
            self.assertFalse(kwargs.get("shell", False))
        for output in (b"", b"bad-json", b'{"schema":1,"schema":1}', b"\xff", b"x" * 4_000_001):
            with self.subTest(size=len(output)), patch.object(subprocess, "run", return_value=SimpleNamespace(returncode=0, stdout=output)):
                with self.assertRaises(helper.AuthorizationError): helper.collect_ownership()
        for error in (OSError("fixture"), subprocess.TimeoutExpired("fixture", 20)):
            with patch.object(subprocess, "run", side_effect=error):
                with self.assertRaises(helper.AuthorizationError): helper.collect_ownership()
        with patch.object(subprocess, "run", return_value=SimpleNamespace(returncode=1, stdout=b"fixture")):
            with self.assertRaises(helper.AuthorizationError): helper.collect_ownership()

    def test_static_owner_and_receipt_checks_remain(self):
        owner = dict(ownership=dict(instance_id="setup-diagnostics-wizard-v1-18038", http_host="127.0.0.1",
                                    http_port=18038, service_root=str(helper.SERVICE_ROOT),
                                    approved_commit=helper.COMMIT,
                                    branch="codex/sdw-qualified-host-continuation-20261104"))
        with patch.object(helper, "OWNER_PATH", SimpleNamespace(read_bytes=lambda: json.dumps(owner).encode())):
            helper.verify_static_owner()
            for field, value in (("instance_id", "other"), ("http_host", "127.0.0.2"),
                                 ("http_port", 18039), ("service_root", "C:/other"),
                                 ("approved_commit", "0" * 40), ("branch", "main")):
                original = owner["ownership"][field]
                owner["ownership"][field] = value
                with self.assertRaises(helper.AuthorizationError): helper.verify_static_owner()
                owner["ownership"][field] = original
        for value in ("", "0", "-1", "101\n202", "true", "4294967296", "1; echo bad"):
            with self.subTest(value=value), patch.object(helper, "PID_PATH", SimpleNamespace(read_text=lambda **kw: value)):
                with self.assertRaises(helper.AuthorizationError): helper.read_launcher_pid()
        with patch.object(helper, "PID_PATH", SimpleNamespace(read_text=lambda **kw: "101\n")):
            self.assertEqual(helper.read_launcher_pid(), ROOT_PID)

    def test_cli_does_not_echo_sensitive_exception_contents(self):
        for error in (OSError(FAKE_KEY), ValueError(FAKE_KEY), http.client.BadStatusLine(FAKE_KEY), RuntimeError(FAKE_KEY)):
            with patch.object(helper, "authorize", side_effect=error), patch("sys.stderr", new_callable=io.StringIO) as stderr:
                with self.assertRaises(SystemExit) as raised:
                    helper.main(["--pairing-code", "ABCDEF1234"])
                self.assertEqual(raised.exception.code, 1)
                self.assertNotIn(FAKE_KEY, stderr.getvalue())
                self.assertNotIn("Traceback", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
