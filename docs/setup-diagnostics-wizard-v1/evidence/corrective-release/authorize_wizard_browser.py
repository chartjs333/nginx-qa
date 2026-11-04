"""Approve a pairing code only on a proven, already-connected local socket.

The marker is configuration, not live recipient proof. No HTTP bytes or admin
key are loaded until CIM proves this exact TCP connection's launcher ancestry.
The operator helper directory and PID receipt must be writable only by the
operator and SYSTEM; this proof assumes those local trust inputs are protected.
"""

import argparse
import base64
from datetime import datetime
import http.client
import json
from pathlib import Path
import re
import subprocess


HOST = "127.0.0.1"
PORT = 18038
SERVICE_ROOT = Path("D:/nq-sdw-launcher")
OWNER_PATH = Path("C:/nqt37/.nginx-qa-staging-owner.json")
PID_PATH = Path("C:/nqt35/client/sdw-corrective-20261104/host-launch-pid.txt")
SECRET_PATH = SERVICE_ROOT / ".env.staging"
POWERSHELL = r"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe"
COMMIT = "65662f69db52c27b6c0972ccec8031cfb68a02fc"
LAUNCH_ARGUMENTS = (
    POWERSHELL, "-NoProfile", "-ExecutionPolicy", "Bypass", "-File",
    r"D:\nq-sdw-launcher\run_staging.ps1", "-Action", "Start", "-Profile",
    "setup-diagnostics-wizard-v1", "-ExpectedCommit", COMMIT,
)
# These arguments contain no whitespace/quotes. Accept whole-argument Windows
# quoting, never extra arguments/commands or a matching command-line substring.
LAUNCH_COMMAND = re.compile(
    r"[ \t]*" + r"[ \t]+".join(
        r'(?:' + re.escape(arg) + r'|"' + re.escape(arg) + r'")'
        for arg in LAUNCH_ARGUMENTS
    ) + r"[ \t]*", re.IGNORECASE,
)

# Fixed trusted executable and absolute CIM module; no receipt, pairing code,
# port, or other variable is interpolated into the PowerShell program.
# Process snapshots bracket the TCP query to catch process exit/PID reuse.
CIM_SCRIPT = r"""
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
[Console]::OutputEncoding = New-Object System.Text.UTF8Encoding($false)
Import-Module 'C:\Windows\System32\WindowsPowerShell\v1.0\Modules\CimCmdlets\CimCmdlets.psd1' -ErrorAction Stop
function Read-Processes {
    @(CimCmdlets\Get-CimInstance -Namespace root/cimv2 -ClassName Win32_Process -ErrorAction Stop | ForEach-Object {
        [ordered]@{
            pid = [long]$_.ProcessId
            parent_pid = [long]$_.ParentProcessId
            name = $_.Name
            executable = $_.ExecutablePath
            command_line = $_.CommandLine
            created = if ($null -ne $_.CreationDate) { $_.CreationDate.ToUniversalTime().ToString('o') } else { $null }
        }
    })
}
$before = @(Read-Processes)
$tcp = @(CimCmdlets\Get-CimInstance -Namespace root/StandardCimv2 -ClassName MSFT_NetTCPConnection -Filter 'LocalPort = 18038' -ErrorAction Stop | ForEach-Object {
    [ordered]@{
        local_address = $_.LocalAddress
        local_port = [int]$_.LocalPort
        remote_address = $_.RemoteAddress
        remote_port = [int]$_.RemotePort
        state = [int]$_.State
        pid = [long]$_.OwningProcess
    }
})
$after = @(Read-Processes)
[ordered]@{schema = 1; before = $before; tcp = $tcp; after = $after} | ConvertTo-Json -Depth 5 -Compress
"""


class AuthorizationError(Exception):
    """Sanitized failure: no subprocess output, request headers, or secrets."""


def fail_owner():
    raise AuthorizationError("Живой владелец соединения не подтверждён. Вход не выполнен.")


def strict_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            fail_owner()
        result[key] = value
    return result


def read_json(value):
    try:
        return json.loads(value, object_pairs_hook=strict_object)
    except (ValueError, TypeError, RecursionError):
        fail_owner()


def verify_static_owner():
    try:
        owner = read_json(OWNER_PATH.read_bytes())["ownership"]
        if (owner["instance_id"] != "setup-diagnostics-wizard-v1-18038"
                or owner["http_host"] != HOST
                or type(owner["http_port"]) is not int or owner["http_port"] != PORT
                or Path(owner["service_root"]).resolve() != SERVICE_ROOT.resolve()):
            fail_owner()
    except (OSError, KeyError, TypeError, ValueError):
        fail_owner()


def read_launcher_pid():
    try:
        value = PID_PATH.read_text(encoding="ascii").strip()
        if not re.fullmatch(r"[1-9][0-9]{0,9}", value):
            fail_owner()
        pid = int(value)
        if pid > 0xFFFFFFFF:
            fail_owner()
        return pid
    except (OSError, UnicodeError, ValueError):
        fail_owner()


def collect_ownership():
    try:
        result = subprocess.run(
            [POWERSHELL, "-NoLogo", "-NoProfile", "-NonInteractive", "-EncodedCommand",
             base64.b64encode(CIM_SCRIPT.encode("utf-16le")).decode("ascii")],
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            timeout=20, check=False, creationflags=subprocess.CREATE_NO_WINDOW,
        )
        if result.returncode != 0 or not result.stdout or len(result.stdout) > 4_000_000:
            fail_owner()
        return read_json(result.stdout.decode("utf-8-sig"))
    except (OSError, subprocess.SubprocessError, UnicodeError):
        fail_owner()


def is_pid(value):
    return type(value) is int and 0 < value <= 0xFFFFFFFF


def process_index(rows):
    if not isinstance(rows, list) or not rows:
        fail_owner()
    result = {}
    for row in rows:
        if not isinstance(row, dict):
            fail_owner()
        pid = row.get("pid")
        if type(pid) is not int or not 0 <= pid <= 0xFFFFFFFF or pid in result:
            fail_owner()
        result[pid] = row
    return result


def creation_time(process):
    try:
        value = process["created"]
        if not isinstance(value, str):
            fail_owner()
        result = datetime.fromisoformat(value)
        if result.tzinfo is None or result.utcoffset() is None:
            fail_owner()
        return result
    except (KeyError, ValueError):
        fail_owner()


def verify_snapshot(snapshot, root_pid, client_port):
    """Prove listener and exact server-side ESTABLISHED tuple have one owner."""
    if (not isinstance(snapshot, dict) or type(snapshot.get("schema")) is not int
            or snapshot["schema"] != 1 or not is_pid(root_pid)
            or type(client_port) is not int or not 0 < client_port <= 65535):
        fail_owner()
    before = process_index(snapshot.get("before"))
    after = process_index(snapshot.get("after"))
    tcp = snapshot.get("tcp")
    if not isinstance(tcp, list) or not tcp:
        fail_owner()
    listeners, recipients = [], []
    for row in tcp:
        if (not isinstance(row, dict) or type(row.get("pid")) is not int
                or not 0 <= row["pid"] <= 0xFFFFFFFF
                or not isinstance(row.get("local_address"), str)
                or not isinstance(row.get("remote_address"), str)
                or type(row.get("local_port")) is not int or row["local_port"] != PORT
                or type(row.get("remote_port")) is not int or not 0 <= row["remote_port"] <= 65535
                or type(row.get("state")) is not int or not 1 <= row["state"] <= 12):
            fail_owner()
        if row["state"] == 2:  # MSFT_NetTCPConnection: Listen
            listeners.append(row)
        if (row["state"] == 5 and row["local_address"] == HOST  # Established
                and row["remote_address"] == HOST and row["remote_port"] == client_port):
            recipients.append(row)
    if (len(listeners) != 1 or len(recipients) != 1
            or listeners[0]["local_address"] != HOST
            or listeners[0]["remote_address"] != "0.0.0.0" or listeners[0]["remote_port"] != 0
            or not is_pid(listeners[0]["pid"])
            or listeners[0]["pid"] != recipients[0]["pid"]):
        fail_owner()
    root = before.get(root_pid)
    if (not root or root != after.get(root_pid)
            or not isinstance(root.get("name"), str) or root["name"].lower() != "powershell.exe"
            or not isinstance(root.get("executable"), str)
            or root["executable"].lower() != POWERSHELL.lower()
            or not isinstance(root.get("command_line"), str)
            or LAUNCH_COMMAND.fullmatch(root["command_line"]) is None):
        fail_owner()
    # ParentProcessId alone is not ownership: a parent PID may have been reused.
    # Every link must exist unchanged across the TCP query and predate its child.
    pid = recipients[0]["pid"]
    seen = set()
    child_time = None
    while True:
        process = before.get(pid)
        if pid in seen or not process or process != after.get(pid):
            fail_owner()
        seen.add(pid)
        born = creation_time(process)
        if child_time is not None and born > child_time:
            fail_owner()
        if pid == root_pid:
            break
        child_time = born
        pid = process.get("parent_pid")
        if not is_pid(pid):
            fail_owner()
    return recipients[0]["pid"]


class BoundHTTPConnection(http.client.HTTPConnection):
    """One connect attempt; writes require the proven original socket."""

    auto_open = 0

    def __init__(self):
        super().__init__(HOST, PORT, timeout=15)
        self._attempted = False
        self._original_socket = None
        self._proven = False
        self._client_port = None

    def connect(self):
        if self._attempted:
            fail_owner()
        self._attempted = True
        super().connect()  # TCP handshake only; no HTTP request or credential.
        self._original_socket = self.sock
        self._client_port = self.bound_client_port()

    def bound_client_port(self):
        if self.sock is None or self.sock is not self._original_socket:
            fail_owner()
        try:
            local, peer = self.sock.getsockname(), self.sock.getpeername()
        except OSError:
            fail_owner()
        if (len(local) != 2 or local[0] != HOST or type(local[1]) is not int
                or not 0 < local[1] <= 65535 or peer != (HOST, PORT)
                or self._client_port is not None and local[1] != self._client_port):
            fail_owner()
        return local[1]

    def mark_proven(self):
        self.bound_client_port()
        self._proven = True

    def send(self, data):
        if not self._proven:
            fail_owner()
        self.bound_client_port()
        # auto_open is disabled; connect() independently refuses a second try.
        super().send(data)


def load_admin_key():
    try:
        values = [line.split("=", 1)[1].strip().strip('"') for line in
                  SECRET_PATH.read_text().splitlines()
                  if line.startswith("NGINX_QA_SCOPE_CONTROL_ADMIN_TOKEN=")]
    except (OSError, UnicodeError):
        raise AuthorizationError("Административный ключ экземпляра недоступен.") from None
    if len(values) != 1 or len(values[0]) < 32 or "\r" in values[0] or "\n" in values[0]:
        raise AuthorizationError("Административный ключ экземпляра недоступен.")
    return values[0]


def authorize(code):
    code = code.strip().upper()
    if not re.fullmatch(r"[0-9A-F]{10}", code):
        raise AuthorizationError("Нужен десятизначный код из страницы мастера.")
    verify_static_owner()
    root_pid = read_launcher_pid()
    connection = BoundHTTPConnection()
    try:
        connection.connect()
        verify_snapshot(collect_ownership(), root_pid, connection.bound_client_port())
        if read_launcher_pid() != root_pid:
            fail_owner()
        connection.mark_proven()
        key = load_admin_key()
        connection.request("POST", "/api/v1/operator/session/authorize",
            body=json.dumps({"pairing_code": code, "operator_id": "madoev"}),
            headers={"Content-Type": "application/json", "Origin": "http://127.0.0.1:18038",
                     "X-Nginx-QA-Scope-Control-Token": key})
        response = connection.getresponse()
        data = read_json(response.read(4096))
        if response.status != 200 or not isinstance(data, dict) or data.get("authorized") is not True:
            raise AuthorizationError("Код истёк или не принят. Обновите страницу и используйте новый код.")
    finally:
        connection.close()


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--pairing-code")
    args = parser.parse_args(argv)
    try:
        code = args.pairing_code or input("Код из браузера http://127.0.0.1:18038/setup: ")
        authorize(code)
    except AuthorizationError as error:
        parser.exit(1, str(error) + "\n")
    except Exception:
        # Never print a request, key, process command line, or arbitrary response.
        parser.exit(1, "Подтверждение не выполнено. Проверьте свой экземпляр мастера и повторите вручную.\n")
    print("Вход madoev подтверждён. Вернитесь на страницу мастера и нажмите «Проверить подтверждение сессии».")


if __name__ == "__main__":
    main()
