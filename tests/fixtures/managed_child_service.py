"""Small stdlib-only HTTP service used by managed-process integration tests.

The service requires the five environment variables injected by the managed
process supervisor::

    NGINX_QA_MANAGED_HOST
    NGINX_QA_MANAGED_PORT
    NGINX_QA_MANAGED_PROCESS_ID
    NGINX_QA_MANAGED_RUNTIME_ROOT
    NGINX_QA_MANAGED_LAUNCH_NONCE

``GET /health`` returns the process identity, cwd, runtime root, endpoint, PID,
and launch nonce.  The process also writes ``managed-child-marker.json`` below
its runtime root and emits one JSON line to both stdout and stderr.

Failure modes can be selected with ``--mode`` (or ``MANAGED_CHILD_MODE``):
``serve``, ``delayed-start``, ``crash-after-health``, ``immediate-crash``,
``spawn-descendant``, and ``controlled-exit``.  The modes can be refined or
combined with these options/environment variables:

* ``--delay-start`` / ``MANAGED_CHILD_START_DELAY_SECONDS``;
* ``--crash-immediately`` / ``MANAGED_CHILD_CRASH_IMMEDIATELY``;
* ``--crash-after-health`` / ``MANAGED_CHILD_CRASH_AFTER_HEALTH_SECONDS``;
* ``--spawn-descendant`` / ``MANAGED_CHILD_SPAWN_DESCENDANT``;
* ``--exit-after`` / ``MANAGED_CHILD_EXIT_AFTER_SECONDS``.

``GET`` or ``POST /control/exit`` requests a clean shutdown.  The fixture is
deliberately standalone and must never be imported by production code.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Callable, TextIO
from urllib.parse import urlsplit


_DEFAULT_DELAY_SECONDS = 0.5
_DEFAULT_CRASH_DELAY_SECONDS = 0.1
_DEFAULT_CONTROLLED_EXIT_SECONDS = 0.5
_DEFAULT_CRASH_EXIT_CODE = 23
_MARKER_NAME = "managed-child-marker.json"
_MODES = {
    "serve",
    "delayed-start",
    "crash-after-health",
    "immediate-crash",
    "spawn-descendant",
    "controlled-exit",
}
_TRUE_VALUES = {"1", "true", "yes", "on"}
_FALSE_VALUES = {"0", "false", "no", "off"}


class FixtureConfigurationError(ValueError):
    """Raised when the test fixture receives an invalid configuration."""


class ManagedChildHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = False
    daemon_threads = True
    block_on_close = False


class Settings:
    def __init__(
        self,
        *,
        host: str,
        port: int,
        process_id: str,
        runtime_root: Path,
        launch_nonce: str,
        health_path: str,
        start_delay: float,
        crash_immediately: bool,
        crash_after_health: float | None,
        spawn_descendant: bool,
        exit_after: float | None,
        exit_code: int,
        crash_exit_code: int,
        descendant_worker: bool,
    ) -> None:
        self.host = host
        self.port = port
        self.process_id = process_id
        self.runtime_root = runtime_root
        self.launch_nonce = launch_nonce
        self.health_path = health_path
        self.start_delay = start_delay
        self.crash_immediately = crash_immediately
        self.crash_after_health = crash_after_health
        self.spawn_descendant = spawn_descendant
        self.exit_after = exit_after
        self.exit_code = exit_code
        self.crash_exit_code = crash_exit_code
        self.descendant_worker = descendant_worker


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _emit(stream: TextIO, event: str, settings: Settings, **extra: object) -> None:
    payload: dict[str, object] = {
        "fixture": "managed_child_service",
        "event": event,
        "process_id": settings.process_id,
        "pid": os.getpid(),
        "port": settings.port,
    }
    payload.update(extra)
    try:
        print(json.dumps(payload, sort_keys=True), file=stream, flush=True)
    except (BrokenPipeError, OSError):
        # A supervisor may close a log pipe while it terminates the process
        # group.  Logging must not obscure the intended process outcome.
        pass


def _required_environment(name: str) -> str:
    value = os.environ.get(name)
    if value is None or not value.strip():
        raise FixtureConfigurationError(f"missing required environment variable: {name}")
    return value


def _nonnegative_float(value: str, *, name: str) -> float:
    try:
        result = float(value)
    except ValueError as exc:
        raise FixtureConfigurationError(f"{name} must be a number") from exc
    if result < 0 or result == float("inf") or result != result:
        raise FixtureConfigurationError(f"{name} must be a finite non-negative number")
    return result


def _optional_environment_float(name: str) -> float | None:
    value = os.environ.get(name)
    if value is None or not value.strip():
        return None
    return _nonnegative_float(value, name=name)


def _environment_bool(name: str) -> bool:
    value = os.environ.get(name)
    if value is None or not value.strip():
        return False
    normalized = value.strip().casefold()
    if normalized in _TRUE_VALUES:
        return True
    if normalized in _FALSE_VALUES:
        return False
    raise FixtureConfigurationError(
        f"{name} must be one of: 1/0, true/false, yes/no, on/off"
    )


def _exit_code(value: str, *, name: str, allow_zero: bool) -> int:
    try:
        result = int(value, 10)
    except ValueError as exc:
        raise FixtureConfigurationError(f"{name} must be an integer") from exc
    lower = 0 if allow_zero else 1
    if not lower <= result <= 255:
        raise FixtureConfigurationError(f"{name} must be between {lower} and 255")
    return result


def _parse_settings(arguments: list[str] | None = None) -> Settings:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=sorted(_MODES))
    parser.add_argument(
        "--delay-start",
        "--start-delay",
        dest="start_delay",
        metavar="SECONDS",
    )
    parser.add_argument("--crash-immediately", action="store_true")
    parser.add_argument(
        "--crash-after-health",
        nargs="?",
        const=str(_DEFAULT_CRASH_DELAY_SECONDS),
        metavar="SECONDS",
    )
    parser.add_argument("--spawn-descendant", action="store_true")
    parser.add_argument("--exit-after", metavar="SECONDS")
    parser.add_argument("--exit-code")
    parser.add_argument("--crash-exit-code")
    parser.add_argument("--health-path")
    parser.add_argument("--descendant-worker", action="store_true", help=argparse.SUPPRESS)
    parsed = parser.parse_args(arguments)

    try:
        mode = parsed.mode or os.environ.get("MANAGED_CHILD_MODE", "serve").strip()
        if mode not in _MODES:
            raise FixtureConfigurationError(
                "MANAGED_CHILD_MODE must be one of: " + ", ".join(sorted(_MODES))
            )

        raw_port = _required_environment("NGINX_QA_MANAGED_PORT")
        try:
            port = int(raw_port, 10)
        except ValueError as exc:
            raise FixtureConfigurationError(
                "NGINX_QA_MANAGED_PORT must be an integer"
            ) from exc
        if not 1 <= port <= 65535:
            raise FixtureConfigurationError(
                "NGINX_QA_MANAGED_PORT must be between 1 and 65535"
            )

        host = _required_environment("NGINX_QA_MANAGED_HOST")
        if host != "127.0.0.1":
            raise FixtureConfigurationError(
                "NGINX_QA_MANAGED_HOST must be exactly 127.0.0.1"
            )

        process_id = _required_environment("NGINX_QA_MANAGED_PROCESS_ID")
        launch_nonce = _required_environment("NGINX_QA_MANAGED_LAUNCH_NONCE")
        runtime_root = Path(
            _required_environment("NGINX_QA_MANAGED_RUNTIME_ROOT")
        )
        if not runtime_root.is_absolute():
            raise FixtureConfigurationError(
                "NGINX_QA_MANAGED_RUNTIME_ROOT must be absolute"
            )
        runtime_root = runtime_root.resolve(strict=False)

        health_path = (
            parsed.health_path
            or os.environ.get("MANAGED_CHILD_HEALTH_PATH")
            or "/health"
        )
        if (
            not health_path.startswith("/")
            or health_path.startswith("//")
            or "?" in health_path
            or "#" in health_path
        ):
            raise FixtureConfigurationError("health path must be an absolute URL path")

        environment_delay = _optional_environment_float(
            "MANAGED_CHILD_START_DELAY_SECONDS"
        )
        start_delay = (
            _nonnegative_float(parsed.start_delay, name="--delay-start")
            if parsed.start_delay is not None
            else environment_delay
            if environment_delay is not None
            else _DEFAULT_DELAY_SECONDS
            if mode == "delayed-start"
            else 0.0
        )

        environment_crash_delay = _optional_environment_float(
            "MANAGED_CHILD_CRASH_AFTER_HEALTH_SECONDS"
        )
        crash_after_health = (
            _nonnegative_float(
                parsed.crash_after_health,
                name="--crash-after-health",
            )
            if parsed.crash_after_health is not None
            else environment_crash_delay
            if environment_crash_delay is not None
            else _DEFAULT_CRASH_DELAY_SECONDS
            if mode == "crash-after-health"
            else None
        )

        environment_exit_delay = _optional_environment_float(
            "MANAGED_CHILD_EXIT_AFTER_SECONDS"
        )
        exit_after = (
            _nonnegative_float(parsed.exit_after, name="--exit-after")
            if parsed.exit_after is not None
            else environment_exit_delay
            if environment_exit_delay is not None
            else _DEFAULT_CONTROLLED_EXIT_SECONDS
            if mode == "controlled-exit"
            else None
        )

        raw_exit_code = (
            parsed.exit_code
            or os.environ.get("MANAGED_CHILD_EXIT_CODE")
            or "0"
        )
        raw_crash_exit_code = (
            parsed.crash_exit_code
            or os.environ.get("MANAGED_CHILD_CRASH_EXIT_CODE")
            or str(_DEFAULT_CRASH_EXIT_CODE)
        )
        return Settings(
            host=host,
            port=port,
            process_id=process_id,
            runtime_root=runtime_root,
            launch_nonce=launch_nonce,
            health_path=health_path,
            start_delay=start_delay,
            crash_immediately=(
                parsed.crash_immediately
                or _environment_bool("MANAGED_CHILD_CRASH_IMMEDIATELY")
                or mode == "immediate-crash"
            ),
            crash_after_health=crash_after_health,
            spawn_descendant=(
                parsed.spawn_descendant
                or _environment_bool("MANAGED_CHILD_SPAWN_DESCENDANT")
                or mode == "spawn-descendant"
            ),
            exit_after=exit_after,
            exit_code=_exit_code(
                raw_exit_code,
                name="MANAGED_CHILD_EXIT_CODE/--exit-code",
                allow_zero=True,
            ),
            crash_exit_code=_exit_code(
                raw_crash_exit_code,
                name="MANAGED_CHILD_CRASH_EXIT_CODE/--crash-exit-code",
                allow_zero=False,
            ),
            descendant_worker=parsed.descendant_worker,
        )
    except FixtureConfigurationError as exc:
        parser.error(str(exc))
        raise AssertionError("argparse.error must terminate") from exc


def _atomic_json_write(path: Path, payload: dict[str, object]) -> None:
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
    )
    encoded = json.dumps(payload, sort_keys=True, indent=2) + "\n"
    temporary.write_text(encoded, encoding="utf-8")
    os.replace(temporary, path)


def _marker_payload(
    settings: Settings,
    state: str,
    *,
    descendant_pid: int | None = None,
    reason: str | None = None,
) -> dict[str, object]:
    return {
        "state": state,
        "identity": settings.process_id,
        "process_id": settings.process_id,
        "cwd": str(Path.cwd().resolve(strict=False)),
        "runtime": str(settings.runtime_root),
        "runtime_root": str(settings.runtime_root),
        "host": settings.host,
        "port": settings.port,
        "pid": os.getpid(),
        "ppid": os.getppid(),
        "nonce": settings.launch_nonce,
        "launch_nonce": settings.launch_nonce,
        "descendant_pid": descendant_pid,
        "reason": reason,
        "updated_at": _utc_now(),
    }


def _write_marker(
    settings: Settings,
    state: str,
    *,
    descendant_pid: int | None = None,
    reason: str | None = None,
) -> None:
    _atomic_json_write(
        settings.runtime_root / _MARKER_NAME,
        _marker_payload(
            settings,
            state,
            descendant_pid=descendant_pid,
            reason=reason,
        ),
    )


def _install_signal_handlers(callback: Callable[[int], None]) -> None:
    def handler(signum: int, _frame: object) -> None:
        callback(signum)

    handled = [signal.SIGINT, signal.SIGTERM]
    sigbreak = getattr(signal, "SIGBREAK", None)
    if sigbreak is not None:
        handled.append(sigbreak)
    for signum in handled:
        try:
            signal.signal(signum, handler)
        except (OSError, ValueError):
            pass


class ServiceState:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.stop_requested = threading.Event()
        self._lock = threading.Lock()
        self._server: ManagedChildHTTPServer | None = None
        self._shutdown_started = False
        self._health_seen = False
        self.reason = "normal"
        self.exit_code = settings.exit_code
        self.descendant: subprocess.Popen[bytes] | None = None

    @property
    def descendant_pid(self) -> int | None:
        descendant = self.descendant
        return descendant.pid if descendant is not None else None

    def attach_server(self, server: ManagedChildHTTPServer) -> None:
        with self._lock:
            self._server = server

    def request_shutdown(self, reason: str, *, exit_code: int | None = None) -> None:
        with self._lock:
            self.reason = reason
            if exit_code is not None:
                self.exit_code = exit_code
            self.stop_requested.set()
            server = self._server
            if server is None or self._shutdown_started:
                return
            self._shutdown_started = True
        threading.Thread(
            target=server.shutdown,
            name="managed-child-shutdown",
            daemon=True,
        ).start()

    def health_payload(self) -> dict[str, object]:
        descendant = self.descendant
        return {
            "status": "ok",
            "identity": self.settings.process_id,
            "process_id": self.settings.process_id,
            "cwd": str(Path.cwd().resolve(strict=False)),
            "runtime": str(self.settings.runtime_root),
            "runtime_root": str(self.settings.runtime_root),
            "host": self.settings.host,
            "port": self.settings.port,
            "pid": os.getpid(),
            "ppid": os.getppid(),
            "nonce": self.settings.launch_nonce,
            "launch_nonce": self.settings.launch_nonce,
            "descendant_pid": descendant.pid if descendant is not None else None,
            "descendant_alive": (
                descendant is not None and descendant.poll() is None
            ),
        }

    def health_was_served(self) -> None:
        with self._lock:
            if self._health_seen:
                return
            self._health_seen = True
        try:
            _write_marker(
                self.settings,
                "healthy",
                descendant_pid=self.descendant_pid,
            )
        except OSError as exc:
            _emit(
                sys.stderr,
                "marker_write_failed",
                self.settings,
                error=exc.__class__.__name__,
            )
        delay = self.settings.crash_after_health
        if delay is None:
            return

        def crash() -> None:
            if delay:
                time.sleep(delay)
            _emit(sys.stdout, "crash_after_health", self.settings)
            _emit(sys.stderr, "crash_after_health", self.settings)
            os._exit(self.settings.crash_exit_code)

        threading.Thread(
            target=crash,
            name="managed-child-crash",
            daemon=True,
        ).start()


def _handler_type(state: ServiceState) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "ManagedChildFixture/1"
        sys_version = ""
        protocol_version = "HTTP/1.1"

        def _send_json(self, status: int, payload: dict[str, object]) -> None:
            body = (json.dumps(payload, sort_keys=True) + "\n").encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Connection", "close")
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)
                self.wfile.flush()

        def _path(self) -> str:
            return urlsplit(self.path).path

        def _handle_health(self) -> None:
            self._send_json(200, state.health_payload())
            state.health_was_served()

        def _handle_exit(self) -> None:
            self._send_json(202, {"status": "stopping", "pid": os.getpid()})
            state.request_shutdown("http_controlled_exit")

        def do_GET(self) -> None:  # noqa: N802 - stdlib callback name
            path = self._path()
            if path == state.settings.health_path:
                self._handle_health()
            elif path in {"/exit", "/control/exit"}:
                self._handle_exit()
            else:
                self._send_json(404, {"status": "not_found", "path": path})

        def do_HEAD(self) -> None:  # noqa: N802 - stdlib callback name
            if self._path() == state.settings.health_path:
                self._handle_health()
            else:
                self._send_json(404, {"status": "not_found"})

        def do_POST(self) -> None:  # noqa: N802 - stdlib callback name
            if self._path() in {"/exit", "/control/exit"}:
                self._handle_exit()
            else:
                self._send_json(404, {"status": "not_found"})

        def log_message(self, _format: str, *_arguments: object) -> None:
            return

    return Handler


def _spawn_descendant(settings: Settings) -> subprocess.Popen[bytes]:
    command = (
        sys.executable,
        str(Path(__file__).resolve(strict=True)),
        "--descendant-worker",
    )
    return subprocess.Popen(
        command,
        stdin=subprocess.DEVNULL,
        stdout=None,
        stderr=None,
        shell=False,
    )


def _stop_descendant(descendant: subprocess.Popen[bytes] | None) -> None:
    if descendant is None or descendant.poll() is not None:
        return
    try:
        descendant.terminate()
        descendant.wait(timeout=2.0)
    except (OSError, subprocess.TimeoutExpired):
        try:
            descendant.kill()
            descendant.wait(timeout=2.0)
        except (OSError, subprocess.TimeoutExpired):
            pass


def _run_descendant(settings: Settings) -> int:
    stop = threading.Event()
    marker_path = settings.runtime_root / f"managed-child-descendant-{os.getpid()}.json"

    def stop_for_signal(_signum: int) -> None:
        stop.set()

    _install_signal_handlers(stop_for_signal)
    payload = _marker_payload(settings, "descendant-running")
    payload["root_process_id"] = settings.process_id
    _atomic_json_write(marker_path, payload)
    _emit(sys.stdout, "descendant_started", settings, ppid=os.getppid())
    _emit(sys.stderr, "descendant_started", settings, ppid=os.getppid())
    while not stop.wait(0.1):
        pass
    payload = _marker_payload(settings, "descendant-stopped", reason="signal")
    payload["root_process_id"] = settings.process_id
    _atomic_json_write(marker_path, payload)
    _emit(sys.stdout, "descendant_stopped", settings)
    _emit(sys.stderr, "descendant_stopped", settings)
    return 0


def _run_service(settings: Settings) -> int:
    settings.runtime_root.mkdir(parents=True, exist_ok=True)
    state = ServiceState(settings)

    def stop_for_signal(signum: int) -> None:
        try:
            reason = f"signal:{signal.Signals(signum).name}"
        except ValueError:
            reason = f"signal:{signum}"
        state.request_shutdown(reason)

    _install_signal_handlers(stop_for_signal)
    _write_marker(settings, "starting")
    _emit(
        sys.stdout,
        "starting",
        settings,
        cwd=str(Path.cwd().resolve(strict=False)),
        runtime_root=str(settings.runtime_root),
    )
    _emit(
        sys.stderr,
        "starting",
        settings,
        cwd=str(Path.cwd().resolve(strict=False)),
        runtime_root=str(settings.runtime_root),
    )

    if settings.crash_immediately:
        _emit(sys.stdout, "immediate_crash", settings)
        _emit(sys.stderr, "immediate_crash", settings)
        os._exit(settings.crash_exit_code)

    if settings.start_delay:
        _write_marker(settings, "delayed-start")
        if state.stop_requested.wait(settings.start_delay):
            _write_marker(settings, "stopped", reason=state.reason)
            return state.exit_code

    if settings.spawn_descendant:
        state.descendant = _spawn_descendant(settings)
        _write_marker(
            settings,
            "starting",
            descendant_pid=state.descendant_pid,
        )

    server: ManagedChildHTTPServer | None = None
    try:
        server = ManagedChildHTTPServer(
            (settings.host, settings.port),
            _handler_type(state),
        )
        state.attach_server(server)
        _write_marker(
            settings,
            "listening",
            descendant_pid=state.descendant_pid,
        )
        _emit(
            sys.stdout,
            "listening",
            settings,
            descendant_pid=state.descendant_pid,
        )
        _emit(
            sys.stderr,
            "listening",
            settings,
            descendant_pid=state.descendant_pid,
        )

        if settings.exit_after is not None:
            def scheduled_exit() -> None:
                if settings.exit_after:
                    state.stop_requested.wait(settings.exit_after)
                if not state.stop_requested.is_set():
                    state.request_shutdown("scheduled_controlled_exit")

            threading.Thread(
                target=scheduled_exit,
                name="managed-child-controlled-exit",
                daemon=True,
            ).start()

        if not state.stop_requested.is_set():
            server.serve_forever(poll_interval=0.05)
    except OSError as exc:
        state.reason = "bind_failed"
        state.exit_code = 98
        _write_marker(
            settings,
            "failed",
            descendant_pid=state.descendant_pid,
            reason=state.reason,
        )
        _emit(
            sys.stderr,
            "bind_failed",
            settings,
            error=exc.__class__.__name__,
        )
    finally:
        if server is not None:
            server.server_close()
        _stop_descendant(state.descendant)

    _write_marker(
        settings,
        "stopped" if state.exit_code == 0 else "failed",
        descendant_pid=state.descendant_pid,
        reason=state.reason,
    )
    _emit(sys.stdout, "stopped", settings, reason=state.reason)
    _emit(sys.stderr, "stopped", settings, reason=state.reason)
    return state.exit_code


def main(arguments: list[str] | None = None) -> int:
    settings = _parse_settings(arguments)
    settings.runtime_root.mkdir(parents=True, exist_ok=True)
    if settings.descendant_worker:
        return _run_descendant(settings)
    return _run_service(settings)


if __name__ == "__main__":
    raise SystemExit(main())
