"""Isolated Git access for managed sprints.

The provider deliberately has no API which accepts a user checkout path.  A
logical repository ID is resolved through a trusted registry and materialised
as one bare mirror below the configured managed root.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import stat
import subprocess
import threading
import time
from typing import Callable, Mapping, Sequence
import urllib.parse
import uuid

from .sprint_types import (
    git_ref_format_valid,
    mirror_storage_key,
    relative_git_path_valid,
)


_REPOSITORY_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,95}\Z")
_COMMIT_ID = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")
_OBJECT_FORMAT_LENGTHS = {"sha1": 40, "sha256": 64}
_CREDENTIAL_REFERENCE = re.compile(
    r"(?:env|keyring|secret-manager|vault|windows-credential):"
    r"[A-Za-z0-9][A-Za-z0-9._/@+-]{0,254}\Z"
)
_SECRET_LITERAL = re.compile(
    r"(?:"
    r"\bBearer\s+[A-Za-z0-9._~+/=-]{8,}|"
    r"-----BEGIN(?: [A-Z0-9]+)? PRIVATE KEY-----|"
    r"\b(?:sk-(?:proj-)?|ghp_|github_pat_|glpat-)[A-Za-z0-9_-]{8,}|"
    r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}"
    r")",
    re.IGNORECASE,
)
_URL_CREDENTIAL = re.compile(r"(?P<prefix>://[^/@\s:]+):[^@/\s]+@")
_OUTPUT_LIMIT = 64 * 1024
_DEFAULT_BLOB_LIMIT = 16 * 1024 * 1024
TRANSIENT_GIT_ERROR_CODES = frozenset(
    {
        "GIT_COMMAND_TIMEOUT",
        "GIT_EXECUTABLE_UNAVAILABLE",
        "REPOSITORY_FETCH_FAILED",
        "REPOSITORY_FETCH_LOCK_TIMEOUT",
    }
)
_RESERVED_GIT_ENVIRONMENT = frozenset(
    {
        "PATH",
        "HOME",
        "USERPROFILE",
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_OBJECT_DIRECTORY",
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
        "GIT_CONFIG_GLOBAL",
        "GIT_CONFIG_NOSYSTEM",
        "GIT_TERMINAL_PROMPT",
        "GIT_OPTIONAL_LOCKS",
        "GCM_INTERACTIVE",
    }
)


def deterministic_branch_publication_id(
    mirror_key: str, branch: str, assignment_id: str
) -> str:
    """Bind one branch-publication identity to its durable assignment lease."""

    digest = hashlib.sha256(
        f"{mirror_key}\0{branch.casefold()}\0{assignment_id}".encode("utf-8")
    ).hexdigest()
    return f"branch-lease-{digest}"


class ManagedGitError(RuntimeError):
    """A transport-neutral, redacted Git provider failure."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        stdout: str = "",
        stderr: str = "",
        returncode: int | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.stdout = stdout
        self.stderr = stderr
        self.returncode = returncode


class FileLockTimeout(TimeoutError):
    """Raised when a managed interprocess lock cannot be acquired in time."""


_THREAD_LOCKS_GUARD = threading.Lock()
_THREAD_LOCKS: dict[str, threading.Lock] = {}


class ManagedFileLock:
    """Small cross-process file lock with an additional in-process mutex."""

    def __init__(self, path: Path, *, timeout: float = 30.0) -> None:
        self.path = Path(path)
        self.timeout = timeout
        self._handle = None
        self._thread_lock: threading.Lock | None = None

    def __enter__(self) -> "ManagedFileLock":
        if self.timeout <= 0:
            raise ValueError("lock timeout must be positive")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        key = os.path.normcase(str(self.path.resolve(strict=False)))
        with _THREAD_LOCKS_GUARD:
            self._thread_lock = _THREAD_LOCKS.setdefault(key, threading.Lock())
        if not self._thread_lock.acquire(timeout=self.timeout):
            raise FileLockTimeout(f"timed out acquiring lock {self.path.name}")

        deadline = time.monotonic() + self.timeout
        try:
            self._handle = self.path.open("a+b")
            self._handle.seek(0, os.SEEK_END)
            if self._handle.tell() == 0:
                self._handle.write(b"\0")
                self._handle.flush()
                os.fsync(self._handle.fileno())
            while True:
                try:
                    self._lock_byte()
                    return self
                except (OSError, BlockingIOError):
                    if time.monotonic() >= deadline:
                        raise FileLockTimeout(
                            f"timed out acquiring lock {self.path.name}"
                        ) from None
                    time.sleep(min(0.05, max(0.0, deadline - time.monotonic())))
        except BaseException:
            if self._handle is not None:
                self._handle.close()
                self._handle = None
            self._thread_lock.release()
            self._thread_lock = None
            raise

    def _lock_byte(self) -> None:
        assert self._handle is not None
        self._handle.seek(0)
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(self._handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(self._handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

    def _unlock_byte(self) -> None:
        assert self._handle is not None
        self._handle.seek(0)
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(self._handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)

    def __exit__(self, exc_type, exc, traceback) -> None:  # type: ignore[no-untyped-def]
        try:
            if self._handle is not None:
                self._unlock_byte()
                self._handle.close()
        finally:
            self._handle = None
            if self._thread_lock is not None:
                self._thread_lock.release()
                self._thread_lock = None


@dataclass(frozen=True)
class RepositorySpec:
    """Trusted registry projection for one logical repository alias."""

    repository_id: str
    canonical_remote: str
    transport_url: str
    credential_reference: str | None = None

    @property
    def repository_key(self) -> str:
        return self.canonical_remote

    @property
    def storage_key(self) -> str:
        return mirror_storage_key(self.canonical_remote)


@dataclass(frozen=True)
class GitCommandResult:
    argv: tuple[str, ...]
    returncode: int
    stdout: str
    stderr: str
    elapsed_seconds: float


@dataclass(frozen=True)
class ManagedRepository:
    spec: RepositorySpec
    repository_id: str
    canonical_remote: str
    mirror_storage_key: str
    mirror_path: Path
    object_format: str
    fetch_result: GitCommandResult


RegistryValue = RepositorySpec | Mapping[str, object]
RegistryResolver = Mapping[str, RegistryValue] | Callable[[str], RegistryValue]
CredentialResolver = Callable[[str], Mapping[str, str]]


def _minimal_environment(overrides: Mapping[str, str] | None = None) -> dict[str, str]:
    names = (
        "PATH",
        "SystemRoot",
        "WINDIR",
        "COMSPEC",
        "PATHEXT",
        "TEMP",
        "TMP",
        "LANG",
        "LC_ALL",
    )
    environment = {name: os.environ[name] for name in names if name in os.environ}
    environment.update(
        {
            "GIT_TERMINAL_PROMPT": "0",
            "GIT_OPTIONAL_LOCKS": "0",
            "GCM_INTERACTIVE": "Never",
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": os.devnull,
        }
    )
    if overrides:
        environment.update(overrides)
    return environment


def _redact(value: str, secrets: Sequence[str] = ()) -> str:
    redacted = value
    for secret in sorted({item for item in secrets if item}, key=len, reverse=True):
        redacted = redacted.replace(secret, "[REDACTED]")
    redacted = _URL_CREDENTIAL.sub(r"\g<prefix>:[REDACTED]@", redacted)
    redacted = _SECRET_LITERAL.sub("[REDACTED]", redacted)
    if len(redacted) > _OUTPUT_LIMIT:
        redacted = redacted[:_OUTPUT_LIMIT] + "\n[OUTPUT TRUNCATED]"
    return redacted


def _process_group_options() -> dict[str, object]:
    if os.name == "nt":
        return {
            "creationflags": getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        }
    return {"start_new_session": True}


class _ManagedProcessTree:
    """Own a command and its descendants for finite timeout enforcement."""

    def __init__(self, process: subprocess.Popen[str]) -> None:
        self.process = process
        self._job_handle = None
        self._kernel32 = None
        if os.name == "nt":
            self._create_windows_job()

    def _create_windows_job(self) -> None:
        try:
            import ctypes
            from ctypes import wintypes

            class BasicLimitInformation(ctypes.Structure):
                _fields_ = [
                    ("PerProcessUserTimeLimit", ctypes.c_int64),
                    ("PerJobUserTimeLimit", ctypes.c_int64),
                    ("LimitFlags", wintypes.DWORD),
                    ("MinimumWorkingSetSize", ctypes.c_size_t),
                    ("MaximumWorkingSetSize", ctypes.c_size_t),
                    ("ActiveProcessLimit", wintypes.DWORD),
                    ("Affinity", ctypes.c_size_t),
                    ("PriorityClass", wintypes.DWORD),
                    ("SchedulingClass", wintypes.DWORD),
                ]

            class IoCounters(ctypes.Structure):
                _fields_ = [
                    ("ReadOperationCount", ctypes.c_uint64),
                    ("WriteOperationCount", ctypes.c_uint64),
                    ("OtherOperationCount", ctypes.c_uint64),
                    ("ReadTransferCount", ctypes.c_uint64),
                    ("WriteTransferCount", ctypes.c_uint64),
                    ("OtherTransferCount", ctypes.c_uint64),
                ]

            class ExtendedLimitInformation(ctypes.Structure):
                _fields_ = [
                    ("BasicLimitInformation", BasicLimitInformation),
                    ("IoInfo", IoCounters),
                    ("ProcessMemoryLimit", ctypes.c_size_t),
                    ("JobMemoryLimit", ctypes.c_size_t),
                    ("PeakProcessMemoryUsed", ctypes.c_size_t),
                    ("PeakJobMemoryUsed", ctypes.c_size_t),
                ]

            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            kernel32.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
            kernel32.CreateJobObjectW.restype = wintypes.HANDLE
            kernel32.SetInformationJobObject.argtypes = [
                wintypes.HANDLE,
                ctypes.c_int,
                ctypes.c_void_p,
                wintypes.DWORD,
            ]
            kernel32.SetInformationJobObject.restype = wintypes.BOOL
            kernel32.AssignProcessToJobObject.argtypes = [
                wintypes.HANDLE,
                wintypes.HANDLE,
            ]
            kernel32.AssignProcessToJobObject.restype = wintypes.BOOL
            kernel32.TerminateJobObject.argtypes = [wintypes.HANDLE, wintypes.UINT]
            kernel32.TerminateJobObject.restype = wintypes.BOOL
            kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
            kernel32.CloseHandle.restype = wintypes.BOOL

            handle = kernel32.CreateJobObjectW(None, None)
            if not handle:
                return
            information = ExtendedLimitInformation()
            information.BasicLimitInformation.LimitFlags = 0x00002000
            configured = kernel32.SetInformationJobObject(
                handle,
                9,
                ctypes.byref(information),
                ctypes.sizeof(information),
            )
            assigned = configured and kernel32.AssignProcessToJobObject(
                handle, wintypes.HANDLE(int(self.process._handle))  # type: ignore[attr-defined]
            )
            if not assigned:
                kernel32.CloseHandle(handle)
                return
            self._job_handle = handle
            self._kernel32 = kernel32
        except (AttributeError, OSError, TypeError, ValueError):
            self._job_handle = None
            self._kernel32 = None

    def _close_job(self) -> None:
        if self._job_handle is not None and self._kernel32 is not None:
            self._kernel32.CloseHandle(self._job_handle)
        self._job_handle = None
        self._kernel32 = None

    def _fallback_windows_terminate(self) -> None:
        system_root = os.environ.get("SystemRoot") or os.environ.get("WINDIR")
        taskkill = (
            str(Path(system_root) / "System32" / "taskkill.exe")
            if system_root
            else "taskkill.exe"
        )
        try:
            subprocess.run(
                (taskkill, "/PID", str(self.process.pid), "/T", "/F"),
                env=_minimal_environment(),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=1.0,
                shell=False,
                check=False,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
        except (OSError, subprocess.TimeoutExpired):
            pass

    def terminate(self) -> None:
        if self._job_handle is not None and self._kernel32 is not None:
            self._kernel32.TerminateJobObject(self._job_handle, 1)
            self._close_job()
        elif os.name == "nt":
            self._fallback_windows_terminate()
        else:
            try:
                os.killpg(self.process.pid, signal.SIGKILL)
            except (OSError, ProcessLookupError):
                pass
        if self.process.poll() is None:
            try:
                self.process.kill()
            except OSError:
                pass
        try:
            self.process.wait(timeout=0.5)
        except subprocess.TimeoutExpired:
            pass

    def close_after_parent(self) -> None:
        if self._job_handle is not None:
            # KILL_ON_JOB_CLOSE ends helpers which inherited our output pipes.
            self._close_job()
        elif os.name != "nt":
            try:
                os.killpg(self.process.pid, signal.SIGKILL)
            except (OSError, ProcessLookupError):
                pass


def _join_reader_threads(
    threads: Sequence[threading.Thread], *, timeout: float
) -> bool:
    deadline = time.monotonic() + max(0.0, timeout)
    for thread in threads:
        thread.join(max(0.0, deadline - time.monotonic()))
    return not any(thread.is_alive() for thread in threads)


def _normalise_repository_path(hostname: str, raw_path: str) -> str:
    path = raw_path.strip("/")
    if path.lower().endswith(".git"):
        path = path[:-4]
    return path.lower() if hostname.rstrip(".").lower() == "github.com" else path


def canonical_remote_from_address(value: str) -> str | None:
    """Return the existing nginx-qa remote identity for a network address."""

    if not isinstance(value, str):
        return None
    address = value.strip()
    if (
        not address
        or len(address) > 2048
        or any(ord(character) < 32 or ord(character) == 127 for character in address)
    ):
        return None
    if re.match(r"^[A-Za-z]:[\\/]", address) or address.startswith(("/", "\\\\")):
        return None
    if "\x00" in address:
        return None

    if "://" not in address:
        scp_match = re.fullmatch(
            r"(?:[^@\s/:]+@)?(?P<host>[A-Za-z0-9.-]+):(?P<path>[^\s]+)",
            address,
        )
        if scp_match:
            host = scp_match.group("host").lower()
            path = _normalise_repository_path(host, scp_match.group("path"))
            return f"{host}/{path}" if path else None

    parsed = urllib.parse.urlparse(address)
    if parsed.scheme:
        if parsed.scheme.lower() not in {"http", "https", "ssh", "git"}:
            return None
        if not parsed.hostname or parsed.password is not None:
            return None
        if parsed.scheme.lower() in {"http", "https", "git"} and parsed.username:
            return None
        if parsed.query or parsed.fragment:
            return None
        try:
            port = parsed.port
        except ValueError:
            return None
        host = parsed.hostname.lower()
        default_port = {"http": 80, "https": 443, "ssh": 22, "git": 9418}[
            parsed.scheme.lower()
        ]
        if port is not None and port != default_port:
            host = f"{host}:{port}"
        path = _normalise_repository_path(parsed.hostname, parsed.path)
        return f"{host}/{path}" if path else None

    bare_match = re.fullmatch(
        r"(?P<host>(?:localhost|[A-Za-z0-9.-]+\.[A-Za-z0-9.-]+))/(?P<path>[^\s]+)",
        address,
        re.IGNORECASE,
    )
    if bare_match:
        host = bare_match.group("host").lower()
        path = _normalise_repository_path(host, bare_match.group("path"))
        return f"{host}/{path}" if path else None
    return None


class GitCommandRunner:
    """Run Git with direct argv, bounded time and redacted captured output."""

    def __init__(self, *, executable: str = "git", timeout: float = 60.0) -> None:
        if timeout <= 0:
            raise ValueError("Git timeout must be positive")
        self.executable = executable
        self.timeout = timeout

    def run(
        self,
        arguments: Sequence[str],
        *,
        cwd: Path | None = None,
        environment: Mapping[str, str] | None = None,
        stdin_text: str | None = None,
        allowed_returncodes: frozenset[int] = frozenset({0}),
        error_code: str = "GIT_COMMAND_FAILED",
    ) -> GitCommandResult:
        if stdin_text is not None and len(stdin_text.encode("utf-8")) > 64 * 1024:
            raise ValueError("Git standard input exceeds the managed limit")
        argv = (self.executable, *(str(item) for item in arguments))
        secrets = tuple((environment or {}).values())
        started = time.monotonic()
        deadline = started + self.timeout
        try:
            process = subprocess.Popen(
                argv,
                cwd=str(cwd) if cwd is not None else None,
                env=_minimal_environment(environment),
                stdin=(
                    subprocess.PIPE
                    if stdin_text is not None
                    else subprocess.DEVNULL
                ),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
                shell=False,
                **_process_group_options(),
            )
        except OSError as exc:
            raise ManagedGitError(
                "GIT_EXECUTABLE_UNAVAILABLE",
                f"Git could not be started: {exc.__class__.__name__}",
            ) from None
        process_tree = _ManagedProcessTree(process)

        stdout_parts: list[str] = []
        stderr_parts: list[str] = []
        stdout_size = 0
        stderr_size = 0
        reader_errors: list[BaseException] = []

        def retain(parts: list[str], value: str, current_size: int) -> int:
            if current_size <= _OUTPUT_LIMIT:
                remaining = _OUTPUT_LIMIT + 1 - current_size
                parts.append(value[:remaining])
                current_size += min(len(value), remaining)
            return current_size

        def drain_stdout() -> None:
            nonlocal stdout_size
            assert process.stdout is not None
            try:
                while True:
                    chunk = process.stdout.read(8192)
                    if not chunk:
                        break
                    stdout_size = retain(stdout_parts, chunk, stdout_size)
            except BaseException as exc:
                reader_errors.append(exc)

        def drain_stderr() -> None:
            nonlocal stderr_size
            assert process.stderr is not None
            try:
                while True:
                    chunk = process.stderr.read(8192)
                    if not chunk:
                        break
                    stderr_size = retain(stderr_parts, chunk, stderr_size)
            except BaseException as exc:
                reader_errors.append(exc)

        stdout_thread = threading.Thread(target=drain_stdout, daemon=True)
        stderr_thread = threading.Thread(target=drain_stderr, daemon=True)
        stdout_thread.start()
        stderr_thread.start()
        if stdin_text is not None:
            assert process.stdin is not None
            try:
                process.stdin.write(stdin_text)
            except (BrokenPipeError, OSError):
                pass
            finally:
                process.stdin.close()
        try:
            returncode = process.wait(timeout=max(0.0, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            process_tree.terminate()
            _join_reader_threads((stdout_thread, stderr_thread), timeout=0.5)
            assert process.stdout is not None and process.stderr is not None
            if not stdout_thread.is_alive():
                process.stdout.close()
            if not stderr_thread.is_alive():
                process.stderr.close()
            raise ManagedGitError(
                "GIT_COMMAND_TIMEOUT",
                f"Git command exceeded the {self.timeout:g}s timeout",
                stdout=_redact("".join(stdout_parts), secrets),
                stderr=_redact("".join(stderr_parts), secrets),
            ) from None
        process_tree.close_after_parent()
        if not _join_reader_threads(
            (stdout_thread, stderr_thread),
            timeout=max(0.0, deadline - time.monotonic()),
        ):
            process_tree.terminate()
            _join_reader_threads((stdout_thread, stderr_thread), timeout=0.5)
            raise ManagedGitError(
                "GIT_COMMAND_TIMEOUT",
                f"Git command exceeded the {self.timeout:g}s timeout",
                stdout=_redact("".join(stdout_parts), secrets),
                stderr=_redact("".join(stderr_parts), secrets),
            ) from None
        assert process.stdout is not None and process.stderr is not None
        process.stdout.close()
        process.stderr.close()
        if reader_errors:
            raise ManagedGitError(
                error_code, "Git command output could not be captured safely"
            ) from None

        result = GitCommandResult(
            argv=tuple(_redact(item, secrets) for item in argv),
            returncode=returncode,
            stdout=_redact("".join(stdout_parts), secrets),
            stderr=_redact("".join(stderr_parts), secrets),
            elapsed_seconds=time.monotonic() - started,
        )
        if result.returncode not in allowed_returncodes:
            raise ManagedGitError(
                error_code,
                f"Git command failed with exit code {result.returncode}",
                stdout=result.stdout,
                stderr=result.stderr,
                returncode=result.returncode,
            )
        return result

    def run_raw_stdout(
        self,
        arguments: Sequence[str],
        *,
        cwd: Path | None = None,
        environment: Mapping[str, str] | None = None,
        max_stdout_bytes: int,
        allowed_returncodes: frozenset[int] = frozenset({0}),
        error_code: str = "GIT_COMMAND_FAILED",
    ) -> bytes:
        """Return bounded raw stdout without decoding or secret redaction.

        Callers must use this only for non-loggable protocol data whose size was
        independently bounded. Error evidence never includes raw stdout.
        """

        if max_stdout_bytes < 0:
            raise ValueError("max_stdout_bytes cannot be negative")
        argv = (self.executable, *(str(item) for item in arguments))
        secrets = tuple((environment or {}).values())
        try:
            process = subprocess.Popen(
                argv,
                cwd=str(cwd) if cwd is not None else None,
                env=_minimal_environment(environment),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=False,
                shell=False,
                **_process_group_options(),
            )
        except OSError as exc:
            raise ManagedGitError(
                "GIT_EXECUTABLE_UNAVAILABLE",
                f"Git could not be started: {exc.__class__.__name__}",
            ) from None
        process_tree = _ManagedProcessTree(process)
        try:
            try:
                stdout, stderr = process.communicate(timeout=self.timeout)
            except subprocess.TimeoutExpired:
                process_tree.terminate()
                try:
                    process.communicate(timeout=0.5)
                except (subprocess.TimeoutExpired, OSError):
                    pass
                raise ManagedGitError(
                    "GIT_COMMAND_TIMEOUT",
                    f"Git command exceeded the {self.timeout:g}s timeout",
                ) from None
            process_tree.close_after_parent()
            stderr_text = _redact(
                stderr.decode("utf-8", errors="replace")[:_OUTPUT_LIMIT],
                secrets,
            )
            if process.returncode not in allowed_returncodes:
                raise ManagedGitError(
                    error_code,
                    f"Git command failed with exit code {process.returncode}",
                    stderr=stderr_text,
                    returncode=process.returncode,
                )
            if len(stdout) > max_stdout_bytes:
                raise ManagedGitError(
                    error_code, "Git raw output exceeded the managed limit"
                )
            return bytes(stdout)
        finally:
            if process.poll() is None:
                process_tree.terminate()

    def run_streaming_lines(
        self,
        arguments: Sequence[str],
        on_stdout_line: Callable[[str], None],
        *,
        cwd: Path | None = None,
        environment: Mapping[str, str] | None = None,
        allowed_returncodes: frozenset[int] = frozenset({0}),
        error_code: str = "GIT_COMMAND_FAILED",
    ) -> GitCommandResult:
        """Process unbounded line output with constant-size retained evidence."""

        argv = (self.executable, *(str(item) for item in arguments))
        secrets = tuple((environment or {}).values())
        started = time.monotonic()
        deadline = started + self.timeout
        try:
            process = subprocess.Popen(
                argv,
                cwd=str(cwd) if cwd is not None else None,
                env=_minimal_environment(environment),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
                shell=False,
                bufsize=1,
                **_process_group_options(),
            )
        except OSError as exc:
            raise ManagedGitError(
                "GIT_EXECUTABLE_UNAVAILABLE",
                f"Git could not be started: {exc.__class__.__name__}",
            ) from None
        process_tree = _ManagedProcessTree(process)

        stdout_parts: list[str] = []
        stderr_parts: list[str] = []
        stdout_size = 0
        stderr_size = 0
        callback_errors: list[BaseException] = []

        def retain(parts: list[str], value: str, current_size: int) -> int:
            if current_size <= _OUTPUT_LIMIT:
                remaining = _OUTPUT_LIMIT + 1 - current_size
                parts.append(value[:remaining])
                current_size += min(len(value), remaining)
            return current_size

        def read_stdout() -> None:
            nonlocal stdout_size
            assert process.stdout is not None
            try:
                for line in process.stdout:
                    stdout_size = retain(stdout_parts, line, stdout_size)
                    on_stdout_line(line.rstrip("\r\n"))
            except BaseException as exc:
                callback_errors.append(exc)
                process_tree.terminate()

        def read_stderr() -> None:
            nonlocal stderr_size
            assert process.stderr is not None
            while True:
                chunk = process.stderr.read(8192)
                if not chunk:
                    break
                stderr_size = retain(stderr_parts, chunk, stderr_size)

        stdout_thread = threading.Thread(target=read_stdout, daemon=True)
        stderr_thread = threading.Thread(target=read_stderr, daemon=True)
        stdout_thread.start()
        stderr_thread.start()
        try:
            returncode = process.wait(timeout=max(0.0, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            process_tree.terminate()
            _join_reader_threads((stdout_thread, stderr_thread), timeout=0.5)
            assert process.stdout is not None and process.stderr is not None
            if not stdout_thread.is_alive():
                process.stdout.close()
            if not stderr_thread.is_alive():
                process.stderr.close()
            raise ManagedGitError(
                "GIT_COMMAND_TIMEOUT",
                f"Git command exceeded the {self.timeout:g}s timeout",
                stdout=_redact("".join(stdout_parts), secrets),
                stderr=_redact("".join(stderr_parts), secrets),
            ) from None
        process_tree.close_after_parent()
        if not _join_reader_threads(
            (stdout_thread, stderr_thread),
            timeout=max(0.0, deadline - time.monotonic()),
        ):
            process_tree.terminate()
            _join_reader_threads((stdout_thread, stderr_thread), timeout=0.5)
            raise ManagedGitError(
                "GIT_COMMAND_TIMEOUT",
                f"Git command exceeded the {self.timeout:g}s timeout",
                stdout=_redact("".join(stdout_parts), secrets),
                stderr=_redact("".join(stderr_parts), secrets),
            ) from None
        assert process.stdout is not None and process.stderr is not None
        process.stdout.close()
        process.stderr.close()
        if callback_errors:
            error = callback_errors[0]
            if isinstance(error, ManagedGitError):
                raise error
            raise ManagedGitError(
                error_code, "Git streaming output could not be validated"
            ) from None

        result = GitCommandResult(
            argv=tuple(_redact(item, secrets) for item in argv),
            returncode=returncode,
            stdout=_redact("".join(stdout_parts), secrets),
            stderr=_redact("".join(stderr_parts), secrets),
            elapsed_seconds=time.monotonic() - started,
        )
        if result.returncode not in allowed_returncodes:
            raise ManagedGitError(
                error_code,
                f"Git command failed with exit code {result.returncode}",
                stdout=result.stdout,
                stderr=result.stderr,
                returncode=result.returncode,
            )
        return result


class ManagedGitProvider:
    """Resolve registry aliases and maintain canonical bare repository mirrors."""

    def __init__(
        self,
        managed_root: str | os.PathLike[str],
        registry: RegistryResolver,
        *,
        credential_resolver: CredentialResolver | None = None,
        git_executable: str = "git",
        command_timeout: float = 60.0,
        lock_timeout: float = 60.0,
        allow_local_transport: bool = False,
    ) -> None:
        root = Path(managed_root)
        if not root.is_absolute():
            raise ValueError("managed_root must be absolute")
        self.managed_root = root.resolve(strict=False)
        if self.managed_root.parent == self.managed_root:
            raise ValueError("managed_root cannot be a filesystem root")
        self.repositories_root = self.managed_root / "repositories"
        self.request_bindings_root = self.managed_root / "repository-bindings"
        self.locks_root = self.managed_root / "locks" / "repositories"
        self.request_binding_locks_root = (
            self.managed_root / "locks" / "repository-bindings"
        )
        self.registry = registry
        self.credential_resolver = credential_resolver
        self.runner = GitCommandRunner(executable=git_executable, timeout=command_timeout)
        self.lock_timeout = lock_timeout
        self.allow_local_transport = allow_local_transport

    def resolve_repository(self, repository_id: str) -> RepositorySpec:
        if _REPOSITORY_ID.fullmatch(repository_id) is None:
            raise ManagedGitError("REPOSITORY_ID_INVALID", "repository_id is invalid")
        try:
            raw = (
                self.registry(repository_id)
                if callable(self.registry)
                else self.registry[repository_id]
            )
        except (KeyError, LookupError):
            raise ManagedGitError(
                "REPOSITORY_NOT_FOUND", "repository_id is not present in the registry"
            ) from None

        if isinstance(raw, RepositorySpec):
            spec = raw
        elif isinstance(raw, Mapping):
            spec = RepositorySpec(
                repository_id=str(raw.get("repository_id") or repository_id),
                canonical_remote=str(
                    raw.get("canonical_remote") or raw.get("repository_key") or ""
                ),
                transport_url=str(
                    raw.get("transport_url") or raw.get("fetch_url") or ""
                ),
                credential_reference=(
                    str(raw["credential_reference"])
                    if raw.get("credential_reference") is not None
                    else None
                ),
            )
        else:
            raise ManagedGitError(
                "REPOSITORY_REGISTRY_INVALID", "registry entry has an invalid shape"
            )

        if spec.repository_id != repository_id:
            raise ManagedGitError(
                "REPOSITORY_IDENTITY_MISMATCH",
                "registry entry does not match the requested repository_id",
            )
        if not isinstance(spec.canonical_remote, str) or not spec.canonical_remote:
            raise ManagedGitError(
                "REPOSITORY_REGISTRY_INVALID", "canonical repository identity is invalid"
            )
        try:
            spec.canonical_remote.encode("utf-8", errors="strict")
        except UnicodeEncodeError:
            raise ManagedGitError(
                "REPOSITORY_REGISTRY_INVALID",
                "canonical repository identity is not valid UTF-8",
            ) from None
        if _SECRET_LITERAL.search(spec.canonical_remote):
            raise ManagedGitError(
                "REPOSITORY_REGISTRY_INVALID", "canonical repository identity is invalid"
            )
        reference = spec.credential_reference
        if reference is not None:
            if not isinstance(reference, str):
                raise ManagedGitError(
                    "CREDENTIAL_REFERENCE_INVALID", "credential reference is invalid"
                )
            try:
                reference.encode("utf-8", errors="strict")
            except UnicodeEncodeError:
                raise ManagedGitError(
                    "CREDENTIAL_REFERENCE_INVALID", "credential reference is invalid"
                ) from None
            if (
                _CREDENTIAL_REFERENCE.fullmatch(reference) is None
                or _SECRET_LITERAL.search(reference) is not None
            ):
                raise ManagedGitError(
                    "CREDENTIAL_REFERENCE_INVALID", "credential reference is invalid"
                )

        if (
            not isinstance(spec.transport_url, str)
            or not spec.transport_url
            or len(spec.transport_url) > 2048
            or any(
                ord(character) < 32 or ord(character) == 127
                for character in spec.transport_url
            )
            or _SECRET_LITERAL.search(spec.transport_url)
        ):
            raise ManagedGitError(
                "REPOSITORY_TRANSPORT_INVALID", "repository transport is invalid"
            )
        try:
            spec.transport_url.encode("utf-8", errors="strict")
        except UnicodeEncodeError:
            raise ManagedGitError(
                "REPOSITORY_TRANSPORT_INVALID", "repository transport is invalid"
            ) from None
        transport_identity = canonical_remote_from_address(spec.transport_url)
        if transport_identity is None:
            if not self.allow_local_transport or not self._is_local_transport(
                spec.transport_url
            ):
                raise ManagedGitError(
                    "REPOSITORY_TRANSPORT_INVALID",
                    "repository transport must be a configured network remote",
                )
        elif transport_identity != spec.canonical_remote:
            raise ManagedGitError(
                "REPOSITORY_IDENTITY_MISMATCH",
                "registry transport and canonical repository identity differ",
            )
        return spec

    @staticmethod
    def _is_local_transport(value: str) -> bool:
        if not value or "\x00" in value:
            return False
        if value.lower().startswith("file://"):
            parsed = urllib.parse.urlparse(value)
            return bool(parsed.path) and not parsed.username and not parsed.password
        return Path(value).is_absolute()

    def _credential_environment(self, spec: RepositorySpec) -> Mapping[str, str]:
        if spec.credential_reference is None:
            return {}
        if self.credential_resolver is None:
            raise ManagedGitError(
                "CREDENTIAL_REFERENCE_UNRESOLVED",
                "repository credential reference has no configured resolver",
            )
        try:
            environment = self.credential_resolver(spec.credential_reference)
        except Exception:
            raise ManagedGitError(
                "CREDENTIAL_REFERENCE_UNRESOLVED",
                "repository credential reference could not be resolved",
            ) from None
        if not isinstance(environment, Mapping) or any(
            not isinstance(key, str) or not isinstance(value, str)
            for key, value in environment.items()
        ):
            raise ManagedGitError(
                "CREDENTIAL_REFERENCE_UNRESOLVED",
                "credential resolver returned an invalid environment",
            )
        for key, value in environment.items():
            upper_key = key.upper()
            if (
                upper_key in _RESERVED_GIT_ENVIRONMENT
                or upper_key.startswith("GIT_CONFIG_")
                or "\x00" in key
                or "=" in key
                or "\x00" in value
            ):
                raise ManagedGitError(
                    "CREDENTIAL_REFERENCE_UNRESOLVED",
                    "credential resolver attempted to override a reserved Git setting",
                )
        return dict(environment)

    def mirror_path_for(self, canonical_remote: str) -> Path:
        return self.repositories_root / f"{mirror_storage_key(canonical_remote)}.git"

    def _ensure_internal_directory(self, path: Path, *, error_code: str) -> None:
        self._assert_internal_path(path)
        try:
            path.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise ManagedGitError(
                error_code, "managed repository directory is unavailable"
            ) from exc
        self._assert_internal_path(path)
        if (
            not path.is_dir()
            or path.is_symlink()
            or _path_is_junction(path)
            or not _path_resolves_to_itself(path)
        ):
            raise ManagedGitError(
                error_code, "managed repository directory is redirected"
            )

    def _prepare_internal_lock_file(self, path: Path, *, error_code: str) -> None:
        """Create and validate a provider lock path before ManagedFileLock opens it."""

        self._assert_internal_path(path)
        if os.path.lexists(path) and (
            not path.is_file()
            or path.is_symlink()
            or _path_is_junction(path)
            or not _path_resolves_to_itself(path)
        ):
            raise ManagedGitError(error_code, "managed lock path is redirected")
        try:
            with path.open("a+b") as handle:
                handle.seek(0, os.SEEK_END)
                if handle.tell() == 0:
                    handle.write(b"\0")
                    handle.flush()
                    os.fsync(handle.fileno())
        except OSError as exc:
            raise ManagedGitError(
                error_code, "managed lock path is unavailable"
            ) from exc
        self._assert_internal_path(path)
        if (
            not path.is_file()
            or path.is_symlink()
            or _path_is_junction(path)
            or not _path_resolves_to_itself(path)
        ):
            raise ManagedGitError(error_code, "managed lock path is redirected")

    def request_operation_lock(
        self, binding_key: str, *, timeout: float
    ) -> ManagedFileLock:
        """Return a root-checked cross-process lock for one start request."""

        if re.fullmatch(r"[0-9a-f]{64}", binding_key) is None:
            raise ManagedGitError(
                "GIT_PIN_INVALID", "request repository binding key is invalid"
            )
        lock_root = self.managed_root / "locks" / "import-requests"
        lock_path = lock_root / f"{binding_key}.lock"
        self._ensure_internal_directory(
            lock_root, error_code="REPOSITORY_BINDING_INVALID"
        )
        self._prepare_internal_lock_file(
            lock_path, error_code="REPOSITORY_BINDING_INVALID"
        )
        return ManagedFileLock(lock_path, timeout=timeout)

    def bind_request_repository(
        self,
        binding_key: str,
        request_fingerprint: str,
        repository_id: str,
        *,
        create: bool = True,
    ) -> tuple[RepositorySpec, bool]:
        """CAS-bind one idempotency scope inside the isolated bare cache."""

        if re.fullmatch(r"[0-9a-f]{64}", binding_key) is None:
            raise ManagedGitError(
                "GIT_PIN_INVALID", "request repository binding key is invalid"
            )
        if re.fullmatch(r"[0-9a-f]{64}", request_fingerprint) is None:
            raise ManagedGitError(
                "GIT_PIN_INVALID", "request repository fingerprint is invalid"
            )
        if _REPOSITORY_ID.fullmatch(repository_id) is None:
            raise ManagedGitError("REPOSITORY_ID_INVALID", "repository_id is invalid")

        binding_ref = f"refs/nginx-qa/request-bindings/{binding_key}"
        binding_path = self.request_bindings_root / f"{binding_key}.json"
        lock_path = self.request_binding_locks_root / f"{binding_key}.lock"
        self._assert_internal_path(self.request_bindings_root)
        self._assert_internal_path(binding_path)
        self._assert_internal_path(self.request_binding_locks_root)
        self._assert_internal_path(lock_path)

        def provider_for(spec: RepositorySpec) -> "ManagedGitProvider":
            provider = ManagedGitProvider(
                self.managed_root,
                {repository_id: spec},
                credential_resolver=self.credential_resolver,
                git_executable=self.runner.executable,
                command_timeout=self.runner.timeout,
                lock_timeout=self.lock_timeout,
                allow_local_transport=self.allow_local_transport,
            )
            provider.runner = self.runner
            return provider

        def decode_binding(raw: bytes) -> RepositorySpec:
            if not raw or len(raw) > 16 * 1024:
                raise ManagedGitError(
                    "REPOSITORY_BINDING_INVALID",
                    "request repository binding size is invalid",
                )
            def reject_nonfinite_constant(value: str) -> None:
                raise ValueError(f"non-finite JSON constant: {value}")

            try:
                payload = json.loads(
                    raw.decode("utf-8", errors="strict"),
                    parse_constant=reject_nonfinite_constant,
                )
                canonical = json.dumps(
                    payload,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            except (
                UnicodeDecodeError,
                UnicodeEncodeError,
                json.JSONDecodeError,
                RecursionError,
                ValueError,
            ) as exc:
                raise ManagedGitError(
                    "REPOSITORY_BINDING_INVALID",
                    "request repository binding is malformed",
                ) from exc
            if raw != canonical or not isinstance(payload, dict):
                raise ManagedGitError(
                    "REPOSITORY_BINDING_INVALID",
                    "request repository binding is not canonical",
                )
            if set(payload) != {
                "schema_version",
                "request_fingerprint",
                "repository",
            } or type(payload.get("schema_version")) is not int or payload.get(
                "schema_version"
            ) != 1:
                raise ManagedGitError(
                    "REPOSITORY_BINDING_INVALID",
                    "request repository binding schema is invalid",
                )
            stored_fingerprint = payload.get("request_fingerprint")
            if not isinstance(stored_fingerprint, str):
                raise ManagedGitError(
                    "REPOSITORY_BINDING_INVALID",
                    "request repository fingerprint is invalid",
                )
            if stored_fingerprint != request_fingerprint:
                raise ManagedGitError(
                    "IDEMPOTENCY_KEY_CONFLICT",
                    "idempotency key is bound to another request",
                )
            repository = payload.get("repository")
            if not isinstance(repository, dict) or set(repository) != {
                "canonical_remote",
                "credential_reference",
                "repository_id",
                "transport_url",
            }:
                raise ManagedGitError(
                    "REPOSITORY_BINDING_INVALID",
                    "request repository identity is invalid",
                )
            if (
                not isinstance(repository.get("repository_id"), str)
                or not isinstance(repository.get("canonical_remote"), str)
                or not isinstance(repository.get("transport_url"), str)
                or (
                    repository.get("credential_reference") is not None
                    and not isinstance(repository.get("credential_reference"), str)
                )
            ):
                raise ManagedGitError(
                    "REPOSITORY_BINDING_INVALID",
                    "request repository identity has invalid field types",
                )
            spec = RepositorySpec(
                repository_id=repository["repository_id"],
                canonical_remote=repository["canonical_remote"],
                transport_url=repository["transport_url"],
                credential_reference=repository["credential_reference"],
            )
            if spec.repository_id != repository_id:
                raise ManagedGitError(
                    "REPOSITORY_BINDING_INVALID",
                    "request repository alias disagrees with its fingerprint",
                )
            provider = provider_for(spec)
            return provider.resolve_repository(repository_id)

        def read_root_binding() -> tuple[RepositorySpec, bytes] | None:
            if not os.path.lexists(binding_path):
                return None
            self._assert_internal_path(binding_path)
            if (
                not binding_path.is_file()
                or binding_path.is_symlink()
                or _path_is_junction(binding_path)
                or not _path_resolves_to_itself(binding_path)
            ):
                raise ManagedGitError(
                    "REPOSITORY_BINDING_INVALID",
                    "request repository binding is not a regular managed file",
                )
            try:
                with binding_path.open("rb") as handle:
                    raw = handle.read(16 * 1024 + 1)
            except OSError as exc:
                raise ManagedGitError(
                    "REPOSITORY_BINDING_INVALID",
                    "request repository binding could not be read",
                ) from exc
            return decode_binding(raw), raw

        def ensure_binding_directory(path: Path) -> None:
            try:
                path.mkdir(parents=True, exist_ok=True)
            except OSError as exc:
                raise ManagedGitError(
                    "REPOSITORY_BINDING_INVALID",
                    "request repository binding directory is unavailable",
                ) from exc
            self._assert_internal_path(path)
            if (
                not path.is_dir()
                or path.is_symlink()
                or _path_is_junction(path)
                or not _path_resolves_to_itself(path)
            ):
                raise ManagedGitError(
                    "REPOSITORY_BINDING_INVALID",
                    "request repository binding directory is redirected",
                )

        def persist_root_binding(raw: bytes) -> bool:
            ensure_binding_directory(self.request_bindings_root)
            self._assert_internal_path(binding_path)
            existing = read_root_binding()
            if existing is not None:
                if existing[1] != raw:
                    raise ManagedGitError(
                        "REPOSITORY_BINDING_INVALID",
                        "request repository binding disagrees with durable identity",
                    )
                return False

            temporary_path = self.request_bindings_root / (
                f".{binding_key}.{uuid.uuid4().hex}.tmp"
            )
            self._assert_internal_path(temporary_path)
            descriptor: int | None = None
            try:
                descriptor = os.open(
                    temporary_path,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                    0o600,
                )
                with os.fdopen(descriptor, "wb") as handle:
                    descriptor = None
                    handle.write(raw)
                    handle.flush()
                    os.fsync(handle.fileno())
                self._assert_internal_path(temporary_path)
                if (
                    not temporary_path.is_file()
                    or temporary_path.is_symlink()
                    or _path_is_junction(temporary_path)
                    or not _path_resolves_to_itself(temporary_path)
                ):
                    raise ManagedGitError(
                        "REPOSITORY_BINDING_INVALID",
                        "request repository binding preparation was redirected",
                    )
                try:
                    os.link(temporary_path, binding_path)
                except FileExistsError:
                    existing = read_root_binding()
                    if existing is None or existing[1] != raw:
                        raise ManagedGitError(
                            "REPOSITORY_BINDING_INVALID",
                            "request repository binding publication conflicted",
                        )
                    return False
                self._assert_internal_path(binding_path)
                published = read_root_binding()
                if published is None or published[1] != raw:
                    raise ManagedGitError(
                        "REPOSITORY_BINDING_INVALID",
                        "request repository binding publication was not durable",
                    )
                if os.name != "nt":
                    directory_descriptor: int | None = None
                    try:
                        directory_descriptor = os.open(
                            self.request_bindings_root,
                            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
                        )
                        os.fsync(directory_descriptor)
                    except OSError:
                        # The file itself is already fsynced and atomically linked.
                        pass
                    finally:
                        if directory_descriptor is not None:
                            os.close(directory_descriptor)
                return True
            except ManagedGitError:
                raise
            except OSError as exc:
                raise ManagedGitError(
                    "REPOSITORY_BINDING_INVALID",
                    "request repository binding could not be published",
                ) from exc
            finally:
                if descriptor is not None:
                    os.close(descriptor)
                try:
                    temporary_path.unlink(missing_ok=True)
                except OSError:
                    pass

        def find_git_binding() -> tuple[RepositorySpec, bytes] | None:
            if not self.repositories_root.exists():
                return None
            self._assert_internal_path(self.repositories_root)
            matches: list[tuple[Path, bytes]] = []
            for mirror_path in sorted(self.repositories_root.glob("*.git")):
                if mirror_path.is_symlink() or not mirror_path.is_dir():
                    raise ManagedGitError(
                        "REPOSITORY_MIRROR_INVALID",
                        "managed repository cache contains a redirected mirror",
                    )
                self._assert_internal_path(mirror_path)
                result = self.runner.run(
                    (
                        "--git-dir",
                        str(mirror_path),
                        "rev-parse",
                        "--verify",
                        "--quiet",
                        binding_ref,
                    ),
                    allowed_returncodes=frozenset({0, 1}),
                    error_code="REPOSITORY_BINDING_INVALID",
                )
                if result.returncode == 1:
                    continue
                object_id = result.stdout.strip()
                if _COMMIT_ID.fullmatch(object_id) is None:
                    raise ManagedGitError(
                        "REPOSITORY_BINDING_INVALID",
                        "request repository binding object is invalid",
                    )
                size_result = self.runner.run(
                    ("--git-dir", str(mirror_path), "cat-file", "-s", object_id),
                    error_code="REPOSITORY_BINDING_INVALID",
                )
                try:
                    blob_size = int(size_result.stdout.strip())
                except ValueError as exc:
                    raise ManagedGitError(
                        "REPOSITORY_BINDING_INVALID",
                        "request repository binding size is malformed",
                    ) from exc
                if blob_size < 1 or blob_size > 16 * 1024:
                    raise ManagedGitError(
                        "REPOSITORY_BINDING_INVALID",
                        "request repository binding size is invalid",
                    )
                blob = self.runner.run_raw_stdout(
                    ("--git-dir", str(mirror_path), "cat-file", "blob", object_id),
                    max_stdout_bytes=blob_size,
                    error_code="REPOSITORY_BINDING_INVALID",
                )
                if len(blob) != blob_size:
                    raise ManagedGitError(
                        "REPOSITORY_BINDING_INVALID",
                        "request repository binding size changed while reading",
                    )
                matches.append((mirror_path, blob))
            if not matches:
                return None
            if len(matches) != 1:
                raise ManagedGitError(
                    "REPOSITORY_BINDING_INVALID",
                    "request repository binding exists in multiple mirrors",
                )
            mirror_path, raw = matches[0]
            spec = decode_binding(raw)
            expected_mirror = provider_for(spec).mirror_path_for(spec.canonical_remote)
            if expected_mirror.resolve(strict=False) != mirror_path.resolve(
                strict=False
            ):
                raise ManagedGitError(
                    "REPOSITORY_BINDING_INVALID",
                    "request repository binding is stored in another mirror",
                )
            return spec, raw

        def serialize_binding(spec: RepositorySpec) -> bytes:
            payload = {
                "schema_version": 1,
                "request_fingerprint": request_fingerprint,
                "repository": {
                    "repository_id": spec.repository_id,
                    "canonical_remote": spec.canonical_remote,
                    "transport_url": spec.transport_url,
                    "credential_reference": spec.credential_reference,
                },
            }
            return json.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")

        def publish_git_binding(repository: ManagedRepository, raw: bytes) -> None:
            object_id = self.runner.run(
                (
                    "--git-dir",
                    str(repository.mirror_path),
                    "hash-object",
                    "-w",
                    "--stdin",
                ),
                stdin_text=raw.decode("utf-8", errors="strict"),
                error_code="REPOSITORY_BINDING_INVALID",
            ).stdout.strip()
            if _COMMIT_ID.fullmatch(object_id) is None:
                raise ManagedGitError(
                    "REPOSITORY_BINDING_INVALID",
                    "request repository binding object was not created",
                )
            zero = "0" * (40 if repository.object_format == "sha1" else 64)
            self.runner.run(
                (
                    "--git-dir",
                    str(repository.mirror_path),
                    "update-ref",
                    binding_ref,
                    object_id,
                    zero,
                ),
                error_code="REPOSITORY_BINDING_INVALID",
            )

        try:
            ensure_binding_directory(self.request_binding_locks_root)
            self._prepare_internal_lock_file(
                lock_path, error_code="REPOSITORY_BINDING_INVALID"
            )
            with ManagedFileLock(lock_path, timeout=self.lock_timeout):
                root_binding = read_root_binding()
                git_binding = find_git_binding()
                created = False
                if root_binding is not None:
                    spec, serialized = root_binding
                    if git_binding is not None and git_binding[1] != serialized:
                        raise ManagedGitError(
                            "REPOSITORY_BINDING_INVALID",
                            "Git binding disagrees with durable request identity",
                        )
                elif git_binding is not None:
                    spec, serialized = git_binding
                    persist_root_binding(serialized)
                else:
                    if not create:
                        raise ManagedGitError(
                            "REPOSITORY_BINDING_NOT_FOUND",
                            "durable request repository binding is missing",
                        )
                    spec = self.resolve_repository(repository_id)
                    serialized = serialize_binding(spec)
                    # This immutable receipt must exist before mirror creation,
                    # fetching, or any other crashable repository operation.
                    created = persist_root_binding(serialized)

                provider = provider_for(spec)
                repository = provider.ensure_mirror(repository_id, fetch=created)
                if git_binding is None:
                    publish_git_binding(repository, serialized)
                return spec, created
        except FileLockTimeout:
            raise ManagedGitError(
                "REPOSITORY_FETCH_LOCK_TIMEOUT",
                "timed out waiting for the request repository binding lock",
            ) from None

    def _assert_internal_path(self, path: Path) -> None:
        resolved = _resolve_from_nearest_existing(path)
        if os.path.normcase(os.path.normpath(str(path.absolute()))) != os.path.normcase(
            os.path.normpath(str(resolved))
        ):
            raise ManagedGitError(
                "REPOSITORY_MIRROR_INVALID",
                "managed repository path contains a symlink or junction alias",
            )
        try:
            common = os.path.commonpath(
                (
                    os.path.normcase(str(resolved)),
                    os.path.normcase(str(self.managed_root)),
                )
            )
        except ValueError:
            common = ""
        if common != os.path.normcase(str(self.managed_root)) or resolved == self.managed_root:
            raise ManagedGitError(
                "REPOSITORY_MIRROR_INVALID",
                "managed repository path escaped the managed root",
            )

    def ensure_mirror(self, repository_id: str, *, fetch: bool = True) -> ManagedRepository:
        spec = self.resolve_repository(repository_id)
        mirror_path = self.mirror_path_for(spec.canonical_remote)
        lock_path = self.locks_root / f"{spec.storage_key}.lock"
        self._ensure_internal_directory(
            self.repositories_root, error_code="REPOSITORY_MIRROR_INVALID"
        )
        self._ensure_internal_directory(
            self.locks_root, error_code="REPOSITORY_MIRROR_INVALID"
        )
        self._prepare_internal_lock_file(
            lock_path, error_code="REPOSITORY_MIRROR_INVALID"
        )
        try:
            with ManagedFileLock(lock_path, timeout=self.lock_timeout):
                self._ensure_internal_directory(
                    self.repositories_root,
                    error_code="REPOSITORY_MIRROR_INVALID",
                )
                self._ensure_internal_directory(
                    self.locks_root,
                    error_code="REPOSITORY_MIRROR_INVALID",
                )
                existed = mirror_path.exists()
                if existed:
                    object_format = self._verify_bare_mirror(spec, mirror_path)
                    if fetch:
                        result = self.runner.run(
                            (
                                "--git-dir",
                                str(mirror_path),
                                "fetch",
                                "--prune",
                                "origin",
                            ),
                            environment=self._credential_environment(spec),
                            error_code="REPOSITORY_FETCH_FAILED",
                        )
                    else:
                        result = GitCommandResult((), 0, "", "", 0.0)
                else:
                    # Keep fetched branch observations separate from service-owned
                    # local branch and immutable pin refs.  A clone --mirror
                    # refspec (+refs/*:refs/*) would allow a fetch to overwrite
                    # both namespaces.
                    preparation_path = self.repositories_root / (
                        f".prepare-{uuid.uuid4().hex}"
                    )
                    if preparation_path.exists() or preparation_path.is_symlink():
                        raise ManagedGitError(
                            "REPOSITORY_MIRROR_INVALID",
                            "repository preparation path already exists",
                        )
                    self._assert_internal_path(preparation_path)
                    object_format = self._detect_remote_object_format(spec)
                    self.runner.run(
                        (
                            "init",
                            "--bare",
                            f"--object-format={object_format}",
                            "--",
                            str(preparation_path),
                        ),
                        error_code="REPOSITORY_FETCH_FAILED",
                    )
                    self.runner.run(
                        (
                            "--git-dir",
                            str(preparation_path),
                            "remote",
                            "add",
                            "origin",
                            spec.transport_url,
                        ),
                        error_code="REPOSITORY_FETCH_FAILED",
                    )
                    self.runner.run(
                        (
                            "--git-dir",
                            str(preparation_path),
                            "config",
                            "remote.origin.fetch",
                            "+refs/heads/*:refs/remotes/origin/*",
                        ),
                        error_code="REPOSITORY_FETCH_FAILED",
                    )
                    self.runner.run(
                        (
                            "--git-dir",
                            str(preparation_path),
                            "config",
                            "--add",
                            "remote.origin.fetch",
                            "+refs/tags/*:refs/tags/*",
                        ),
                        error_code="REPOSITORY_FETCH_FAILED",
                    )
                    result = self.runner.run(
                        (
                            "--git-dir",
                            str(preparation_path),
                            "fetch",
                            "--prune",
                            "origin",
                        ),
                        environment=self._credential_environment(spec),
                        error_code="REPOSITORY_FETCH_FAILED",
                    )
                    actual_format = self._verify_bare_mirror(spec, preparation_path)
                    if actual_format != object_format:
                        raise ManagedGitError(
                            "REPOSITORY_MIRROR_INVALID",
                            "managed mirror object format changed during preparation",
                        )
                    if mirror_path.exists():
                        raise ManagedGitError(
                            "REPOSITORY_MIRROR_INVALID",
                            "managed mirror appeared during locked creation",
                        )
                    preparation_path.rename(mirror_path)
                    actual_format = self._verify_bare_mirror(spec, mirror_path)
                    if actual_format != object_format:
                        raise ManagedGitError(
                            "REPOSITORY_MIRROR_INVALID",
                            "managed mirror object format changed during publication",
                        )
        except FileLockTimeout:
            raise ManagedGitError(
                "REPOSITORY_FETCH_LOCK_TIMEOUT",
                "timed out waiting for the repository fetch lock",
            ) from None
        return ManagedRepository(
            spec=spec,
            repository_id=repository_id,
            canonical_remote=spec.canonical_remote,
            mirror_storage_key=spec.storage_key,
            mirror_path=mirror_path,
            object_format=object_format,
            fetch_result=result,
        )

    def _detect_remote_object_format(self, spec: RepositorySpec) -> str:
        formats: set[str] = set()

        def inspect(line: str) -> None:
            if not line:
                return
            fields = line.split("\t", 1)
            if len(fields) != 2:
                raise ManagedGitError(
                    "REPOSITORY_FETCH_FAILED",
                    "remote advertised a malformed ref",
                )
            object_id = fields[0].lower()
            if re.fullmatch(r"[0-9a-f]{40}", object_id):
                formats.add("sha1")
            elif re.fullmatch(r"[0-9a-f]{64}", object_id):
                formats.add("sha256")
            else:
                raise ManagedGitError(
                    "REPOSITORY_FETCH_FAILED",
                    "remote advertised an unsupported object ID",
                )
            if len(formats) != 1:
                raise ManagedGitError(
                    "REPOSITORY_FETCH_FAILED",
                    "remote advertised inconsistent object formats",
                )

        self.runner.run_streaming_lines(
            ("ls-remote", "--refs", "--", spec.transport_url),
            inspect,
            environment=self._credential_environment(spec),
            error_code="REPOSITORY_FETCH_FAILED",
        )
        if len(formats) != 1:
            raise ManagedGitError(
                "REPOSITORY_FETCH_FAILED",
                "remote did not advertise an object format",
            )
        return next(iter(formats))

    def _verify_bare_mirror(self, spec: RepositorySpec, path: Path) -> str:
        if (
            not path.is_dir()
            or path.is_symlink()
            or _path_is_junction(path)
            or not _path_resolves_to_itself(path)
        ):
            raise ManagedGitError(
                "REPOSITORY_MIRROR_INVALID", "managed mirror is not a regular directory"
            )
        expected = path.resolve(strict=True)
        repositories_root = self.repositories_root.resolve(strict=True)
        try:
            expected.relative_to(repositories_root)
        except ValueError:
            raise ManagedGitError(
                "REPOSITORY_MIRROR_INVALID", "managed mirror escaped its repository root"
            ) from None
        bare = self.runner.run(
            ("--git-dir", str(path), "rev-parse", "--is-bare-repository"),
            error_code="REPOSITORY_MIRROR_INVALID",
        )
        if bare.stdout.strip() != "true":
            raise ManagedGitError(
                "REPOSITORY_MIRROR_INVALID", "managed repository cache is not bare"
            )
        object_format = self.runner.run(
            ("--git-dir", str(path), "rev-parse", "--show-object-format"),
            error_code="REPOSITORY_MIRROR_INVALID",
        ).stdout.strip()
        if object_format not in _OBJECT_FORMAT_LENGTHS:
            raise ManagedGitError(
                "REPOSITORY_MIRROR_INVALID",
                "managed mirror uses an unsupported object format",
            )
        actual_dir = self.runner.run(
            ("--git-dir", str(path), "rev-parse", "--absolute-git-dir"),
            error_code="REPOSITORY_MIRROR_INVALID",
        ).stdout.strip()
        if not _same_path(Path(actual_dir), expected):
            raise ManagedGitError(
                "REPOSITORY_MIRROR_INVALID", "managed mirror Git directory is mismatched"
            )
        common_dir = self.runner.run(
            (
                "--git-dir",
                str(path),
                "rev-parse",
                "--path-format=absolute",
                "--git-common-dir",
            ),
            error_code="REPOSITORY_MIRROR_INVALID",
        ).stdout.strip()
        object_dir = self.runner.run(
            (
                "--git-dir",
                str(path),
                "rev-parse",
                "--path-format=absolute",
                "--git-path",
                "objects",
            ),
            error_code="REPOSITORY_MIRROR_INVALID",
        ).stdout.strip()
        refs_dir = self.runner.run(
            (
                "--git-dir",
                str(path),
                "rev-parse",
                "--path-format=absolute",
                "--git-path",
                "refs",
            ),
            error_code="REPOSITORY_MIRROR_INVALID",
        ).stdout.strip()
        expected_objects = expected / "objects"
        expected_refs = expected / "refs"
        if (
            not _managed_git_storage_safe(expected, bare=True)
            or not _path_resolves_to_itself(expected_objects)
            or not _same_path(Path(common_dir), expected)
            or not _same_path(Path(object_dir), expected_objects)
            or not _same_path(Path(refs_dir), expected_refs)
        ):
            raise ManagedGitError(
                "REPOSITORY_MIRROR_INVALID",
                "managed mirror depends on external common or object storage",
            )
        alternates = expected / "objects" / "info" / "alternates"
        if alternates.exists() or alternates.is_symlink():
            raise ManagedGitError(
                "REPOSITORY_MIRROR_INVALID",
                "managed mirror may not use an alternate object store",
            )
        fetch_urls = self.runner.run(
            ("--git-dir", str(path), "remote", "get-url", "--all", "origin"),
            error_code="REPOSITORY_MIRROR_INVALID",
        ).stdout.splitlines()
        push_urls = self.runner.run(
            (
                "--git-dir",
                str(path),
                "remote",
                "get-url",
                "--push",
                "--all",
                "origin",
            ),
            error_code="REPOSITORY_MIRROR_INVALID",
        ).stdout.splitlines()
        if not self.remote_configuration_matches(spec, fetch_urls, push_urls):
            raise ManagedGitError(
                "REPOSITORY_IDENTITY_MISMATCH",
                "managed mirror remote configuration does not match the registry",
            )
        fetch_refspec = self.runner.run(
            (
                "--git-dir",
                str(path),
                "config",
                "--get-all",
                "remote.origin.fetch",
            ),
            allowed_returncodes=frozenset({0, 1}),
            error_code="REPOSITORY_MIRROR_INVALID",
        )
        refspecs = tuple(
            line.strip() for line in fetch_refspec.stdout.splitlines() if line.strip()
        )
        if refspecs != (
            "+refs/heads/*:refs/remotes/origin/*",
            "+refs/tags/*:refs/tags/*",
        ):
            raise ManagedGitError(
                "REPOSITORY_MIRROR_INVALID",
                "managed mirror has a non-isolated fetch refspec",
            )
        return object_format

    def remote_matches(self, spec: RepositorySpec, actual_remote: str) -> bool:
        actual_identity = canonical_remote_from_address(actual_remote)
        if actual_identity is not None:
            return actual_identity == spec.canonical_remote
        if not self.allow_local_transport or not self._is_local_transport(actual_remote):
            return False
        return _same_local_transport(actual_remote, spec.transport_url)

    def remote_configuration_matches(
        self,
        spec: RepositorySpec,
        fetch_urls: Sequence[str],
        push_urls: Sequence[str],
    ) -> bool:
        clean_fetch = tuple(value.strip() for value in fetch_urls if value.strip())
        clean_push = tuple(value.strip() for value in push_urls if value.strip())
        return (
            len(clean_fetch) == 1
            and len(clean_push) == 1
            and self.remote_matches(spec, clean_fetch[0])
            and self.remote_matches(spec, clean_push[0])
        )

    def resolve_commit(self, repository: ManagedRepository, ref: str) -> str:
        if not git_ref_format_valid(ref):
            raise ManagedGitError("GIT_REF_INVALID", "source ref is invalid")
        # Managed mirrors deliberately keep fetched branches outside
        # refs/heads so a fetch can never overwrite a service-owned assignment
        # branch.  Public source refs still use the ordinary refs/heads form;
        # resolve those against the isolated fetched namespace.
        resolved_ref = (
            f"refs/remotes/origin/{ref.removeprefix('refs/heads/')}"
            if ref.startswith("refs/heads/")
            else ref
        )
        result = self.runner.run(
            (
                "--git-dir",
                str(repository.mirror_path),
                "rev-parse",
                "--verify",
                f"{resolved_ref}^{{commit}}",
            ),
            error_code="SOURCE_COMMIT_NOT_FOUND",
        )
        commit = result.stdout.strip().lower()
        if not _commit_matches_object_format(commit, repository.object_format):
            raise ManagedGitError(
                "SOURCE_COMMIT_NOT_FOUND", "source ref did not resolve to a full commit"
            )
        return commit

    def read_blob(
        self,
        repository: ManagedRepository,
        commit: str,
        path: str,
        *,
        max_bytes: int = _DEFAULT_BLOB_LIMIT,
    ) -> bytes:
        """Read exact immutable blob bytes from the managed bare mirror.

        The size is checked before materialising output so a repository cannot
        make the API retain an unbounded blob.  No checkout path, shell, or
        caller-controlled environment is involved.
        """

        self.assert_commit(repository, commit)
        if not relative_git_path_valid(path):
            raise ManagedGitError("GIT_PATH_INVALID", "repository path is invalid")
        if (
            not isinstance(max_bytes, int)
            or isinstance(max_bytes, bool)
            or max_bytes < 1
        ):
            raise ValueError("max_bytes must be a positive integer")

        object_name = f"{commit}:{path}"
        size_result = self.runner.run(
            (
                "--git-dir",
                str(repository.mirror_path),
                "cat-file",
                "-s",
                object_name,
            ),
            error_code="GIT_BLOB_NOT_FOUND",
        )
        try:
            size = int(size_result.stdout.strip())
        except ValueError:
            raise ManagedGitError(
                "REPOSITORY_MIRROR_INVALID", "Git returned an invalid blob size"
            ) from None
        if size < 0 or size > max_bytes:
            raise ManagedGitError(
                "GIT_BLOB_TOO_LARGE", "repository blob exceeds the configured limit"
            )

        argv = (
            self.runner.executable,
            "--git-dir",
            str(repository.mirror_path),
            "cat-file",
            "blob",
            object_name,
        )
        try:
            process = subprocess.Popen(
                argv,
                env=_minimal_environment(),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                shell=False,
                **_process_group_options(),
            )
        except OSError as exc:
            raise ManagedGitError(
                "GIT_EXECUTABLE_UNAVAILABLE",
                f"Git could not be started: {exc.__class__.__name__}",
            ) from None
        process_tree = _ManagedProcessTree(process)
        try:
            stdout, stderr = process.communicate(timeout=self.runner.timeout)
        except subprocess.TimeoutExpired:
            process_tree.terminate()
            try:
                process.communicate(timeout=0.5)
            except subprocess.TimeoutExpired:
                pass
            raise ManagedGitError(
                "GIT_COMMAND_TIMEOUT",
                f"Git command exceeded the {self.runner.timeout:g}s timeout",
            ) from None
        finally:
            process_tree.close_after_parent()
        if process.returncode != 0:
            safe_stderr = _redact(
                stderr.decode("utf-8", errors="replace")[:_OUTPUT_LIMIT]
            )
            raise ManagedGitError(
                "GIT_BLOB_NOT_FOUND",
                f"Git command failed with exit code {process.returncode}",
                stderr=safe_stderr,
                returncode=process.returncode,
            )
        if len(stdout) != size or len(stdout) > max_bytes:
            raise ManagedGitError(
                "REPOSITORY_MIRROR_INVALID",
                "Git blob size changed while reading immutable content",
            )
        return stdout

    def assert_commit(self, repository: ManagedRepository, commit: str) -> str:
        if not _commit_matches_object_format(commit, repository.object_format):
            raise ManagedGitError("SOURCE_COMMIT_NOT_FOUND", "commit ID is not canonical")
        result = self.runner.run(
            (
                "--git-dir",
                str(repository.mirror_path),
                "rev-parse",
                "--verify",
                f"{commit}^{{commit}}",
            ),
            error_code="SOURCE_COMMIT_NOT_FOUND",
        )
        resolved = result.stdout.strip().lower()
        if resolved != commit:
            raise ManagedGitError("SOURCE_COMMIT_NOT_FOUND", "commit ID did not resolve exactly")
        return resolved

    def branch_heads(
        self, repository: ManagedRepository, branch: str
    ) -> Mapping[str, str]:
        self.assert_branch_name(branch)
        requested_components = tuple(component.casefold() for component in branch.split("/"))
        result: dict[str, str] = {}

        def inspect(line: str) -> None:
            if not line:
                return
            try:
                ref, commit = line.split("\x00", 1)
            except ValueError:
                raise ManagedGitError(
                    "REPOSITORY_MIRROR_INVALID", "branch listing is malformed"
                ) from None
            namespace: str | None = None
            suffix = ""
            if ref.startswith("refs/heads/"):
                namespace = "local"
                suffix = ref.removeprefix("refs/heads/")
            elif ref.startswith("refs/remotes/origin/"):
                namespace = "remote"
                suffix = ref.removeprefix("refs/remotes/origin/")
            if namespace is None:
                return
            components = tuple(component.casefold() for component in suffix.split("/"))
            if namespace == "local" and (
                components[: len(requested_components)] == requested_components
                or requested_components[: len(components)] == components
            ) and components != requested_components:
                raise ManagedGitError(
                    "BRANCH_ALREADY_EXISTS",
                    "assigned branch has a local directory/file ref collision",
                )
            if components != requested_components:
                return
            if suffix != branch:
                raise ManagedGitError(
                    "BRANCH_DIVERGED",
                    "assigned branch has a case-folded alias in the mirror",
                )
            commit = commit.strip().lower()
            if not _commit_matches_object_format(commit, repository.object_format):
                raise ManagedGitError(
                    "REPOSITORY_MIRROR_INVALID",
                    "branch ref is not a canonical object ID",
                )
            expected_ref = (
                f"refs/heads/{branch}"
                if namespace == "local"
                else f"refs/remotes/origin/{branch}"
            )
            if expected_ref in result:
                raise ManagedGitError(
                    "BRANCH_DIVERGED", "assigned branch has ambiguous aliases"
                )
            result[expected_ref] = commit

        self.runner.run_streaming_lines(
            (
                "--git-dir",
                str(repository.mirror_path),
                "for-each-ref",
                "--format=%(refname)%00%(objectname)",
                "refs/heads",
                "refs/remotes/origin",
            ),
            inspect,
            error_code="REPOSITORY_MIRROR_INVALID",
        )
        for commit in result.values():
            self.assert_commit(repository, commit)
        return result

    def _read_ref(self, repository: ManagedRepository, ref: str) -> str | None:
        command = self.runner.run(
            (
                "--git-dir",
                str(repository.mirror_path),
                "rev-parse",
                "--verify",
                "--quiet",
                ref,
            ),
            allowed_returncodes=frozenset({0, 1}),
            error_code="REPOSITORY_MIRROR_INVALID",
        )
        if command.returncode == 1:
            return None
        value = command.stdout.strip().lower()
        if (
            not _commit_matches_object_format(value, repository.object_format)
            or "\n" in value
        ):
            raise ManagedGitError(
                "REPOSITORY_MIRROR_INVALID", "repository ref is not a canonical object ID"
            )
        return value

    def _read_internal_blob(
        self,
        repository: ManagedRepository,
        object_id: str,
        *,
        error_code: str,
        max_bytes: int = 16 * 1024,
    ) -> bytes:
        """Read a small provider-owned blob without accepting a tree path."""

        if not _commit_matches_object_format(object_id, repository.object_format):
            raise ManagedGitError(error_code, "internal Git object ID is invalid")
        size_result = self.runner.run(
            (
                "--git-dir",
                str(repository.mirror_path),
                "cat-file",
                "-s",
                object_id,
            ),
            error_code=error_code,
        )
        try:
            size = int(size_result.stdout.strip())
        except ValueError:
            raise ManagedGitError(
                error_code, "internal Git blob size is invalid"
            ) from None
        if size < 1 or size > max_bytes:
            raise ManagedGitError(error_code, "internal Git blob size is invalid")
        raw = self.runner.run_raw_stdout(
            (
                "--git-dir",
                str(repository.mirror_path),
                "cat-file",
                "blob",
                object_id,
            ),
            max_stdout_bytes=size,
            error_code=error_code,
        )
        if len(raw) != size:
            raise ManagedGitError(
                error_code, "internal Git blob size changed while reading"
            )
        return raw

    def _write_internal_blob(
        self,
        repository: ManagedRepository,
        raw: bytes,
        *,
        error_code: str,
    ) -> str:
        """Write bounded canonical provider metadata as an immutable Git blob."""

        if not raw or len(raw) > 16 * 1024:
            raise ManagedGitError(error_code, "internal Git blob size is invalid")
        try:
            text = raw.decode("utf-8", errors="strict")
        except UnicodeDecodeError:
            raise ManagedGitError(
                error_code, "internal Git blob encoding is invalid"
            ) from None
        object_id = self.runner.run(
            (
                "--git-dir",
                str(repository.mirror_path),
                "hash-object",
                "-w",
                "--stdin",
            ),
            stdin_text=text,
            error_code=error_code,
        ).stdout.strip().lower()
        if not _commit_matches_object_format(object_id, repository.object_format):
            raise ManagedGitError(error_code, "internal Git blob was not created")
        return object_id

    def _reclaim_expected_ref_lock(
        self,
        repository: ManagedRepository,
        ref: str,
        expected_object_id: str,
        *,
        error_code: str,
    ) -> bool:
        """Remove only a provenance-bound lock left by a dead Git child.

        Callers hold the repository's managed mutation lock.  The exact custom
        ref and exact object ID are both fixed before this method is reached;
        arbitrary Git locks are never scanned or removed.
        """

        if not (
            ref.startswith("refs/nginx-qa/") or ref.startswith("refs/heads/")
        ) or not _commit_matches_object_format(
            expected_object_id, repository.object_format
        ):
            raise ManagedGitError(
                error_code, "internal Git ref lock identity is invalid"
            )
        ref_path = repository.mirror_path.joinpath(*ref.split("/"))
        lock_path = ref_path.with_name(f"{ref_path.name}.lock")
        self._assert_internal_path(lock_path)
        if not os.path.lexists(lock_path):
            return False
        if (
            not lock_path.is_file()
            or lock_path.is_symlink()
            or _path_is_junction(lock_path)
            or not _path_resolves_to_itself(lock_path)
        ):
            raise ManagedGitError(error_code, "internal Git ref lock is redirected")
        try:
            with lock_path.open("rb") as handle:
                raw = handle.read(len(expected_object_id) + 2)
        except OSError as exc:
            raise ManagedGitError(
                error_code, "internal Git ref lock is unreadable"
            ) from exc
        if raw != f"{expected_object_id}\n".encode("ascii"):
            raise ManagedGitError(
                error_code, "internal Git ref lock does not match publication intent"
            )
        try:
            lock_path.unlink()
        except OSError as exc:
            raise ManagedGitError(
                error_code, "stale internal Git ref lock remains"
            ) from exc
        return True

    def _create_internal_ref(
        self,
        repository: ManagedRepository,
        ref: str,
        object_id: str,
        *,
        error_code: str,
    ) -> None:
        """CAS-create one custom ref and recover only its exact stale lock."""

        existing = self._read_ref(repository, ref)
        if existing is not None:
            if existing != object_id:
                raise ManagedGitError(error_code, "internal Git ref disagrees with intent")
            return
        zero = "0" * _OBJECT_FORMAT_LENGTHS[repository.object_format]
        for attempt in range(2):
            try:
                self.runner.run(
                    (
                        "--git-dir",
                        str(repository.mirror_path),
                        "update-ref",
                        ref,
                        object_id,
                        zero,
                    ),
                    error_code=error_code,
                )
            except ManagedGitError:
                existing = self._read_ref(repository, ref)
                if existing == object_id:
                    return
                if attempt == 0 and self._reclaim_expected_ref_lock(
                    repository,
                    ref,
                    object_id,
                    error_code=error_code,
                ):
                    continue
                raise
            if self._read_ref(repository, ref) != object_id:
                raise ManagedGitError(error_code, "internal Git ref was not committed")
            return
        raise ManagedGitError(error_code, "internal Git ref could not be committed")

    def _update_publication_branch(
        self,
        repository: ManagedRepository,
        branch_ref: str,
        selected_head: str,
        expected_old_head: str | None,
    ) -> None:
        """CAS one intent-backed branch and recover only its exact stale lock."""

        zero = "0" * _OBJECT_FORMAT_LENGTHS[repository.object_format]
        expected = expected_old_head if expected_old_head is not None else zero
        for attempt in range(2):
            try:
                self.runner.run(
                    (
                        "--git-dir",
                        str(repository.mirror_path),
                        "update-ref",
                        branch_ref,
                        selected_head,
                        expected,
                    ),
                    error_code="BRANCH_DIVERGED",
                )
            except ManagedGitError:
                current = self._read_ref(repository, branch_ref)
                if current == selected_head:
                    return
                if current != expected_old_head:
                    raise
                if attempt == 0 and self._reclaim_expected_ref_lock(
                    repository,
                    branch_ref,
                    selected_head,
                    error_code="BRANCH_DIVERGED",
                ):
                    continue
                raise
            if self._read_ref(repository, branch_ref) != selected_head:
                raise ManagedGitError(
                    "BRANCH_DIVERGED", "branch publication was not committed"
                )
            return
        raise ManagedGitError("BRANCH_DIVERGED", "branch publication failed")

    def is_ancestor(
        self, repository: ManagedRepository, ancestor: str, descendant: str
    ) -> bool:
        self.assert_commit(repository, ancestor)
        self.assert_commit(repository, descendant)
        result = self.runner.run(
            (
                "--git-dir",
                str(repository.mirror_path),
                "merge-base",
                "--is-ancestor",
                ancestor,
                descendant,
            ),
            allowed_returncodes=frozenset({0, 1}),
            error_code="REPOSITORY_MIRROR_INVALID",
        )
        return result.returncode == 0

    def pin_commit(
        self,
        repository: ManagedRepository,
        commit: str,
        *,
        owner_id: str,
    ) -> str:
        """Keep an immutable object reachable across later fetches and GC."""

        if not owner_id or len(owner_id) > 512:
            raise ManagedGitError("GIT_PIN_INVALID", "pin owner is invalid")
        self.assert_commit(repository, commit)
        digest = hashlib.sha256(
            f"{repository.canonical_remote}\0{owner_id}".encode("utf-8")
        ).hexdigest()
        pin_ref = f"refs/nginx-qa/pins/{digest}"
        lock_path = self.locks_root / f"{repository.mirror_storage_key}.lock"
        self._assert_internal_path(self.locks_root)
        self._assert_internal_path(lock_path)
        try:
            with ManagedFileLock(lock_path, timeout=self.lock_timeout):
                current = self._read_ref(repository, pin_ref)
                if current is not None:
                    if current != commit:
                        raise ManagedGitError(
                            "GIT_PIN_CONFLICT",
                            "immutable commit pin already names another object",
                        )
                    return pin_ref
                self.runner.run(
                    (
                        "--git-dir",
                        str(repository.mirror_path),
                        "update-ref",
                        pin_ref,
                        commit,
                        "0" * len(commit),
                    ),
                    error_code="REPOSITORY_MIRROR_INVALID",
                )
        except FileLockTimeout:
            raise ManagedGitError(
                "REPOSITORY_FETCH_LOCK_TIMEOUT",
                "timed out waiting for the repository mutation lock",
            ) from None
        return pin_ref

    def pinned_commit(
        self,
        repository: ManagedRepository,
        *,
        owner_id: str,
    ) -> str | None:
        """Read an immutable owner pin without consulting a mutable source ref."""

        if not owner_id or len(owner_id) > 512:
            raise ManagedGitError("GIT_PIN_INVALID", "pin owner is invalid")
        digest = hashlib.sha256(
            f"{repository.canonical_remote}\0{owner_id}".encode("utf-8")
        ).hexdigest()
        return self._read_ref(repository, f"refs/nginx-qa/pins/{digest}")

    def resolve_and_pin_commit(
        self,
        repository: ManagedRepository,
        ref: str,
        *,
        owner_id: str,
    ) -> str:
        """Resolve a mutable ref and create its owner pin in one Git command.

        ``git update-ref`` resolves the revision and commits the new pin inside
        the child process.  A caller death can therefore leave either no pin or
        the complete immutable pin, never an observed-but-unrecorded object ID.
        Exact restarts read an existing pin before consulting ``ref``.
        """

        if not owner_id or len(owner_id) > 512:
            raise ManagedGitError("GIT_PIN_INVALID", "pin owner is invalid")
        if not git_ref_format_valid(ref):
            raise ManagedGitError("GIT_REF_INVALID", "source ref is invalid")
        resolved_ref = (
            f"refs/remotes/origin/{ref.removeprefix('refs/heads/')}"
            if ref.startswith("refs/heads/")
            else ref
        )
        digest = hashlib.sha256(
            f"{repository.canonical_remote}\0{owner_id}".encode("utf-8")
        ).hexdigest()
        pin_ref = f"refs/nginx-qa/pins/{digest}"
        lock_path = self.locks_root / f"{repository.mirror_storage_key}.lock"
        self._assert_internal_path(self.locks_root)
        self._assert_internal_path(lock_path)
        try:
            with ManagedFileLock(lock_path, timeout=self.lock_timeout):
                current = self._read_ref(repository, pin_ref)
                if current is not None:
                    self.assert_commit(repository, current)
                    return current
                self.runner.run(
                    (
                        "--git-dir",
                        str(repository.mirror_path),
                        "update-ref",
                        pin_ref,
                        f"{resolved_ref}^{{commit}}",
                        "0" * (40 if repository.object_format == "sha1" else 64),
                    ),
                    error_code="SOURCE_COMMIT_NOT_FOUND",
                )
                pinned = self._read_ref(repository, pin_ref)
                if pinned is None:
                    raise ManagedGitError(
                        "REPOSITORY_MIRROR_INVALID",
                        "immutable source pin was not committed",
                    )
                self.assert_commit(repository, pinned)
                return pinned
        except FileLockTimeout:
            raise ManagedGitError(
                "REPOSITORY_FETCH_LOCK_TIMEOUT",
                "timed out waiting for the repository mutation lock",
            ) from None

    def ensure_local_branch_publication(
        self,
        repository: ManagedRepository,
        branch: str,
        *,
        selected_head: str,
        publication_id: str,
        assignment_id: str,
        policy: str,
        source_commit: str,
        expected_branch_head: str | None,
    ) -> str:
        """Publish a branch from a lease-bound intent, then record completion.

        Git's files ref backend commits the refs in a multi-ref transaction one
        at a time.  A process or I/O failure can therefore leave the branch
        visible without the old completion receipt.  The immutable intent is
        committed first and freezes the exact predecessor observations.  Only
        an intent-backed retry may finish a branch-only partial publication.
        """

        self.assert_branch_name(branch)
        self.assert_commit(repository, selected_head)
        self.assert_commit(repository, source_commit)
        if policy not in {
            "create",
            "resume",
            "reject_if_exists",
            "require_exact_head",
        }:
            raise ManagedGitError("BRANCH_POLICY_INVALID", "branch policy is invalid")
        if not publication_id or len(publication_id) > 512:
            raise ManagedGitError(
                "GIT_REF_INVALID", "branch publication identity is invalid"
            )
        if not assignment_id or len(assignment_id) > 200:
            raise ManagedGitError(
                "GIT_REF_INVALID", "branch publication assignment is invalid"
            )
        expected_publication_id = deterministic_branch_publication_id(
            repository.mirror_storage_key, branch, assignment_id
        )
        if publication_id != expected_publication_id:
            raise ManagedGitError(
                "GIT_REF_INVALID",
                "branch publication identity is not bound to the assignment",
            )
        publication_digest = hashlib.sha256(
            f"{repository.canonical_remote}\0{publication_id}".encode("utf-8")
        ).hexdigest()
        publication_ref = f"refs/nginx-qa/publications/{publication_digest}"
        intent_ref = f"refs/nginx-qa/intents/{publication_digest}"
        branch_ref = f"refs/heads/{branch}"
        lock_path = self.locks_root / f"{repository.mirror_storage_key}.lock"
        self._assert_internal_path(self.locks_root)
        self._assert_internal_path(lock_path)

        stable_ownership = {
            "assignment_id": assignment_id,
            "branch": branch,
            "branch_key": branch.casefold(),
            "mirror_storage_key": repository.mirror_storage_key,
            "publication_id": publication_id,
            "repository_id": repository.repository_id,
            "repository_key": repository.canonical_remote,
            "schema_version": 1,
            "selected_head": selected_head,
            "source_commit": source_commit,
        }
        requested_intent = {
            **stable_ownership,
            "expected_branch_head": expected_branch_head,
            "policy": policy,
        }

        def serialize_intent(
            observed_local_head: str | None,
            observed_remote_head: str | None,
        ) -> bytes:
            payload = {
                **requested_intent,
                "observed_local_head": observed_local_head,
                "observed_remote_head": observed_remote_head,
            }
            return json.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")

        def read_intent(object_id: str) -> Mapping[str, object]:
            raw = self._read_internal_blob(
                repository,
                object_id,
                error_code="REPOSITORY_MIRROR_INVALID",
            )

            def reject_nonfinite_constant(value: str) -> None:
                raise ValueError(f"non-finite JSON constant: {value}")

            try:
                payload = json.loads(
                    raw.decode("utf-8", errors="strict"),
                    parse_constant=reject_nonfinite_constant,
                )
                canonical = json.dumps(
                    payload,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            except (
                UnicodeDecodeError,
                UnicodeEncodeError,
                json.JSONDecodeError,
                RecursionError,
                ValueError,
            ) as exc:
                raise ManagedGitError(
                    "REPOSITORY_MIRROR_INVALID",
                    "branch publication intent is malformed",
                ) from exc
            expected_keys = set(requested_intent) | {
                "observed_local_head",
                "observed_remote_head",
            }
            if (
                raw != canonical
                or not isinstance(payload, dict)
                or set(payload) != expected_keys
            ):
                raise ManagedGitError(
                    "REPOSITORY_MIRROR_INVALID",
                    "branch publication intent is not canonical",
                )
            if any(
                payload.get(key) != value
                for key, value in stable_ownership.items()
            ):
                raise ManagedGitError(
                    "BRANCH_DIVERGED",
                    "branch publication intent disagrees with durable ownership",
                )
            stored_policy = payload.get("policy")
            stored_expected_head = payload.get("expected_branch_head")
            if stored_policy not in {
                "create",
                "resume",
                "reject_if_exists",
                "require_exact_head",
            } or (
                stored_expected_head is not None
                and not isinstance(stored_expected_head, str)
            ):
                raise ManagedGitError(
                    "REPOSITORY_MIRROR_INVALID",
                    "branch publication intent policy is invalid",
                )
            if isinstance(stored_expected_head, str):
                self.assert_commit(repository, stored_expected_head)
            for key in ("observed_local_head", "observed_remote_head"):
                observed = payload.get(key)
                if observed is not None:
                    if not isinstance(observed, str):
                        raise ManagedGitError(
                            "REPOSITORY_MIRROR_INVALID",
                            "branch publication predecessor is invalid",
                        )
                    self.assert_commit(repository, observed)
            return payload

        def validate_frozen_policy(
            local_head: str | None,
            remote_head: str | None,
            *,
            frozen_policy: str,
            frozen_expected_branch_head: str | None,
        ) -> None:
            if frozen_policy in {"create", "reject_if_exists"} and (
                local_head is not None or remote_head is not None
            ):
                raise ManagedGitError(
                    "BRANCH_ALREADY_EXISTS",
                    "branch appeared before publication intent was committed",
                )
            if local_head is not None and local_head != selected_head:
                if frozen_policy != "resume" or not self.is_ancestor(
                    repository, local_head, selected_head
                ):
                    raise ManagedGitError(
                        "BRANCH_DIVERGED",
                        "branch changed before publication intent was committed",
                    )
            if frozen_policy == "resume" and (
                (
                    remote_head is not None
                    and remote_head != selected_head
                    and not self.is_ancestor(repository, remote_head, selected_head)
                )
                or (
                    local_head is None
                    and remote_head is None
                    and selected_head != source_commit
                )
            ):
                raise ManagedGitError(
                    "BRANCH_DIVERGED",
                    "resumed branch changed before publication intent was committed",
                )
            if frozen_policy == "require_exact_head" and (
                frozen_expected_branch_head != selected_head
                or (local_head is None and remote_head is None)
                or (remote_head is not None and remote_head != selected_head)
            ):
                raise ManagedGitError(
                    "BRANCH_DIVERGED",
                    "exact branch head changed before publication intent was committed",
                )

        try:
            with ManagedFileLock(lock_path, timeout=self.lock_timeout):
                receipt = self._read_ref(repository, publication_ref)
                intent_object_id = self._read_ref(repository, intent_ref)
                heads = self.branch_heads(repository, branch)
                current = heads.get(branch_ref)
                remote = heads.get(f"refs/remotes/origin/{branch}")
                if receipt is not None:
                    if intent_object_id is not None:
                        read_intent(intent_object_id)
                    if (
                        receipt != selected_head
                        or current is None
                        or not self.is_ancestor(
                            repository, selected_head, current
                        )
                    ):
                        raise ManagedGitError(
                            "BRANCH_DIVERGED",
                            "branch publication receipt disagrees with durable state",
                    )
                    return publication_ref

                if intent_object_id is None:
                    validate_frozen_policy(
                        current,
                        remote,
                        frozen_policy=policy,
                        frozen_expected_branch_head=expected_branch_head,
                    )
                    intent_raw = serialize_intent(current, remote)
                    intent_object_id = self._write_internal_blob(
                        repository,
                        intent_raw,
                        error_code="REPOSITORY_MIRROR_INVALID",
                    )
                    self._create_internal_ref(
                        repository,
                        intent_ref,
                        intent_object_id,
                        error_code="BRANCH_DIVERGED",
                    )
                    intent = read_intent(intent_object_id)
                else:
                    intent = read_intent(intent_object_id)

                observed_local = intent.get("observed_local_head")
                observed_remote = intent.get("observed_remote_head")
                frozen_policy = intent.get("policy")
                frozen_expected_head = intent.get("expected_branch_head")
                assert observed_local is None or isinstance(observed_local, str)
                assert observed_remote is None or isinstance(observed_remote, str)
                assert isinstance(frozen_policy, str)
                assert frozen_expected_head is None or isinstance(
                    frozen_expected_head, str
                )
                validate_frozen_policy(
                    observed_local,
                    observed_remote,
                    frozen_policy=frozen_policy,
                    frozen_expected_branch_head=frozen_expected_head,
                )

                if remote != observed_remote:
                    raise ManagedGitError(
                        "BRANCH_DIVERGED",
                        "remote branch changed after publication intent",
                    )
                if current == selected_head:
                    pass
                elif current == observed_local:
                    self._update_publication_branch(
                        repository,
                        branch_ref,
                        selected_head,
                        observed_local,
                    )
                else:
                    raise ManagedGitError(
                        "BRANCH_DIVERGED",
                        "branch changed after publication intent",
                    )

                if self._read_ref(repository, branch_ref) != selected_head:
                    raise ManagedGitError(
                        "BRANCH_DIVERGED", "branch publication was not committed"
                    )
                self._create_internal_ref(
                    repository,
                    publication_ref,
                    selected_head,
                    error_code="BRANCH_DIVERGED",
                )
                if self._read_ref(repository, publication_ref) != selected_head:
                    raise ManagedGitError(
                        "REPOSITORY_MIRROR_INVALID",
                        "branch publication receipt was not committed",
                    )
        except FileLockTimeout:
            raise ManagedGitError(
                "REPOSITORY_FETCH_LOCK_TIMEOUT",
                "timed out waiting for the repository mutation lock",
            ) from None
        return publication_ref

    def reserve_local_branch(
        self,
        repository: ManagedRepository,
        branch: str,
        *,
        policy: str,
        source_commit: str,
        expected_branch_head: str | None,
        selected_head: str,
    ) -> str:
        """Atomically revalidate branch policy and reserve the local ref.

        Once the exact local ref exists at ``selected_head`` it is the durable
        proof that this assignment already crossed the ownership point.  Until
        then, current fetched refs are re-evaluated while the fetch lock is held.
        """

        if policy not in {
            "create",
            "resume",
            "reject_if_exists",
            "require_exact_head",
        }:
            raise ManagedGitError("BRANCH_POLICY_INVALID", "branch policy is invalid")
        self.assert_branch_name(branch)
        self.assert_commit(repository, source_commit)
        self.assert_commit(repository, selected_head)
        if expected_branch_head is not None:
            self.assert_commit(repository, expected_branch_head)
        ref = f"refs/heads/{branch}"
        remote_ref = f"refs/remotes/origin/{branch}"
        lock_path = self.locks_root / f"{repository.mirror_storage_key}.lock"
        self._assert_internal_path(self.locks_root)
        self._assert_internal_path(lock_path)
        try:
            with ManagedFileLock(lock_path, timeout=self.lock_timeout):
                heads = self.branch_heads(repository, branch)
                local_head = heads.get(ref)
                if local_head is not None:
                    if local_head != selected_head:
                        raise ManagedGitError(
                            "BRANCH_DIVERGED",
                            "owned local branch differs from the frozen initial head",
                        )
                    return local_head

                remote_head = heads.get(remote_ref)
                if policy in {"create", "reject_if_exists"}:
                    if remote_head is not None:
                        raise ManagedGitError(
                            "BRANCH_ALREADY_EXISTS",
                            "assigned branch appeared before ownership was reserved",
                        )
                    current_selection = source_commit
                elif policy == "resume":
                    if remote_head is None:
                        current_selection = source_commit
                    elif self.is_ancestor(repository, source_commit, remote_head):
                        current_selection = remote_head
                    else:
                        raise ManagedGitError(
                            "BRANCH_DIVERGED",
                            "fetched branch no longer descends from the pinned source",
                        )
                else:
                    if remote_head is None or remote_head != expected_branch_head:
                        raise ManagedGitError(
                            "BRANCH_DIVERGED",
                            "fetched branch no longer has the exact required head",
                        )
                    current_selection = remote_head

                if current_selection != selected_head:
                    raise ManagedGitError(
                        "BRANCH_DIVERGED",
                        "branch changed between policy observation and reservation",
                    )
                self.runner.run(
                    (
                        "--git-dir",
                        str(repository.mirror_path),
                        "update-ref",
                        ref,
                        selected_head,
                        "0" * len(selected_head),
                    ),
                    error_code="BRANCH_DIVERGED",
                )
        except FileLockTimeout:
            raise ManagedGitError(
                "REPOSITORY_FETCH_LOCK_TIMEOUT",
                "timed out waiting for the repository mutation lock",
            ) from None
        return selected_head

    def advance_local_branch(
        self,
        repository: ManagedRepository,
        branch: str,
        *,
        expected_head: str,
        new_head: str,
    ) -> str:
        """Fast-forward a service branch with compare-and-swap semantics."""

        self.assert_branch_name(branch)
        self.assert_commit(repository, expected_head)
        self.assert_commit(repository, new_head)
        ref = f"refs/heads/{branch}"
        lock_path = self.locks_root / f"{repository.mirror_storage_key}.lock"
        self._assert_internal_path(self.locks_root)
        self._assert_internal_path(lock_path)
        try:
            with ManagedFileLock(lock_path, timeout=self.lock_timeout):
                current = self.branch_heads(repository, branch).get(ref)
                if current == new_head:
                    if not self.is_ancestor(repository, expected_head, new_head):
                        raise ManagedGitError(
                            "BRANCH_DIVERGED",
                            "service-owned branch result is not a fast-forward",
                        )
                    return new_head
                if current != expected_head or not self.is_ancestor(
                    repository, expected_head, new_head
                ):
                    raise ManagedGitError(
                        "BRANCH_DIVERGED",
                        "service-owned branch cannot be advanced from the expected head",
                    )
                self.runner.run(
                    (
                        "--git-dir",
                        str(repository.mirror_path),
                        "update-ref",
                        ref,
                        new_head,
                        expected_head,
                    ),
                    error_code="BRANCH_DIVERGED",
                )
        except FileLockTimeout:
            raise ManagedGitError(
                "REPOSITORY_FETCH_LOCK_TIMEOUT",
                "timed out waiting for the repository mutation lock",
            ) from None
        return new_head

    def assert_branch_name(self, branch: str) -> None:
        if not git_ref_format_valid(branch, branch=True) or any(
            len(component.encode("utf-16-le")) // 2 > 255
            for component in branch.split("/")
        ):
            raise ManagedGitError("GIT_BRANCH_INVALID", "assigned branch is invalid")
        result = self.runner.run(
            ("check-ref-format", "--branch", branch),
            allowed_returncodes=frozenset({0, 1}),
            error_code="GIT_BRANCH_INVALID",
        )
        if result.returncode != 0:
            raise ManagedGitError("GIT_BRANCH_INVALID", "assigned branch is invalid")


def _same_path(first: Path, second: Path) -> bool:
    try:
        first_value = str(first.resolve(strict=True))
        second_value = str(second.resolve(strict=True))
    except OSError:
        return False
    return os.path.normcase(first_value) == os.path.normcase(second_value)


def _commit_matches_object_format(value: str, object_format: str) -> bool:
    expected_length = _OBJECT_FORMAT_LENGTHS.get(object_format)
    return (
        expected_length is not None
        and len(value) == expected_length
        and _COMMIT_ID.fullmatch(value) is not None
    )


def _path_resolves_to_itself(path: Path) -> bool:
    if path.is_symlink() or _path_is_junction(path):
        return False
    try:
        lexical = os.path.normcase(os.path.normpath(str(path.absolute())))
        resolved = os.path.normcase(
            os.path.normpath(str(Path(os.path.realpath(path.resolve(strict=True)))))
        )
    except OSError:
        return False
    return lexical == resolved


def _path_is_junction(path: Path) -> bool:
    checker = getattr(path, "is_junction", None)
    try:
        return bool(checker()) if checker is not None else False
    except OSError:
        return True


def _managed_git_storage_safe(git_dir: Path, *, bare: bool) -> bool:
    for directory in (git_dir / "objects", git_dir / "refs"):
        if (
            not directory.is_dir()
            or not _path_resolves_to_itself(directory)
            or _tree_contains_redirect(directory)
        ):
            return False
    for required_file in (git_dir / "HEAD", git_dir / "config"):
        if not required_file.is_file() or not _path_resolves_to_itself(required_file):
            return False
    for optional_directory in (git_dir / "logs",):
        if (
            optional_directory.exists()
            or optional_directory.is_symlink()
            or _path_is_junction(optional_directory)
        ) and (
            not optional_directory.is_dir()
            or not _path_resolves_to_itself(optional_directory)
        ):
            return False
        if optional_directory.is_dir() and _tree_contains_redirect(optional_directory):
            return False
    for optional_file in (git_dir / "packed-refs",):
        if (optional_file.exists() or optional_file.is_symlink()) and (
            not optional_file.is_file() or not _path_resolves_to_itself(optional_file)
        ):
            return False
    if (
        (git_dir / "commondir").exists()
        or (git_dir / "commondir").is_symlink()
        or (git_dir / "objects" / "info" / "alternates").exists()
        or (git_dir / "objects" / "info" / "alternates").is_symlink()
        or (bare and ((git_dir / "index").exists() or (git_dir / "index").is_symlink()))
    ):
        return False
    return True


def _tree_contains_redirect(root: Path) -> bool:
    pending = [root]
    while pending:
        current = pending.pop()
        try:
            with os.scandir(current) as entries:
                for entry in entries:
                    path = Path(entry.path)
                    if entry.is_symlink() or _path_is_junction(path):
                        return True
                    stat_result = entry.stat(follow_symlinks=False)
                    reparse_flag = getattr(stat_result, "st_file_attributes", 0) & getattr(
                        stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400
                    )
                    if reparse_flag:
                        return True
                    if entry.is_dir(follow_symlinks=False):
                        pending.append(path)
        except OSError:
            return True
    return False


def _same_local_transport(first: str, second: str) -> bool:
    def local_path(value: str) -> Path | None:
        if value.lower().startswith("file://"):
            parsed = urllib.parse.urlparse(value)
            raw = urllib.parse.unquote(parsed.path)
            if os.name == "nt" and re.match(r"^/[A-Za-z]:/", raw):
                raw = raw[1:]
            return Path(raw)
        path = Path(value)
        return path if path.is_absolute() else None

    first_path = local_path(first)
    second_path = local_path(second)
    if first_path is None or second_path is None:
        return False
    return _same_path(first_path, second_path)


def _resolve_from_nearest_existing(path: Path) -> Path:
    missing: list[str] = []
    current = path
    while not current.exists() and not current.is_symlink():
        if current.parent == current:
            break
        missing.append(current.name)
        current = current.parent
    try:
        resolved = Path(os.path.realpath(current.resolve(strict=True)))
    except OSError:
        resolved = Path(os.path.realpath(current.resolve(strict=False)))
    for component in reversed(missing):
        resolved /= component
    return resolved


__all__ = [
    "FileLockTimeout",
    "GitCommandResult",
    "GitCommandRunner",
    "ManagedFileLock",
    "ManagedGitError",
    "ManagedGitProvider",
    "ManagedRepository",
    "RepositorySpec",
    "canonical_remote_from_address",
    "deterministic_branch_publication_id",
]
