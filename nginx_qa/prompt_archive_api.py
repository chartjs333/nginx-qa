"""Bounded, authenticated HTTP reads for PAS1 prompt archives.

The adapter deliberately owns authorization and admission while the runtime
facade owns bounded locator/CAS verification.  In particular, no call to
``prompt_archive_runtime.for_project`` is made before the selected route
family has authenticated the request.
"""
from __future__ import annotations

import asyncio
from collections import deque
from copy import deepcopy
import inspect
import ipaddress
import json
from pathlib import Path
import re
import time
from typing import Any, Callable, Mapping
import urllib.parse
from uuid import uuid4

from fastapi import FastAPI, Request, Security
from fastapi.responses import JSONResponse, Response
from fastapi.security import APIKeyHeader

from .legacy_scope_control import ADMIN_TOKEN_HEADER, ROLE_TOKEN_HEADER
from .prompt_archive_storage import (
    PromptArchiveCancellationToken,
    PromptArchiveStorageError,
)


_SAFE_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,159}", re.ASCII)
_ARCHIVE_ID_RE = re.compile(r"par1-[0-9a-f]{64}", re.ASCII)
_PHONE_RE = re.compile(r"[0-9]{1,16}", re.ASCII)
_ERROR_MESSAGE = "Prompt archive is unavailable for this request."
_CONTENT_HEADERS = {
    "Content-Disposition": 'attachment; filename="prompt-archive.json"',
    "X-Content-Type-Options": "nosniff",
    "Cache-Control": "private, no-store",
}
_JSON_HEADERS = {
    "Cache-Control": "private, no-store",
    "X-Content-Type-Options": "nosniff",
}


def _record_hydration_metric(host: Any, started_ns: int, succeeded: bool) -> None:
    """Record one closed metric event without affecting the request outcome."""

    metrics = getattr(host, "prompt_archive_runtime_metrics", None)
    recorder = getattr(metrics, "record_hydration", None)
    if not callable(recorder):
        return
    try:
        recorder(
            latency_ns=max(0, time.perf_counter_ns() - started_ns),
            succeeded=succeeded,
        )
    except Exception:
        return


def _record_corruption_metric(host: Any) -> None:
    metrics = getattr(host, "prompt_archive_runtime_metrics", None)
    recorder = getattr(metrics, "record_corruption_failure", None)
    if not callable(recorder):
        return
    try:
        recorder()
    except Exception:
        return

_DEFAULT_GLOBAL_HYDRATIONS = 4
_DEFAULT_PROJECT_HYDRATIONS = 2
_DEFAULT_QUEUE_LIMIT = 16
_DEFAULT_SPOOL_BYTES = 1024 * 1024 * 1024
_DEFAULT_WALL_SECONDS = 120.0
_DEFAULT_MAX_HYDRATED_BYTES = 256 * 1024 * 1024

_HARD_GLOBAL_HYDRATIONS = 32
_HARD_PROJECT_HYDRATIONS = 8
_HARD_QUEUE_LIMIT = 128
_HARD_SPOOL_BYTES = 8 * 1024 * 1024 * 1024
_HARD_WALL_SECONDS = 600.0

_SCOPE_TOKEN = APIKeyHeader(
    name=ROLE_TOKEN_HEADER,
    scheme_name="PromptArchiveManagedScopeToken",
    description=(
        "Protected role token for the exact managed agent phone. Invalid or "
        "missing credentials use the same 404 response as an absent archive."
    ),
    auto_error=False,
)


class _ArchiveNotAuthorized(RuntimeError):
    """Internal marker which must always become the generic 404 envelope."""


class _AdmissionLease:
    def __init__(self, admission: "_HydrationAdmission", project_id: str, size: int):
        self._admission = admission
        self._project_id = project_id
        self._size = size
        self._released = False

    async def begin_work(self) -> "_HydrationWorkLease":
        if self._released:
            raise RuntimeError("Prompt archive admission lease is closed")
        transfer = asyncio.create_task(
            self._admission._begin_work(self._project_id, self._size)
        )
        try:
            await asyncio.shield(transfer)
        except asyncio.CancelledError:
            # Complete the atomic counter transfer, then release both sides;
            # cancellation between them must not strand either budget.
            await transfer
            self._released = True
            work = _HydrationWorkLease(
                self._admission,
                self._project_id,
                self._size,
            )
            await work.release()
            raise
        self._released = True
        return _HydrationWorkLease(
            self._admission,
            self._project_id,
            self._size,
        )

    async def release(self) -> None:
        if self._released:
            return
        self._released = True
        await self._admission._release(self._project_id, self._size)


class _HydrationWorkLease:
    """Capacity held by a running cooperatively-cancellable worker thread.

    Request admission and its spool reservation are transferred out promptly
    before synchronous work starts.  This separate budget remains charged
    until cancellation has been observed and the thread has exited, preventing
    disconnected requests from creating an unbounded thread-pool backlog.
    """

    def __init__(self, admission: "_HydrationAdmission", project_id: str, size: int):
        self._admission = admission
        self._project_id = project_id
        self._size = size
        self._released = False

    async def release(self) -> None:
        if self._released:
            return
        self._released = True
        await self._admission._release_work(self._project_id, self._size)


class _AccountedContentResponse(Response):
    """Hold hydrated-byte accounting through the complete ASGI send."""

    def __init__(
        self,
        content: bytes,
        *,
        work_lease: _HydrationWorkLease,
        headers: Mapping[str, str],
    ) -> None:
        super().__init__(
            content=content,
            media_type="application/json",
            headers=dict(headers),
        )
        self._work_lease = work_lease

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        try:
            await super().__call__(scope, receive, send)
        finally:
            # Includes slow-send completion, send failure, task cancellation,
            # and an ASGI disconnect after headers were committed.
            release = asyncio.create_task(self._work_lease.release())
            try:
                await asyncio.shield(release)
            except asyncio.CancelledError:
                await release
                raise


class _Waiter:
    def __init__(self, project_id: str, size: int, future: asyncio.Future[None]):
        self.project_id = project_id
        self.size = size
        self.future = future
        self.granted = False


class _HydrationAdmission:
    """Small FIFO admission controller shared by both HTTP route families."""

    def __init__(
        self,
        *,
        global_limit: int = _DEFAULT_GLOBAL_HYDRATIONS,
        project_limit: int = _DEFAULT_PROJECT_HYDRATIONS,
        queue_limit: int = _DEFAULT_QUEUE_LIMIT,
        spool_bytes: int = _DEFAULT_SPOOL_BYTES,
        wall_seconds: float = _DEFAULT_WALL_SECONDS,
    ) -> None:
        limits = (
            (global_limit, 1, _HARD_GLOBAL_HYDRATIONS),
            (project_limit, 1, _HARD_PROJECT_HYDRATIONS),
            (queue_limit, 0, _HARD_QUEUE_LIMIT),
            (spool_bytes, 1, _HARD_SPOOL_BYTES),
        )
        if any(type(value) is not int or value < minimum or value > maximum
               for value, minimum, maximum in limits):
            raise ValueError("Prompt archive admission limits are invalid")
        if project_limit > global_limit:
            raise ValueError("Per-project hydration limit exceeds global limit")
        if (
            not isinstance(wall_seconds, (int, float))
            or isinstance(wall_seconds, bool)
            or not 0 < float(wall_seconds) <= _HARD_WALL_SECONDS
        ):
            raise ValueError("Prompt archive wall-time limit is invalid")
        self.global_limit = global_limit
        self.project_limit = project_limit
        self.queue_limit = queue_limit
        self.spool_bytes = spool_bytes
        self.wall_seconds = float(wall_seconds)
        self._lock = asyncio.Lock()
        self._active_global = 0
        self._active_projects: dict[str, int] = {}
        self._reserved_bytes = 0
        self._working_global = 0
        self._working_projects: dict[str, int] = {}
        self._working_bytes = 0
        self._waiters: deque[_Waiter] = deque()

    def _can_grant(self, project_id: str, size: int) -> bool:
        return (
            self._active_global + self._working_global < self.global_limit
            and (
                self._active_projects.get(project_id, 0)
                + self._working_projects.get(project_id, 0)
                < self.project_limit
            )
            and self._reserved_bytes + self._working_bytes + size <= self.spool_bytes
        )

    def _grant(self, waiter: _Waiter) -> None:
        self._active_global += 1
        self._active_projects[waiter.project_id] = (
            self._active_projects.get(waiter.project_id, 0) + 1
        )
        self._reserved_bytes += waiter.size
        waiter.granted = True
        if not waiter.future.done():
            waiter.future.set_result(None)

    def _drain(self) -> None:
        # Deliberately do not skip the head: admission is FIFO, not best effort.
        while self._waiters:
            waiter = self._waiters[0]
            if waiter.future.cancelled():
                self._waiters.popleft()
                continue
            if not self._can_grant(waiter.project_id, waiter.size):
                break
            self._waiters.popleft()
            self._grant(waiter)

    async def acquire(
        self,
        project_id: str,
        size: int,
        *,
        wait_seconds: float | None = None,
    ) -> _AdmissionLease:
        if type(size) is not int or size < 0:
            raise PromptArchiveStorageError("PROMPT_ARCHIVE_REFERENCE_INVALID")
        if size > self.spool_bytes:
            raise PromptArchiveStorageError("PROMPT_ARCHIVE_BUSY")
        if wait_seconds is not None and (
            not isinstance(wait_seconds, (int, float))
            or isinstance(wait_seconds, bool)
            or not 0 < float(wait_seconds) <= _HARD_WALL_SECONDS
        ):
            raise PromptArchiveStorageError("PROMPT_ARCHIVE_REQUEST_INVALID")
        loop = asyncio.get_running_loop()
        waiter = _Waiter(project_id, size, loop.create_future())
        async with self._lock:
            if not self._waiters and self._can_grant(project_id, size):
                self._grant(waiter)
            else:
                if len(self._waiters) >= self.queue_limit:
                    raise PromptArchiveStorageError("PROMPT_ARCHIVE_BUSY")
                self._waiters.append(waiter)
        try:
            if wait_seconds is None:
                await waiter.future
            else:
                await asyncio.wait_for(
                    asyncio.shield(waiter.future),
                    timeout=float(wait_seconds),
                )
        except asyncio.TimeoutError:
            await self._abandon_waiter(waiter)
            raise PromptArchiveStorageError("PROMPT_ARCHIVE_BUSY") from None
        except BaseException:
            await self._abandon_waiter(waiter)
            raise
        return _AdmissionLease(self, project_id, size)

    async def _abandon_waiter(self, waiter: _Waiter) -> None:
        async with self._lock:
            if waiter.granted:
                self._release_locked(waiter.project_id, waiter.size)
            else:
                try:
                    self._waiters.remove(waiter)
                except ValueError:
                    pass
                if not waiter.future.done():
                    waiter.future.cancel()
            self._drain()

    def _release_locked(self, project_id: str, size: int) -> None:
        self._active_global -= 1
        remaining = self._active_projects.get(project_id, 0) - 1
        if remaining:
            self._active_projects[project_id] = remaining
        else:
            self._active_projects.pop(project_id, None)
        self._reserved_bytes -= size

    async def _release(self, project_id: str, size: int) -> None:
        async with self._lock:
            self._release_locked(project_id, size)
            self._drain()

    async def _begin_work(self, project_id: str, size: int) -> None:
        async with self._lock:
            self._release_locked(project_id, size)
            self._working_global += 1
            self._working_projects[project_id] = (
                self._working_projects.get(project_id, 0) + 1
            )
            self._working_bytes += size
            # Capacity is unchanged by the transfer, so no waiter can be
            # admitted here. Draining occurs only when work actually exits.

    async def _release_work(self, project_id: str, size: int) -> None:
        async with self._lock:
            self._working_global -= 1
            remaining = self._working_projects.get(project_id, 0) - 1
            if remaining:
                self._working_projects[project_id] = remaining
            else:
                self._working_projects.pop(project_id, None)
            self._working_bytes -= size
            self._drain()


def _admission_from_host(host: Any) -> _HydrationAdmission:
    raw = getattr(host, "prompt_archive_hydration_limits", None)
    if raw is None:
        raw = {}
    if not isinstance(raw, Mapping):
        raise ValueError("Prompt archive hydration limits must be a mapping")
    allowed = {
        "global_limit",
        "project_limit",
        "queue_limit",
        "spool_bytes",
        "wall_seconds",
    }
    if not set(raw).issubset(allowed):
        raise ValueError("Unknown prompt archive hydration limit")
    return _HydrationAdmission(**dict(raw))


def _safe_id(value: str) -> bool:
    return type(value) is str and _SAFE_ID_RE.fullmatch(value) is not None


def _correlation_id() -> str:
    return f"prompt-archive-request-{uuid4().hex}"


def _error_response(
    code: str,
    correlation_id: str,
    *,
    forced_status: int | None = None,
) -> JSONResponse:
    error = PromptArchiveStorageError(code)
    headers = {
        "Cache-Control": "private, no-store",
        "X-Content-Type-Options": "nosniff",
        "X-Correlation-ID": correlation_id,
    }
    if error.code == "PROMPT_ARCHIVE_BUSY":
        headers["Retry-After"] = "1"
    return JSONResponse(
        status_code=forced_status or error.http_status,
        content={
            "detail": {
                "schema_version": 1,
                "error": error.code,
                "message": _ERROR_MESSAGE,
                "correlation_id": correlation_id,
                "retryable": error.retryable,
            }
        },
        headers=headers,
    )


def _response_headers(correlation_id: str, *, content: bool = False) -> dict[str, str]:
    headers = dict(_CONTENT_HEADERS if content else _JSON_HEADERS)
    headers["X-Correlation-ID"] = correlation_id
    return headers


def _fallback_request_is_local(request: Request) -> bool:
    if request.headers.get("cf-connecting-ip") or request.headers.get("x-forwarded-for"):
        return False
    host = str(request.headers.get("host") or "").strip()
    try:
        hostname = str(urllib.parse.urlsplit(f"//{host}").hostname or "").casefold()
    except ValueError:
        return False
    if hostname not in {"localhost", "127.0.0.1", "::1", "testserver"}:
        return False
    forwarded_host = str(request.headers.get("x-forwarded-host") or "").split(
        ",", 1
    )[0].strip()
    if forwarded_host:
        try:
            forwarded_hostname = str(
                urllib.parse.urlsplit(f"//{forwarded_host}").hostname or ""
            ).casefold()
        except ValueError:
            return False
        if forwarded_hostname not in {"localhost", "127.0.0.1", "::1", "testserver"}:
            return False
    if request.client is None:
        return False
    try:
        client = ipaddress.ip_address(str(request.client.host).split("%", 1)[0])
    except ValueError:
        return False
    return client.is_loopback


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


async def _wait_for_disconnect(request: Request) -> None:
    # These GET routes never consume a request body, so one dedicated receiver
    # can reliably observe disconnect instead of polling Starlette's
    # cancellation-based nonblocking probe (whose behavior depends on the ASGI
    # receive implementation).
    while True:
        message = await request.receive()
        if message.get("type") == "http.disconnect":
            return


async def _acquire_for_request(
    admission: _HydrationAdmission,
    request: Request,
    project_id: str,
    size: int,
) -> _AdmissionLease:
    acquire = asyncio.create_task(
        admission.acquire(
            project_id,
            size,
            wait_seconds=admission.wall_seconds,
        )
    )
    disconnected = asyncio.create_task(_wait_for_disconnect(request))
    try:
        done, _pending = await asyncio.wait(
            {acquire, disconnected},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if disconnected in done:
            if acquire.done() and not acquire.cancelled():
                try:
                    lease = acquire.result()
                except BaseException:
                    pass
                else:
                    await lease.release()
            else:
                acquire.cancel()
                await asyncio.gather(acquire, return_exceptions=True)
            raise asyncio.CancelledError
        return acquire.result()
    except BaseException:
        if not acquire.done():
            acquire.cancel()
            await asyncio.gather(acquire, return_exceptions=True)
        raise
    finally:
        disconnected.cancel()
        await asyncio.gather(disconnected, return_exceptions=True)


async def _await_hydration(
    request: Request,
    hydration: asyncio.Task[bytes],
    *,
    wall_seconds: float,
) -> bytes:
    disconnected = asyncio.create_task(_wait_for_disconnect(request))
    try:
        done, _pending = await asyncio.wait(
            {hydration, disconnected},
            timeout=wall_seconds,
            return_when=asyncio.FIRST_COMPLETED,
        )
        if disconnected in done:
            raise asyncio.CancelledError
        if hydration in done:
            return hydration.result()
        raise asyncio.TimeoutError
    finally:
        disconnected.cancel()
        await asyncio.gather(disconnected, return_exceptions=True)


async def _authorize_operator(host: Any, request: Request) -> None:
    local_check = getattr(host, "pending_sprints_request_is_local", None)
    if local_check is None:
        local_check = _fallback_request_is_local
    try:
        if not bool(await _maybe_await(local_check(request))):
            raise _ArchiveNotAuthorized
        operator = getattr(host, "scope_api_operator", None)
        if not callable(operator) or not await _maybe_await(operator(request)):
            raise _ArchiveNotAuthorized
    except BaseException as exc:
        if isinstance(exc, (KeyboardInterrupt, SystemExit, asyncio.CancelledError)):
            raise
        raise _ArchiveNotAuthorized from None


async def _authorize_managed(
    host: Any,
    *,
    project_id: str,
    sprint_id: str,
    agent_phone: str,
    scope_token: str | None,
) -> Mapping[str, Any]:
    continuity = getattr(host, "managed_continuity_runtime", None)
    if continuity is None:
        resolver = getattr(host, "managed_continuity_runtime_for_requests", None)
        if callable(resolver):
            try:
                continuity = await _maybe_await(resolver())
            except BaseException as exc:
                if isinstance(exc, (KeyboardInterrupt, SystemExit, asyncio.CancelledError)):
                    raise
                raise _ArchiveNotAuthorized from None
    authorize = getattr(continuity, "authorize_prompt_archive_read", None)
    if not callable(authorize):
        raise _ArchiveNotAuthorized
    try:
        snapshot = await asyncio.to_thread(
            authorize,
            project_id,
            sprint_id,
            agent_phone,
            scope_token or "",
        )
    except BaseException as exc:
        if isinstance(exc, (KeyboardInterrupt, SystemExit, asyncio.CancelledError)):
            raise
        raise _ArchiveNotAuthorized from None
    if not isinstance(snapshot, Mapping) or (
        str(snapshot.get("project_id") or "") != project_id
        or str(snapshot.get("sprint_id") or "") != sprint_id
        or str(snapshot.get("agent_phone") or "") != agent_phone
        or not isinstance(snapshot.get("authorization"), Mapping)
    ):
        raise _ArchiveNotAuthorized
    return snapshot


def _validate_request_identity(
    project_id: str,
    sprint_id: str,
    archive_id: str,
    agent_phone: str | None,
) -> None:
    if not _safe_id(project_id) or not _safe_id(sprint_id):
        raise PromptArchiveStorageError("PROMPT_ARCHIVE_REQUEST_INVALID")
    if _ARCHIVE_ID_RE.fullmatch(archive_id) is None:
        raise PromptArchiveStorageError("PROMPT_ARCHIVE_REQUEST_INVALID")
    if agent_phone is not None and _PHONE_RE.fullmatch(agent_phone) is None:
        raise PromptArchiveStorageError("PROMPT_ARCHIVE_REQUEST_INVALID")


def _validated_read(
    host: Any,
    project_id: str,
    sprint_id: str,
    archive_id: str,
    cancellation: PromptArchiveCancellationToken | None = None,
) -> tuple[Any, Any]:
    registry = getattr(host, "prompt_archive_runtime", None)
    factory = getattr(registry, "for_project", None)
    if not callable(factory):
        raise PromptArchiveStorageError("PROMPT_ARCHIVE_UNAVAILABLE")
    runtime = factory(project_id)
    if runtime is None or str(getattr(runtime, "project_id", project_id)) != project_id:
        raise PromptArchiveStorageError("PROMPT_ARCHIVE_NOT_FOUND")
    metadata_lookup = getattr(runtime, "lookup_archive_metadata", None)
    if not callable(metadata_lookup):
        raise PromptArchiveStorageError("PROMPT_ARCHIVE_UNAVAILABLE")
    if cancellation is None:
        read = metadata_lookup(sprint_id=sprint_id, archive_id=archive_id)
    else:
        read = metadata_lookup(
            sprint_id=sprint_id,
            archive_id=archive_id,
            cancellation=cancellation,
        )
    scope = getattr(read, "scope", None)
    if not isinstance(scope, Mapping) or (
        str(scope.get("project_id") or "") != project_id
        or str(scope.get("sprint_id") or "") != sprint_id
        or str(getattr(read, "archive_id", "")) != archive_id
    ):
        raise PromptArchiveStorageError("PROMPT_ARCHIVE_NOT_FOUND")
    if (
        not isinstance(getattr(read, "integrity", None), Mapping)
        or not isinstance(getattr(read, "archive_format", None), str)
        or not isinstance(getattr(read, "created_at", None), str)
    ):
        raise PromptArchiveStorageError("PROMPT_ARCHIVE_REFERENCE_INVALID")
    return runtime, read


def _hydration_reservation(host: Any) -> int:
    registry = getattr(host, "prompt_archive_runtime", None)
    config = getattr(registry, "config", None)
    maximum = getattr(config, "max_hydrated_bytes", _DEFAULT_MAX_HYDRATED_BYTES)
    if (
        type(maximum) is not int
        or maximum <= 0
        or maximum > 512 * 1024 * 1024
    ):
        raise PromptArchiveStorageError("PROMPT_ARCHIVE_UNAVAILABLE")
    return maximum


def _hydrate_verified_content(
    host: Any,
    project_id: str,
    sprint_id: str,
    archive_id: str,
    reservation: int,
    cancellation: PromptArchiveCancellationToken,
) -> bytes:
    cancellation.check()
    runtime, read = _validated_read(
        host,
        project_id,
        sprint_id,
        archive_id,
        cancellation,
    )
    cancellation.check()
    integrity = getattr(read, "integrity", None)
    expected_size = (
        integrity.get("hydrated_size") if isinstance(integrity, Mapping) else None
    )
    if type(expected_size) is not int or expected_size < 0:
        raise PromptArchiveStorageError("PROMPT_ARCHIVE_REFERENCE_INVALID")
    if expected_size > reservation:
        raise PromptArchiveStorageError("PROMPT_ARCHIVE_LIMIT_EXCEEDED")
    content = runtime.hydrate_content(read, cancellation=cancellation)
    if not isinstance(content, bytes):
        raise PromptArchiveStorageError("PROMPT_ARCHIVE_REFERENCE_INVALID")
    if len(content) != expected_size:
        raise PromptArchiveStorageError("PROMPT_ARCHIVE_INTEGRITY_MISMATCH")
    cancellation.check()
    return content


def _descriptor(
    read: Any,
    *,
    project_id: str,
    sprint_id: str,
    archive_id: str,
    correlation_id: str,
    agent_phone: str | None,
) -> dict[str, Any]:
    if agent_phone is None:
        route_context = "local_operator"
        base = (
            f"/api/v1/projects/{project_id}/sprints/{sprint_id}"
            f"/prompt-archives/{archive_id}"
        )
    else:
        route_context = "managed_agent"
        base = (
            f"/api/v1/projects/{project_id}/agents/{agent_phone}/sprints/"
            f"{sprint_id}/prompt-archives/{archive_id}"
        )
    return {
        "schema_version": 1,
        "kind": "prompt_archive_descriptor",
        "media_type": "application/vnd.nginx-qa.prompt-archive-descriptor+json",
        "correlation_id": correlation_id,
        "route_context": route_context,
        "agent_phone": agent_phone,
        "archive_id": archive_id,
        "archive_format": read.archive_format,
        "scope": deepcopy(dict(read.scope)),
        "integrity": deepcopy(dict(read.integrity)),
        "created_at": read.created_at,
        "descriptor_url": base,
        "manifest_url": base + "/manifest",
        "content_url": base + "/content",
    }


def _inline_schema(filename: str) -> dict[str, Any]:
    """Fallback local schema inliner for independently mounted test hosts."""

    schema_root = (Path(__file__).resolve().parent.parent / "schemas").resolve()
    cache: dict[Path, Any] = {}

    def load(path: Path) -> Any:
        resolved = path.resolve()
        try:
            resolved.relative_to(schema_root)
        except ValueError as exc:
            raise RuntimeError("OpenAPI schema reference escapes schema root") from exc
        if resolved not in cache:
            cache[resolved] = json.loads(resolved.read_text(encoding="utf-8"))
        return cache[resolved]

    def pointer(document: Any, fragment: str) -> Any:
        if not fragment:
            return document
        if not fragment.startswith("/"):
            raise RuntimeError("OpenAPI schema reference has an invalid pointer")
        current = document
        for raw in fragment[1:].split("/"):
            part = urllib.parse.unquote(raw).replace("~1", "/").replace("~0", "~")
            current = current[int(part)] if isinstance(current, list) else current[part]
        return current

    def expand(value: Any, current: Path, stack: frozenset[tuple[Path, str]]) -> Any:
        if isinstance(value, list):
            return [expand(item, current, stack) for item in value]
        if not isinstance(value, dict):
            return value
        reference = value.get("$ref")
        if isinstance(reference, str):
            file_part, _, fragment = reference.partition("#")
            if urllib.parse.urlsplit(file_part).scheme:
                raise RuntimeError("OpenAPI schema reference must be local")
            target_path = (current.parent / file_part).resolve() if file_part else current
            key = (target_path, fragment)
            if key in stack:
                raise RuntimeError("OpenAPI schema reference cycle is unsupported")
            target = expand(pointer(load(target_path), fragment), target_path, stack | {key})
            siblings = {key: item for key, item in value.items() if key != "$ref"}
            return (
                {"allOf": [target, expand(siblings, current, stack)]}
                if siblings
                else target
            )
        return {
            key: expand(item, current, stack)
            for key, item in value.items()
            if key not in {"$schema", "$id"}
        }

    root = (schema_root / filename).resolve()
    return expand(load(root), root, frozenset())


def _openapi_responses(host: Any, *, kind: str) -> dict[int, Any]:
    loader: Callable[[str], dict[str, Any]] = getattr(
        host, "_inline_openapi_schema", _inline_schema
    )
    descriptor = loader("prompt-archive-descriptor-v1.schema.json")
    manifest = loader("prompt-archive-manifest-v1.schema.json")
    error = loader("prompt-archive-error-v1.schema.json")
    success_headers: dict[str, Any] = {
        "Cache-Control": {
            "description": "Private response which must not be cached.",
            "schema": {"type": "string", "const": "private, no-store"},
        },
        "X-Content-Type-Options": {
            "description": "MIME sniffing is disabled.",
            "schema": {"type": "string", "const": "nosniff"},
        },
        "X-Correlation-ID": {
            "description": "Safe request correlation identifier.",
            "schema": {"type": "string"},
        },
    }
    if kind == "descriptor":
        schema = descriptor
        description = "Authorized sprint-scoped prompt archive descriptor."
    elif kind == "manifest":
        schema = manifest
        description = "Verified immutable prompt archive manifest."
    else:
        schema = {
            "type": "string",
            "format": "binary",
            "description": "Exact fully verified archived JSON response.",
        }
        description = "Fully verified JSON attachment; ranges are unsupported."
        success_headers.update(
            {
                "Content-Disposition": {
                    "description": "Fixed safe attachment filename.",
                    "schema": {
                        "type": "string",
                        "const": 'attachment; filename="prompt-archive.json"',
                    },
                },
            }
        )
    result: dict[int, Any] = {
        200: {
            "description": description,
            "headers": success_headers,
            "content": {"application/json": {"schema": schema}},
        }
    }
    descriptions = {
        400: "Malformed route identity, query, or unsupported Range request.",
        404: "Archive absent or unavailable to the authenticated scope.",
        409: "Committed archive version, reference, generation, or integrity failure.",
        413: "Declared or observed resource bound exceeded.",
        429: "Bounded hydration admission is full; retry after one second.",
        503: "Archive storage or bounded hydration is temporarily unavailable.",
    }
    for status_code, error_description in descriptions.items():
        headers = {
            "Cache-Control": deepcopy(success_headers["Cache-Control"]),
            "X-Content-Type-Options": deepcopy(
                success_headers["X-Content-Type-Options"]
            ),
            "X-Correlation-ID": deepcopy(success_headers["X-Correlation-ID"]),
        }
        if status_code == 429:
            headers["Retry-After"] = {
                "description": "Fixed retry delay in seconds.",
                "schema": {"type": "string", "const": "1"},
            }
        result[status_code] = {
            "description": error_description,
            "headers": headers,
            "content": {"application/json": {"schema": deepcopy(error)}},
        }
    return result


def register_prompt_archive_api(host: Any) -> None:
    """Register the frozen operator and managed PAS1 read-only routes."""

    app: FastAPI = host.app
    admission = _admission_from_host(host)
    # Exposed for bounded instrumentation and deterministic isolated tests.
    app.state.prompt_archive_hydration_admission = admission
    admission_profile = {
        "profile": "prompt-archive-http-admission-v1",
        "global_concurrent_hydrations": admission.global_limit,
        "per_project_concurrent_hydrations": admission.project_limit,
        "queued_hydration_requests": admission.queue_limit,
        "aggregate_hydration_spool_bytes": admission.spool_bytes,
        "queue_wait_wall_seconds": admission.wall_seconds,
        "hydration_wall_seconds": admission.wall_seconds,
        "max_hydrated_response_bytes": _hydration_reservation(host),
        "order": "authenticate-admit-lookup-verify-respond",
    }
    app.state.prompt_archive_hydration_profile = deepcopy(admission_profile)

    operator_base = (
        "/api/v1/projects/{project_id}/sprints/{sprint_id}"
        "/prompt-archives/{archive_id}"
    )
    managed_base = (
        "/api/v1/projects/{project_id}/agents/{agent_phone}/sprints/{sprint_id}"
        "/prompt-archives/{archive_id}"
    )
    descriptor_responses = _openapi_responses(host, kind="descriptor")
    manifest_responses = _openapi_responses(host, kind="manifest")
    content_responses = _openapi_responses(host, kind="content")

    async def authorize_and_validate(
        request: Request,
        project_id: str,
        sprint_id: str,
        archive_id: str,
        *,
        agent_phone: str | None,
        scope_token: str | None,
    ) -> None:
        if agent_phone is None:
            await _authorize_operator(host, request)
        else:
            await _authorize_managed(
                host,
                project_id=project_id,
                sprint_id=sprint_id,
                agent_phone=agent_phone,
                scope_token=scope_token,
            )
        if request.url.query or request.headers.get("range") is not None:
            raise PromptArchiveStorageError("PROMPT_ARCHIVE_REQUEST_INVALID")
        _validate_request_identity(project_id, sprint_id, archive_id, agent_phone)

    async def lookup(
        request: Request,
        project_id: str,
        sprint_id: str,
        archive_id: str,
        *,
        agent_phone: str | None,
        scope_token: str | None,
    ) -> tuple[Any, Any]:
        await authorize_and_validate(
            request,
            project_id,
            sprint_id,
            archive_id,
            agent_phone=agent_phone,
            scope_token=scope_token,
        )
        return await asyncio.to_thread(
            _validated_read, host, project_id, sprint_id, archive_id
        )

    async def dispatch(
        request: Request,
        project_id: str,
        sprint_id: str,
        archive_id: str,
        *,
        agent_phone: str | None,
        scope_token: str | None,
        kind: str,
    ) -> Response:
        correlation_id = _correlation_id()
        try:
            if kind == "content":
                # Authentication, route validation and admission all precede
                # locator, pointer, manifest, or graph access.
                await authorize_and_validate(
                    request,
                    project_id,
                    sprint_id,
                    archive_id,
                    agent_phone=agent_phone,
                    scope_token=scope_token,
                )
                reservation = _hydration_reservation(host)
                lease = await _acquire_for_request(
                    admission,
                    request,
                    project_id,
                    reservation,
                )
                work_lease = await lease.begin_work()
                cancellation = PromptArchiveCancellationToken.with_timeout(
                    admission.wall_seconds
                )
                hydration_started_ns = time.perf_counter_ns()
                hydration = asyncio.create_task(
                    asyncio.to_thread(
                        _hydrate_verified_content,
                        host,
                        project_id,
                        sprint_id,
                        archive_id,
                        reservation,
                        cancellation,
                    )
                )
                response_owns_release = False

                try:
                    content = await _await_hydration(
                        request,
                        hydration,
                        wall_seconds=admission.wall_seconds,
                    )
                    _record_hydration_metric(
                        host,
                        hydration_started_ns,
                        True,
                    )
                    # No Response exists before full graph verification and
                    # final length agreement. The response then owns the lease
                    # until its entire ASGI send completes.
                    response = _AccountedContentResponse(
                        content=content,
                        work_lease=work_lease,
                        headers=_response_headers(correlation_id, content=True),
                    )
                    response_owns_release = True
                    return response
                except asyncio.TimeoutError:
                    # Signal cooperative cancellation, then join the worker
                    # before releasing its bounded work/byte reservation.  No
                    # response headers have been constructed at this point.
                    cancellation.cancel()
                    await asyncio.gather(
                        asyncio.shield(hydration),
                        return_exceptions=True,
                    )
                    _record_hydration_metric(
                        host,
                        hydration_started_ns,
                        False,
                    )
                    raise PromptArchiveStorageError(
                        "PROMPT_ARCHIVE_UNAVAILABLE"
                    ) from None
                except asyncio.CancelledError:
                    cancellation.cancel()
                    await asyncio.gather(
                        asyncio.shield(hydration),
                        return_exceptions=True,
                    )
                    _record_hydration_metric(
                        host,
                        hydration_started_ns,
                        False,
                    )
                    raise
                except BaseException:
                    _record_hydration_metric(
                        host,
                        hydration_started_ns,
                        False,
                    )
                    raise
                finally:
                    if not response_owns_release:
                        release = asyncio.create_task(work_lease.release())
                        try:
                            await asyncio.shield(release)
                        except asyncio.CancelledError:
                            await release
                            raise

            runtime, read = await lookup(
                request,
                project_id,
                sprint_id,
                archive_id,
                agent_phone=agent_phone,
                scope_token=scope_token,
            )
            if kind == "descriptor":
                return JSONResponse(
                    _descriptor(
                        read,
                        project_id=project_id,
                        sprint_id=sprint_id,
                        archive_id=archive_id,
                        correlation_id=correlation_id,
                        agent_phone=agent_phone,
                    ),
                    headers=_response_headers(correlation_id),
                )
            if kind == "manifest":
                manifest = await asyncio.to_thread(runtime.read_manifest, read)
                if not isinstance(manifest, Mapping):
                    raise PromptArchiveStorageError("PROMPT_ARCHIVE_REFERENCE_INVALID")
                return JSONResponse(
                    deepcopy(dict(manifest)),
                    headers=_response_headers(correlation_id),
                )
            raise PromptArchiveStorageError("PROMPT_ARCHIVE_REQUEST_INVALID")
        except _ArchiveNotAuthorized:
            return _error_response("PROMPT_ARCHIVE_NOT_FOUND", correlation_id)
        except PromptArchiveStorageError as exc:
            if exc.code in {
                "PROMPT_ARCHIVE_BLOB_CORRUPT",
                "PROMPT_ARCHIVE_INTEGRITY_MISMATCH",
            }:
                _record_corruption_metric(host)
            return _error_response(exc.code, correlation_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            return _error_response("PROMPT_ARCHIVE_UNAVAILABLE", correlation_id)

    @app.get(operator_base, responses=deepcopy(descriptor_responses))
    async def operator_prompt_archive_descriptor(
        project_id: str, sprint_id: str, archive_id: str, request: Request
    ) -> Response:
        return await dispatch(
            request,
            project_id,
            sprint_id,
            archive_id,
            agent_phone=None,
            scope_token=None,
            kind="descriptor",
        )

    @app.get(operator_base + "/manifest", responses=deepcopy(manifest_responses))
    async def operator_prompt_archive_manifest(
        project_id: str, sprint_id: str, archive_id: str, request: Request
    ) -> Response:
        return await dispatch(
            request,
            project_id,
            sprint_id,
            archive_id,
            agent_phone=None,
            scope_token=None,
            kind="manifest",
        )

    @app.get(operator_base + "/content", responses=deepcopy(content_responses))
    async def operator_prompt_archive_content(
        project_id: str, sprint_id: str, archive_id: str, request: Request
    ) -> Response:
        return await dispatch(
            request,
            project_id,
            sprint_id,
            archive_id,
            agent_phone=None,
            scope_token=None,
            kind="content",
        )

    @app.get(managed_base, responses=deepcopy(descriptor_responses))
    async def managed_prompt_archive_descriptor(
        project_id: str,
        agent_phone: str,
        sprint_id: str,
        archive_id: str,
        request: Request,
        scope_token: str | None = Security(_SCOPE_TOKEN),
    ) -> Response:
        return await dispatch(
            request,
            project_id,
            sprint_id,
            archive_id,
            agent_phone=agent_phone,
            scope_token=scope_token,
            kind="descriptor",
        )

    @app.get(managed_base + "/manifest", responses=deepcopy(manifest_responses))
    async def managed_prompt_archive_manifest(
        project_id: str,
        agent_phone: str,
        sprint_id: str,
        archive_id: str,
        request: Request,
        scope_token: str | None = Security(_SCOPE_TOKEN),
    ) -> Response:
        return await dispatch(
            request,
            project_id,
            sprint_id,
            archive_id,
            agent_phone=agent_phone,
            scope_token=scope_token,
            kind="manifest",
        )

    @app.get(managed_base + "/content", responses=deepcopy(content_responses))
    async def managed_prompt_archive_content(
        project_id: str,
        agent_phone: str,
        sprint_id: str,
        archive_id: str,
        request: Request,
        scope_token: str | None = Security(_SCOPE_TOKEN),
    ) -> Response:
        return await dispatch(
            request,
            project_id,
            sprint_id,
            archive_id,
            agent_phone=agent_phone,
            scope_token=scope_token,
            kind="content",
        )

    archive_paths = {
        operator_base,
        operator_base + "/manifest",
        operator_base + "/content",
        managed_base,
        managed_base + "/manifest",
        managed_base + "/content",
    }
    previous_openapi = app.openapi

    def prompt_archive_openapi() -> dict[str, Any]:
        document = previous_openapi()
        schemes = document.setdefault("components", {}).setdefault(
            "securitySchemes", {}
        )
        schemes.update(
            {
                "PromptArchiveOperatorAdminToken": {
                    "type": "apiKey",
                    "in": "header",
                    "name": ADMIN_TOKEN_HEADER,
                    "description": "Protected local operator administrative token.",
                },
                "PromptArchiveOperatorSession": {
                    "type": "apiKey",
                    "in": "cookie",
                    "name": "nginx_qa_operator",
                    "description": "Authenticated local operator session cookie.",
                },
                "PromptArchiveOperatorCsrf": {
                    "type": "apiKey",
                    "in": "header",
                    "name": "X-Nginx-QA-CSRF",
                    "description": "CSRF token paired with the operator session.",
                },
            }
        )
        for path in archive_paths:
            operation = (document.get("paths", {}).get(path) or {}).get("get")
            if isinstance(operation, dict):
                # Every request component is parsed as a string and validated by
                # this adapter into the frozen 400/404 envelopes. FastAPI's
                # generic 422 response is therefore neither reachable nor part
                # of the V1 contract.
                responses = operation.get("responses")
                if isinstance(responses, dict):
                    responses.pop("422", None)
                if path.endswith("/content"):
                    operation["x-nginx-qa-prompt-archive-admission"] = deepcopy(
                        admission_profile
                    )
                if "/agents/{agent_phone}/" not in path:
                    operation["security"] = [
                        {"PromptArchiveOperatorAdminToken": []},
                        {
                            "PromptArchiveOperatorSession": [],
                            "PromptArchiveOperatorCsrf": [],
                        },
                    ]
        return document

    app.openapi = prompt_archive_openapi
    app.openapi_schema = None


__all__ = ["register_prompt_archive_api"]
