from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import hmac
import html
import json
import os
import random
import re
import shutil
import subprocess
import time as time_module
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from copy import deepcopy
from contextlib import contextmanager
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

import uvicorn
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse


app = FastAPI()

QUEUE_DEFINITIONS: dict[str, dict[str, str]] = {
    "work": {
        "route": "/work",
        "context": "backend",
        "context_label": "Контур данных и алгоритмов",
        "post_event": "queued_to_backend",
        "get_event": "delivered_to_backend",
        "default_sender": "Analyst",
        "default_receiver": "Backend",
        "label": "Analyst -> Backend",
    },
    "test": {
        "route": "/test",
        "context": "backend",
        "context_label": "Контур данных и алгоритмов",
        "post_event": "queued_to_analyst",
        "get_event": "delivered_to_analyst",
        "default_sender": "Backend",
        "default_receiver": "Analyst",
        "label": "Backend -> Analyst",
    },
    "work-design": {
        "route": "/work-design",
        "context": "frontend",
        "context_label": "Контур интерфейса и UX",
        "post_event": "queued_to_frontend",
        "get_event": "delivered_to_frontend",
        "default_sender": "Designer",
        "default_receiver": "Frontend",
        "label": "Designer -> Frontend",
    },
    "test-design": {
        "route": "/test-design",
        "context": "frontend",
        "context_label": "Контур интерфейса и UX",
        "post_event": "queued_to_designer",
        "get_event": "delivered_to_designer",
        "default_sender": "Frontend",
        "default_receiver": "Designer",
        "label": "Frontend -> Designer",
    },
    "worker-all": {
        "route": "/worker/all/{conversation_phone}",
        "context": "common",
        "context_label": "Общий адресный канал",
        "post_event": "queued_to_worker_all",
        "get_event": "delivered_to_worker_phone",
        "default_sender": "Owner",
        "default_receiver": "Worker",
        "label": "Phone-addressed common channel",
    },
    "tester-all": {
        "route": "/tester/all/{conversation_phone}",
        "context": "common",
        "context_label": "Адресный канал тестировщиков",
        "post_event": "queued_to_tester_all",
        "get_event": "delivered_to_tester_phone",
        "default_sender": "Owner",
        "default_receiver": "Tester",
        "label": "Phone-addressed tester channel",
    },
    "consultant-all": {
        "route": "/consultant/all/{conversation_phone}",
        "context": "consultant",
        "context_label": "Адресный канал консультантов",
        "post_event": "queued_to_consultant_all",
        "get_event": "delivered_to_consultant_phone",
        "default_sender": "Owner",
        "default_receiver": "Technical Advisor",
        "label": "Phone-addressed consultant channel",
    },
}
queues: dict[str, deque[Any]] = {name: deque() for name in QUEUE_DEFINITIONS}
locks: dict[str, asyncio.Lock] = {name: asyncio.Lock() for name in QUEUE_DEFINITIONS}
history_lock = asyncio.Lock()
git_config_lock = asyncio.Lock()
email_routes_lock = asyncio.Lock()
agents_lock = asyncio.Lock()
specialization_queues: dict[str, list[dict[str, Any]]] = {}
specialization_lock = asyncio.Lock()
screenshot_folders_lock = asyncio.Lock()
evidence_folders_lock = asyncio.Lock()
scheduled_tasks_lock = asyncio.Lock()
group_task_submission_lock = asyncio.Lock()
sprint_history_lock = asyncio.Lock()
scheduled_tasks: dict[str, dict[str, Any]] = {}
scheduled_timer_tasks: dict[str, asyncio.Task[Any]] = {}
base_dir = Path(__file__).resolve().parent
history_path = base_dir / "conversation_log.jsonl"
git_config_path = base_dir / "port_git_map.json"
PHONE_GIT_CONTEXTS_KEY = "phone_git_contexts"
PROJECTS_KEY = "projects"
group_templates_path = base_dir / "group_templates.json"
email_routes_path = base_dir / "email_routes.json"
agents_path = base_dir / "agents.json"
sprint_history_path = base_dir / "project_sprints.json"
specializations_path = base_dir / "specializations.json"
attachments_path = base_dir / "attachments"
screenshot_folders_path = base_dir / "screenshot_folders"
evidence_folders_path = base_dir / "evidence_folders"
SCREENSHOT_FOLDER_PREFIX = "screenshot_folder_"
EVIDENCE_FOLDER_PREFIX = "evidence_folder_"
FOLDER_GIT_CONTEXT_FILENAME = ".git_context.json"
DEFAULT_PROJECT_NAME = "LLM Extractor"
PROJECT_NAMES_BY_GIT_CONTEXT = {
    "github.com/chartjs333/nginx-qc-qc_symptoms-mdsgene-validator": DEFAULT_PROJECT_NAME,
}
GIT_CONTEXT_HEADER_PATTERN = re.compile(r"^\s*(?:\[)?GIT CONTEXT(?:\])?\s*:?\s*$", re.IGNORECASE)
PASS_STATUS_PATTERN = re.compile(r"STATUS:\s*PASS\b", re.IGNORECASE)
SCHEDULE_DELAY_STEP_MINUTES = 5
SCHEDULE_DELAY_MINUTES_MIN = 5
SCHEDULE_DELAY_MINUTES_MAX = 60 * 24 * 365
SCREENSHOT_EXTENSIONS = {
    ".png",
    ".jpg",
    ".jpeg",
    ".webp",
    ".gif",
    ".bmp",
    ".tif",
    ".tiff",
    ".avif",
}
MAX_SCREENSHOT_SIZE = 25 * 1024 * 1024
MAX_EVIDENCE_SIZE = 100 * 1024 * 1024
MAX_PATCH_SIZE = 5 * 1024 * 1024
PROJECT_STATE_PATCH_CACHE_MAX = 512
project_state_patch_cache: dict[tuple[str, str, str], dict[str, Any]] = {}
PROJECT_MANAGER_PHONE = "0001"
PROJECT_MANAGER_AGENT_ID = "agent-project-manager"
PROJECT_MANAGER_AGENT_NAME = "Project Manager"
PROJECT_PHONE_MIN = 9000
PROJECT_PHONE_MAX = 9999
GROUP_AGENT_PHONE_MIN = 4000
GROUP_AGENT_PHONE_MAX = 8999
IMPORTED_ACTOR_PHONE_MIN = 2000
IMPORTED_ACTOR_PHONE_MAX = 2999
MAX_ACTOR_IMPORT_BYTES = 1024 * 1024
GROUP_QUEUE_NAMES = {"worker-all", "tester-all", "consultant-all"}
AGENT_COMMUNICATION_BLOCK_START = "=== NGINX-QA: AUTOMATIC AGENT COMMUNICATION START ==="
AGENT_COMMUNICATION_BLOCK_END = "=== NGINX-QA: AUTOMATIC AGENT COMMUNICATION END ==="
AGENT_COMMUNICATION_VERSION = "5"
AGENT_HEARTBEAT_INTERVAL_SECONDS = 300
AGENT_HEARTBEAT_TTL_SECONDS = 900
PROJECT_AGENT_ASSIGNMENT_KEY = "agent_assignment"
AGENT_ASSIGNMENT_MODES = {"parallel", "sequential"}
CYCLE_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,159}$")
CYCLE_LIFECYCLE_EVENT_TYPES = {
    "ARTIFACT_CREATED",
    "GROUP_REPORT_SUBMITTED",
    "CYCLE_COMPLETED",
}
DEFAULT_AGENT_PHONES: dict[str, str] = {
    PROJECT_MANAGER_AGENT_NAME: PROJECT_MANAGER_PHONE,
    "Owner": "1000",
    "Analyst": "1001",
    "Backend": "1002",
    "Frontend": "1003",
    "Designer": "1004",
    "Tester": "1005",
    "Programmer": "1006",
    "Worker": "1099",
    "Backend Technical Advisor": "2001",
    "Frontend & UX Technical Advisor": "2002",
}
DEFAULT_AGENTS: list[dict[str, str]] = [
    {
        "id": PROJECT_MANAGER_AGENT_ID,
        "name": PROJECT_MANAGER_AGENT_NAME,
        "phone": PROJECT_MANAGER_PHONE,
        "profile": (
            "Системный менеджер проектов. По Git-адресу находит существующий "
            "проект и его агентов или регистрирует новый чистый проект."
        ),
        "status": "system",
    },
    {
        "id": "agent-analyst",
        "name": "Analyst",
        "phone": "1001",
        "profile": "Формирует backend задачи, проверяет реализацию, пишет FAIL/PASS и требования для доработки.",
    },
    {
        "id": "agent-backend",
        "name": "Backend",
        "phone": "1002",
        "profile": "Реализует серверную логику, API, очереди, хранение данных и отправляет результат на проверку аналитику.",
    },
    {
        "id": "agent-frontend",
        "name": "Frontend",
        "phone": "1003",
        "profile": "Реализует интерфейс, клиентскую логику, адаптивность и отправляет UI на проверку дизайнеру.",
    },
    {
        "id": "agent-designer",
        "name": "Designer",
        "phone": "1004",
        "profile": "Формирует UI/UX задачи, проверяет визуальное качество, сценарии пользователя и доступность.",
    },
    {
        "id": "agent-owner",
        "name": "Owner",
        "phone": "1000",
        "profile": "Управляет очередями, историей, приоритетами и ручными операциями в UI.",
    },
    {
        "id": "agent-tester",
        "name": "Tester",
        "phone": "1005",
        "profile": "Проверяет поведение приложения по сценариям, фиксирует дефекты и результаты ретеста.",
    },
    {
        "id": "agent-programmer",
        "name": "Programmer",
        "phone": "1006",
        "profile": "Общий исполнитель задач разработки, когда роль не разделена на backend или frontend.",
    },
    {
        "id": "agent-empty-template",
        "name": "Empty Agent Template",
        "phone": "3000",
        "profile": "Универсальный стартовый агент. Клонируйте для динамического создания и специализации по телефону.",
        "status": "template",
    },
]
try:
    local_timezone = ZoneInfo("Europe/Berlin")
except Exception:
    local_timezone = datetime.now().astimezone().tzinfo or timezone.utc


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_history_line_unlocked(record: dict[str, Any]) -> None:
    history_path.parent.mkdir(parents=True, exist_ok=True)
    with history_path.open("a", encoding="utf-8") as file:
        file.write(json.dumps(record, ensure_ascii=False) + "\n")
        file.flush()
        os.fsync(file.fileno())


def write_history_line(record: dict[str, Any]) -> None:
    lock_path = history_path.with_name(f"{history_path.name}.lock")
    with interprocess_file_lock(lock_path):
        _write_history_line_unlocked(record)


def local_today_iso() -> str:
    return datetime.now(local_timezone).date().isoformat()


def parse_date_boundary(date_text: str, *, end: bool) -> datetime:
    parsed_date = date.fromisoformat(date_text)
    if end:
        parsed_date += timedelta(days=1)
    return datetime.combine(parsed_date, time.min, tzinfo=local_timezone)


def parse_history_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None

    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(local_timezone)


def read_git_config_file() -> dict[str, Any]:
    if not git_config_path.exists():
        return {}

    with git_config_path.open("r", encoding="utf-8") as file:
        try:
            data = json.load(file)
        except json.JSONDecodeError:
            return {}

    return data if isinstance(data, dict) else {}


@contextmanager
def interprocess_file_lock(lock_path: Path, timeout_seconds: float = 30.0):
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+b") as lock_file:
        lock_file.seek(0, os.SEEK_END)
        if lock_file.tell() == 0:
            lock_file.write(b"\0")
            lock_file.flush()
        lock_file.seek(0)

        if os.name == "nt":
            import msvcrt

            expires_at = time_module.monotonic() + timeout_seconds
            while True:
                try:
                    msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)
                    break
                except OSError:
                    if time_module.monotonic() >= expires_at:
                        raise TimeoutError("Timed out waiting for Git config file lock")
                    time_module.sleep(0.05)
            try:
                yield
            finally:
                lock_file.seek(0)
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            expires_at = time_module.monotonic() + timeout_seconds
            while True:
                try:
                    fcntl.flock(
                        lock_file.fileno(),
                        fcntl.LOCK_EX | fcntl.LOCK_NB,
                    )
                    break
                except BlockingIOError:
                    if time_module.monotonic() >= expires_at:
                        raise TimeoutError("Timed out waiting for Git config file lock")
                    time_module.sleep(0.05)
            try:
                yield
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


@contextmanager
def git_config_file_lock(timeout_seconds: float = 30.0):
    lock_path = git_config_path.with_name(f"{git_config_path.name}.lock")
    with interprocess_file_lock(lock_path, timeout_seconds):
        yield


@contextmanager
def agents_file_lock(timeout_seconds: float = 30.0):
    lock_path = agents_path.with_name(f"{agents_path.name}.lock")
    with interprocess_file_lock(lock_path, timeout_seconds):
        yield


@contextmanager
def sprint_history_file_lock(timeout_seconds: float = 30.0):
    lock_path = sprint_history_path.with_name(f"{sprint_history_path.name}.lock")
    with interprocess_file_lock(lock_path, timeout_seconds):
        yield


def write_json_file_atomic(target_path: Path, data: Any) -> None:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = target_path.with_name(
        f".{target_path.name}.{os.getpid()}.{uuid4().hex}.tmp"
    )
    try:
        with temp_path.open("w", encoding="utf-8") as file:
            json.dump(data, file, ensure_ascii=False, indent=2)
            file.write("\n")
            file.flush()
            os.fsync(file.fileno())
        replace_deadline = time_module.monotonic() + 5.0
        while True:
            try:
                os.replace(temp_path, target_path)
                break
            except PermissionError:
                if time_module.monotonic() >= replace_deadline:
                    raise
                time_module.sleep(0.05)
    finally:
        try:
            temp_path.unlink(missing_ok=True)
        except OSError:
            pass


def write_git_config_file(data: dict[str, Any]) -> None:
    write_json_file_atomic(git_config_path, data)


def empty_sprint_history() -> dict[str, Any]:
    return {"schema_version": 1, "projects": {}}


def read_sprint_history_file() -> dict[str, Any]:
    if not sprint_history_path.exists():
        return empty_sprint_history()
    with sprint_history_path.open("r", encoding="utf-8") as file:
        try:
            data = json.load(file)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"Sprint history file is invalid JSON: {sprint_history_path}"
            ) from exc
    if not isinstance(data, dict) or not isinstance(data.get("projects"), dict):
        raise RuntimeError(
            f"Sprint history file has an invalid structure: {sprint_history_path}"
        )
    data.setdefault("schema_version", 1)
    return data


def write_sprint_history_file(data: dict[str, Any]) -> None:
    write_json_file_atomic(sprint_history_path, data)


def normalize_email_route(raw_route: Any) -> dict[str, str] | None:
    if not isinstance(raw_route, dict):
        return None

    email = str(raw_route.get("email", "")).strip()
    sender = str(raw_route.get("sender", "")).strip()
    route_id = str(raw_route.get("id", "")).strip() or str(uuid4())
    if not email or not sender:
        return None

    return {
        "id": route_id,
        "email": email,
        "sender": sender,
    }


def default_agents() -> list[dict[str, Any]]:
    return [agent.copy() for agent in DEFAULT_AGENTS]


def project_manager_agent() -> dict[str, Any]:
    return {
        "id": PROJECT_MANAGER_AGENT_ID,
        "name": PROJECT_MANAGER_AGENT_NAME,
        "phone": PROJECT_MANAGER_PHONE,
        "profile": (
            "Системный менеджер проектов. По Git-адресу находит существующий "
            "проект и его агентов или регистрирует новый чистый проект."
        ),
        "parameters": {},
        "template_source": "system:project_manager",
        "status": "system",
    }


def normalize_agent_parameters(raw_parameters: Any) -> dict[str, str]:
    if not isinstance(raw_parameters, dict):
        return {}

    parameters: dict[str, str] = {}
    for key, value in raw_parameters.items():
        parameter_key = str(key).strip()
        if not parameter_key or parameter_key == "status":
            continue
        parameters[parameter_key] = str(value).strip()

    return parameters


def normalize_agent_status(raw_agent: dict[str, Any]) -> str:
    raw_status = raw_agent.get("status")
    if not raw_status and isinstance(raw_agent.get("parameters"), dict):
        raw_status = raw_agent["parameters"].get("status")
    return str(raw_status or "").strip()


def normalize_stored_actor_tasks(raw_tasks: Any) -> list[dict[str, Any]]:
    if not isinstance(raw_tasks, list):
        return []

    tasks: list[dict[str, Any]] = []
    for index, raw_task in enumerate(raw_tasks, start=1):
        if isinstance(raw_task, str):
            message = raw_task.strip()
            task: dict[str, Any] = {}
        elif isinstance(raw_task, dict):
            message = str(raw_task.get("message") or raw_task.get("task") or "").strip()
            task = {
                key: deepcopy(value)
                for key, value in raw_task.items()
                if key not in {"message", "task"}
            }
        else:
            continue
        if not message:
            continue
        queue_name = str(task.get("queue") or "worker-all").strip()
        if queue_name not in GROUP_QUEUE_NAMES:
            queue_name = "worker-all"
        task["message"] = message
        task["queue"] = queue_name
        task["task_id"] = str(
            task.get("task_id") or task.get("id") or f"task-{index}"
        ).strip()
        task.pop("id", None)
        tasks.append(task)
    return tasks


def normalize_agent(raw_agent: Any) -> dict[str, Any] | None:
    if not isinstance(raw_agent, dict):
        return None

    name = str(raw_agent.get("name", "")).strip()
    if not name:
        return None

    profile = str(raw_agent.get("profile", "")).strip()
    agent_id = str(raw_agent.get("id", "")).strip() or str(uuid4())
    phone = str(raw_agent.get("phone", "")).strip() or DEFAULT_AGENT_PHONES.get(name, "")
    template_source = str(raw_agent.get("template_source", "")).strip()
    parameters = normalize_agent_parameters(raw_agent.get("parameters"))
    git_branch = str(
        raw_agent.get("git_branch") or parameters.get("git_branch") or ""
    ).strip()
    if git_branch:
        parameters["git_branch"] = git_branch
    agent = {
        "id": agent_id,
        "name": name,
        "phone": phone,
        "profile": profile,
        "parameters": parameters,
        "template_source": template_source,
    }
    if git_branch:
        agent["git_branch"] = git_branch
    agent_status = normalize_agent_status(raw_agent)
    if agent_status:
        agent["status"] = agent_status
    if "tasks" in raw_agent:
        agent["tasks"] = normalize_stored_actor_tasks(raw_agent.get("tasks"))
    return agent


def normalize_agents(raw_agents: Any) -> list[dict[str, Any]]:
    if not isinstance(raw_agents, list):
        return [project_manager_agent()]

    agents: list[dict[str, Any]] = [project_manager_agent()]
    seen_names: set[str] = {PROJECT_MANAGER_AGENT_NAME.casefold()}
    for raw_agent in raw_agents:
        agent = normalize_agent(raw_agent)
        if agent is None:
            continue

        if (
            agent["id"] == PROJECT_MANAGER_AGENT_ID
            or agent["phone"] == PROJECT_MANAGER_PHONE
            or agent["name"].casefold() == PROJECT_MANAGER_AGENT_NAME.casefold()
        ):
            continue

        name_key = agent["name"].casefold()
        if name_key in seen_names:
            continue

        seen_names.add(name_key)
        agents.append(agent)

    return agents


def validate_project_manager_reservation(raw_agents: Any) -> None:
    if not isinstance(raw_agents, list):
        return

    for raw_agent in raw_agents:
        agent = normalize_agent(raw_agent)
        if agent is None:
            continue
        if agent["phone"] == PROJECT_MANAGER_PHONE and agent["id"] != PROJECT_MANAGER_AGENT_ID:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Phone {PROJECT_MANAGER_PHONE} is reserved for Project Manager",
            )
        if agent["id"] == PROJECT_MANAGER_AGENT_ID and agent["phone"] != PROJECT_MANAGER_PHONE:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    f"Project Manager must keep reserved phone {PROJECT_MANAGER_PHONE}"
                ),
            )


def read_agents_file() -> list[dict[str, Any]]:
    if not agents_path.exists():
        return normalize_agents(default_agents())

    with agents_path.open("r", encoding="utf-8") as file:
        try:
            data = json.load(file)
        except json.JSONDecodeError:
            return normalize_agents(default_agents())

    if isinstance(data, dict):
        raw_agents = data.get("agents", [])
    else:
        raw_agents = data

    if not isinstance(raw_agents, list):
        return normalize_agents(default_agents())

    return normalize_agents(raw_agents)


def write_agents_file_unlocked(agents: list[dict[str, Any]]) -> None:
    write_json_file_atomic(agents_path, {"agents": agents})


def write_agents_file(agents: list[dict[str, Any]]) -> None:
    with agents_file_lock():
        write_agents_file_unlocked(agents)


def normalize_specialization(raw_spec: Any) -> dict[str, Any] | None:
    if not isinstance(raw_spec, dict):
        return None

    name = str(raw_spec.get("name", "")).strip()
    profile = str(raw_spec.get("profile", "")).strip()
    if not name or not profile:
        return None

    return {
        "name": name,
        "profile": profile,
        "parameters": normalize_agent_parameters(raw_spec.get("parameters")),
        "status": "active",
    }


def read_specialization_queues_file() -> dict[str, list[dict[str, Any]]]:
    if not specializations_path.exists():
        return {}

    with specializations_path.open("r", encoding="utf-8") as file:
        try:
            data = json.load(file)
        except json.JSONDecodeError:
            return {}

    raw_queues = data.get("queues", data) if isinstance(data, dict) else {}
    if not isinstance(raw_queues, dict):
        return {}

    queues_data: dict[str, list[dict[str, Any]]] = {}
    for phone, raw_specs in raw_queues.items():
        phone_key = str(phone).strip()
        if not phone_key:
            continue
        if isinstance(raw_specs, dict):
            raw_specs = [raw_specs]
        if not isinstance(raw_specs, list):
            continue
        specs = [
            spec
            for spec in (normalize_specialization(raw_spec) for raw_spec in raw_specs)
            if spec is not None
        ]
        if specs:
            queues_data[phone_key] = specs

    return queues_data


def write_specialization_queues_file(queues_data: dict[str, list[dict[str, Any]]]) -> None:
    specializations_path.parent.mkdir(parents=True, exist_ok=True)
    clean_queues: dict[str, list[dict[str, Any]]] = {}
    for phone, specs in queues_data.items():
        phone_key = str(phone).strip()
        if not phone_key:
            continue
        clean_specs = [
            spec
            for spec in (normalize_specialization(raw_spec) for raw_spec in specs)
            if spec is not None
        ]
        if clean_specs:
            clean_queues[phone_key] = clean_specs

    with specializations_path.open("w", encoding="utf-8") as file:
        json.dump({"queues": clean_queues}, file, ensure_ascii=False, indent=2)
        file.write("\n")


def safe_attachment_filename(filename: str) -> str:
    clean_name = Path(str(filename or "attachment")).name.strip()
    if not clean_name:
        clean_name = "attachment"
    clean_name = re.sub(r"[^A-Za-z0-9А-Яа-яЁё._ -]+", "_", clean_name)
    clean_name = re.sub(r"\s+", "_", clean_name).strip("._- ")
    return clean_name or "attachment"


def save_attachment_file(payload: dict[str, Any]) -> dict[str, Any]:
    filename = safe_attachment_filename(str(payload.get("filename", "")))
    description = str(payload.get("description", "")).strip()
    content_base64 = str(payload.get("content_base64", "")).strip()
    if not content_base64:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Attachment content is empty.",
        )

    if "," in content_base64 and content_base64.split(",", 1)[0].startswith("data:"):
        content_base64 = content_base64.split(",", 1)[1]

    try:
        content = base64.b64decode(content_base64, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Attachment content is not valid base64.",
        ) from exc

    if not content:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Attachment file is empty.",
        )
    if len(content) > 25 * 1024 * 1024:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail="Attachment is larger than 25 MB.",
        )

    timestamp = datetime.now(local_timezone).strftime("%Y%m%d_%H%M%S")
    target_dir = attachments_path / datetime.now(local_timezone).strftime("%Y-%m-%d")
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / f"{timestamp}_{uuid4().hex[:8]}_{filename}"
    target_path.write_bytes(content)

    return {
        "filename": filename,
        "description": description,
        "path": str(target_path.resolve()),
        "size": len(content),
        "saved_at": utc_now(),
    }


def screenshot_folder_number(folder_id: str) -> int | None:
    match = re.fullmatch(rf"{re.escape(SCREENSHOT_FOLDER_PREFIX)}(\d+)", str(folder_id or ""))
    return int(match.group(1)) if match else None


def ensure_screenshot_folders_root() -> Path:
    screenshot_folders_path.mkdir(parents=True, exist_ok=True)
    return screenshot_folders_path.resolve()


def folder_git_context_path(folder_path: Path) -> Path:
    return folder_path / FOLDER_GIT_CONTEXT_FILENAME


def read_folder_git_context(folder_path: Path) -> dict[str, Any]:
    metadata_path = folder_git_context_path(folder_path)
    if not metadata_path.exists():
        return {}

    try:
        data = json.loads(metadata_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}

    return data if isinstance(data, dict) else {}


def write_folder_git_context(folder_path: Path, git_context: dict[str, Any] | None) -> None:
    if not git_context:
        return

    metadata = {
        key: git_context.get(key)
        for key in (
            "queue_phone",
            "git_context_phone",
            "project_name",
            "git_context_key",
            "git_address",
            "git_commit",
            "git_commit_short",
            "git_error",
        )
        if git_context.get(key) is not None
    }
    if not metadata:
        return

    metadata["updated_at"] = utc_now()
    folder_git_context_path(folder_path).write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def folder_matches_git_context(folder_path: Path, git_context_key: str | None) -> bool:
    if not git_context_key:
        return True

    metadata = read_folder_git_context(folder_path)
    return str(metadata.get("git_context_key") or "").strip() == git_context_key


def screenshot_file_snapshot(file_path: Path) -> dict[str, Any]:
    stat = file_path.stat()
    return {
        "name": file_path.name,
        "path": str(file_path.resolve()),
        "size": stat.st_size,
        "modified_at": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(),
    }


def screenshot_folder_snapshot(folder_path: Path) -> dict[str, Any]:
    stat = folder_path.stat()
    folder_number = screenshot_folder_number(folder_path.name)
    files = [
        screenshot_file_snapshot(file_path)
        for file_path in sorted(folder_path.iterdir(), key=lambda item: item.name.lower())
        if file_path.is_file() and file_path.name != FOLDER_GIT_CONTEXT_FILENAME
    ]
    git_context = read_folder_git_context(folder_path)
    return {
        "id": folder_path.name,
        "number": folder_number,
        "path": str(folder_path.resolve()),
        "created_at": datetime.fromtimestamp(stat.st_ctime, timezone.utc).isoformat(),
        "file_count": len(files),
        "files": files,
        "metadata": git_context,
    }


def screenshot_folder_created_local(folder_path: Path) -> datetime:
    return datetime.fromtimestamp(folder_path.stat().st_ctime, timezone.utc).astimezone(local_timezone)


def screenshot_folder_in_date_range(
    folder_path: Path,
    date_from: str | None,
    date_to: str | None,
) -> bool:
    created_at = screenshot_folder_created_local(folder_path)
    if date_from and created_at < parse_date_boundary(date_from, end=False):
        return False
    if date_to and created_at >= parse_date_boundary(date_to, end=True):
        return False
    return True


def list_screenshot_folders_file(
    date_from: str | None = None,
    date_to: str | None = None,
    git_context_key: str | None = None,
) -> dict[str, Any]:
    root = ensure_screenshot_folders_root()
    folders = [
        path
        for path in root.iterdir()
        if (
            path.is_dir()
            and screenshot_folder_number(path.name) is not None
            and screenshot_folder_in_date_range(path, date_from, date_to)
            and folder_matches_git_context(path, git_context_key)
        )
    ]
    folders.sort(key=lambda path: screenshot_folder_number(path.name) or 0)
    return {
        "root_path": str(root),
        "date_from": date_from,
        "date_to": date_to,
        "git_context_key": git_context_key,
        "folders": [screenshot_folder_snapshot(path) for path in folders],
    }


def create_screenshot_folder_file(git_context: dict[str, Any] | None = None) -> dict[str, Any]:
    root = ensure_screenshot_folders_root()
    used_numbers = [
        screenshot_folder_number(path.name)
        for path in root.iterdir()
        if path.is_dir() and screenshot_folder_number(path.name) is not None
    ]
    next_number = (max(number for number in used_numbers if number is not None) + 1) if used_numbers else 1
    while True:
        folder_path = root / f"{SCREENSHOT_FOLDER_PREFIX}{next_number}"
        if not folder_path.exists():
            folder_path.mkdir(parents=False, exist_ok=False)
            write_folder_git_context(folder_path, git_context)
            break
        next_number += 1

    return {
        "root_path": str(root),
        "folder": screenshot_folder_snapshot(folder_path),
        "folders": list_screenshot_folders_file(
            git_context_key=str((git_context or {}).get("git_context_key") or "").strip() or None,
        )["folders"],
    }


def resolve_screenshot_folder_path(folder_id: str) -> Path:
    root = ensure_screenshot_folders_root()
    clean_folder_id = str(folder_id or "").strip()
    if screenshot_folder_number(clean_folder_id) is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid screenshot folder id.",
        )

    folder_path = (root / clean_folder_id).resolve()
    if not folder_path.is_relative_to(root):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid screenshot folder path.",
        )
    if not folder_path.exists() or not folder_path.is_dir():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Screenshot folder not found.",
        )
    return folder_path


def resolve_screenshot_file_path(folder_id: str, filename: str) -> Path:
    folder_path = resolve_screenshot_folder_path(folder_id)
    clean_filename = Path(str(filename or "")).name.strip()
    if not clean_filename:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Screenshot filename is empty.",
        )

    file_path = (folder_path / clean_filename).resolve()
    if not file_path.is_relative_to(folder_path.resolve()):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid screenshot file path.",
        )
    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Screenshot file not found.",
        )
    return file_path


def decode_screenshot_content(payload: dict[str, Any]) -> bytes:
    content_base64 = str(payload.get("content_base64", "")).strip()
    if not content_base64:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Screenshot content is empty.",
        )

    if "," in content_base64 and content_base64.split(",", 1)[0].startswith("data:"):
        content_base64 = content_base64.split(",", 1)[1]

    try:
        content = base64.b64decode(content_base64, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Screenshot content is not valid base64.",
        ) from exc

    if not content:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Screenshot file is empty.",
        )
    if len(content) > MAX_SCREENSHOT_SIZE:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail="Screenshot is larger than 25 MB.",
        )
    return content


def save_screenshot_file_to_folder(folder_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    folder_path = resolve_screenshot_folder_path(folder_id)
    filename = safe_attachment_filename(str(payload.get("filename", "")))
    extension = Path(filename).suffix.lower()
    if extension not in SCREENSHOT_EXTENSIONS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only image screenshot files are allowed.",
        )

    content = decode_screenshot_content(payload)
    timestamp = datetime.now(local_timezone).strftime("%Y%m%d_%H%M%S")
    target_path = folder_path / f"{timestamp}_{uuid4().hex[:8]}_{filename}"
    target_path.write_bytes(content)
    return {
        "status": "saved",
        "folder": screenshot_folder_snapshot(folder_path),
        "file": screenshot_file_snapshot(target_path),
    }


def delete_screenshot_file_from_folder(folder_id: str, filename: str) -> dict[str, Any]:
    file_path = resolve_screenshot_file_path(folder_id, filename)
    folder_path = file_path.parent
    file_path.unlink()
    return {
        "status": "deleted",
        "folder": screenshot_folder_snapshot(folder_path),
        "filename": filename,
    }


def unique_copied_screenshot_path(target_folder: Path, source_name: str) -> Path:
    candidate = target_folder / source_name
    if not candidate.exists():
        return candidate

    source_path = Path(source_name)
    stem = source_path.stem or "screenshot"
    suffix = source_path.suffix
    while True:
        timestamp = datetime.now(local_timezone).strftime("%Y%m%d_%H%M%S")
        candidate = target_folder / f"{stem}_{timestamp}_{uuid4().hex[:8]}{suffix}"
        if not candidate.exists():
            return candidate


def copy_screenshot_file_between_folders(
    source_folder_id: str,
    filename: str,
    target_folder_id: str,
) -> dict[str, Any]:
    source_file_path = resolve_screenshot_file_path(source_folder_id, filename)
    target_folder_path = resolve_screenshot_folder_path(target_folder_id)
    if source_file_path.parent.resolve() == target_folder_path.resolve():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Choose a different target folder.",
        )

    target_path = unique_copied_screenshot_path(target_folder_path, source_file_path.name)
    shutil.copy2(source_file_path, target_path)
    return {
        "status": "copied",
        "source_folder_id": source_folder_id,
        "target_folder_id": target_folder_id,
        "file": screenshot_file_snapshot(target_path),
        "folder": screenshot_folder_snapshot(target_folder_path),
    }


def evidence_folder_number(folder_id: str) -> int | None:
    match = re.fullmatch(rf"{re.escape(EVIDENCE_FOLDER_PREFIX)}(\d+)", str(folder_id or ""))
    return int(match.group(1)) if match else None


def ensure_evidence_folders_root() -> Path:
    evidence_folders_path.mkdir(parents=True, exist_ok=True)
    return evidence_folders_path.resolve()


def evidence_file_snapshot(file_path: Path) -> dict[str, Any]:
    stat = file_path.stat()
    extension = file_path.suffix.lower()
    return {
        "name": file_path.name,
        "path": str(file_path.resolve()),
        "size": stat.st_size,
        "extension": extension,
        "is_image": extension in SCREENSHOT_EXTENSIONS,
        "modified_at": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(),
    }


def evidence_folder_snapshot(folder_path: Path) -> dict[str, Any]:
    stat = folder_path.stat()
    folder_number = evidence_folder_number(folder_path.name)
    files = [
        evidence_file_snapshot(file_path)
        for file_path in sorted(folder_path.iterdir(), key=lambda item: item.name.lower())
        if file_path.is_file() and file_path.name != FOLDER_GIT_CONTEXT_FILENAME
    ]
    git_context = read_folder_git_context(folder_path)
    return {
        "id": folder_path.name,
        "number": folder_number,
        "path": str(folder_path.resolve()),
        "created_at": datetime.fromtimestamp(stat.st_ctime, timezone.utc).isoformat(),
        "file_count": len(files),
        "files": files,
        "metadata": git_context,
    }


def evidence_folder_created_local(folder_path: Path) -> datetime:
    return datetime.fromtimestamp(folder_path.stat().st_ctime, timezone.utc).astimezone(local_timezone)


def evidence_folder_in_date_range(
    folder_path: Path,
    date_from: str | None,
    date_to: str | None,
) -> bool:
    created_at = evidence_folder_created_local(folder_path)
    if date_from and created_at < parse_date_boundary(date_from, end=False):
        return False
    if date_to and created_at >= parse_date_boundary(date_to, end=True):
        return False
    return True


def list_evidence_folders_file(
    date_from: str | None = None,
    date_to: str | None = None,
    git_context_key: str | None = None,
) -> dict[str, Any]:
    root = ensure_evidence_folders_root()
    folders = [
        path
        for path in root.iterdir()
        if (
            path.is_dir()
            and evidence_folder_number(path.name) is not None
            and evidence_folder_in_date_range(path, date_from, date_to)
            and folder_matches_git_context(path, git_context_key)
        )
    ]
    folders.sort(key=lambda path: evidence_folder_number(path.name) or 0)
    return {
        "root_path": str(root),
        "date_from": date_from,
        "date_to": date_to,
        "git_context_key": git_context_key,
        "folders": [evidence_folder_snapshot(path) for path in folders],
    }


def create_evidence_folder_file(git_context: dict[str, Any] | None = None) -> dict[str, Any]:
    root = ensure_evidence_folders_root()
    used_numbers = [
        evidence_folder_number(path.name)
        for path in root.iterdir()
        if path.is_dir() and evidence_folder_number(path.name) is not None
    ]
    next_number = (max(number for number in used_numbers if number is not None) + 1) if used_numbers else 1
    while True:
        folder_path = root / f"{EVIDENCE_FOLDER_PREFIX}{next_number}"
        if not folder_path.exists():
            folder_path.mkdir(parents=False, exist_ok=False)
            write_folder_git_context(folder_path, git_context)
            break
        next_number += 1

    return {
        "root_path": str(root),
        "folder": evidence_folder_snapshot(folder_path),
        "folders": list_evidence_folders_file(
            git_context_key=str((git_context or {}).get("git_context_key") or "").strip() or None,
        )["folders"],
    }


def resolve_evidence_folder_path(folder_id: str) -> Path:
    root = ensure_evidence_folders_root()
    clean_folder_id = str(folder_id or "").strip()
    if evidence_folder_number(clean_folder_id) is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid evidence folder id.",
        )

    folder_path = (root / clean_folder_id).resolve()
    if not folder_path.is_relative_to(root):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid evidence folder path.",
        )
    if not folder_path.exists() or not folder_path.is_dir():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Evidence folder not found.",
        )
    return folder_path


def resolve_evidence_file_path(folder_id: str, filename: str) -> Path:
    folder_path = resolve_evidence_folder_path(folder_id)
    clean_filename = Path(str(filename or "")).name.strip()
    if not clean_filename:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Evidence filename is empty.",
        )

    file_path = (folder_path / clean_filename).resolve()
    if not file_path.is_relative_to(folder_path.resolve()):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid evidence file path.",
        )
    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Evidence file not found.",
        )
    return file_path


def decode_evidence_content(payload: dict[str, Any]) -> bytes:
    content_base64 = str(payload.get("content_base64", "")).strip()
    if not content_base64:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Evidence content is empty.",
        )

    if "," in content_base64 and content_base64.split(",", 1)[0].startswith("data:"):
        content_base64 = content_base64.split(",", 1)[1]

    try:
        content = base64.b64decode(content_base64, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Evidence content is not valid base64.",
        ) from exc

    if not content:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Evidence file is empty.",
        )
    if len(content) > MAX_EVIDENCE_SIZE:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail="Evidence file is larger than 100 MB.",
        )
    return content


def save_evidence_file_to_folder(folder_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    folder_path = resolve_evidence_folder_path(folder_id)
    filename = safe_attachment_filename(str(payload.get("filename", "")))
    content = decode_evidence_content(payload)
    timestamp = datetime.now(local_timezone).strftime("%Y%m%d_%H%M%S")
    target_path = folder_path / f"{timestamp}_{uuid4().hex[:8]}_{filename}"
    target_path.write_bytes(content)
    return {
        "status": "saved",
        "folder": evidence_folder_snapshot(folder_path),
        "file": evidence_file_snapshot(target_path),
    }


def delete_evidence_file_from_folder(folder_id: str, filename: str) -> dict[str, Any]:
    file_path = resolve_evidence_file_path(folder_id, filename)
    folder_path = file_path.parent
    file_path.unlink()
    return {
        "status": "deleted",
        "folder": evidence_folder_snapshot(folder_path),
        "filename": filename,
    }


def unique_copied_evidence_path(target_folder: Path, source_name: str) -> Path:
    candidate = target_folder / source_name
    if not candidate.exists():
        return candidate

    source_path = Path(source_name)
    stem = source_path.stem or "evidence"
    suffix = source_path.suffix
    while True:
        timestamp = datetime.now(local_timezone).strftime("%Y%m%d_%H%M%S")
        candidate = target_folder / f"{stem}_{timestamp}_{uuid4().hex[:8]}{suffix}"
        if not candidate.exists():
            return candidate


def copy_evidence_file_between_folders(
    source_folder_id: str,
    filename: str,
    target_folder_id: str,
) -> dict[str, Any]:
    source_file_path = resolve_evidence_file_path(source_folder_id, filename)
    target_folder_path = resolve_evidence_folder_path(target_folder_id)
    if source_file_path.parent.resolve() == target_folder_path.resolve():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Choose a different target folder.",
        )

    target_path = unique_copied_evidence_path(target_folder_path, source_file_path.name)
    shutil.copy2(source_file_path, target_path)
    return {
        "status": "copied",
        "source_folder_id": source_folder_id,
        "target_folder_id": target_folder_id,
        "file": evidence_file_snapshot(target_path),
        "folder": evidence_folder_snapshot(target_folder_path),
    }


def read_email_routes_file() -> list[dict[str, str]]:
    if not email_routes_path.exists():
        return []

    with email_routes_path.open("r", encoding="utf-8") as file:
        try:
            data = json.load(file)
        except json.JSONDecodeError:
            return []

    if isinstance(data, dict):
        raw_routes = data.get("routes", [])
    else:
        raw_routes = data

    if not isinstance(raw_routes, list):
        return []

    routes: list[dict[str, str]] = []
    for raw_route in raw_routes:
        route = normalize_email_route(raw_route)
        if route is not None:
            routes.append(route)

    return routes


def write_email_routes_file(routes: list[dict[str, str]]) -> None:
    email_routes_path.parent.mkdir(parents=True, exist_ok=True)
    with email_routes_path.open("w", encoding="utf-8") as file:
        json.dump({"routes": routes}, file, ensure_ascii=False, indent=2)
        file.write("\n")


async def read_git_config() -> dict[str, Any]:
    async with git_config_lock:
        return await asyncio.to_thread(read_git_config_file)


async def read_email_routes() -> list[dict[str, str]]:
    async with email_routes_lock:
        return await asyncio.to_thread(read_email_routes_file)


async def read_agents() -> list[dict[str, Any]]:
    async with agents_lock:
        return await asyncio.to_thread(read_agents_file)


def save_agents_transaction(raw_agents: list[Any]) -> list[dict[str, Any]]:
    with agents_file_lock():
        existing_agents = read_agents_file()
        incoming_agents = normalize_agents(raw_agents)
        managed_agents = [
            agent for agent in existing_agents if is_group_managed_agent(agent)
        ]
        managed_by_id = {
            str(agent.get("id") or "").strip(): agent
            for agent in managed_agents
        }
        managed_names = {
            str(agent.get("name") or "").strip().casefold(): agent
            for agent in managed_agents
        }
        managed_phones = {
            str(agent.get("phone") or "").strip(): agent
            for agent in managed_agents
        }

        retained: list[dict[str, Any]] = []
        submitted_managed_ids: set[str] = set()
        for candidate in incoming_agents:
            candidate_id = str(candidate.get("id") or "").strip()
            candidate_name = str(candidate.get("name") or "").strip().casefold()
            candidate_phone = str(candidate.get("phone") or "").strip()
            protected = (
                managed_by_id.get(candidate_id)
                or managed_names.get(candidate_name)
                or managed_phones.get(candidate_phone)
            )
            if protected is not None:
                protected_id = str(protected.get("id") or "").strip()
                if candidate_id != protected_id or candidate != protected:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "error": "group_managed_agent_is_immutable",
                            "agent_id": protected_id,
                        },
                    )
                submitted_managed_ids.add(protected_id)
                retained.append(deepcopy(protected))
                continue
            retained.append(candidate)

        for agent_id, managed_agent in managed_by_id.items():
            if agent_id not in submitted_managed_ids:
                retained.append(deepcopy(managed_agent))
        agents = normalize_agents(retained)
        missing_managed_ids = set(managed_by_id) - {
            str(agent.get("id") or "").strip() for agent in agents
        }
        if missing_managed_ids:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "error": "group_managed_agent_name_conflict",
                    "agent_ids": sorted(missing_managed_ids),
                },
            )
        write_agents_file_unlocked(agents)
        return agents


async def save_agents(raw_agents: Any) -> list[dict[str, Any]]:
    if not isinstance(raw_agents, list):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Expected agents list",
        )

    validate_project_manager_reservation(raw_agents)
    async with agents_lock:
        return await asyncio.to_thread(save_agents_transaction, raw_agents)


EMPTY_AGENT_POLL_INTERVAL = "5 минут"
EMPTY_AGENT_WAIT_WINDOW = "2 часа"
EMPTY_AGENT_PHONE_MIN = 3000
EMPTY_AGENT_PHONE_MAX = 3999

EMPTY_AGENT_PROMPT_TEMPLATE = """# Prompt: Empty Agent (Generic Startup Agent)

Ты — универсальный стартовый агент в системе QA Queue Control. На данный момент у тебя нет заданной специализации (роли).

## Твои параметры:
* Твой идентификационный телефон: ${agent_phone}
* Эндпоинт для опроса специализации: GET ${specialization_endpoint}
* Интервал опроса: ${poll_interval}
* Максимальное время ожидания специализации: ${wait_window}

## Инструкция по поведению:
1. **Опрос специализации:**
   Периодически отправляй HTTP-запросы GET ${specialization_endpoint}.
2. **Ожидание:**
   Если эндпоинт возвращает 404 или статус ожидания, сделай паузу на ${poll_interval} и повтори запрос. Ничего не делай в кодовой базе и не пытайся симулировать работу.
   Ожидай специализацию не дольше ${wait_window}. Если за это время специализация не пришла, заверши работу и сообщи владельцу, что пустой агент умер из-за таймаута ожидания.
3. **Получение роли (Специализация):**
   Как только GET запрос вернет JSON со спецификацией, выполни следующие шаги:
   * Распарси JSON. Извлеки поля name, profile и parameters.
   * **Обнови свою личность (Persona):** Прими новые инструкции из profile как свой новый системный промпт.
   * Настрой свои новые рабочие эндпоинты из поля parameters (например, опрос /work или /test).
   * Отправь подтверждение на сервер: POST ${specialization_endpoint}/confirm со статусом ACCEPTED.
   * Начни выполнять задачи согласно новой роли.
"""


def profile_template_text(profile: str, parameters: dict[str, str]) -> str:
    return re.sub(
        r"\$\s*\{\s*([A-Za-z0-9_.-]+)\s*\}",
        lambda match: str(parameters.get(match.group(1), match.group(0))),
        profile,
    )


def agent_status_value(agent: dict[str, Any]) -> str:
    status_value = str(agent.get("status", "")).strip()
    if status_value:
        return status_value
    parameters = agent.get("parameters")
    if isinstance(parameters, dict):
        return str(parameters.get("status", "")).strip()
    return ""


def unique_random_phone_for_agents(agents: list[dict[str, Any]]) -> str:
    existing_phones = {
        str(agent.get("phone", "")).strip()
        for agent in agents
        if str(agent.get("phone", "")).strip()
    }
    existing_phones.update(DEFAULT_AGENT_PHONES.values())
    candidates = list(range(EMPTY_AGENT_PHONE_MIN, EMPTY_AGENT_PHONE_MAX + 1))
    random.shuffle(candidates)
    for phone_number in candidates:
        phone = str(phone_number)
        if phone not in existing_phones:
            return phone

    raise HTTPException(
        status_code=status.HTTP_409_CONFLICT,
        detail="No free empty-agent phone numbers in range 3000-3999.",
    )


def parse_utc_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None

    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def agent_presence_snapshot(
    agent: dict[str, Any],
    now: datetime | None = None,
) -> dict[str, Any]:
    parameters = agent.get("parameters") if isinstance(agent.get("parameters"), dict) else {}
    checked_at = now or datetime.now(timezone.utc)
    last_seen_at = parse_utc_datetime(parameters.get("last_seen_at"))
    alive_until = parse_utc_datetime(parameters.get("alive_until"))
    is_alive = bool(
        str(parameters.get("presence_status") or "").strip() == "alive"
        and last_seen_at is not None
        and alive_until is not None
        and alive_until > checked_at
    )
    try:
        heartbeat_count = max(0, int(parameters.get("heartbeat_count") or 0))
    except (TypeError, ValueError):
        heartbeat_count = 0
    return {
        "status": "alive" if is_alive else ("offline" if last_seen_at else "unknown"),
        "is_alive": is_alive,
        "created_at": parameters.get("created_at"),
        "first_seen_at": parameters.get("first_seen_at"),
        "last_seen_at": parameters.get("last_seen_at"),
        "alive_until": parameters.get("alive_until"),
        "heartbeat_count": heartbeat_count,
        "heartbeat_interval_seconds": AGENT_HEARTBEAT_INTERVAL_SECONDS,
    }


def agent_with_presence(agent: dict[str, Any]) -> dict[str, Any]:
    public_agent = deepcopy(agent)
    public_agent["presence"] = agent_presence_snapshot(agent)
    return public_agent


def empty_agent_expires_at(agent: dict[str, Any]) -> datetime | None:
    parameters = agent.get("parameters") if isinstance(agent.get("parameters"), dict) else {}
    return parse_utc_datetime(parameters.get("expires_at") or agent.get("expires_at"))


def empty_agent_is_expired(agent: dict[str, Any]) -> bool:
    if agent_status_value(agent) != "empty":
        return False
    expires_at = empty_agent_expires_at(agent)
    return expires_at is not None and expires_at <= datetime.now(timezone.utc)


def extract_json_object(text: str) -> dict[str, Any]:
    cleaned = text.strip()
    fence_match = re.search(r"```(?:json)?\s*(.*?)\s*```", cleaned, re.DOTALL | re.IGNORECASE)
    if fence_match:
        cleaned = fence_match.group(1).strip()

    try:
        parsed = json.loads(cleaned)
    except json.JSONDecodeError:
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start < 0 or end <= start:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Gemini returned text without a JSON object.",
            )
        try:
            parsed = json.loads(cleaned[start : end + 1])
        except json.JSONDecodeError as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Gemini returned invalid JSON.",
            ) from exc

    if not isinstance(parsed, dict):
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Gemini returned JSON, but not an object.",
        )
    return parsed


def build_agent_clone_prompt(
    source_agent: dict[str, Any],
    refinements: str,
    existing_agent_names: list[str],
) -> str:
    source_json = json.dumps(source_agent, ensure_ascii=False, indent=2)
    names_json = json.dumps(existing_agent_names, ensure_ascii=False)
    refinements_text = refinements.strip() or "Нет дополнительных уточнений."
    return f"""Ты помогаешь клонировать профиль агента в локальной системе QA Queue Control.

Нужно создать нового агента на основе исходного агента и уточнений пользователя.

Правила:
- Верни только валидный JSON-объект без Markdown и пояснений.
- Новый агент должен быть похож на исходного по структуре, но учитывать уточнения.
- Имя нового агента не должно совпадать с существующими именами.
- Если новый телефон неизвестен, верни пустую строку в поле "phone".
- Сохрани placeholders вида ${{name}} внутри profile, если они все еще нужны.
- Поле "parameters" должно содержать все параметры, которые нужны итоговому profile.
- Не добавляй секреты, которых нет в исходном агенте или уточнениях.

Формат ответа:
{{
  "name": "New Agent Name",
  "phone": "",
  "profile": "Полный профиль нового агента",
  "parameters": {{
    "key": "value"
  }}
}}

Существующие имена агентов:
{names_json}

Исходный агент:
{source_json}

Уточнения пользователя:
{refinements_text}
"""


def gemini_response_text(data: dict[str, Any]) -> str:
    texts: list[str] = []
    candidates = data.get("candidates")
    if isinstance(candidates, list):
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            content = candidate.get("content")
            if not isinstance(content, dict):
                continue
            parts = content.get("parts")
            if not isinstance(parts, list):
                continue
            for part in parts:
                if isinstance(part, dict) and isinstance(part.get("text"), str):
                    texts.append(part["text"])

    text = "\n".join(texts).strip()
    if not text:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Gemini response does not contain text.",
        )
    return text


def gemini_error_detail(error: urllib.error.HTTPError) -> str:
    body = error.read().decode("utf-8", errors="replace")
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return body.strip() or error.reason

    if isinstance(payload, dict):
        error_payload = payload.get("error")
        if isinstance(error_payload, dict) and isinstance(error_payload.get("message"), str):
            return error_payload["message"]
    return body.strip() or error.reason


def call_gemini_clone_model(api_key: str, model: str, prompt: str) -> str:
    model_name = model.strip().removeprefix("models/") or "gemini-2.5-flash"
    query = urllib.parse.urlencode({"key": api_key})
    url = (
        "https://generativelanguage.googleapis.com/v1beta/models/"
        f"{urllib.parse.quote(model_name, safe='')}:generateContent?{query}"
    )
    payload = {
        "contents": [
            {
                "role": "user",
                "parts": [{"text": prompt}],
            }
        ],
        "generationConfig": {
            "temperature": 0.25,
            "responseMimeType": "application/json",
            "responseSchema": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "phone": {"type": "string"},
                    "profile": {"type": "string"},
                    "parameters": {
                        "type": "object",
                    },
                },
                "required": ["name", "phone", "profile", "parameters"],
                "propertyOrdering": ["name", "phone", "profile", "parameters"],
            },
        },
    }
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={"Content-Type": "application/json"},
    )

    try:
        with urllib.request.urlopen(request, timeout=90) as response:
            response_body = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Gemini API error: {gemini_error_detail(exc)}",
        ) from exc
    except urllib.error.URLError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Gemini API request failed: {exc.reason}",
        ) from exc

    try:
        data = json.loads(response_body)
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Gemini returned a non-JSON API response.",
        ) from exc
    if not isinstance(data, dict):
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Gemini returned an unexpected API response.",
        )
    return gemini_response_text(data)


def normalize_gemini_model(raw_model: Any) -> dict[str, Any] | None:
    if not isinstance(raw_model, dict):
        return None

    name = str(raw_model.get("name", "")).strip()
    if not name:
        return None

    supported_methods = raw_model.get("supportedGenerationMethods", [])
    if not isinstance(supported_methods, list):
        supported_methods = []

    short_name = name.removeprefix("models/")
    return {
        "name": name,
        "short_name": short_name,
        "display_name": str(raw_model.get("displayName", "")).strip() or short_name,
        "description": str(raw_model.get("description", "")).strip(),
        "version": str(raw_model.get("version", "")).strip(),
        "input_token_limit": raw_model.get("inputTokenLimit"),
        "output_token_limit": raw_model.get("outputTokenLimit"),
        "supported_generation_methods": [
            str(method)
            for method in supported_methods
            if isinstance(method, str) and method.strip()
        ],
    }


def call_gemini_models_api(api_key: str) -> list[dict[str, Any]]:
    models: list[dict[str, Any]] = []
    page_token = ""

    for _ in range(10):
        query_params = {"key": api_key, "pageSize": "1000"}
        if page_token:
            query_params["pageToken"] = page_token
        url = "https://generativelanguage.googleapis.com/v1beta/models?" + urllib.parse.urlencode(query_params)
        request = urllib.request.Request(url, method="GET")

        try:
            with urllib.request.urlopen(request, timeout=45) as response:
                response_body = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Gemini models API error: {gemini_error_detail(exc)}",
            ) from exc
        except urllib.error.URLError as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Gemini models API request failed: {exc.reason}",
            ) from exc

        try:
            data = json.loads(response_body)
        except json.JSONDecodeError as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Gemini models API returned a non-JSON response.",
            ) from exc

        if not isinstance(data, dict):
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Gemini models API returned an unexpected response.",
            )

        raw_models = data.get("models", [])
        if isinstance(raw_models, list):
            for raw_model in raw_models:
                model = normalize_gemini_model(raw_model)
                if model is not None:
                    models.append(model)

        page_token = str(data.get("nextPageToken", "")).strip()
        if not page_token:
            break

    return models


def build_gemini_models_response(payload: dict[str, Any]) -> dict[str, Any]:
    api_key = str(payload.get("api_key", "")).strip() or os.getenv("GEMINI_API_KEY", "").strip()
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Gemini API key is empty. Enter a key or set GEMINI_API_KEY on the server.",
        )

    models = call_gemini_models_api(api_key)
    generate_content_models = [
        model
        for model in models
        if "generateContent" in model["supported_generation_methods"]
    ]
    generate_content_models.sort(key=lambda model: model["short_name"])
    models.sort(key=lambda model: model["short_name"])
    return {
        "models": models,
        "generate_content_models": generate_content_models,
        "count": len(models),
        "generate_content_count": len(generate_content_models),
    }


def build_agent_clone_preview(payload: dict[str, Any]) -> dict[str, Any]:
    source_agent = normalize_agent(payload.get("source_agent"))
    if source_agent is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Source agent is invalid.",
        )

    api_key = str(payload.get("api_key", "")).strip() or os.getenv("GEMINI_API_KEY", "").strip()
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Gemini API key is empty. Enter a key or set GEMINI_API_KEY on the server.",
        )

    model = str(payload.get("model", "")).strip() or "gemini-2.5-flash"
    refinements = str(payload.get("refinements", "")).strip()
    existing_agent_names = [
        str(name).strip()
        for name in payload.get("existing_agent_names", [])
        if str(name).strip()
    ]
    prompt = build_agent_clone_prompt(source_agent, refinements, existing_agent_names)
    raw_response = call_gemini_clone_model(api_key, model, prompt)
    try:
        raw_clone = extract_json_object(raw_response)
    except HTTPException as exc:
        if "without a JSON object" not in str(exc.detail):
            raise
        raw_clone = {
            "name": f"{source_agent['name']} Clone",
            "phone": "",
            "profile": raw_response.strip(),
            "parameters": source_agent.get("parameters", {}),
        }

    raw_parameters = raw_clone.get("parameters")
    if not isinstance(raw_parameters, dict):
        raw_parameters = source_agent.get("parameters", {})

    candidate = normalize_agent(
        {
            "id": str(uuid4()),
            "name": str(raw_clone.get("name", "")).strip() or f"{source_agent['name']} Clone",
            "phone": str(raw_clone.get("phone", "")).strip(),
            "profile": str(raw_clone.get("profile", "")).strip() or source_agent["profile"],
            "parameters": raw_parameters,
            "template_source": f"clone:{source_agent['name']}",
        }
    )
    if candidate is None:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Gemini returned an invalid agent clone.",
        )

    return {
        "candidate": candidate,
        "raw_response": raw_response,
        "model": model,
    }


async def save_email_routes(raw_routes: Any) -> list[dict[str, str]]:
    if not isinstance(raw_routes, list):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Expected routes list",
        )

    routes: list[dict[str, str]] = []
    for raw_route in raw_routes:
        route = normalize_email_route(raw_route)
        if route is not None:
            routes.append(route)

    async with email_routes_lock:
        await asyncio.to_thread(write_email_routes_file, routes)

    return routes


def save_git_address_transaction(
    port: int,
    entry: dict[str, Any],
    phone_key: str,
) -> dict[str, Any]:
    with git_config_file_lock():
        config = read_git_config_file()
        canonical_context_key = canonical_project_context_key_for_phone(
            config,
            phone_key,
        )
        if (
            canonical_context_key
            and canonical_context_key != entry.get("git_context_key")
        ):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    f"Phone {phone_key} is reserved for project "
                    f"{canonical_context_key}"
                ),
            )
        raw_phone_map = config.get(PHONE_GIT_CONTEXTS_KEY)
        protected_mapping = (
            raw_phone_map.get(phone_key)
            if phone_key and isinstance(raw_phone_map, dict)
            else None
        )
        if (
            isinstance(protected_mapping, dict)
            and str(protected_mapping.get("managed_by") or "").strip()
            == "group_api"
        ):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "error": "group_managed_phone_is_immutable",
                    "phone": phone_key,
                    "agent_id": protected_mapping.get("agent_id"),
                },
            )
        config[str(port)] = entry
        if phone_key:
            phone_map = dict(raw_phone_map) if isinstance(raw_phone_map, dict) else {}
            phone_map[phone_key] = {
                **entry,
                "phone": phone_key,
                "fastapi_port": port,
                **(
                    {"project_phone": phone_key}
                    if canonical_context_key
                    else {}
                ),
            }
            config[PHONE_GIT_CONTEXTS_KEY] = phone_map
        write_git_config_file(config)
    return {
        **entry,
        **({"phone": phone_key} if phone_key else {}),
    }


async def save_git_address(
    port: int,
    git_address: str,
    project_name: str | None = None,
    phone: str | None = None,
    git_context_key: str | None = None,
) -> dict[str, Any]:
    clean_git_address = git_address.strip()
    clean_git_context_key = (
        normalize_project_context_reference(git_context_key)
        if git_context_key
        else normalize_project_context_reference(clean_git_address)
    )
    if git_context_key:
        try:
            _, repository_key = normalize_project_git_address(clean_git_address)
        except HTTPException:
            repository_key = ""
        if (
            repository_key
            and project_repository_key_from_context(clean_git_context_key)
            != repository_key
        ):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="git_context_key belongs to a different Git repository",
            )
    entry = {
        "git_address": clean_git_address,
        "project_name": normalize_project_name(project_name, clean_git_address),
        "git_context_key": clean_git_context_key,
        "updated_at": utc_now(),
    }
    phone_key = normalize_phone_key(phone)
    if phone_key == PROJECT_MANAGER_PHONE:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Phone {PROJECT_MANAGER_PHONE} is reserved for Project Manager",
        )
    async with git_config_lock:
        return await asyncio.to_thread(
            save_git_address_transaction,
            port,
            entry,
            phone_key,
        )


def delete_git_context_phone_transaction(phone_key: str) -> dict[str, Any]:
    with git_config_file_lock():
        config = read_git_config_file()
        canonical_context_key = canonical_project_context_key_for_phone(
            config,
            phone_key,
        )
        if canonical_context_key:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    f"Phone {phone_key} is reserved for project "
                    f"{canonical_context_key} and cannot be deleted"
                ),
            )
        raw_phone_map = config.get(PHONE_GIT_CONTEXTS_KEY)
        if not isinstance(raw_phone_map, dict) or phone_key not in raw_phone_map:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Git context for phone {phone_key} was not found",
            )
        protected_mapping = raw_phone_map.get(phone_key)
        if (
            isinstance(protected_mapping, dict)
            and str(protected_mapping.get("managed_by") or "").strip()
            == "group_api"
        ):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "error": "group_managed_phone_is_immutable",
                    "phone": phone_key,
                    "agent_id": protected_mapping.get("agent_id"),
                },
            )

        phone_map = dict(raw_phone_map)
        removed = phone_map.pop(phone_key)
        if phone_map:
            config[PHONE_GIT_CONTEXTS_KEY] = phone_map
        else:
            config.pop(PHONE_GIT_CONTEXTS_KEY, None)
        write_git_config_file(config)
    return removed if isinstance(removed, dict) else {"phone": phone_key}


async def delete_git_context_phone(phone: str) -> dict[str, Any]:
    phone_key = normalize_phone_key(phone)
    if not phone_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Phone is required",
        )

    async with git_config_lock:
        return await asyncio.to_thread(delete_git_context_phone_transaction, phone_key)


def request_port(request: Request) -> int | None:
    if request.url.port:
        return request.url.port

    host = request.headers.get("host", "")
    if ":" not in host:
        return None

    port_text = host.rsplit(":", 1)[-1]
    try:
        return int(port_text)
    except ValueError:
        return None


def resolve_git_reference(git_address: str) -> dict[str, Any]:
    address = git_address.strip()
    if not address:
        return {}

    local_path = Path(address)
    if local_path.exists():
        command = ["git", "-C", str(local_path), "rev-parse", "HEAD"]
    else:
        command = ["git", "ls-remote", address, "HEAD"]

    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=15,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"git_error": str(exc)}

    if result.returncode != 0:
        return {"git_error": (result.stderr or result.stdout).strip()}

    output = result.stdout.strip()
    if not output:
        return {"git_error": "Git returned an empty commit reference"}

    commit = output.split()[0]
    return {
        "git_commit": commit,
        "git_commit_short": commit[:12],
    }


def normalize_commit_ref(value: str | None) -> str:
    commit = str(value or "").strip()
    if not re.fullmatch(r"[0-9a-fA-F]{4,64}", commit):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Commit hash must be a hexadecimal Git object id",
        )
    return commit


def github_commit_base_url(git_address: str) -> str | None:
    address = git_address.strip()
    if not address:
        return None

    ssh_match = re.fullmatch(r"git@github\.com:([^/]+)/(.+?)(?:\.git)?", address)
    if ssh_match:
      owner, repo = ssh_match.groups()
      return f"https://github.com/{owner}/{repo}"

    parsed = urllib.parse.urlparse(address)
    if parsed.netloc.lower() != "github.com":
        return None

    parts = [part for part in parsed.path.strip("/").split("/") if part]
    if len(parts) < 2:
        return None

    owner = parts[0]
    repo = parts[1][:-4] if parts[1].endswith(".git") else parts[1]
    if not owner or not repo:
        return None

    return f"https://github.com/{owner}/{repo}"


def read_url_limited(url: str, max_bytes: int = MAX_PATCH_SIZE) -> str:
    try:
        with urllib.request.urlopen(url, timeout=30) as response:
            payload = response.read(max_bytes + 1)
    except urllib.error.HTTPError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"GitHub patch request failed: HTTP {exc.code}",
        ) from exc
    except (urllib.error.URLError, TimeoutError) as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"GitHub patch request failed: {exc}",
        ) from exc

    if len(payload) > max_bytes:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail="Patch is too large to copy safely",
        )

    return payload.decode("utf-8", errors="replace")


def git_patch_from_local_repo(
    repo_path: Path,
    to_commit: str,
    from_commit: str | None = None,
) -> str:
    if from_commit:
        command = [
            "git",
            "-C",
            str(repo_path),
            "diff",
            "--no-color",
            "--no-ext-diff",
            "--stat",
            "--patch",
            from_commit,
            to_commit,
        ]
    else:
        command = [
            "git",
            "-C",
            str(repo_path),
            "show",
            "--format=fuller",
            "--patch",
            "--stat",
            "--no-color",
            "--no-ext-diff",
            to_commit,
        ]

    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Local git patch failed: {exc}",
        ) from exc

    if result.returncode != 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(result.stderr or result.stdout or "Local git patch failed").strip(),
        )

    if len(result.stdout.encode("utf-8", errors="replace")) > MAX_PATCH_SIZE:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail="Patch is too large to copy safely",
        )

    return result.stdout


def normalize_git_remote_address(value: str) -> str:
    address = value.strip().lower()
    ssh_match = re.fullmatch(r"git@github\.com:([^/]+)/(.+?)(?:\.git)?", address)
    if ssh_match:
        owner, repo = ssh_match.groups()
        return f"github.com/{owner}/{repo}"

    parsed = urllib.parse.urlparse(address)
    if parsed.netloc:
        path = parsed.path.strip("/")
        if path.lower().endswith(".git"):
            path = path[:-4]
        return f"{parsed.netloc.lower()}/{path.lower()}"

    if address.endswith(".git"):
        address = address[:-4]
    return address


def normalize_git_context_key(value: str) -> str:
    address = str(value or "").strip()
    if not address:
        return ""

    local_path = Path(address)
    if local_path.exists():
        try:
            return f"local:{local_path.resolve()}".lower()
        except OSError:
            return f"local:{local_path.absolute()}".lower()

    return normalize_git_remote_address(address)


def normalize_project_repository_path(hostname: str, raw_path: str) -> str:
    path = raw_path.strip("/")
    if path.lower().endswith(".git"):
        path = path[:-4]
    if hostname.rstrip(".").lower() == "github.com":
        return path.lower()
    return path


def normalize_project_git_address(value: Any) -> tuple[str, str]:
    if not isinstance(value, str):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="git_address must be a string",
        )

    address = value.strip()
    if not address:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="git_address is required",
        )
    if len(address) > 2048:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="git_address is too long",
        )
    if any(ord(character) < 32 or ord(character) == 127 for character in address):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="git_address contains control characters",
        )

    is_windows_path = bool(re.match(r"^[A-Za-z]:[\\/]", address))
    is_absolute_path = is_windows_path or address.startswith(("/", "\\\\"))
    if is_absolute_path:
        if "#" in address:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Local git_address must not contain '#'",
            )
        try:
            resolved_path = Path(os.path.abspath(address))
        except OSError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="git_address contains an invalid local path",
            ) from exc
        return address, f"local:{resolved_path}".lower()

    scp_match = None
    if "://" not in address:
        scp_match = re.fullmatch(
            r"(?:[^@\s/:]+@)?(?P<host>[A-Za-z0-9.-]+):(?P<path>[^\s]+)",
            address,
        )
    if scp_match:
        host = scp_match.group("host").lower()
        path = normalize_project_repository_path(host, scp_match.group("path"))
        if not path:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="git_address does not contain a repository path",
            )
        return address, f"{host}/{path}"

    parsed = urllib.parse.urlparse(address)
    if parsed.scheme:
        if parsed.scheme.lower() not in {"http", "https", "ssh", "git"}:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="git_address uses an unsupported URL scheme",
            )
        if not parsed.hostname:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="git_address does not contain a host",
            )
        if parsed.password is not None or (
            parsed.scheme.lower() in {"http", "https", "git"}
            and parsed.username is not None
        ):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="git_address must not contain credentials",
            )
        if parsed.query or parsed.fragment:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="git_address must not contain a query or fragment",
            )
        path = normalize_project_repository_path(parsed.hostname, parsed.path)
        if not path:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="git_address does not contain a repository path",
            )
        try:
            port = parsed.port
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="git_address contains an invalid port",
            ) from exc
        host = parsed.hostname.lower()
        default_port = {
            "http": 80,
            "https": 443,
            "ssh": 22,
            "git": 9418,
        }[parsed.scheme.lower()]
        if port is not None and port != default_port:
            host = f"{host}:{port}"
        return address, f"{host}/{path}"

    bare_remote_match = re.fullmatch(
        r"(?P<host>(?:localhost|[A-Za-z0-9.-]+\.[A-Za-z0-9.-]+))/(?P<path>[^\s]+)",
        address,
        re.IGNORECASE,
    )
    if bare_remote_match:
        host = bare_remote_match.group("host").lower()
        path = normalize_project_repository_path(
            host,
            bare_remote_match.group("path"),
        )
        if path:
            return address, f"{host}/{path}"

    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=(
            "git_address must be an absolute local path, a Git URL, "
            "an SCP-style address, or a host/repository path"
        ),
    )


def project_repository_key_from_context(value: Any) -> str:
    raw_key = str(value or "").strip()
    base_reference = raw_key.split("#", 1)[0]
    if not base_reference:
        return ""
    if not base_reference.lower().startswith("local:"):
        try:
            _, repository_key = normalize_project_git_address(base_reference)
            return repository_key
        except HTTPException:
            pass
    normalized_key = normalize_git_context_key(base_reference)
    return normalized_key.split("#", 1)[0]


def normalize_project_context_reference(value: Any) -> str:
    raw_key = str(value or "").strip()
    if not raw_key:
        return ""
    base_reference, separator, raw_suffix = raw_key.partition("#")
    repository_key = project_repository_key_from_context(base_reference)
    if not repository_key:
        return ""
    suffix = raw_suffix.strip().casefold() if separator else ""
    return f"{repository_key}#{suffix}" if suffix else repository_key


def normalize_requested_project_context_key(value: Any, repository_key: str) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="git_context_key must be a string",
        )

    raw_key = value.strip()
    if not raw_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="git_context_key must not be blank when provided",
        )
    if len(raw_key) > 512 or any(ord(character) < 32 for character in raw_key):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="git_context_key is invalid",
        )

    context_key = normalize_project_context_reference(raw_key)
    if not context_key or project_repository_key_from_context(context_key) != repository_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="git_context_key belongs to a different Git repository",
        )
    return context_key


def project_name_from_git_address(git_address: str) -> str:
    try:
        _, context_key = normalize_project_git_address(git_address)
    except HTTPException:
        context_key = normalize_git_context_key(git_address)
    if context_key in PROJECT_NAMES_BY_GIT_CONTEXT:
        return PROJECT_NAMES_BY_GIT_CONTEXT[context_key]

    clean_address = git_address.strip()
    if re.match(r"^[A-Za-z]:[\\/]", clean_address) or clean_address.startswith("\\\\"):
        raw_name = Path(clean_address.rstrip("/\\")).name
        return raw_name or DEFAULT_PROJECT_NAME

    parsed = urllib.parse.urlparse(clean_address)
    raw_name = ""
    if parsed.path:
        raw_name = parsed.path.rstrip("/").rsplit("/", 1)[-1]
    if not raw_name:
        raw_name = git_address.strip().rstrip("/").rsplit("/", 1)[-1]
    if raw_name.lower().endswith(".git"):
        raw_name = raw_name[:-4]
    return raw_name or DEFAULT_PROJECT_NAME


def normalize_project_name(value: Any, git_address: str = "") -> str:
    name = str(value or "").strip()
    return name or project_name_from_git_address(git_address) or DEFAULT_PROJECT_NAME


def normalize_phone_key(value: Any) -> str:
    return str(value or "").strip()


def normalize_project_phone(value: Any) -> str:
    phone = normalize_phone_key(value)
    if not (phone.isdigit() and len(phone) == 4):
        return ""
    numeric_phone = int(phone)
    if not PROJECT_PHONE_MIN <= numeric_phone <= PROJECT_PHONE_MAX:
        return ""
    return phone


def canonical_project_context_key_for_phone(
    config: dict[str, Any],
    phone: Any,
) -> str:
    project_phone = normalize_project_phone(phone)
    if not project_phone:
        return ""
    for context_key, entry in project_registry_from_config(config).items():
        if normalize_project_phone(entry.get("project_phone")) == project_phone:
            return context_key
    return ""


def normalize_git_context_config_entry(raw_entry: Any) -> dict[str, Any] | None:
    if not isinstance(raw_entry, dict):
        return None

    git_address = raw_entry.get("git_address")
    if not isinstance(git_address, str) or not git_address.strip():
        return None

    clean_git_address = git_address.strip()
    raw_context_key = str(raw_entry.get("git_context_key") or "").strip()
    address_context_key = normalize_project_context_reference(clean_git_address)
    if not raw_context_key:
        context_key = address_context_key
    else:
        context_key = normalize_project_context_reference(raw_context_key)
        address_repository_key = project_repository_key_from_context(address_context_key)
        context_repository_key = project_repository_key_from_context(context_key)
        if (
            address_repository_key
            and context_repository_key
            and address_repository_key != context_repository_key
            and address_repository_key.casefold() == context_repository_key.casefold()
        ):
            _, separator, suffix = context_key.partition("#")
            context_key = (
                f"{address_repository_key}#{suffix}"
                if separator and suffix
                else address_repository_key
            )
    if not context_key:
        return None

    entry: dict[str, Any] = {
        "git_address": clean_git_address,
        "project_name": normalize_project_name(raw_entry.get("project_name"), clean_git_address),
        "git_context_key": context_key,
    }
    for key in (
        "created_at",
        "updated_at",
        "fastapi_port",
        "phone",
        "project_phone",
    ):
        value = raw_entry.get(key)
        if value is not None:
            entry[key] = value
    for key, expected_type, default_value in (
        ("groups", list, []),
        ("group_relationships", list, []),
        ("customer_reporting", dict, {}),
    ):
        value = raw_entry.get(key)
        entry[key] = deepcopy(value) if isinstance(value, expected_type) else default_value
    return entry


def project_registry_from_config(config: dict[str, Any]) -> dict[str, dict[str, Any]]:
    raw_projects = config.get(PROJECTS_KEY)
    if not isinstance(raw_projects, dict):
        return {}

    projects: dict[str, dict[str, Any]] = {}
    for raw_key, raw_entry in raw_projects.items():
        if not isinstance(raw_entry, dict):
            continue
        candidate = dict(raw_entry)
        candidate.setdefault("git_context_key", str(raw_key))
        entry = normalize_git_context_config_entry(candidate)
        if entry is None:
            continue
        projects[entry["git_context_key"]] = entry
    return projects


def phone_git_contexts_from_config(config: dict[str, Any]) -> dict[str, dict[str, Any]]:
    raw_phone_map = config.get(PHONE_GIT_CONTEXTS_KEY)
    if not isinstance(raw_phone_map, dict):
        return {}

    phone_map: dict[str, dict[str, Any]] = {}
    for raw_phone, raw_entry in raw_phone_map.items():
        phone = normalize_phone_key(raw_phone)
        entry = normalize_git_context_config_entry(raw_entry)
        if not phone or phone == PROJECT_MANAGER_PHONE or entry is None:
            continue
        phone_map[phone] = {
            **entry,
            "phone": phone,
        }
    return phone_map


def phone_git_context_entry_from_config(
    config: dict[str, Any],
    phone: str,
) -> dict[str, Any] | None:
    return phone_git_contexts_from_config(config).get(normalize_phone_key(phone))


def git_context_entry_has_consistent_repository(entry: dict[str, Any]) -> bool:
    try:
        _, address_repository_key = normalize_project_git_address(
            entry.get("git_address")
        )
    except HTTPException:
        safe_address = safe_public_git_address(entry.get("git_address"))
        if not safe_address:
            return False
        try:
            _, address_repository_key = normalize_project_git_address(safe_address)
        except HTTPException:
            return False
    context_repository_key = project_repository_key_from_context(
        entry.get("git_context_key")
    )
    return bool(context_repository_key) and context_repository_key == address_repository_key


def configured_git_contexts_from_config(
    config: dict[str, Any],
    current_port: int | None = None,
) -> list[dict[str, Any]]:
    contexts_by_key: dict[str, dict[str, Any]] = {}

    def add_context(raw_entry: Any, *, port_text: str = "", phone: str = "") -> None:
        entry = normalize_git_context_config_entry(raw_entry)
        if entry is None:
            return
        if not git_context_entry_has_consistent_repository(entry):
            return

        context_key = entry["git_context_key"]
        context = contexts_by_key.setdefault(
            context_key,
            {
                "git_context_key": context_key,
                "project_name": entry["project_name"],
                "git_address": entry["git_address"],
                "ports": [],
                "phones": [],
                "project_phone": "",
                "groups": [],
                "group_relationships": [],
                "customer_reporting": {},
                "created_at": entry.get("created_at"),
                "updated_at": entry.get("updated_at"),
                "is_current_port": False,
            },
        )
        project_phone = normalize_project_phone(entry.get("project_phone"))
        if project_phone and not context.get("project_phone"):
            context["project_phone"] = project_phone
        if entry.get("groups") and not context.get("groups"):
            context["groups"] = deepcopy(entry["groups"])
        if entry.get("group_relationships") and not context.get("group_relationships"):
            context["group_relationships"] = deepcopy(entry["group_relationships"])
        if entry.get("customer_reporting") and not context.get("customer_reporting"):
            context["customer_reporting"] = deepcopy(entry["customer_reporting"])
        if not context.get("created_at") and entry.get("created_at"):
            context["created_at"] = entry["created_at"]
        if not context.get("updated_at") and entry.get("updated_at"):
            context["updated_at"] = entry["updated_at"]
        if port_text and port_text not in context["ports"]:
            context["ports"].append(port_text)
        if phone and phone not in context["phones"]:
            context["phones"].append(phone)
        if current_port is not None and port_text == str(current_port):
            context["is_current_port"] = True
            context["project_name"] = entry["project_name"]
            context["git_address"] = entry["git_address"]
            context["updated_at"] = entry.get("updated_at")

    for entry in project_registry_from_config(config).values():
        add_context(entry)

    for raw_port, entry in config.items():
        if raw_port in {PHONE_GIT_CONTEXTS_KEY, PROJECTS_KEY}:
            continue
        if not isinstance(entry, dict):
            continue

        port_text = str(raw_port)
        add_context(entry, port_text=port_text)

    for phone, entry in phone_git_contexts_from_config(config).items():
        port_text = str(entry.get("fastapi_port") or "")
        add_context(entry, port_text=port_text, phone=phone)

    return sorted(
        contexts_by_key.values(),
        key=lambda item: (
            not bool(item.get("is_current_port")),
            str(item.get("project_name", "")).lower(),
            str(item.get("git_context_key", "")),
        ),
    )


def configured_git_context_for_key(
    config: dict[str, Any],
    git_context_key: str,
    current_port: int | None = None,
) -> dict[str, Any] | None:
    normalized_key = normalize_project_context_reference(git_context_key)
    for context in configured_git_contexts_from_config(config, current_port):
        if context.get("git_context_key") == normalized_key:
            return context
    return None


def project_repository_key_for_context(context: dict[str, Any]) -> str:
    git_address = context.get("git_address")
    if isinstance(git_address, str) and git_address.strip():
        try:
            _, repository_key = normalize_project_git_address(git_address)
            return repository_key
        except HTTPException:
            safe_address = safe_public_git_address(git_address)
            if safe_address:
                try:
                    _, repository_key = normalize_project_git_address(safe_address)
                    return repository_key
                except HTTPException:
                    pass
    return project_repository_key_from_context(context.get("git_context_key"))


def safe_public_git_address(value: Any) -> str:
    address = str(value or "").strip()
    if re.match(r"^[A-Za-z]:[\\/]", address) or address.startswith(("/", "\\\\")):
        return address

    scp_match = re.fullmatch(
        r"(?:(?P<user>[^@\s/:]+)@)?(?P<host>[A-Za-z0-9.-]+):(?P<path>[^\s]+)",
        address,
    )
    if scp_match:
        user = str(scp_match.group("user") or "")
        safe_user = "git@" if user.casefold() == "git" else ""
        return f"{safe_user}{scp_match.group('host')}:{scp_match.group('path')}"

    parsed = urllib.parse.urlparse(address)
    if not parsed.scheme:
        bare_remote_match = re.fullmatch(
            r"(?:localhost|[A-Za-z0-9.-]+\.[A-Za-z0-9.-]+)/[^\s]+",
            address,
            re.IGNORECASE,
        )
        return address if bare_remote_match else ""
    if parsed.scheme.lower() not in {"http", "https", "ssh", "git"} or not parsed.hostname:
        return ""

    host = parsed.hostname
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    try:
        port = parsed.port
    except ValueError:
        port = None
    if port is not None:
        host = f"{host}:{port}"
    return urllib.parse.urlunparse(
        (parsed.scheme, host, parsed.path, parsed.params, "", "")
    )


def public_project_context(context: dict[str, Any]) -> dict[str, Any]:
    project = {
        "project_name": normalize_project_name(
            context.get("project_name"),
            str(context.get("git_address") or ""),
        ),
        "git_address": safe_public_git_address(context.get("git_address")),
        "git_context_key": normalize_project_context_reference(
            str(context.get("git_context_key") or "").strip()
        ),
        "project_phone": normalize_project_phone(context.get("project_phone")),
        "project_id": normalize_project_phone(context.get("project_phone")),
        "groups": deepcopy(context.get("groups"))
        if isinstance(context.get("groups"), list)
        else [],
        "group_relationships": deepcopy(context.get("group_relationships"))
        if isinstance(context.get("group_relationships"), list)
        else [],
        "customer_reporting": deepcopy(context.get("customer_reporting"))
        if isinstance(context.get("customer_reporting"), dict)
        else {},
        "phones": sorted(
            {
                str(phone).strip()
                for phone in context.get("phones", [])
                if str(phone).strip()
            }
        ),
        "ports": sorted(
            {
                str(port).strip()
                for port in context.get("ports", [])
                if str(port).strip()
            }
        ),
    }
    for key in ("created_at", "updated_at"):
        value = context.get(key)
        if value:
            project[key] = value
    return project


def resolve_project_context_from_config(
    config: dict[str, Any],
    repository_key: str,
    requested_context_key: str = "",
    current_port: int | None = None,
) -> dict[str, Any] | None:
    contexts = configured_git_contexts_from_config(config, current_port)
    candidates = [
        context
        for context in contexts
        if project_repository_key_for_context(context) == repository_key
        and project_repository_key_from_context(context.get("git_context_key"))
        == repository_key
    ]

    if requested_context_key:
        for context in candidates:
            if context.get("git_context_key") == requested_context_key:
                return context
        if candidates:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "error": "project_context_not_found",
                    "message": (
                        "The requested git_context_key is not registered for this "
                        "Git repository; choose one of the listed values"
                    ),
                    "candidates": [
                        public_project_context(context) for context in candidates
                    ],
                },
            )
        return None

    exact_match = next(
        (
            context
            for context in candidates
            if context.get("git_context_key") == repository_key
        ),
        None,
    )
    if exact_match is not None:
        return exact_match
    if len(candidates) == 1:
        return candidates[0]
    if len(candidates) > 1:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "ambiguous_project",
                "message": (
                    "Several projects use this Git repository; repeat the request "
                    "with one of the listed git_context_key values"
                ),
                "candidates": [public_project_context(context) for context in candidates],
            },
        )
    return None


def resolve_or_create_project_transaction(
    git_address: str,
    repository_key: str,
    requested_context_key: str = "",
    project_name: str | None = None,
    current_port: int | None = None,
) -> tuple[bool, dict[str, Any], str, bool]:
    with git_config_file_lock():
        config = read_git_config_file()
        existing = resolve_project_context_from_config(
            config,
            repository_key,
            requested_context_key,
            current_port,
        )
        created = existing is None
        timestamp = utc_now()
        if created:
            context_key = requested_context_key or repository_key
            raw_projects = config.get(PROJECTS_KEY)
            projects = dict(raw_projects) if isinstance(raw_projects, dict) else {}
            projects[context_key] = {
                "project_name": normalize_project_name(project_name, git_address),
                "git_address": git_address,
                "git_context_key": context_key,
                "created_at": timestamp,
                "updated_at": timestamp,
            }
            config[PROJECTS_KEY] = projects
            project_context = configured_git_context_for_key(
                config,
                context_key,
                current_port,
            )
        else:
            project_context = existing

        if project_context is None:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Project was saved but could not be loaded",
            )

        context_key = str(project_context.get("git_context_key") or "").strip()
        project_phone = normalize_project_phone(
            project_context.get("project_phone")
        )
        phone_assigned = not project_phone

        raw_phone_map = config.get(PHONE_GIT_CONTEXTS_KEY)
        phone_map = dict(raw_phone_map) if isinstance(raw_phone_map, dict) else {}
        raw_projects = config.get(PROJECTS_KEY)
        projects = dict(raw_projects) if isinstance(raw_projects, dict) else {}

        registry_key = context_key
        registry_entry: dict[str, Any] | None = None
        for raw_key, raw_entry in projects.items():
            if not isinstance(raw_entry, dict):
                continue
            candidate = dict(raw_entry)
            candidate.setdefault("git_context_key", str(raw_key))
            normalized_entry = normalize_git_context_config_entry(candidate)
            if (
                normalized_entry is not None
                and normalized_entry.get("git_context_key") == context_key
            ):
                registry_key = str(raw_key)
                registry_entry = dict(raw_entry)
                break

        def persist_project_phone_binding(phone: str) -> bool:
            changed = False
            project_entry = dict(registry_entry or {})
            required_project_fields = {
                "project_name": normalize_project_name(
                    project_context.get("project_name"),
                    git_address,
                ),
                "git_address": git_address,
                "git_context_key": context_key,
                "project_phone": phone,
            }
            if registry_entry is None:
                project_entry.update(required_project_fields)
                for date_key in ("created_at", "updated_at"):
                    if project_context.get(date_key):
                        project_entry[date_key] = project_context[date_key]
                changed = True
            elif project_entry.get("project_phone") != phone:
                project_entry["project_phone"] = phone
                changed = True
            projects[registry_key] = project_entry
            config[PROJECTS_KEY] = projects

            raw_mapping = phone_map.get(phone)
            mapping = dict(raw_mapping) if isinstance(raw_mapping, dict) else {}
            normalized_mapping = normalize_git_context_config_entry(mapping)
            raw_mapping_key = (
                str(normalized_mapping.get("git_context_key") or "").strip()
                if normalized_mapping is not None
                else normalize_project_context_reference(
                    str(mapping.get("git_context_key") or "").strip()
                )
            )
            if raw_mapping_key and raw_mapping_key != context_key:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "error": "project_phone_conflict",
                        "message": (
                            f"Project phone {phone} is already mapped to a different "
                            "Git context"
                        ),
                    },
                )

            required_mapping_fields: dict[str, Any] = {
                "project_name": required_project_fields["project_name"],
                "git_address": git_address,
                "git_context_key": context_key,
                "phone": phone,
                "project_phone": phone,
            }
            if current_port is not None and not mapping.get("fastapi_port"):
                required_mapping_fields["fastapi_port"] = current_port
            mapping_changed = not isinstance(raw_mapping, dict) or any(
                mapping.get(key) != value
                for key, value in required_mapping_fields.items()
            )
            if mapping_changed:
                mapping.update(required_mapping_fields)
                mapping.setdefault("created_at", timestamp)
                mapping["updated_at"] = timestamp
                phone_map[phone] = mapping
                config[PHONE_GIT_CONTEXTS_KEY] = phone_map
                changed = True
            return changed

        if phone_assigned:
            with agents_file_lock():
                used_phones = {
                    normalize_phone_key(phone)
                    for phone in phone_map
                    if normalize_phone_key(phone)
                }
                used_phones.update(
                    normalize_project_phone(entry.get("project_phone"))
                    for entry in project_registry_from_config(config).values()
                    if normalize_project_phone(entry.get("project_phone"))
                )
                used_phones.update(
                    normalize_phone_key(agent.get("phone"))
                    for agent in read_agents_file()
                    if normalize_phone_key(agent.get("phone"))
                )
                used_phones.update(DEFAULT_AGENT_PHONES.values())
                used_phones.add(PROJECT_MANAGER_PHONE)

                for candidate in range(PROJECT_PHONE_MIN, PROJECT_PHONE_MAX + 1):
                    candidate_phone = f"{candidate:04d}"
                    if candidate_phone not in used_phones:
                        project_phone = candidate_phone
                        break
                if not project_phone:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail=(
                            "No free project phone numbers in range "
                            f"{PROJECT_PHONE_MIN:04d}-{PROJECT_PHONE_MAX:04d}."
                        ),
                    )

                persist_project_phone_binding(project_phone)
                write_git_config_file(config)
        else:
            binding_changed = persist_project_phone_binding(project_phone)
            if created or binding_changed:
                write_git_config_file(config)

        resolved = configured_git_context_for_key(
            config,
            context_key,
            current_port,
        )
        if resolved is None:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Project was saved but could not be loaded",
            )
        return created, resolved, project_phone, phone_assigned


async def resolve_or_create_project(
    git_address: str,
    repository_key: str,
    requested_context_key: str = "",
    project_name: str | None = None,
    current_port: int | None = None,
) -> tuple[bool, dict[str, Any], str, bool]:
    async with git_config_lock:
        return await asyncio.to_thread(
            resolve_or_create_project_transaction,
            git_address,
            repository_key,
            requested_context_key,
            project_name,
            current_port,
        )


def agent_context_key_values(raw_value: Any) -> set[str]:
    if isinstance(raw_value, (list, tuple, set)):
        raw_items = list(raw_value)
    else:
        raw_items = re.split(r"[,\n;|]+", str(raw_value or ""))

    keys: set[str] = set()
    for raw_item in raw_items:
        item = str(raw_item or "").strip().strip("[]\"'").strip()
        if not item:
            continue
        context_key = normalize_project_context_reference(item)
        if context_key:
            keys.add(context_key)
    return keys


def agent_matches_project_context(
    agent: dict[str, Any],
    target_context_key: str,
    phone_contexts: dict[str, dict[str, Any]],
) -> bool:
    if (
        str(agent.get("id") or "").strip() == PROJECT_MANAGER_AGENT_ID
        or str(agent.get("phone") or "").strip() == PROJECT_MANAGER_PHONE
    ):
        return False

    normalized_target_context_key = normalize_project_context_reference(
        target_context_key
    )
    parameters = agent.get("parameters")
    if not isinstance(parameters, dict):
        parameters = {}

    included_keys: set[str] = set()
    agent_phone = str(agent.get("phone") or "").strip()
    phone_context = phone_contexts.get(agent_phone)
    if phone_context is not None:
        included_keys.update(agent_context_key_values(phone_context.get("git_context_key")))
    included_keys.update(agent_context_key_values(parameters.get("git_context_key")))
    included_keys.update(agent_context_key_values(parameters.get("git_context_keys")))

    excluded_keys = agent_context_key_values(
        parameters.get("git_context_excluded_keys")
    )
    return (
        normalized_target_context_key in included_keys
        and normalized_target_context_key not in excluded_keys
    )


def full_agents_for_project(
    agents: list[dict[str, Any]],
    target_context_key: str,
    phone_contexts: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    project_agents: list[dict[str, Any]] = []
    for agent in agents:
        if not agent_matches_project_context(
            agent,
            target_context_key,
            phone_contexts,
        ):
            continue
        full_agent = dict(agent)
        parameters = agent.get("parameters")
        full_agent["parameters"] = (
            dict(parameters) if isinstance(parameters, dict) else {}
        )
        full_agent["status"] = agent_status_value(agent) or "active"
        full_agent["presence"] = agent_presence_snapshot(agent)
        project_agents.append(full_agent)
    return sorted(
        project_agents,
        key=lambda agent: (
            str(agent.get("name") or "").casefold(),
            str(agent.get("phone") or ""),
            str(agent.get("id") or ""),
        ),
    )


def normalized_agent_git_branch(value: Any, field_name: str = "git_branch") -> str:
    branch = str(value or "").strip()
    if not branch:
        return ""
    invalid = (
        len(branch) > 240
        or branch == "@"
        or branch.startswith(("/", ".", "-"))
        or branch.endswith(("/", ".", ".lock"))
        or ".." in branch
        or "//" in branch
        or "@{" in branch
        or any(character.isspace() or ord(character) < 32 for character in branch)
        or any(character in "~^:?*[\\" for character in branch)
        or any(part.startswith(".") or part.endswith(".lock") for part in branch.split("/"))
    )
    if invalid:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"{field_name} is not a valid Git branch name",
        )
    return branch


def default_imported_agent_branch(agent_id: str, name: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9._-]+", "-", agent_id or name).strip("-._").lower()
    if not slug:
        slug = hashlib.sha256(f"{agent_id}\0{name}".encode("utf-8")).hexdigest()[:12]
    return f"agent/{slug[:120]}"


def profile_without_agent_communication_block(profile: Any) -> str:
    text = str(profile or "").strip()
    start = text.find(AGENT_COMMUNICATION_BLOCK_START)
    while start >= 0:
        end = text.find(AGENT_COMMUNICATION_BLOCK_END, start)
        if end < 0:
            text = text[:start].rstrip()
            break
        end += len(AGENT_COMMUNICATION_BLOCK_END)
        text = (text[:start].rstrip() + "\n\n" + text[end:].lstrip()).strip()
        start = text.find(AGENT_COMMUNICATION_BLOCK_START)
    return text


def agent_communication_profile(
    profile: Any,
    agent: dict[str, Any],
    project_phone: str,
    project: dict[str, Any],
    project_agents: list[dict[str, Any]],
    assignment_mode: str = "parallel",
) -> str:
    authored_profile = profile_without_agent_communication_block(profile)
    agent_name = str(agent.get("name") or "").strip()
    agent_id = str(agent.get("id") or "").strip()
    agent_phone = str(agent.get("phone") or "").strip()
    parameters = agent.get("parameters") if isinstance(agent.get("parameters"), dict) else {}
    git_branch = str(agent.get("git_branch") or parameters.get("git_branch") or "").strip()
    project_name = str(project.get("project_name") or project_phone).strip()
    git_address = str(project.get("git_address") or "").strip()

    peer_lines: list[str] = []
    for peer in sorted(
        project_agents,
        key=lambda item: (
            str(item.get("name") or "").casefold(),
            str(item.get("phone") or ""),
        ),
    ):
        peer_phone = str(peer.get("phone") or "").strip()
        if not peer_phone or peer_phone == agent_phone:
            continue
        peer_parameters = (
            peer.get("parameters") if isinstance(peer.get("parameters"), dict) else {}
        )
        peer_branch = str(
            peer.get("git_branch") or peer_parameters.get("git_branch") or ""
        ).strip()
        peer_description = (
            f"- {str(peer.get('name') or peer.get('id') or peer_phone).strip()}: "
            f"phone={peer_phone}, id={str(peer.get('id') or '').strip()}"
        )
        if peer_branch:
            peer_description += f", git_branch={peer_branch}"
        peer_lines.append(peer_description)
    if not peer_lines:
        peer_lines.append("- Других агентов в проекте пока нет.")

    example_payload = json.dumps(
        {
            "from_phone": agent_phone,
            "to_phone": "<PHONE_ДРУГОГО_АГЕНТА>",
            "sender": agent_name,
            "receiver": "<ИМЯ_ДРУГОГО_АГЕНТА>",
            "message": "<ТЕКСТ_СООБЩЕНИЯ>",
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )
    lines = [
        AGENT_COMMUNICATION_BLOCK_START,
        f"Версия инструкции: {AGENT_COMMUNICATION_VERSION}.",
        f"Проект: {project_name}; project_phone={project_phone}.",
    ]
    if git_address:
        lines.append(f"Git-репозиторий: {git_address}.")
    lines.extend(
        [
            f"Вы: {agent_name}; agent_id={agent_id}; agent_phone={agent_phone}.",
            f"Ваша рабочая Git-ветка: {git_branch}.",
            (
                "Работайте, коммитьте и отправляйте изменения только в эту ветку. "
                "Не переключайтесь на ветки других агентов и не изменяйте их без явного задания."
            ),
            (
                "Перед началом выполните git fetch origin. Если ветка уже существует, "
                f"переключитесь на {git_branch}; если нет — создайте её командой "
                f"git switch -c {git_branch} от базовой ветки проекта и опубликуйте "
                f"git push -u origin {git_branch}."
            ),
            *(
                [
                    "Проект работает в последовательном режиме обхода графа: один исполнитель меняет роль по адресату следующего задания в очереди.",
                    (
                        "Чтобы узнать следующую роль, вызовите общий endpoint:"
                    ),
                    "GET или POST /api/v1/agents/whoami",
                    (
                        "Он спросит Git-репозиторий и вернёт reply_url. Отправьте на "
                        "reply_url git_address проекта один раз и сохраните весь ответ. "
                        "Система извлечёт следующий "
                        "элемент очереди; его адресат определит новые agent.profile, "
                        "agent.phone и git_branch."
                    ),
                    "Не закрепляйте исполнителя за ролью: на следующем узле графа роль может измениться.",
                    (
                        "Если ответ содержит agent и active_task, сразу покажите требуемую "
                        "сводку и приступайте к работе: не ждите отдельного подтверждения."
                    ),
                    (
                        "Повторный запрос до handoff безопасно вернёт ту же активную "
                        "identity/task и не заберёт новый элемент очереди."
                    ),
                ]
                if assignment_mode == "sequential"
                else []
            ),
            "",
            "Получить свою актуальную карточку (включая этот профиль и ветку):",
            f"GET /api/v1/projects/{project_phone}/agents/{agent_phone}",
            "",
            *(
                [
                    "Получить следующую личность и задачу нужно только через общий queue-graph flow:",
                    "GET или POST /api/v1/agents/whoami, затем POST выданного reply_url с git_address.",
                    "После получения agent и active_task начинайте работу сразу, без запроса дополнительного разрешения.",
                    "Не используйте phone-specific whoami для перехода между узлами графа.",
                ]
                if assignment_mode == "sequential"
                else [
                    "Представиться системе, спросить «Кто я?» и отметить себя живым:",
                    f"POST /api/v1/projects/{project_phone}/agents/{agent_phone}/whoami",
                    'Content-Type: application/json; body: {"message":"Кто я?"}',
                    (
                        "При первом запуске ответ содержит вашу полную карточку, назначенные задачи "
                        "и всю историю работы с момента создания. Повторяйте heartbeat не реже "
                        f"чем раз в {AGENT_HEARTBEAT_INTERVAL_SECONDS // 60} минут."
                    ),
                ]
            ),
            "",
            "Получать адресованные вам сообщения (опрашивайте все три очереди):",
            f"GET /worker/all/{project_phone}?to_phone={agent_phone}",
            f"GET /tester/all/{project_phone}?to_phone={agent_phone}",
            f"GET /consultant/all/{project_phone}?to_phone={agent_phone}",
            "Ответ 404 означает, что в этой очереди сейчас нет сообщения; повторите опрос позже.",
            "",
            "Отправить сообщение другому агенту этого проекта:",
            f"POST /worker/all/{project_phone} — рабочие задачи и передача результата;",
            f"POST /tester/all/{project_phone} — проверка и результаты тестирования;",
            f"POST /consultant/all/{project_phone} — вопросы и консультации.",
            "Content-Type: application/json",
            example_payload,
            "Всегда берите to_phone из адресной книги ниже; from_phone должен быть вашим телефоном.",
            "",
            "Другие агенты этого проекта:",
            *peer_lines,
            AGENT_COMMUNICATION_BLOCK_END,
        ]
    )
    communication_block = "\n".join(lines)
    return f"{authored_profile}\n\n{communication_block}" if authored_profile else communication_block


def normalize_actor_import_task(raw_task: Any, index: int) -> dict[str, Any]:
    if isinstance(raw_task, str):
        message = raw_task.strip()
        task: dict[str, Any] = {}
    elif isinstance(raw_task, dict):
        message = str(raw_task.get("message") or raw_task.get("task") or "").strip()
        task = {
            key: deepcopy(value)
            for key, value in raw_task.items()
            if key not in {"message", "task", "id"}
        }
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"actors.items[].tasks[{index}] must be a string or object",
        )
    if not message:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"actors.items[].tasks[{index}].message is required",
        )
    queue_name = str(task.get("queue") or "worker-all").strip()
    if queue_name not in GROUP_QUEUE_NAMES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "invalid_actor_task_queue",
                "queue": queue_name,
                "allowed": sorted(GROUP_QUEUE_NAMES),
            },
        )
    task_id = str(
        task.get("task_id")
        or (raw_task.get("id") if isinstance(raw_task, dict) else "")
        or f"task-{index + 1}"
    ).strip()
    if len(task_id) > 160:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Actor task id must not exceed 160 characters",
        )
    task["message"] = message
    task["queue"] = queue_name
    task["task_id"] = task_id
    return task


GRAPH_NODE_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,95}$")
GRAPH_OUTCOME_PATTERN = re.compile(r"^[A-Z][A-Z0-9_-]{0,31}$")
SEQUENTIAL_REVIEW_DECISIONS = {"APPROVE", "REJECT"}


def normalize_graph_node_id(value: Any, field_name: str) -> str:
    node_id = str(value or "").strip()
    if not GRAPH_NODE_ID_PATTERN.fullmatch(node_id):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"{field_name} must match {GRAPH_NODE_ID_PATTERN.pattern}",
        )
    return node_id


def normalize_graph_outcome(value: Any, field_name: str) -> str:
    outcome = str(value or "").strip().upper()
    if not GRAPH_OUTCOME_PATTERN.fullmatch(outcome):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"{field_name} must match {GRAPH_OUTCOME_PATTERN.pattern}",
        )
    return outcome


def sequential_graph_import_definition(payload: dict[str, Any]) -> dict[str, Any] | None:
    raw_execution = payload.get("execution")
    raw_nodes = payload.get("nodes")
    if raw_execution is None and raw_nodes is None:
        return None
    if not isinstance(raw_execution, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="execution must be an object when nodes are supplied",
        )
    if str(raw_execution.get("mode") or "sequential").strip().lower() != "sequential":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="execution.mode must be sequential for a graph import",
        )
    if not isinstance(raw_nodes, list) or not raw_nodes:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="nodes must be a non-empty array",
        )

    try:
        max_rework_cycles = int(raw_execution.get("max_rework_cycles", 5))
    except (TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="execution.max_rework_cycles must be an integer",
        ) from exc
    if not 0 <= max_rework_cycles <= 100:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="execution.max_rework_cycles must be between 0 and 100",
        )

    try:
        required_approvals = int(raw_execution.get("required_approvals", 2))
    except (TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="execution.required_approvals must be 2",
        ) from exc
    if required_approvals != 2:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Every graph transition requires exactly two reviewer approvals",
        )

    raw_reviewers = raw_execution.get("reviewers")
    if not isinstance(raw_reviewers, list) or len(raw_reviewers) != 2:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="execution.reviewers must contain exactly two reviewers",
        )

    actor_items: list[dict[str, Any]] = []
    reviewer_agent_ids: list[str] = []
    reviewer_names: list[str] = []
    for index, raw_reviewer in enumerate(raw_reviewers):
        if isinstance(raw_reviewer, str):
            reviewer = {"name": raw_reviewer}
        elif isinstance(raw_reviewer, dict):
            reviewer = deepcopy(raw_reviewer)
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"execution.reviewers[{index}] must be a string or object",
            )
        reviewer_name = str(
            reviewer.get("name") or reviewer.get("agent") or ""
        ).strip()
        if not reviewer_name:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"execution.reviewers[{index}].name is required",
            )
        reviewer_id = normalize_graph_node_id(
            reviewer.get("id") or f"transition-reviewer-{index + 1}",
            f"execution.reviewers[{index}].id",
        )
        parameters = (
            deepcopy(reviewer.get("parameters"))
            if isinstance(reviewer.get("parameters"), dict)
            else {}
        )
        parameters.update(
            {
                "workflow_role": "transition_reviewer",
                "reviewer_order": str(index + 1),
            }
        )
        actor_items.append(
            {
                **reviewer,
                "id": reviewer_id,
                "name": reviewer_name,
                "parameters": parameters,
                "tasks": [],
            }
        )
        reviewer_agent_ids.append(reviewer_id)
        reviewer_names.append(reviewer_name.casefold())
    if len(set(reviewer_agent_ids)) != 2 or len(set(reviewer_names)) != 2:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="The two transition reviewers must be different agents",
        )

    graph_nodes: list[dict[str, Any]] = []
    terminal_nodes: dict[str, dict[str, Any]] = {}
    seen_node_ids: set[str] = set()
    for index, raw_node in enumerate(raw_nodes):
        if not isinstance(raw_node, dict):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"nodes[{index}] must be an object",
            )
        node_id = normalize_graph_node_id(raw_node.get("id"), f"nodes[{index}].id")
        if node_id in seen_node_ids:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={"error": "duplicate_graph_node_id", "node_id": node_id},
            )
        seen_node_ids.add(node_id)
        node_type = str(raw_node.get("type") or "task").strip().lower()
        if node_type == "terminal":
            terminal_nodes[node_id] = {
                "id": node_id,
                "type": "terminal",
                "status": str(raw_node.get("status") or "DONE").strip().upper(),
                "message": str(raw_node.get("message") or "Workflow completed").strip(),
            }
            continue
        if node_type not in {"task", "agent"}:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"nodes[{index}].type must be task, agent, or terminal",
            )

        raw_agent = raw_node.get("agent")
        if isinstance(raw_agent, str):
            agent = {"name": raw_agent}
        elif isinstance(raw_agent, dict):
            agent = deepcopy(raw_agent)
        elif raw_agent is None:
            agent = {}
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"nodes[{index}].agent must be a string or object",
            )
        agent_id = normalize_graph_node_id(
            agent.get("id") or raw_node.get("agent_id") or node_id,
            f"nodes[{index}].agent.id",
        )
        agent_name = str(
            agent.get("name") or raw_node.get("name") or raw_agent or node_id
        ).strip()
        raw_tasks = raw_node.get("tasks")
        if raw_tasks is None and "task" in raw_node:
            raw_tasks = [raw_node.get("task")]
        if raw_tasks is None:
            raw_tasks = agent.get("tasks", [])
        if not isinstance(raw_tasks, list):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"nodes[{index}].tasks must be an array",
            )
        raw_transitions = raw_node.get("transitions")
        if not isinstance(raw_transitions, dict) or not raw_transitions:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"nodes[{index}].transitions must be a non-empty object",
            )
        transitions: dict[str, str] = {}
        for raw_outcome, raw_target in raw_transitions.items():
            outcome = normalize_graph_outcome(
                raw_outcome,
                f"nodes[{index}].transitions outcome",
            )
            target = normalize_graph_node_id(
                raw_target,
                f"nodes[{index}].transitions.{outcome}",
            )
            transitions[outcome] = target
        parameters = (
            deepcopy(agent.get("parameters"))
            if isinstance(agent.get("parameters"), dict)
            else {}
        )
        parameters.update({"workflow_role": "graph_node", "workflow_node_id": node_id})
        actor_items.append(
            {
                **agent,
                "id": agent_id,
                "name": agent_name,
                "profile": agent.get("profile") or raw_node.get("profile"),
                "phone": agent.get("phone") or raw_node.get("phone"),
                "git_branch": agent.get("git_branch") or raw_node.get("git_branch"),
                "parameters": parameters,
                "tasks": raw_tasks,
            }
        )
        graph_nodes.append(
            {
                "id": node_id,
                "agent_id": agent_id,
                "agent_name": agent_name,
                "transitions": transitions,
            }
        )

    if not graph_nodes:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="nodes must contain at least one non-terminal node",
        )
    active_node_ids = {node["id"] for node in graph_nodes}
    start_node_id = normalize_graph_node_id(
        raw_execution.get("start_node") or graph_nodes[0]["id"],
        "execution.start_node",
    )
    if start_node_id not in active_node_ids:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "unknown_graph_start_node", "node_id": start_node_id},
        )
    for node in graph_nodes:
        for outcome, target in node["transitions"].items():
            if target not in active_node_ids and target not in terminal_nodes:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail={
                        "error": "unknown_graph_transition_target",
                        "node_id": node["id"],
                        "outcome": outcome,
                        "target": target,
                    },
                )

    reachable = {start_node_id}
    pending = [start_node_id]
    nodes_by_id = {node["id"]: node for node in graph_nodes}
    while pending:
        current_id = pending.pop()
        for target in nodes_by_id[current_id]["transitions"].values():
            if target in active_node_ids and target not in reachable:
                reachable.add(target)
                pending.append(target)
    unreachable = sorted(active_node_ids - reachable)
    if unreachable:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "unreachable_graph_nodes", "nodes": unreachable},
        )

    graph_overview_lines = [
        "Вы — один из двух обязательных ревьюверов каждого перехода проекта.",
        "Переход выполняется только после двух независимых решений APPROVE.",
        "Любой REJECT возвращает исходный узел на доработку с замечанием.",
        "Полный граф проекта:",
    ]
    for node in graph_nodes:
        transition_text = ", ".join(
            f"{outcome} -> {target}"
            for outcome, target in node["transitions"].items()
        )
        graph_overview_lines.append(
            f"- {node['id']} ({node['agent_name']}): {transition_text}"
        )
    for terminal in terminal_nodes.values():
        graph_overview_lines.append(
            f"- {terminal['id']} (terminal, status={terminal['status']}): "
            f"{terminal['message']}"
        )
    graph_overview = "\n".join(graph_overview_lines)
    for item in actor_items:
        if item.get("id") not in reviewer_agent_ids:
            continue
        authored_profile = str(item.get("profile") or "").strip()
        item["profile"] = (
            f"{authored_profile}\n\n{graph_overview}"
            if authored_profile
            else graph_overview
        )

    return {
        "actor_items": actor_items,
        "workflow": {
            "enabled": True,
            "initialize_reviewers": bool(
                raw_execution.get("initialize_reviewers", False)
            ),
            "start_node_id": start_node_id,
            "max_rework_cycles": max_rework_cycles,
            "required_approvals": required_approvals,
            "reviewer_agent_ids": reviewer_agent_ids,
            "nodes": graph_nodes,
            "terminal_nodes": terminal_nodes,
        },
    }


def actor_import_options(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Expected JSON object",
        )
    graph_definition = sequential_graph_import_definition(payload)
    section_name = "agents" if "agents" in payload or graph_definition else "actors"
    raw_section = payload.get(section_name)
    if graph_definition is not None:
        if raw_section is None:
            raw_section = {}
        if not isinstance(raw_section, dict):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="agents must be an object for a graph import",
            )
        raw_items = graph_definition["actor_items"]
        overwrite = raw_section.get("overwrite", payload.get("overwrite", False))
        include_managed = raw_section.get(
            "include_managed", payload.get("include_managed", False)
        )
        assignment_mode = "sequential"
    elif isinstance(raw_section, list):
        raw_items = raw_section
        overwrite = payload.get("overwrite", False)
        include_managed = payload.get("include_managed", False)
        assignment_mode = payload.get("assignment_mode", "parallel")
    elif isinstance(raw_section, dict):
        raw_items = raw_section.get("items")
        if raw_items is None:
            raw_items = raw_section.get(section_name)
        overwrite = raw_section.get("overwrite", payload.get("overwrite", False))
        include_managed = raw_section.get(
            "include_managed",
            payload.get("include_managed", False),
        )
        assignment_mode = raw_section.get(
            "assignment_mode",
            payload.get("assignment_mode", "parallel"),
        )
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="agents (or legacy actors) must be an object with items or an array",
        )
    if not isinstance(overwrite, bool) or not isinstance(include_managed, bool):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"{section_name}.overwrite and {section_name}.include_managed must be booleans",
        )
    assignment_mode = str(assignment_mode or "parallel").strip().lower()
    if assignment_mode not in AGENT_ASSIGNMENT_MODES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "invalid_agent_assignment_mode",
                "assignment_mode": assignment_mode,
                "allowed": sorted(AGENT_ASSIGNMENT_MODES),
            },
        )
    if assignment_mode == "sequential" and not overwrite:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Sequential agent assignment requires overwrite: true",
        )
    if not isinstance(raw_items, list):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"{section_name}.items must be an array",
        )
    if len(raw_items) > 200:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail="An actor import can contain at most 200 actors",
        )

    actors: list[dict[str, Any]] = []
    seen_names: set[str] = set()
    seen_ids: set[str] = set()
    seen_phones: set[str] = set()
    seen_branches: set[str] = set()
    task_count = 0
    for actor_index, raw_actor in enumerate(raw_items):
        if not isinstance(raw_actor, dict):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"actors.items[{actor_index}] must be an object",
            )
        name = str(raw_actor.get("name") or "").strip()
        if not name:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"actors.items[{actor_index}].name is required",
            )
        name_key = name.casefold()
        if name_key in seen_names:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={"error": "duplicate_actor_name", "name": name},
            )
        seen_names.add(name_key)

        actor_id = str(raw_actor.get("id") or "").strip()
        if actor_id and actor_id in seen_ids:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={"error": "duplicate_actor_id", "actor_id": actor_id},
            )
        if actor_id:
            seen_ids.add(actor_id)

        phone = str(raw_actor.get("phone") or "").strip()
        if phone and not (
            phone.isdigit()
            and len(phone) == 4
            and 1000 <= int(phone) <= GROUP_AGENT_PHONE_MAX
        ):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "error": "invalid_actor_phone",
                    "phone": phone,
                    "message": "Actor phone must be a four-digit number from 1000 to 8999",
                },
            )
        if phone and phone in seen_phones:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={"error": "duplicate_actor_phone", "phone": phone},
            )
        if phone:
            seen_phones.add(phone)

        raw_tasks = raw_actor.get("tasks", [])
        if not isinstance(raw_tasks, list):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"actors.items[{actor_index}].tasks must be an array",
            )
        tasks = [
            normalize_actor_import_task(raw_task, task_index)
            for task_index, raw_task in enumerate(raw_tasks)
        ]
        task_count += len(tasks)
        if task_count > 1000:
            raise HTTPException(
                status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                detail="An actor import can contain at most 1000 tasks",
            )
        raw_parameters = normalize_agent_parameters(raw_actor.get("parameters"))
        git_branch = normalized_agent_git_branch(
            raw_actor.get("git_branch") or raw_parameters.get("git_branch"),
            f"{section_name}.items[{actor_index}].git_branch",
        )
        if git_branch:
            branch_key = git_branch.casefold()
            if branch_key in seen_branches:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={"error": "duplicate_agent_git_branch", "git_branch": git_branch},
                )
            seen_branches.add(branch_key)
        actors.append(
            {
                "id": actor_id,
                "name": name,
                "phone": phone,
                "profile": str(raw_actor.get("profile") or "").strip(),
                "parameters": raw_parameters,
                "git_branch": git_branch,
                "template_source": str(
                    raw_actor.get("template_source") or "json-import"
                ).strip(),
                "status": str(raw_actor.get("status") or "active").strip(),
                "tasks": tasks,
            }
        )
    return {
        "overwrite": overwrite,
        "include_managed": include_managed,
        "assignment_mode": assignment_mode,
        "actors": actors,
        "task_count": task_count,
        "workflow": deepcopy(graph_definition["workflow"]) if graph_definition else None,
    }


def allocate_imported_actor_phone(occupied: set[str]) -> str:
    for number in range(IMPORTED_ACTOR_PHONE_MIN, IMPORTED_ACTOR_PHONE_MAX + 1):
        phone = str(number)
        if phone not in occupied:
            occupied.add(phone)
            return phone
    raise HTTPException(
        status_code=status.HTTP_409_CONFLICT,
        detail={
            "error": "no_free_imported_actor_phone",
            "range": f"{IMPORTED_ACTOR_PHONE_MIN}-{IMPORTED_ACTOR_PHONE_MAX}",
        },
    )


def project_actor_mutation_transaction(
    project_id: str,
    options: dict[str, Any],
    delete_all: bool = False,
) -> dict[str, Any]:
    with git_config_file_lock():
        with agents_file_lock():
            config = read_git_config_file()
            raw_key, context_key, project_entry, context = project_for_group_api(
                config,
                project_id,
            )
            expected_context_key = normalize_project_context_reference(
                options.get("expected_git_context_key")
            )
            if expected_context_key and context_key != expected_context_key:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "error": "project_context_changed",
                        "message": (
                            "The project Git context changed after the JSON was "
                            "validated; import was cancelled"
                        ),
                        "expected_git_context_key": expected_context_key,
                        "actual_git_context_key": context_key,
                    },
                )
            expected_repository_key = str(
                options.get("expected_repository_key") or ""
            ).strip()
            actual_repository_key = project_repository_key_for_context(context)
            if (
                expected_repository_key
                and actual_repository_key != expected_repository_key
            ):
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "error": "project_repository_changed",
                        "message": (
                            "The project Git repository changed after the JSON was "
                            "validated; import was cancelled"
                        ),
                        "expected_repository_key": expected_repository_key,
                        "actual_repository_key": actual_repository_key,
                    },
                )
            project_phone = normalize_project_phone(project_entry.get("project_phone"))
            phone_contexts = phone_git_contexts_from_config(config)
            previous_agents = read_agents_file()
            project_agents = full_agents_for_project(
                previous_agents,
                context_key,
                phone_contexts,
            )
            project_by_id = {
                str(agent.get("id") or "").strip(): agent
                for agent in project_agents
            }
            project_by_name = {
                str(agent.get("name") or "").strip().casefold(): agent
                for agent in project_agents
            }
            overwrite = bool(options.get("overwrite")) or delete_all
            include_managed = bool(options.get("include_managed"))
            assignment_mode = str(
                options.get("assignment_mode") or "parallel"
            ).strip().lower()
            imported_specs = [] if delete_all else list(options.get("actors") or [])

            matched_by_spec: dict[int, dict[str, Any]] = {}
            replacement_ids: set[str] = set()
            for index, spec in enumerate(imported_specs):
                candidate_id = str(spec.get("id") or "").strip()
                candidate_name = str(spec.get("name") or "").strip().casefold()
                by_id = project_by_id.get(candidate_id) if candidate_id else None
                by_name = project_by_name.get(candidate_name)
                if by_id is not None and by_name is not None and by_id is not by_name:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "error": "actor_identity_conflict",
                            "actor_id": candidate_id,
                            "name": spec.get("name"),
                        },
                    )
                matched = by_id or by_name
                if matched is not None:
                    if is_group_managed_agent(matched) and not (
                        overwrite and include_managed
                    ):
                        raise HTTPException(
                            status_code=status.HTTP_409_CONFLICT,
                            detail={
                                "error": "group_managed_actor_is_immutable",
                                "actor_id": matched.get("id"),
                            },
                        )
                    matched_by_spec[index] = matched
                    replacement_ids.add(str(matched.get("id") or "").strip())

            removable_agents = [
                agent
                for agent in project_agents
                if include_managed or not is_group_managed_agent(agent)
            ]
            if overwrite:
                replacement_ids.update(
                    str(agent.get("id") or "").strip()
                    for agent in removable_agents
                )
            removed_agents = [
                deepcopy(agent)
                for agent in project_agents
                if str(agent.get("id") or "").strip() in replacement_ids
            ]
            retained_agents = [
                deepcopy(agent)
                for agent in previous_agents
                if str(agent.get("id") or "").strip() not in replacement_ids
            ]

            removed_ids = {
                str(agent.get("id") or "").strip()
                for agent in removed_agents
            }
            removed_phones = {
                str(agent.get("phone") or "").strip()
                for agent in removed_agents
                if str(agent.get("phone") or "").strip()
            }
            phone_map_raw = config.get(PHONE_GIT_CONTEXTS_KEY)
            phone_map = dict(phone_map_raw) if isinstance(phone_map_raw, dict) else {}
            config_changed = False
            if include_managed and removed_ids:
                for phone, raw_mapping in list(phone_map.items()):
                    mapping = raw_mapping if isinstance(raw_mapping, dict) else {}
                    if (
                        str(mapping.get("agent_id") or "").strip() in removed_ids
                        and str(mapping.get("managed_by") or "").strip() == "group_api"
                    ):
                        phone_map.pop(phone, None)
                        config_changed = True
                if config_changed:
                    config[PHONE_GIT_CONTEXTS_KEY] = phone_map

                raw_groups = project_entry.get("groups")
                groups = [
                    deepcopy(group)
                    for group in raw_groups
                    if isinstance(group, dict)
                ] if isinstance(raw_groups, list) else []
                timestamp = utc_now()
                groups_changed = False
                for group in groups:
                    if str(group.get("status") or "active") == "archived":
                        continue
                    group["status"] = "archived"
                    group["revision"] = int(group.get("revision") or 1) + 1
                    group["updated_at"] = timestamp
                    group["archived_at"] = timestamp
                    groups_changed = True
                if groups_changed:
                    project_entry["groups"] = groups
                    project_entry["updated_at"] = timestamp
                    refresh_project_group_relationships(
                        project_entry,
                        read_group_templates_file(),
                    )
                    raw_projects = config.get(PROJECTS_KEY)
                    projects = dict(raw_projects) if isinstance(raw_projects, dict) else {}
                    projects[raw_key] = project_entry
                    config[PROJECTS_KEY] = projects
                    config_changed = True

            occupied_ids = {
                str(agent.get("id") or "").strip()
                for agent in retained_agents
                if str(agent.get("id") or "").strip()
            }
            occupied_names = {
                str(agent.get("name") or "").strip().casefold()
                for agent in retained_agents
                if str(agent.get("name") or "").strip()
            }
            occupied_phones = {
                str(agent.get("phone") or "").strip()
                for agent in retained_agents
                if str(agent.get("phone") or "").strip()
            }
            occupied_phones.update(str(phone).strip() for phone in phone_map)
            occupied_phones.update(
                phone
                for entry in project_registry_from_config(config).values()
                if (phone := normalize_project_phone(entry.get("project_phone")))
            )
            occupied_phones.update(DEFAULT_AGENT_PHONES.values())
            occupied_phones.add(PROJECT_MANAGER_PHONE)

            retained_project_branches = {
                str(
                    agent.get("git_branch")
                    or (
                        agent.get("parameters", {}).get("git_branch")
                        if isinstance(agent.get("parameters"), dict)
                        else ""
                    )
                    or ""
                ).strip().casefold(): str(agent.get("id") or "").strip()
                for agent in project_agents
                if str(agent.get("id") or "").strip() not in replacement_ids
                and str(
                    agent.get("git_branch")
                    or (
                        agent.get("parameters", {}).get("git_branch")
                        if isinstance(agent.get("parameters"), dict)
                        else ""
                    )
                    or ""
                ).strip()
            }

            imported_agents: list[dict[str, Any]] = []
            import_timestamp = utc_now()
            for index, spec in enumerate(imported_specs):
                matched = matched_by_spec.get(index)
                actor_id = str(spec.get("id") or "").strip()
                if not actor_id and matched is not None:
                    actor_id = str(matched.get("id") or "").strip()
                actor_id = actor_id or str(uuid4())
                name = str(spec.get("name") or "").strip()
                name_key = name.casefold()
                if actor_id in occupied_ids:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={"error": "actor_id_conflict", "actor_id": actor_id},
                    )
                if name_key in occupied_names:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={"error": "actor_name_conflict", "name": name},
                    )

                phone = str(spec.get("phone") or "").strip()
                matched_phone = str((matched or {}).get("phone") or "").strip()
                if not phone and matched_phone and matched_phone not in occupied_phones:
                    phone = matched_phone
                if not phone:
                    phone = allocate_imported_actor_phone(occupied_phones)
                elif phone in occupied_phones:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={"error": "actor_phone_conflict", "phone": phone},
                    )
                else:
                    occupied_phones.add(phone)

                parameters = dict(spec.get("parameters") or {})
                matched_parameters = (
                    matched.get("parameters")
                    if matched is not None and isinstance(matched.get("parameters"), dict)
                    else {}
                )
                preserved_presence_keys = (
                    ("created_at",)
                    if assignment_mode == "sequential"
                    else (
                        "created_at",
                        "first_seen_at",
                        "last_seen_at",
                        "alive_until",
                        "heartbeat_count",
                        "presence_status",
                    )
                )
                for presence_key in preserved_presence_keys:
                    if not parameters.get(presence_key) and matched_parameters.get(
                        presence_key
                    ):
                        parameters[presence_key] = matched_parameters[presence_key]
                if assignment_mode == "sequential":
                    for reset_key in (
                        "first_seen_at",
                        "last_seen_at",
                        "alive_until",
                        "heartbeat_count",
                        "presence_status",
                        "assignment_status",
                        "assignment_completed_at",
                        "current_assignment_id",
                    ):
                        parameters.pop(reset_key, None)
                created_at = str(
                    parameters.get("created_at") or import_timestamp
                ).strip()
                git_branch = normalized_agent_git_branch(
                    spec.get("git_branch")
                    or parameters.get("git_branch")
                    or (matched or {}).get("git_branch")
                    or matched_parameters.get("git_branch")
                    or default_imported_agent_branch(actor_id, name),
                    f"agents.items[{index}].git_branch",
                )
                branch_owner = retained_project_branches.get(git_branch.casefold())
                if branch_owner and branch_owner != actor_id:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "error": "agent_git_branch_conflict",
                            "git_branch": git_branch,
                            "agent_id": branch_owner,
                        },
                    )
                retained_project_branches[git_branch.casefold()] = actor_id
                parameters.update(
                    {
                        "git_context_key": context_key,
                        "git_context_keys": context_key,
                        "project_phone": project_phone,
                        "conversation_phone": project_phone,
                        "agent_phone": phone,
                        "git_branch": git_branch,
                        "assignment_order": str(index + 1),
                        "assignment_mode": assignment_mode,
                        "created_at": created_at,
                        "imported_at": import_timestamp,
                        "profile_endpoint": (
                            f"/api/v1/projects/{project_phone}/agents/{phone}"
                        ),
                        "whoami_endpoint": (
                            f"/api/v1/projects/{project_phone}/agents/{phone}/whoami"
                        ),
                        "heartbeat_interval_seconds": str(
                            AGENT_HEARTBEAT_INTERVAL_SECONDS
                        ),
                        "worker_receive_endpoint": (
                            f"/worker/all/{project_phone}?to_phone={phone}"
                        ),
                        "worker_send_endpoint": f"/worker/all/{project_phone}",
                        "tester_receive_endpoint": (
                            f"/tester/all/{project_phone}?to_phone={phone}"
                        ),
                        "tester_send_endpoint": f"/tester/all/{project_phone}",
                        "consultant_receive_endpoint": (
                            f"/consultant/all/{project_phone}?to_phone={phone}"
                        ),
                        "consultant_send_endpoint": f"/consultant/all/{project_phone}",
                        "communication_instructions_version": AGENT_COMMUNICATION_VERSION,
                        "imported_from": str(options.get("source") or "json"),
                    }
                )
                agent = {
                    "id": actor_id,
                    "name": name,
                    "phone": phone,
                    "profile": profile_without_agent_communication_block(
                        spec.get("profile")
                    ),
                    "git_branch": git_branch,
                    "parameters": normalize_agent_parameters(parameters),
                    "template_source": str(
                        spec.get("template_source") or "json-import"
                    ).strip(),
                    "status": str(spec.get("status") or "active").strip(),
                    "tasks": deepcopy(spec.get("tasks") or []),
                }
                normalized = normalize_agent(agent)
                if normalized is None:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Actor '{name}' is invalid",
                    )
                imported_agents.append(normalized)
                occupied_ids.add(actor_id)
                occupied_names.add(name_key)

            final_agents = normalize_agents(retained_agents + imported_agents)
            project_agents_for_profiles = full_agents_for_project(
                final_agents,
                context_key,
                phone_git_contexts_from_config(config),
            )
            for imported_agent in imported_agents:
                imported_agent["profile"] = agent_communication_profile(
                    imported_agent.get("profile"),
                    imported_agent,
                    project_phone,
                    project_entry,
                    project_agents_for_profiles,
                    assignment_mode,
                )
            final_agents = normalize_agents(retained_agents + imported_agents)
            assignment_timestamp = utc_now()
            workflow = (
                deepcopy(options.get("workflow"))
                if isinstance(options.get("workflow"), dict)
                else None
            )
            graph_role_agent_ids = [
                str(node.get("agent_id") or "").strip()
                for node in (workflow or {}).get("nodes", [])
                if isinstance(node, dict) and str(node.get("agent_id") or "").strip()
            ]
            assignment_state = {
                "mode": assignment_mode,
                "strategy": (
                    "conditional_graph"
                    if workflow
                    else ("queue_graph" if assignment_mode == "sequential" else "parallel")
                ),
                "status": (
                    "ready"
                    if assignment_mode == "sequential" and imported_agents
                    else ("completed" if assignment_mode == "sequential" else "parallel")
                ),
                "revision": 1,
                "created_at": assignment_timestamp,
                "updated_at": assignment_timestamp,
                "current_agent_id": None,
                "current_node_id": None,
                "phase": "node",
                "current_started_at": None,
                "completed_agent_ids": [],
                "assignments": [],
                "role_agent_ids": graph_role_agent_ids or [
                    str(agent.get("id") or "").strip() for agent in imported_agents
                ],
                "workflow": workflow,
                "pending_transition": None,
                "rework_cycle_count": 0,
                "visit_counts": {},
            }
            project_entry[PROJECT_AGENT_ASSIGNMENT_KEY] = assignment_state
            project_entry["updated_at"] = assignment_timestamp
            raw_projects = config.get(PROJECTS_KEY)
            projects = dict(raw_projects) if isinstance(raw_projects, dict) else {}
            projects[raw_key] = project_entry
            config[PROJECTS_KEY] = projects
            config_changed = True
            write_agents_file_unlocked(final_agents)
            if config_changed:
                try:
                    write_git_config_file(config)
                except Exception:
                    write_agents_file_unlocked(previous_agents)
                    raise

            public_context = public_project_context(
                configured_git_context_for_key(config, context_key) or context
            )
            queue_context = {
                "queue_phone": project_phone,
                "git_context_phone": project_phone,
                "project_phone": project_phone,
                "project_name": project_entry.get("project_name"),
                "git_context_key": context_key,
                "git_address": project_entry.get("git_address"),
            }
            return {
                "project_id": project_phone,
                "project_phone": project_phone,
                "project": public_context,
                "overwrite": overwrite,
                "include_managed": include_managed,
                "assignment_mode": assignment_mode,
                "assignment": deepcopy(assignment_state),
                "removed_agents": removed_agents,
                "removed_actor_ids": sorted(removed_ids),
                "removed_actor_phones": sorted(removed_phones),
                "imported_agents": deepcopy(imported_agents),
                "agents": full_agents_for_project(
                    final_agents,
                    context_key,
                    phone_git_contexts_from_config(config),
                ),
                "queue_context": queue_context,
            }


GROUP_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,95}$")


def read_group_templates_file() -> dict[str, Any]:
    if not group_templates_path.exists():
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Group template registry was not found: {group_templates_path}",
        )
    try:
        with group_templates_path.open("r", encoding="utf-8") as file:
            data = json.load(file)
    except (OSError, json.JSONDecodeError) as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Group template registry is invalid: {exc}",
        ) from exc
    if not isinstance(data, dict):
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Group template registry must contain a JSON object",
        )
    for key in ("agent_specs", "group_templates"):
        if not isinstance(data.get(key), dict):
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Group template registry field '{key}' must be an object",
            )
    if not isinstance(data.get("group_topologies", {}), dict):
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Group template registry field 'group_topologies' must be an object",
        )
    return data


def normalized_group_id(value: Any) -> str:
    group_id = str(value or "").strip()
    if not group_id or not GROUP_ID_PATTERN.fullmatch(group_id):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "group_id must contain 1-96 letters, digits, dots, underscores, "
                "or hyphens and start with a letter or digit"
            ),
        )
    return group_id


def stable_group_id(context_key: str, template_id: str, group_name: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9]+", "-", template_id).strip("-").lower()
    slug = slug[:36] or "group"
    digest = hashlib.sha256(
        f"{context_key}\0{template_id}\0{group_name.casefold()}".encode("utf-8")
    ).hexdigest()[:12]
    return f"group-{slug}-{digest}"


def raw_project_registry_entry_for_context(
    config: dict[str, Any],
    context_key: str,
) -> tuple[str, dict[str, Any]] | None:
    raw_projects = config.get(PROJECTS_KEY)
    if not isinstance(raw_projects, dict):
        return None
    for raw_key, raw_entry in raw_projects.items():
        if not isinstance(raw_entry, dict):
            continue
        candidate = dict(raw_entry)
        candidate.setdefault("git_context_key", str(raw_key))
        normalized = normalize_git_context_config_entry(candidate)
        if normalized is not None and normalized.get("git_context_key") == context_key:
            return str(raw_key), dict(raw_entry)
    return None


def project_for_group_api(
    config: dict[str, Any],
    project_id: str,
) -> tuple[str, str, dict[str, Any], dict[str, Any]]:
    clean_project_id = urllib.parse.unquote(str(project_id or "").strip())
    project_phone = normalize_project_phone(clean_project_id)
    if not project_phone:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "invalid_project_id",
                "message": (
                    "project_id must be the canonical project_phone returned by "
                    "Project Manager 0001"
                ),
            },
        )
    context_key = canonical_project_context_key_for_phone(config, project_phone)
    if not context_key:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "project_not_found",
                "message": f"Canonical project phone {project_phone} was not found",
            },
        )

    located = raw_project_registry_entry_for_context(config, context_key)
    if located is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "project_not_found",
                "message": f"Project {clean_project_id} was not found",
            },
        )
    raw_key, project_entry = located
    canonical_phone = normalize_project_phone(project_entry.get("project_phone"))
    if not canonical_phone:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "project_phone_required",
                "message": "Resolve the project through Project Manager 0001 first",
            },
        )
    context = configured_git_context_for_key(config, context_key)
    if context is None:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Project registry entry could not be normalized",
        )
    return raw_key, context_key, project_entry, context


def group_template_agent_definitions(
    registry: dict[str, Any],
    template: dict[str, Any],
    requested_roles: list[str] | None = None,
) -> list[dict[str, Any]]:
    raw_definitions = template.get("agent_templates")
    if not isinstance(raw_definitions, list) or not raw_definitions:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Group template must define a non-empty agent_templates list",
        )
    specs = registry["agent_specs"]
    definitions: list[dict[str, Any]] = []
    seen_roles: set[str] = set()
    for raw_definition in raw_definitions:
        if not isinstance(raw_definition, dict):
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Each group agent template must be an object",
            )
        role = str(raw_definition.get("role") or "").strip()
        spec_id = str(raw_definition.get("spec") or "").strip()
        if not role or not GROUP_ID_PATTERN.fullmatch(role):
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Invalid group role: {role or '<empty>'}",
            )
        if role in seen_roles:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Duplicate group role: {role}",
            )
        if spec_id not in specs or not isinstance(specs[spec_id], dict):
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Unknown agent spec '{spec_id}' for role '{role}'",
            )
        seen_roles.add(role)
        definitions.append(
            {
                "role": role,
                "spec": spec_id,
                "is_entrypoint": bool(raw_definition.get("is_entrypoint")),
            }
        )

    if requested_roles is None:
        return definitions
    clean_roles = [str(role or "").strip() for role in requested_roles]
    if not clean_roles or any(not role for role in clean_roles):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="task_template.agents must contain at least one role",
        )
    if len(set(clean_roles)) != len(clean_roles):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="task_template.agents contains duplicate roles",
        )
    unknown = sorted(set(clean_roles) - seen_roles)
    if unknown:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "unknown_group_agent_role",
                "roles": unknown,
            },
        )
    selected = set(clean_roles)
    return [definition for definition in definitions if definition["role"] in selected]


def normalize_group_connections(
    raw_connections: Any,
    allowed_roles: set[str],
) -> list[dict[str, str]]:
    if raw_connections is None:
        return []
    if not isinstance(raw_connections, list):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="connections must be a list",
        )
    connections: list[dict[str, str]] = []
    seen_ids: set[str] = set()
    for index, raw_connection in enumerate(raw_connections):
        if not isinstance(raw_connection, dict):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"connections[{index}] must be an object",
            )
        from_role = str(
            raw_connection.get("from") or raw_connection.get("from_role") or ""
        ).strip()
        to_role = str(
            raw_connection.get("to") or raw_connection.get("to_role") or ""
        ).strip()
        queue_name = str(raw_connection.get("queue") or "worker-all").strip()
        event = str(raw_connection.get("event") or "task").strip()
        channel_type = str(
            raw_connection.get("channel_type") or "queue"
        ).strip()
        connection_id = str(raw_connection.get("id") or "").strip()
        if not connection_id:
            connection_id = f"{from_role}-to-{to_role}-{event}"
        normalized_group_id(connection_id)
        if connection_id in seen_ids:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Duplicate connection id: {connection_id}",
            )
        if from_role not in allowed_roles or to_role not in allowed_roles:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "error": "connection_role_not_in_group",
                    "connection_id": connection_id,
                },
            )
        if queue_name not in GROUP_QUEUE_NAMES:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "error": "unsupported_group_queue",
                    "queue": queue_name,
                },
            )
        if channel_type != "queue":
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "error": "unsupported_group_channel_type",
                    "channel_type": channel_type,
                },
            )
        if not event:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Connection event is empty: {connection_id}",
            )
        seen_ids.add(connection_id)
        connections.append(
            {
                "id": connection_id,
                "from_role": from_role,
                "to_role": to_role,
                "channel_type": "queue",
                "queue": queue_name,
                "event": event,
            }
        )
    return connections


def group_queue_route(queue_name: str, project_phone: str) -> str:
    route = QUEUE_DEFINITIONS[queue_name]["route"]
    if "{conversation_phone}" in route:
        return route.replace("{conversation_phone}", project_phone)
    return f"{route}/{project_phone}"


def expand_group_connections(
    connections: list[dict[str, str]],
    role_bindings: dict[str, str],
    agents_by_id: dict[str, dict[str, Any]],
    project_phone: str,
    group_id: str,
) -> list[dict[str, Any]]:
    expanded: list[dict[str, Any]] = []
    for connection in connections:
        from_agent_id = role_bindings[connection["from_role"]]
        to_agent_id = role_bindings[connection["to_role"]]
        from_agent = agents_by_id[from_agent_id]
        to_agent = agents_by_id[to_agent_id]
        route = group_queue_route(connection["queue"], project_phone)
        to_phone = str(to_agent.get("phone") or "").strip()
        expanded.append(
            {
                **connection,
                "group_id": group_id,
                "from_agent_id": from_agent_id,
                "from_phone": str(from_agent.get("phone") or "").strip(),
                "to_agent_id": to_agent_id,
                "to_phone": to_phone,
                "post_endpoint": route,
                "poll_endpoint": (
                    f"{route}?to_phone={urllib.parse.quote(to_phone, safe='')}"
                ),
                "send_endpoint": (
                    f"/api/v1/groups/{urllib.parse.quote(group_id, safe='')}"
                    f"/connections/{urllib.parse.quote(connection['id'], safe='')}/tasks"
                ),
                "receive_endpoint": (
                    f"/api/v1/groups/{urllib.parse.quote(group_id, safe='')}"
                    f"/agents/{urllib.parse.quote(to_agent_id, safe='')}/tasks"
                ),
                "allowed_actions": ["post", "poll"],
            }
        )
    return expanded


def load_group_agent_profile(spec_id: str, spec: dict[str, Any]) -> tuple[str, str]:
    inline_profile = str(spec.get("profile") or "").strip()
    profile_path_value = str(spec.get("profile_path") or "").strip()
    if bool(inline_profile) == bool(profile_path_value):
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=(
                f"Agent spec '{spec_id}' must define exactly one of profile "
                "or profile_path"
            ),
        )
    if inline_profile:
        return inline_profile, f"group_templates:{spec_id}"
    resolved_path = (base_dir / profile_path_value).resolve()
    resolved_prompts = (base_dir / "prompts").resolve()
    if resolved_path == resolved_prompts or resolved_prompts not in resolved_path.parents:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Agent spec '{spec_id}' profile_path must stay inside prompts/",
        )
    try:
        profile = resolved_path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Could not read agent spec '{spec_id}': {exc}",
        ) from exc
    if not profile:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Agent spec '{spec_id}' profile is empty",
        )
    return profile, profile_path_value


def canonical_json_fingerprint(value: Any) -> str:
    serialized = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return f"sha256:{hashlib.sha256(serialized.encode('utf-8')).hexdigest()}"


def group_template_for_id(
    registry: dict[str, Any],
    template_id: Any,
) -> tuple[str, dict[str, Any]]:
    clean_template_id = str(template_id or "").strip()
    if not clean_template_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="template_id is required",
        )
    raw_template = registry["group_templates"].get(clean_template_id)
    if not isinstance(raw_template, dict):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "group_template_not_found",
                "template_id": clean_template_id,
            },
        )
    declared_template_id = str(raw_template.get("template_id") or clean_template_id).strip()
    if declared_template_id != clean_template_id:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=(
                f"Group template registry key '{clean_template_id}' does not match "
                f"template_id '{declared_template_id}'"
            ),
        )
    return clean_template_id, deepcopy(raw_template)


def normalize_requested_group_roles(raw_roles: Any) -> list[str] | None:
    if raw_roles is None:
        return None
    if not isinstance(raw_roles, list):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="The group agent list must be an array of roles",
        )
    roles: list[str] = []
    for index, raw_role in enumerate(raw_roles):
        if isinstance(raw_role, dict):
            role = str(raw_role.get("role") or "").strip()
        else:
            role = str(raw_role or "").strip()
        if not role:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"agents[{index}] must identify a role",
            )
        roles.append(role)
    return roles


def requested_group_roles_from_payload(payload: dict[str, Any]) -> list[str] | None:
    enabled_roles = normalize_requested_group_roles(payload.get("enabled_roles"))
    task_template = payload.get("task_template")
    if task_template is not None and not isinstance(task_template, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="task_template must be an object",
        )
    task_roles = (
        normalize_requested_group_roles(task_template.get("agents"))
        if isinstance(task_template, dict) and "agents" in task_template
        else None
    )
    if enabled_roles is not None and task_roles is not None:
        if set(enabled_roles) != set(task_roles):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="enabled_roles and task_template.agents must select the same roles",
            )
        return enabled_roles
    return enabled_roles if enabled_roles is not None else task_roles


def base_group_connection(connection: dict[str, Any]) -> dict[str, str]:
    return {
        "id": str(connection.get("id") or "").strip(),
        "from_role": str(
            connection.get("from_role") or connection.get("from") or ""
        ).strip(),
        "to_role": str(
            connection.get("to_role") or connection.get("to") or ""
        ).strip(),
        "channel_type": "queue",
        "queue": str(connection.get("queue") or "worker-all").strip(),
        "event": str(connection.get("event") or "task").strip(),
    }


def validate_group_reporting_rule(
    raw_rule: Any,
    allowed_roles: set[str],
) -> dict[str, Any]:
    if raw_rule is None:
        return {}
    if not isinstance(raw_rule, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="reporting_rule must be an object",
        )
    rule = deepcopy(raw_rule)
    report_from = str(rule.get("report_from") or "").strip()
    if report_from and report_from not in allowed_roles:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "reporting_role_not_in_group",
                "role": report_from,
            },
        )
    output_queue = str(rule.get("output_queue") or "").strip()
    if output_queue and output_queue not in GROUP_QUEUE_NAMES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "unsupported_group_queue",
                "queue": output_queue,
            },
        )
    return rule


def validate_group_graph(
    definitions: list[dict[str, Any]],
    connections: list[dict[str, str]],
) -> str:
    entrypoints = [
        definition["role"]
        for definition in definitions
        if definition.get("is_entrypoint")
    ]
    if len(entrypoints) != 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "group_entrypoint_invalid",
                "message": "A deployed group must contain exactly one entrypoint",
                "entrypoints": entrypoints,
            },
        )

    entrypoint = entrypoints[0]
    reachable = {entrypoint}
    while True:
        expanded = {
            connection["to_role"]
            for connection in connections
            if connection["from_role"] in reachable
        }
        next_reachable = reachable | expanded
        if next_reachable == reachable:
            break
        reachable = next_reachable
    all_roles = {definition["role"] for definition in definitions}
    unreachable = sorted(all_roles - reachable)
    if unreachable:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "group_agents_unreachable",
                "entrypoint": entrypoint,
                "roles": unreachable,
            },
        )
    return entrypoint


def group_blueprint_from_payload(
    registry: dict[str, Any],
    payload: dict[str, Any],
    *,
    current_group: dict[str, Any] | None = None,
) -> dict[str, Any]:
    template_value = payload.get("template_id")
    if template_value is None and current_group is not None:
        template_value = current_group.get("template_id")
    template_id, template = group_template_for_id(registry, template_value)

    if current_group is not None and template_id != current_group.get("template_id"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="template_id is immutable; create another group for another template",
        )

    requested_roles = requested_group_roles_from_payload(payload)
    if requested_roles is None and current_group is not None:
        requested_roles = [
            str(binding.get("role") or "").strip()
            for binding in current_group.get("agents", [])
            if isinstance(binding, dict) and str(binding.get("role") or "").strip()
        ]
    all_definitions = group_template_agent_definitions(
        registry,
        template,
    )
    definitions = group_template_agent_definitions(
        registry,
        template,
        requested_roles,
    )
    allowed_roles = {definition["role"] for definition in definitions}
    all_roles = {definition["role"] for definition in all_definitions}
    full_connections = normalize_group_connections(
        template.get("internal_connections", []),
        all_roles,
    )
    validate_group_graph(all_definitions, full_connections)
    full_reporting_rule = validate_group_reporting_rule(
        template.get("reporting_rule", {}),
        all_roles,
    )
    current_roles = {
        str(binding.get("role") or "").strip()
        for binding in (current_group or {}).get("agents", [])
        if isinstance(binding, dict) and str(binding.get("role") or "").strip()
    }
    roles_were_reselected = (
        current_group is not None
        and requested_roles is not None
        and current_roles != allowed_roles
    )

    if "connections" in payload:
        raw_connections = payload.get("connections")
    elif payload.get("custom_connections"):
        raw_connections = payload.get("custom_connections")
    elif current_group is not None and not roles_were_reselected:
        raw_connections = [
            base_group_connection(connection)
            for connection in current_group.get("connections", [])
            if isinstance(connection, dict)
            and connection.get("from_role") in allowed_roles
            and connection.get("to_role") in allowed_roles
        ]
    else:
        raw_connections = [
            connection
            for connection in template.get("internal_connections", [])
            if isinstance(connection, dict)
            and str(connection.get("from") or connection.get("from_role") or "").strip()
            in allowed_roles
            and str(connection.get("to") or connection.get("to_role") or "").strip()
            in allowed_roles
        ]
    connections = normalize_group_connections(raw_connections, allowed_roles)
    entrypoint = validate_group_graph(definitions, connections)

    if "reporting_rule" in payload:
        raw_reporting_rule = payload.get("reporting_rule")
    elif current_group is not None:
        raw_reporting_rule = current_group.get("reporting_rule", {})
    else:
        raw_reporting_rule = template.get("reporting_rule", {})
    reporting_rule = validate_group_reporting_rule(
        raw_reporting_rule,
        allowed_roles,
    )

    raw_group_name = payload.get("group_name")
    if raw_group_name is None:
        raw_group_name = payload.get("name")
    if raw_group_name is None and current_group is not None:
        raw_group_name = current_group.get("group_name")
    group_name = str(raw_group_name or template.get("name") or template_id).strip()
    if not group_name or len(group_name) > 160:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="group_name must contain 1-160 characters",
        )

    raw_group_key = payload.get("group_key")
    if raw_group_key is None and current_group is not None:
        raw_group_key = current_group.get("group_key")
    if raw_group_key is None:
        raw_group_key = re.sub(r"[^A-Za-z0-9._-]+", "-", group_name).strip("-").lower()
    group_key = normalized_group_id(raw_group_key)
    description = str(
        payload.get("description")
        if payload.get("description") is not None
        else (
            current_group.get("description")
            if current_group is not None
            else template.get("description") or ""
        )
    ).strip()
    if "task_template" in payload and isinstance(payload.get("task_template"), dict):
        task_template = deepcopy(payload["task_template"])
    elif current_group is not None and isinstance(current_group.get("task_template"), dict):
        task_template = deepcopy(current_group["task_template"])
    else:
        task_template = {}
    if isinstance(task_template.get("agents"), list):
        task_template["agents"] = sorted(
            normalize_requested_group_roles(task_template["agents"]) or []
        )

    all_spec_snapshots: dict[str, dict[str, Any]] = {}
    for definition in all_definitions:
        spec_id = definition["spec"]
        if spec_id in all_spec_snapshots:
            continue
        spec = registry["agent_specs"][spec_id]
        spec_name = str(spec.get("name") or "").strip()
        if not spec_name:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Agent spec '{spec_id}' must define a non-empty name",
            )
        if "parameters" in spec and not isinstance(spec.get("parameters"), dict):
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Agent spec '{spec_id}' parameters must be an object",
            )
        profile, template_source = load_group_agent_profile(spec_id, spec)
        parameters = normalize_agent_parameters(spec.get("parameters"))
        snapshot = {
            "spec": spec_id,
            "name": spec_name,
            "profile": profile,
            "parameters": parameters,
            "template_source": template_source,
            "status": str(spec.get("status") or "active").strip() or "active",
        }
        snapshot["fingerprint"] = canonical_json_fingerprint(snapshot)
        all_spec_snapshots[spec_id] = snapshot

    selected_spec_ids = {definition["spec"] for definition in definitions}
    spec_snapshots = {
        spec_id: snapshot
        for spec_id, snapshot in all_spec_snapshots.items()
        if spec_id in selected_spec_ids
    }

    template_snapshot = {
        "template_id": template_id,
        "name": str(template.get("name") or template_id),
        "description": str(template.get("description") or ""),
        "agent_templates": all_definitions,
        "connections": sorted(full_connections, key=lambda item: item["id"]),
        "reporting_rule": full_reporting_rule,
        "agent_specs": {
            spec_id: {
                key: value
                for key, value in snapshot.items()
                if key != "profile"
            }
            for spec_id, snapshot in sorted(all_spec_snapshots.items())
        },
    }
    template_fingerprint = canonical_json_fingerprint(template_snapshot)
    desired_snapshot = {
        "group_key": group_key,
        "template_id": template_id,
        "template_fingerprint": template_fingerprint,
        "group_name": group_name,
        "description": description,
        "roles": sorted(allowed_roles),
        "connections": sorted(connections, key=lambda item: item["id"]),
        "reporting_rule": reporting_rule,
        "task_template": task_template,
    }
    return {
        "group_key": group_key,
        "template_id": template_id,
        "template": template,
        "template_snapshot": template_snapshot,
        "template_fingerprint": template_fingerprint,
        "definition_fingerprint": canonical_json_fingerprint(desired_snapshot),
        "group_name": group_name,
        "description": description,
        "definitions": definitions,
        "connections": connections,
        "reporting_rule": reporting_rule,
        "entrypoint_role": entrypoint,
        "spec_snapshots": spec_snapshots,
        "task_template": deepcopy(task_template) if isinstance(task_template, dict) else {},
    }


def group_agent_id(context_key: str, spec_id: str) -> str:
    digest = hashlib.sha256(
        f"{context_key}\0{spec_id}".encode("utf-8")
    ).hexdigest()[:24]
    return f"agent-group-{digest}"


def group_ids_from_agent(agent: dict[str, Any]) -> set[str]:
    parameters = agent.get("parameters")
    if not isinstance(parameters, dict):
        return set()
    return {
        value.strip()
        for value in re.split(r"[,\n;|]+", str(parameters.get("group_ids") or ""))
        if value.strip()
    }


def is_group_managed_agent(agent: dict[str, Any]) -> bool:
    parameters = agent.get("parameters")
    return (
        isinstance(parameters, dict)
        and str(parameters.get("managed_by") or "").strip() == "group_api"
    )


def occupied_group_agent_phones(
    config: dict[str, Any],
    agents: list[dict[str, Any]],
) -> set[str]:
    occupied = {
        str(agent.get("phone") or "").strip()
        for agent in agents
        if str(agent.get("phone") or "").strip()
    }
    occupied.update(DEFAULT_AGENT_PHONES.values())
    occupied.add(PROJECT_MANAGER_PHONE)
    occupied.update(phone_git_contexts_from_config(config).keys())
    occupied.update(
        phone
        for entry in project_registry_from_config(config).values()
        if (phone := normalize_project_phone(entry.get("project_phone")))
    )
    return occupied


def allocate_group_agent_phone(occupied: set[str]) -> str:
    for number in range(GROUP_AGENT_PHONE_MIN, GROUP_AGENT_PHONE_MAX + 1):
        phone = str(number)
        if phone not in occupied:
            occupied.add(phone)
            return phone
    raise HTTPException(
        status_code=status.HTTP_409_CONFLICT,
        detail={
            "error": "no_free_group_agent_phone",
            "range": f"{GROUP_AGENT_PHONE_MIN}-{GROUP_AGENT_PHONE_MAX}",
        },
    )


def ensure_group_agents(
    config: dict[str, Any],
    agents: list[dict[str, Any]],
    project_entry: dict[str, Any],
    context_key: str,
    project_phone: str,
    group_id: str,
    definitions: list[dict[str, Any]],
    spec_snapshots: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], int, int, bool]:
    agents_by_id = {
        str(agent.get("id") or "").strip(): agent
        for agent in agents
        if str(agent.get("id") or "").strip()
    }
    identity_agents: dict[tuple[str, str], list[dict[str, Any]]] = {}
    phone_owners: dict[str, list[str]] = {}
    for agent in agents:
        agent_id = str(agent.get("id") or "").strip()
        phone = str(agent.get("phone") or "").strip()
        if phone:
            phone_owners.setdefault(phone, []).append(agent_id)
        if not is_group_managed_agent(agent):
            continue
        parameters = agent.get("parameters") if isinstance(agent.get("parameters"), dict) else {}
        agent_context = normalize_project_context_reference(
            str(parameters.get("git_context_key") or "").strip()
        )
        spec_id = str(parameters.get("group_agent_spec") or "").strip()
        if agent_context and spec_id:
            identity_agents.setdefault((agent_context, spec_id), []).append(agent)

    occupied = occupied_group_agent_phones(config, agents)
    phone_map_raw = config.get(PHONE_GIT_CONTEXTS_KEY)
    phone_map = dict(phone_map_raw) if isinstance(phone_map_raw, dict) else {}
    bindings: list[dict[str, Any]] = []
    resolved_by_spec: dict[str, tuple[dict[str, Any], bool]] = {}
    created_count = 0
    reused_count = 0
    changed = False

    for definition in definitions:
        role = definition["role"]
        spec_id = definition["spec"]
        if spec_id in resolved_by_spec:
            agent, was_created = resolved_by_spec[spec_id]
        else:
            expected_id = group_agent_id(context_key, spec_id)
            matches = identity_agents.get((context_key, spec_id), [])
            if len(matches) > 1:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "error": "group_agent_identity_conflict",
                        "spec": spec_id,
                        "agent_ids": [match.get("id") for match in matches],
                    },
                )
            by_id = agents_by_id.get(expected_id)
            if matches:
                agent = matches[0]
                if str(agent.get("id") or "").strip() != expected_id:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "error": "group_agent_identity_conflict",
                            "spec": spec_id,
                            "message": "Managed agent has a non-canonical id",
                        },
                    )
            elif by_id is not None:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "error": "group_agent_identity_conflict",
                        "spec": spec_id,
                        "agent_id": expected_id,
                    },
                )
            else:
                agent = {}

            snapshot = spec_snapshots[spec_id]
            stored_parameters = (
                agent.get("parameters")
                if isinstance(agent.get("parameters"), dict)
                else {}
            )
            stored_fingerprint = str(
                stored_parameters.get("group_agent_spec_fingerprint") or ""
            ).strip()
            if stored_fingerprint and stored_fingerprint != snapshot["fingerprint"]:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "error": "group_agent_spec_conflict",
                        "spec": spec_id,
                        "stored_fingerprint": stored_fingerprint,
                        "registry_fingerprint": snapshot["fingerprint"],
                    },
                )

            was_created = not bool(agent)
            if was_created:
                phone = allocate_group_agent_phone(occupied)
                agent = {
                    "id": expected_id,
                    "name": "",
                    "phone": phone,
                    "profile": "",
                    "parameters": {},
                    "template_source": "",
                    "status": "active",
                }
                agents.append(agent)
                agents_by_id[expected_id] = agent
                created_count += 1
                changed = True
            else:
                phone = str(agent.get("phone") or "").strip()
                if not (
                    phone.isdigit()
                    and GROUP_AGENT_PHONE_MIN <= int(phone) <= GROUP_AGENT_PHONE_MAX
                ):
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "error": "group_agent_phone_invalid",
                            "agent_id": expected_id,
                            "phone": phone,
                        },
                    )
                owners = phone_owners.get(phone, [])
                if owners != [expected_id]:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "error": "group_agent_phone_conflict",
                            "agent_id": expected_id,
                            "phone": phone,
                            "owners": owners,
                        },
                    )
                reused_count += 1

            membership = group_ids_from_agent(agent)
            membership.add(group_id)
            desired_parameters = {
                **snapshot["parameters"],
                "managed_by": "group_api",
                "group_agent_spec": spec_id,
                "group_agent_spec_fingerprint": snapshot["fingerprint"],
                "git_context_key": context_key,
                "git_context_keys": context_key,
                "project_phone": project_phone,
                "conversation_phone": project_phone,
                "agent_phone": phone,
                "group_ids": ",".join(sorted(membership)),
                "group_task_endpoint_template": (
                    "/api/v1/groups/{group_id}/agents/"
                    f"{expected_id}/tasks"
                ),
            }
            desired_agent = {
                "id": expected_id,
                "name": f"{snapshot['name']} [{project_phone}:{spec_id}]",
                "phone": phone,
                "profile": snapshot["profile"],
                "parameters": normalize_agent_parameters(desired_parameters),
                "template_source": snapshot["template_source"],
                "status": snapshot["status"],
            }
            if agent != desired_agent:
                agent.clear()
                agent.update(desired_agent)
                changed = True

            raw_mapping = phone_map.get(phone)
            mapping = dict(raw_mapping) if isinstance(raw_mapping, dict) else {}
            normalized_mapping = normalize_git_context_config_entry(mapping)
            if (
                normalized_mapping is not None
                and normalized_mapping.get("git_context_key") != context_key
            ):
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "error": "group_agent_phone_mapping_conflict",
                        "phone": phone,
                    },
                )
            desired_mapping = {
                **mapping,
                "project_name": normalize_project_name(
                    project_entry.get("project_name"),
                    str(project_entry.get("git_address") or ""),
                ),
                "git_address": str(project_entry.get("git_address") or "").strip(),
                "git_context_key": context_key,
                "phone": phone,
                "project_phone": project_phone,
                "managed_by": "group_api",
                "agent_id": expected_id,
                "group_agent_spec": spec_id,
            }
            if mapping != desired_mapping:
                phone_map[phone] = desired_mapping
                changed = True

            resolved_by_spec[spec_id] = (agent, was_created)

        bindings.append(
            {
                "role": role,
                "spec": spec_id,
                "agent_id": str(agent.get("id") or ""),
                "agent_phone": str(agent.get("phone") or ""),
                "is_entrypoint": bool(definition.get("is_entrypoint")),
            }
        )

    config[PHONE_GIT_CONTEXTS_KEY] = phone_map
    return bindings, created_count, reused_count, changed


def remove_group_membership_from_agents(
    agents: list[dict[str, Any]],
    group_id: str,
    agent_ids: set[str],
) -> bool:
    changed = False
    for agent in agents:
        if str(agent.get("id") or "").strip() not in agent_ids:
            continue
        parameters = (
            dict(agent.get("parameters"))
            if isinstance(agent.get("parameters"), dict)
            else {}
        )
        memberships = group_ids_from_agent(agent)
        if group_id not in memberships:
            continue
        memberships.discard(group_id)
        parameters["group_ids"] = ",".join(sorted(memberships))
        agent["parameters"] = normalize_agent_parameters(parameters)
        changed = True
    return changed


def persist_group_transaction_files(
    config: dict[str, Any],
    agents: list[dict[str, Any]],
    previous_agents: list[dict[str, Any]],
    *,
    agents_changed: bool,
) -> None:
    if agents_changed:
        write_agents_file_unlocked(agents)
    try:
        write_git_config_file(config)
    except Exception:
        if agents_changed:
            write_agents_file_unlocked(previous_agents)
        raise


def group_instance_from_blueprint(
    blueprint: dict[str, Any],
    bindings: list[dict[str, Any]],
    agents: list[dict[str, Any]],
    context_key: str,
    project_phone: str,
    group_id: str,
    *,
    current_group: dict[str, Any] | None = None,
    status_value: str = "active",
    revision: int = 1,
    created_at: str | None = None,
    updated_at: str | None = None,
) -> dict[str, Any]:
    agents_by_id = {
        str(agent.get("id") or "").strip(): agent
        for agent in agents
        if str(agent.get("id") or "").strip()
    }
    role_bindings = {
        binding["role"]: binding["agent_id"]
        for binding in bindings
    }
    connections = expand_group_connections(
        blueprint["connections"],
        role_bindings,
        agents_by_id,
        project_phone,
        group_id,
    )
    timestamp = updated_at or utc_now()
    task_template = blueprint.get("task_template", {})
    return {
        "group_id": group_id,
        "group_key": blueprint["group_key"],
        "project_id": project_phone,
        "project_phone": project_phone,
        "git_context_key": context_key,
        "template_id": blueprint["template_id"],
        "template_fingerprint": blueprint["template_fingerprint"],
        "template_snapshot": deepcopy(blueprint["template_snapshot"]),
        "definition_fingerprint": blueprint["definition_fingerprint"],
        "group_name": blueprint["group_name"],
        "description": blueprint["description"],
        "status": status_value,
        "revision": revision,
        "agents": deepcopy(bindings),
        "agent_ids": sorted({binding["agent_id"] for binding in bindings}),
        "connections": connections,
        "reporting_rule": deepcopy(blueprint["reporting_rule"]),
        "task_template": deepcopy(task_template) if isinstance(task_template, dict) else {},
        "created_at": created_at or timestamp,
        "updated_at": timestamp,
    }


def active_project_groups(project_entry: dict[str, Any]) -> list[dict[str, Any]]:
    raw_groups = project_entry.get("groups")
    if not isinstance(raw_groups, list):
        return []
    return [
        group
        for group in raw_groups
        if isinstance(group, dict)
        and str(group.get("group_id") or "").strip()
        and str(group.get("status") or "active").strip() != "archived"
    ]


def group_role_binding(
    group: dict[str, Any],
    role: str,
) -> dict[str, Any] | None:
    raw_bindings = group.get("agents")
    if not isinstance(raw_bindings, list):
        return None
    for binding in raw_bindings:
        if (
            isinstance(binding, dict)
            and str(binding.get("role") or "").strip() == role
        ):
            return binding
    return None


def refresh_project_group_relationships(
    project_entry: dict[str, Any],
    registry: dict[str, Any],
) -> None:
    groups = active_project_groups(project_entry)
    groups_by_template: dict[str, list[dict[str, Any]]] = {}
    for group in groups:
        template_id = str(group.get("template_id") or "").strip()
        if template_id:
            groups_by_template.setdefault(template_id, []).append(group)

    materialized: list[dict[str, Any]] = []
    topology_states: list[dict[str, Any]] = []
    reporting_rules: list[dict[str, Any]] = []
    raw_topologies = registry.get("group_topologies", {})
    for raw_topology_id, raw_topology in raw_topologies.items():
        if not isinstance(raw_topology, dict):
            continue
        topology_id = str(
            raw_topology.get("topology_id") or raw_topology_id
        ).strip()
        required_templates = [
            str(template_id or "").strip()
            for template_id in raw_topology.get("groups", [])
            if str(template_id or "").strip()
        ]
        selected: dict[str, dict[str, Any]] = {}
        ambiguous: dict[str, list[str]] = {}
        missing: list[str] = []
        for template_id in required_templates:
            candidates = groups_by_template.get(template_id, [])
            if len(candidates) == 1:
                selected[template_id] = candidates[0]
            elif not candidates:
                missing.append(template_id)
            else:
                ambiguous[template_id] = [
                    str(candidate.get("group_id") or "")
                    for candidate in candidates
                ]
        if missing or ambiguous:
            topology_states.append(
                {
                    "topology_id": topology_id,
                    "status": "pending_binding" if ambiguous else "waiting_for_groups",
                    "missing_templates": missing,
                    "ambiguous_templates": ambiguous,
                }
            )
            continue

        topology_connections: list[dict[str, Any]] = []
        valid = True
        for raw_connection in raw_topology.get("connections", []):
            if not isinstance(raw_connection, dict):
                valid = False
                break
            from_template = str(raw_connection.get("from_group") or "").strip()
            to_template = str(raw_connection.get("to_group") or "").strip()
            from_group = selected.get(from_template)
            to_group = selected.get(to_template)
            from_role = str(raw_connection.get("from_role") or "").strip()
            to_role = str(raw_connection.get("to_role") or "").strip()
            from_binding = group_role_binding(from_group or {}, from_role)
            to_binding = group_role_binding(to_group or {}, to_role)
            queue_name = str(raw_connection.get("queue") or "worker-all").strip()
            if (
                from_group is None
                or to_group is None
                or from_binding is None
                or to_binding is None
                or queue_name not in GROUP_QUEUE_NAMES
            ):
                valid = False
                break
            base_id = normalized_group_id(raw_connection.get("id"))
            relationship_id = f"{topology_id}.{base_id}"
            from_group_id = str(from_group.get("group_id") or "")
            to_group_id = str(to_group.get("group_id") or "")
            relationship = {
                "id": relationship_id,
                "topology_id": topology_id,
                "from_group_id": from_group_id,
                "from_template_id": from_template,
                "from_role": from_role,
                "from_agent_id": from_binding.get("agent_id"),
                "from_phone": from_binding.get("agent_phone"),
                "to_group_id": to_group_id,
                "to_template_id": to_template,
                "to_role": to_role,
                "to_agent_id": to_binding.get("agent_id"),
                "to_phone": to_binding.get("agent_phone"),
                "channel_type": "queue",
                "queue": queue_name,
                "event": str(raw_connection.get("event") or "task").strip(),
                "send_endpoint": (
                    f"/api/v1/groups/{urllib.parse.quote(from_group_id, safe='')}"
                    f"/connections/{urllib.parse.quote(relationship_id, safe='')}/tasks"
                ),
                "receive_endpoint": (
                    f"/api/v1/groups/{urllib.parse.quote(to_group_id, safe='')}"
                    f"/agents/{urllib.parse.quote(str(to_binding.get('agent_id') or ''), safe='')}/tasks"
                ),
            }
            topology_connections.append(relationship)

        if not valid:
            topology_states.append(
                {
                    "topology_id": topology_id,
                    "status": "invalid_for_current_groups",
                }
            )
            continue
        materialized.extend(topology_connections)
        topology_states.append(
            {
                "topology_id": topology_id,
                "status": "active",
                "group_bindings": {
                    template_id: group.get("group_id")
                    for template_id, group in selected.items()
                },
                "connection_count": len(topology_connections),
            }
        )

        raw_reporting = raw_topology.get("customer_reporting")
        if isinstance(raw_reporting, dict):
            reporting = deepcopy(raw_reporting)
            reporter_template = str(reporting.get("reporter_group") or "").strip()
            reporter_group = selected.get(reporter_template)
            reporter_binding = group_role_binding(
                reporter_group or {},
                str(reporting.get("reporter_role") or "").strip(),
            )
            reporting.update(
                {
                    "topology_id": topology_id,
                    "reporter_group_id": (
                        reporter_group.get("group_id") if reporter_group else None
                    ),
                    "reporter_agent_id": (
                        reporter_binding.get("agent_id") if reporter_binding else None
                    ),
                    "reporter_phone": (
                        reporter_binding.get("agent_phone") if reporter_binding else None
                    ),
                    "project_manager_phone": PROJECT_MANAGER_PHONE,
                }
            )
            reporting_rules.append(reporting)

    project_entry["group_relationships"] = materialized
    project_entry["customer_reporting"] = {
        "topologies": topology_states,
        "rules": reporting_rules,
    }


def locate_group_in_config(
    config: dict[str, Any],
    group_id: str,
) -> tuple[str, str, dict[str, Any], list[dict[str, Any]], int, dict[str, Any]]:
    clean_group_id = normalized_group_id(group_id)
    raw_projects = config.get(PROJECTS_KEY)
    if not isinstance(raw_projects, dict):
        raw_projects = {}
    found: list[
        tuple[str, str, dict[str, Any], list[dict[str, Any]], int, dict[str, Any]]
    ] = []
    for raw_key, raw_entry in raw_projects.items():
        if not isinstance(raw_entry, dict):
            continue
        candidate = dict(raw_entry)
        candidate.setdefault("git_context_key", str(raw_key))
        normalized = normalize_git_context_config_entry(candidate)
        if normalized is None:
            continue
        raw_groups = raw_entry.get("groups")
        if not isinstance(raw_groups, list):
            continue
        groups = [deepcopy(group) for group in raw_groups if isinstance(group, dict)]
        for index, group in enumerate(groups):
            if str(group.get("group_id") or "").strip() == clean_group_id:
                found.append(
                    (
                        str(raw_key),
                        normalized["git_context_key"],
                        dict(raw_entry),
                        groups,
                        index,
                        group,
                    )
                )
    if not found:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "group_not_found", "group_id": clean_group_id},
        )
    if len(found) > 1:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Group id '{clean_group_id}' is registered more than once",
        )
    return found[0]


def group_agents_for_response(
    agents: list[dict[str, Any]],
    group: dict[str, Any],
) -> list[dict[str, Any]]:
    agent_ids = {
        str(binding.get("agent_id") or "").strip()
        for binding in group.get("agents", [])
        if isinstance(binding, dict)
    }
    return [
        deepcopy(agent)
        for agent in agents
        if str(agent.get("id") or "").strip() in agent_ids
    ]


def create_group_transaction(
    project_id: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    registry = read_group_templates_file()
    blueprint = group_blueprint_from_payload(registry, payload)
    with git_config_file_lock():
        with agents_file_lock():
            config = read_git_config_file()
            raw_key, context_key, project_entry, _ = project_for_group_api(
                config,
                project_id,
            )
            project_phone = normalize_project_phone(project_entry.get("project_phone"))
            groups = [
                deepcopy(group)
                for group in project_entry.get("groups", [])
                if isinstance(group, dict)
            ]
            same_key = [
                group
                for group in groups
                if str(group.get("group_key") or "").strip() == blueprint["group_key"]
            ]
            if len(same_key) > 1:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Duplicate group_key '{blueprint['group_key']}' in project",
                )
            existing_group = same_key[0] if same_key else None
            group_id = (
                str(existing_group.get("group_id") or "").strip()
                if existing_group is not None
                else stable_group_id(
                    context_key,
                    blueprint["template_id"],
                    blueprint["group_key"],
                )
            )
            if existing_group is not None:
                if str(existing_group.get("status") or "active") == "archived":
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "error": "group_archived",
                            "group_id": group_id,
                        },
                    )
                if existing_group.get("definition_fingerprint") != blueprint["definition_fingerprint"]:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "error": "group_definition_conflict",
                            "group_id": group_id,
                            "message": "Use PUT to update an existing group",
                        },
                    )
            else:
                for registered in project_registry_from_config(config).values():
                    for candidate in registered.get("groups", []):
                        if (
                            isinstance(candidate, dict)
                            and candidate.get("group_id") == group_id
                        ):
                            raise HTTPException(
                                status_code=status.HTTP_409_CONFLICT,
                                detail={
                                    "error": "group_id_conflict",
                                    "group_id": group_id,
                                },
                            )

            previous_agents = read_agents_file()
            agents = deepcopy(previous_agents)
            bindings, created_count, reused_count, agents_changed = ensure_group_agents(
                config,
                agents,
                project_entry,
                context_key,
                project_phone,
                group_id,
                blueprint["definitions"],
                blueprint["spec_snapshots"],
            )
            timestamp = utc_now()
            if existing_group is None:
                group = group_instance_from_blueprint(
                    blueprint,
                    bindings,
                    agents,
                    context_key,
                    project_phone,
                    group_id,
                    updated_at=timestamp,
                )
                groups.append(group)
                created = True
            else:
                group = group_instance_from_blueprint(
                    blueprint,
                    bindings,
                    agents,
                    context_key,
                    project_phone,
                    group_id,
                    current_group=existing_group,
                    status_value=str(existing_group.get("status") or "active"),
                    revision=int(existing_group.get("revision") or 1),
                    created_at=str(existing_group.get("created_at") or timestamp),
                    updated_at=str(existing_group.get("updated_at") or timestamp),
                )
                existing_index = next(
                    index
                    for index, candidate in enumerate(groups)
                    if candidate.get("group_id") == group_id
                )
                groups[existing_index] = group
                created = False

            project_entry["groups"] = groups
            project_entry["git_context_key"] = context_key
            project_entry["project_phone"] = project_phone
            if created:
                project_entry["updated_at"] = timestamp
            refresh_project_group_relationships(project_entry, registry)
            raw_projects = config.get(PROJECTS_KEY)
            projects = dict(raw_projects) if isinstance(raw_projects, dict) else {}
            projects[raw_key] = project_entry
            config[PROJECTS_KEY] = projects
            persist_group_transaction_files(
                config,
                agents,
                previous_agents,
                agents_changed=agents_changed,
            )
            return {
                "created": created,
                "project_id": project_phone,
                "project_phone": project_phone,
                "created_agent_count": created_count,
                "reused_agent_count": reused_count,
                "group": deepcopy(group),
                "agents": group_agents_for_response(agents, group),
            }


def mutable_group_snapshot(group: dict[str, Any]) -> dict[str, Any]:
    return {
        "group_name": group.get("group_name"),
        "description": group.get("description"),
        "status": group.get("status"),
        "template_fingerprint": group.get("template_fingerprint"),
        "definition_fingerprint": group.get("definition_fingerprint"),
        "agents": group.get("agents"),
        "connections": group.get("connections"),
        "reporting_rule": group.get("reporting_rule"),
        "task_template": group.get("task_template"),
    }


def update_group_transaction(
    group_id: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    registry = read_group_templates_file()
    with git_config_file_lock():
        with agents_file_lock():
            config = read_git_config_file()
            (
                raw_key,
                context_key,
                project_entry,
                groups,
                group_index,
                current_group,
            ) = locate_group_in_config(config, group_id)
            current_revision = int(current_group.get("revision") or 1)
            expected_revision = payload.get("expected_revision")
            if expected_revision is not None:
                if isinstance(expected_revision, bool):
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="expected_revision must be an integer",
                    )
                try:
                    clean_expected_revision = int(expected_revision)
                except (TypeError, ValueError) as exc:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="expected_revision must be an integer",
                    ) from exc
                if clean_expected_revision != current_revision:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "error": "group_revision_conflict",
                            "expected_revision": clean_expected_revision,
                            "current_revision": current_revision,
                        },
                    )

            for immutable_key in (
                "group_id",
                "project_id",
                "project_phone",
                "git_context_key",
                "group_key",
            ):
                if immutable_key not in payload:
                    continue
                current_value = (
                    current_group.get(immutable_key)
                    if immutable_key != "project_id"
                    else current_group.get("project_id")
                )
                if str(payload.get(immutable_key)) != str(current_value):
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"{immutable_key} is server-owned and immutable",
                    )
            if "agents" in payload:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=(
                        "agents is server-owned; select template roles with "
                        "enabled_roles or task_template.agents"
                    ),
                )
            if str(current_group.get("status") or "active") == "archived":
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={"error": "group_archived", "group_id": group_id},
                )

            effective_payload = dict(payload)
            add_roles = normalize_requested_group_roles(
                effective_payload.pop("_add_roles", None)
            )
            if add_roles is not None:
                current_roles = [
                    str(binding.get("role") or "").strip()
                    for binding in current_group.get("agents", [])
                    if isinstance(binding, dict)
                    and str(binding.get("role") or "").strip()
                ]
                effective_payload["enabled_roles"] = list(
                    dict.fromkeys([*current_roles, *add_roles])
                )

            blueprint = group_blueprint_from_payload(
                registry,
                effective_payload,
                current_group=current_group,
            )
            stored_template_fingerprint = str(
                current_group.get("template_fingerprint") or ""
            ).strip()
            if (
                stored_template_fingerprint
                and stored_template_fingerprint != blueprint["template_fingerprint"]
            ):
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "error": "group_template_changed",
                        "message": (
                            "The versioned template changed after deployment; "
                            "publish a new template_id"
                        ),
                    },
                )

            requested_status = effective_payload.get(
                "status",
                current_group.get("status", "active"),
            )
            status_value = str(requested_status or "").strip()
            if status_value not in {"active", "paused", "completed"}:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="status must be active, paused, or completed",
                )

            project_phone = normalize_project_phone(project_entry.get("project_phone"))
            previous_agents = read_agents_file()
            agents = deepcopy(previous_agents)
            bindings, created_count, reused_count, agents_changed = ensure_group_agents(
                config,
                agents,
                project_entry,
                context_key,
                project_phone,
                group_id,
                blueprint["definitions"],
                blueprint["spec_snapshots"],
            )
            previous_agent_ids = {
                str(binding.get("agent_id") or "").strip()
                for binding in current_group.get("agents", [])
                if isinstance(binding, dict)
            }
            next_agent_ids = {binding["agent_id"] for binding in bindings}
            agents_changed = (
                remove_group_membership_from_agents(
                    agents,
                    group_id,
                    previous_agent_ids - next_agent_ids,
                )
                or agents_changed
            )

            timestamp = utc_now()
            candidate = group_instance_from_blueprint(
                blueprint,
                bindings,
                agents,
                context_key,
                project_phone,
                group_id,
                current_group=current_group,
                status_value=status_value,
                revision=current_revision,
                created_at=str(current_group.get("created_at") or timestamp),
                updated_at=str(current_group.get("updated_at") or timestamp),
            )
            group_changed = mutable_group_snapshot(candidate) != mutable_group_snapshot(
                current_group
            )
            if group_changed:
                candidate["revision"] = current_revision + 1
                candidate["updated_at"] = timestamp
                groups[group_index] = candidate
                project_entry["groups"] = groups
                project_entry["updated_at"] = timestamp
            else:
                candidate = current_group

            if group_changed or agents_changed:
                refresh_project_group_relationships(project_entry, registry)
                raw_projects = config.get(PROJECTS_KEY)
                projects = dict(raw_projects) if isinstance(raw_projects, dict) else {}
                projects[raw_key] = project_entry
                config[PROJECTS_KEY] = projects
                persist_group_transaction_files(
                    config,
                    agents,
                    previous_agents,
                    agents_changed=agents_changed,
                )

            return {
                "updated": group_changed,
                "repaired": bool(agents_changed and not group_changed),
                "project_id": project_phone,
                "project_phone": project_phone,
                "created_agent_count": created_count,
                "reused_agent_count": reused_count,
                "group": deepcopy(candidate),
                "agents": group_agents_for_response(agents, candidate),
            }


def delete_group_transaction(group_id: str) -> dict[str, Any]:
    registry = read_group_templates_file()
    with git_config_file_lock():
        with agents_file_lock():
            config = read_git_config_file()
            (
                raw_key,
                _,
                project_entry,
                groups,
                group_index,
                current_group,
            ) = locate_group_in_config(config, group_id)
            project_phone = normalize_project_phone(project_entry.get("project_phone"))
            if str(current_group.get("status") or "active") == "archived":
                return {
                    "deleted": False,
                    "archived": True,
                    "project_id": project_phone,
                    "group": deepcopy(current_group),
                }

            previous_agents = read_agents_file()
            agents = deepcopy(previous_agents)
            agent_ids = {
                str(binding.get("agent_id") or "").strip()
                for binding in current_group.get("agents", [])
                if isinstance(binding, dict)
            }
            agents_changed = remove_group_membership_from_agents(
                agents,
                group_id,
                agent_ids,
            )
            timestamp = utc_now()
            archived_group = deepcopy(current_group)
            archived_group["status"] = "archived"
            archived_group["revision"] = int(current_group.get("revision") or 1) + 1
            archived_group["updated_at"] = timestamp
            archived_group["archived_at"] = timestamp
            groups[group_index] = archived_group
            project_entry["groups"] = groups
            project_entry["updated_at"] = timestamp
            refresh_project_group_relationships(project_entry, registry)
            raw_projects = config.get(PROJECTS_KEY)
            projects = dict(raw_projects) if isinstance(raw_projects, dict) else {}
            projects[raw_key] = project_entry
            config[PROJECTS_KEY] = projects
            persist_group_transaction_files(
                config,
                agents,
                previous_agents,
                agents_changed=agents_changed,
            )
            return {
                "deleted": True,
                "archived": True,
                "project_id": project_phone,
                "group": deepcopy(archived_group),
            }


async def run_group_write_transaction(
    transaction: Any,
    *args: Any,
) -> dict[str, Any]:
    async with git_config_lock:
        async with agents_lock:
            return await asyncio.to_thread(transaction, *args)


def read_group_with_agents_file(group_id: str) -> dict[str, Any]:
    config = read_git_config_file()
    (
        _,
        context_key,
        project_entry,
        _,
        _,
        group,
    ) = locate_group_in_config(config, group_id)
    registry = read_group_templates_file()
    refresh_project_group_relationships(project_entry, registry)
    agents = read_agents_file()
    normalized_context = configured_git_context_for_key(config, context_key) or {
        **project_entry,
        "git_context_key": context_key,
        "phones": [],
        "ports": [],
    }
    normalized_context = {
        **normalized_context,
        "groups": deepcopy(project_entry.get("groups", [])),
        "group_relationships": deepcopy(
            project_entry.get("group_relationships", [])
        ),
        "customer_reporting": deepcopy(
            project_entry.get("customer_reporting", {})
        ),
    }
    return {
        "project": public_project_context(normalized_context),
        "project_entry": project_entry,
        "group": deepcopy(group),
        "agents": group_agents_for_response(agents, group),
    }


async def read_group_with_agents(group_id: str) -> dict[str, Any]:
    async with git_config_lock:
        async with agents_lock:
            return await asyncio.to_thread(read_group_with_agents_file, group_id)


async def queued_group_tasks(group_id: str) -> list[dict[str, Any]]:
    tasks: list[dict[str, Any]] = []
    for queue_name in sorted(GROUP_QUEUE_NAMES):
        async with locks[queue_name]:
            matching = [
                item
                for item in queues[queue_name]
                if str(queue_item_metadata(item).get("group_id") or "").strip()
                == group_id
            ]
        for item in matching:
            metadata = deepcopy(queue_item_metadata(item))
            tasks.append(
                {
                    "queue": queue_name,
                    "id": queue_item_id(item),
                    "queued_at": queue_item_queued_at(item),
                    "message": deepcopy(queue_item_message(item)),
                    "task_id": metadata.get("task_id"),
                    "cycle_id": metadata.get("cycle_id"),
                    "request_id": metadata.get("request_id"),
                    "connection_id": metadata.get("connection_id"),
                    "from_agent_id": metadata.get("from_agent_id"),
                    "to_agent_id": metadata.get("to_agent_id"),
                    "metadata": metadata,
                }
            )
    return sorted(
        tasks,
        key=lambda item: (
            str(item.get("queued_at") or ""),
            str(item.get("id") or ""),
        ),
    )


async def remove_queued_cycle_items(cycle_id: str) -> list[dict[str, Any]]:
    removed: list[dict[str, Any]] = []
    removed_items: list[tuple[str, Any]] = []
    acquired_queue_locks: list[asyncio.Lock] = []
    try:
        for queue_name in sorted(GROUP_QUEUE_NAMES):
            await locks[queue_name].acquire()
            acquired_queue_locks.append(locks[queue_name])
        for queue_name in sorted(GROUP_QUEUE_NAMES):
            kept_items: deque[Any] = deque()
            while queues[queue_name]:
                item = queues[queue_name].popleft()
                metadata = queue_item_metadata(item)
                if str(metadata.get("cycle_id") or "").strip() == cycle_id:
                    removed_items.append((queue_name, item))
                    removed.append(
                        {
                            "queue": queue_name,
                            "queue_item_id": queue_item_id(item),
                            "task_id": metadata.get("task_id"),
                            "task_node_id": metadata.get("task_node_id"),
                            "group_id": metadata.get("group_id"),
                        }
                    )
                    continue
                kept_items.append(item)
            queues[queue_name] = kept_items
    finally:
        for queue_lock in reversed(acquired_queue_locks):
            queue_lock.release()
    for queue_name, item in removed_items:
        metadata = deepcopy(queue_item_metadata(item))
        await append_history(
            f"removed_from_{QUEUE_DEFINITIONS[queue_name]['context']}_queue",
            queue_name,
            deepcopy(queue_item_message(item)),
            {
                "queue_item_id": queue_item_id(item),
                **metadata,
                "action": "cancelled_by_cycle_completion",
                "scheduled_event": "cycle_completion_cancelled",
            },
            git_context={
                key: metadata.get(key)
                for key in (
                    "fastapi_port",
                    "project_name",
                    "git_context_key",
                    "git_address",
                    "git_commit",
                    "git_commit_short",
                    "git_error",
                )
                if metadata.get(key) is not None
            }
            or None,
        )
    return removed


def group_git_context_for_queue(
    group_data: dict[str, Any],
) -> dict[str, Any]:
    project_entry = group_data["project_entry"]
    group = group_data["group"]
    return {
        "project_name": normalize_project_name(
            project_entry.get("project_name"),
            str(project_entry.get("git_address") or ""),
        ),
        "git_address": str(project_entry.get("git_address") or "").strip(),
        "git_context_key": str(group.get("git_context_key") or "").strip(),
        "queue_phone": str(group.get("project_phone") or "").strip(),
        "git_context_phone": str(group.get("project_phone") or "").strip(),
    }


def task_message_from_payload(payload: dict[str, Any]) -> Any:
    message = payload.get("message")
    task_template = payload.get("task_template")
    if message is None and isinstance(task_template, dict):
        message = task_template.get("task")
        if message is None:
            message = task_template.get("message")
    if message is None:
        message = payload.get("task")
    if message is None or (isinstance(message, str) and not message.strip()):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="message or task_template.task is required",
        )
    return message.strip() if isinstance(message, str) else deepcopy(message)


def task_identity_from_payload(payload: dict[str, Any]) -> tuple[str, str]:
    task_template = payload.get("task_template")
    if task_template is not None and not isinstance(task_template, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="task_template must be an object",
        )
    template_id = (
        str(task_template.get("id") or "").strip()
        if isinstance(task_template, dict)
        else ""
    )
    request_id = str(payload.get("request_id") or "").strip()
    if request_id and len(request_id) > 160:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="request_id must contain at most 160 characters",
        )
    return request_id or str(uuid4()), template_id


def normalized_cycle_id(value: Any, *, required: bool = False) -> str:
    if value is None:
        if required:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="cycle_id is required",
            )
        return ""
    if not isinstance(value, str):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="cycle_id must be a string",
        )
    clean_value = value.strip()
    if not clean_value:
        if required:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="cycle_id is required",
            )
        return ""
    if not CYCLE_ID_PATTERN.fullmatch(clean_value):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "cycle_id must contain 1-160 letters, digits, '.', '_', ':', "
                "or '-' and must start with a letter or digit"
            ),
        )
    return clean_value


def cycle_identity_from_task_payload(
    payload: dict[str, Any],
    *,
    request_scope: str,
    message: Any,
    task_template_id: str,
) -> tuple[str, str, bool]:
    explicit_cycle = "cycle_id" in payload and payload.get("cycle_id") is not None
    cycle_id = normalized_cycle_id(payload.get("cycle_id"))
    request_id = str(payload.get("request_id") or "").strip()
    if not cycle_id:
        stable_seed = (
            f"{request_scope}:{request_id}"
            if request_id
            else f"{request_scope}:{uuid4()}"
        )
        suffix = hashlib.sha256(stable_seed.encode("utf-8")).hexdigest()[:12]
        cycle_id = (
            f"cycle-{suffix}"
            if request_id
            else f"cycle-{datetime.now(timezone.utc):%Y%m%d}-{suffix}"
        )

    raw_title = payload.get("cycle_title")
    if raw_title is not None and not isinstance(raw_title, str):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="cycle_title must be a string",
        )
    cycle_title = str(raw_title or "").strip()
    if len(cycle_title) > 300:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="cycle_title must contain at most 300 characters",
        )
    if not cycle_title:
        cycle_title = task_template_id or summarize_message(message)
    return cycle_id, cycle_title, explicit_cycle


def cycle_task_node_id(cycle_id: str, request_scope: str, task_id: str) -> str:
    digest = hashlib.sha256(
        f"{cycle_id}:{request_scope}:{task_id}".encode("utf-8")
    ).hexdigest()[:20]
    return f"cycle-task-{digest}"


def persisted_group_task_request(
    request_scope: str,
    request_id: str,
) -> dict[str, Any] | None:
    if not history_path.exists():
        return None
    try:
        lines = history_path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return None
    for line in reversed(lines):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(record, dict):
            continue
        metadata = record.get("metadata")
        if not isinstance(metadata, dict):
            continue
        if (
            str(metadata.get("task_request_scope") or "").strip() == request_scope
            and str(metadata.get("request_id") or "").strip() == request_id
        ):
            event = str(record.get("event") or "").strip()
            if event.startswith("queued_to_"):
                # The queue itself is in memory. If the item is no longer there
                # after a restart, a retry must restore it instead of treating a
                # historical enqueue record as a still-pending durable item.
                return None
            return {
                "queue": str(record.get("queue") or "").strip(),
                "id": str(metadata.get("queue_item_id") or "").strip(),
                "metadata": metadata,
                "state": "delivered"
                if event.startswith("delivered_to_")
                else "processed",
            }
    return None


async def find_existing_group_task_request(
    request_scope: str,
    request_id: str,
) -> dict[str, Any] | None:
    for candidate_queue in sorted(GROUP_QUEUE_NAMES):
        async with locks[candidate_queue]:
            for item in queues[candidate_queue]:
                item_metadata = queue_item_metadata(item)
                if (
                    str(item_metadata.get("task_request_scope") or "").strip()
                    == request_scope
                    and str(item_metadata.get("request_id") or "").strip()
                    == request_id
                ):
                    return {
                        "queue": candidate_queue,
                        "id": queue_item_id(item) or "",
                        "metadata": deepcopy(item_metadata),
                        "state": "queued",
                    }
    return await asyncio.to_thread(
        persisted_group_task_request,
        request_scope,
        request_id,
    )


def deduplicated_group_task_result(
    existing: dict[str, Any],
    fingerprint: str,
    request_id: str,
    default_queue: str,
) -> dict[str, Any]:
    existing_metadata = existing.get("metadata")
    existing_fingerprint = (
        str(existing_metadata.get("task_request_fingerprint") or "").strip()
        if isinstance(existing_metadata, dict)
        else ""
    )
    if existing_fingerprint != fingerprint:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "task_request_conflict",
                "request_id": request_id,
            },
        )
    existing_queue = str(existing.get("queue") or default_queue)
    return {
        "status": existing.get("state") or "queued",
        "queue": existing_queue,
        "id": existing.get("id"),
        "size": len(queues.get(existing_queue, [])),
        "deduplicated": True,
        "metadata": deepcopy(existing_metadata)
        if isinstance(existing_metadata, dict)
        else {},
    }


async def enqueue_idempotent_group_task(
    queue_name: str,
    conversation_phone: str,
    message: Any,
    metadata: dict[str, Any],
    port: int | None,
    git_context: dict[str, Any],
    *,
    submission_lock_held: bool = False,
) -> dict[str, Any]:
    request_id = str(metadata.get("request_id") or "").strip()
    request_scope = str(metadata.get("task_request_scope") or "").strip()
    fingerprint = str(metadata.get("task_request_fingerprint") or "").strip()
    if not request_id:
        return await enqueue_phone_channel(
            queue_name,
            conversation_phone,
            message,
            metadata,
            port,
            git_context,
        )

    acquired_submission_lock = False
    if not submission_lock_held:
        await group_task_submission_lock.acquire()
        acquired_submission_lock = True
    try:
        existing = await find_existing_group_task_request(
            request_scope,
            request_id,
        )
        if existing is not None:
            return deduplicated_group_task_result(
                existing,
                fingerprint,
                request_id,
                queue_name,
            )

        queued = await enqueue_phone_channel(
            queue_name,
            conversation_phone,
            message,
            metadata,
            port,
            git_context,
        )
        queued["deduplicated"] = False
        queued["metadata"] = deepcopy(metadata)
        return queued
    finally:
        if acquired_submission_lock:
            group_task_submission_lock.release()


def entrypoint_binding_for_group(group: dict[str, Any]) -> dict[str, Any]:
    entrypoints = [
        binding
        for binding in group.get("agents", [])
        if isinstance(binding, dict) and binding.get("is_entrypoint")
    ]
    if len(entrypoints) != 1:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "group_entrypoint_invalid",
                "group_id": group.get("group_id"),
            },
        )
    return entrypoints[0]


def ensure_group_accepts_tasks(group: dict[str, Any]) -> None:
    group_status = str(group.get("status") or "active").strip()
    if group_status != "active":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "group_not_active",
                "group_id": group.get("group_id"),
                "status": group_status,
            },
        )


def external_group_task_components(
    group_data: dict[str, Any],
    payload: dict[str, Any],
) -> dict[str, Any]:
    group = group_data["group"]
    group_id = str(group.get("group_id") or "").strip()
    ensure_group_accepts_tasks(group)
    entrypoint = entrypoint_binding_for_group(group)
    task_template = payload.get("task_template")
    message = task_message_from_payload(payload)
    task_id, task_template_id = task_identity_from_payload(payload)
    request_id = str(payload.get("request_id") or "").strip()
    project_phone = str(group.get("project_phone") or "").strip()
    to_phone = str(entrypoint.get("agent_phone") or "").strip()
    fingerprint_template = deepcopy(task_template) if isinstance(task_template, dict) else {}
    if isinstance(fingerprint_template.get("agents"), list):
        fingerprint_template["agents"] = sorted(
            normalize_requested_group_roles(fingerprint_template["agents"]) or []
        )
    request_scope = f"{group_id}:external"
    cycle_id, cycle_title, cycle_explicit = cycle_identity_from_task_payload(
        payload,
        request_scope=request_scope,
        message=message,
        task_template_id=task_template_id,
    )
    task_node_id = cycle_task_node_id(cycle_id, request_scope, task_id)
    request_fingerprint = canonical_json_fingerprint(
        {
            "message": message,
            "task_template": fingerprint_template,
            "cycle_id": cycle_id,
            "cycle_title": cycle_title,
            "from_agent_id": PROJECT_MANAGER_AGENT_ID,
            "to_agent_id": entrypoint.get("agent_id"),
        }
    )
    metadata = {
        "submitted_via": "group_api",
        "sender": "Project Manager",
        "receiver": entrypoint.get("role"),
        "group_id": group_id,
        "task_id": task_id,
        "task_node_id": task_node_id,
        "parent_task_id": None,
        "parent_task_node_id": None,
        "cycle_id": cycle_id,
        "cycle_title": cycle_title,
        "cycle_origin": "external_task",
        "request_id": request_id or None,
        "task_request_scope": request_scope if request_id else None,
        "task_request_fingerprint": request_fingerprint if request_id else None,
        "task_template_id": task_template_id or None,
        "event": "external_task",
        "send_endpoint": f"/api/v1/groups/{urllib.parse.quote(group_id, safe='')}/tasks",
        "receive_endpoint": (
            f"/api/v1/groups/{urllib.parse.quote(group_id, safe='')}/agents/"
            f"{urllib.parse.quote(str(entrypoint.get('agent_id') or ''), safe='')}/tasks"
        ),
        "project_phone": project_phone,
        "git_context_key": group.get("git_context_key"),
        "from_phone": PROJECT_MANAGER_PHONE,
        "from_agent_id": PROJECT_MANAGER_AGENT_ID,
        "to_phone": to_phone,
        "to_agent_id": entrypoint.get("agent_id"),
        "to_role": entrypoint.get("role"),
    }
    return {
        "group": group,
        "entrypoint": entrypoint,
        "message": message,
        "task_id": task_id,
        "task_node_id": task_node_id,
        "task_template_id": task_template_id,
        "request_id": request_id,
        "request_scope": request_scope,
        "request_fingerprint": request_fingerprint,
        "cycle_id": cycle_id,
        "cycle_title": cycle_title,
        "cycle_explicit": cycle_explicit,
        "project_phone": project_phone,
        "to_phone": to_phone,
        "metadata": metadata,
    }


def external_group_task_response(
    group_id: str,
    components: dict[str, Any],
    queued: dict[str, Any],
) -> dict[str, Any]:
    stored_metadata = queued.get("metadata")
    effective_metadata = (
        stored_metadata if isinstance(stored_metadata, dict) else components["metadata"]
    )
    entrypoint = components["entrypoint"]
    return {
        "status": queued.get("status") or "queued",
        "group_id": str(effective_metadata.get("group_id") or group_id),
        "task_id": str(effective_metadata.get("task_id") or components["task_id"]),
        "task_node_id": str(
            effective_metadata.get("task_node_id") or components["task_node_id"]
        ),
        "cycle_id": str(
            effective_metadata.get("cycle_id") or components["cycle_id"]
        ),
        "task_template_id": str(
            effective_metadata.get("task_template_id")
            or components["task_template_id"]
        ),
        "queue": queued.get("queue") or "worker-all",
        "queue_item_id": queued["id"],
        "deduplicated": bool(queued.get("deduplicated")),
        "from_agent_id": effective_metadata.get("from_agent_id")
        or PROJECT_MANAGER_AGENT_ID,
        "from_phone": effective_metadata.get("from_phone") or PROJECT_MANAGER_PHONE,
        "to_agent_id": effective_metadata.get("to_agent_id")
        or entrypoint.get("agent_id"),
        "to_phone": effective_metadata.get("to_phone")
        or components["to_phone"],
        "entrypoint": deepcopy(entrypoint),
    }


async def enqueue_external_group_task_from_data(
    group_id: str,
    group_data: dict[str, Any],
    payload: dict[str, Any],
    port: int | None,
) -> dict[str, Any]:
    components = external_group_task_components(group_data, payload)
    queued = await enqueue_idempotent_group_task(
        "worker-all",
        components["project_phone"],
        components["message"],
        components["metadata"],
        port,
        group_git_context_for_queue(group_data),
        submission_lock_held=True,
    )
    return external_group_task_response(group_id, components, queued)


async def enqueue_external_group_task(
    group_id: str,
    payload: dict[str, Any],
    port: int | None,
) -> dict[str, Any]:
    task_template = payload.get("task_template")
    task_roles = (
        normalize_requested_group_roles(task_template.get("agents"))
        if isinstance(task_template, dict) and "agents" in task_template
        else None
    )
    async with group_task_submission_lock:
        initial_group_data = await read_group_with_agents(group_id)
        initial_components = external_group_task_components(
            initial_group_data,
            payload,
        )
        effective_payload = deepcopy(payload)
        effective_payload["cycle_id"] = initial_components["cycle_id"]
        effective_payload["cycle_title"] = initial_components["cycle_title"]
        if initial_components["request_id"]:
            existing = await find_existing_group_task_request(
                initial_components["request_scope"],
                initial_components["request_id"],
            )
            if existing is not None:
                queued = deduplicated_group_task_result(
                    existing,
                    initial_components["request_fingerprint"],
                    initial_components["request_id"],
                    "worker-all",
                )
                return external_group_task_response(
                    group_id,
                    initial_components,
                    queued,
                )

        await ensure_cycle_can_receive_task(
            initial_components["cycle_id"],
            initial_components["project_phone"],
            allow_create=True,
            request_scope=initial_components["request_scope"],
            request_id=initial_components["request_id"],
        )

        if task_roles:
            await run_group_write_transaction(
                update_group_transaction,
                group_id,
                {"_add_roles": task_roles},
            )

        # Keep the project mutation lock through the final status check and
        # enqueue so DELETE/PUT cannot commit between them.
        async with git_config_lock:
            async with agents_lock:
                group_data = await asyncio.to_thread(
                    read_group_with_agents_file,
                    group_id,
                )
            return await enqueue_external_group_task_from_data(
                group_id,
                group_data,
                effective_payload,
                port,
            )


def connection_for_group_task(
    group_data: dict[str, Any],
    connection_id: str,
) -> dict[str, Any]:
    group = group_data["group"]
    clean_connection_id = normalized_group_id(connection_id)
    candidates = [
        connection
        for connection in group.get("connections", [])
        if isinstance(connection, dict)
        and str(connection.get("id") or "").strip() == clean_connection_id
    ]
    project_entry = group_data["project_entry"]
    candidates.extend(
        connection
        for connection in project_entry.get("group_relationships", [])
        if isinstance(connection, dict)
        and str(connection.get("id") or "").strip() == clean_connection_id
        and str(connection.get("from_group_id") or "").strip()
        == str(group.get("group_id") or "").strip()
    )
    if not candidates:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "group_connection_not_found",
                "connection_id": clean_connection_id,
            },
        )
    if len(candidates) > 1:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Connection id '{clean_connection_id}' is ambiguous",
        )
    return candidates[0]


async def enqueue_group_connection_task_from_data(
    group_data: dict[str, Any],
    target_group_data: dict[str, Any],
    connection_id: str,
    payload: dict[str, Any],
    port: int | None,
) -> dict[str, Any]:
    group = group_data["group"]
    group_id = str(group.get("group_id") or "").strip()
    ensure_group_accepts_tasks(group)
    target_group = target_group_data["group"]
    ensure_group_accepts_tasks(target_group)
    connection = connection_for_group_task(group_data, connection_id)
    source_project_phone = str(group.get("project_phone") or "").strip()
    target_project_phone = str(target_group.get("project_phone") or "").strip()
    target_group_id = str(connection.get("to_group_id") or group_id).strip()
    expected_source_group_id = str(
        connection.get("from_group_id")
        or connection.get("group_id")
        or group_id
    ).strip()
    if (
        source_project_phone != target_project_phone
        or expected_source_group_id != group_id
        or target_group_id != str(target_group.get("group_id") or "").strip()
    ):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "group_connection_project_binding_invalid",
                "connection_id": connection.get("id"),
                "source_group_id": group_id,
                "target_group_id": target_group_id,
            },
        )

    expected_from_agent_id = str(connection.get("from_agent_id") or "").strip()
    expected_to_agent_id = str(connection.get("to_agent_id") or "").strip()
    source_bindings = [
        binding
        for binding in group.get("agents", [])
        if isinstance(binding, dict)
        and str(binding.get("agent_id") or "").strip() == expected_from_agent_id
        and str(binding.get("role") or "").strip()
        == str(connection.get("from_role") or "").strip()
        and str(binding.get("agent_phone") or "").strip()
        == str(connection.get("from_phone") or "").strip()
    ]
    target_bindings = [
        binding
        for binding in target_group.get("agents", [])
        if isinstance(binding, dict)
        and str(binding.get("agent_id") or "").strip() == expected_to_agent_id
        and str(binding.get("role") or "").strip()
        == str(connection.get("to_role") or "").strip()
        and str(binding.get("agent_phone") or "").strip()
        == str(connection.get("to_phone") or "").strip()
    ]
    if len(source_bindings) != 1 or len(target_bindings) != 1:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "group_connection_agent_binding_invalid",
                "connection_id": connection.get("id"),
            },
        )
    from_agent_id = str(payload.get("from_agent_id") or "").strip()
    if not from_agent_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="from_agent_id is required",
        )
    if from_agent_id != expected_from_agent_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "error": "group_connection_forbidden",
                "connection_id": connection.get("id"),
                "expected_from_agent_id": expected_from_agent_id,
            },
        )
    submitted_from_phone = str(payload.get("from_phone") or "").strip()
    expected_from_phone = str(connection.get("from_phone") or "").strip()
    if submitted_from_phone and submitted_from_phone != expected_from_phone:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="from_phone does not belong to the connection source agent",
        )

    message = task_message_from_payload(payload)
    task_id, task_template_id = task_identity_from_payload(payload)
    request_id = str(payload.get("request_id") or "").strip()
    project_phone = str(group.get("project_phone") or "").strip()
    queue_name = str(connection.get("queue") or "worker-all").strip()
    request_scope = f"{group_id}:connection:{connection.get('id')}"
    cycle_id, cycle_title, cycle_explicit = cycle_identity_from_task_payload(
        payload,
        request_scope=request_scope,
        message=message,
        task_template_id=task_template_id,
    )
    task_node_id = cycle_task_node_id(cycle_id, request_scope, task_id)
    parent_task_id = str(payload.get("parent_task_id") or "").strip()
    parent_task_node_id = str(payload.get("parent_task_node_id") or "").strip()
    if cycle_explicit and not parent_task_id and not parent_task_node_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "parent_task_id or parent_task_node_id is required when a "
                "connection continues an explicit cycle_id"
            ),
        )
    if not cycle_explicit and (parent_task_id or parent_task_node_id):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="parent task reference requires an explicit cycle_id",
        )
    parent_task_id, parent_task_node_id = await ensure_cycle_can_receive_task(
        cycle_id,
        project_phone,
        allow_create=not cycle_explicit,
        request_scope=request_scope,
        request_id=request_id,
        parent_task_id=parent_task_id,
        parent_task_node_id=parent_task_node_id,
        parent_group_id=group_id,
        parent_agent_id=expected_from_agent_id,
    )
    fingerprint_template = payload.get("task_template")
    request_fingerprint = canonical_json_fingerprint(
        {
            "message": message,
            "task_template": fingerprint_template
            if isinstance(fingerprint_template, dict)
            else {},
            "connection_id": connection.get("id"),
            "from_agent_id": expected_from_agent_id,
            "to_agent_id": connection.get("to_agent_id"),
            "from_phone": expected_from_phone,
            "to_phone": connection.get("to_phone"),
            "source_group_id": group_id,
            "target_group_id": target_group_id,
            "cycle_id": cycle_id,
            "cycle_title": cycle_title,
            "parent_task_id": parent_task_id,
            "parent_task_node_id": parent_task_node_id,
            "queue": queue_name,
            "event": connection.get("event"),
        }
    )
    metadata = {
        "submitted_via": "group_connection_api",
        "sender": connection.get("from_role"),
        "receiver": connection.get("to_role"),
        "group_id": target_group_id,
        "source_group_id": group_id,
        "source_group_revision": group.get("revision"),
        "target_group_revision": target_group_data["group"].get("revision"),
        "task_id": task_id,
        "task_node_id": task_node_id,
        "parent_task_id": parent_task_id or None,
        "parent_task_node_id": parent_task_node_id or None,
        "cycle_id": cycle_id,
        "cycle_title": cycle_title,
        "cycle_origin": "connection_task" if not cycle_explicit else "handoff",
        "request_id": request_id or None,
        "task_request_scope": request_scope if request_id else None,
        "task_request_fingerprint": request_fingerprint if request_id else None,
        "task_template_id": task_template_id or None,
        "connection_id": connection.get("id"),
        "event": connection.get("event"),
        "send_endpoint": connection.get("send_endpoint"),
        "receive_endpoint": connection.get("receive_endpoint"),
        "project_phone": project_phone,
        "git_context_key": group.get("git_context_key"),
        "from_phone": expected_from_phone,
        "from_agent_id": expected_from_agent_id,
        "from_role": connection.get("from_role"),
        "to_phone": str(connection.get("to_phone") or "").strip(),
        "to_agent_id": connection.get("to_agent_id"),
        "to_role": connection.get("to_role"),
        "target_group_id": target_group_id,
    }
    queued = await enqueue_idempotent_group_task(
        queue_name,
        project_phone,
        message,
        metadata,
        port,
        group_git_context_for_queue(group_data),
        submission_lock_held=True,
    )
    stored_metadata = queued.get("metadata")
    effective_metadata = (
        stored_metadata if isinstance(stored_metadata, dict) else metadata
    )
    return {
        "status": queued.get("status") or "queued",
        "group_id": effective_metadata.get("group_id") or target_group_id,
        "source_group_id": effective_metadata.get("source_group_id") or group_id,
        "task_id": effective_metadata.get("task_id") or task_id,
        "task_node_id": effective_metadata.get("task_node_id") or task_node_id,
        "parent_task_id": effective_metadata.get("parent_task_id"),
        "parent_task_node_id": effective_metadata.get("parent_task_node_id"),
        "cycle_id": effective_metadata.get("cycle_id") or cycle_id,
        "task_template_id": effective_metadata.get("task_template_id")
        or task_template_id,
        "connection_id": effective_metadata.get("connection_id")
        or connection.get("id"),
        "queue": queued.get("queue") or queue_name,
        "queue_item_id": queued["id"],
        "deduplicated": bool(queued.get("deduplicated")),
        "from_agent_id": effective_metadata.get("from_agent_id")
        or expected_from_agent_id,
        "from_phone": effective_metadata.get("from_phone") or expected_from_phone,
        "to_agent_id": effective_metadata.get("to_agent_id")
        or connection.get("to_agent_id"),
        "to_phone": effective_metadata.get("to_phone")
        or connection.get("to_phone"),
    }


async def enqueue_group_connection_task(
    group_id: str,
    connection_id: str,
    payload: dict[str, Any],
    port: int | None,
) -> dict[str, Any]:
    async with group_task_submission_lock:
        # Keep the project mutation lock until the queue item is committed.
        async with git_config_lock:
            async with agents_lock:
                group_data = await asyncio.to_thread(
                    read_group_with_agents_file,
                    group_id,
                )
                connection = connection_for_group_task(group_data, connection_id)
                target_group_id = str(
                    connection.get("to_group_id") or group_id
                ).strip()
                target_group_data = (
                    group_data
                    if target_group_id == group_id
                    else await asyncio.to_thread(
                        read_group_with_agents_file,
                        target_group_id,
                    )
                )
            return await enqueue_group_connection_task_from_data(
                group_data,
                target_group_data,
                connection_id,
                payload,
                port,
            )


async def dequeue_group_agent_task(
    group_id: str,
    agent_id: str,
    port: int | None,
) -> dict[str, Any]:
    group_data = await read_group_with_agents(group_id)
    group = group_data["group"]
    ensure_group_accepts_tasks(group)
    bindings = [
        binding
        for binding in group.get("agents", [])
        if isinstance(binding, dict)
        and str(binding.get("agent_id") or "").strip() == agent_id
    ]
    if not bindings:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "agent_not_in_group",
                "group_id": group_id,
                "agent_id": agent_id,
            },
        )

    # The same barrier is used by request-id submission. Keep it until the
    # delivered history record is durable so a concurrent retry cannot mistake
    # the short queue-to-history transition for a lost task.
    async with group_task_submission_lock:
        delivered_item: Any | None = None
        delivered_queue = ""
        queue_names = sorted(GROUP_QUEUE_NAMES)
        acquired_queue_locks: list[asyncio.Lock] = []
        try:
            for queue_name in queue_names:
                await locks[queue_name].acquire()
                acquired_queue_locks.append(locks[queue_name])

            candidates: list[tuple[str, str, str, Any]] = []
            for queue_name in queue_names:
                for item in queues[queue_name]:
                    metadata = queue_item_metadata(item)
                    if (
                        str(metadata.get("group_id") or "").strip() == group_id
                        and str(metadata.get("to_agent_id") or "").strip() == agent_id
                    ):
                        candidates.append(
                            (
                                str(queue_item_queued_at(item) or ""),
                                str(queue_item_id(item) or ""),
                                queue_name,
                                item,
                            )
                        )
            if candidates:
                _, selected_id, delivered_queue, delivered_item = min(
                    candidates,
                    key=lambda candidate: (candidate[0], candidate[1]),
                )
                removed = False
                kept_items: deque[Any] = deque()
                while queues[delivered_queue]:
                    item = queues[delivered_queue].popleft()
                    if not removed and str(queue_item_id(item) or "") == selected_id:
                        removed = True
                        continue
                    kept_items.append(item)
                queues[delivered_queue] = kept_items
        finally:
            for queue_lock in reversed(acquired_queue_locks):
                queue_lock.release()

        if delivered_item is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No queued group task for this agent",
            )
        metadata = deepcopy(queue_item_metadata(delivered_item))
        item_id = queue_item_id(delivered_item)
        message = deepcopy(queue_item_message(delivered_item))
        await append_history(
            QUEUE_DEFINITIONS[delivered_queue]["get_event"],
            delivered_queue,
            message,
            {"queue_item_id": item_id, **metadata},
            port,
            group_git_context_for_queue(group_data),
        )
        return {
            "message": message,
            "id": item_id,
            "queue": delivered_queue,
            "group_id": group_id,
            "agent_id": agent_id,
            "cycle_id": metadata.get("cycle_id"),
            "task_id": metadata.get("task_id"),
            "task_node_id": metadata.get("task_node_id"),
            "parent_task_id": metadata.get("parent_task_id"),
            "parent_task_node_id": metadata.get("parent_task_node_id"),
            "metadata": metadata,
        }


def git_context_metadata_from_record(record: dict[str, Any]) -> dict[str, Any]:
    metadata = record.get("metadata")
    return metadata if isinstance(metadata, dict) else {}


def git_context_key_from_metadata(metadata: dict[str, Any]) -> str:
    explicit_key = str(metadata.get("git_context_key") or "").strip()
    if explicit_key:
        return normalize_project_context_reference(explicit_key)

    git_address = metadata.get("git_address")
    if isinstance(git_address, str) and git_address.strip():
        return normalize_project_context_reference(git_address)

    return ""


def project_name_from_metadata(metadata: dict[str, Any]) -> str:
    project_name = str(metadata.get("project_name") or "").strip()
    if project_name:
        return project_name

    git_address = metadata.get("git_address")
    if isinstance(git_address, str) and git_address.strip():
        return project_name_from_git_address(git_address)

    return ""


def strip_git_context_block_from_text(text: str) -> str:
    lines = text.splitlines()
    stripped_lines: list[str] = []
    skipping = False
    skipped = False

    for line in lines:
        if not skipped and GIT_CONTEXT_HEADER_PATTERN.match(line):
            skipping = True
            skipped = True
            continue
        if skipping:
            if not line.strip():
                skipping = False
            continue
        stripped_lines.append(line)

    return "\n".join(stripped_lines).strip()


def local_repo_remote_urls(repo_path: Path) -> list[str]:
    command = ["git", "-C", str(repo_path), "remote", "-v"]
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return []

    if result.returncode != 0:
        return []

    urls: list[str] = []
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) >= 2:
            urls.append(parts[1])
    return urls


def local_repo_for_remote(git_address: str) -> Path | None:
    target = normalize_git_remote_address(git_address)
    candidates: list[Path] = [base_dir]
    workspace_root = base_dir.parent
    try:
        for child in workspace_root.iterdir():
            if child.is_dir() and (child / ".git").exists():
                candidates.append(child)
    except OSError:
        pass

    try:
        config = read_git_config_file()
    except Exception:
        config = {}

    for entry in config.values():
        if not isinstance(entry, dict):
            continue
        address = entry.get("git_address")
        if not isinstance(address, str):
            continue
        path = Path(address)
        if path.exists():
            candidates.append(path)

    seen: set[Path] = set()
    for candidate in candidates:
        try:
            resolved = candidate.resolve()
        except OSError:
            continue
        if resolved in seen or not (resolved / ".git").exists():
            continue
        seen.add(resolved)
        remotes = [normalize_git_remote_address(url) for url in local_repo_remote_urls(resolved)]
        if target in remotes:
            return resolved

    return None


def resolve_local_git_reference(git_address: str) -> dict[str, Any]:
    """Resolve HEAD without contacting a remote Git server."""
    address = git_address.strip()
    if not address:
        return {}

    local_path = Path(address)
    if not local_path.exists():
        return {}
    return resolve_git_reference(str(local_path))


def resolve_git_patch(
    git_address: str,
    to_commit: str,
    from_commit: str | None = None,
) -> dict[str, Any]:
    address = git_address.strip()
    if not address:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Git address is required for patch lookup",
        )

    local_path = Path(address)
    if local_path.exists():
        patch = git_patch_from_local_repo(local_path, to_commit, from_commit)
        return {"source": "local_git", "patch": patch}

    matching_local_repo = local_repo_for_remote(address)
    if matching_local_repo is not None:
        patch = git_patch_from_local_repo(matching_local_repo, to_commit, from_commit)
        return {
            "source": "local_git_remote_match",
            "local_repo": str(matching_local_repo),
            "patch": patch,
        }

    github_base = github_commit_base_url(address)
    if github_base:
        if from_commit:
            url = f"{github_base}/compare/{from_commit}...{to_commit}.patch"
        else:
            url = f"{github_base}/commit/{to_commit}.patch"
        return {
            "source": "github_patch_url",
            "patch_url": url,
            "patch": read_url_limited(url),
        }

    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="Only local Git repositories and GitHub repository URLs are supported",
    )


async def git_context_for_port(port: int | None) -> dict[str, Any]:
    if port is None:
        return {}

    config = await read_git_config()
    entry = config.get(str(port))
    if not isinstance(entry, dict):
        return {"fastapi_port": port}

    git_address = entry.get("git_address")
    if not isinstance(git_address, str) or not git_address.strip():
        return {"fastapi_port": port}

    project_name = normalize_project_name(entry.get("project_name"), git_address)
    git_context_key = normalize_project_context_reference(
        str(entry.get("git_context_key") or "").strip()
    )
    if not git_context_key:
        git_context_key = normalize_project_context_reference(git_address)
    git_context = await asyncio.to_thread(resolve_git_reference, git_address)
    return {
        "fastapi_port": port,
        "project_name": project_name,
        "git_context_key": git_context_key,
        "git_address": git_address,
        **git_context,
    }


async def git_context_for_phone(phone: str) -> dict[str, Any]:
    phone_key = normalize_phone_key(phone)
    if not phone_key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Phone is required for queue Git context",
        )

    config = await read_git_config()
    entry = phone_git_context_entry_from_config(config, phone_key)
    if entry is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Phone {phone_key} is not mapped to a Git context",
        )

    git_address = entry["git_address"]
    git_context = await asyncio.to_thread(resolve_git_reference, git_address)
    return {
        "fastapi_port": entry.get("fastapi_port"),
        "queue_phone": phone_key,
        "git_context_phone": phone_key,
        "project_name": entry["project_name"],
        "git_context_key": entry["git_context_key"],
        "git_address": git_address,
        **git_context,
    }


async def git_context_for_phone_if_mapped(phone: str) -> dict[str, Any] | None:
    phone_key = normalize_phone_key(phone)
    if not phone_key:
        return None

    config = await read_git_config()
    entry = phone_git_context_entry_from_config(config, phone_key)
    if entry is None:
        return None

    git_address = entry["git_address"]
    git_context = await asyncio.to_thread(resolve_git_reference, git_address)
    return {
        "fastapi_port": entry.get("fastapi_port"),
        "queue_phone": phone_key,
        "git_context_phone": phone_key,
        "project_name": entry["project_name"],
        "git_context_key": entry["git_context_key"],
        "git_address": git_address,
        **git_context,
    }


def queue_definition(queue_name: str) -> dict[str, str]:
    try:
        return QUEUE_DEFINITIONS[queue_name]
    except KeyError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Unknown queue",
        ) from exc


async def append_history(
    event: str,
    queue_name: str,
    message: Any,
    metadata: dict[str, Any] | None = None,
    port: int | None = None,
    git_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    queue_meta = queue_definition(queue_name)
    submitted_metadata = {
        key: value for key, value in (metadata or {}).items() if value is not None
    }
    history_git_context = (
        {key: value for key, value in git_context.items() if value is not None}
        if git_context is not None
        else await git_context_for_port(port)
    )
    enriched_metadata = {
        "route": queue_meta["route"],
        "context": queue_meta["context"],
        "context_label": queue_meta["context_label"],
        "sender": submitted_metadata.get("sender") or queue_meta["default_sender"],
        "receiver": submitted_metadata.get("receiver") or queue_meta["default_receiver"],
        "direction": queue_meta["label"],
        **submitted_metadata,
        **history_git_context,
    }
    record = {
        "id": str(uuid4()),
        "timestamp": utc_now(),
        "event": event,
        "context": queue_meta["context"],
        "queue": queue_name,
        "route": queue_meta["route"],
        "message": message,
        "metadata": enriched_metadata,
    }
    async with history_lock:
        await asyncio.to_thread(write_history_line, record)
    if should_release_scheduled_on_pass(message, enriched_metadata):
        await release_pass_scheduled_tasks(record, port)
    return record


def make_queue_item(message: Any, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "id": str(uuid4()),
        "queued_at": utc_now(),
        "message": message,
        "metadata": {key: value for key, value in (metadata or {}).items() if value is not None},
    }


def queue_item_message(item: Any) -> Any:
    if isinstance(item, dict) and "id" in item and "message" in item:
        return item["message"]
    return item


def queue_item_id(item: Any) -> str | None:
    if isinstance(item, dict) and isinstance(item.get("id"), str):
        return item["id"]
    return None


def queue_item_queued_at(item: Any) -> str | None:
    if isinstance(item, dict) and isinstance(item.get("queued_at"), str):
        return item["queued_at"]
    return None


def queue_item_metadata(item: Any) -> dict[str, Any]:
    if isinstance(item, dict) and isinstance(item.get("metadata"), dict):
        return item["metadata"]
    return {}


def queue_item_matches_git_context(item: Any, git_context_key: str | None) -> bool:
    if not git_context_key:
        return True
    return git_context_key_from_metadata(queue_item_metadata(item)) == git_context_key


def queue_items_for_git_context(
    items: deque[Any],
    git_context_key: str | None,
) -> list[Any]:
    return [item for item in items if queue_item_matches_git_context(item, git_context_key)]


def summarize_message(message: Any) -> str:
    if isinstance(message, str):
        text = " ".join(strip_git_context_block_from_text(message).split())
    else:
        text = json.dumps(message, ensure_ascii=False)

    return text[:180] + ("..." if len(text) > 180 else "")


def queue_item_snapshot(item: Any) -> dict[str, Any]:
    message = queue_item_message(item)
    metadata = queue_item_metadata(item)
    return {
        "id": queue_item_id(item),
        "queued_at": queue_item_queued_at(item),
        "summary": summarize_message(message),
        "metadata": metadata,
    }


def message_text_for_search(message: Any) -> str:
    if isinstance(message, str):
        return message
    return json.dumps(message, ensure_ascii=False, default=str)


def should_release_scheduled_on_pass(message: Any, metadata: dict[str, Any]) -> bool:
    if metadata.get("scheduled_release") or metadata.get("scheduled_event"):
        return False
    return bool(PASS_STATUS_PATTERN.search(message_text_for_search(message)))


def parse_schedule_delay_minutes(schedule: dict[str, Any]) -> int:
    raw_minutes = schedule.get("delay_minutes")
    try:
        minutes = int(raw_minutes)
    except (TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="delay_minutes must be a number.",
        ) from exc

    if minutes < SCHEDULE_DELAY_MINUTES_MIN:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Delay must be at least {SCHEDULE_DELAY_MINUTES_MIN} minutes.",
        )
    if minutes > SCHEDULE_DELAY_MINUTES_MAX:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Delay is too large.",
        )
    if minutes % SCHEDULE_DELAY_STEP_MINUTES != 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Delay must be divisible by {SCHEDULE_DELAY_STEP_MINUTES} minutes.",
        )
    return minutes


def scheduled_task_snapshot(task: dict[str, Any]) -> dict[str, Any]:
    queue_meta = queue_definition(str(task.get("queue", "")))
    metadata = task.get("metadata") if isinstance(task.get("metadata"), dict) else {}
    return {
        "id": task.get("id"),
        "queue": task.get("queue"),
        "route": queue_meta["route"],
        "context": queue_meta["context"],
        "context_label": queue_meta["context_label"],
        "direction": queue_meta["label"],
        "schedule_mode": task.get("schedule_mode"),
        "created_at": task.get("created_at"),
        "due_at": task.get("due_at"),
        "delay_minutes": task.get("delay_minutes"),
        "trigger_text": task.get("trigger_text"),
        "summary": summarize_message(task.get("message")),
        "metadata": {
            key: metadata.get(key)
            for key in (
                "sender",
                "receiver",
                "status",
                "submitted_via",
                "project_name",
                "git_context_key",
                "git_address",
                "git_commit_short",
            )
            if metadata.get(key) is not None
        },
    }


async def delayed_release_task(task_id: str, delay_seconds: int) -> None:
    try:
        await asyncio.sleep(delay_seconds)
        await release_scheduled_task(task_id, "delay_elapsed")
    except asyncio.CancelledError:
        return


async def release_scheduled_task(
    task_id: str,
    reason: str,
    trigger_record: dict[str, Any] | None = None,
    port: int | None = None,
) -> dict[str, Any] | None:
    async with scheduled_tasks_lock:
        task = scheduled_tasks.pop(task_id, None)
        timer = scheduled_timer_tasks.pop(task_id, None)

    if task is None:
        return None

    if timer is not None and timer is not asyncio.current_task():
        timer.cancel()

    metadata = task.get("metadata") if isinstance(task.get("metadata"), dict) else {}
    task_git_context = {
        key: metadata.get(key)
        for key in (
            "fastapi_port",
            "project_name",
            "git_context_key",
            "git_address",
            "git_commit",
            "git_commit_short",
            "git_error",
        )
        if metadata.get(key) is not None
    }
    release_metadata = {
        **metadata,
        "submitted_via": "scheduled_release",
        "scheduled_release": True,
        "scheduled_task_id": task_id,
        "scheduled_mode": task.get("schedule_mode"),
        "scheduled_created_at": task.get("created_at"),
        "scheduled_due_at": task.get("due_at"),
        "scheduled_release_reason": reason,
    }
    if trigger_record is not None:
        release_metadata["scheduled_trigger_record_id"] = trigger_record.get("id")
        release_metadata["scheduled_trigger_event"] = trigger_record.get("event")
        release_metadata["scheduled_trigger_queue"] = trigger_record.get("queue")

    return await enqueue(
        str(task["queue"]),
        task.get("message"),
        release_metadata,
        task.get("port", port),
        task_git_context,
    )


async def release_pass_scheduled_tasks(
    trigger_record: dict[str, Any],
    port: int | None = None,
) -> list[dict[str, Any]]:
    async with scheduled_tasks_lock:
        trigger_context_key = git_context_key_from_metadata(
            git_context_metadata_from_record(trigger_record)
        )
        task_ids = [
            task_id
            for task_id, task in scheduled_tasks.items()
            if task.get("schedule_mode") == "pass"
            and (
                not trigger_context_key
                or git_context_key_from_metadata(
                    task.get("metadata") if isinstance(task.get("metadata"), dict) else {}
                )
                == trigger_context_key
            )
        ]

    released: list[dict[str, Any]] = []
    for task_id in task_ids:
        result = await release_scheduled_task(task_id, "status_pass", trigger_record, port)
        if result is not None:
            released.append(result)
    return released


async def schedule_message(
    queue_name: str,
    message: Any,
    metadata: dict[str, Any] | None = None,
    port: int | None = None,
    schedule: dict[str, Any] | None = None,
    git_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    schedule = schedule if isinstance(schedule, dict) else {}
    mode = str(schedule.get("mode") or "now").strip().lower()
    if mode in {"", "now", "immediate"}:
        return await enqueue(queue_name, message, metadata, port, git_context)

    if mode not in {"delay", "pass"}:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Unknown schedule mode.",
        )

    task_id = str(uuid4())
    created_at = datetime.now(timezone.utc)
    task: dict[str, Any] = {
        "id": task_id,
        "queue": queue_name,
        "message": message,
        "metadata": {
            key: value
            for key, value in {
                **(metadata or {}),
                **(git_context or {}),
            }.items()
            if value is not None
        },
        "port": port,
        "schedule_mode": mode,
        "created_at": created_at.isoformat(),
    }

    if mode == "delay":
        delay_minutes = parse_schedule_delay_minutes(schedule)
        due_at = created_at + timedelta(minutes=delay_minutes)
        task["delay_minutes"] = delay_minutes
        task["due_at"] = due_at.isoformat()
    else:
        task["trigger_text"] = "STATUS: PASS"

    async with scheduled_tasks_lock:
        scheduled_tasks[task_id] = task
        if mode == "delay":
            scheduled_timer_tasks[task_id] = asyncio.create_task(
                delayed_release_task(task_id, int(task["delay_minutes"]) * 60)
            )

    await append_history(
        "scheduled_for_delay" if mode == "delay" else "scheduled_waiting_for_pass",
        queue_name,
        message,
        {
            **(metadata or {}),
            "scheduled_event": "created",
            "scheduled_task_id": task_id,
            "scheduled_mode": mode,
            "scheduled_due_at": task.get("due_at"),
            "scheduled_delay_minutes": task.get("delay_minutes"),
            "scheduled_trigger_text": task.get("trigger_text"),
        },
        port,
        git_context,
    )
    return {
        "status": "scheduled",
        "queue": queue_name,
        "id": task_id,
        "task": scheduled_task_snapshot(task),
    }


async def list_scheduled_tasks(git_context_key: str | None = None) -> list[dict[str, Any]]:
    async with scheduled_tasks_lock:
        tasks = [
            scheduled_task_snapshot(task)
            for task in scheduled_tasks.values()
            if not git_context_key
            or git_context_key_from_metadata(
                task.get("metadata") if isinstance(task.get("metadata"), dict) else {}
            )
            == git_context_key
        ]

    return sorted(
        tasks,
        key=lambda item: (
            item.get("due_at") or "9999-12-31T23:59:59+00:00",
            item.get("created_at") or "",
        ),
    )


async def cancel_scheduled_task(
    task_id: str,
    port: int | None = None,
) -> dict[str, Any]:
    async with scheduled_tasks_lock:
        task = scheduled_tasks.pop(task_id, None)
        timer = scheduled_timer_tasks.pop(task_id, None)

    if task is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Scheduled task not found",
        )

    if timer is not None:
        timer.cancel()

    metadata = task.get("metadata") if isinstance(task.get("metadata"), dict) else {}
    task_git_context = {
        key: metadata.get(key)
        for key in (
            "fastapi_port",
            "project_name",
            "git_context_key",
            "git_address",
            "git_commit",
            "git_commit_short",
            "git_error",
        )
        if metadata.get(key) is not None
    }
    await append_history(
        "scheduled_cancelled",
        str(task["queue"]),
        task.get("message"),
        {
            **metadata,
            "scheduled_event": "cancelled",
            "scheduled_task_id": task_id,
            "scheduled_mode": task.get("schedule_mode"),
        },
        port,
        task_git_context,
    )
    return {
        "status": "cancelled",
        "id": task_id,
        "queue": task.get("queue"),
        "task": scheduled_task_snapshot(task),
    }


def record_matches_date_range(
    record: dict[str, Any],
    start_at: datetime | None,
    end_before: datetime | None,
) -> bool:
    record_time = parse_history_timestamp(record.get("timestamp"))
    if record_time is None:
        return False

    if start_at is not None and record_time < start_at:
        return False

    if end_before is not None and record_time >= end_before:
        return False

    return True


def normalize_history_git_context_filter(value: str | None) -> str:
    if not value:
        return ""
    clean_value = value.strip()
    if not clean_value or clean_value.lower() in {"all", "*"}:
        return ""
    return normalize_git_context_key(clean_value)


def record_matches_git_context(
    record: dict[str, Any],
    git_context_key: str,
) -> bool:
    if not git_context_key:
        return True

    metadata = git_context_metadata_from_record(record)
    return git_context_key_from_metadata(metadata) == git_context_key


def read_history_file(
    limit: int,
    date_from: str | None = None,
    date_to: str | None = None,
    git_context: str | None = None,
) -> list[dict[str, Any]]:
    if not history_path.exists():
        return []

    with history_path.open("r", encoding="utf-8") as file:
        lines = file.readlines()

    start_at = parse_date_boundary(date_from or local_today_iso(), end=False)
    end_before = parse_date_boundary(date_to, end=True) if date_to else None
    git_context_key = normalize_history_git_context_filter(git_context)
    records: list[dict[str, Any]] = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            record = {
                "id": str(uuid4()),
                "timestamp": utc_now(),
                "event": "history_read_error",
                "queue": "system",
                "message": line,
                "metadata": {"error": "Invalid JSONL record"},
            }

        if record_matches_date_range(record, start_at, end_before):
            if record_matches_git_context(record, git_context_key):
                records.append(record)

    return records[-limit:]


async def read_history(
    limit: int = 200,
    date_from: str | None = None,
    date_to: str | None = None,
    git_context: str | None = None,
) -> list[dict[str, Any]]:
    safe_limit = max(1, min(limit, 10000))
    async with history_lock:
        return await asyncio.to_thread(
            read_history_file,
            safe_limit,
            date_from,
            date_to,
            git_context,
        )


def history_record_message_for_context(record: dict[str, Any]) -> str:
    message = record.get("message")
    if isinstance(message, str):
        direct_message = message
    elif message is None:
        direct_message = ""
    else:
        direct_message = json.dumps(
            message,
            ensure_ascii=False,
            indent=2,
            default=str,
        )
    if direct_message.strip():
        return direct_message

    metadata = (
        record.get("metadata")
        if isinstance(record.get("metadata"), dict)
        else {}
    )
    payload = metadata.get("cycle_event_payload")
    if record.get("event") == "cycle_lifecycle_event" and isinstance(payload, dict):
        event_type = str(
            metadata.get("cycle_event_type") or "CYCLE_EVENT"
        ).strip().upper()
        return "\n".join(
            [
                f"EVENT: {event_type}",
                "",
                "PAYLOAD:",
                json.dumps(payload, ensure_ascii=False, indent=2, default=str),
            ]
        )
    return direct_message


def history_record_revision_for_context(
    record: dict[str, Any],
) -> tuple[str, str]:
    metadata = (
        record.get("metadata")
        if isinstance(record.get("metadata"), dict)
        else {}
    )
    commit = str(
        metadata.get("git_commit_short") or metadata.get("git_commit") or ""
    ).strip()
    if commit:
        return "Commit", commit

    payload = metadata.get("cycle_event_payload")
    payload = payload if isinstance(payload, dict) else {}
    artifact = payload.get("artifact")
    artifact = artifact if isinstance(artifact, dict) else {}
    artifact_ref = str(artifact.get("ref") or "").strip()
    if artifact_ref:
        return "Ref", artifact_ref
    for candidate in [
        artifact.get("artifact_id"),
        artifact.get("path"),
        *(artifact.get("paths") if isinstance(artifact.get("paths"), list) else []),
    ]:
        if candidate is not None and str(candidate).strip():
            return "Artifact", str(candidate).strip()
    cycle_id = str(metadata.get("cycle_id") or "").strip()
    if record.get("event") == "cycle_lifecycle_event" and cycle_id:
        return "Cycle", cycle_id
    return "Commit", "no-commit"


def format_history_record_for_context(record: dict[str, Any]) -> str:
    metadata = (
        record.get("metadata")
        if isinstance(record.get("metadata"), dict)
        else {}
    )
    revision_label, revision_value = history_record_revision_for_context(record)
    project = str(metadata.get("project_name") or "no-project")
    actor = " -> ".join(
        str(value).strip()
        for value in (metadata.get("sender"), metadata.get("receiver"))
        if str(value or "").strip()
    )
    if not actor:
        actor = str(
            metadata.get("direction")
            or record.get("route")
            or record.get("queue")
            or "unknown"
        )
    event_label = str(metadata.get("cycle_event_type") or record.get("event") or "")
    timestamp = str(record.get("timestamp") or "")
    message = history_record_message_for_context(record)
    return (
        f"[{timestamp}] [Project: {project}] "
        f"[{revision_label}: {revision_value}] [{actor}] [{event_label}]\n"
        f"{message}"
    )


def history_commit_info(record: dict[str, Any]) -> dict[str, str]:
    metadata = (
        record.get("metadata")
        if isinstance(record.get("metadata"), dict)
        else {}
    )
    full = str(
        metadata.get("git_commit") or metadata.get("git_commit_short") or ""
    ).strip()
    short = str(metadata.get("git_commit_short") or full[:12]).strip()
    return {"full": full, "short": short}


def history_patch_block_header(
    previous_commit: dict[str, str],
    current_commit: dict[str, str],
    git_address: str,
    patch_data: dict[str, Any] | None = None,
) -> str:
    lines = [
        "[PATCH BETWEEN COMMITS]",
        f"From: {previous_commit.get('short') or previous_commit['full'][:12]}",
        f"To: {current_commit.get('short') or current_commit['full'][:12]}",
    ]
    if git_address:
        lines.append(f"Repository: {git_address}")
    if patch_data and patch_data.get("source"):
        lines.append(f"Source: {patch_data['source']}")
    if patch_data and patch_data.get("patch_url"):
        lines.append(f"Patch URL: {patch_data['patch_url']}")
    return "\n".join(lines)


def history_with_patches_context(
    records: list[dict[str, Any]],
    git_address: str,
) -> dict[str, Any]:
    sections: list[str] = []
    timeline: list[dict[str, Any]] = []
    patches: list[dict[str, Any]] = []
    used_patch_keys: set[tuple[str, str, str]] = set()
    previous_commit: dict[str, str] | None = None
    patch_count = 0
    patch_error_count = 0

    for record in records:
        current_commit = history_commit_info(record)
        if (
            previous_commit is not None
            and current_commit["full"]
            and previous_commit["full"].casefold()
            != current_commit["full"].casefold()
        ):
            key = (
                git_address,
                previous_commit["full"].casefold(),
                current_commit["full"].casefold(),
            )
            if key not in used_patch_keys:
                used_patch_keys.add(key)
                patch_entry: dict[str, Any] = {
                    "type": "patch",
                    "from_commit": deepcopy(previous_commit),
                    "to_commit": deepcopy(current_commit),
                    "git_address": git_address,
                    "inserted_before_activity_id": record.get("id"),
                }
                try:
                    if not git_address:
                        raise ValueError("Git address is missing in project context")
                    safe_from_commit = normalize_commit_ref(previous_commit["full"])
                    safe_to_commit = normalize_commit_ref(current_commit["full"])
                    cached_patch = project_state_patch_cache.get(key)
                    if cached_patch is None:
                        cached_patch = resolve_git_patch(
                            git_address,
                            safe_to_commit,
                            safe_from_commit,
                        )
                        if len(project_state_patch_cache) >= PROJECT_STATE_PATCH_CACHE_MAX:
                            project_state_patch_cache.pop(
                                next(iter(project_state_patch_cache)),
                                None,
                            )
                        project_state_patch_cache[key] = deepcopy(cached_patch)
                    patch_data = deepcopy(cached_patch)
                    patch_text = str(patch_data.get("patch") or "")
                    patch_entry.update(
                        {
                            "status": "available",
                            "source": patch_data.get("source"),
                            "patch_url": patch_data.get("patch_url"),
                            "local_repo": patch_data.get("local_repo"),
                            "patch": patch_text,
                        }
                    )
                    sections.append(
                        "\n\n".join(
                            [
                                history_patch_block_header(
                                    previous_commit,
                                    current_commit,
                                    git_address,
                                    patch_data,
                                ),
                                patch_text,
                            ]
                        )
                        + "\n[END PATCH]"
                    )
                    patch_count += 1
                except Exception as exc:
                    reason: Any = exc
                    if isinstance(exc, HTTPException):
                        reason = exc.detail
                    if isinstance(reason, (dict, list)):
                        reason = json.dumps(reason, ensure_ascii=False, default=str)
                    reason_text = str(reason)
                    patch_entry.update(
                        {
                            "status": "unavailable",
                            "reason": reason_text,
                            "patch": "",
                        }
                    )
                    sections.append(
                        history_patch_block_header(
                            previous_commit,
                            current_commit,
                            git_address,
                        )
                        + "\nStatus: unavailable\n"
                        + f"Reason: {reason_text}\n[END PATCH]"
                    )
                    patch_error_count += 1
                patches.append(patch_entry)
                timeline.append(deepcopy(patch_entry))

        sections.append(format_history_record_for_context(record))
        timeline.append(
            {
                "type": "activity",
                "activity": deepcopy(record),
            }
        )
        if current_commit["full"]:
            previous_commit = current_commit

    return {
        "text": "\n\n---\n\n".join(sections),
        "record_count": len(records),
        "patch_count": patch_count,
        "patch_error_count": patch_error_count,
        "deduplicated_by_commit_pair": True,
        "timeline": timeline,
        "patches": patches,
    }


async def project_history_with_patches_context(
    git_context_key: str,
    git_address: str,
    *,
    limit: int = 10000,
    assignment_id: str | None = None,
) -> dict[str, Any]:
    records = await read_history(
        limit=limit,
        date_from="1970-01-01",
        git_context=git_context_key,
    )
    if assignment_id:
        records = [
            record
            for record in records
            if str(
                (
                    record.get("metadata")
                    if isinstance(record.get("metadata"), dict)
                    else {}
                ).get("assignment_id")
                or ""
            ).strip()
            == assignment_id
        ]
    return await asyncio.to_thread(
        history_with_patches_context,
        records,
        git_address,
    )


def agent_history_direction(
    record: dict[str, Any],
    agent: dict[str, Any],
) -> str:
    metadata = record.get("metadata") if isinstance(record.get("metadata"), dict) else {}
    agent_id = str(agent.get("id") or "").strip()
    agent_phone = str(agent.get("phone") or "").strip()
    agent_name = str(agent.get("name") or "").strip().casefold()

    from_agent_id = str(
        metadata.get("from_agent_id") or record.get("from_agent_id") or ""
    ).strip()
    to_agent_id = str(
        metadata.get("to_agent_id") or record.get("to_agent_id") or ""
    ).strip()
    from_phone = str(metadata.get("from_phone") or record.get("from_phone") or "").strip()
    to_phone = str(metadata.get("to_phone") or record.get("to_phone") or "").strip()
    sender = str(metadata.get("sender") or record.get("sender") or "").strip().casefold()
    receiver = str(metadata.get("receiver") or record.get("receiver") or "").strip().casefold()
    from_matches = bool(
        (agent_id and from_agent_id == agent_id)
        or (agent_phone and from_phone == agent_phone)
        or (agent_name and sender == agent_name)
    )
    to_matches = bool(
        (agent_id and to_agent_id == agent_id)
        or (agent_phone and to_phone == agent_phone)
        or (agent_name and receiver == agent_name)
    )
    if from_matches and to_matches:
        return "self"
    if from_matches:
        return "sent"
    if to_matches:
        return "received"
    return ""


def read_agent_work_history_file(
    agent: dict[str, Any],
    git_context_key: str,
    created_at: str | None = None,
) -> list[dict[str, Any]]:
    if not history_path.exists():
        return []
    created_time = parse_utc_datetime(created_at)
    records: list[dict[str, Any]] = []
    with history_path.open("r", encoding="utf-8") as file:
        for raw_line in file:
            line = raw_line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(record, dict):
                continue
            if git_context_key and not record_matches_git_context(
                record,
                git_context_key,
            ):
                continue
            record_time = parse_history_timestamp(record.get("timestamp"))
            if created_time is not None and (
                record_time is None or record_time < created_time
            ):
                continue
            direction = agent_history_direction(record, agent)
            if not direction:
                continue
            public_record = deepcopy(record)
            public_record["agent_direction"] = direction
            records.append(public_record)
    return records


def agent_work_history_summary(
    history: list[dict[str, Any]],
    assigned_task_count: int,
) -> dict[str, Any]:
    event_counts: dict[str, int] = {}
    task_ids: set[str] = set()
    direction_counts = {"sent": 0, "received": 0, "self": 0}
    for record in history:
        event = str(record.get("event") or "unknown").strip() or "unknown"
        event_counts[event] = event_counts.get(event, 0) + 1
        direction = str(record.get("agent_direction") or "").strip()
        if direction in direction_counts:
            direction_counts[direction] += 1
        metadata = record.get("metadata") if isinstance(record.get("metadata"), dict) else {}
        task_id = str(metadata.get("task_id") or record.get("task_id") or "").strip()
        if task_id:
            task_ids.add(task_id)
    return {
        "assigned_task_count": assigned_task_count,
        "history_event_count": len(history),
        "sent_event_count": direction_counts["sent"],
        "received_event_count": direction_counts["received"],
        "self_event_count": direction_counts["self"],
        "known_task_ids": sorted(task_ids),
        "first_activity_at": history[0].get("timestamp") if history else None,
        "last_activity_at": history[-1].get("timestamp") if history else None,
        "event_counts": dict(sorted(event_counts.items())),
    }


def cycle_metadata_from_record(record: dict[str, Any]) -> dict[str, Any]:
    metadata = record.get("metadata")
    return metadata if isinstance(metadata, dict) else {}


def cycle_id_from_record(record: dict[str, Any]) -> str:
    metadata = cycle_metadata_from_record(record)
    return str(metadata.get("cycle_id") or record.get("cycle_id") or "").strip()


def project_phone_from_cycle_record(record: dict[str, Any]) -> str:
    metadata = cycle_metadata_from_record(record)
    return normalize_project_phone(
        metadata.get("project_phone")
        or metadata.get("project_id")
        or record.get("project_id")
    )


def _read_cycle_records_file_unlocked(
    *,
    cycle_id: str = "",
    project_phone: str = "",
) -> list[dict[str, Any]]:
    if not history_path.exists():
        return []
    clean_cycle_id = normalized_cycle_id(cycle_id) if cycle_id else ""
    clean_project_phone = normalize_project_phone(project_phone)
    records: list[dict[str, Any]] = []
    try:
        with history_path.open("r", encoding="utf-8") as file:
            for line_number, line in enumerate(file, start=1):
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    record = json.loads(stripped)
                except json.JSONDecodeError:
                    continue
                if not isinstance(record, dict):
                    continue
                record_cycle_id = cycle_id_from_record(record)
                if not record_cycle_id:
                    continue
                if clean_cycle_id and record_cycle_id != clean_cycle_id:
                    continue
                if (
                    clean_project_phone
                    and project_phone_from_cycle_record(record)
                    != clean_project_phone
                ):
                    continue
                stored = deepcopy(record)
                stored["_cycle_log_sequence"] = line_number
                records.append(stored)
    except OSError:
        return []
    return records


def read_cycle_records_file(
    *,
    cycle_id: str = "",
    project_phone: str = "",
) -> list[dict[str, Any]]:
    lock_path = history_path.with_name(f"{history_path.name}.lock")
    with interprocess_file_lock(lock_path):
        return _read_cycle_records_file_unlocked(
            cycle_id=cycle_id,
            project_phone=project_phone,
        )


async def read_cycle_records(
    *,
    cycle_id: str = "",
    project_phone: str = "",
) -> list[dict[str, Any]]:
    async with history_lock:
        return await asyncio.to_thread(
            read_cycle_records_file,
            cycle_id=cycle_id,
            project_phone=project_phone,
        )


def cycle_agent_snapshot(
    metadata: dict[str, Any],
    prefix: str,
) -> dict[str, Any] | None:
    agent_id = str(metadata.get(f"{prefix}_agent_id") or "").strip()
    role = str(
        metadata.get(f"{prefix}_role")
        or metadata.get("sender" if prefix == "from" else "receiver")
        or ""
    ).strip()
    phone = str(metadata.get(f"{prefix}_phone") or "").strip()
    if not agent_id and not role and not phone:
        return None
    return {
        "id": agent_id or None,
        "role": role or None,
        "phone": phone or None,
    }


def cycle_group_ids_from_metadata(metadata: dict[str, Any]) -> list[str]:
    values = [
        str(metadata.get("source_group_id") or "").strip(),
        str(metadata.get("group_id") or "").strip(),
        str(metadata.get("target_group_id") or "").strip(),
    ]
    return list(dict.fromkeys(value for value in values if value))


def cycle_event_record(
    *,
    event_id: str,
    timestamp: str,
    cycle_id: str,
    project_id: str,
    event_type: str,
    metadata: dict[str, Any],
    source_record_id: str,
    payload: dict[str, Any],
    group_id: str = "",
    source_group_id: str = "",
    target_group_id: str = "",
) -> dict[str, Any]:
    from_agent = cycle_agent_snapshot(metadata, "from")
    to_agent = cycle_agent_snapshot(metadata, "to")
    return {
        "event_id": event_id,
        "timestamp": timestamp,
        "cycle_id": cycle_id,
        "project_id": project_id,
        "group_id": group_id or None,
        "source_group_id": source_group_id or None,
        "target_group_id": target_group_id or group_id or None,
        "event_type": event_type,
        "task_id": metadata.get("task_id"),
        "task_node_id": metadata.get("task_node_id"),
        "parent_task_id": metadata.get("parent_task_id"),
        "parent_task_node_id": metadata.get("parent_task_node_id"),
        "request_id": metadata.get("request_id")
        or metadata.get("cycle_event_request_id"),
        "connection_id": metadata.get("connection_id"),
        "queue": metadata.get("phone_channel"),
        "queue_item_id": metadata.get("queue_item_id"),
        "channel": metadata.get("route"),
        "send_endpoint": metadata.get("send_endpoint"),
        "receive_endpoint": metadata.get("receive_endpoint"),
        "from_agent": from_agent,
        "to_agent": to_agent,
        "from_agent_id": from_agent.get("id") if from_agent else None,
        "to_agent_id": to_agent.get("id") if to_agent else None,
        "source_record_id": source_record_id,
        "payload": payload,
    }


def cycle_events_from_records(
    records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not records:
        return []
    ordered_records = sorted(
        records,
        key=lambda record: int(record.get("_cycle_log_sequence") or 0),
    )
    events: list[dict[str, Any]] = []
    started_cycles: set[str] = set()
    deployed_groups: set[tuple[str, str]] = set()
    for record in ordered_records:
        metadata = cycle_metadata_from_record(record)
        cycle_id = cycle_id_from_record(record)
        if not cycle_id:
            continue
        project_id = project_phone_from_cycle_record(record)
        timestamp = str(record.get("timestamp") or "")
        source_record_id = str(record.get("id") or uuid4())
        group_ids = cycle_group_ids_from_metadata(metadata)
        group_id = str(metadata.get("group_id") or "").strip()
        source_group_id = str(metadata.get("source_group_id") or "").strip()
        target_group_id = str(
            metadata.get("target_group_id") or group_id
        ).strip()

        if cycle_id not in started_cycles:
            started_cycles.add(cycle_id)
            events.append(
                cycle_event_record(
                    event_id=f"{source_record_id}:cycle-started",
                    timestamp=timestamp,
                    cycle_id=cycle_id,
                    project_id=project_id,
                    event_type="CYCLE_STARTED",
                    metadata=metadata,
                    source_record_id=source_record_id,
                    group_id=group_id,
                    source_group_id=source_group_id,
                    target_group_id=target_group_id,
                    payload={
                        "cycle_title": metadata.get("cycle_title"),
                        "cycle_origin": metadata.get("cycle_origin"),
                        "root_task_id": metadata.get("task_id"),
                        "root_task_node_id": metadata.get("task_node_id"),
                    },
                )
            )

        for deployed_group_id in group_ids:
            deployed_key = (cycle_id, deployed_group_id)
            if deployed_key in deployed_groups:
                continue
            deployed_groups.add(deployed_key)
            events.append(
                cycle_event_record(
                    event_id=f"{source_record_id}:group:{deployed_group_id}",
                    timestamp=timestamp,
                    cycle_id=cycle_id,
                    project_id=project_id,
                    event_type="GROUP_DEPLOYED",
                    metadata=metadata,
                    source_record_id=source_record_id,
                    group_id=deployed_group_id,
                    source_group_id=source_group_id,
                    target_group_id=target_group_id,
                    payload={
                        "binding": "bound_to_cycle",
                        "group_id": deployed_group_id,
                    },
                )
            )

        lifecycle_event_type = str(
            metadata.get("cycle_event_type") or ""
        ).strip().upper()
        if lifecycle_event_type:
            raw_payload = metadata.get("cycle_event_payload")
            lifecycle_payload = (
                deepcopy(raw_payload) if isinstance(raw_payload, dict) else {}
            )
            if record.get("message") is not None:
                lifecycle_payload.setdefault("message", deepcopy(record.get("message")))
            events.append(
                cycle_event_record(
                    event_id=source_record_id,
                    timestamp=timestamp,
                    cycle_id=cycle_id,
                    project_id=project_id,
                    event_type=lifecycle_event_type,
                    metadata=metadata,
                    source_record_id=source_record_id,
                    group_id=group_id,
                    source_group_id=source_group_id,
                    target_group_id=target_group_id,
                    payload=lifecycle_payload,
                )
            )
            continue

        raw_event = str(record.get("event") or "").strip()
        if raw_event.startswith("queued_to_"):
            if str(metadata.get("submitted_via") or "").strip() == (
                "group_connection_api"
            ):
                events.append(
                    cycle_event_record(
                        event_id=f"{source_record_id}:handoff",
                        timestamp=timestamp,
                        cycle_id=cycle_id,
                        project_id=project_id,
                        event_type="HANDOFF_TRIGGERED",
                        metadata=metadata,
                        source_record_id=source_record_id,
                        group_id=source_group_id or group_id,
                        source_group_id=source_group_id,
                        target_group_id=target_group_id,
                        payload={
                            "event": metadata.get("event"),
                            "message": deepcopy(record.get("message")),
                        },
                    )
                )
            event_type = "MESSAGE_QUEUED"
        elif raw_event.startswith("delivered_to_"):
            event_type = "TASK_STARTED"
        elif raw_event.startswith("removed_from_"):
            event_type = "MESSAGE_REMOVED"
        else:
            continue
        events.append(
            cycle_event_record(
                event_id=source_record_id,
                timestamp=timestamp,
                cycle_id=cycle_id,
                project_id=project_id,
                event_type=event_type,
                metadata=metadata,
                source_record_id=source_record_id,
                group_id=group_id,
                source_group_id=source_group_id,
                target_group_id=target_group_id,
                payload={
                    "message": deepcopy(record.get("message")),
                    "transport_event": raw_event,
                    "transport_metadata": deepcopy(metadata),
                },
            )
        )

    for sequence, event in enumerate(events, start=1):
        event["sequence"] = sequence
    return events


def cycle_summary_from_records(
    records: list[dict[str, Any]],
) -> dict[str, Any] | None:
    events = cycle_events_from_records(records)
    if not events:
        return None
    cycle_id = str(events[0]["cycle_id"])
    project_id = str(events[0].get("project_id") or "")
    event_types = [str(event.get("event_type") or "") for event in events]
    completed_events = [
        event for event in events if event.get("event_type") == "CYCLE_COMPLETED"
    ]
    if completed_events:
        cycle_status = "completed"
    elif any(
        event_type
        in {
            "TASK_STARTED",
            "HANDOFF_TRIGGERED",
            "ARTIFACT_CREATED",
            "GROUP_REPORT_SUBMITTED",
        }
        for event_type in event_types
    ):
        cycle_status = "in_progress"
    else:
        cycle_status = "queued"

    group_ids = list(
        dict.fromkeys(
            str(group_id)
            for event in events
            for group_id in (
                event.get("source_group_id"),
                event.get("target_group_id"),
                event.get("group_id"),
            )
            if group_id
        )
    )
    task_keys: list[str] = []
    task_ids: list[str] = []
    for event in events:
        task_id = str(event.get("task_id") or "").strip()
        task_node = str(event.get("task_node_id") or "").strip()
        if task_id and task_id not in task_ids:
            task_ids.append(task_id)
        key = task_node or task_id
        if key and key not in task_keys:
            task_keys.append(key)

    first_record_metadata = cycle_metadata_from_record(records[0])
    started_event = next(
        (event for event in events if event.get("event_type") == "CYCLE_STARTED"),
        events[0],
    )
    root_task_id = started_event.get("task_id") or first_record_metadata.get("task_id")
    root_task_node_id = (
        started_event.get("task_node_id")
        or first_record_metadata.get("task_node_id")
    )
    return {
        "cycle_id": cycle_id,
        "project_id": project_id,
        "project_phone": project_id,
        "git_context_key": first_record_metadata.get("git_context_key"),
        "title": first_record_metadata.get("cycle_title")
        or started_event.get("payload", {}).get("cycle_title")
        or root_task_id
        or cycle_id,
        "status": cycle_status,
        "root_task_id": root_task_id,
        "root_task_node_id": root_task_node_id,
        "root_group_id": started_event.get("target_group_id")
        or started_event.get("group_id"),
        "request_id": started_event.get("request_id"),
        "group_ids": group_ids,
        "task_ids": task_ids,
        "task_count": len(task_keys),
        "event_count": len(events),
        "handoff_count": event_types.count("HANDOFF_TRIGGERED"),
        "artifact_count": event_types.count("ARTIFACT_CREATED"),
        "report_count": event_types.count("GROUP_REPORT_SUBMITTED"),
        "cancelled_task_count": sum(
            1
            for event in events
            if event.get("event_type") == "MESSAGE_REMOVED"
            and isinstance(event.get("payload"), dict)
            and isinstance(event["payload"].get("transport_metadata"), dict)
            and event["payload"]["transport_metadata"].get("action")
            == "cancelled_by_cycle_completion"
        ),
        "started_at": events[0].get("timestamp"),
        "updated_at": events[-1].get("timestamp"),
        "completed_at": completed_events[-1].get("timestamp")
        if completed_events
        else None,
        "last_event_type": events[-1].get("event_type"),
    }


def cycle_summaries_from_records(
    records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        cycle_id = cycle_id_from_record(record)
        if cycle_id:
            grouped.setdefault(cycle_id, []).append(record)
    summaries = [
        summary
        for cycle_records in grouped.values()
        if (summary := cycle_summary_from_records(cycle_records)) is not None
    ]
    return sorted(
        summaries,
        key=lambda summary: (
            str(summary.get("started_at") or ""),
            str(summary.get("cycle_id") or ""),
        ),
        reverse=True,
    )


def cycle_not_found(cycle_id: str) -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail={"error": "cycle_not_found", "cycle_id": cycle_id},
    )


async def cycle_records_and_summary(
    cycle_id: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    clean_cycle_id = normalized_cycle_id(cycle_id, required=True)
    records = await read_cycle_records(cycle_id=clean_cycle_id)
    summary = cycle_summary_from_records(records)
    if summary is None:
        raise cycle_not_found(clean_cycle_id)
    return records, summary


def matching_cycle_task_records(
    records: list[dict[str, Any]],
    *,
    task_id: str = "",
    task_node_id: str = "",
    group_id: str = "",
    to_agent_id: str = "",
) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    for record in records:
        metadata = cycle_metadata_from_record(record)
        if task_node_id and str(metadata.get("task_node_id") or "") != task_node_id:
            continue
        if task_id and str(metadata.get("task_id") or "") != task_id:
            continue
        if group_id and str(metadata.get("group_id") or "") != group_id:
            continue
        if to_agent_id and str(metadata.get("to_agent_id") or "") != to_agent_id:
            continue
        if task_id or task_node_id:
            matches.append(record)
    return matches


def task_belongs_to_cycle(
    records: list[dict[str, Any]],
    *,
    task_id: str = "",
    task_node_id: str = "",
    group_id: str = "",
    to_agent_id: str = "",
) -> bool:
    return bool(
        matching_cycle_task_records(
            records,
            task_id=task_id,
            task_node_id=task_node_id,
            group_id=group_id,
            to_agent_id=to_agent_id,
        )
    )


def canonicalize_cycle_task_reference(
    records: list[dict[str, Any]],
    normalized_payload: dict[str, Any],
) -> None:
    task_id = str(normalized_payload.get("task_id") or "").strip()
    task_node_id = str(normalized_payload.get("task_node_id") or "").strip()
    if not task_id and not task_node_id:
        return
    group_id = str(normalized_payload.get("group_id") or "").strip()
    matches = matching_cycle_task_records(
        records,
        task_id=task_id,
        task_node_id=task_node_id,
        group_id=group_id,
    )
    if not matches:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "task_not_in_cycle",
                "cycle_id": cycle_id_from_record(records[0]) if records else None,
                "group_id": group_id or None,
                "task_id": task_id or None,
                "task_node_id": task_node_id or None,
            },
        )
    canonical_nodes = {
        str(cycle_metadata_from_record(record).get("task_node_id") or "")
        for record in matches
    }
    canonical_nodes.discard("")
    if not task_node_id and len(canonical_nodes) > 1:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "task_reference_ambiguous",
                "task_id": task_id,
                "candidate_task_node_ids": sorted(canonical_nodes),
            },
        )
    canonical_task_ids = {
        str(cycle_metadata_from_record(record).get("task_id") or "")
        for record in matches
    }
    canonical_task_ids.discard("")
    if not task_id and len(canonical_task_ids) > 1:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "task_reference_ambiguous",
                "task_node_id": task_node_id,
                "candidate_task_ids": sorted(canonical_task_ids),
            },
        )
    if not task_node_id and canonical_nodes:
        normalized_payload["task_node_id"] = next(iter(canonical_nodes))
    if not task_id and canonical_task_ids:
        normalized_payload["task_id"] = next(iter(canonical_task_ids))


async def ensure_cycle_can_receive_task(
    cycle_id: str,
    project_phone: str,
    *,
    allow_create: bool,
    request_scope: str = "",
    request_id: str = "",
    parent_task_id: str = "",
    parent_task_node_id: str = "",
    parent_group_id: str = "",
    parent_agent_id: str = "",
) -> tuple[str, str]:
    clean_cycle_id = normalized_cycle_id(cycle_id, required=True)
    clean_project_phone = normalize_project_phone(project_phone)
    records = await read_cycle_records(cycle_id=clean_cycle_id)
    summary = cycle_summary_from_records(records)
    if summary is None:
        if allow_create:
            return parent_task_id, parent_task_node_id
        raise cycle_not_found(clean_cycle_id)
    if str(summary.get("project_id") or "") != clean_project_phone:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "cycle_project_conflict",
                "cycle_id": clean_cycle_id,
                "expected_project_id": summary.get("project_id"),
                "received_project_id": clean_project_phone,
            },
        )
    if parent_task_id or parent_task_node_id:
        parent_matches = matching_cycle_task_records(
            records,
            task_id=parent_task_id,
            task_node_id=parent_task_node_id,
            group_id=parent_group_id,
            to_agent_id=parent_agent_id,
        )
        if not parent_matches:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "error": "parent_task_not_in_cycle",
                    "cycle_id": clean_cycle_id,
                    "parent_task_id": parent_task_id or None,
                    "parent_task_node_id": parent_task_node_id or None,
                    "expected_group_id": parent_group_id or None,
                    "expected_agent_id": parent_agent_id or None,
                },
            )
        if parent_agent_id and not any(
            str(record.get("event") or "").startswith("delivered_to_")
            for record in parent_matches
        ):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "error": "parent_task_not_started",
                    "cycle_id": clean_cycle_id,
                    "parent_task_id": parent_task_id or None,
                    "parent_task_node_id": parent_task_node_id or None,
                },
            )
        matching_nodes = {
            str(cycle_metadata_from_record(record).get("task_node_id") or "")
            for record in parent_matches
        }
        matching_nodes.discard("")
        if not parent_task_node_id and len(matching_nodes) > 1:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "error": "parent_task_ambiguous",
                    "cycle_id": clean_cycle_id,
                    "parent_task_id": parent_task_id,
                    "candidate_task_node_ids": sorted(matching_nodes),
                },
            )
        if not parent_task_node_id and matching_nodes:
            parent_task_node_id = next(iter(matching_nodes))
        matching_task_ids = {
            str(cycle_metadata_from_record(record).get("task_id") or "")
            for record in parent_matches
        }
        matching_task_ids.discard("")
        if not parent_task_id and len(matching_task_ids) > 1:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "error": "parent_task_ambiguous",
                    "cycle_id": clean_cycle_id,
                    "parent_task_node_id": parent_task_node_id,
                    "candidate_task_ids": sorted(matching_task_ids),
                },
            )
        if not parent_task_id and matching_task_ids:
            parent_task_id = next(iter(matching_task_ids))
    if summary.get("status") == "completed":
        if request_scope and request_id:
            existing = await find_existing_group_task_request(
                request_scope,
                request_id,
            )
            if existing is not None:
                return parent_task_id, parent_task_node_id
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "cycle_completed",
                "cycle_id": clean_cycle_id,
            },
        )
    return parent_task_id, parent_task_node_id


def cycle_graph_from_events(
    summary: dict[str, Any],
    events: list[dict[str, Any]],
) -> dict[str, Any]:
    participants: dict[str, dict[str, Any]] = {}
    task_nodes: dict[str, dict[str, Any]] = {}
    lineage_edges: dict[tuple[str, str, str], dict[str, Any]] = {}
    communication_edges: dict[tuple[str, str, str], dict[str, Any]] = {}

    for event in events:
        for actor_name in ("from_agent", "to_agent"):
            actor = event.get(actor_name)
            if not isinstance(actor, dict):
                continue
            actor_id = str(actor.get("id") or actor.get("phone") or "").strip()
            if not actor_id:
                continue
            participant = participants.setdefault(
                actor_id,
                {
                    "id": actor_id,
                    "role": actor.get("role"),
                    "phone": actor.get("phone"),
                    "group_ids": [],
                },
            )
            if actor_id == PROJECT_MANAGER_AGENT_ID:
                actor_group_ids: list[Any] = []
            elif actor_name == "from_agent":
                actor_group_ids = [
                    event.get("source_group_id") or event.get("group_id")
                ]
            else:
                actor_group_ids = [
                    event.get("target_group_id") or event.get("group_id")
                ]
            for group_id in actor_group_ids:
                if group_id and group_id not in participant["group_ids"]:
                    participant["group_ids"].append(group_id)

        task_id = str(event.get("task_id") or "").strip()
        task_node_id = str(event.get("task_node_id") or task_id).strip()
        if task_node_id:
            task_node = task_nodes.setdefault(
                task_node_id,
                {
                    "id": task_node_id,
                    "task_id": task_id or None,
                    "parent_task_id": event.get("parent_task_id"),
                    "parent_task_node_id": event.get("parent_task_node_id"),
                    "group_id": event.get("target_group_id")
                    or event.get("group_id"),
                    "first_event_at": event.get("timestamp"),
                    "last_event_at": event.get("timestamp"),
                    "event_types": [],
                },
            )
            task_node["last_event_at"] = event.get("timestamp")
            event_type = event.get("event_type")
            if event_type and event_type not in task_node["event_types"]:
                task_node["event_types"].append(event_type)
            parent_node_id = str(event.get("parent_task_node_id") or "").strip()
            parent_task_id = str(event.get("parent_task_id") or "").strip()
            parent_key = parent_node_id
            if not parent_key and parent_task_id:
                parent_key = next(
                    (
                        str(candidate.get("id") or "")
                        for candidate in task_nodes.values()
                        if str(candidate.get("task_id") or "") == parent_task_id
                    ),
                    parent_task_id,
                )
            if parent_key:
                edge_key = (parent_key, task_node_id, "HANDOFF")
                lineage_edges.setdefault(
                    edge_key,
                    {
                        "id": "edge-" + hashlib.sha256(
                            ":".join(edge_key).encode("utf-8")
                        ).hexdigest()[:16],
                        "from": parent_key,
                        "to": task_node_id,
                        "type": "HANDOFF",
                        "connection_id": event.get("connection_id"),
                    },
                )

        if event.get("event_type") == "MESSAGE_QUEUED":
            from_agent_id = str(event.get("from_agent_id") or "").strip()
            to_agent_id = str(event.get("to_agent_id") or "").strip()
            if from_agent_id and to_agent_id:
                connection_id = str(event.get("connection_id") or "external")
                edge_key = (from_agent_id, to_agent_id, connection_id)
                edge = communication_edges.setdefault(
                    edge_key,
                    {
                        "id": "comm-" + hashlib.sha256(
                            ":".join(edge_key).encode("utf-8")
                        ).hexdigest()[:16],
                        "from": from_agent_id,
                        "to": to_agent_id,
                        "connection_id": event.get("connection_id"),
                        "count": 0,
                        "task_node_ids": [],
                    },
                )
                edge["count"] += 1
                if task_node_id and task_node_id not in edge["task_node_ids"]:
                    edge["task_node_ids"].append(task_node_id)

    resolved_lineage_edges = [
        edge
        for edge in lineage_edges.values()
        if edge.get("from") in task_nodes and edge.get("to") in task_nodes
    ]
    return {
        "cycle": summary,
        "graph_type": "task_lineage_with_agent_communications",
        "nodes": {
            "tasks": sorted(task_nodes.values(), key=lambda node: node["id"]),
            "agents": sorted(participants.values(), key=lambda node: node["id"]),
        },
        "edges": {
            "task_lineage": sorted(
                resolved_lineage_edges, key=lambda edge: edge["id"]
            ),
            "communications": sorted(
                communication_edges.values(), key=lambda edge: edge["id"]
            ),
        },
        "event_count": len(events),
    }


def normalize_cycle_lifecycle_payload(
    payload: dict[str, Any],
) -> dict[str, Any]:
    event_type = str(payload.get("event_type") or "").strip().upper()
    if event_type not in CYCLE_LIFECYCLE_EVENT_TYPES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "unsupported_cycle_event_type",
                "allowed": sorted(CYCLE_LIFECYCLE_EVENT_TYPES),
            },
        )
    for server_owned_field in ("event_id", "timestamp", "project_id"):
        if server_owned_field in payload:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"{server_owned_field} is server-owned",
            )
    request_id = str(payload.get("request_id") or "").strip()
    if len(request_id) > 160:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="request_id must contain at most 160 characters",
        )
    group_id = str(payload.get("group_id") or "").strip()
    from_agent_id = str(payload.get("from_agent_id") or "").strip()
    task_id = str(payload.get("task_id") or "").strip()
    task_node_id = str(payload.get("task_node_id") or "").strip()
    event_payload = payload.get("payload")
    if event_payload is not None and not isinstance(event_payload, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="payload must be an object",
        )
    normalized_payload = deepcopy(event_payload) if isinstance(event_payload, dict) else {}
    message = payload.get("message")

    if event_type == "ARTIFACT_CREATED":
        artifact = payload.get("artifact")
        if not isinstance(artifact, dict) or not artifact:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="artifact must be a non-empty object",
            )
        normalized_payload["artifact"] = deepcopy(artifact)
    elif event_type == "GROUP_REPORT_SUBMITTED":
        report = payload.get("report")
        if report is not None and not isinstance(report, dict):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="report must be an object",
            )
        if isinstance(report, dict):
            normalized_payload["report"] = deepcopy(report)
        if not normalized_payload and message is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="report, payload, or message is required",
            )
    elif event_type == "CYCLE_COMPLETED":
        decision = payload.get("decision")
        if not isinstance(decision, dict) or not decision:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="decision must be a non-empty object",
            )
        normalized_payload["decision"] = deepcopy(decision)

    if event_type != "CYCLE_COMPLETED":
        if not group_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="group_id is required for this cycle event",
            )
        if not from_agent_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="from_agent_id is required for this cycle event",
            )
        if from_agent_id == PROJECT_MANAGER_AGENT_ID:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=(
                    "Project Manager cannot submit agent artifact or group "
                    "report events"
                ),
            )
    elif from_agent_id and from_agent_id != PROJECT_MANAGER_AGENT_ID:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only Project Manager can complete a development cycle",
        )

    return {
        "event_type": event_type,
        "request_id": request_id,
        "group_id": group_id,
        "from_agent_id": from_agent_id or PROJECT_MANAGER_AGENT_ID,
        "task_id": task_id,
        "task_node_id": task_node_id,
        "message": deepcopy(message),
        "payload": normalized_payload,
    }


def cycle_lifecycle_actor(
    normalized_payload: dict[str, Any],
    group_data: dict[str, Any] | None,
) -> dict[str, Any]:
    from_agent_id = normalized_payload["from_agent_id"]
    if from_agent_id == PROJECT_MANAGER_AGENT_ID:
        return {
            "from_agent_id": PROJECT_MANAGER_AGENT_ID,
            "from_role": "project_manager",
            "from_phone": PROJECT_MANAGER_PHONE,
            "sender": PROJECT_MANAGER_AGENT_NAME,
        }
    if group_data is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="group_id is required for the selected agent",
        )
    bindings = [
        binding
        for binding in group_data["group"].get("agents", [])
        if isinstance(binding, dict)
        and str(binding.get("agent_id") or "").strip() == from_agent_id
    ]
    if not bindings:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "error": "cycle_event_agent_not_in_group",
                "group_id": normalized_payload["group_id"],
                "from_agent_id": from_agent_id,
            },
        )
    binding = bindings[0]
    return {
        "from_agent_id": from_agent_id,
        "from_role": binding.get("role"),
        "from_phone": binding.get("agent_phone"),
        "sender": binding.get("role"),
    }


def cycle_lifecycle_request_details(
    cycle_id: str,
    normalized_payload: dict[str, Any],
) -> tuple[str, str]:
    event_type = normalized_payload["event_type"]
    request_scope = f"{cycle_id}:event"
    fingerprint = canonical_json_fingerprint(
        {
            "event_type": event_type,
            "group_id": normalized_payload["group_id"],
            "from_agent_id": normalized_payload["from_agent_id"],
            "task_id": normalized_payload["task_id"],
            "task_node_id": normalized_payload["task_node_id"],
            "message": normalized_payload["message"],
            "payload": normalized_payload["payload"],
        }
    )
    return request_scope, fingerprint


def deduplicated_cycle_lifecycle_result(
    records: list[dict[str, Any]],
    summary: dict[str, Any],
    normalized_payload: dict[str, Any],
) -> dict[str, Any] | None:
    request_id = normalized_payload["request_id"]
    if not request_id:
        return None
    cycle_id = str(summary.get("cycle_id") or "")
    event_type = normalized_payload["event_type"]
    request_scope, fingerprint = cycle_lifecycle_request_details(
        cycle_id,
        normalized_payload,
    )
    for record in records:
        metadata = cycle_metadata_from_record(record)
        if (
            str(metadata.get("cycle_event_request_scope") or "")
            != request_scope
            or str(metadata.get("cycle_event_request_id") or "") != request_id
        ):
            continue
        existing_fingerprint = str(metadata.get("cycle_event_fingerprint") or "")
        if existing_fingerprint != fingerprint:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "error": "cycle_event_request_conflict",
                    "request_id": request_id,
                },
            )
        all_events = cycle_events_from_records(records)
        existing_event = next(
            event
            for event in all_events
            if event.get("source_record_id") == record.get("id")
            and event.get("event_type") == event_type
        )
        return {
            "created": False,
            "deduplicated": True,
            "cycle": cycle_summary_from_records(records) or summary,
            "event": existing_event,
        }
    return None


def append_cycle_lifecycle_event_file(
    cycle_id: str,
    summary: dict[str, Any],
    normalized_payload: dict[str, Any],
    actor: dict[str, Any],
    git_context: dict[str, Any],
) -> dict[str, Any]:
    event_type = normalized_payload["event_type"]
    request_id = normalized_payload["request_id"]
    request_scope, fingerprint = cycle_lifecycle_request_details(
        cycle_id,
        normalized_payload,
    )
    lock_path = history_path.with_name(f"{history_path.name}.lock")
    with interprocess_file_lock(lock_path):
        records = _read_cycle_records_file_unlocked(
            cycle_id=cycle_id,
        )
        deduplicated = deduplicated_cycle_lifecycle_result(
            records,
            summary,
            normalized_payload,
        )
        if deduplicated is not None:
            return deduplicated

        current_summary = cycle_summary_from_records(records) or summary
        if current_summary.get("status") == "completed":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={"error": "cycle_completed", "cycle_id": cycle_id},
            )
        if event_type == "CYCLE_COMPLETED" and any(
            cycle_metadata_from_record(record).get("cycle_event_type")
            == "CYCLE_COMPLETED"
            for record in records
        ):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={"error": "cycle_completed", "cycle_id": cycle_id},
            )

        record = {
            "id": str(uuid4()),
            "timestamp": utc_now(),
            "event": "cycle_lifecycle_event",
            "context": "development_cycle",
            "queue": "cycle-events",
            "route": f"/api/v1/cycles/{cycle_id}/events",
            "message": normalized_payload["message"],
            "metadata": {
                "route": f"/api/v1/cycles/{cycle_id}/events",
                "context": "development_cycle",
                "context_label": "Development Cycle Audit Trail",
                "cycle_id": cycle_id,
                "cycle_title": summary.get("title"),
                "project_phone": summary.get("project_id"),
                "git_context_key": summary.get("git_context_key"),
                "group_id": normalized_payload["group_id"] or None,
                "task_id": normalized_payload["task_id"] or None,
                "task_node_id": normalized_payload["task_node_id"] or None,
                "cycle_event_type": event_type,
                "cycle_event_payload": deepcopy(normalized_payload["payload"]),
                "cycle_event_request_scope": request_scope if request_id else None,
                "cycle_event_request_id": request_id or None,
                "cycle_event_fingerprint": fingerprint if request_id else None,
                **actor,
                **{
                    key: value
                    for key, value in git_context.items()
                    if value is not None
                },
            },
        }
        record["metadata"] = {
            key: value
            for key, value in record["metadata"].items()
            if value is not None
        }
        _write_history_line_unlocked(record)
        records = _read_cycle_records_file_unlocked(
            cycle_id=cycle_id,
        )
        events = cycle_events_from_records(records)
        created_event = next(
            event
            for event in events
            if event.get("source_record_id") == record["id"]
            and event.get("event_type") == event_type
        )
        return {
            "created": True,
            "deduplicated": False,
            "cycle": cycle_summary_from_records(records),
            "event": created_event,
        }


async def append_cycle_lifecycle_event(
    cycle_id: str,
    summary: dict[str, Any],
    normalized_payload: dict[str, Any],
    actor: dict[str, Any],
    git_context: dict[str, Any],
) -> dict[str, Any]:
    async with history_lock:
        return await asyncio.to_thread(
            append_cycle_lifecycle_event_file,
            cycle_id,
            deepcopy(summary),
            deepcopy(normalized_payload),
            deepcopy(actor),
            deepcopy(git_context),
        )


def delete_removed_backend_history_record_file(record_id: str) -> dict[str, Any]:
    lock_path = history_path.with_name(f"{history_path.name}.lock")
    with interprocess_file_lock(lock_path):
        if not history_path.exists():
            return {"deleted": False, "remaining": 0, "reason": "not_found"}

        with history_path.open("r", encoding="utf-8") as file:
            lines = file.readlines()

        kept_lines: list[str] = []
        deleted = False
        found_protected = False
        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue

            try:
                record = json.loads(stripped)
            except json.JSONDecodeError:
                kept_lines.append(line)
                continue

            if record.get("id") == record_id:
                if (
                    record.get("event") == "removed_from_backend_queue"
                    and not cycle_id_from_record(record)
                ):
                    deleted = True
                    continue
                found_protected = True

            kept_lines.append(line if line.endswith("\n") else f"{line}\n")

        if deleted:
            temp_path = history_path.with_suffix(".jsonl.tmp")
            with temp_path.open("w", encoding="utf-8") as file:
                file.writelines(kept_lines)
                file.flush()
                os.fsync(file.fileno())
            temp_path.replace(history_path)

        reason = "protected_event" if found_protected else "not_found"
        return {"deleted": deleted, "remaining": len(kept_lines), "reason": reason}


async def delete_removed_backend_history_record(record_id: str) -> dict[str, Any]:
    async with history_lock:
        return await asyncio.to_thread(delete_removed_backend_history_record_file, record_id)


def find_history_record_file(record_id: str) -> dict[str, Any] | None:
    if not history_path.exists():
        return None

    with history_path.open("r", encoding="utf-8") as file:
        for line in file:
            stripped = line.strip()
            if not stripped:
                continue
            try:
                record = json.loads(stripped)
            except json.JSONDecodeError:
                continue
            if record.get("id") == record_id:
                return record

    return None


async def find_history_record(record_id: str) -> dict[str, Any] | None:
    async with history_lock:
        return await asyncio.to_thread(find_history_record_file, record_id)


def render_index() -> str:
    escaped_history_path = html.escape(str(history_path))
    escaped_git_config_path = html.escape(str(git_config_path))
    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>QA Queue Control</title>
  <style>
    :root {{
      color-scheme: light;
      --bg: #f7f8fa;
      --panel: #ffffff;
      --text: #1f2328;
      --muted: #606975;
      --line: #d7dce2;
      --accent: #0f6cbd;
      --accent-dark: #0b5798;
      --danger: #b42318;
      --ok: #067647;
    }}
    * {{
      box-sizing: border-box;
    }}
    body {{
      margin: 0;
      min-height: 100vh;
      background: var(--bg);
      color: var(--text);
      font-family: "Segoe UI", Arial, sans-serif;
      font-size: 14px;
      line-height: 1.45;
    }}
    header {{
      border-bottom: 1px solid var(--line);
      background: var(--panel);
    }}
    .header-inner {{
      max-width: 1280px;
      margin: 0 auto;
      padding: 18px 24px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
    }}
    h1 {{
      margin: 0;
      font-size: 20px;
      font-weight: 650;
      letter-spacing: 0;
    }}
    .subtle {{
      color: var(--muted);
      font-size: 13px;
    }}
    main {{
      max-width: 1280px;
      margin: 0 auto;
      padding: 24px;
      display: grid;
      grid-template-columns: minmax(340px, 480px) minmax(0, 1fr);
      gap: 24px;
    }}
    section {{
      min-width: 0;
    }}
    .panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 18px;
    }}
    .panel + .panel {{
      margin-top: 16px;
    }}
    h2 {{
      margin: 0 0 14px;
      font-size: 16px;
      font-weight: 650;
      letter-spacing: 0;
    }}
    label {{
      display: block;
      margin: 12px 0 6px;
      color: #30363d;
      font-weight: 600;
      font-size: 13px;
    }}
    select,
    input,
    textarea {{
      width: 100%;
      border: 1px solid #c8d0d9;
      border-radius: 6px;
      padding: 9px 10px;
      color: var(--text);
      background: #ffffff;
      font: inherit;
    }}
    textarea {{
      min-height: 260px;
      resize: vertical;
      font-family: Consolas, "Courier New", monospace;
      white-space: pre;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
    }}
    .actions {{
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      margin-top: 14px;
    }}
    button {{
      border: 1px solid transparent;
      border-radius: 6px;
      min-height: 36px;
      padding: 8px 12px;
      cursor: pointer;
      font: inherit;
      font-weight: 600;
    }}
    button.primary {{
      background: var(--accent);
      color: #ffffff;
    }}
    button.primary:hover {{
      background: var(--accent-dark);
    }}
    button.secondary {{
      background: #eef2f6;
      color: #24292f;
      border-color: #d0d7de;
    }}
    .status {{
      margin-top: 10px;
      min-height: 20px;
      color: var(--muted);
    }}
    .status.ok {{
      color: var(--ok);
    }}
    .status.error {{
      color: var(--danger);
    }}
    .queues {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
    }}
    .queue-stat {{
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 12px;
      background: #fbfcfd;
    }}
    .queue-stat strong {{
      display: block;
      font-size: 22px;
      margin-top: 4px;
    }}
    .history-toolbar {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      margin-bottom: 12px;
    }}
    .history-list {{
      display: flex;
      flex-direction: column;
      gap: 12px;
      max-height: calc(100vh - 170px);
      overflow: auto;
      padding-right: 4px;
    }}
    .entry {{
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #ffffff;
      padding: 12px;
    }}
    .entry-head {{
      display: flex;
      align-items: baseline;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 8px;
    }}
    .badge {{
      display: inline-flex;
      align-items: center;
      border-radius: 999px;
      padding: 2px 8px;
      font-size: 12px;
      font-weight: 700;
      background: #eef2f6;
      color: #30363d;
    }}
    .badge.work {{
      background: #e7f0ff;
      color: #0f4b8f;
    }}
    .badge.test {{
      background: #e6f6ef;
      color: #05603a;
    }}
    pre {{
      margin: 0;
      padding: 10px;
      border-radius: 6px;
      background: #f6f8fa;
      overflow: auto;
      white-space: pre-wrap;
      word-break: break-word;
      font-family: Consolas, "Courier New", monospace;
      font-size: 13px;
    }}
    @media (max-width: 900px) {{
      main {{
        grid-template-columns: 1fr;
        padding: 16px;
      }}
      .header-inner {{
        padding: 16px;
        align-items: flex-start;
        flex-direction: column;
      }}
      .history-list {{
        max-height: none;
      }}
    }}
  </style>
</head>
<body>
  <header>
    <div class="header-inner">
      <div>
        <h1>QA Queue Control</h1>
        <div class="subtle">Отправка задач в /work и /test, журнал сохраняется в {escaped_history_path}</div>
      </div>
      <button class="secondary" id="refreshButton" type="button">Обновить</button>
    </div>
  </header>
  <main>
    <section>
      <div class="panel">
        <h2>Отправить сообщение</h2>
        <div class="grid">
          <div>
            <label for="queue">Куда отправить</label>
            <select id="queue">
              <option value="work">/work - программисту</option>
              <option value="test">/test - тестировщику/аналитику</option>
            </select>
          </div>
          <div>
            <label for="status">Статус</label>
            <select id="status">
              <option value="INITIAL_TASK">INITIAL_TASK</option>
              <option value="READY_FOR_TEST">READY_FOR_TEST</option>
              <option value="FAIL">FAIL</option>
              <option value="PASS">PASS</option>
              <option value="CUSTOM">CUSTOM</option>
            </select>
          </div>
          <div>
            <label for="sender">От кого</label>
            <select id="sender">
              <option>Analyst</option>
              <option>Owner</option>
              <option>Tester</option>
              <option>Programmer</option>
            </select>
          </div>
          <div>
            <label for="receiver">Кому</label>
            <select id="receiver">
              <option>Programmer</option>
              <option>Analyst</option>
              <option>Tester</option>
              <option>Owner</option>
            </select>
          </div>
        </div>
        <label for="message">Текст сообщения</label>
        <textarea id="message" spellcheck="false"></textarea>
        <div class="actions">
          <button class="primary" id="sendButton" type="button">Отправить</button>
          <button class="secondary" id="taskTemplateButton" type="button">Шаблон задачи</button>
          <button class="secondary" id="failTemplateButton" type="button">Шаблон FAIL</button>
          <button class="secondary" id="readyTemplateButton" type="button">Шаблон READY</button>
        </div>
        <div class="status" id="sendStatus"></div>
      </div>
      <div class="panel">
        <h2>Телефонная пара</h2>
        <div class="subtle">Новый адресный канал: сообщение идет через номер разговора и доставляется только на телефон получателя.</div>
        <div class="grid">
          <div>
            <label for="phoneChannel">Канал пары</label>
            <select id="phoneChannel">
              <option value="worker-all">Worker channel</option>
              <option value="tester-all">Tester channel</option>
            </select>
          </div>
          <div>
            <label for="conversationPhone">Номер разговора</label>
            <input id="conversationPhone" placeholder="case-001">
          </div>
          <div>
            <label for="phoneSender">От кого</label>
            <select id="phoneSender"></select>
          </div>
          <div>
            <label for="phoneReceiver">Кому</label>
            <select id="phoneReceiver"></select>
          </div>
        </div>
        <label for="phoneMessage">Текст сообщения</label>
        <textarea class="phone-message" id="phoneMessage" spellcheck="false"></textarea>
        <div class="actions">
          <button class="primary" id="sendPhoneMessageButton" type="button">Отправить по телефону</button>
        </div>
        <div class="status" id="phoneStatus"></div>
      </div>
      <div class="panel">
        <h2>Git context</h2>
        <label for="gitAddress">Git address для текущего порта</label>
        <input id="gitAddress" placeholder="D:\\nginx или https://github.com/org/repo.git">
        <label for="gitProjectName">Project name</label>
        <input id="gitProjectName" placeholder="LLM Extractor">
        <div class="actions">
          <button class="primary" id="saveGitButton" type="button">Сохранить Git address</button>
          <button class="secondary" id="checkGitButton" type="button">Проверить commit</button>
        </div>
        <div class="status" id="gitStatus">Файл привязки: {escaped_git_config_path}</div>
      </div>
      <div class="panel">
        <h2>Очереди</h2>
        <div class="queues">
          <div class="queue-stat">
            <span class="subtle">/work</span>
            <strong id="workSize">0</strong>
          </div>
          <div class="queue-stat">
            <span class="subtle">/test</span>
            <strong id="testSize">0</strong>
          </div>
        </div>
      </div>
    </section>
    <section>
      <div class="history-toolbar">
        <h2>История переписки</h2>
        <span class="subtle" id="historyCount"></span>
      </div>
      <div class="history-list" id="history"></div>
    </section>
  </main>
  <script>
    const queueEl = document.getElementById("queue");
    const statusEl = document.getElementById("status");
    const senderEl = document.getElementById("sender");
    const receiverEl = document.getElementById("receiver");
    const messageEl = document.getElementById("message");
    const sendStatusEl = document.getElementById("sendStatus");
    const activeGitContextSummaryEl = document.getElementById("activeGitContextSummary");
    const attachmentFolderSelectEl = document.getElementById("attachmentFolderSelect");
    const attachmentDescriptionEl = document.getElementById("attachmentDescription");
    const attachmentStatusEl = document.getElementById("attachmentStatus");
    const phoneChannelEl = document.getElementById("phoneChannel");
    const conversationPhoneEl = document.getElementById("conversationPhone");
    const phoneSenderEl = document.getElementById("phoneSender");
    const phoneReceiverEl = document.getElementById("phoneReceiver");
    const phoneMessageEl = document.getElementById("phoneMessage");
    const phoneStatusEl = document.getElementById("phoneStatus");
    const consultantExpertEl = document.getElementById("consultantExpert");
    const consultantConversationEl = document.getElementById("consultantConversation");
    const consultantSenderEl = document.getElementById("consultantSender");
    const consultantReplyPhoneEl = document.getElementById("consultantReplyPhone");
    const consultantQuestionEl = document.getElementById("consultantQuestion");
    const consultantReplyEl = document.getElementById("consultantReply");
    const consultantStatusEl = document.getElementById("consultantStatus");
    const historyEl = document.getElementById("history");
    const historyCountEl = document.getElementById("historyCount");
    const workSizeEl = document.getElementById("workSize");
    const testSizeEl = document.getElementById("testSize");
    const gitAddressEl = document.getElementById("gitAddress");
    const gitStatusEl = document.getElementById("gitStatus");

    function setStatus(text, state = "") {{
      sendStatusEl.textContent = text;
      sendStatusEl.className = "status" + (state ? " " + state : "");
    }}

    function setGitStatus(text, state = "") {{
      gitStatusEl.textContent = text;
      gitStatusEl.className = "status" + (state ? " " + state : "");
    }}

    function setTemplate(type) {{
      if (type === "task") {{
        queueEl.value = "work";
        senderEl.value = "Analyst";
        receiverEl.value = "Programmer";
        statusEl.value = "INITIAL_TASK";
        messageEl.value = `TO: Programmer
FROM: Analyst
STATUS: INITIAL_TASK

PROGRAM:
- 

REQUIREMENTS:
- 

INITIAL TASK:
- `;
      }}
      if (type === "fail") {{
        queueEl.value = "work";
        senderEl.value = "Analyst";
        receiverEl.value = "Programmer";
        statusEl.value = "FAIL";
        messageEl.value = `TO: Programmer
FROM: Analyst
STATUS: FAIL

CHECKED:
- 

PROBLEMS & STATISTICAL ANOMALIES:
- problem:
- page/url/endpoint:
- expected:
- actual:
- hardcoding risk:
- steps to reproduce / logs:

FIX REQUEST:
- 

RETEST AFTER FIX:
- `;
      }}
      if (type === "ready") {{
        queueEl.value = "test";
        senderEl.value = "Programmer";
        receiverEl.value = "Analyst";
        statusEl.value = "READY_FOR_TEST";
        messageEl.value = `TO: Analyst
FROM: Programmer
STATUS: READY_FOR_TEST

PROGRAM:
- 

REQUIREMENTS:
- 

URL:
- https://gp2admin.neuro.uni-luebeck.de/extractor/

CHANGES:
- 

LOCAL VERIFICATION:
- 

UC1.72 SERVER DEPLOYMENT AND VERIFICATION:
- 

TEST TASK FOR ANALYST:
- `;
      }}
      setStatus("");
    }}

    function escapeHtml(value) {{
      return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#039;");
    }}

    function formatMessage(message) {{
      if (typeof message === "string") {{
        return message;
      }}
      return JSON.stringify(message, null, 2);
    }}

    async function sendMessage() {{
      const message = messageEl.value.trim();
      if (!message) {{
        setStatus("Введите текст сообщения.", "error");
        return;
      }}
      setStatus("Отправляю...");
      const response = await fetch("/ui/send", {{
        method: "POST",
        headers: {{"Content-Type": "application/json"}},
        body: JSON.stringify({{
          queue: queueEl.value,
          sender: senderEl.value,
          receiver: receiverEl.value,
          status: statusEl.value,
          message
        }})
      }});
      const data = await response.json();
      if (!response.ok) {{
        setStatus(data.detail || "Ошибка отправки.", "error");
        return;
      }}
      setStatus(`Отправлено в ${{data.queue}}. Размер очереди: ${{data.size}}.`, "ok");
      await refresh();
    }}

    async function refreshQueues() {{
      const response = await fetch("/queues");
      const data = await response.json();
      workSizeEl.textContent = data.queues.work;
      testSizeEl.textContent = data.queues.test;
    }}

    async function refreshGitConfig() {{
      const response = await fetch("/git-config");
      const data = await response.json();
      gitAddressEl.value = data.git_address || "";
      if (data.git_commit_short) {{
        setGitStatus(`Порт ${{data.port}}: commit ${{data.git_commit_short}}`, "ok");
      }} else if (data.git_error) {{
        setGitStatus(`Порт ${{data.port}}: ${{data.git_error}}`, "error");
      }} else {{
        setGitStatus(`Порт ${{data.port}}: Git address не задан. Файл привязки: ${{data.config_path}}`);
      }}
    }}

    async function saveGitConfig() {{
      const gitAddress = gitAddressEl.value.trim();
      if (!gitAddress) {{
        setGitStatus("Введите Git address.", "error");
        return;
      }}
      setGitStatus("Сохраняю...");
      const response = await fetch("/git-config", {{
        method: "POST",
        headers: {{"Content-Type": "application/json"}},
        body: JSON.stringify({{git_address: gitAddress}})
      }});
      const data = await response.json();
      if (!response.ok) {{
        setGitStatus(data.detail || "Ошибка сохранения.", "error");
        return;
      }}
      if (data.git_commit_short) {{
        setGitStatus(`Сохранено для порта ${{data.port}}. Commit: ${{data.git_commit_short}}`, "ok");
      }} else if (data.git_error) {{
        setGitStatus(`Сохранено для порта ${{data.port}}, но commit не получен: ${{data.git_error}}`, "error");
      }} else {{
        setGitStatus(`Сохранено для порта ${{data.port}}.`, "ok");
      }}
    }}

    async function refreshHistory() {{
      const response = await fetch("/history?limit=200");
      const records = await response.json();
      historyCountEl.textContent = `${{records.length}} последних записей`;
      historyEl.innerHTML = records.slice().reverse().map((record) => {{
        const timestamp = new Date(record.timestamp).toLocaleString();
        const queueClass = record.queue === "work" ? "work" : record.queue === "test" ? "test" : "";
        const meta = record.metadata || {{}};
        const actor = [meta.sender, meta.receiver].filter(Boolean).join(" -> ");
        const commit = meta.git_commit_short || "";
        return `<article class="entry">
          <div class="entry-head">
            <div>
              <span class="badge ${{queueClass}}">${{escapeHtml(record.queue)}}</span>
              <span class="badge">${{escapeHtml(record.event)}}</span>
              ${{commit ? `<span class="badge">commit ${{escapeHtml(commit)}}</span>` : ""}}
              ${{actor ? `<span class="subtle">${{escapeHtml(actor)}}</span>` : ""}}
            </div>
            <time class="subtle">${{escapeHtml(timestamp)}}</time>
          </div>
          <pre>${{escapeHtml(formatMessage(record.message))}}</pre>
        </article>`;
      }}).join("") || `<div class="panel subtle">История пока пустая.</div>`;
    }}

    async function refresh() {{
      await Promise.all([refreshQueues(), refreshHistory(), refreshGitConfig()]);
    }}

    document.getElementById("sendButton").addEventListener("click", () => {{
      sendMessage().catch((error) => setStatus(error.message, "error"));
    }});
    document.getElementById("taskTemplateButton").addEventListener("click", () => setTemplate("task"));
    document.getElementById("failTemplateButton").addEventListener("click", () => setTemplate("fail"));
    document.getElementById("readyTemplateButton").addEventListener("click", () => setTemplate("ready"));
    document.getElementById("refreshButton").addEventListener("click", refresh);
    document.getElementById("saveGitButton").addEventListener("click", () => {{
      saveGitConfig().then(refreshHistory).catch((error) => setGitStatus(error.message, "error"));
    }});
    document.getElementById("checkGitButton").addEventListener("click", () => {{
      refreshGitConfig().catch((error) => setGitStatus(error.message, "error"));
    }});

    setTemplate("task");
    refresh();
    setInterval(refresh, 5000);
  </script>
</body>
</html>"""


def render_index_v2() -> str:
    escaped_history_path = html.escape(str(history_path))
    escaped_git_config_path = html.escape(str(git_config_path))
    escaped_email_routes_path = html.escape(str(email_routes_path))
    escaped_agents_path = html.escape(str(agents_path))
    escaped_screenshot_folders_path = html.escape(str(screenshot_folders_path))
    escaped_evidence_folders_path = html.escape(str(evidence_folders_path))
    queue_config_json = json.dumps(QUEUE_DEFINITIONS, ensure_ascii=False)
    template = """<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>QA Queue Control</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f7f8fa;
      --panel: #ffffff;
      --text: #1f2328;
      --muted: #606975;
      --line: #d7dce2;
      --accent: #0f6cbd;
      --accent-dark: #0b5798;
      --green-bg: #e6f6ef;
      --green-text: #05603a;
      --blue-bg: #e7f0ff;
      --blue-text: #0f4b8f;
      --orange-bg: #fff4e5;
      --orange-text: #8a4b00;
      --danger: #b42318;
      --ok: #067647;
    }
    * {
      box-sizing: border-box;
    }
    body {
      margin: 0;
      min-height: 100vh;
      background: var(--bg);
      color: var(--text);
      font-family: "Segoe UI", Arial, sans-serif;
      font-size: 14px;
      line-height: 1.45;
    }
    header {
      border-bottom: 1px solid var(--line);
      background: var(--panel);
    }
    .header-inner {
      max-width: 1320px;
      margin: 0 auto;
      padding: 18px 24px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
    }
    .page-tabs-shell {
      border-bottom: 1px solid var(--line);
      background: var(--panel);
    }
    .page-tabs {
      max-width: 1320px;
      margin: 0 auto;
      padding: 10px 24px;
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
    }
    .page-tab {
      min-height: 38px;
      border: 1px solid var(--line);
      background: #eef2f6;
      color: #30363d;
    }
    .page-tab.active {
      border-color: var(--accent);
      background: var(--blue-bg);
      color: var(--blue-text);
    }
    h1 {
      margin: 0;
      font-size: 20px;
      font-weight: 650;
      letter-spacing: 0;
    }
    h2 {
      margin: 0 0 14px;
      font-size: 16px;
      font-weight: 650;
      letter-spacing: 0;
    }
    main {
      max-width: 1320px;
      margin: 0 auto;
      padding: 24px;
      display: grid;
      grid-template-columns: minmax(360px, 520px) minmax(0, 1fr);
      gap: 24px;
    }
    main.view:not(.active) {
      display: none;
    }
    main.agents-view,
    main.consultants-view {
      grid-template-columns: minmax(0, 1fr);
    }
    section {
      min-width: 0;
    }
    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 18px;
    }
    .panel + .panel {
      margin-top: 16px;
    }
    .subtle {
      color: var(--muted);
      font-size: 13px;
    }
    .tabs {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 8px;
      margin-bottom: 12px;
    }
    .tab {
      min-height: 42px;
      border: 1px solid var(--line);
      background: #eef2f6;
      color: #30363d;
    }
    .tab.active {
      border-color: var(--accent);
      background: var(--blue-bg);
      color: var(--blue-text);
    }
    label {
      display: block;
      margin: 12px 0 6px;
      color: #30363d;
      font-weight: 600;
      font-size: 13px;
    }
    select,
    input,
    textarea {
      width: 100%;
      border: 1px solid #c8d0d9;
      border-radius: 6px;
      padding: 9px 10px;
      color: var(--text);
      background: #ffffff;
      font: inherit;
    }
    textarea {
      min-height: 250px;
      resize: vertical;
      font-family: Consolas, "Courier New", monospace;
      white-space: pre;
    }
    code {
      font-family: Consolas, "Courier New", monospace;
      font-size: 13px;
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
    }
    .commit-banner {
      margin: 10px 0 8px;
      padding: 10px 12px;
      border: 1px solid #b6d4fe;
      border-radius: 8px;
      background: var(--blue-bg);
      color: var(--blue-text);
    }
    .attachment-panel {
      margin-top: 10px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #fbfcfd;
      padding: 12px;
    }
    .attachment-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px;
      align-items: end;
    }
    .attachment-grid label {
      margin-top: 0;
    }
    .attachment-grid select,
    .attachment-grid input {
      min-width: 0;
    }
    .attachment-grid button {
      white-space: nowrap;
    }
    .attachment-hint {
      margin-top: 8px;
    }
    .schedule-panel {
      margin-top: 10px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #fbfcfd;
      padding: 12px;
    }
    .schedule-grid {
      display: grid;
      grid-template-columns: minmax(160px, 0.8fr) minmax(140px, 0.6fr) minmax(0, 1fr);
      gap: 10px;
      align-items: end;
    }
    .schedule-grid label {
      margin-top: 0;
    }
    .schedule-delay-field[hidden] {
      display: none;
    }
    .schedule-hint {
      margin-top: 8px;
    }
    .actions {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      margin-top: 14px;
    }
    button {
      border: 1px solid transparent;
      border-radius: 6px;
      min-height: 36px;
      padding: 8px 12px;
      cursor: pointer;
      font: inherit;
      font-weight: 600;
    }
    button.primary {
      background: var(--accent);
      color: #ffffff;
    }
    button.primary:hover {
      background: var(--accent-dark);
    }
    button.secondary {
      background: #eef2f6;
      color: #24292f;
      border-color: #d0d7de;
    }
    button.danger {
      background: #fff1f0;
      color: var(--danger);
      border-color: #f1a7a0;
    }
    a.secondary {
      display: inline-flex;
      align-items: center;
      min-height: 36px;
      border: 1px solid #d0d7de;
      border-radius: 6px;
      padding: 8px 12px;
      background: #eef2f6;
      color: #24292f;
      font-weight: 600;
      text-decoration: none;
    }
    button.icon-button {
      width: 36px;
      min-width: 36px;
      padding: 0;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      background: #eef2f6;
      color: #24292f;
      border-color: #d0d7de;
    }
    .help-button {
      width: 24px;
      min-width: 24px;
      min-height: 24px;
      height: 24px;
      padding: 0;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      border-radius: 999px;
      border-color: #9cc7ef;
      background: #ffffff;
      color: var(--accent);
      font-size: 13px;
      font-weight: 800;
      line-height: 1;
      vertical-align: middle;
    }
    .help-button:hover {
      border-color: var(--accent);
      background: var(--blue-bg);
      color: var(--blue-text);
    }
    h2 .help-button,
    h3 .help-button,
    label .help-button {
      margin-left: 6px;
    }
    .actions .help-button,
    .agent-toolbar-actions .help-button,
    .history-filter-actions .help-button,
    .modal-actions .help-button {
      margin-left: -4px;
      margin-right: 2px;
    }
    .page-tabs .help-button {
      margin-right: 6px;
    }
    .tab-help-pair {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 6px;
      align-items: center;
      min-width: 0;
    }
    .tab-help-pair .tab {
      width: 100%;
    }
    .modal.help-modal {
      width: min(680px, 100%);
    }
    .help-modal-body {
      margin-top: 14px;
      display: flex;
      flex-direction: column;
      gap: 12px;
    }
    .help-modal-section-title {
      display: block;
      margin-bottom: 4px;
      color: #30363d;
      font-weight: 700;
    }
    .help-modal-body p {
      margin: 0;
      color: var(--text);
    }
    .help-modal-body ul {
      margin: 4px 0 0;
      padding-left: 20px;
    }
    .help-modal-body li {
      margin: 4px 0;
    }
    .status {
      margin-top: 10px;
      min-height: 20px;
      color: var(--muted);
    }
    .status.ok {
      color: var(--ok);
    }
    .status.error {
      color: var(--danger);
    }
    .queues {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
    }
    .queue-stat {
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 12px;
      background: #fbfcfd;
    }
    .queue-stat strong {
      display: block;
      font-size: 22px;
      margin-top: 4px;
    }
    .queue-stat.backend {
      border-left: 4px solid #0f6cbd;
    }
    .queue-stat.frontend {
      border-left: 4px solid #067647;
    }
    .queue-stat.common {
      border-left: 4px solid #8a4b00;
    }
    .queue-stat.common {
      border-left: 4px solid #8a4b00;
    }
    .queue-items {
      display: flex;
      flex-direction: column;
      gap: 8px;
      margin-top: 10px;
    }
    .queue-item {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 8px;
      align-items: start;
      padding-top: 8px;
      border-top: 1px solid var(--line);
    }
    .queue-item-text {
      color: var(--muted);
      font-size: 12px;
      overflow-wrap: anywhere;
    }
    .queue-item-phone {
      margin-bottom: 4px;
      color: #30363d;
      font-size: 12px;
      font-weight: 700;
      overflow-wrap: anywhere;
    }
    .scheduled-toolbar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      margin: 16px 0 10px;
    }
    .scheduled-toolbar h3 {
      margin: 0;
      font-size: 15px;
    }
    .scheduled-list {
      display: flex;
      flex-direction: column;
      gap: 10px;
    }
    .scheduled-card {
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 12px;
      background: #ffffff;
    }
    .scheduled-card-head {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 10px;
      align-items: start;
    }
    .scheduled-summary {
      margin-top: 6px;
      color: var(--muted);
      font-size: 12px;
      overflow-wrap: anywhere;
    }
    .queue-item-phone {
      margin-bottom: 4px;
      color: #30363d;
      font-size: 12px;
      font-weight: 700;
      overflow-wrap: anywhere;
    }
    .email-routes {
      display: flex;
      flex-direction: column;
      gap: 8px;
      margin-top: 12px;
    }
    .email-route-row {
      display: grid;
      grid-template-columns: minmax(0, 1.4fr) minmax(140px, 0.8fr) auto;
      gap: 8px;
      align-items: center;
    }
    .email-route-row input,
    .email-route-row select {
      min-width: 0;
    }
    .email-route-row button {
      min-width: 36px;
      padding-left: 10px;
      padding-right: 10px;
    }
    .screenshot-folder-list {
      display: flex;
      flex-direction: column;
      gap: 12px;
      max-height: calc(100vh - 170px);
      overflow: auto;
      padding-right: 4px;
    }
    .screenshot-folder-card {
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #ffffff;
      padding: 12px;
    }
    .screenshot-folder-card.drag-over {
      border-color: var(--accent);
      background: var(--blue-bg);
    }
    .screenshot-folder-head {
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      gap: 12px;
    }
    .screenshot-folder-title {
      font-weight: 700;
    }
    .screenshot-path-row {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 8px;
      align-items: center;
      margin-top: 8px;
    }
    .screenshot-path-value,
    .screenshot-file-name {
      min-width: 0;
      overflow-wrap: anywhere;
    }
    .screenshot-files {
      display: flex;
      flex-direction: column;
      gap: 8px;
      margin-top: 12px;
    }
    .screenshot-folder-files[hidden] {
      display: none;
    }
    .screenshot-file-row {
      display: grid;
      grid-template-columns: 64px minmax(0, 1fr) minmax(160px, 0.45fr) auto auto auto auto;
      gap: 8px;
      align-items: center;
      padding-top: 8px;
      border-top: 1px solid var(--line);
    }
    .screenshot-thumbnail {
      width: 64px;
      height: 44px;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: #f6f8fa;
      object-fit: cover;
      cursor: zoom-in;
    }
    .screenshot-file-row select {
      min-width: 0;
    }
    .screenshot-file-row[draggable="true"] {
      cursor: grab;
    }
    .screenshot-drop-zone,
    .screenshot-trash-zone {
      margin-top: 12px;
      min-height: 74px;
      border: 1px dashed #9aa4b2;
      border-radius: 8px;
      background: #fbfcfd;
      color: var(--muted);
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 12px;
      text-align: center;
    }
    .screenshot-drop-zone.drag-over,
    .screenshot-trash-zone.drag-over {
      border-color: var(--accent);
      background: var(--blue-bg);
      color: var(--blue-text);
    }
    .screenshot-trash-list {
      display: flex;
      flex-direction: column;
      gap: 8px;
      margin-top: 12px;
    }
    .screenshot-trash-item {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 8px;
      align-items: center;
      padding: 8px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #fff1f0;
    }
    .screenshot-empty {
      margin-top: 10px;
      padding-top: 8px;
      border-top: 1px solid var(--line);
    }
    .modal.screenshot-preview-modal {
      width: min(1120px, 100%);
    }
    .screenshot-preview-frame {
      margin-top: 12px;
      max-height: calc(100vh - 170px);
      overflow: auto;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #f6f8fa;
      padding: 10px;
    }
    .screenshot-preview-frame img {
      display: block;
      max-width: 100%;
      height: auto;
      margin: 0 auto;
    }
    .evidence-folder-list {
      display: flex;
      flex-direction: column;
      gap: 12px;
      max-height: calc(100vh - 170px);
      overflow: auto;
      padding-right: 4px;
    }
    .evidence-folder-card {
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #ffffff;
      padding: 12px;
    }
    .evidence-folder-card.drag-over {
      border-color: var(--accent);
      background: var(--blue-bg);
    }
    .evidence-folder-head {
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      gap: 12px;
    }
    .evidence-folder-title {
      font-weight: 700;
    }
    .evidence-path-row {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 8px;
      align-items: center;
      margin-top: 8px;
    }
    .evidence-path-value,
    .evidence-file-name {
      min-width: 0;
      overflow-wrap: anywhere;
    }
    .evidence-files {
      display: flex;
      flex-direction: column;
      gap: 8px;
      margin-top: 12px;
    }
    .evidence-folder-files[hidden] {
      display: none;
    }
    .evidence-file-row {
      display: grid;
      grid-template-columns: 64px minmax(0, 1fr) minmax(160px, 0.45fr) auto auto auto auto;
      gap: 8px;
      align-items: center;
      padding-top: 8px;
      border-top: 1px solid var(--line);
    }
    .evidence-thumbnail,
    .evidence-file-tile {
      width: 64px;
      height: 44px;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: #f6f8fa;
    }
    .evidence-thumbnail {
      object-fit: cover;
      cursor: zoom-in;
    }
    .evidence-file-tile {
      display: flex;
      align-items: center;
      justify-content: center;
      color: var(--muted);
      font-size: 12px;
      font-weight: 700;
      text-transform: uppercase;
      cursor: pointer;
      overflow: hidden;
      padding: 4px;
    }
    .evidence-file-row select {
      min-width: 0;
    }
    .evidence-file-row[draggable="true"] {
      cursor: grab;
    }
    .evidence-drop-zone,
    .evidence-trash-zone {
      margin-top: 12px;
      min-height: 74px;
      border: 1px dashed #9aa4b2;
      border-radius: 8px;
      background: #fbfcfd;
      color: var(--muted);
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 12px;
      text-align: center;
    }
    .evidence-drop-zone.drag-over,
    .evidence-trash-zone.drag-over {
      border-color: var(--accent);
      background: var(--blue-bg);
      color: var(--blue-text);
    }
    .evidence-trash-list {
      display: flex;
      flex-direction: column;
      gap: 8px;
      margin-top: 12px;
    }
    .evidence-trash-item {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 8px;
      align-items: center;
      padding: 8px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #fff1f0;
    }
    .evidence-empty {
      margin-top: 10px;
      padding-top: 8px;
      border-top: 1px solid var(--line);
    }
    .modal.evidence-preview-modal {
      width: min(1120px, 100%);
    }
    .evidence-preview-frame {
      margin-top: 12px;
      max-height: calc(100vh - 170px);
      overflow: auto;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #f6f8fa;
      padding: 10px;
    }
    .evidence-preview-frame img,
    .evidence-preview-frame iframe {
      display: block;
      width: 100%;
      max-width: 100%;
      min-height: 70vh;
      border: 0;
      margin: 0 auto;
    }
    .evidence-preview-frame img {
      width: auto;
      min-height: 0;
      height: auto;
    }
    .agent-toolbar {
      display: grid;
      grid-template-columns: minmax(220px, 1fr) auto;
      gap: 12px;
      align-items: end;
      margin-top: 12px;
    }
    .agent-toolbar label {
      margin-top: 0;
    }
    .agent-toolbar-actions {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }
    .agent-project-manager {
      margin-top: 12px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #fbfcfd;
      padding: 12px;
    }
    .agent-project-grid {
      display: grid;
      grid-template-columns: minmax(220px, 1fr) auto;
      gap: 12px;
      align-items: end;
    }
    .agent-project-grid label {
      margin-top: 0;
    }
    .agent-project-actions {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }
    .agent-project-meta {
      margin-top: 8px;
    }
    .agent-project-lists {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
      margin-top: 12px;
    }
    .agent-project-list {
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #ffffff;
      padding: 10px;
      min-width: 0;
    }
    .agent-project-list h3 {
      margin: 0 0 8px;
      font-size: 13px;
      font-weight: 650;
      letter-spacing: 0;
    }
    .agent-project-items {
      display: flex;
      flex-direction: column;
      gap: 8px;
    }
    .agent-project-item {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 8px;
      align-items: center;
      padding-top: 8px;
      border-top: 1px solid var(--line);
    }
    .agent-project-item:first-child {
      padding-top: 0;
      border-top: 0;
    }
    .agent-project-item-name {
      min-width: 0;
      overflow-wrap: anywhere;
      font-weight: 650;
    }
    .agent-project-item-meta {
      color: var(--muted);
      font-size: 12px;
      overflow-wrap: anywhere;
    }
    .agent-project-item-actions {
      display: flex;
      gap: 8px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }
    .sprint-history-panel {
      margin-top: 12px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #ffffff;
      padding: 10px;
    }
    .sprint-history-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      margin-bottom: 8px;
    }
    .sprint-history-head h3 {
      margin: 0;
      font-size: 14px;
    }
    .sprint-history-items {
      display: flex;
      flex-direction: column;
      gap: 8px;
    }
    .sprint-history-item {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 10px;
      align-items: center;
      padding-top: 8px;
      border-top: 1px solid var(--line);
    }
    .sprint-history-item:first-child {
      padding-top: 0;
      border-top: 0;
    }
    .sprint-history-title {
      font-weight: 650;
      overflow-wrap: anywhere;
    }
    .sprint-history-badge {
      display: inline-block;
      margin-left: 6px;
      border-radius: 999px;
      padding: 2px 7px;
      background: #ddf4ff;
      color: #0550ae;
      font-size: 11px;
      font-weight: 650;
    }
    .sprint-history-badge.archived {
      background: #f1f3f5;
      color: #57606a;
    }
    .agent-editor {
      margin-top: 12px;
    }
    .agent-row {
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #fbfcfd;
      padding: 12px;
    }
    .agent-row-head {
      display: grid;
      grid-template-columns: minmax(0, 1fr) minmax(140px, 0.45fr);
      gap: 10px;
      align-items: end;
    }
    .agent-row-head label {
      margin-top: 0;
    }
    textarea.agent-profile {
      min-height: 120px;
      font-family: "Segoe UI", Arial, sans-serif;
      white-space: pre-wrap;
    }
    textarea.phone-message {
      min-height: 120px;
      white-space: pre-wrap;
    }
    textarea.phone-message {
      min-height: 120px;
      white-space: pre-wrap;
    }
    .agent-template-preview {
      margin-top: 12px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #ffffff;
      padding: 12px;
    }
    .agent-template-preview h3 {
      margin: 0 0 8px;
      font-size: 14px;
      font-weight: 650;
      letter-spacing: 0;
    }
    .actor-task-list {
      margin: 0;
      padding-left: 22px;
      display: grid;
      gap: 8px;
    }
    .actor-task-list li div {
      margin-top: 2px;
      white-space: pre-wrap;
      word-break: break-word;
    }
    .agent-preview-body {
      margin-top: 8px;
      padding: 10px;
      border-radius: 6px;
      background: #f6f8fa;
      white-space: pre-wrap;
      word-break: break-word;
      font-family: Consolas, "Courier New", monospace;
      font-size: 13px;
      max-height: 320px;
      overflow: auto;
    }
    .missing-placeholder {
      display: inline;
      color: var(--danger);
      background: #fff1f0;
      border: 1px solid #fecdca;
      border-radius: 4px;
      padding: 0 3px;
      font-weight: 700;
    }
    .agent-params-head {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 10px;
      margin-top: 12px;
    }
    .agent-params-head label {
      margin: 0;
    }
    .agent-params {
      display: flex;
      flex-direction: column;
      gap: 8px;
      margin-top: 8px;
    }
    .agent-param-row {
      display: grid;
      grid-template-columns: minmax(120px, 0.8fr) minmax(180px, 1.4fr) auto;
      gap: 8px;
      align-items: center;
    }
    .agent-param-row input {
      min-width: 0;
    }
    .agent-status-summary {
      margin-top: 12px;
      padding-top: 12px;
      border-top: 1px solid var(--line);
    }
    .agent-status-metrics {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 10px;
    }
    .agent-status-metric {
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #fbfcfd;
      padding: 10px;
    }
    .agent-status-metric strong {
      display: block;
      margin-top: 2px;
      font-size: 20px;
    }
    .agent-status-lists {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 12px;
      margin-top: 12px;
    }
    .agent-status-list h3 {
      margin: 0 0 6px;
      font-size: 13px;
      font-weight: 650;
      letter-spacing: 0;
    }
    .agent-status-list ul {
      margin: 0;
      padding-left: 18px;
    }
    .agent-status-list li {
      margin: 4px 0;
      overflow-wrap: anywhere;
    }
    .git-context-projects {
      margin-bottom: 16px;
    }
    .git-context-list-head {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 10px;
      margin-bottom: 8px;
    }
    .git-context-list-head h3 {
      margin: 0;
      font-size: 13px;
      font-weight: 650;
      letter-spacing: 0;
    }
    .git-context-list {
      display: grid;
      gap: 8px;
    }
    .git-context-apply-row {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 8px;
      align-items: end;
    }
    .git-context-apply-row select {
      min-height: 38px;
    }
    .git-context-card {
      width: 100%;
      min-height: 72px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #ffffff;
      color: var(--text);
      padding: 10px 12px;
      text-align: left;
      cursor: pointer;
    }
    .git-context-card:hover {
      border-color: var(--accent);
      background: #f7fbff;
    }
    .git-context-card.active {
      border-color: var(--accent);
      background: var(--blue-bg);
      box-shadow: inset 3px 0 0 var(--accent);
    }
    .git-context-card-title {
      display: flex;
      justify-content: space-between;
      gap: 8px;
      align-items: center;
      font-weight: 650;
    }
    .git-context-card-badge {
      flex: 0 0 auto;
      border: 1px solid #b7d7f7;
      border-radius: 999px;
      background: #f7fbff;
      color: var(--blue-text);
      padding: 2px 8px;
      font-size: 12px;
      font-weight: 650;
    }
    .git-context-card-meta {
      display: grid;
      gap: 3px;
      margin-top: 6px;
      color: var(--muted);
      font-size: 12px;
      overflow-wrap: anywhere;
    }
    .agent-state-notice {
      margin-top: 12px;
      border: 1px solid var(--line);
      border-left-width: 4px;
      border-radius: 8px;
      padding: 10px 12px;
      background: #fbfcfd;
      color: #30363d;
      font-weight: 600;
    }
    .agent-state-notice.waiting {
      border-left-color: #8a4b00;
      background: var(--orange-bg);
      color: var(--orange-text);
    }
    .agent-state-notice.sent {
      border-left-color: #0f6cbd;
      background: var(--blue-bg);
      color: var(--blue-text);
    }
    .agent-state-notice.expired {
      border-left-color: var(--danger);
      background: #fff1f0;
      color: var(--danger);
    }
    .agent-state-chip {
      display: inline-flex;
      align-items: center;
      border-radius: 999px;
      padding: 1px 7px;
      font-size: 12px;
      font-weight: 700;
      margin-left: 4px;
      background: #eef2f6;
      color: #30363d;
    }
    .agent-state-chip.waiting {
      background: var(--orange-bg);
      color: var(--orange-text);
    }
    .agent-state-chip.sent {
      background: var(--blue-bg);
      color: var(--blue-text);
    }
    .agent-state-chip.alive {
      background: var(--green-bg);
      color: var(--green-text);
    }
    .agent-state-chip.expired {
      background: #fff1f0;
      color: var(--danger);
    }
    .modal-backdrop {
      position: fixed;
      inset: 0;
      z-index: 50;
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 16px;
      background: rgba(31, 35, 40, 0.42);
    }
    .modal-backdrop[hidden] {
      display: none;
    }
    .modal {
      width: min(920px, 100%);
      max-height: calc(100vh - 32px);
      overflow: auto;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #ffffff;
      box-shadow: 0 18px 48px rgba(31, 35, 40, 0.22);
      padding: 16px;
    }
    .modal-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
    }
    .modal-head h2 {
      margin: 0;
    }
    .clone-form-grid {
      display: grid;
      grid-template-columns: minmax(0, 1.2fr) minmax(180px, 0.8fr);
      gap: 12px;
      margin-top: 12px;
    }
    .clone-form-grid .wide {
      grid-column: 1 / -1;
    }
    .inline-control {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 8px;
      align-items: center;
    }
    .inline-control button {
      white-space: nowrap;
    }
    textarea.clone-refinements {
      min-height: 90px;
      white-space: pre-wrap;
    }
    .clone-preview {
      margin-top: 12px;
      border-top: 1px solid var(--line);
      padding-top: 12px;
    }
    .clone-preview-fields {
      display: grid;
      grid-template-columns: minmax(0, 1fr) minmax(160px, 0.35fr);
      gap: 10px;
    }
    textarea.clone-profile-preview,
    textarea.clone-raw-response {
      min-height: 150px;
      white-space: pre-wrap;
      font-family: Consolas, "Courier New", monospace;
      font-size: 13px;
    }
    .modal-actions {
      display: flex;
      justify-content: flex-end;
      flex-wrap: wrap;
      gap: 10px;
      margin-top: 12px;
    }
    .history-toolbar {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      margin-bottom: 12px;
    }
    .history-filters {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
      margin-bottom: 12px;
    }
    .history-filters label {
      margin-top: 0;
    }
    .history-filter-actions {
      grid-column: 1 / -1;
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
    }
    .history-filters .status {
      grid-column: 1 / -1;
      margin-top: 0;
    }
    .history-list {
      display: flex;
      flex-direction: column;
      gap: 12px;
      max-height: calc(100vh - 170px);
      overflow: auto;
      padding-right: 4px;
    }
    main.cycle-graph-view {
      grid-template-columns: minmax(280px, 350px) minmax(0, 1fr);
      align-items: start;
    }
    .cycle-graph-sidebar {
      display: flex;
      flex-direction: column;
      gap: 12px;
    }
    .cycle-graph-project {
      margin-bottom: 12px;
      padding: 10px 12px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #f6f8fa;
      overflow-wrap: anywhere;
    }
    .cycle-graph-controls {
      display: grid;
      grid-template-columns: minmax(0, 1fr) auto;
      gap: 8px;
      align-items: end;
    }
    .cycle-graph-controls label {
      margin-top: 0;
    }
    .cycle-list {
      display: flex;
      flex-direction: column;
      gap: 8px;
      max-height: calc(100vh - 330px);
      overflow: auto;
      margin-top: 12px;
      padding-right: 3px;
    }
    .cycle-list-card {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #ffffff;
      color: var(--text);
      padding: 10px 12px;
      text-align: left;
      cursor: pointer;
    }
    .cycle-list-card:hover {
      border-color: var(--accent);
      background: #f7fbff;
    }
    .cycle-list-card.active {
      border-color: var(--accent);
      background: var(--blue-bg);
      box-shadow: inset 3px 0 0 var(--accent);
    }
    .cycle-list-card-head {
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      gap: 8px;
    }
    .cycle-list-card-title {
      min-width: 0;
      font-weight: 650;
      overflow-wrap: anywhere;
    }
    .cycle-list-card-meta {
      display: flex;
      flex-wrap: wrap;
      gap: 5px;
      margin-top: 7px;
      color: var(--muted);
      font-size: 12px;
    }
    .cycle-status-chip {
      flex: 0 0 auto;
      display: inline-flex;
      align-items: center;
      border-radius: 999px;
      padding: 2px 8px;
      background: var(--orange-bg);
      color: var(--orange-text);
      font-size: 11px;
      font-weight: 700;
    }
    .cycle-status-chip.completed {
      background: var(--green-bg);
      color: var(--green-text);
    }
    .cycle-status-chip.queued {
      background: #eef2f6;
      color: #30363d;
    }
    .cycle-graph-toolbar {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 12px;
    }
    .cycle-graph-toolbar h2 {
      margin-bottom: 3px;
    }
    .cycle-graph-metrics {
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
      justify-content: flex-end;
    }
    .cycle-graph-legend {
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      margin-bottom: 10px;
      color: var(--muted);
      font-size: 12px;
    }
    .cycle-graph-legend-item {
      display: inline-flex;
      align-items: center;
      gap: 6px;
    }
    .cycle-legend-swatch {
      width: 22px;
      height: 4px;
      border-radius: 999px;
      background: var(--accent);
    }
    .cycle-legend-swatch.active {
      height: 10px;
      background: var(--ok);
      box-shadow: 0 0 0 4px rgba(6, 118, 71, 0.13);
    }
    .cycle-legend-swatch.handoff {
      background: #d97706;
      background-image: linear-gradient(90deg, #d97706 55%, transparent 55%);
      background-size: 8px 4px;
    }
    .cycle-legend-swatch.task {
      background: #7c3aed;
    }
    .cycle-graph-canvas {
      min-height: 410px;
      border: 1px solid var(--line);
      border-radius: 8px;
      background:
        linear-gradient(#eef2f6 1px, transparent 1px),
        linear-gradient(90deg, #eef2f6 1px, transparent 1px),
        #fbfcfd;
      background-size: 24px 24px;
      overflow: auto;
    }
    .cycle-graph-canvas svg {
      display: block;
      min-width: 100%;
      font-family: "Segoe UI", Arial, sans-serif;
    }
    .cycle-communication-edge,
    .cycle-task-edge {
      fill: none;
      stroke-linecap: round;
      stroke-linejoin: round;
    }
    .cycle-communication-edge {
      stroke: #6b8eb5;
      stroke-width: 2.5;
    }
    .cycle-communication-edge.handoff-active {
      stroke: #d97706;
      stroke-width: 4;
      stroke-dasharray: 11 7;
      animation: cycle-handoff-flow 0.8s linear infinite;
    }
    .cycle-task-edge {
      stroke: #7c3aed;
      stroke-width: 2;
      stroke-dasharray: 5 5;
    }
    .cycle-agent-node .cycle-agent-body {
      fill: #ffffff;
      stroke: #6b8eb5;
      stroke-width: 2;
    }
    .cycle-agent-node .cycle-agent-halo {
      fill: rgba(6, 118, 71, 0.1);
      stroke: rgba(6, 118, 71, 0.34);
      stroke-width: 2;
      opacity: 0;
      transform-box: fill-box;
      transform-origin: center;
    }
    .cycle-agent-node.active .cycle-agent-body {
      fill: #ecfdf3;
      stroke: var(--ok);
      stroke-width: 3;
    }
    .cycle-agent-node.active .cycle-agent-halo {
      opacity: 1;
      animation: cycle-agent-pulse 1.6s ease-out infinite;
    }
    .cycle-agent-role {
      fill: var(--text);
      font-size: 13px;
      font-weight: 700;
      text-anchor: middle;
    }
    .cycle-agent-meta {
      fill: var(--muted);
      font-size: 11px;
      text-anchor: middle;
    }
    .cycle-task-node rect {
      fill: #f5f3ff;
      stroke: #7c3aed;
      stroke-width: 1.5;
    }
    .cycle-task-node.active rect {
      fill: #ecfdf3;
      stroke: var(--ok);
      stroke-width: 2.5;
    }
    .cycle-task-title {
      fill: #3b2763;
      font-size: 11px;
      font-weight: 700;
      text-anchor: middle;
    }
    .cycle-task-meta {
      fill: var(--muted);
      font-size: 10px;
      text-anchor: middle;
    }
    .cycle-graph-empty {
      display: grid;
      min-height: 408px;
      place-items: center;
      padding: 24px;
      color: var(--muted);
      text-align: center;
    }
    .cycle-lineage-summary {
      margin-top: 10px;
      color: var(--muted);
      font-size: 12px;
      overflow-wrap: anywhere;
    }
    .cycle-event-list {
      display: flex;
      flex-direction: column;
      gap: 8px;
      max-height: 360px;
      overflow: auto;
    }
    .cycle-event {
      display: grid;
      grid-template-columns: auto minmax(0, 1fr) auto;
      gap: 9px;
      align-items: start;
      padding: 9px 0;
      border-top: 1px solid var(--line);
    }
    .cycle-event:first-child {
      border-top: 0;
      padding-top: 0;
    }
    .cycle-event-sequence {
      display: grid;
      width: 28px;
      height: 28px;
      place-items: center;
      border-radius: 50%;
      background: #eef2f6;
      color: var(--muted);
      font-size: 11px;
      font-weight: 700;
    }
    .cycle-event.handoff .cycle-event-sequence {
      background: var(--orange-bg);
      color: var(--orange-text);
    }
    .cycle-event.started .cycle-event-sequence {
      background: var(--green-bg);
      color: var(--green-text);
    }
    .cycle-event-title {
      font-weight: 650;
      overflow-wrap: anywhere;
    }
    .cycle-event-meta {
      margin-top: 2px;
      color: var(--muted);
      font-size: 12px;
      overflow-wrap: anywhere;
    }
    @keyframes cycle-agent-pulse {
      0% { transform: scale(0.88); opacity: 0.75; }
      75%, 100% { transform: scale(1.18); opacity: 0; }
    }
    @keyframes cycle-handoff-flow {
      to { stroke-dashoffset: -18; }
    }
    @media (prefers-reduced-motion: reduce) {
      .cycle-agent-node.active .cycle-agent-halo,
      .cycle-communication-edge.handoff-active {
        animation: none;
      }
    }
    .entry {
      border: 1px solid var(--line);
      border-radius: 8px;
      background: #ffffff;
      padding: 12px;
    }
    .entry-head {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 8px;
    }
    .entry-title {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: 6px;
    }
    .dot {
      width: 10px;
      height: 10px;
      border-radius: 50%;
      background: #606975;
      flex: 0 0 auto;
    }
    .dot.backend {
      background: #0f6cbd;
    }
    .dot.frontend {
      background: #067647;
    }
    .dot.common {
      background: #8a4b00;
    }
    .badge {
      display: inline-flex;
      align-items: center;
      border-radius: 999px;
      padding: 2px 8px;
      font-size: 12px;
      font-weight: 700;
      background: #eef2f6;
      color: #30363d;
    }
    .badge.backend {
      background: var(--blue-bg);
      color: var(--blue-text);
    }
    .badge.frontend {
      background: var(--green-bg);
      color: var(--green-text);
    }
    .badge.common {
      background: var(--orange-bg);
      color: var(--orange-text);
    }
    .badge.event {
      background: var(--orange-bg);
      color: var(--orange-text);
    }
    .entry-summary {
      margin: 6px 0 0;
    }
    details {
      margin-top: 8px;
    }
    summary {
      cursor: pointer;
      color: var(--muted);
      font-size: 13px;
    }
    pre {
      margin: 8px 0 0;
      padding: 10px;
      border-radius: 6px;
      background: #f6f8fa;
      overflow: auto;
      white-space: pre-wrap;
      word-break: break-word;
      font-family: Consolas, "Courier New", monospace;
      font-size: 13px;
    }
    @media (max-width: 940px) {
      main {
        grid-template-columns: 1fr;
        padding: 16px;
      }
      .header-inner {
        padding: 16px;
        align-items: flex-start;
        flex-direction: column;
      }
      .history-list {
        max-height: none;
      }
      .cycle-list {
        max-height: none;
      }
      .cycle-graph-toolbar {
        flex-direction: column;
      }
      .cycle-graph-metrics {
        justify-content: flex-start;
      }
      .email-route-row {
        grid-template-columns: 1fr;
      }
      .screenshot-folder-list {
        max-height: none;
      }
      .evidence-folder-list {
        max-height: none;
      }
      .screenshot-folder-head,
      .screenshot-path-row,
      .screenshot-file-row,
      .screenshot-trash-item,
      .evidence-folder-head,
      .evidence-path-row,
      .evidence-file-row,
      .evidence-trash-item {
        grid-template-columns: 1fr;
      }
      .attachment-grid {
        grid-template-columns: 1fr;
      }
      .schedule-grid {
        grid-template-columns: 1fr;
      }
      .agent-toolbar {
        grid-template-columns: 1fr;
      }
      .agent-project-grid {
        grid-template-columns: 1fr;
      }
      .agent-project-lists,
      .agent-project-item,
      .sprint-history-item {
        grid-template-columns: 1fr;
      }
      .agent-project-item-actions,
      .sprint-history-item .actions {
        justify-content: flex-start;
      }
      .agent-row-head {
        grid-template-columns: 1fr;
      }
      .agent-toolbar-actions,
      .agent-project-actions {
        justify-content: flex-start;
      }
      .agent-param-row {
        grid-template-columns: 1fr;
      }
      .agent-status-metrics,
      .agent-status-lists {
        grid-template-columns: 1fr;
      }
      .clone-form-grid,
      .clone-preview-fields {
        grid-template-columns: 1fr;
      }
      .inline-control {
        grid-template-columns: 1fr;
      }
    }
  </style>
</head>
<body>
  <header>
    <div class="header-inner">
      <div>
        <h1>QA Queue Control</h1>
        <div class="subtle">Два контура разработки, журнал: __HISTORY_PATH__</div>
      </div>
      <button class="secondary" id="refreshButton" type="button">Обновить</button>
    </div>
  </header>
  <div class="page-tabs-shell">
    <nav class="page-tabs" aria-label="Разделы UI">
      <button class="page-tab active" data-view="messages" type="button">Сообщения</button>
      <button class="help-button" data-help-topic="page:messages" type="button" title="Что это?" aria-label="Подсказка: Сообщения">?</button>
      <button class="page-tab" data-view="git-context" type="button">Git context</button>
      <button class="help-button" data-help-topic="page:git-context" type="button" title="Что это?" aria-label="Подсказка: Git context">?</button>
      <button class="page-tab" data-view="cycles" type="button">Граф группы и циклы</button>
      <button class="help-button" data-help-topic="page:cycles" type="button" title="Что это?" aria-label="Подсказка: Граф группы и циклы">?</button>
      <button class="page-tab" data-view="screenshots" type="button">Скриншоты</button>
      <button class="help-button" data-help-topic="page:screenshots" type="button" title="Что это?" aria-label="Подсказка: Скриншоты">?</button>
      <button class="page-tab" data-view="evidence" type="button">Доказательства</button>
      <button class="help-button" data-help-topic="page:evidence" type="button" title="Что это?" aria-label="Подсказка: Доказательства">?</button>
      <button class="page-tab" data-view="agents" type="button">Агенты</button>
      <button class="help-button" data-help-topic="page:agents" type="button" title="Что это?" aria-label="Подсказка: Агенты">?</button>
      <button class="page-tab" data-view="consultants" type="button">Консультанты</button>
      <button class="help-button" data-help-topic="page:consultants" type="button" title="Что это?" aria-label="Подсказка: Консультанты">?</button>
    </nav>
  </div>
  <main class="view active" data-view="messages">
    <section>
      <div class="panel">
        <h2>Контур разработки</h2>
        <div class="tabs">
          <div class="tab-help-pair">
            <button class="tab active" data-context="backend" type="button">Данные и алгоритмы</button>
            <button class="help-button" data-help-topic="context:backend" type="button" title="Что это?" aria-label="Подсказка: Данные и алгоритмы">?</button>
          </div>
          <div class="tab-help-pair">
            <button class="tab" data-context="frontend" type="button">Интерфейс и UX</button>
            <button class="help-button" data-help-topic="context:frontend" type="button" title="Что это?" aria-label="Подсказка: Интерфейс и UX">?</button>
          </div>
        </div>
        <div class="subtle" id="contextSummary"></div>
      </div>
      <div class="panel">
        <h2>Отправить сообщение</h2>
        <div class="grid">
          <div>
            <label for="queue">Направление</label>
            <select id="queue"></select>
          </div>
          <div>
            <label for="status">Статус</label>
            <select id="status">
              <option value="INITIAL_TASK">INITIAL_TASK</option>
              <option value="INITIAL_UI_TASK">INITIAL_UI_TASK</option>
              <option value="READY_FOR_TEST">READY_FOR_TEST</option>
              <option value="READY_FOR_UI_TEST">READY_FOR_UI_TEST</option>
              <option value="FAIL">FAIL</option>
              <option value="PASS">PASS</option>
              <option value="CUSTOM">CUSTOM</option>
            </select>
          </div>
          <div>
            <label for="sender">От кого</label>
            <select id="sender"></select>
          </div>
          <div>
            <label for="receiver">Кому</label>
            <select id="receiver"></select>
          </div>
        </div>
        <label for="message">Текст сообщения</label>
        <div class="commit-banner" id="activeGitContextSummary">Git context не выбран</div>
        <div class="commit-banner">Текущий Git-коммит репозитория: <code id="commitValue">не задан</code></div>
        <div class="attachment-panel">
          <div class="attachment-grid">
            <div>
              <label for="attachmentFolderSelect">Папка</label>
              <select id="attachmentFolderSelect"></select>
            </div>
            <div>
              <label for="attachmentDescription">Текст пояснения</label>
              <input id="attachmentDescription" placeholder="См. скриншоты в папке.">
            </div>
            <button class="secondary" id="copyAttachmentFolderPathButton" type="button">Скопировать путь</button>
            <button class="secondary" id="copyAttachmentMessageButton" type="button">Скопировать сообщение</button>
            <button class="secondary" id="refreshAttachmentFoldersButton" type="button">Обновить список</button>
          </div>
          <div class="subtle attachment-hint">Выберите папку со скриншотами или доказательствами. Сообщение копируется в формате: текст пояснения : абсолютный путь к папке.</div>
          <div class="status" id="attachmentStatus"></div>
        </div>
        <div class="schedule-panel">
          <div class="schedule-grid">
            <div>
              <label for="scheduleMode">Когда поставить в очередь</label>
              <select id="scheduleMode">
                <option value="now" selected>Сейчас</option>
                <option value="delay">Через время</option>
                <option value="pass">После STATUS: PASS</option>
              </select>
            </div>
            <div class="schedule-delay-field" id="scheduleDelayField" hidden>
              <label for="scheduleDelayMinutes">Минуты</label>
              <input id="scheduleDelayMinutes" type="number" min="5" step="5" value="5">
            </div>
            <div class="subtle schedule-hint" id="scheduleHint">Сообщение сразу попадет в выбранную очередь.</div>
          </div>
        </div>
        <textarea id="message" spellcheck="false"></textarea>
        <div class="actions">
          <button class="primary" id="sendButton" type="button">Отправить</button>
          <button class="secondary" id="taskTemplateButton" type="button">Шаблон задачи</button>
          <button class="secondary" id="failTemplateButton" type="button">Шаблон FAIL</button>
          <button class="secondary" id="readyTemplateButton" type="button">Шаблон READY</button>
        </div>
        <div class="status" id="sendStatus"></div>
      </div>
      <div class="panel">
        <h2>Телефонная пара</h2>
        <div class="subtle">Парная адресная связь: worker/tester используют один номер разговора, но сообщения доставляются по телефону получателя.</div>
        <div class="grid">
          <div>
            <label for="phoneChannel">Канал пары</label>
            <select id="phoneChannel">
              <option value="worker-all">Worker channel</option>
              <option value="tester-all">Tester channel</option>
            </select>
          </div>
          <div>
            <label for="conversationPhone">Номер разговора</label>
            <input id="conversationPhone" placeholder="case-001">
          </div>
          <div>
            <label for="phoneSender">От кого</label>
            <select id="phoneSender"></select>
          </div>
          <div>
            <label for="phoneReceiver">Кому</label>
            <select id="phoneReceiver"></select>
          </div>
        </div>
        <label for="phoneMessage">Текст сообщения</label>
        <textarea class="phone-message" id="phoneMessage" spellcheck="false"></textarea>
        <div class="actions">
          <button class="primary" id="sendPhoneMessageButton" type="button">Отправить по телефону</button>
        </div>
        <div class="status" id="phoneStatus"></div>
      </div>
      <div class="panel">
        <h2>Email routes</h2>
        <div class="subtle">Email может повторяться; каждая строка сохраняется отдельно.</div>
        <datalist id="emailSenderOptions"></datalist>
        <div class="email-routes" id="emailRoutes"></div>
        <div class="actions">
          <button class="primary" id="addEmailRouteButton" type="button">Добавить email</button>
          <button class="secondary" id="saveEmailRoutesButton" type="button">Сохранить список</button>
        </div>
        <div class="status" id="emailRoutesStatus">Файл: __EMAIL_ROUTES_PATH__</div>
      </div>
      <div class="panel">
        <h2>Очереди</h2>
        <div class="queues" id="queueStats"></div>
        <div class="scheduled-toolbar">
          <h3>Ожидающие задания</h3>
          <span class="subtle" id="scheduledTasksCount"></span>
        </div>
        <div class="scheduled-list" id="scheduledTasks"></div>
        <div class="status" id="scheduledTasksStatus"></div>
      </div>
    </section>
    <section>
      <div class="history-toolbar">
        <h2>История циклов разработки</h2>
        <span class="subtle" id="historyCount"></span>
      </div>
      <div class="panel history-filters">
        <div>
          <label for="historyGitContext">Активный Git context</label>
          <select id="historyGitContext" disabled></select>
        </div>
        <div>
          <label for="historyFrom">От</label>
          <input id="historyFrom" type="date">
        </div>
        <div>
          <label for="historyTo">До</label>
          <input id="historyTo" type="date">
        </div>
        <div class="history-filter-actions">
          <button class="primary" id="applyHistoryFilterButton" type="button">Показать</button>
          <button class="secondary" id="copyHistoryButton" type="button">Скопировать все сообщения</button>
          <button class="secondary" id="copyHistoryWithPatchesButton" type="button">Скопировать сообщения + патчи</button>
        </div>
        <div class="status" id="historyStatus"></div>
      </div>
      <div class="history-list" id="history"></div>
    </section>
  </main>
  <main class="view git-context-view" data-view="git-context">
    <section>
      <div class="panel">
        <h2>Git context</h2>
        <div class="git-context-projects">
          <div class="git-context-list-head">
            <h3>Активный проект</h3>
            <span class="subtle" id="gitContextListCount"></span>
          </div>
          <div class="git-context-apply-row">
            <select id="gitContextList"></select>
            <button class="primary" id="applyGitContextProjectButton" type="button">Применить</button>
          </div>
        </div>
        <div class="grid">
          <div>
            <label for="gitPhone">Телефон привязки для редактирования</label>
            <select id="gitPhone"></select>
          </div>
          <div>
            <label for="gitNewPhone">Новый номер Git context</label>
            <input id="gitNewPhone" placeholder="9002">
          </div>
        </div>
        <div class="actions">
          <button class="secondary" id="addGitContextButton" type="button">Добавить Git context</button>
          <button class="secondary" id="deleteGitContextButton" type="button">Удалить выбранный Git context</button>
        </div>
        <label for="gitAddress">Git address для выбранного телефона</label>
        <input id="gitAddress" placeholder="D:\\nginx или https://github.com/org/repo.git">
        <label for="gitProjectName">Project name</label>
        <input id="gitProjectName" placeholder="LLM Extractor">
        <label for="gitContextKey">Git context key</label>
        <input id="gitContextKey" placeholder="github.com/org/repo#project">
        <div class="actions">
          <button class="primary" id="saveGitButton" type="button">Сохранить привязку телефона</button>
          <button class="secondary" id="checkGitButton" type="button">Проверить commit</button>
        </div>
        <div class="status" id="gitStatus">Файл привязки: __GIT_CONFIG_PATH__</div>
      </div>
    </section>
    <section>
      <div class="panel">
        <h2>Активный контекст</h2>
        <div class="agent-status-metrics">
          <div class="agent-status-metric">
            <span class="subtle">Телефон</span>
            <strong id="activeGitContextPhone">не выбран</strong>
          </div>
          <div class="agent-status-metric">
            <span class="subtle">Проект</span>
            <strong id="activeGitContextProject">не задан</strong>
          </div>
          <div class="agent-status-metric">
            <span class="subtle">Commit</span>
            <strong id="activeGitContextCommit">не задан</strong>
          </div>
        </div>
      </div>
    </section>
  </main>
  <main class="view cycle-graph-view" data-view="cycles" data-state="empty">
    <section class="cycle-graph-sidebar">
      <div class="panel">
        <h2>Циклы разработки</h2>
        <div class="cycle-graph-project" id="cycleGraphProjectSummary">Выберите проект во вкладке Git context.</div>
        <div class="cycle-graph-controls">
          <div>
            <label for="cycleGraphCycleSelect">Цикл</label>
            <select id="cycleGraphCycleSelect" disabled>
              <option value="">Нет доступных циклов</option>
            </select>
          </div>
          <button class="secondary" id="refreshCycleGraphButton" type="button">Обновить</button>
        </div>
        <div class="status" id="cycleGraphStatus"></div>
        <div class="cycle-list" id="cycleGraphCycleList"></div>
      </div>
    </section>
    <section>
      <div class="panel">
        <div class="cycle-graph-toolbar">
          <div>
            <h2 id="cycleGraphTitle">Граф группы</h2>
            <div class="subtle" id="cycleGraphSubtitle">Выберите цикл, чтобы увидеть движение задач.</div>
          </div>
          <div class="cycle-graph-metrics" id="cycleGraphMetrics"></div>
        </div>
        <div class="cycle-graph-legend" aria-label="Легенда графа">
          <span class="cycle-graph-legend-item"><span class="cycle-legend-swatch active"></span>активный агент</span>
          <span class="cycle-graph-legend-item"><span class="cycle-legend-swatch handoff"></span>последний handoff</span>
          <span class="cycle-graph-legend-item"><span class="cycle-legend-swatch"></span>связь агентов</span>
          <span class="cycle-graph-legend-item"><span class="cycle-legend-swatch task"></span>lineage задач</span>
        </div>
        <div class="cycle-graph-canvas" id="cycleGraphCanvas">
          <div class="cycle-graph-empty">Граф появится после выбора проекта и цикла.</div>
        </div>
        <div class="cycle-lineage-summary" id="cycleGraphTaskLineage"></div>
      </div>
      <div class="panel">
        <div class="history-toolbar">
          <h2>Хронология переходов</h2>
          <span class="subtle" id="cycleGraphEventCount"></span>
        </div>
        <div class="cycle-event-list" id="cycleGraphHistory">
          <div class="subtle">Событий пока нет.</div>
        </div>
      </div>
    </section>
  </main>
  <main class="view screenshots-view" data-view="screenshots">
    <section>
      <div class="panel">
        <h2>Создать папку со скриншотами</h2>
        <div class="subtle">Базовый каталог: __SCREENSHOT_FOLDERS_PATH__</div>
        <div class="actions">
          <button class="primary" id="createScreenshotFolderButton" type="button">Создать папку</button>
          <button class="secondary" id="refreshScreenshotFoldersButton" type="button">Обновить список</button>
        </div>
        <div class="status" id="screenshotFoldersStatus"></div>
      </div>
      <div class="panel">
        <h2>Загрузить скриншоты</h2>
        <label for="screenshotFolderSelect">Папка назначения</label>
        <select id="screenshotFolderSelect"></select>
        <label for="screenshotFiles">Скриншоты</label>
        <input id="screenshotFiles" type="file" accept="image/*" multiple>
        <div class="screenshot-drop-zone" id="screenshotDropZone">Перетащите скриншоты сюда</div>
        <div class="actions">
          <button class="primary" id="uploadScreenshotsButton" type="button">Загрузить</button>
          <button class="secondary" id="pasteScreenshotButton" type="button">Вставить из буфера</button>
          <button class="secondary" id="copySelectedScreenshotFolderPathButton" type="button">Скопировать путь папки</button>
        </div>
        <div class="status" id="screenshotUploadStatus"></div>
      </div>
      <div class="panel">
        <h2>Корзина</h2>
        <div class="screenshot-trash-zone" id="screenshotTrashZone">Перетащите сюда скриншоты из папки</div>
        <div class="screenshot-trash-list" id="screenshotTrashItems"></div>
        <div class="actions">
          <button class="secondary" id="clearScreenshotTrashButton" type="button">Очистить корзину</button>
        </div>
        <div class="status" id="screenshotTrashStatus"></div>
      </div>
    </section>
    <section>
      <div class="history-toolbar">
        <h2>Папки со скриншотами</h2>
        <span class="subtle" id="screenshotFoldersCount"></span>
      </div>
      <div class="panel history-filters">
        <div>
          <label for="screenshotFoldersFrom">От</label>
          <input id="screenshotFoldersFrom" type="date">
        </div>
        <div>
          <label for="screenshotFoldersTo">До</label>
          <input id="screenshotFoldersTo" type="date">
        </div>
        <div class="history-filter-actions">
          <button class="primary" id="applyScreenshotFolderFilterButton" type="button">Показать</button>
          <button class="secondary" id="resetScreenshotFolderFilterButton" type="button">Сегодня</button>
        </div>
        <div class="status" id="screenshotFoldersFilterStatus"></div>
      </div>
      <div class="screenshot-folder-list" id="screenshotFolders"></div>
    </section>
  </main>
  <div class="modal-backdrop" id="screenshotPreviewModal" hidden>
    <div class="modal screenshot-preview-modal" role="dialog" aria-modal="true" aria-labelledby="screenshotPreviewTitle">
      <div class="modal-head">
        <h2 id="screenshotPreviewTitle">Просмотр скриншота</h2>
        <button class="icon-button" id="closeScreenshotPreviewButton" type="button" title="Закрыть" aria-label="Закрыть">×</button>
      </div>
      <div class="subtle" id="screenshotPreviewMeta"></div>
      <div class="screenshot-preview-frame">
        <img id="screenshotPreviewImage" alt="Просмотр скриншота">
      </div>
    </div>
  </div>
  <main class="view evidence-view" data-view="evidence">
    <section>
      <div class="panel">
        <h2>Создать папку с доказательствами</h2>
        <div class="subtle">Базовый каталог: __EVIDENCE_FOLDERS_PATH__</div>
        <div class="actions">
          <button class="primary" id="createEvidenceFolderButton" type="button">Создать папку</button>
          <button class="secondary" id="refreshEvidenceFoldersButton" type="button">Обновить список</button>
        </div>
        <div class="status" id="evidenceFoldersStatus"></div>
      </div>
      <div class="panel">
        <h2>Загрузить файлы</h2>
        <label for="evidenceFolderSelect">Папка назначения</label>
        <select id="evidenceFolderSelect"></select>
        <label for="evidenceFiles">Файлы</label>
        <input id="evidenceFiles" type="file" multiple>
        <div class="evidence-drop-zone" id="evidenceDropZone">Перетащите файлы сюда</div>
        <div class="actions">
          <button class="primary" id="uploadEvidenceButton" type="button">Загрузить</button>
          <button class="secondary" id="copySelectedEvidenceFolderPathButton" type="button">Скопировать путь папки</button>
        </div>
        <div class="status" id="evidenceUploadStatus"></div>
      </div>
      <div class="panel">
        <h2>Корзина</h2>
        <div class="evidence-trash-zone" id="evidenceTrashZone">Перетащите сюда файлы из папки</div>
        <div class="evidence-trash-list" id="evidenceTrashItems"></div>
        <div class="actions">
          <button class="secondary" id="clearEvidenceTrashButton" type="button">Очистить корзину</button>
        </div>
        <div class="status" id="evidenceTrashStatus"></div>
      </div>
    </section>
    <section>
      <div class="history-toolbar">
        <h2>Папки с доказательствами</h2>
        <span class="subtle" id="evidenceFoldersCount"></span>
      </div>
      <div class="panel history-filters">
        <div>
          <label for="evidenceFoldersFrom">От</label>
          <input id="evidenceFoldersFrom" type="date">
        </div>
        <div>
          <label for="evidenceFoldersTo">До</label>
          <input id="evidenceFoldersTo" type="date">
        </div>
        <div class="history-filter-actions">
          <button class="primary" id="applyEvidenceFolderFilterButton" type="button">Показать</button>
          <button class="secondary" id="resetEvidenceFolderFilterButton" type="button">Сегодня</button>
        </div>
        <div class="status" id="evidenceFoldersFilterStatus"></div>
      </div>
      <div class="evidence-folder-list" id="evidenceFolders"></div>
    </section>
  </main>
  <div class="modal-backdrop" id="evidencePreviewModal" hidden>
    <div class="modal evidence-preview-modal" role="dialog" aria-modal="true" aria-labelledby="evidencePreviewTitle">
      <div class="modal-head">
        <h2 id="evidencePreviewTitle">Просмотр доказательства</h2>
        <button class="icon-button" id="closeEvidencePreviewButton" type="button" title="Закрыть" aria-label="Закрыть">×</button>
      </div>
      <div class="subtle" id="evidencePreviewMeta"></div>
      <div class="evidence-preview-frame" id="evidencePreviewFrame"></div>
    </div>
  </div>
  <main class="view agents-view" data-view="agents">
    <section>
      <div class="panel">
        <h2>Агенты и профили</h2>
        <div class="subtle">Имена агентов используются в полях «От кого», «Кому» и в подсказках email routes. Профили сохраняются в __AGENTS_PATH__.</div>
        <div class="agent-toolbar">
          <div>
            <label for="agentSelector">Агент</label>
            <select id="agentSelector"></select>
          </div>
          <div class="agent-toolbar-actions">
            <button class="secondary" id="createEmptyAgentButton" type="button">Сгенерировать пустого агента</button>
            <button class="secondary" id="addAgentButton" type="button">Добавить агента</button>
            <button class="secondary" id="cloneAgentButton" type="button">Скопировать агента</button>
            <button class="secondary" id="removeSelectedAgentButton" type="button">Удалить агента</button>
          </div>
        </div>
        <div class="agent-project-manager" id="agentProjectManager">
          <div class="agent-project-grid">
            <div>
              <label for="projectAgentAttachSelect">Скопировать существующего агента в активный проект</label>
              <select id="projectAgentAttachSelect"></select>
            </div>
            <div class="agent-project-actions">
              <button class="secondary" id="attachAgentToProjectButton" type="button">Создать копию в проекте</button>
              <button class="secondary" id="detachSelectedAgentFromProjectButton" type="button">Снять выбранного с проекта</button>
            </div>
          </div>
          <div class="agent-project-grid">
            <div>
              <label for="projectActorsJsonFile">Импорт агентов и их задач из JSON</label>
              <input id="projectActorsJsonFile" type="file" accept="application/json,.json">
            </div>
            <div class="agent-project-actions">
              <button class="secondary" id="importProjectActorsButton" type="button">Загрузить JSON</button>
              <button class="danger" id="deleteAllProjectActorsButton" type="button">Удалить всех агентов</button>
            </div>
          </div>
          <div class="subtle">JSON должен содержать <code>project_id</code>/<code>project_phone</code>, <code>git_context_key</code> или <code>git_address</code>; сервер сверяет проект до любых изменений. Для полной замены укажите <code>agents.overwrite: true</code>. Каждому агенту автоматически добавляются его Git-ветка, адресная книга проекта и инструкция обмена сообщениями.</div>
          <div class="subtle agent-project-meta" id="agentProjectStatus"></div>
          <div class="sprint-history-panel">
            <div class="sprint-history-head">
              <h3>История спринтов</h3>
              <button class="secondary" id="refreshProjectSprintsButton" type="button">Обновить</button>
            </div>
            <div class="subtle" id="projectSprintsStatus">Выберите проект, чтобы увидеть сохранённые спринты.</div>
            <div class="sprint-history-items" id="projectSprints"></div>
          </div>
          <div class="agent-project-lists">
            <div class="agent-project-list">
              <h3>Доступные агенты</h3>
              <div class="agent-project-items" id="projectAvailableAgents"></div>
            </div>
            <div class="agent-project-list">
              <h3>Агенты проекта</h3>
              <div class="agent-project-items" id="projectAttachedAgents"></div>
            </div>
          </div>
        </div>
        <div class="agent-status-summary" id="agentStatusSummary">
          <div class="agent-status-metrics">
            <div class="agent-status-metric">
              <span class="subtle">Специализированные</span>
              <strong id="specializedAgentsCount">0</strong>
            </div>
            <div class="agent-status-metric">
              <span class="subtle">Неспециализированные живые</span>
              <strong id="liveEmptyAgentsCount">0</strong>
            </div>
            <div class="agent-status-metric">
              <span class="subtle">Ожидание истекло</span>
              <strong id="expiredEmptyAgentsCount">0</strong>
            </div>
          </div>
          <div class="agent-status-lists">
            <div class="agent-status-list">
              <h3>Специализированные агенты</h3>
              <ul id="specializedAgentsList"></ul>
            </div>
            <div class="agent-status-list">
              <h3>Живые пустые агенты</h3>
              <ul id="liveEmptyAgentsList"></ul>
            </div>
            <div class="agent-status-list">
              <h3>Истекшие пустые агенты</h3>
              <ul id="expiredEmptyAgentsList"></ul>
            </div>
          </div>
        </div>
        <div class="agent-editor" id="agents"></div>
        <div class="actions">
          <button class="secondary" id="copyAgentProfileButton" type="button">Скопировать с параметрами</button>
          <button class="primary" id="saveAgentsButton" type="button">Сохранить агентов</button>
        </div>
        <div class="status" id="agentsStatus">Файл: __AGENTS_PATH__</div>
        <div class="agent-template-preview" id="emptyAgentPromptPanel" hidden>
          <h3>Prompt пустого агента для запуска</h3>
          <div class="subtle" id="emptyAgentPromptMeta"></div>
          <textarea class="clone-raw-response" id="emptyAgentPromptText" readonly spellcheck="false"></textarea>
          <div class="actions">
            <button class="secondary" id="copyEmptyAgentPromptButton" type="button">Скопировать prompt</button>
          </div>
        </div>
      </div>
      <div class="panel" id="specializeAgentPanel" style="margin-top: 16px;">
        <h2>Специализировать пустого агента</h2>
        <div class="subtle">Отправить профиль и роль пустому агенту по его номеру телефона.</div>
        <div class="grid" style="margin-top: 12px;">
          <div>
            <label for="specializePhone">Телефон пустого агента</label>
            <select id="specializePhone"></select>
          </div>
          <div>
            <label for="specializeTemplate">Шаблон специализации</label>
            <select id="specializeTemplate"></select>
          </div>
        </div>
        <label for="specializeName" style="margin-top: 12px;">Новое имя агента</label>
        <input id="specializeName" placeholder="Например: Expert Analyst">
        <label for="specializeProfile" style="margin-top: 12px;">Новый профиль (системный промпт)</label>
        <textarea id="specializeProfile" style="min-height: 120px;" placeholder="Введите профиль..."></textarea>
        <div class="agent-state-notice waiting" id="specializeAgentState"></div>
        <div class="actions">
          <button class="primary" id="sendSpecializationButton" type="button">Отправить специализацию</button>
        </div>
        <div class="status" id="specializeStatus"></div>
      </div>
    </section>
  </main>
  <div class="modal-backdrop" id="cloneAgentModal" hidden>
    <div class="modal" role="dialog" aria-modal="true" aria-labelledby="cloneAgentTitle">
      <div class="modal-head">
        <h2 id="cloneAgentTitle">Скопировать агента</h2>
        <button class="icon-button" id="closeCloneAgentButton" type="button" title="Закрыть" aria-label="Закрыть">×</button>
      </div>
      <div class="subtle">Выберите исходного агента, добавьте уточнения и отправьте профиль в Gemini. Ключ используется только для этого запроса и не сохраняется.</div>
      <div class="clone-form-grid">
        <div>
          <label for="cloneSourceAgent">Кого клонировать</label>
          <select id="cloneSourceAgent"></select>
        </div>
        <div>
          <label for="cloneGeminiModel">Gemini model</label>
          <div class="inline-control">
            <input id="cloneGeminiModel" list="cloneGeminiModels" value="gemini-2.5-flash" placeholder="gemini-2.5-flash">
            <button class="secondary" id="loadGeminiModelsButton" type="button">Загрузить модели</button>
          </div>
          <datalist id="cloneGeminiModels">
            <option value="gemini-2.5-flash"></option>
            <option value="gemini-2.5-pro"></option>
            <option value="gemini-2.5-flash-lite"></option>
          </datalist>
        </div>
        <div class="wide">
          <label for="cloneGeminiApiKey">Gemini API key</label>
          <input id="cloneGeminiApiKey" type="password" autocomplete="off" placeholder="AIza...">
          <div class="subtle">Если поле пустое, сервер попробует переменную окружения GEMINI_API_KEY.</div>
        </div>
        <div class="wide">
          <label for="cloneRefinements">Уточнения для нового агента</label>
          <textarea class="clone-refinements" id="cloneRefinements" spellcheck="false" placeholder="Например: сделать этого агента консультантом по backend архитектуре, сохранить стиль отчетов Analyst, но убрать UI-валидацию."></textarea>
        </div>
      </div>
      <div class="actions">
        <button class="primary" id="requestCloneAgentButton" type="button">Сформировать клон</button>
      </div>
      <div class="status" id="cloneAgentStatus"></div>
      <div class="clone-preview" id="cloneAgentPreview" hidden>
        <div class="clone-preview-fields">
          <div>
            <label for="cloneCandidateName">Имя нового агента</label>
            <input id="cloneCandidateName">
          </div>
          <div>
            <label for="cloneCandidatePhone">Телефон</label>
            <input id="cloneCandidatePhone">
          </div>
        </div>
        <label for="cloneCandidateProfile">Профиль нового агента</label>
        <textarea class="clone-profile-preview" id="cloneCandidateProfile" spellcheck="false"></textarea>
        <label for="cloneRawResponse">Ответ Gemini</label>
        <textarea class="clone-raw-response" id="cloneRawResponse" readonly spellcheck="false"></textarea>
      </div>
      <div class="modal-actions">
        <button class="secondary" id="cancelCloneAgentButton" type="button">Отмена</button>
        <button class="primary" id="acceptCloneAgentButton" type="button" disabled>Accept</button>
      </div>
    </div>
  </div>
  <div class="modal-backdrop" id="copyProfilePhoneModal" hidden>
    <div class="modal" role="dialog" aria-modal="true" aria-labelledby="copyProfilePhoneTitle">
      <div class="modal-head">
        <h2 id="copyProfilePhoneTitle">Скопировать с параметрами</h2>
        <button class="icon-button" id="closeCopyProfilePhoneButton" type="button" title="Закрыть" aria-label="Закрыть">×</button>
      </div>
      <div class="subtle">Выберите телефон, который будет добавлен в URL очередей в скопированном тексте.</div>
      <div class="clone-form-grid">
        <div>
          <label for="copyProfilePhoneSelect">Телефон агента</label>
          <select id="copyProfilePhoneSelect"></select>
        </div>
        <div>
          <label for="copyProfilePhoneManual">Другой номер</label>
          <input id="copyProfilePhoneManual" placeholder="1001">
        </div>
      </div>
      <div class="status" id="copyProfilePhoneStatus"></div>
      <div class="modal-actions">
        <button class="secondary" id="cancelCopyProfilePhoneButton" type="button">Отмена</button>
        <button class="primary" id="confirmCopyProfilePhoneButton" type="button">Скопировать</button>
      </div>
    </div>
  </div>
  <main class="view consultants-view" data-view="consultants">
    <section>
      <div class="panel">
        <h2>Консультация с экспертами</h2>
        <div class="subtle">Отдельное телефонное окно для Subject Matter Experts. Вопрос уходит на телефон выбранного консультанта, ответ проверяется по телефону отправителя.</div>
        <div class="grid">
          <div>
            <label for="consultantExpert">Консультант</label>
            <select id="consultantExpert"></select>
          </div>
          <div>
            <label for="consultantConversation">Номер разговора</label>
            <input id="consultantConversation" placeholder="consult-001">
          </div>
          <div>
            <label for="consultantSender">Кто спрашивает</label>
            <select id="consultantSender"></select>
          </div>
          <div>
            <label for="consultantReplyPhone">Ответ придет на телефон</label>
            <input id="consultantReplyPhone" readonly>
          </div>
        </div>
        <label for="consultantQuestion">Вопрос консультанту</label>
        <textarea class="phone-message" id="consultantQuestion" spellcheck="false"></textarea>
        <div class="actions">
          <button class="primary" id="askConsultantButton" type="button">Отправить вопрос</button>
          <button class="secondary" id="checkConsultantReplyButton" type="button">Проверить ответ</button>
        </div>
        <div class="status" id="consultantStatus"></div>
        <label for="consultantReply">Ответ консультанта</label>
        <textarea class="phone-message" id="consultantReply" readonly spellcheck="false"></textarea>
      </div>
    </section>
  </main>
  <div class="modal-backdrop" id="helpModal" hidden>
    <div class="modal help-modal" role="dialog" aria-modal="true" aria-labelledby="helpModalTitle">
      <div class="modal-head">
        <h2 id="helpModalTitle">Подсказка</h2>
        <button class="icon-button" id="closeHelpModalButton" type="button" title="Закрыть" aria-label="Закрыть">×</button>
      </div>
      <div class="help-modal-body" id="helpModalBody"></div>
    </div>
  </div>
  <script>
    const queueConfig = __QUEUE_CONFIG_JSON__;
    const defaultActors = ["Analyst", "Backend", "Frontend", "Designer", "Owner", "Tester", "Programmer"];
    let actors = defaultActors.slice();
    const contextSummary = {
      backend: "Backend: /work для задач бэкендеру, /test для проверки аналитиком.",
      frontend: "Frontend: /work-design для задач фронтендеру, /test-design для проверки дизайнером."
    };
    let activeContext = "backend";
    const helpCatalog = {
      "page:messages": {
        title: "Сообщения",
        purpose: "Здесь создают задачи, отправляют отчеты и смотрят, что уже произошло в рабочем процессе.",
        logic: "Вы выбираете направление, отправителя, получателя и статус. После отправки сообщение попадает в нужную очередь, а запись сохраняется в истории."
      },
      "page:git-context": {
        title: "Git context",
        purpose: "Здесь выбирают активный проект/телефон и управляют связями phone -> Git repository.",
        logic: "После выбора активного номера остальные вкладки показывают очереди, историю и файлы только этого Git context."
      },
      "page:cycles": {
        title: "Граф группы и циклы",
        purpose: "Показывает, какие агенты участвуют в цикле разработки, кто сейчас активен и как задача передается между ними.",
        logic: "Интерфейс читает журнал Groups & Cycles API. TASK_STARTED подсвечивает активного агента, а HANDOFF_TRIGGERED выделяет фактический переход задачи."
      },
      "page:screenshots": {
        title: "Скриншоты",
        purpose: "Раздел нужен, чтобы хранить изображения с доказательствами: ошибки интерфейса, состояние страницы, результат проверки.",
        logic: "Сначала создайте папку, затем загрузите или вставьте скриншоты. Путь к папке можно скопировать и приложить к сообщению."
      },
      "page:evidence": {
        title: "Доказательства",
        purpose: "Здесь хранят файлы, которые подтверждают выводы: таблицы, JSON, отчеты, документы и другие артефакты.",
        logic: "Файлы складываются в отдельные папки. Ссылка на папку или файл помогает другому участнику быстро найти подтверждение."
      },
      "page:agents": {
        title: "Агенты",
        purpose: "Агенты - это участники процесса с именем, телефоном и описанием роли.",
        logic: "Имена агентов используются в полях отправителя и получателя. Профиль объясняет агенту, чем он занимается и как должен отвечать."
      },
      "page:consultants": {
        title: "Консультанты",
        purpose: "Раздел для вопросов экспертам, когда нужно мнение специалиста по архитектуре, backend, frontend или UX.",
        logic: "Вы выбираете консультанта, пишете вопрос и отправляете его в отдельный канал. Ответ можно проверить по телефону отправителя."
      },
      "context:backend": {
        title: "Данные и алгоритмы",
        purpose: "Этот контур используют для задач, связанных с серверной логикой, обработкой данных, API и проверкой качества алгоритмов.",
        logic: "Задачи уходят разработчику backend, а результаты возвращаются аналитику на проверку."
      },
      "context:frontend": {
        title: "Интерфейс и UX",
        purpose: "Этот контур используют для задач по внешнему виду, удобству, кликам, адаптивности и ошибкам интерфейса.",
        logic: "Задачи уходят frontend-разработчику, а результат возвращается дизайнеру или UI-тестировщику."
      },
      "heading:Контур разработки": {
        title: "Контур разработки",
        purpose: "Помогает выбрать, с какой частью проекта вы работаете: с данными или с интерфейсом.",
        logic: "Выбор контура меняет доступные направления очередей, чтобы сообщение попало правильной команде."
      },
      "heading:Отправить сообщение": {
        title: "Отправить сообщение",
        purpose: "Главная форма для постановки задач и передачи результатов проверки.",
        logic: "Заполните направление, статус, отправителя, получателя и текст. Затем нажмите «Отправить»."
      },
      "heading:Телефонная пара": {
        title: "Телефонная пара",
        purpose: "Нужна для адресного обмена, когда сообщение должно попасть только участнику с конкретным телефоном.",
        logic: "Один номер разговора объединяет переписку, а телефон получателя определяет, кто именно увидит сообщение."
      },
      "heading:Git context": {
        title: "Git context",
        purpose: "Управляет связями номер телефона -> репозиторий проекта.",
        logic: "Очереди используют URL вида /work/{phone}; система берет Git context по этому номеру и пишет его в историю."
      },
      "heading:Email routes": {
        title: "Email routes",
        purpose: "Позволяет связать email с именем отправителя для внешних уведомлений или маршрутизации.",
        logic: "Каждая строка хранится отдельно. Один email можно использовать несколько раз для разных ролей."
      },
      "heading:Очереди": {
        title: "Очереди",
        purpose: "Показывает, сколько сообщений ожидают обработки в каждом направлении.",
        logic: "Когда сообщение отправлено, оно появляется в очереди. Когда получатель его забирает, оно уходит из очереди в историю."
      },
      "heading:Ожидающие задания": {
        title: "Ожидающие задания",
        purpose: "Здесь видны сообщения, которые еще не поставлены в очередь.",
        logic: "Они ждут заданного времени или события, например появления STATUS: PASS."
      },
      "heading:История циклов разработки": {
        title: "История циклов разработки",
        purpose: "Это журнал всех отправленных, полученных и удаленных сообщений.",
        logic: "По истории можно восстановить, кто что просил, что было исправлено и почему задача прошла или не прошла проверку."
      },
      "heading:Создать папку со скриншотами": {
        title: "Создать папку со скриншотами",
        purpose: "Создает отдельное место для набора скриншотов по одной проверке или одной задаче.",
        logic: "После создания папку можно выбрать при загрузке изображений и приложить ее путь к сообщению."
      },
      "heading:Загрузить скриншоты": {
        title: "Загрузить скриншоты",
        purpose: "Добавляет изображения в выбранную папку.",
        logic: "Можно выбрать файлы, перетащить их мышью или вставить изображение из буфера обмена."
      },
      "heading:Корзина": {
        title: "Корзина",
        purpose: "Временное место для файлов, которые вы хотите удалить.",
        logic: "Сначала переместите файлы в корзину, затем очистите корзину, если точно хотите удалить их окончательно."
      },
      "heading:Папки со скриншотами": {
        title: "Папки со скриншотами",
        purpose: "Список созданных папок и файлов внутри них.",
        logic: "Можно открыть папку, посмотреть изображения, скопировать путь или выбрать папку для дальнейшей загрузки."
      },
      "heading:Создать папку с доказательствами": {
        title: "Создать папку с доказательствами",
        purpose: "Создает отдельное место для файлов, которые подтверждают выводы проверки.",
        logic: "Одна папка обычно соответствует одной задаче, одному отчету или одному набору доказательств."
      },
      "heading:Загрузить файлы": {
        title: "Загрузить файлы",
        purpose: "Добавляет документы, таблицы, JSON и другие файлы в выбранную папку доказательств.",
        logic: "После загрузки путь к файлу или папке можно вставить в сообщение как подтверждение."
      },
      "heading:Папки с доказательствами": {
        title: "Папки с доказательствами",
        purpose: "Список папок с файлами, которые помогают проверить или подтвердить результат.",
        logic: "Файлы можно открыть, скопировать, перенести в другую папку или удалить через корзину."
      },
      "heading:Агенты и профили": {
        title: "Агенты и профили",
        purpose: "Здесь настраивают участников процесса: имя, телефон, роль и рабочие инструкции.",
        logic: "Профиль агента превращается в понятную инструкцию для выбранной роли. Параметры подставляют разные URL, телефоны и настройки."
      },
      "heading:Специализированные агенты": {
        title: "Специализированные агенты",
        purpose: "Это агенты, которым уже назначена конкретная роль.",
        logic: "Они готовы получать задачи и отвечать в рамках своего профиля."
      },
      "heading:Живые пустые агенты": {
        title: "Живые пустые агенты",
        purpose: "Это временные агенты без роли, которые еще ждут специализацию.",
        logic: "Пока срок ожидания не истек, им можно отправить профиль и превратить в полноценного участника."
      },
      "heading:Истекшие пустые агенты": {
        title: "Истекшие пустые агенты",
        purpose: "Это пустые агенты, которые слишком долго не получили роль.",
        logic: "Они больше не считаются активными кандидатами для специализации."
      },
      "heading:Prompt пустого агента для запуска": {
        title: "Prompt пустого агента",
        purpose: "Текст, который нужно дать новому пустому агенту при запуске.",
        logic: "Этот prompt объясняет агенту, как получить специализацию и как подтвердить готовность."
      },
      "heading:Специализировать пустого агента": {
        title: "Специализировать пустого агента",
        purpose: "Позволяет назначить пустому агенту имя и конкретную рабочую роль.",
        logic: "Вы выбираете телефон пустого агента, берете шаблон, при необходимости правите профиль и отправляете специализацию."
      },
      "heading:Скопировать агента": {
        title: "Скопировать агента",
        purpose: "Создает нового агента на основе уже существующего профиля.",
        logic: "Можно добавить уточнения, чтобы новая роль была похожа на исходную, но с другими обязанностями."
      },
      "heading:Консультация с экспертами": {
        title: "Консультация с экспертами",
        purpose: "Форма для вопроса специалисту, когда обычной очереди разработки недостаточно.",
        logic: "Вопрос отправляется выбранному консультанту, а ответ ожидается на телефон того, кто спрашивает."
      },
      "heading:Предпросмотр с параметрами": {
        title: "Предпросмотр с параметрами",
        purpose: "Показывает, как будет выглядеть профиль агента после подстановки значений.",
        logic: "Если параметр не заполнен, он подсвечивается, чтобы вы заметили пропуск до сохранения."
      },
      "heading:Просмотр скриншота": {
        title: "Просмотр скриншота",
        purpose: "Открывает изображение крупно, чтобы его можно было рассмотреть.",
        logic: "Это только просмотр: файл не меняется и не удаляется."
      },
      "heading:Просмотр доказательства": {
        title: "Просмотр доказательства",
        purpose: "Открывает файл доказательства внутри окна.",
        logic: "Если браузер может показать файл, он откроется для просмотра. Оригинал при этом не меняется."
      },
      "field:queue": {
        title: "Направление",
        purpose: "Показывает, куда отправится сообщение.",
        logic: "Выбор зависит от контура. Например, задача backend уходит в одну очередь, а проверка результата - в другую."
      },
      "field:status": {
        title: "Статус",
        purpose: "Коротко объясняет смысл сообщения.",
        logic: "INITIAL означает новую задачу, READY - готово к проверке, FAIL - нужно исправить, PASS - принято."
      },
      "field:sender": {
        title: "От кого",
        purpose: "Имя участника, который отправляет сообщение.",
        logic: "Это помогает получателю понять источник задачи или отчета."
      },
      "field:receiver": {
        title: "Кому",
        purpose: "Имя участника, которому адресовано сообщение.",
        logic: "Получатель показывает, кто должен взять сообщение в работу."
      },
      "field:message": {
        title: "Текст сообщения",
        purpose: "Основное содержание задачи, отчета или ответа.",
        logic: "Пишите сюда требования, что было проверено, что найдено и что нужно сделать дальше."
      },
      "field:attachmentFolderSelect": {
        title: "Папка",
        purpose: "Выбор папки со скриншотами или доказательствами для приложения к сообщению.",
        logic: "Система копирует путь к выбранной папке, чтобы другой участник смог открыть нужные файлы."
      },
      "field:attachmentDescription": {
        title: "Текст пояснения",
        purpose: "Короткое человеческое описание того, что лежит в выбранной папке.",
        logic: "Пояснение вместе с путем можно вставить в сообщение одной кнопкой."
      },
      "field:scheduleMode": {
        title: "Когда поставить в очередь",
        purpose: "Позволяет отправить сообщение сразу или отложить его.",
        logic: "Можно отправить сейчас, через заданное время или после того, как в процессе появится PASS."
      },
      "field:scheduleDelayMinutes": {
        title: "Минуты",
        purpose: "Сколько минут подождать перед постановкой сообщения в очередь.",
        logic: "Используйте, если нужно дать участнику время завершить предыдущий шаг."
      },
      "field:phoneChannel": {
        title: "Канал пары",
        purpose: "Выбирает тип адресного телефонного канала.",
        logic: "Канал определяет, какие роли обычно обмениваются сообщениями в этой паре."
      },
      "field:conversationPhone": {
        title: "Номер разговора",
        purpose: "Общий номер темы или случая, по которому идет переписка.",
        logic: "Одинаковый номер помогает связать несколько сообщений в одну рабочую линию."
      },
      "field:phoneSender": {
        title: "От кого",
        purpose: "Кто отправляет сообщение в телефонной паре.",
        logic: "Система возьмет телефон выбранного агента и сохранит это в адресной истории."
      },
      "field:phoneReceiver": {
        title: "Кому",
        purpose: "Кто должен получить сообщение по телефону.",
        logic: "Только агент с подходящим телефоном увидит это сообщение при опросе."
      },
      "field:phoneMessage": {
        title: "Текст сообщения",
        purpose: "Содержание адресного сообщения.",
        logic: "Пишите конкретно: что нужно сделать, что проверить или на что ответить."
      },
      "field:gitPhone": {
        title: "Номер проекта / Git context",
        purpose: "Номер, через который очереди определяют проект и Git context.",
        logic: "Сообщения агентов используют URL вида /work/{phone}; этот номер должен быть привязан к одному Git context."
      },
      "field:gitNewPhone": {
        title: "Новый номер Git context",
        purpose: "Номер для новой связи phone -> Git context.",
        logic: "Введите свободный номер, нажмите «Добавить Git context», затем заполните Git address и сохраните привязку."
      },
      "field:gitAddress": {
        title: "Git address",
        purpose: "Адрес папки или репозитория для выбранного телефона.",
        logic: "При сохранении система связывает телефон с этим репозиторием и определяет текущий commit."
      },
      "field:historyFrom": {
        title: "От",
        purpose: "Начальная дата фильтра истории.",
        logic: "Показывает записи, которые появились не раньше выбранной даты."
      },
      "field:historyTo": {
        title: "До",
        purpose: "Конечная дата фильтра истории.",
        logic: "Если поле пустое, показывается история начиная с даты «От» без верхней границы."
      },
      "field:screenshotFolderSelect": {
        title: "Папка назначения",
        purpose: "Папка, куда будут загружены скриншоты.",
        logic: "Перед загрузкой выберите существующую папку или создайте новую."
      },
      "field:screenshotFiles": {
        title: "Скриншоты",
        purpose: "Выбор изображений с компьютера.",
        logic: "Можно выбрать несколько файлов сразу. Поддерживаются обычные форматы изображений."
      },
      "field:screenshotFoldersFrom": {
        title: "От",
        purpose: "Дата, начиная с которой показывать папки со скриншотами.",
        logic: "Помогает быстро найти сегодняшние или недавние папки."
      },
      "field:screenshotFoldersTo": {
        title: "До",
        purpose: "Дата, до которой показывать папки со скриншотами.",
        logic: "Используйте, если нужно найти папки за конкретный период."
      },
      "field:evidenceFolderSelect": {
        title: "Папка назначения",
        purpose: "Папка, куда будут загружены доказательные файлы.",
        logic: "Выбранная папка станет местом хранения новых файлов."
      },
      "field:evidenceFiles": {
        title: "Файлы",
        purpose: "Выбор файлов-доказательств с компьютера.",
        logic: "Можно загрузить документы, таблицы, JSON и другие файлы, если они нужны для проверки."
      },
      "field:evidenceFoldersFrom": {
        title: "От",
        purpose: "Дата, начиная с которой показывать папки с доказательствами.",
        logic: "Помогает не искать вручную среди старых папок."
      },
      "field:evidenceFoldersTo": {
        title: "До",
        purpose: "Дата, до которой показывать папки с доказательствами.",
        logic: "Используется для поиска папок в выбранном диапазоне дат."
      },
      "field:agentSelector": {
        title: "Агент",
        purpose: "Выбор агента, которого вы хотите посмотреть или изменить.",
        logic: "После выбора ниже откроется профиль этого агента."
      },
      "field:specializePhone": {
        title: "Телефон пустого агента",
        purpose: "Телефон агента, которому нужно назначить роль.",
        logic: "Телефон нужен, чтобы специализация попала именно этому агенту."
      },
      "field:specializeTemplate": {
        title: "Шаблон специализации",
        purpose: "Готовый профиль, от которого можно начать.",
        logic: "Шаблон заполняет имя и профиль, после чего текст можно отредактировать под задачу."
      },
      "field:specializeName": {
        title: "Новое имя агента",
        purpose: "Имя, под которым агент будет виден в системе.",
        logic: "Имя должно быть понятным: например, Backend Advisor или UI Tester."
      },
      "field:specializeProfile": {
        title: "Новый профиль",
        purpose: "Инструкция, которая объясняет агенту его роль и правила работы.",
        logic: "Чем понятнее профиль, тем стабильнее агент будет отвечать."
      },
      "field:cloneSourceAgent": {
        title: "Кого клонировать",
        purpose: "Исходный агент, на основе которого создается новый.",
        logic: "Система берет профиль выбранного агента и помогает адаптировать его под новую роль."
      },
      "field:cloneGeminiModel": {
        title: "Gemini model",
        purpose: "Модель, которая поможет переписать или адаптировать профиль агента.",
        logic: "Если вы не уверены, оставьте значение по умолчанию."
      },
      "field:cloneGeminiApiKey": {
        title: "Gemini API key",
        purpose: "Ключ доступа к Gemini для разового создания профиля.",
        logic: "Ключ не сохраняется в этом приложении. Если поле пустое, сервер попробует взять ключ из переменной окружения."
      },
      "field:cloneRefinements": {
        title: "Уточнения для нового агента",
        purpose: "Ваши пожелания к отличиям нового агента от исходного.",
        logic: "Напишите простыми словами, что сохранить, что убрать и какую новую задачу должен выполнять агент."
      },
      "field:cloneCandidateName": {
        title: "Имя нового агента",
        purpose: "Имя, предложенное для копии агента.",
        logic: "Перед принятием можно изменить имя вручную."
      },
      "field:cloneCandidatePhone": {
        title: "Телефон",
        purpose: "Телефон нового агента.",
        logic: "Телефон используется для адресной доставки сообщений."
      },
      "field:cloneCandidateProfile": {
        title: "Профиль нового агента",
        purpose: "Готовая инструкция для нового агента.",
        logic: "Проверьте текст перед сохранением: он определяет поведение агента."
      },
      "field:cloneRawResponse": {
        title: "Ответ Gemini",
        purpose: "Исходный ответ модели, из которого сформирован профиль.",
        logic: "Это поле помогает понять, что именно вернула модель, если результат нужно проверить."
      },
      "field:consultantExpert": {
        title: "Консультант",
        purpose: "Эксперт, которому вы хотите задать вопрос.",
        logic: "Выбирайте консультанта по теме вопроса: backend, frontend, UX или архитектура."
      },
      "field:consultantConversation": {
        title: "Номер разговора",
        purpose: "Номер темы консультации.",
        logic: "Одинаковый номер помогает связать вопрос и ответ."
      },
      "field:consultantSender": {
        title: "Кто спрашивает",
        purpose: "Участник, от имени которого задается вопрос.",
        logic: "Ответ будет ожидаться на телефоне этого участника."
      },
      "field:consultantReplyPhone": {
        title: "Ответ придет на телефон",
        purpose: "Показывает, где ждать ответ консультанта.",
        logic: "Поле заполняется автоматически по выбранному отправителю."
      },
      "field:consultantQuestion": {
        title: "Вопрос консультанту",
        purpose: "Текст вопроса для эксперта.",
        logic: "Пишите проблему, контекст и что именно хотите получить: решение, оценку риска или совет."
      },
      "field:consultantReply": {
        title: "Ответ консультанта",
        purpose: "Здесь появится ответ, когда консультант его отправит.",
        logic: "Нажмите «Проверить ответ», чтобы обновить это поле."
      },
      "label:Имя агента": {
        title: "Имя агента",
        purpose: "Человеческое имя роли в системе.",
        logic: "Это имя будет видно в списках отправителей, получателей и агентов."
      },
      "label:Телефон": {
        title: "Телефон",
        purpose: "Адрес агента для личной доставки сообщений.",
        logic: "Если сообщение идет по телефону, система ищет агента с этим номером."
      },
      "label:Профиль агента": {
        title: "Профиль агента",
        purpose: "Описание роли, обязанностей и формата ответа агента.",
        logic: "Профиль влияет на то, как агент понимает задачи и что считает правильным результатом."
      },
      "label:Параметры шаблона": {
        title: "Параметры шаблона",
        purpose: "Переменные, которые подставляются в профиль агента.",
        logic: "Например, один профиль может использовать разные URL или телефоны без переписывания всего текста."
      },
      "button:refreshButton": {
        title: "Обновить",
        purpose: "Обновляет данные на странице.",
        logic: "Используйте, если хотите сразу увидеть новые очереди, историю или файлы, не дожидаясь автообновления."
      },
      "button:sendButton": {
        title: "Отправить",
        purpose: "Отправляет подготовленное сообщение.",
        logic: "Сообщение попадет в выбранную очередь сейчас или по выбранному расписанию."
      },
      "button:taskTemplateButton": {
        title: "Шаблон задачи",
        purpose: "Вставляет заготовку для новой задачи.",
        logic: "Шаблон помогает не забыть требования и начальное задание."
      },
      "button:failTemplateButton": {
        title: "Шаблон FAIL",
        purpose: "Вставляет заготовку отчета о проблеме.",
        logic: "Используйте, когда проверка не пройдена и нужно объяснить, что исправить."
      },
      "button:readyTemplateButton": {
        title: "Шаблон READY",
        purpose: "Вставляет заготовку сообщения о готовности к проверке.",
        logic: "Используйте, когда работа завершена и ее нужно передать проверяющему."
      },
      "button:copyAttachmentFolderPathButton": {
        title: "Скопировать путь",
        purpose: "Копирует путь к выбранной папке.",
        logic: "Путь можно вставить в сообщение, чтобы другой участник открыл нужные файлы."
      },
      "button:copyAttachmentMessageButton": {
        title: "Скопировать сообщение",
        purpose: "Копирует готовую строку с пояснением и путем.",
        logic: "Так проще приложить доказательства к задаче или отчету."
      },
      "button:refreshAttachmentFoldersButton": {
        title: "Обновить список",
        purpose: "Перечитывает список доступных папок.",
        logic: "Нужно, если папка была создана недавно и еще не появилась в списке."
      },
      "button:sendPhoneMessageButton": {
        title: "Отправить по телефону",
        purpose: "Отправляет адресное сообщение выбранному получателю.",
        logic: "Сообщение увидит агент с телефоном получателя в рамках указанного номера разговора."
      },
      "button:saveGitButton": {
        title: "Сохранить привязку телефона",
        purpose: "Сохраняет связь выбранного телефона с репозиторием проекта.",
        logic: "После сохранения queue URL с этим телефоном получает project/git metadata автоматически."
      },
      "button:addGitContextButton": {
        title: "Добавить Git context",
        purpose: "Добавляет новый номер в список Git contexts на экране.",
        logic: "Номер появится в выборе сразу, но постоянной связь станет после сохранения Git address."
      },
      "button:deleteGitContextButton": {
        title: "Удалить выбранный Git context",
        purpose: "Удаляет сохраненную связь выбранного номера с репозиторием.",
        logic: "История не стирается, но новые очереди больше не смогут использовать этот номер, пока вы не сохраните его снова."
      },
      "button:checkGitButton": {
        title: "Проверить commit",
        purpose: "Показывает текущую версию кода.",
        logic: "Commit помогает понять, какую именно версию проверяют участники."
      },
      "button:addEmailRouteButton": {
        title: "Добавить email",
        purpose: "Добавляет новую строку email-маршрута.",
        logic: "Заполните email и имя отправителя, затем сохраните список."
      },
      "button:saveEmailRoutesButton": {
        title: "Сохранить список",
        purpose: "Сохраняет все email-маршруты.",
        logic: "Без сохранения изменения останутся только на экране."
      },
      "button:applyHistoryFilterButton": {
        title: "Показать",
        purpose: "Применяет выбранный фильтр по датам.",
        logic: "История перезагрузится и покажет только подходящие записи."
      },
      "button:copyHistoryButton": {
        title: "Скопировать все сообщения",
        purpose: "Копирует видимую историю в буфер обмена.",
        logic: "Это удобно для передачи полного контекста в отчет или другому участнику."
      },
      "button:copyHistoryWithPatchesButton": {
        title: "Скопировать сообщения + патчи",
        purpose: "Копирует историю и вставляет между сообщениями patch-блоки, когда commit изменился.",
        logic: "Если несколько сообщений относятся к одному и тому же commit, patch не повторяется. Когда следующий commit отличается, система получает diff между предыдущим и новым commit и вставляет его перед сообщением нового commit."
      },
      "button:createScreenshotFolderButton": {
        title: "Создать папку",
        purpose: "Создает новую папку для скриншотов.",
        logic: "После создания она автоматически становится доступной для загрузки."
      },
      "button:refreshScreenshotFoldersButton": {
        title: "Обновить список",
        purpose: "Перечитывает список папок со скриншотами.",
        logic: "Используйте, если папки или файлы изменились."
      },
      "button:uploadScreenshotsButton": {
        title: "Загрузить",
        purpose: "Загружает выбранные скриншоты в папку назначения.",
        logic: "Файлы сохраняются на сервере и появляются в списке папки."
      },
      "button:pasteScreenshotButton": {
        title: "Вставить из буфера",
        purpose: "Добавляет изображение, которое сейчас находится в буфере обмена.",
        logic: "Полезно после Print Screen или копирования изображения из браузера."
      },
      "button:copySelectedScreenshotFolderPathButton": {
        title: "Скопировать путь папки",
        purpose: "Копирует путь выбранной папки скриншотов.",
        logic: "Путь можно вставить в сообщение как ссылку на визуальные доказательства."
      },
      "button:clearScreenshotTrashButton": {
        title: "Очистить корзину",
        purpose: "Окончательно удаляет скриншоты, которые лежат в корзине.",
        logic: "Перед удалением система попросит подтверждение."
      },
      "button:applyScreenshotFolderFilterButton": {
        title: "Показать",
        purpose: "Показывает папки со скриншотами за выбранные даты.",
        logic: "Фильтр помогает быстро найти нужную папку."
      },
      "button:resetScreenshotFolderFilterButton": {
        title: "Сегодня",
        purpose: "Возвращает фильтр папок со скриншотами к сегодняшней дате.",
        logic: "Это быстрый способ увидеть свежие материалы."
      },
      "button:createEvidenceFolderButton": {
        title: "Создать папку",
        purpose: "Создает новую папку для доказательных файлов.",
        logic: "После создания в нее можно загрузить таблицы, отчеты и другие материалы."
      },
      "button:refreshEvidenceFoldersButton": {
        title: "Обновить список",
        purpose: "Перечитывает список папок с доказательствами.",
        logic: "Используйте, если папки или файлы были изменены."
      },
      "button:uploadEvidenceButton": {
        title: "Загрузить",
        purpose: "Загружает выбранные файлы в папку доказательств.",
        logic: "После загрузки файл можно открыть, скопировать или приложить к сообщению."
      },
      "button:copySelectedEvidenceFolderPathButton": {
        title: "Скопировать путь папки",
        purpose: "Копирует путь выбранной папки доказательств.",
        logic: "Путь можно вставить в отчет, чтобы другой участник быстро нашел файлы."
      },
      "button:clearEvidenceTrashButton": {
        title: "Очистить корзину",
        purpose: "Окончательно удаляет файлы, помещенные в корзину.",
        logic: "Удаление происходит только после подтверждения."
      },
      "button:applyEvidenceFolderFilterButton": {
        title: "Показать",
        purpose: "Показывает папки доказательств за выбранные даты.",
        logic: "Фильтр сокращает список до нужного периода."
      },
      "button:resetEvidenceFolderFilterButton": {
        title: "Сегодня",
        purpose: "Возвращает список папок доказательств к сегодняшней дате.",
        logic: "Удобно для текущих проверок."
      },
      "button:createEmptyAgentButton": {
        title: "Сгенерировать пустого агента",
        purpose: "Создает временного агента без конкретной роли.",
        logic: "Такой агент ждет, пока вы отправите ему специализацию."
      },
      "button:addAgentButton": {
        title: "Добавить агента",
        purpose: "Создает новую запись агента вручную.",
        logic: "После заполнения имени, телефона и профиля не забудьте сохранить список."
      },
      "button:cloneAgentButton": {
        title: "Скопировать агента",
        purpose: "Открывает окно создания агента на основе существующего.",
        logic: "Это быстрее, чем писать похожий профиль с нуля."
      },
      "button:removeSelectedAgentButton": {
        title: "Удалить агента",
        purpose: "Удаляет выбранного агента из списка на экране.",
        logic: "Чтобы изменение осталось после перезагрузки, сохраните агентов."
      },
      "button:copyAgentProfileButton": {
        title: "Скопировать с параметрами",
        purpose: "Открывает выбор телефона и копирует профиль агента уже с подставленными значениями.",
        logic: "Выбранный номер добавляется в URL очередей в скопированном тексте."
      },
      "field:copyProfilePhoneSelect": {
        title: "Телефон агента",
        purpose: "Выбирает номер, который будет добавлен к endpoint URL в копируемом профиле.",
        logic: "Например, GET http://localhost:8025/test станет GET http://localhost:8025/test/1001."
      },
      "field:copyProfilePhoneManual": {
        title: "Другой номер",
        purpose: "Позволяет вручную указать телефон, которого нет в списке агентов.",
        logic: "Если поле заполнено, оно имеет приоритет над выбранным номером из списка."
      },
      "button:saveAgentsButton": {
        title: "Сохранить агентов",
        purpose: "Сохраняет текущий список агентов и их профили.",
        logic: "Без сохранения изменения могут потеряться после обновления страницы."
      },
      "button:copyEmptyAgentPromptButton": {
        title: "Скопировать prompt",
        purpose: "Копирует инструкцию для пустого агента.",
        logic: "Этот текст нужен для запуска агента, который потом получит специализацию."
      },
      "button:sendSpecializationButton": {
        title: "Отправить специализацию",
        purpose: "Назначает пустому агенту имя и профиль.",
        logic: "Агент получит специализацию при следующем опросе своего телефона."
      },
      "button:loadGeminiModelsButton": {
        title: "Загрузить модели",
        purpose: "Получает список доступных моделей Gemini.",
        logic: "Используйте, если хотите выбрать другую модель для создания профиля."
      },
      "button:requestCloneAgentButton": {
        title: "Сформировать клон",
        purpose: "Просит модель подготовить новый профиль агента.",
        logic: "Перед сохранением вы увидите результат и сможете его поправить."
      },
      "button:cancelCloneAgentButton": {
        title: "Отмена",
        purpose: "Закрывает окно копирования агента без сохранения.",
        logic: "Все несохраненные данные в этом окне будут отброшены."
      },
      "button:acceptCloneAgentButton": {
        title: "Accept",
        purpose: "Принимает подготовленного агента и добавляет его в список.",
        logic: "После принятия сохраните агентов, чтобы новый профиль остался в файле."
      },
      "button:askConsultantButton": {
        title: "Отправить вопрос",
        purpose: "Отправляет вопрос выбранному консультанту.",
        logic: "Вопрос попадает в отдельный телефонный канал эксперта."
      },
      "button:checkConsultantReplyButton": {
        title: "Проверить ответ",
        purpose: "Проверяет, пришел ли ответ консультанта.",
        logic: "Если ответ есть, он появится в поле ниже."
      },
      "action:remove-email-route": {
        title: "Удалить",
        purpose: "Убирает строку email-маршрута.",
        logic: "Изменение станет постоянным только после сохранения списка."
      },
      "action:add-agent-param": {
        title: "Добавить параметр",
        purpose: "Добавляет новую переменную для профиля агента.",
        logic: "Слева пишется имя параметра, справа - значение, которое будет подставлено."
      },
      "action:remove-agent-param": {
        title: "Удалить параметр",
        purpose: "Удаляет одну переменную профиля.",
        logic: "Если профиль использует этот параметр, в предпросмотре появится пропуск."
      },
      "action:toggle-screenshot-folder": {
        title: "Развернуть / свернуть",
        purpose: "Показывает или скрывает файлы внутри папки.",
        logic: "Это не меняет файлы, а только раскрывает список на экране."
      },
      "action:copy-screenshot-folder-path": {
        title: "Скопировать путь",
        purpose: "Копирует путь к папке со скриншотами.",
        logic: "Путь можно приложить к сообщению как доказательство."
      },
      "action:select-screenshot-folder": {
        title: "Выбрать",
        purpose: "Делает эту папку папкой назначения.",
        logic: "Новые скриншоты будут загружаться именно сюда."
      },
      "action:preview-screenshot-file": {
        title: "Просмотр",
        purpose: "Открывает скриншот в большом окне.",
        logic: "Это безопасный просмотр без изменения файла."
      },
      "action:copy-screenshot-path": {
        title: "Путь",
        purpose: "Копирует путь к конкретному скриншоту.",
        logic: "Используйте, если в отчете нужен не весь набор, а один файл."
      },
      "action:copy-screenshot-file": {
        title: "Копировать",
        purpose: "Копирует скриншот в другую папку.",
        logic: "Выберите папку назначения в списке рядом, затем нажмите копирование."
      },
      "action:delete-screenshot-file": {
        title: "Удалить",
        purpose: "Удаляет скриншот из папки.",
        logic: "Используйте осторожно: файл будет удален с сервера."
      },
      "action:remove-from-screenshot-trash": {
        title: "Убрать из корзины",
        purpose: "Возвращает файл из списка на удаление.",
        logic: "Файл не будет удален при очистке корзины."
      },
      "action:toggle-evidence-folder": {
        title: "Развернуть / свернуть",
        purpose: "Показывает или скрывает файлы внутри папки доказательств.",
        logic: "Это только меняет видимость списка."
      },
      "action:copy-evidence-folder-path": {
        title: "Скопировать путь",
        purpose: "Копирует путь к папке доказательств.",
        logic: "Путь можно вставить в отчет или сообщение."
      },
      "action:select-evidence-folder": {
        title: "Выбрать",
        purpose: "Делает эту папку папкой назначения.",
        logic: "Новые файлы доказательств будут загружаться сюда."
      },
      "action:preview-evidence-file": {
        title: "Открыть",
        purpose: "Открывает файл доказательства для просмотра.",
        logic: "Файл не меняется; вы только смотрите его содержимое."
      },
      "action:copy-evidence-path": {
        title: "Путь",
        purpose: "Копирует путь к конкретному файлу доказательства.",
        logic: "Так можно сослаться на один важный файл."
      },
      "action:copy-evidence-file": {
        title: "Копировать",
        purpose: "Копирует файл доказательства в другую папку.",
        logic: "Выберите папку назначения рядом и нажмите копирование."
      },
      "action:delete-evidence-file": {
        title: "Удалить",
        purpose: "Удаляет файл доказательства из папки.",
        logic: "Используйте осторожно: файл будет удален с сервера."
      },
      "action:remove-from-evidence-trash": {
        title: "Убрать из корзины",
        purpose: "Убирает файл из списка на удаление.",
        logic: "Файл останется на месте после очистки корзины."
      },
      "action:delete-queued-message": {
        title: "Удалить из очереди",
        purpose: "Убирает сообщение, которое еще ожидает обработки.",
        logic: "История сохраняет факт удаления, чтобы было понятно, что произошло."
      },
      "action:cancel-scheduled-task": {
        title: "Отменить ожидание",
        purpose: "Отменяет отложенную отправку сообщения.",
        logic: "После отмены сообщение не попадет в очередь автоматически."
      },
      "action:copy-history-record": {
        title: "Скопировать сообщение",
        purpose: "Копирует одну запись истории.",
        logic: "Это удобно, если нужно процитировать конкретный отчет или задачу."
      },
      "action:restore-history-record": {
        title: "Вернуть сообщение в очередь",
        purpose: "Повторно ставит удаленное сообщение в очередь.",
        logic: "Используйте, если сообщение удалили ошибочно и его нужно снова обработать."
      },
      "action:delete-history-record": {
        title: "Стереть запись",
        purpose: "Удаляет техническую запись о ручном удалении из истории.",
        logic: "Это нужно редко. Используйте только если такая запись мешает журналу."
      }
    };

    const queueEl = document.getElementById("queue");
    const statusEl = document.getElementById("status");
    const senderEl = document.getElementById("sender");
    const receiverEl = document.getElementById("receiver");
    const messageEl = document.getElementById("message");
    const sendStatusEl = document.getElementById("sendStatus");
    const attachmentFolderSelectEl = document.getElementById("attachmentFolderSelect");
    const attachmentDescriptionEl = document.getElementById("attachmentDescription");
    const attachmentStatusEl = document.getElementById("attachmentStatus");
    const scheduleModeEl = document.getElementById("scheduleMode");
    const scheduleDelayFieldEl = document.getElementById("scheduleDelayField");
    const scheduleDelayMinutesEl = document.getElementById("scheduleDelayMinutes");
    const scheduleHintEl = document.getElementById("scheduleHint");
    const screenshotFolderSelectEl = document.getElementById("screenshotFolderSelect");
    const screenshotFilesEl = document.getElementById("screenshotFiles");
    const screenshotDropZoneEl = document.getElementById("screenshotDropZone");
    const screenshotFoldersStatusEl = document.getElementById("screenshotFoldersStatus");
    const screenshotUploadStatusEl = document.getElementById("screenshotUploadStatus");
    const screenshotTrashStatusEl = document.getElementById("screenshotTrashStatus");
    const screenshotFoldersCountEl = document.getElementById("screenshotFoldersCount");
    const screenshotFoldersFromEl = document.getElementById("screenshotFoldersFrom");
    const screenshotFoldersToEl = document.getElementById("screenshotFoldersTo");
    const screenshotFoldersFilterStatusEl = document.getElementById("screenshotFoldersFilterStatus");
    const screenshotFoldersEl = document.getElementById("screenshotFolders");
    const screenshotTrashZoneEl = document.getElementById("screenshotTrashZone");
    const screenshotTrashItemsEl = document.getElementById("screenshotTrashItems");
    const screenshotPreviewModalEl = document.getElementById("screenshotPreviewModal");
    const screenshotPreviewImageEl = document.getElementById("screenshotPreviewImage");
    const screenshotPreviewMetaEl = document.getElementById("screenshotPreviewMeta");
    const evidenceFolderSelectEl = document.getElementById("evidenceFolderSelect");
    const evidenceFilesEl = document.getElementById("evidenceFiles");
    const evidenceDropZoneEl = document.getElementById("evidenceDropZone");
    const evidenceFoldersStatusEl = document.getElementById("evidenceFoldersStatus");
    const evidenceUploadStatusEl = document.getElementById("evidenceUploadStatus");
    const evidenceTrashStatusEl = document.getElementById("evidenceTrashStatus");
    const evidenceFoldersCountEl = document.getElementById("evidenceFoldersCount");
    const evidenceFoldersFromEl = document.getElementById("evidenceFoldersFrom");
    const evidenceFoldersToEl = document.getElementById("evidenceFoldersTo");
    const evidenceFoldersFilterStatusEl = document.getElementById("evidenceFoldersFilterStatus");
    const evidenceFoldersEl = document.getElementById("evidenceFolders");
    const evidenceTrashZoneEl = document.getElementById("evidenceTrashZone");
    const evidenceTrashItemsEl = document.getElementById("evidenceTrashItems");
    const evidencePreviewModalEl = document.getElementById("evidencePreviewModal");
    const evidencePreviewFrameEl = document.getElementById("evidencePreviewFrame");
    const evidencePreviewMetaEl = document.getElementById("evidencePreviewMeta");
    const phoneChannelEl = document.getElementById("phoneChannel");
    const conversationPhoneEl = document.getElementById("conversationPhone");
    const phoneSenderEl = document.getElementById("phoneSender");
    const phoneReceiverEl = document.getElementById("phoneReceiver");
    const phoneMessageEl = document.getElementById("phoneMessage");
    const phoneStatusEl = document.getElementById("phoneStatus");
    const consultantExpertEl = document.getElementById("consultantExpert");
    const consultantConversationEl = document.getElementById("consultantConversation");
    const consultantSenderEl = document.getElementById("consultantSender");
    const consultantReplyPhoneEl = document.getElementById("consultantReplyPhone");
    const consultantQuestionEl = document.getElementById("consultantQuestion");
    const consultantReplyEl = document.getElementById("consultantReply");
    const consultantStatusEl = document.getElementById("consultantStatus");
    const cycleGraphViewEl = document.querySelector('main[data-view="cycles"]');
    const cycleGraphProjectSummaryEl = document.getElementById("cycleGraphProjectSummary");
    const cycleGraphCycleSelectEl = document.getElementById("cycleGraphCycleSelect");
    const cycleGraphCycleListEl = document.getElementById("cycleGraphCycleList");
    const cycleGraphStatusEl = document.getElementById("cycleGraphStatus");
    const cycleGraphTitleEl = document.getElementById("cycleGraphTitle");
    const cycleGraphSubtitleEl = document.getElementById("cycleGraphSubtitle");
    const cycleGraphMetricsEl = document.getElementById("cycleGraphMetrics");
    const cycleGraphCanvasEl = document.getElementById("cycleGraphCanvas");
    const cycleGraphTaskLineageEl = document.getElementById("cycleGraphTaskLineage");
    const cycleGraphEventCountEl = document.getElementById("cycleGraphEventCount");
    const cycleGraphHistoryEl = document.getElementById("cycleGraphHistory");
    const historyEl = document.getElementById("history");
    const historyCountEl = document.getElementById("historyCount");
    const historyGitContextEl = document.getElementById("historyGitContext");
    const historyFromEl = document.getElementById("historyFrom");
    const historyToEl = document.getElementById("historyTo");
    const historyStatusEl = document.getElementById("historyStatus");
    const queueStatsEl = document.getElementById("queueStats");
    const scheduledTasksEl = document.getElementById("scheduledTasks");
    const scheduledTasksCountEl = document.getElementById("scheduledTasksCount");
    const scheduledTasksStatusEl = document.getElementById("scheduledTasksStatus");
    const gitPhoneEl = document.getElementById("gitPhone");
    const gitNewPhoneEl = document.getElementById("gitNewPhone");
    const gitAddressEl = document.getElementById("gitAddress");
    const gitProjectNameEl = document.getElementById("gitProjectName");
    const gitContextKeyEl = document.getElementById("gitContextKey");
    const gitStatusEl = document.getElementById("gitStatus");
    const gitContextListEl = document.getElementById("gitContextList");
    const gitContextListCountEl = document.getElementById("gitContextListCount");
    const applyGitContextProjectButtonEl = document.getElementById("applyGitContextProjectButton");
    const activeGitContextSummaryEl = document.getElementById("activeGitContextSummary");
    const activeGitContextPhoneEl = document.getElementById("activeGitContextPhone");
    const activeGitContextProjectEl = document.getElementById("activeGitContextProject");
    const activeGitContextCommitEl = document.getElementById("activeGitContextCommit");
    const emailRoutesEl = document.getElementById("emailRoutes");
    const emailRoutesStatusEl = document.getElementById("emailRoutesStatus");
    const emailSenderOptionsEl = document.getElementById("emailSenderOptions");
    const agentSelectorEl = document.getElementById("agentSelector");
    const agentProjectManagerEl = document.getElementById("agentProjectManager");
    const projectAgentAttachSelectEl = document.getElementById("projectAgentAttachSelect");
    const attachAgentToProjectButtonEl = document.getElementById("attachAgentToProjectButton");
    const detachSelectedAgentFromProjectButtonEl = document.getElementById("detachSelectedAgentFromProjectButton");
    const projectActorsJsonFileEl = document.getElementById("projectActorsJsonFile");
    const importProjectActorsButtonEl = document.getElementById("importProjectActorsButton");
    const deleteAllProjectActorsButtonEl = document.getElementById("deleteAllProjectActorsButton");
    const agentProjectStatusEl = document.getElementById("agentProjectStatus");
    const projectSprintsEl = document.getElementById("projectSprints");
    const projectSprintsStatusEl = document.getElementById("projectSprintsStatus");
    const refreshProjectSprintsButtonEl = document.getElementById("refreshProjectSprintsButton");
    const projectAvailableAgentsEl = document.getElementById("projectAvailableAgents");
    const projectAttachedAgentsEl = document.getElementById("projectAttachedAgents");
    const agentsEl = document.getElementById("agents");
    const agentsStatusEl = document.getElementById("agentsStatus");
    const specializedAgentsCountEl = document.getElementById("specializedAgentsCount");
    const liveEmptyAgentsCountEl = document.getElementById("liveEmptyAgentsCount");
    const expiredEmptyAgentsCountEl = document.getElementById("expiredEmptyAgentsCount");
    const specializedAgentsListEl = document.getElementById("specializedAgentsList");
    const liveEmptyAgentsListEl = document.getElementById("liveEmptyAgentsList");
    const expiredEmptyAgentsListEl = document.getElementById("expiredEmptyAgentsList");
    const cloneAgentModalEl = document.getElementById("cloneAgentModal");
    const cloneSourceAgentEl = document.getElementById("cloneSourceAgent");
    const cloneGeminiModelEl = document.getElementById("cloneGeminiModel");
    const cloneGeminiModelsEl = document.getElementById("cloneGeminiModels");
    const cloneGeminiApiKeyEl = document.getElementById("cloneGeminiApiKey");
    const cloneRefinementsEl = document.getElementById("cloneRefinements");
    const cloneAgentStatusEl = document.getElementById("cloneAgentStatus");
    const cloneAgentPreviewEl = document.getElementById("cloneAgentPreview");
    const cloneCandidateNameEl = document.getElementById("cloneCandidateName");
    const cloneCandidatePhoneEl = document.getElementById("cloneCandidatePhone");
    const cloneCandidateProfileEl = document.getElementById("cloneCandidateProfile");
    const cloneRawResponseEl = document.getElementById("cloneRawResponse");
    const acceptCloneAgentButtonEl = document.getElementById("acceptCloneAgentButton");
    const copyProfilePhoneModalEl = document.getElementById("copyProfilePhoneModal");
    const copyProfilePhoneSelectEl = document.getElementById("copyProfilePhoneSelect");
    const copyProfilePhoneManualEl = document.getElementById("copyProfilePhoneManual");
    const copyProfilePhoneStatusEl = document.getElementById("copyProfilePhoneStatus");
    const confirmCopyProfilePhoneButtonEl = document.getElementById("confirmCopyProfilePhoneButton");
    const commitValueEl = document.getElementById("commitValue");
    const contextSummaryEl = document.getElementById("contextSummary");
    const createEmptyAgentButtonEl = document.getElementById("createEmptyAgentButton");
    const emptyAgentPromptPanelEl = document.getElementById("emptyAgentPromptPanel");
    const emptyAgentPromptMetaEl = document.getElementById("emptyAgentPromptMeta");
    const emptyAgentPromptTextEl = document.getElementById("emptyAgentPromptText");
    const copyEmptyAgentPromptButtonEl = document.getElementById("copyEmptyAgentPromptButton");
    const specializePhoneEl = document.getElementById("specializePhone");
    const specializeTemplateEl = document.getElementById("specializeTemplate");
    const specializeNameEl = document.getElementById("specializeName");
    const specializeProfileEl = document.getElementById("specializeProfile");
    const sendSpecializationButtonEl = document.getElementById("sendSpecializationButton");
    const specializeAgentStateEl = document.getElementById("specializeAgentState");
    const specializeStatusEl = document.getElementById("specializeStatus");
    const helpModalEl = document.getElementById("helpModal");
    const helpModalTitleEl = document.getElementById("helpModalTitle");
    const helpModalBodyEl = document.getElementById("helpModalBody");
    const closeHelpModalButtonEl = document.getElementById("closeHelpModalButton");
    let currentHistoryRecords = [];
    let gitContexts = [];
    let phoneGitContexts = [];
    let currentGitContext = null;
    let activeQueueItemIds = new Set();
    let emailRoutes = [];
    let attachmentFolderChoices = [];
    let screenshotFolders = [];
    let selectedScreenshotFolderId = "";
    let expandedScreenshotFolderIds = new Set();
    let screenshotTrash = [];
    let dragCandidateScreenshot = null;
    let draggedScreenshot = null;
    let evidenceFolders = [];
    let selectedEvidenceFolderId = "";
    let expandedEvidenceFolderIds = new Set();
    let evidenceTrash = [];
    let dragCandidateEvidence = null;
    let draggedEvidence = null;
    let agents = [];
    let allAgents = [];
    let projectSprints = [];
    let projectSprintsProjectPhone = "";
    let pendingSpecializations = {};
    let selectedAgentId = "";
    let pendingGitContextKey = "";
    let activeUnmappedGitContextKey = "";
    let agentEditorDirty = false;
    let cloneCandidateAgent = null;
    let messageCursor = {start: 0, end: 0};
    let helpRenderScheduled = false;
    let cycleGraphCycles = [];
    let cycleGraphSelectedCycleId = "";
    let cycleGraphProjectId = "";
    let cycleGraphRequestVersion = 0;
    let cycleGraphAbortController = null;
    let cycleGraphRefreshInFlight = false;

    function setStatus(text, state = "") {
      sendStatusEl.textContent = text;
      sendStatusEl.className = "status" + (state ? " " + state : "");
    }

    function setAttachmentStatus(text, state = "") {
      attachmentStatusEl.textContent = text;
      attachmentStatusEl.className = "status" + (state ? " " + state : "");
    }

    function setScheduledTasksStatus(text, state = "") {
      scheduledTasksStatusEl.textContent = text;
      scheduledTasksStatusEl.className = "status" + (state ? " " + state : "");
    }

    function setScreenshotFoldersStatus(text, state = "") {
      screenshotFoldersStatusEl.textContent = text;
      screenshotFoldersStatusEl.className = "status" + (state ? " " + state : "");
    }

    function setScreenshotUploadStatus(text, state = "") {
      screenshotUploadStatusEl.textContent = text;
      screenshotUploadStatusEl.className = "status" + (state ? " " + state : "");
    }

    function setScreenshotTrashStatus(text, state = "") {
      screenshotTrashStatusEl.textContent = text;
      screenshotTrashStatusEl.className = "status" + (state ? " " + state : "");
    }

    function setScreenshotFoldersFilterStatus(text, state = "") {
      screenshotFoldersFilterStatusEl.textContent = text;
      screenshotFoldersFilterStatusEl.className = "status" + (state ? " " + state : "");
    }

    function setEvidenceFoldersStatus(text, state = "") {
      evidenceFoldersStatusEl.textContent = text;
      evidenceFoldersStatusEl.className = "status" + (state ? " " + state : "");
    }

    function setEvidenceUploadStatus(text, state = "") {
      evidenceUploadStatusEl.textContent = text;
      evidenceUploadStatusEl.className = "status" + (state ? " " + state : "");
    }

    function setEvidenceTrashStatus(text, state = "") {
      evidenceTrashStatusEl.textContent = text;
      evidenceTrashStatusEl.className = "status" + (state ? " " + state : "");
    }

    function setEvidenceFoldersFilterStatus(text, state = "") {
      evidenceFoldersFilterStatusEl.textContent = text;
      evidenceFoldersFilterStatusEl.className = "status" + (state ? " " + state : "");
    }

    function setPhoneStatus(text, state = "") {
      phoneStatusEl.textContent = text;
      phoneStatusEl.className = "status" + (state ? " " + state : "");
    }

    function setConsultantStatus(text, state = "") {
      consultantStatusEl.textContent = text;
      consultantStatusEl.className = "status" + (state ? " " + state : "");
    }

    function setGitStatus(text, state = "") {
      gitStatusEl.textContent = text;
      gitStatusEl.className = "status" + (state ? " " + state : "");
    }

    function setHistoryStatus(text, state = "") {
      historyStatusEl.textContent = text;
      historyStatusEl.className = "status" + (state ? " " + state : "");
    }

    function setEmailRoutesStatus(text, state = "") {
      emailRoutesStatusEl.textContent = text;
      emailRoutesStatusEl.className = "status" + (state ? " " + state : "");
    }

    function setAgentsStatus(text, state = "") {
      agentsStatusEl.textContent = text;
      agentsStatusEl.className = "status" + (state ? " " + state : "");
    }

    function markAgentEditorDirty() {
      agentEditorDirty = true;
    }

    function clearAgentEditorDirty() {
      agentEditorDirty = false;
    }

    function shouldHoldAgentRefresh() {
      return agentEditorDirty;
    }

    function setCloneAgentStatus(text, state = "") {
      cloneAgentStatusEl.textContent = text;
      cloneAgentStatusEl.className = "status" + (state ? " " + state : "");
    }

    function todayInputValue() {
      const now = new Date();
      const local = new Date(now.getTime() - now.getTimezoneOffset() * 60000);
      return local.toISOString().slice(0, 10);
    }

    function escapeHtml(value) {
      return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#039;");
    }

    function textWithoutHelp(element) {
      const clone = element.cloneNode(true);
      clone.querySelectorAll(".help-button").forEach((button) => button.remove());
      return clone.textContent.trim().replace(/\\s+/g, " ");
    }

    function normalizedHelpText(text) {
      return String(text || "").trim().replace(/\\s+/g, " ");
    }

    function helpSectionHtml(title, content) {
      if (!content) {
        return "";
      }
      if (Array.isArray(content)) {
        return `<section><span class="help-modal-section-title">${escapeHtml(title)}</span><ul>${content.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}</ul></section>`;
      }
      return `<section><span class="help-modal-section-title">${escapeHtml(title)}</span><p>${escapeHtml(content)}</p></section>`;
    }

    function genericHelpEntry(topic, element) {
      const label = element ? textWithoutHelp(element) : "Этот пункт";
      if (topic.startsWith("field:") || topic.startsWith("label:")) {
        return {
          title: label || "Поле",
          purpose: "Это поле помогает заполнить форму.",
          logic: "Введите или выберите значение, затем используйте основную кнопку раздела. Само поле ничего не отправляет, пока вы не нажмете действие."
        };
      }
      if (topic.startsWith("button:") || topic.startsWith("action:")) {
        return {
          title: label || "Действие",
          purpose: "Эта кнопка запускает действие на экране.",
          logic: "Перед нажатием проверьте выбранные поля рядом. Если действие меняет данные, система обычно показывает статус или просит подтверждение."
        };
      }
      return {
        title: label || "Раздел",
        purpose: "Этот пункт помогает ориентироваться в рабочем процессе.",
        logic: "Откройте раздел, заполните нужные поля и используйте кнопки действий. Результат будет показан в статусе, списке или истории."
      };
    }

    function helpEntry(topic, element = null) {
      return helpCatalog[topic] || genericHelpEntry(topic, element);
    }

    function openHelp(topic, element = null) {
      const entry = helpEntry(topic, element);
      helpModalTitleEl.textContent = entry.title || "Подсказка";
      helpModalBodyEl.innerHTML = [
        helpSectionHtml("Что это значит", entry.purpose),
        helpSectionHtml("Как работает", entry.logic),
        helpSectionHtml("Когда использовать", entry.tips)
      ].join("");
      helpModalEl.hidden = false;
      closeHelpModalButtonEl.focus();
    }

    function closeHelpModal() {
      helpModalEl.hidden = true;
      helpModalTitleEl.textContent = "Подсказка";
      helpModalBodyEl.innerHTML = "";
    }

    function bindHelpButton(button) {
      if (!button || button.dataset.helpBound === "true") {
        return;
      }
      button.dataset.helpBound = "true";
      button.addEventListener("click", (event) => {
        event.preventDefault();
        event.stopPropagation();
        const topic = button.dataset.helpTopic || "generic";
        const targetId = button.dataset.helpTargetId || "";
        const target = targetId ? document.getElementById(targetId) : null;
        openHelp(topic, target || button.previousElementSibling || button.parentElement);
      });
    }

    function makeHelpButton(topic, label, targetElement = null) {
      const button = document.createElement("button");
      button.className = "help-button";
      button.type = "button";
      button.textContent = "?";
      button.title = "Что это?";
      button.setAttribute("aria-label", `Подсказка: ${label || "что это"}`);
      button.dataset.helpTopic = topic;
      if (targetElement && targetElement.id) {
        button.dataset.helpTargetId = targetElement.id;
      }
      bindHelpButton(button);
      return button;
    }

    function addInlineHelp(element, topic, label) {
      if (!element || element.dataset.helpReady === "true" || element.closest("#helpModal")) {
        return;
      }
      element.dataset.helpReady = "true";
      element.appendChild(makeHelpButton(topic, label, element));
    }

    function addSiblingHelp(element, topic, label) {
      if (!element || element.dataset.helpReady === "true" || element.closest("#helpModal")) {
        return;
      }
      element.dataset.helpReady = "true";
      element.insertAdjacentElement("afterend", makeHelpButton(topic, label, element));
    }

    function headingHelpTopic(heading) {
      return `heading:${normalizedHelpText(textWithoutHelp(heading))}`;
    }

    function labelHelpTopic(label) {
      const fieldId = label.getAttribute("for");
      if (fieldId) {
        return `field:${fieldId}`;
      }
      return `label:${normalizedHelpText(textWithoutHelp(label))}`;
    }

    function buttonHelpTopic(button) {
      if (button.id) {
        return `button:${button.id}`;
      }
      if (button.dataset && button.dataset.action) {
        return `action:${button.dataset.action}`;
      }
      return `button-text:${normalizedHelpText(textWithoutHelp(button))}`;
    }

    function shouldAddActionHelp(button) {
      if (!button || button.classList.contains("help-button") || button.closest("#helpModal")) {
        return false;
      }
      if (button.classList.contains("page-tab") || button.classList.contains("tab")) {
        return false;
      }
      return Boolean(
        button.id === "refreshButton"
        || button.closest(".actions")
        || button.closest(".agent-toolbar-actions")
        || button.closest(".history-filter-actions")
        || button.closest(".modal-actions")
      );
    }

    function applyHelpButtons() {
      document.querySelectorAll(".help-button[data-help-topic]").forEach(bindHelpButton);

      document.querySelectorAll("h2, h3").forEach((heading) => {
        const title = normalizedHelpText(textWithoutHelp(heading));
        if (!title) {
          return;
        }
        addInlineHelp(heading, headingHelpTopic(heading), title);
      });

      document.querySelectorAll("label").forEach((label) => {
        const title = normalizedHelpText(textWithoutHelp(label));
        if (!title) {
          return;
        }
        addInlineHelp(label, labelHelpTopic(label), title);
      });

      document.querySelectorAll("button").forEach((button) => {
        if (!shouldAddActionHelp(button)) {
          return;
        }
        addSiblingHelp(button, buttonHelpTopic(button), textWithoutHelp(button));
      });
    }

    function scheduleApplyHelpButtons() {
      if (helpRenderScheduled) {
        return;
      }
      helpRenderScheduled = true;
      window.requestAnimationFrame(() => {
        helpRenderScheduled = false;
        applyHelpButtons();
      });
    }

    const helpObserver = new MutationObserver(scheduleApplyHelpButtons);

    function initializeHelpSystem() {
      applyHelpButtons();
      helpObserver.observe(document.body, {
        childList: true,
        subtree: true
      });
      closeHelpModalButtonEl.addEventListener("click", closeHelpModal);
      helpModalEl.addEventListener("click", (event) => {
        if (event.target === helpModalEl) {
          closeHelpModal();
        }
      });
    }

    function formatMessage(message) {
      if (message === null || message === undefined) {
        return "";
      }
      if (typeof message === "string") {
        return message;
      }
      const serialized = JSON.stringify(message, null, 2);
      return serialized === undefined ? String(message) : serialized;
    }

    function lifecycleEventStatus(eventType, payload) {
      const artifact = payload && typeof payload.artifact === "object" ? payload.artifact : {};
      const report = payload && typeof payload.report === "object" ? payload.report : {};
      const decision = payload && typeof payload.decision === "object" ? payload.decision : {};
      let value = "";
      if (eventType === "ARTIFACT_CREATED") {
        value = artifact.status
          || artifact.release_status
          || artifact.clinical_status
          || artifact.verification_status
          || "RECORDED";
      } else if (eventType === "GROUP_REPORT_SUBMITTED") {
        value = report.status || "";
        if (!value && typeof report.RESULT === "string") {
          const resultStatus = report.RESULT.match(/^\\s*(PASS|FAIL|ACCEPT(?:ED)?|BLOCKED|DONE)\\b/i);
          value = resultStatus ? resultStatus[1] : "SUBMITTED";
        }
        value = value || "SUBMITTED";
      } else if (eventType === "CYCLE_COMPLETED") {
        value = decision.status || "COMPLETED";
      } else {
        value = eventType || "RECORDED";
      }
      return String(value).trim().replace(/\\s+/g, "_").toUpperCase();
    }

    function lifecycleEventSummary(eventType, payload) {
      const artifact = payload && typeof payload.artifact === "object" ? payload.artifact : {};
      const report = payload && typeof payload.report === "object" ? payload.report : {};
      const decision = payload && typeof payload.decision === "object" ? payload.decision : {};
      if (eventType === "ARTIFACT_CREATED") {
        return artifact.summary || artifact.path || artifact.ref || artifact.type || "Артефакт зафиксирован";
      }
      if (eventType === "GROUP_REPORT_SUBMITTED") {
        return report.RESULT || report.summary || "Отчёт группы зафиксирован";
      }
      if (eventType === "CYCLE_COMPLETED") {
        return decision.summary || "Цикл разработки завершён";
      }
      return "Событие цикла разработки зафиксировано";
    }

    function formatHistoryRecordMessage(record) {
      const directMessage = formatMessage(record && record.message);
      if (directMessage.trim()) {
        return directMessage;
      }
      const meta = record && record.metadata && typeof record.metadata === "object" ? record.metadata : {};
      if (!record || record.event !== "cycle_lifecycle_event") {
        return directMessage;
      }
      const eventType = String(meta.cycle_event_type || "CYCLE_EVENT").trim().toUpperCase();
      const payload = meta.cycle_event_payload && typeof meta.cycle_event_payload === "object"
        ? meta.cycle_event_payload
        : {};
      return [
        `STATUS: ${lifecycleEventStatus(eventType, payload)}`,
        `SUMMARY: ${lifecycleEventSummary(eventType, payload)}`,
        `EVENT: ${eventType}`,
        "",
        "PAYLOAD:",
        formatMessage(payload) || "{}"
      ].join("\\n");
    }

    function historyRecordRevision(record) {
      const meta = record && record.metadata && typeof record.metadata === "object" ? record.metadata : {};
      const commit = String(meta.git_commit_short || meta.git_commit || "").trim();
      if (commit) {
        return {label: "Commit", value: commit};
      }
      const payload = meta.cycle_event_payload && typeof meta.cycle_event_payload === "object"
        ? meta.cycle_event_payload
        : {};
      const artifact = payload.artifact && typeof payload.artifact === "object" ? payload.artifact : {};
      const artifactRef = String(artifact.ref || "").trim();
      if (artifactRef) {
        return {label: "Ref", value: artifactRef};
      }
      const artifactLocatorCandidates = [
        artifact.artifact_id,
        artifact.path,
        ...(Array.isArray(artifact.paths) ? artifact.paths : [])
      ];
      const artifactLocator = artifactLocatorCandidates.find((value) => {
        return (typeof value === "string" || typeof value === "number") && String(value).trim();
      });
      if (artifactLocator !== undefined) {
        return {label: "Artifact", value: String(artifactLocator).trim()};
      }
      const cycleId = String(meta.cycle_id || "").trim();
      if (record && record.event === "cycle_lifecycle_event" && cycleId) {
        return {label: "Cycle", value: cycleId};
      }
      return {label: "Commit", value: "no-commit"};
    }

    function queuesForContext(context) {
      return Object.entries(queueConfig)
        .filter(([, config]) => config.context === context)
        .map(([name]) => name);
    }

    function actorNamesWithSelected(selected = "") {
      const names = actors.slice();
      if (selected && !names.includes(selected)) {
        names.unshift(selected);
      }
      return names;
    }

    function renderActorOptions(selectEl, selected) {
      const selectedValue = selected || selectEl.value || "";
      selectEl.innerHTML = actorNamesWithSelected(selectedValue).map((actor) => {
        const isSelected = actor === selectedValue ? " selected" : "";
        return `<option${isSelected}>${escapeHtml(actor)}</option>`;
      }).join("");
      if (selectedValue) {
        selectEl.value = selectedValue;
      }
    }

    function updateActorControls() {
      const config = queueConfig[queueEl.value] || {};
      renderActorOptions(senderEl, senderEl.value || config.default_sender || "");
      renderActorOptions(receiverEl, receiverEl.value || config.default_receiver || "");
    }

    function agentById(agentIdValue) {
      return agents.find((agent) => agent.id === agentIdValue) || null;
    }

    function activeQueuePhone() {
      return String(gitPhoneEl.value || "").trim();
    }

    function activeMappedQueuePhone() {
      if (activeUnmappedGitContextKey) {
        return "";
      }
      const phone = activeQueuePhone();
      return phone && phoneContextByPhone(phone) ? phone : "";
    }

    function activeGitContextRequiredMessage() {
      const unmappedContext = projectContextByKey(activeUnmappedGitContextKey);
      if (unmappedContext) {
        return `Проект ${unmappedContext.project_name || unmappedContext.git_context_key} пока не имеет номера. Повторите запрос к Project Manager или сохраните привязку.`;
      }
      const phone = activeQueuePhone();
      return phone
        ? `Номер ${phone} не привязан к Git context. Сохраните привязку во вкладке Git context.`
        : "Выберите активный Git context во вкладке Git context.";
    }

    function phoneContextByPhone(phone) {
      const phoneValue = String(phone || "").trim();
      return phoneGitContexts.find((context) => String(context.phone || "").trim() === phoneValue) || null;
    }

    function projectContextByKey(gitContextKey) {
      const key = String(gitContextKey || "").trim();
      return gitContexts.find((context) => String(context.git_context_key || "").trim() === key) || null;
    }

    function activeProjectContext() {
      return phoneContextByPhone(activeQueuePhone())
        || projectContextByKey(activeUnmappedGitContextKey);
    }

    function activeGitContextKey() {
      const context = activeProjectContext();
      return context ? String(context.git_context_key || "").trim() : "";
    }

    function contextKeyList(rawValue) {
      if (Array.isArray(rawValue)) {
        return uniqueList(rawValue.map((value) => String(value || "").trim()).filter(Boolean));
      }
      return uniqueList(String(rawValue || "")
        .split(/[,\\n;|]+/)
        .map((value) => value.trim())
        .filter(Boolean));
    }

    function agentPhoneGitContextKey(agent) {
      const phone = String((agent && agent.phone) || "").trim();
      const context = phoneContextByPhone(phone);
      if (context && context.git_context_key) {
        return String(context.git_context_key).trim();
      }
      return "";
    }

    function agentIncludedGitContextKeys(agent) {
      const parameters = agent && agent.parameters ? agent.parameters : {};
      return new Set([
        agentPhoneGitContextKey(agent),
        String(parameters.git_context_key || "").trim(),
        ...contextKeyList(parameters.git_context_keys)
      ].filter(Boolean));
    }

    function agentExcludedGitContextKeys(agent) {
      const parameters = agent && agent.parameters ? agent.parameters : {};
      return new Set(contextKeyList(parameters.git_context_excluded_keys));
    }

    function agentMatchesActiveGitContext(agent) {
      const activeKey = activeGitContextKey();
      if (!activeKey) {
        return false;
      }
      if (agentExcludedGitContextKeys(agent).has(activeKey)) {
        return false;
      }
      return agentIncludedGitContextKeys(agent).has(activeKey);
    }

    function filteredAgentsForActiveContext(agentList = allAgents) {
      return (agentList || []).filter(agentMatchesActiveGitContext);
    }

    function refreshContextScopedViews() {
      refreshQueues().catch((error) => setStatus(error.message, "error"));
      refreshScheduledTasks().catch((error) => setScheduledTasksStatus(error.message, "error"));
      refreshHistory().catch((error) => setHistoryStatus(error.message, "error"));
      projectSprintsProjectPhone = "";
      renderProjectSprints();
      refreshProjectSprints().catch((error) => {
        projectSprintsStatusEl.textContent = error.message;
      });
      refreshAttachmentFolderChoices().catch((error) => setAttachmentStatus(error.message, "error"));
      refreshScreenshotFolders().catch((error) => setScreenshotFoldersStatus(error.message, "error"));
      refreshEvidenceFolders().catch((error) => setEvidenceFoldersStatus(error.message, "error"));
      renderAgentsForActiveContext(selectedAgentId);
      if (cycleGraphViewIsActive()) {
        refreshCycleGraph({force: true}).catch((error) => setCycleGraphStatus(error.message, "error"));
      }
    }

    function updateActiveGitContextDisplay() {
      const phone = activeQueuePhone();
      const mappedPhone = activeMappedQueuePhone();
      const context = activeProjectContext();
      if (context) {
        const project = context.project_name || "Project";
        const commit = context.git_commit_short || context.git_commit || "commit не получен";
        const key = context.git_context_key || context.git_address || "";
        if (mappedPhone) {
          activeGitContextSummaryEl.textContent = `Активный Git context: ${project} · phone ${mappedPhone} · ${key}`;
          activeGitContextPhoneEl.textContent = mappedPhone;
        } else {
          activeGitContextSummaryEl.textContent = `Активный чистый проект: ${project} · без phone · ${key}`;
          activeGitContextPhoneEl.textContent = "не привязан";
        }
        activeGitContextProjectEl.textContent = project;
        activeGitContextCommitEl.textContent = commit;
        return;
      }
      if (!phone) {
        activeGitContextSummaryEl.textContent = "Git context не выбран";
        activeGitContextPhoneEl.textContent = "не выбран";
        activeGitContextProjectEl.textContent = "не задан";
        activeGitContextCommitEl.textContent = "не задан";
        return;
      }
      if (!context) {
        activeGitContextSummaryEl.textContent = `Активный номер: ${phone}. Git context еще не сохранен.`;
        activeGitContextPhoneEl.textContent = phone;
        activeGitContextProjectEl.textContent = "не сохранен";
        activeGitContextCommitEl.textContent = "не задан";
        return;
      }
    }

    function renderGitContextProjectList() {
      const activeKey = activeGitContextKey();
      const mappedCount = gitContexts.filter((context) => Array.isArray(context.phones) && context.phones.length).length;
      gitContextListCountEl.textContent = gitContexts.length
        ? `${gitContexts.length} проектов · ${mappedCount} с phone`
        : "нет проектов";
      if (!gitContexts.length) {
        gitContextListEl.innerHTML = `<option value="">Нет зарегистрированных проектов</option>`;
        gitContextListEl.disabled = true;
        applyGitContextProjectButtonEl.disabled = true;
        return;
      }

      gitContextListEl.disabled = false;
      applyGitContextProjectButtonEl.disabled = false;
      const pendingKey = projectContextByKey(pendingGitContextKey)
        ? pendingGitContextKey
        : "";
      const selectedKey = pendingKey
        || (projectContextByKey(activeKey) ? activeKey : "")
        || String((gitContexts[0] && gitContexts[0].git_context_key) || "").trim();
      gitContextListEl.innerHTML = gitContexts.map((context) => {
        const key = String(context.git_context_key || "").trim();
        const selected = key === selectedKey ? " selected" : "";
        const project = context.project_name || "Project";
        const phones = Array.isArray(context.phones)
          ? context.phones.map((phone) => String(phone || "").trim()).filter(Boolean)
          : [];
        const phoneText = phones.length ? `phone ${phones.join(", ")}` : "без phone";
        const portText = Array.isArray(context.ports) && context.ports.length
          ? ` · port ${context.ports.join(", ")}`
          : "";
        const label = `${project} · ${phoneText}${portText} · ${key}`;
        return `<option value="${escapeHtml(key)}"${selected}>${escapeHtml(label)}</option>`;
      }).join("");
    }

    function applySelectedGitPhone(phone, refreshViews = false) {
      activeUnmappedGitContextKey = "";
      const phoneValue = String(phone || "").trim();
      if (phoneValue) {
        const hasOption = Array.from(gitPhoneEl.options).some((option) => option.value === phoneValue);
        if (!hasOption) {
          renderGitPhoneOptions(phoneValue);
        }
        gitPhoneEl.value = phoneValue;
      }
      pendingGitContextKey = "";
      const context = phoneContextByPhone(activeQueuePhone());
      updateActiveGitContextDisplay();
      if (context) {
        gitAddressEl.value = context.git_address || "";
        gitProjectNameEl.value = context.project_name || "";
        gitContextKeyEl.value = context.git_context_key || "";
        currentGitContext = context;
        renderGitContextOptions(context.git_context_key || "");
      } else {
        gitAddressEl.value = "";
        gitProjectNameEl.value = "";
        gitContextKeyEl.value = "";
        currentGitContext = null;
        renderGitContextOptions("");
      }
      renderGitContextProjectList();
      if (refreshViews) {
        refreshContextScopedViews();
      }
    }

    function applySelectedGitContext(gitContextKey, refreshViews = false) {
      const context = projectContextByKey(gitContextKey);
      if (!context) {
        setGitStatus("Выбранный проект больше не зарегистрирован.", "error");
        return;
      }
      const phones = Array.isArray(context.phones)
        ? context.phones.map((phone) => String(phone || "").trim()).filter(Boolean)
        : [];
      const mappedPhone = phones.find((phone) => phoneContextByPhone(phone))
        || String((phoneGitContexts.find((item) => item.git_context_key === context.git_context_key) || {}).phone || "").trim();
      pendingGitContextKey = "";
      if (mappedPhone) {
        applySelectedGitPhone(mappedPhone, refreshViews);
        return;
      }

      activeUnmappedGitContextKey = String(context.git_context_key || "").trim();
      renderGitPhoneOptions("");
      gitPhoneEl.value = "";
      gitAddressEl.value = context.git_address || "";
      gitProjectNameEl.value = context.project_name || "";
      gitContextKeyEl.value = context.git_context_key || "";
      currentGitContext = context;
      renderGitContextOptions(context.git_context_key || "");
      updateActiveGitContextDisplay();
      renderGitContextProjectList();
      setGitStatus(`Проект ${context.project_name || context.git_context_key} зарегистрирован без phone. Укажите новый номер и сохраните привязку.`, "ok");
      if (refreshViews) {
        refreshContextScopedViews();
      }
    }

    function renderGitPhoneOptions(preferredPhone = activeQueuePhone()) {
      const seen = new Set();
      const options = [];
      const preferred = String(preferredPhone || "").trim();
      phoneGitContexts.forEach((context) => {
        const phone = String(context.phone || "").trim();
        if (!phone || seen.has(phone)) {
          return;
        }
        seen.add(phone);
        options.push({
          phone,
          label: `${context.project_name || "Project"} · ${phone}`
        });
      });
      agents.forEach((agent) => {
        const phone = String(agent.phone || "").trim();
        if (!phone || seen.has(phone)) {
          return;
        }
        seen.add(phone);
        options.push({
          phone,
          label: `${agent.name || "Agent"} · ${phone}`
        });
      });
      if (preferred && !seen.has(preferred)) {
        seen.add(preferred);
        options.unshift({
          phone: preferred,
          label: `Новый Git context · ${preferred}`
        });
      }
      const mappedDefaultPhone = (phoneGitContexts[0] && phoneGitContexts[0].phone) || "";
      const preserveUnmappedProject = Boolean(activeUnmappedGitContextKey && !preferred);
      const selectedPhone = preserveUnmappedProject
        ? ""
        : options.some((option) => option.phone === preferred)
          ? preferred
          : mappedDefaultPhone && options.some((option) => option.phone === mappedDefaultPhone)
            ? mappedDefaultPhone
            : (options[0] && options[0].phone) || "";
      const unmappedOption = preserveUnmappedProject
        ? `<option value="" selected>Зарегистрированный проект без phone</option>`
        : "";
      gitPhoneEl.innerHTML = options.length || preserveUnmappedProject
        ? unmappedOption + options.map((option) => {
          const selected = option.phone === selectedPhone ? " selected" : "";
          const mapped = phoneContextByPhone(option.phone);
          const suffix = mapped ? ` · ${mapped.project_name || mapped.git_context_key}` : " · не привязан";
          return `<option value="${escapeHtml(option.phone)}"${selected}>${escapeHtml(option.label + suffix)}</option>`;
        }).join("")
        : `<option value="">Нет телефонов агентов</option>`;
      gitPhoneEl.value = selectedPhone;
      updateActiveGitContextDisplay();
      renderGitContextProjectList();
    }

    function renderPhoneAgentOptions(selectEl, preferredName = "") {
      const previousValue = selectEl.value;
      const preferredAgent = agents.find((agent) => agent.name === preferredName);
      const selectedId = preferredAgent ? preferredAgent.id : previousValue;
      selectEl.innerHTML = agents.length
        ? agents.map((agent) => {
          const phone = agent.phone || "no-phone";
          const selected = agent.id === selectedId ? " selected" : "";
          return `<option value="${escapeHtml(agent.id)}"${selected}>${escapeHtml(agent.name)} · ${escapeHtml(phone)}</option>`;
        }).join("")
        : `<option value="">Нет агентов</option>`;
    }

    function updatePhoneAgentControls() {
      let senderName = "Programmer";
      let receiverName = "Tester";
      if (phoneChannelEl.value === "tester-all") {
        senderName = "Tester";
        receiverName = "Programmer";
      }
      if (phoneChannelEl.value === "consultant-all") {
        senderName = "Owner";
        receiverName = "Backend Technical Advisor";
      }
      renderPhoneAgentOptions(phoneSenderEl, senderName);
      renderPhoneAgentOptions(phoneReceiverEl, receiverName);
    }

    function isConsultantAgent(agent) {
      const name = String(agent.name || "");
      return name.includes("Technical Advisor") || name.includes("Architecture Expert");
    }

    function renderConsultantExpertOptions() {
      const consultants = agents.filter(isConsultantAgent);
      const previousValue = consultantExpertEl.value;
      consultantExpertEl.innerHTML = consultants.length
        ? consultants.map((agent) => {
          const phone = agent.phone || "no-phone";
          const selected = agent.id === previousValue ? " selected" : "";
          return `<option value="${escapeHtml(agent.id)}"${selected}>${escapeHtml(agent.name)} · ${escapeHtml(phone)}</option>`;
        }).join("")
        : `<option value="">Нет консультантов</option>`;
    }

    function renderConsultantSenderOptions() {
      const previousValue = consultantSenderEl.value;
      consultantSenderEl.innerHTML = agents.length
        ? agents.filter((agent) => !isConsultantAgent(agent)).map((agent) => {
          const phone = agent.phone || "no-phone";
          const selected = agent.id === previousValue ? " selected" : "";
          return `<option value="${escapeHtml(agent.id)}"${selected}>${escapeHtml(agent.name)} · ${escapeHtml(phone)}</option>`;
        }).join("")
        : `<option value="">Нет агентов</option>`;
      if (!consultantSenderEl.value) {
        const owner = agents.find((agent) => agent.name === "Owner");
        if (owner) {
          consultantSenderEl.value = owner.id;
        }
      }
    }

    function updateConsultantControls() {
      renderConsultantExpertOptions();
      renderConsultantSenderOptions();
      const sender = agentById(consultantSenderEl.value);
      consultantReplyPhoneEl.value = sender && sender.phone ? sender.phone : "";
    }

    function emailRouteId() {
      if (window.crypto && crypto.randomUUID) {
        return crypto.randomUUID();
      }
      return `email-route-${Date.now()}-${Math.random().toString(16).slice(2)}`;
    }

    function senderOptions(selected) {
      return actorNamesWithSelected(selected).map((actor) => {
        const isSelected = actor === selected ? " selected" : "";
        return `<option${isSelected}>${escapeHtml(actor)}</option>`;
      }).join("");
    }

    function renderEmailSenderOptions() {
      emailSenderOptionsEl.innerHTML = actors
        .map((actor) => `<option value="${escapeHtml(actor)}"></option>`)
        .join("");
    }

    function emailRouteRow(route) {
      const selectedSender = route.sender || "Analyst";
      return `<div class="email-route-row" data-route-id="${escapeHtml(route.id || emailRouteId())}">
        <input data-field="email" type="email" placeholder="user@example.org" value="${escapeHtml(route.email || "")}">
        <input data-field="sender" list="emailSenderOptions" placeholder="От кого" value="${escapeHtml(selectedSender)}">
        <button class="secondary" data-action="remove-email-route" type="button">Удалить</button>
      </div>`;
    }

    function renderEmailRoutes(routes) {
      emailRoutes = routes || [];
      emailRoutesEl.innerHTML = emailRoutes.length
        ? emailRoutes.map(emailRouteRow).join("")
        : `<div class="subtle">Список email пока пуст.</div>`;
    }

    function collectEmailRoutes() {
      return Array.from(emailRoutesEl.querySelectorAll(".email-route-row"))
        .map((row) => ({
          id: row.dataset.routeId || emailRouteId(),
          email: row.querySelector('[data-field="email"]').value.trim(),
          sender: row.querySelector('[data-field="sender"]').value.trim()
        }))
        .filter((route) => route.email && route.sender);
    }

    function addEmailRoute(route = null) {
      if (!emailRoutesEl.querySelector(".email-route-row")) {
        emailRoutesEl.innerHTML = "";
      }
      emailRoutesEl.insertAdjacentHTML("beforeend", emailRouteRow(route || {
        id: emailRouteId(),
        email: "",
        sender: senderEl.value || "Analyst"
      }));
      setEmailRoutesStatus("");
    }

    async function refreshEmailRoutes() {
      const response = await fetch("/email-routes");
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.detail || "Ошибка загрузки email routes.");
      }
      renderEmailRoutes(data.routes || []);
      setEmailRoutesStatus(`Загружено: ${(data.routes || []).length}. Файл: ${data.config_path}`);
    }

    async function saveEmailRoutes() {
      const routes = collectEmailRoutes();
      setEmailRoutesStatus("Сохраняю...");
      const response = await fetch("/email-routes", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({routes})
      });
      const data = await response.json();
      if (!response.ok) {
        setEmailRoutesStatus(data.detail || "Ошибка сохранения email routes.", "error");
        return;
      }
      renderEmailRoutes(data.routes || []);
      setEmailRoutesStatus(`Сохранено строк: ${(data.routes || []).length}.`, "ok");
    }

    function agentId() {
      if (window.crypto && crypto.randomUUID) {
        return crypto.randomUUID();
      }
      return `agent-${Date.now()}-${Math.random().toString(16).slice(2)}`;
    }

    function uniqueAgentNames(agentList) {
      const names = [];
      const seen = new Set();
      (agentList || []).forEach((agent) => {
        const name = String(agent.name || "").trim();
        const key = name.toLowerCase();
        if (name && !seen.has(key)) {
          seen.add(key);
          names.push(name);
        }
      });
      return names;
    }

    function nextAgentName() {
      const names = new Set(agents.map((agent) => String(agent.name || "").trim().toLowerCase()));
      let index = agents.length + 1;
      while (names.has(`agent ${index}`)) {
        index += 1;
      }
      return `Agent ${index}`;
    }

    function agentStatus(agent) {
      return String((agent && (agent.status || (agent.parameters && agent.parameters.status))) || "").trim();
    }

    function agentPresenceStatus(agent) {
      const parameters = agent && agent.parameters ? agent.parameters : {};
      const presence = agent && agent.presence ? agent.presence : {};
      return String(presence.status || parameters.presence_status || "unknown").trim();
    }

    function agentExpiresAt(agent) {
      const parameters = agent && agent.parameters ? agent.parameters : {};
      const value = parameters.expires_at || agent.expires_at || "";
      if (!value) {
        return null;
      }
      const date = new Date(value);
      return Number.isNaN(date.getTime()) ? null : date;
    }

    function isExpiredEmptyAgent(agent) {
      const expiresAt = agentExpiresAt(agent);
      return agentStatus(agent) === "empty" && expiresAt !== null && expiresAt.getTime() <= Date.now();
    }

    function isLiveEmptyAgent(agent) {
      return agentStatus(agent) === "empty" && !isExpiredEmptyAgent(agent);
    }

    function isSpecializedAgent(agent) {
      const status = agentStatus(agent);
      return status !== "empty" && status !== "template";
    }

    function formatRemainingTime(expiresAt) {
      if (!expiresAt) {
        return "срок не задан";
      }
      const remainingMs = expiresAt.getTime() - Date.now();
      if (remainingMs <= 0) {
        return "истек";
      }
      const totalMinutes = Math.ceil(remainingMs / 60000);
      const hours = Math.floor(totalMinutes / 60);
      const minutes = totalMinutes % 60;
      if (hours && minutes) {
        return `${hours} ч ${minutes} мин`;
      }
      if (hours) {
        return `${hours} ч`;
      }
      return `${minutes} мин`;
    }

    function emptyAgentStateText(agent) {
      if (!agent) {
        return "Пустой агент не выбран.";
      }
      if (isExpiredEmptyAgent(agent)) {
        return "Срок ожидания истек. Агент должен завершить работу.";
      }
      if (pendingSpecializations[agent.phone]) {
        const spec = pendingSpecializations[agent.phone];
        return `Специализация отправлена: ${spec.name || "без имени"}. Агент получит ее при следующем опросе.`;
      }
      const expiresAt = agentExpiresAt(agent);
      if (!expiresAt) {
        return "Ждет специализацию. Срок ожидания не задан: агент создан старой версией prompt.";
      }
      return `Ждет специализацию. Осталось ${formatRemainingTime(expiresAt)}.`;
    }

    function emptyAgentStateKind(agent) {
      if (!agent) {
        return "waiting";
      }
      if (isExpiredEmptyAgent(agent)) {
        return "expired";
      }
      if (pendingSpecializations[agent.phone]) {
        return "sent";
      }
      return "waiting";
    }

    function emptyAgentStateLabel(agent) {
      const kind = emptyAgentStateKind(agent);
      if (kind === "expired") {
        return "истек";
      }
      if (kind === "sent") {
        return "отправлено";
      }
      return "ждет";
    }

    function updateSpecializeAgentState() {
      const agent = agents.find((item) => item.phone === specializePhoneEl.value) || null;
      const kind = emptyAgentStateKind(agent);
      specializeAgentStateEl.textContent = emptyAgentStateText(agent);
      specializeAgentStateEl.className = `agent-state-notice ${kind}`;
    }

    function agentListItem(agent, extraText = "") {
      const phone = agent.phone ? ` · ${agent.phone}` : "";
      const status = agentStatus(agent) ? ` · ${agentStatus(agent)}` : "";
      const extra = extraText ? ` · ${extraText}` : "";
      if (agentStatus(agent) === "empty") {
        const kind = emptyAgentStateKind(agent);
        return `<li>${escapeHtml(agent.name || "Без имени")}${escapeHtml(phone)} <span class="agent-state-chip ${kind}">${escapeHtml(emptyAgentStateLabel(agent))}</span>${escapeHtml(extra ? " · " + extra : "")}</li>`;
      }
      const presence = agentPresenceStatus(agent) === "alive"
        ? ` <span class="agent-state-chip alive">жив</span>`
        : "";
      return `<li>${escapeHtml(agent.name || "Без имени")}${escapeHtml(phone)}${escapeHtml(status)}${presence}${escapeHtml(extra)}</li>`;
    }

    function renderAgentStatusSummary() {
      const specializedAgents = agents.filter(isSpecializedAgent);
      const liveEmptyAgents = agents.filter(isLiveEmptyAgent);
      const expiredEmptyAgents = agents.filter(isExpiredEmptyAgent);

      specializedAgentsCountEl.textContent = String(specializedAgents.length);
      liveEmptyAgentsCountEl.textContent = String(liveEmptyAgents.length);
      expiredEmptyAgentsCountEl.textContent = String(expiredEmptyAgents.length);

      specializedAgentsListEl.innerHTML = specializedAgents.length
        ? specializedAgents.map((agent) => agentListItem(agent)).join("")
        : `<li class="subtle">Нет специализированных агентов.</li>`;
      liveEmptyAgentsListEl.innerHTML = liveEmptyAgents.length
        ? liveEmptyAgents.map((agent) => agentListItem(agent, emptyAgentStateText(agent))).join("")
        : `<li class="subtle">Нет живых пустых агентов.</li>`;
      expiredEmptyAgentsListEl.innerHTML = expiredEmptyAgents.length
        ? expiredEmptyAgents.map((agent) => agentListItem(agent, "ожидание истекло")).join("")
        : `<li class="subtle">Нет истекших пустых агентов.</li>`;
    }

    function agentParamRow(key = "", value = "") {
      return `<div class="agent-param-row">
        <input data-param-field="key" placeholder="ui_url" value="${escapeHtml(key)}">
        <input data-param-field="value" placeholder="https://..." value="${escapeHtml(value)}">
        <button class="icon-button" data-action="remove-agent-param" type="button" title="Удалить параметр" aria-label="Удалить параметр">×</button>
      </div>`;
    }

    function agentParamsHtml(parameters) {
      const entries = Object.entries(parameters || {});
      return entries.length
        ? entries.map(([key, value]) => agentParamRow(key, value)).join("")
        : `<div class="subtle">Параметры пока не заданы.</div>`;
    }

    function uniqueList(values) {
      return Array.from(new Set(values));
    }

    function profileWithParameters(profile, parameters) {
      const missing = [];
      const text = String(profile || "").replace(/\\$\\s*\\{\\s*([A-Za-z0-9_.-]+)\\s*\\}/g, (match, name) => {
        if (Object.prototype.hasOwnProperty.call(parameters || {}, name) && String(parameters[name]).trim() !== "") {
          return String(parameters[name]);
        }
        missing.push(name);
        return match;
      });
      return {
        text,
        missing: uniqueList(missing)
      };
    }

    function profilePreviewHtml(profile, parameters) {
      const missing = [];
      let html = "";
      let lastIndex = 0;
      const regex = /\\$\\s*\\{\\s*([A-Za-z0-9_.-]+)\\s*\\}/g;
      String(profile || "").replace(regex, (match, name, offset) => {
        html += escapeHtml(String(profile || "").slice(lastIndex, offset));
        if (Object.prototype.hasOwnProperty.call(parameters || {}, name) && String(parameters[name]).trim() !== "") {
          html += escapeHtml(parameters[name]);
        } else {
          missing.push(name);
          html += `<span class="missing-placeholder">${escapeHtml(match)}</span>`;
        }
        lastIndex = offset + match.length;
        return match;
      });
      html += escapeHtml(String(profile || "").slice(lastIndex));
      return {
        html: html || `<span class="subtle">Профиль пуст.</span>`,
        missing: uniqueList(missing)
      };
    }

    function updateAgentPreview() {
      const row = agentsEl.querySelector(".agent-row");
      if (!row) {
        return;
      }
      const agent = currentEditorAgent();
      const previewEl = row.querySelector('[data-field="profile-preview"]');
      const placeholderStatusEl = row.querySelector('[data-field="placeholder-status"]');
      if (!agent || !previewEl || !placeholderStatusEl) {
        return;
      }
      const preview = profilePreviewHtml(agent.profile, agent.parameters);
      previewEl.innerHTML = preview.html;
      if (preview.missing.length) {
        placeholderStatusEl.innerHTML = `Не заполнены параметры: ${preview.missing.map(escapeHtml).join(", ")}`;
        placeholderStatusEl.className = "subtle status error";
      } else {
        placeholderStatusEl.textContent = "Все найденные placeholders заполнены.";
        placeholderStatusEl.className = "subtle status ok";
      }
    }

    function actorTasksHtml(tasks) {
      const items = Array.isArray(tasks) ? tasks : [];
      if (!items.length) {
        return `<div class="subtle">Импортированных задач нет.</div>`;
      }
      return `<ol class="actor-task-list">${items.map((task) => {
        const message = typeof task === "string" ? task : String((task && task.message) || "");
        const queue = typeof task === "object" && task ? String(task.queue || "worker-all") : "worker-all";
        const taskId = typeof task === "object" && task ? String(task.task_id || "") : "";
        const meta = [taskId, queue].filter(Boolean).join(" · ");
        return `<li><strong>${escapeHtml(meta || "task")}</strong><div>${escapeHtml(message)}</div></li>`;
      }).join("")}</ol>`;
    }

    function agentRow(agent) {
      const rowId = agent.id || agentId();
      return `<div class="agent-row" data-agent-id="${escapeHtml(rowId)}">
        ${agent.template_source ? `<div class="subtle">Шаблон: ${escapeHtml(agent.template_source)}</div>` : ""}
        ${agentStatus(agent) ? `<div class="subtle">Статус: ${escapeHtml(agentStatus(agent))}</div>` : ""}
        <div class="agent-row-head">
          <div>
            <label>Имя агента</label>
            <input data-field="name" placeholder="Analyst" value="${escapeHtml(agent.name || "")}">
          </div>
          <div>
            <label>Телефон</label>
            <input data-field="phone" placeholder="1001" value="${escapeHtml(agent.phone || "")}">
          </div>
        </div>
        <label>Профиль агента</label>
        <textarea class="agent-profile" data-field="profile" spellcheck="false" placeholder="Роль, зона ответственности, стиль сообщений">${escapeHtml(agent.profile || "")}</textarea>
        <div class="agent-params-head">
          <label>Параметры шаблона</label>
          <button class="secondary" data-action="add-agent-param" type="button">Добавить параметр</button>
        </div>
        <div class="agent-params" data-field="parameters">
          ${agentParamsHtml(agent.parameters || {})}
        </div>
        <div class="agent-template-preview">
          <h3>Предпросмотр с параметрами</h3>
          <div class="subtle" data-field="placeholder-status"></div>
          <div class="agent-preview-body" data-field="profile-preview"></div>
        </div>
        <div class="agent-template-preview">
          <h3>Задачи из JSON (${Array.isArray(agent.tasks) ? agent.tasks.length : 0})</h3>
          ${actorTasksHtml(agent.tasks)}
        </div>
      </div>`;
    }

    function currentEditorAgent() {
      const row = agentsEl.querySelector(".agent-row");
      if (!row || !selectedAgentId) {
        return null;
      }
      const existingAgent = agents.find((agent) => agent.id === selectedAgentId) || {};
      const parameters = {};
      row.querySelectorAll(".agent-param-row").forEach((paramRow) => {
        const key = paramRow.querySelector('[data-param-field="key"]').value.trim();
        const value = paramRow.querySelector('[data-param-field="value"]').value.trim();
        if (key) {
          parameters[key] = value;
        }
      });
      return {
        id: row.dataset.agentId || selectedAgentId,
        name: row.querySelector('[data-field="name"]').value.trim(),
        phone: row.querySelector('[data-field="phone"]').value.trim(),
        profile: row.querySelector('[data-field="profile"]').value.trim(),
        parameters,
        status: existingAgent.status || "",
        template_source: existingAgent.template_source || "",
        tasks: Array.isArray(existingAgent.tasks) ? existingAgent.tasks : []
      };
    }

    function captureCurrentAgent() {
      const edited = currentEditorAgent();
      if (!edited) {
        return;
      }
      const index = agents.findIndex((agent) => agent.id === selectedAgentId);
      if (index >= 0) {
        agents[index] = edited;
      }
      const allIndex = allAgents.findIndex((agent) => agent.id === selectedAgentId);
      if (allIndex >= 0) {
        allAgents[allIndex] = edited;
      }
    }

    function renderAgentSelector(preferredId = selectedAgentId) {
      if (!agents.length) {
        selectedAgentId = "";
        agentSelectorEl.innerHTML = `<option value="">Нет агентов для выбранного Git context</option>`;
        agentSelectorEl.value = "";
        return;
      }

      if (!agents.some((agent) => agent.id === preferredId)) {
        preferredId = agents[0].id;
      }
      selectedAgentId = preferredId;
      agentSelectorEl.innerHTML = agents.map((agent, index) => {
        const phoneText = agent.phone ? ` · ${agent.phone}` : "";
        const label = `${agent.name || `Agent ${index + 1}`}${phoneText}`;
        const selected = agent.id === selectedAgentId ? " selected" : "";
        return `<option value="${escapeHtml(agent.id)}"${selected}>${escapeHtml(label)}</option>`;
      }).join("");
      agentSelectorEl.value = selectedAgentId;
    }

    function renderSelectedAgent() {
      const agent = agents.find((item) => item.id === selectedAgentId);
      agentsEl.innerHTML = agent
        ? agentRow(agent)
        : `<div class="subtle">Для выбранного Git context нет прикрепленных агентов.</div>`;
      updateAgentPreview();
    }

    function activeProjectLabel() {
      const context = activeProjectContext();
      if (!context) {
        return "активный проект";
      }
      return context.project_name || context.git_context_key || "активный проект";
    }

    function agentProjectCloneSourceId(agent) {
      const parameters = agent && agent.parameters ? agent.parameters : {};
      return String(parameters.project_clone_source_agent_id || "").trim();
    }

    function agentProjectCloneSourceName(agent) {
      const parameters = agent && agent.parameters ? agent.parameters : {};
      return String(parameters.project_clone_source_name || "").trim();
    }

    function projectCloneExistsForSource(sourceAgent) {
      const sourceId = String((sourceAgent && sourceAgent.id) || "").trim();
      const sourceName = String((sourceAgent && sourceAgent.name) || "").trim();
      return allAgents.some((agent) => {
        if (!agentMatchesActiveGitContext(agent)) {
          return false;
        }
        if (sourceId && agentProjectCloneSourceId(agent) === sourceId) {
          return true;
        }
        return Boolean(sourceName && agentProjectCloneSourceName(agent) === sourceName);
      });
    }

    function projectCloneName(sourceAgent, projectLabel) {
      const baseName = String((sourceAgent && sourceAgent.name) || "Agent").trim() || "Agent";
      const cleanProject = String(projectLabel || "Project").trim() || "Project";
      const names = new Set(allAgents.map((agent) => String(agent.name || "").trim().toLowerCase()).filter(Boolean));
      let candidate = `${baseName} (${cleanProject})`;
      let index = 2;
      while (names.has(candidate.toLowerCase())) {
        candidate = `${baseName} (${cleanProject} ${index})`;
        index += 1;
      }
      return candidate;
    }

    function projectCloneParameters(sourceAgent, context) {
      const phone = String((context && context.phone) || activeMappedQueuePhone()).trim();
      const parameters = parametersForProfileCopy(sourceAgent, phone);
      parameters.project_phone = phone;
      parameters.project_name = String((context && context.project_name) || activeProjectLabel()).trim();
      parameters.git_context_key = String((context && context.git_context_key) || activeGitContextKey()).trim();
      parameters.project_clone_source_agent_id = String((sourceAgent && sourceAgent.id) || "").trim();
      parameters.project_clone_source_name = String((sourceAgent && sourceAgent.name) || "").trim();
      delete parameters.git_context_excluded_keys;
      return parameters;
    }

    function agentProjectItemHtml(agent, actions) {
      const phone = String((agent && agent.phone) || "").trim();
      const sourceName = agentProjectCloneSourceName(agent);
      const sourceText = sourceName ? ` · исходный: ${sourceName}` : "";
      const taskCount = Array.isArray(agent && agent.tasks) ? agent.tasks.length : 0;
      const meta = `${phone ? `phone ${phone}` : "phone не задан"}${sourceText} · задач: ${taskCount}`;
      const actionButtons = (actions || []).map((action) => {
        return `<button class="secondary" data-action="${escapeHtml(action.action)}" data-agent-id="${escapeHtml((agent && agent.id) || "")}" type="button">${escapeHtml(action.label)}</button>`;
      }).join("");
      return `<div class="agent-project-item">
        <div>
          <div class="agent-project-item-name">${escapeHtml((agent && agent.name) || "Agent")}</div>
          <div class="agent-project-item-meta">${escapeHtml(meta)}</div>
        </div>
        <div class="agent-project-item-actions">${actionButtons}</div>
      </div>`;
    }

    function renderAgentProjectControls() {
      const activeKey = activeGitContextKey();
      const projectLabel = activeProjectLabel();
      const activePhone = activeMappedQueuePhone();
      const availableAgents = activeKey
        ? allAgents.filter((agent) => (
          agentStatus(agent) !== "system"
          && !agentMatchesActiveGitContext(agent)
          && !projectCloneExistsForSource(agent)
        ))
        : [];
      const attachedAgents = activeKey ? agents : [];

      projectAgentAttachSelectEl.innerHTML = availableAgents.length
        ? availableAgents.map((agent) => {
          const phone = agent.phone ? ` · ${agent.phone}` : "";
          return `<option value="${escapeHtml(agent.id)}">${escapeHtml((agent.name || "Agent") + phone)}</option>`;
        }).join("")
        : `<option value="">Нет агентов для добавления</option>`;

      projectAgentAttachSelectEl.disabled = !activeKey || !activePhone || !availableAgents.length;
      attachAgentToProjectButtonEl.disabled = !activeKey || !activePhone || !availableAgents.length;
      detachSelectedAgentFromProjectButtonEl.disabled = !activeKey || !selectedAgentId;
      importProjectActorsButtonEl.disabled = !activeKey || !activePhone;
      deleteAllProjectActorsButtonEl.disabled = !activeKey || !activePhone || !attachedAgents.length;
      refreshProjectSprintsButtonEl.disabled = !activeKey || !activePhone;
      projectAvailableAgentsEl.innerHTML = activeKey
        ? availableAgents.length
          ? availableAgents.map((agent) => agentProjectItemHtml(agent, [
            {action: "copy-agent-to-project", label: "В проект"}
          ])).join("")
          : `<div class="subtle">Нет агентов для копирования.</div>`
        : `<div class="subtle">Сначала выберите активный проект.</div>`;
      projectAttachedAgentsEl.innerHTML = activeKey
        ? attachedAgents.length
          ? attachedAgents.map((agent) => agentProjectItemHtml(agent, [
            {action: "edit-project-agent", label: "Редактировать"},
            {action: "detach-agent-from-project", label: "Снять"}
          ])).join("")
          : `<div class="subtle">В проекте нет агентов.</div>`
        : `<div class="subtle">Сначала выберите активный проект.</div>`;

      if (!activeKey) {
        agentProjectStatusEl.textContent = "Выберите активный проект во вкладке Git context.";
      } else if (!activePhone) {
        agentProjectStatusEl.textContent = "У активного проекта нет телефона для проектной копии агента.";
      } else {
        agentProjectStatusEl.textContent = `Проект: ${projectLabel} · phone ${activePhone}. В проекте ${agents.length} из ${allAgents.length} агентов.`;
      }
    }

    function renderProjectSprints() {
      const activePhone = activeMappedQueuePhone();
      if (!activePhone) {
        projectSprintsEl.innerHTML = "";
        projectSprintsStatusEl.textContent = "Выберите проект с телефоном, чтобы увидеть сохранённые спринты.";
        return;
      }
      if (projectSprintsProjectPhone !== activePhone) {
        projectSprintsEl.innerHTML = "";
        projectSprintsStatusEl.textContent = "Загружаю историю спринтов...";
        return;
      }
      projectSprintsStatusEl.textContent = projectSprints.length
        ? `Сохранено спринтов: ${projectSprints.length}. Архив содержит исходный JSON и состояние выполнения.`
        : "История пока пуста. Первый импорт создаст текущий спринт и сохранит существующее состояние проекта как архив.";
      projectSprintsEl.innerHTML = projectSprints.map((sprint) => {
        const isCurrent = sprint.status === "current";
        const statusText = isCurrent ? "текущий" : "архив";
        const statusClass = isCurrent ? "" : " archived";
        const savedAt = sprint.archived_at || sprint.imported_at;
        const dateText = savedAt ? formatLocalDateTime(savedAt) : "дата неизвестна";
        const sourceText = sprint.source_filename ? ` · файл: ${sprint.source_filename}` : "";
        const legacyText = sprint.legacy ? " · состояние до включения истории" : "";
        const patchText = Number.isInteger(sprint.code_patch_count)
          ? ` · code patches: ${sprint.code_patch_count}${sprint.code_patch_error_count ? ` (недоступно: ${sprint.code_patch_error_count})` : ""}`
          : "";
        const meta = `#${sprint.sequence} · ${dateText} · агентов: ${sprint.agent_count || 0} · задач: ${sprint.task_count || 0}${patchText}${sourceText}${legacyText}`;
        return `<div class="sprint-history-item">
          <div>
            <div class="sprint-history-title">${escapeHtml(sprint.title || `Спринт ${sprint.sequence}`)}<span class="sprint-history-badge${statusClass}">${statusText}</span></div>
            <div class="agent-project-item-meta">${escapeHtml(meta)}</div>
          </div>
          <div class="actions">
            <button class="secondary" data-action="download-project-sprint" data-sprint-url="${escapeHtml(sprint.download_url || "")}" type="button">Скачать JSON</button>
          </div>
        </div>`;
      }).join("");
    }

    async function refreshProjectSprints() {
      const projectPhone = activeMappedQueuePhone();
      if (!projectPhone) {
        projectSprints = [];
        projectSprintsProjectPhone = "";
        renderProjectSprints();
        return;
      }
      const response = await fetch(`/api/v1/projects/${encodeURIComponent(projectPhone)}/sprints`);
      const data = await response.json();
      if (!response.ok) {
        throw new Error(formatMessage(data.detail || "Ошибка загрузки истории спринтов."));
      }
      if (projectPhone !== activeMappedQueuePhone()) {
        return;
      }
      projectSprintsProjectPhone = projectPhone;
      projectSprints = Array.isArray(data.sprints) ? data.sprints : [];
      renderProjectSprints();
    }

    async function attachAgentToProjectById(sourceAgentId) {
      captureCurrentAgent();
      const context = phoneContextByPhone(activeQueuePhone());
      const activePhone = activeMappedQueuePhone();
      if (!context || !activeGitContextKey() || !activePhone) {
        setAgentsStatus("Сначала выберите активный Git context с телефоном проекта.", "error");
        return;
      }
      const sourceAgent = allAgents.find((agent) => agent.id === sourceAgentId);
      if (!sourceAgent) {
        setAgentsStatus("Выберите агента для копирования в проект.", "error");
        return;
      }
      if (projectCloneExistsForSource(sourceAgent) || agentMatchesActiveGitContext(sourceAgent)) {
        setAgentsStatus("У этого агента уже есть копия или запись в активном проекте.", "error");
        return;
      }

      const clone = {
        id: agentId(),
        name: projectCloneName(sourceAgent, context.project_name || context.git_context_key || "Project"),
        phone: activePhone,
        profile: String(sourceAgent.profile || ""),
        parameters: projectCloneParameters(sourceAgent, context),
        status: sourceAgent.status || "",
        template_source: sourceAgent.template_source
          ? `project-clone:${sourceAgent.template_source}`
          : `project-clone:${sourceAgent.name || sourceAgent.id || "agent"}`
      };
      allAgents.push(clone);
      renderAgents(allAgents, clone.id);
      syncActorsFromAgents(allAgents);
      await saveAgents(`Создана и сохранена проектная копия: ${clone.name} · phone ${activePhone}.`);
    }

    function attachExistingAgentToProject() {
      return attachAgentToProjectById(projectAgentAttachSelectEl.value);
    }

    async function detachAgentFromProjectById(agentIdValue) {
      captureCurrentAgent();
      const targetAgentId = String(agentIdValue || "").trim();
      if (!targetAgentId) {
        setAgentsStatus("Агент не выбран.", "error");
        return;
      }
      const selectedAgent = allAgents.find((agent) => agent.id === targetAgentId);
      if (!selectedAgent || !agentMatchesActiveGitContext(selectedAgent)) {
        setAgentsStatus("Выбранный агент не относится к активному проекту.", "error");
        return;
      }
      const projectLabel = activeProjectLabel();
      const selectedName = selectedAgent.name || "Agent";
      const confirmed = window.confirm(`Снять агента «${selectedName}» с проекта «${projectLabel}»?`);
      if (!confirmed) {
        setAgentsStatus("Снятие агента с проекта отменено.");
        return;
      }
      const currentIndex = agents.findIndex((agent) => agent.id === targetAgentId);
      const removedName = selectedName;
      allAgents = allAgents.filter((agent) => agent.id !== targetAgentId);
      agents = filteredAgentsForActiveContext(allAgents);
      const nextAgent = agents[Math.min(currentIndex, agents.length - 1)];
      renderAgents(allAgents, nextAgent ? nextAgent.id : "");
      syncActorsFromAgents(allAgents);
      await saveAgents(`Агент снят с проекта и сохранен: ${removedName}.`);
    }

    function detachSelectedAgentFromProject() {
      return detachAgentFromProjectById(selectedAgentId);
    }

    async function importProjectActorsFromJson() {
      const projectPhone = activeMappedQueuePhone();
      const file = projectActorsJsonFileEl.files && projectActorsJsonFileEl.files[0];
      if (!projectPhone) {
        setAgentsStatus("Сначала выберите активный проект с телефоном.", "error");
        return;
      }
      if (!file) {
        setAgentsStatus("Выберите JSON-файл с агентами и задачами.", "error");
        return;
      }
      let payload;
      try {
        payload = JSON.parse(await file.text());
      } catch (error) {
        setAgentsStatus(`Файл не является корректным JSON: ${error.message}`, "error");
        return;
      }
      setAgentsStatus("Импортирую агентов и задачи...");
      const response = await fetch(`/api/v1/projects/${encodeURIComponent(projectPhone)}/agents/import`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Nginx-QA-Sprint-Filename": encodeURIComponent(file.name || "sprint.json")
        },
        body: JSON.stringify(payload)
      });
      const data = await response.json();
      if (!response.ok) {
        setAgentsStatus(formatMessage(data.detail || "Ошибка импорта агентов."), "error");
        return;
      }
      projectActorsJsonFileEl.value = "";
      clearAgentEditorDirty();
      await refreshAgents({force: true});
      await Promise.all([refreshQueues(), refreshProjectSprints()]);
      setAgentsStatus(
        `Импорт завершен: сохранён ${data.sprint && data.sprint.title ? data.sprint.title : "новый спринт"}; агентов ${data.imported_agent_count ?? data.imported_actor_count}, удалено ${data.removed_agent_count ?? data.removed_actor_count}, задач поставлено ${data.queued_task_count}.`,
        "ok"
      );
    }

    async function deleteAllProjectActors() {
      const projectPhone = activeMappedQueuePhone();
      if (!projectPhone) {
        setAgentsStatus("Сначала выберите активный проект с телефоном.", "error");
        return;
      }
      const projectLabel = activeProjectLabel();
      const confirmed = window.confirm(
        `Удалить всех агентов проекта «${projectLabel}», их ожидающие задачи и архивировать управляемые группы? История сообщений останется.`
      );
      if (!confirmed) {
        setAgentsStatus("Массовое удаление отменено.");
        return;
      }
      setAgentsStatus("Удаляю всех агентов проекта...");
      const response = await fetch(
        `/api/v1/projects/${encodeURIComponent(projectPhone)}/agents?include_managed=true`,
        {method: "DELETE"}
      );
      const data = await response.json();
      if (!response.ok) {
        setAgentsStatus(formatMessage(data.detail || "Ошибка удаления агентов."), "error");
        return;
      }
      clearAgentEditorDirty();
      await refreshAgents({force: true});
      await refreshQueues();
      setAgentsStatus(
        `Удалено агентов: ${data.deleted_agent_count ?? data.deleted_actor_count}; снято ожидающих задач: ${data.removed_task_count}.`,
        "ok"
      );
    }

    function selectAgentForEdit(agentIdValue) {
      captureCurrentAgent();
      const targetAgentId = String(agentIdValue || "").trim();
      const targetAgent = agents.find((agent) => agent.id === targetAgentId);
      if (!targetAgent) {
        setAgentsStatus("Этот агент не относится к активному проекту.", "error");
        return;
      }
      selectedAgentId = targetAgentId;
      renderAgentSelector(selectedAgentId);
      renderSelectedAgent();
      renderAgentProjectControls();
      setAgentsStatus(`Редактирование агента: ${targetAgent.name || "Agent"}. После правок нажмите «Сохранить агентов».`, "ok");
      agentsEl.scrollIntoView({behavior: "smooth", block: "start"});
      const nameInput = agentsEl.querySelector('[data-field="name"]');
      if (nameInput) {
        nameInput.focus();
      }
    }

    function updateSpecializationPanel() {
      const emptyAgents = agents.filter(isLiveEmptyAgent);
      const prevPhone = specializePhoneEl.value;
      specializePhoneEl.innerHTML = emptyAgents.length
        ? emptyAgents.map((agent) => `<option value="${escapeHtml(agent.phone)}">${escapeHtml(agent.name)} · ${escapeHtml(agent.phone)}</option>`).join("")
        : `<option value="">Нет свободных пустых агентов</option>`;
      if (prevPhone && emptyAgents.some((agent) => agent.phone === prevPhone)) {
        specializePhoneEl.value = prevPhone;
      }

      const templates = agents.filter((agent) => !["empty", "template"].includes(agentStatus(agent)));
      const prevTemplate = specializeTemplateEl.value;
      specializeTemplateEl.innerHTML = `<option value="">-- Выберите шаблон --</option>` + templates.map((agent) => {
        return `<option value="${escapeHtml(agent.id)}">${escapeHtml(agent.name)}</option>`;
      }).join("");
      if (prevTemplate && templates.some((agent) => agent.id === prevTemplate)) {
        specializeTemplateEl.value = prevTemplate;
      }
      updateSpecializeAgentState();
    }

    function normalizeAgentForClient(agent) {
      return {
        id: agent.id || agentId(),
        name: agent.name || "",
        phone: agent.phone || "",
        profile: agent.profile || "",
        parameters: agent.parameters || {},
        status: agent.status || (agent.parameters && agent.parameters.status) || "",
        template_source: agent.template_source || "",
        tasks: Array.isArray(agent.tasks) ? agent.tasks : []
      };
    }

    function renderAgentsForActiveContext(preferredId = selectedAgentId) {
      const previousSelectedId = preferredId || selectedAgentId;
      agents = filteredAgentsForActiveContext(allAgents);
      const nextPreferredId = agents.some((agent) => agent.id === previousSelectedId)
        ? previousSelectedId
        : (agents[0] && agents[0].id) || "";
      renderAgentSelector(nextPreferredId);
      renderSelectedAgent();
      updateSpecializationPanel();
      renderAgentStatusSummary();
      renderAgentProjectControls();
      setAgentsStatus(`Показано агентов для выбранного Git context: ${agents.length} из ${allAgents.length}.`);
    }

    function renderAgents(agentList, preferredId = selectedAgentId) {
      allAgents = (agentList || []).map(normalizeAgentForClient);
      agents = filteredAgentsForActiveContext(allAgents);
      const nextPreferredId = agents.some((agent) => agent.id === preferredId)
        ? preferredId
        : (agents[0] && agents[0].id) || "";
      renderAgentSelector(nextPreferredId);
      renderSelectedAgent();
      updateSpecializationPanel();
      renderAgentStatusSummary();
      renderAgentProjectControls();
    }

    function collectAgents() {
      captureCurrentAgent();
      const cleanAgents = [];
      const seen = new Set();
      allAgents.forEach((agent) => {
        const name = String(agent.name || "").trim();
        const key = name.toLowerCase();
        if (!name || seen.has(key)) {
          return;
        }
        seen.add(key);
        cleanAgents.push({
          id: agent.id || agentId(),
          name,
          phone: String(agent.phone || "").trim(),
          profile: String(agent.profile || "").trim(),
          parameters: agent.parameters || {},
          status: agent.status || "",
          template_source: agent.template_source || "",
          tasks: Array.isArray(agent.tasks) ? agent.tasks : []
        });
      });
      return cleanAgents;
    }

    function syncActorsFromAgents(agentList) {
      actors = uniqueAgentNames(agentList);
      renderEmailSenderOptions();
      updateActorControls();
      updatePhoneAgentControls();
      updateConsultantControls();
    }

    function addAgent(agent = null) {
      captureCurrentAgent();
      markAgentEditorDirty();
      const nextAgent = agent || {
        id: agentId(),
        name: nextAgentName(),
        phone: activeMappedQueuePhone() || "",
        profile: "",
        parameters: {},
        template_source: ""
      };
      allAgents.push(nextAgent);
      renderAgents(allAgents, nextAgent.id);
      syncActorsFromAgents(allAgents);
      setAgentsStatus("");
    }

    function addSelectedAgentParam() {
      const paramsEl = agentsEl.querySelector(".agent-params");
      if (!paramsEl) {
        return;
      }
      markAgentEditorDirty();
      if (!paramsEl.querySelector(".agent-param-row")) {
        paramsEl.innerHTML = "";
      }
      paramsEl.insertAdjacentHTML("beforeend", agentParamRow());
      captureCurrentAgent();
      setAgentsStatus("");
    }

    function removeSelectedAgent() {
      if (!selectedAgentId) {
        return;
      }
      markAgentEditorDirty();
      const currentIndex = agents.findIndex((agent) => agent.id === selectedAgentId);
      allAgents = allAgents.filter((agent) => agent.id !== selectedAgentId);
      agents = filteredAgentsForActiveContext(allAgents);
      const nextAgent = agents[Math.min(currentIndex, agents.length - 1)];
      renderAgents(allAgents, nextAgent ? nextAgent.id : "");
      syncActorsFromAgents(allAgents);
      setAgentsStatus("Агент удален из списка. Нажмите «Сохранить агентов», чтобы записать изменение.", "ok");
    }

    function addPhoneToQueueUrls(text, phone) {
      const phoneValue = String(phone || "").trim();
      if (!phoneValue) {
        return String(text || "");
      }
      const encodedPhone = encodeURIComponent(phoneValue);
      return String(text || "").replace(/\\/(work-design|test-design|work|test)(?:\\/[^/?#\\s`'"]*)?(?=([?#\\s`'"]|$))/g, (match, route) => {
        return `/${route}/${encodedPhone}`;
      });
    }

    function parametersForProfileCopy(agent, phone) {
      const parameters = Object.assign({}, agent.parameters || {});
      const phoneValue = String(phone || "").trim();
      parameters.agent_phone = phoneValue;
      parameters.phone = phoneValue;
      parameters.selected_phone = phoneValue;
      parameters.queue_phone = phoneValue;
      Object.keys(parameters).forEach((key) => {
        const isEndpoint = key === "get_task_endpoint"
          || key === "post_report_endpoint"
          || /(^|_)endpoint$/i.test(key);
        if (isEndpoint) {
          parameters[key] = addPhoneToQueueUrls(parameters[key], phoneValue);
        }
      });
      return parameters;
    }

    function copyProfilePhoneOptions() {
      const currentAgent = agents.find((item) => item.id === selectedAgentId) || null;
      const seen = new Set();
      const options = [];
      [currentAgent].concat(agents).forEach((agent) => {
        if (!agent) {
          return;
        }
        const phone = String(agent.phone || "").trim();
        if (!phone || seen.has(phone)) {
          return;
        }
        seen.add(phone);
        options.push({
          phone,
          label: `${agent.name || "Agent"} · ${phone}`
        });
      });
      phoneGitContexts.forEach((context) => {
        const phone = String(context.phone || "").trim();
        if (!phone || seen.has(phone)) {
          return;
        }
        seen.add(phone);
        options.push({
          phone,
          label: `${context.project_name || "Project"} · ${phone}`
        });
      });
      return options;
    }

    function renderCopyProfilePhoneOptions() {
      const options = copyProfilePhoneOptions();
      copyProfilePhoneSelectEl.innerHTML = options.length
        ? options.map((option) => `<option value="${escapeHtml(option.phone)}">${escapeHtml(option.label)}</option>`).join("")
        : `<option value="">Нет телефонов в списке агентов</option>`;
    }

    function setCopyProfilePhoneStatus(text, state = "") {
      copyProfilePhoneStatusEl.textContent = text;
      copyProfilePhoneStatusEl.className = "status" + (state ? " " + state : "");
    }

    function selectedCopyProfilePhone() {
      return (copyProfilePhoneManualEl.value.trim() || copyProfilePhoneSelectEl.value.trim());
    }

    function openCopyProfilePhoneModal() {
      captureCurrentAgent();
      const agent = agents.find((item) => item.id === selectedAgentId);
      if (!agent) {
        setAgentsStatus("Агент не выбран.", "error");
        return;
      }
      renderCopyProfilePhoneOptions();
      copyProfilePhoneManualEl.value = "";
      setCopyProfilePhoneStatus("");
      copyProfilePhoneModalEl.hidden = false;
      copyProfilePhoneSelectEl.focus();
    }

    function closeCopyProfilePhoneModal() {
      copyProfilePhoneModalEl.hidden = true;
      setCopyProfilePhoneStatus("");
    }

    async function copySelectedAgentProfile() {
      captureCurrentAgent();
      const agent = agents.find((item) => item.id === selectedAgentId);
      if (!agent) {
        setAgentsStatus("Агент не выбран.", "error");
        return;
      }
      const phone = selectedCopyProfilePhone();
      if (!phone) {
        setCopyProfilePhoneStatus("Выберите или введите номер телефона.", "error");
        return;
      }
      const result = profileWithParameters(agent.profile, parametersForProfileCopy(agent, phone));
      result.text = addPhoneToQueueUrls(result.text, phone);
      if (navigator.clipboard && window.isSecureContext) {
        await navigator.clipboard.writeText(result.text);
      } else {
        fallbackCopyText(result.text);
      }
      closeCopyProfilePhoneModal();
      if (result.missing.length) {
        setAgentsStatus(`Скопировано, но не заполнены placeholders: ${result.missing.join(", ")}.`, "error");
      } else {
        setAgentsStatus(`Профиль скопирован с параметрами для телефона ${phone}.`, "ok");
      }
      updateAgentPreview();
    }

    function uniqueAgentName(baseName) {
      const existing = new Set(agents.map((agent) => String(agent.name || "").trim().toLowerCase()).filter(Boolean));
      const fallback = "Agent Clone";
      const cleanBase = String(baseName || "").trim() || fallback;
      if (!existing.has(cleanBase.toLowerCase())) {
        return cleanBase;
      }

      let index = 2;
      while (existing.has(`${cleanBase} ${index}`.toLowerCase())) {
        index += 1;
      }
      return `${cleanBase} ${index}`;
    }

    function nextAgentPhone(preferredPhone = "") {
      const existing = new Set(agents.map((agent) => String(agent.phone || "").trim()).filter(Boolean));
      const preferred = String(preferredPhone || "").trim();
      if (preferred && !existing.has(preferred)) {
        return preferred;
      }

      const numericPhones = agents
        .map((agent) => Number.parseInt(String(agent.phone || ""), 10))
        .filter((phone) => Number.isFinite(phone));
      let nextPhone = numericPhones.length ? Math.max(...numericPhones) + 1 : 1000;
      while (existing.has(String(nextPhone))) {
        nextPhone += 1;
      }
      return String(nextPhone);
    }

    function renderCloneSourceOptions(preferredId = selectedAgentId) {
      if (!agents.length) {
        cloneSourceAgentEl.innerHTML = `<option value="">Нет агентов</option>`;
        cloneSourceAgentEl.value = "";
        return;
      }

      const selectedId = agents.some((agent) => agent.id === preferredId) ? preferredId : agents[0].id;
      cloneSourceAgentEl.innerHTML = agents.map((agent) => {
        const selected = agent.id === selectedId ? " selected" : "";
        const phone = agent.phone || "no-phone";
        return `<option value="${escapeHtml(agent.id)}"${selected}>${escapeHtml(agent.name)} · ${escapeHtml(phone)}</option>`;
      }).join("");
      cloneSourceAgentEl.value = selectedId;
    }

    function resetClonePreview() {
      cloneCandidateAgent = null;
      cloneAgentPreviewEl.hidden = true;
      cloneCandidateNameEl.value = "";
      cloneCandidatePhoneEl.value = "";
      cloneCandidateProfileEl.value = "";
      cloneRawResponseEl.value = "";
      acceptCloneAgentButtonEl.disabled = true;
    }

    function openCloneAgentModal() {
      captureCurrentAgent();
      renderCloneSourceOptions(selectedAgentId);
      resetClonePreview();
      setCloneAgentStatus("");
      cloneAgentModalEl.hidden = false;
      cloneSourceAgentEl.focus();
    }

    function closeCloneAgentModal() {
      cloneAgentModalEl.hidden = true;
    }

    function renderCloneCandidatePreview(candidate, rawResponse) {
      cloneCandidateNameEl.value = candidate.name || "";
      cloneCandidatePhoneEl.value = candidate.phone || "";
      cloneCandidateProfileEl.value = candidate.profile || "";
      cloneRawResponseEl.value = rawResponse || JSON.stringify(candidate, null, 2);
      cloneAgentPreviewEl.hidden = false;
      acceptCloneAgentButtonEl.disabled = false;
    }

    function renderGeminiModelOptions(models) {
      const currentModel = cloneGeminiModelEl.value.trim();
      cloneGeminiModelsEl.innerHTML = (models || []).map((model) => {
        const value = model.short_name || String(model.name || "").replace(/^models\\//, "");
        const label = model.display_name && model.display_name !== value
          ? `${model.display_name} · ${value}`
          : value;
        return `<option value="${escapeHtml(value)}" label="${escapeHtml(label)}"></option>`;
      }).join("");
      if (currentModel) {
        cloneGeminiModelEl.value = currentModel;
      } else if (models && models.length) {
        cloneGeminiModelEl.value = models[0].short_name || String(models[0].name || "").replace(/^models\\//, "");
      }
    }

    async function loadGeminiModels() {
      setCloneAgentStatus("Загружаю список моделей Gemini...");
      const response = await fetch("/gemini/models", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({api_key: cloneGeminiApiKeyEl.value})
      });
      const data = await response.json();
      if (!response.ok) {
        setCloneAgentStatus(data.detail || "Не удалось загрузить список моделей Gemini.", "error");
        return;
      }

      const models = data.generate_content_models || [];
      renderGeminiModelOptions(models);
      if (!models.length) {
        setCloneAgentStatus(`Gemini вернул ${data.count || 0} моделей, но ни одна не поддерживает generateContent.`, "error");
        return;
      }

      setCloneAgentStatus(`Загружено моделей Gemini: ${data.count}. Для клонирования доступно: ${models.length}.`, "ok");
    }

    async function requestAgentClonePreview() {
      captureCurrentAgent();
      resetClonePreview();
      const sourceAgent = agents.find((agent) => agent.id === cloneSourceAgentEl.value);
      if (!sourceAgent) {
        setCloneAgentStatus("Выберите агента для клонирования.", "error");
        return;
      }

      const payload = {
        source_agent: sourceAgent,
        refinements: cloneRefinementsEl.value,
        api_key: cloneGeminiApiKeyEl.value,
        model: cloneGeminiModelEl.value,
        existing_agent_names: uniqueAgentNames(agents)
      };

      setCloneAgentStatus("Отправляю профиль в Gemini...");
      const response = await fetch("/agents/clone-preview", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify(payload)
      });
      const data = await response.json();
      if (!response.ok) {
        setCloneAgentStatus(data.detail || "Gemini не смог сформировать клон.", "error");
        return;
      }

      cloneCandidateAgent = data.candidate || null;
      if (!cloneCandidateAgent) {
        setCloneAgentStatus("Gemini вернул пустой результат.", "error");
        return;
      }

      renderCloneCandidatePreview(cloneCandidateAgent, data.raw_response || "");
      setCloneAgentStatus(`Кандидат сформирован через ${data.model || "Gemini"}. Проверьте профиль и нажмите Accept.`, "ok");
    }

    function acceptAgentClone() {
      if (!cloneCandidateAgent) {
        setCloneAgentStatus("Сначала сформируйте клон через Gemini.", "error");
        return;
      }

      const nextAgent = {
        id: agentId(),
        name: uniqueAgentName(cloneCandidateNameEl.value || cloneCandidateAgent.name),
        phone: nextAgentPhone(cloneCandidatePhoneEl.value || cloneCandidateAgent.phone),
        profile: String(cloneCandidateProfileEl.value || cloneCandidateAgent.profile || "").trim(),
        parameters: cloneCandidateAgent.parameters || {},
        template_source: cloneCandidateAgent.template_source || ""
      };
      addAgent(nextAgent);
      closeCloneAgentModal();
      setAgentsStatus(`Клон «${nextAgent.name}» добавлен в список. Нажмите «Сохранить агентов», чтобы записать изменение.`, "ok");
    }

    async function refreshAgents(options = {}) {
      if (!options.force && shouldHoldAgentRefresh()) {
        setAgentsStatus("Автообновление агентов приостановлено: есть несохраненные изменения. Нажмите «Сохранить агентов», чтобы продолжить обновление.", "ok");
        return;
      }
      const response = await fetch("/agents");
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.detail || "Ошибка загрузки агентов.");
      }
      pendingSpecializations = data.pending_specializations || {};
      renderAgents(data.agents || [], selectedAgentId);
      syncActorsFromAgents(data.agents || []);
      renderGitPhoneOptions();
      setAgentsStatus(`Показано агентов для выбранного Git context: ${agents.length} из ${(data.agents || []).length}. Файл: ${data.config_path}`);
    }

    async function saveAgents(successMessage = "") {
      const nextAgents = collectAgents();
      setAgentsStatus("Сохраняю...");
      const response = await fetch("/agents", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({agents: nextAgents})
      });
      const data = await response.json();
      if (!response.ok) {
        setAgentsStatus(data.detail || "Ошибка сохранения агентов.", "error");
        return;
      }
      pendingSpecializations = data.pending_specializations || pendingSpecializations;
      renderAgents(data.agents || [], selectedAgentId);
      syncActorsFromAgents(data.agents || []);
      renderGitPhoneOptions();
      clearAgentEditorDirty();
      setAgentsStatus(successMessage || `Сохранено агентов: ${(data.agents || []).length}.`, "ok");
    }

    function applyQueueDefaults() {
      const config = queueConfig[queueEl.value];
      renderActorOptions(senderEl, config.default_sender);
      renderActorOptions(receiverEl, config.default_receiver);
    }

    function renderQueueOptions(preferredQueue = null) {
      const names = queuesForContext(activeContext);
      const selected = preferredQueue && names.includes(preferredQueue) ? preferredQueue : names[0];
      queueEl.innerHTML = names.map((name) => {
        const config = queueConfig[name];
        const isSelected = name === selected ? " selected" : "";
        return `<option value="${escapeHtml(name)}"${isSelected}>${escapeHtml(config.route)} - ${escapeHtml(config.label)}</option>`;
      }).join("");
      applyQueueDefaults();
      contextSummaryEl.textContent = contextSummary[activeContext];
    }

    function setActiveContext(context, preferredQueue = null) {
      activeContext = context;
      document.querySelectorAll(".tab").forEach((button) => {
        button.classList.toggle("active", button.dataset.context === context);
      });
      renderQueueOptions(preferredQueue);
      setTemplate("task");
    }

    function templateFor(queueName, type) {
      if (queueName === "work") {
        if (type === "fail") {
          return `TO: Programmer
FROM: Analyst
STATUS: FAIL

CHECKED:
- 

PROBLEMS & STATISTICAL ANOMALIES:
- problem:
- page/url/endpoint:
- expected:
- actual:
- hardcoding risk:
- steps to reproduce / logs:

FIX REQUEST:
- 

RETEST AFTER FIX:
- `;
        }
        return `TO: Programmer
FROM: Analyst
STATUS: INITIAL_TASK

PROGRAM:
- 

REQUIREMENTS:
- 

INITIAL TASK:
- `;
      }
      if (queueName === "test") {
        return `TO: Analyst
FROM: Programmer
STATUS: READY_FOR_TEST

PROGRAM:
- 

REQUIREMENTS:
- 

URL:
- https://gp2admin.neuro.uni-luebeck.de/extractor/

CHANGES:
- 

LOCAL VERIFICATION:
- 

UC1.72 SERVER DEPLOYMENT AND VERIFICATION:
- 

TEST TASK FOR ANALYST:
- `;
      }
      if (queueName === "work-design") {
        if (type === "fail") {
          return `TO: Frontend
FROM: Designer
STATUS: FAIL

CHECKED:
- 

UI/UX PROBLEMS:
- problem:
- page/url:
- expected:
- actual:
- responsive/accessibility risk:
- steps to reproduce:

FIX REQUEST:
- 

RETEST AFTER FIX:
- `;
        }
        return `TO: Frontend
FROM: Designer
STATUS: INITIAL_UI_TASK

PRODUCT/UI:
- 

UX REQUIREMENTS:
- 

INITIAL TASK:
- `;
      }
      return `TO: Designer
FROM: Frontend
STATUS: READY_FOR_UI_TEST

PRODUCT/UI:
- 

UX REQUIREMENTS:
- 

URL:
- https://gp2admin.neuro.uni-luebeck.de/extractor/

CHANGES:
- 

LOCAL VERIFICATION:
- 

UI TEST TASK FOR DESIGNER:
- `;
    }

    function setTemplate(type) {
      let targetQueue = queueEl.value;
      if (type === "task" || type === "fail") {
        targetQueue = activeContext === "backend" ? "work" : "work-design";
      }
      if (type === "ready") {
        targetQueue = activeContext === "backend" ? "test" : "test-design";
      }
      renderQueueOptions(targetQueue);
      const statusByType = {
        task: activeContext === "backend" ? "INITIAL_TASK" : "INITIAL_UI_TASK",
        fail: "FAIL",
        ready: activeContext === "backend" ? "READY_FOR_TEST" : "READY_FOR_UI_TEST"
      };
      statusEl.value = statusByType[type] || "CUSTOM";
      messageEl.value = templateFor(targetQueue, type);
      setStatus("");
    }

    function extractStatus(message) {
      const match = String(message).match(/^STATUS:\\s*(.+)$/m);
      return match ? match[1].trim() : "NO_STATUS";
    }

    function extractSummary(message) {
      let insideGitContext = false;
      const lines = String(message)
        .split("\\n")
        .map((line) => line.trim())
        .filter(Boolean)
        .filter((line) => {
          if (/^\\[?GIT CONTEXT\\]?\\s*:?$/i.test(line)) {
            insideGitContext = true;
            return false;
          }
          if (insideGitContext && /^[-*]?\\s*(Project|Git context|Git address|Repository|Commit)\\s*:/i.test(line)) {
            return false;
          }
          insideGitContext = false;
          return !line.startsWith("TO:") && !line.startsWith("FROM:") && !line.startsWith("STATUS:");
        });
      return lines.slice(0, 2).join(" ");
    }

    function updateScheduleUi() {
      const mode = scheduleModeEl.value || "now";
      scheduleDelayFieldEl.hidden = mode !== "delay";
      if (mode === "delay") {
        scheduleHintEl.textContent = "Задание попадет в очередь через указанное число минут. Шаг: 5 минут.";
        return;
      }
      if (mode === "pass") {
        scheduleHintEl.textContent = "Задание попадет в очередь, когда в системе появится сообщение со строкой STATUS: PASS.";
        return;
      }
      scheduleHintEl.textContent = "Сообщение сразу попадет в выбранную очередь.";
    }

    function buildSchedulePayload() {
      const mode = scheduleModeEl.value || "now";
      if (mode === "delay") {
        const minutes = Number.parseInt(scheduleDelayMinutesEl.value, 10);
        if (!Number.isFinite(minutes) || minutes < 5) {
          throw new Error("Укажите задержку минимум 5 минут.");
        }
        if (minutes % 5 !== 0) {
          throw new Error("Задержка должна быть кратна 5 минутам.");
        }
        return {mode: "delay", delay_minutes: minutes};
      }
      if (mode === "pass") {
        return {mode: "pass", trigger_text: "STATUS: PASS"};
      }
      return {mode: "now"};
    }

    function selectedGitContext() {
      const selectedKey = historyGitContextEl.value || "";
      return gitContexts.find((context) => context.git_context_key === selectedKey)
        || phoneContextByPhone(activeQueuePhone())
        || currentGitContext
        || gitContexts[0]
        || null;
    }

    async function sendMessage() {
      let message = messageEl.value.trim();
      if (!message) {
        setStatus("Введите текст сообщения.", "error");
        return;
      }
      const phone = activeMappedQueuePhone();
      if (!phone) {
        setStatus(activeGitContextRequiredMessage(), "error");
        return;
      }
      const config = queueConfig[queueEl.value];
      const schedule = buildSchedulePayload();
      setStatus("Отправляю...");
      const response = await fetch("/ui/send", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({
          queue: queueEl.value,
          context: config.context,
          sender: senderEl.value,
          receiver: receiverEl.value,
          status: statusEl.value,
          phone,
          message,
          schedule
        })
      });
      const data = await response.json();
      if (!response.ok) {
        setStatus(data.detail || "Ошибка отправки.", "error");
        return;
      }
      if (data.status === "scheduled") {
        if (data.task && data.task.schedule_mode === "delay") {
          setStatus(`Запланировано на ${formatLocalDateTime(data.task.due_at)}. Пока не в очереди.`, "ok");
        } else {
          setStatus("Запланировано: будет поставлено в очередь после STATUS: PASS.", "ok");
        }
        await refresh();
        return;
      }
      setStatus(`Отправлено в ${data.queue}. Размер очереди: ${data.size}.`, "ok");
      await refresh();
    }

    function rememberMessageCursor() {
      messageCursor = {
        start: messageEl.selectionStart || 0,
        end: messageEl.selectionEnd || messageEl.selectionStart || 0
      };
    }

    function insertTextAtMessageCursor(text) {
      const start = Math.max(0, Math.min(messageCursor.start, messageEl.value.length));
      const end = Math.max(start, Math.min(messageCursor.end, messageEl.value.length));
      const before = messageEl.value.slice(0, start);
      const after = messageEl.value.slice(end);
      const prefix = before && !before.endsWith("\\n") ? "\\n" : "";
      const suffix = after && !text.endsWith("\\n") ? "\\n" : "";
      const inserted = `${prefix}${text}${suffix}`;
      messageEl.value = before + inserted + after;
      const nextCursor = before.length + inserted.length;
      messageEl.focus();
      messageEl.setSelectionRange(nextCursor, nextCursor);
      rememberMessageCursor();
    }

    function readFileAsBase64(file) {
      return new Promise((resolve, reject) => {
        const reader = new FileReader();
        reader.onload = () => {
          const result = String(reader.result || "");
          resolve(result.includes(",") ? result.split(",", 2)[1] : result);
        };
        reader.onerror = () => reject(new Error("Не удалось прочитать файл."));
        reader.readAsDataURL(file);
      });
    }

    function screenshotFilePreviewUrl(folderId, fileName) {
      return `/screenshot-folders/${encodeURIComponent(folderId)}/files/${encodeURIComponent(fileName)}/preview`;
    }

    function formatBytes(size) {
      const value = Number(size || 0);
      if (value >= 1024 * 1024) {
        return `${(value / (1024 * 1024)).toFixed(1)} MB`;
      }
      if (value >= 1024) {
        return `${(value / 1024).toFixed(1)} KB`;
      }
      return `${value} B`;
    }

    function formatLocalDateTime(value) {
      if (!value) {
        return "дата неизвестна";
      }
      return new Date(value).toLocaleString("ru-RU");
    }

    function screenshotFolderParams() {
      const params = new URLSearchParams();
      params.set("date_from", screenshotFoldersFromEl.value || todayInputValue());
      if (screenshotFoldersToEl.value) {
        params.set("date_to", screenshotFoldersToEl.value);
      }
      const phone = activeMappedQueuePhone();
      if (phone) {
        params.set("phone", phone);
      }
      return params;
    }

    async function copyTextToClipboard(text) {
      if (navigator.clipboard && window.isSecureContext) {
        await navigator.clipboard.writeText(text);
      } else {
        fallbackCopyText(text);
      }
    }

    function selectedScreenshotFolder() {
      return screenshotFolders.find((folder) => folder.id === screenshotFolderSelectEl.value) || null;
    }

    function renderScreenshotFolderSelect(preferredId = selectedScreenshotFolderId) {
      if (!screenshotFolders.length) {
        screenshotFolderSelectEl.innerHTML = `<option value="">Нет папок</option>`;
        screenshotFolderSelectEl.value = "";
        selectedScreenshotFolderId = "";
        return;
      }

      const selectedId = screenshotFolders.some((folder) => folder.id === preferredId)
        ? preferredId
        : screenshotFolders[0].id;
      screenshotFolderSelectEl.innerHTML = screenshotFolders.map((folder) => {
        const selected = folder.id === selectedId ? " selected" : "";
        return `<option value="${escapeHtml(folder.id)}"${selected}>${escapeHtml(folder.id)} · ${escapeHtml(folder.path)}</option>`;
      }).join("");
      screenshotFolderSelectEl.value = selectedId;
      selectedScreenshotFolderId = selectedId;
    }

    function renderScreenshotCopyTargetOptions(currentFolderId) {
      const targets = screenshotFolders.filter((folder) => folder.id !== currentFolderId);
      if (!targets.length) {
        return `<option value="">Нет другой папки</option>`;
      }
      return targets.map((folder) => `<option value="${escapeHtml(folder.id)}">${escapeHtml(folder.id)}</option>`).join("");
    }

    function renderScreenshotFileRow(folder, file) {
      const previewUrl = screenshotFilePreviewUrl(folder.id, file.name);
      return `<div class="screenshot-file-row" data-folder-id="${escapeHtml(folder.id)}" data-file-name="${escapeHtml(file.name)}" draggable="true">
        <img class="screenshot-thumbnail" src="${escapeHtml(previewUrl)}" alt="${escapeHtml(file.name)}" data-action="preview-screenshot-file" draggable="false">
        <div class="screenshot-file-name">
          <strong>${escapeHtml(file.name)}</strong>
          <div class="subtle">${escapeHtml(formatBytes(file.size))}</div>
          <div class="subtle">${escapeHtml(file.path)}</div>
        </div>
        <select data-role="copy-target">${renderScreenshotCopyTargetOptions(folder.id)}</select>
        <button class="secondary" data-action="preview-screenshot-file" type="button">Просмотр</button>
        <button class="secondary" data-action="copy-screenshot-path" type="button">Путь</button>
        <button class="secondary" data-action="copy-screenshot-file" type="button">Копировать</button>
        <button class="icon-button" data-action="delete-screenshot-file" type="button" title="Удалить" aria-label="Удалить">×</button>
      </div>`;
    }

    function renderScreenshotFolders() {
      renderScreenshotFolderSelect();
      const dateToText = screenshotFoldersToEl.value ? ` - ${screenshotFoldersToEl.value}` : "";
      screenshotFoldersCountEl.textContent = `${screenshotFolders.length} папок с ${screenshotFoldersFromEl.value || todayInputValue()}${dateToText}`;
      screenshotFoldersEl.innerHTML = screenshotFolders.map((folder) => {
        const isExpanded = expandedScreenshotFolderIds.has(folder.id);
        const filesHtml = folder.files && folder.files.length
          ? `<div class="screenshot-files">${folder.files.map((file) => renderScreenshotFileRow(folder, file)).join("")}</div>`
          : `<div class="subtle screenshot-empty">В папке пока нет скриншотов.</div>`;
        return `<article class="screenshot-folder-card" data-folder-id="${escapeHtml(folder.id)}">
          <div class="screenshot-folder-head">
            <div>
              <div class="screenshot-folder-title">${escapeHtml(folder.id)}</div>
              <div class="subtle">${escapeHtml(folder.file_count || 0)} файлов · создано ${escapeHtml(formatLocalDateTime(folder.created_at))}</div>
            </div>
            <div class="actions" style="margin-top: 0;">
              <button class="secondary" data-action="toggle-screenshot-folder" data-folder-id="${escapeHtml(folder.id)}" type="button">${isExpanded ? "Свернуть" : "Развернуть"}</button>
              <button class="secondary" data-action="copy-screenshot-folder-path" data-folder-id="${escapeHtml(folder.id)}" type="button">Скопировать путь</button>
            </div>
          </div>
          <div class="screenshot-path-row">
            <code class="screenshot-path-value">${escapeHtml(folder.path)}</code>
            <button class="secondary" data-action="select-screenshot-folder" data-folder-id="${escapeHtml(folder.id)}" type="button">Выбрать</button>
          </div>
          <div class="screenshot-folder-files" ${isExpanded ? "" : "hidden"}>${filesHtml}</div>
        </article>`;
      }).join("") || `<div class="panel subtle">Папки со скриншотами пока не созданы.</div>`;
    }

    async function refreshScreenshotFolders(preferredId = selectedScreenshotFolderId) {
      if (!activeMappedQueuePhone()) {
        screenshotFolders = [];
        selectedScreenshotFolderId = "";
        renderScreenshotFolders();
        setScreenshotFoldersFilterStatus(activeGitContextRequiredMessage());
        return;
      }
      const response = await fetch(`/screenshot-folders?${screenshotFolderParams().toString()}`);
      const data = await response.json();
      if (!response.ok) {
        setScreenshotFoldersFilterStatus(data.detail || "Не удалось загрузить папки.", "error");
        return;
      }
      screenshotFolders = data.folders || [];
      selectedScreenshotFolderId = preferredId;
      renderScreenshotFolders();
      setScreenshotFoldersFilterStatus("");
    }

    async function createScreenshotFolder() {
      setScreenshotFoldersStatus("Создаю папку...");
      const phone = activeMappedQueuePhone();
      if (!phone) {
        setScreenshotFoldersStatus(activeGitContextRequiredMessage(), "error");
        return;
      }
      const params = new URLSearchParams();
      params.set("phone", phone);
      const response = await fetch(`/screenshot-folders${params.toString() ? "?" + params.toString() : ""}`, {method: "POST"});
      const data = await response.json();
      if (!response.ok) {
        setScreenshotFoldersStatus(data.detail || "Не удалось создать папку.", "error");
        return;
      }
      selectedScreenshotFolderId = data.folder.id;
      setScreenshotFoldersStatus(`Создана папка: ${data.folder.path}`, "ok");
      setScreenshotUploadStatus("");
      await refreshScreenshotFolders(data.folder.id);
      refreshAttachmentFolderChoices(attachmentChoiceValue("screenshots", data.folder.id))
        .catch((error) => setAttachmentStatus(error.message, "error"));
    }

    async function uploadScreenshotFiles(fileList, folderId = screenshotFolderSelectEl.value) {
      const folder = folderById(folderId) || selectedScreenshotFolder();
      const files = Array.from(fileList || []).filter((file) => {
        return file && (
          String(file.type || "").startsWith("image/")
          || /\\.(png|jpe?g|webp|gif|bmp|tiff?|avif)$/i.test(file.name || "")
        );
      });
      if (!folder) {
        setScreenshotUploadStatus("Сначала создайте или выберите папку.", "error");
        return;
      }
      if (!files.length) {
        setScreenshotUploadStatus("Выберите один или несколько скриншотов.", "error");
        return;
      }

      setScreenshotUploadStatus(`Загружаю файлов: ${files.length}...`);
      for (const file of files) {
        if (file.size > 25 * 1024 * 1024) {
          setScreenshotUploadStatus(`Файл ${file.name} больше 25 MB.`, "error");
          return;
        }
        const contentBase64 = await readFileAsBase64(file);
        const response = await fetch(`/screenshot-folders/${encodeURIComponent(folder.id)}/files`, {
          method: "POST",
          headers: {"Content-Type": "application/json"},
          body: JSON.stringify({
            filename: file.name,
            content_base64: contentBase64
          })
        });
        const data = await response.json();
        if (!response.ok) {
          setScreenshotUploadStatus(data.detail || `Не удалось загрузить ${file.name}.`, "error");
          return;
        }
      }

      await refreshScreenshotFolders(folder.id);
      setScreenshotUploadStatus(`Загружено файлов: ${files.length}. Папка: ${folder.path}`, "ok");
    }

    async function uploadScreenshots() {
      await uploadScreenshotFiles(screenshotFilesEl.files || []);
      screenshotFilesEl.value = "";
    }

    async function uploadDroppedScreenshots(fileList, folderId = screenshotFolderSelectEl.value) {
      const folder = folderById(folderId) || selectedScreenshotFolder();
      const files = Array.from(fileList || []);
      if (!folder) {
        setScreenshotUploadStatus("Сначала создайте или выберите папку.", "error");
        return;
      }
      if (!files.length) {
        setScreenshotUploadStatus("В drop нет файлов.", "error");
        return;
      }
      if (!window.confirm(`Загрузить ${files.length} файлов в ${folder.id}?`)) {
        setScreenshotUploadStatus("Загрузка отменена.");
        return;
      }
      selectedScreenshotFolderId = folder.id;
      screenshotFolderSelectEl.value = folder.id;
      await uploadScreenshotFiles(files, folder.id);
    }

    function clipboardImageExtension(mimeType) {
      const normalized = String(mimeType || "").toLowerCase();
      if (normalized.includes("jpeg")) {
        return "jpg";
      }
      if (normalized.includes("webp")) {
        return "webp";
      }
      if (normalized.includes("gif")) {
        return "gif";
      }
      if (normalized.includes("bmp")) {
        return "bmp";
      }
      if (normalized.includes("avif")) {
        return "avif";
      }
      return "png";
    }

    function clipboardScreenshotName(mimeType, index) {
      const stamp = new Date().toISOString().replace(/\\D/g, "").slice(0, 14);
      return `paste_${stamp}_${String(index + 1).padStart(2, "0")}.${clipboardImageExtension(mimeType)}`;
    }

    function normalizeClipboardScreenshotFiles(fileList) {
      return Array.from(fileList || [])
        .filter((file) => file && String(file.type || "").startsWith("image/"))
        .map((file, index) => {
          const hasImageExtension = /\\.(png|jpe?g|webp|gif|bmp|tiff?|avif)$/i.test(file.name || "");
          if (file.name && hasImageExtension) {
            return file;
          }
          return new File([file], clipboardScreenshotName(file.type, index), {type: file.type || "image/png"});
        });
    }

    async function uploadPastedScreenshots(files, folderId = screenshotFolderSelectEl.value) {
      const folder = folderById(folderId) || selectedScreenshotFolder();
      const screenshotFiles = normalizeClipboardScreenshotFiles(files);
      if (!folder) {
        setScreenshotUploadStatus("Сначала создайте или выберите папку.", "error");
        return;
      }
      if (!screenshotFiles.length) {
        setScreenshotUploadStatus("В буфере нет изображения.", "error");
        return;
      }
      if (!window.confirm(`Вставить ${screenshotFiles.length} скриншотов в ${folder.id}?`)) {
        setScreenshotUploadStatus("Вставка из буфера отменена.");
        return;
      }
      selectedScreenshotFolderId = folder.id;
      screenshotFolderSelectEl.value = folder.id;
      await uploadScreenshotFiles(screenshotFiles, folder.id);
    }

    async function pasteScreenshotsFromClipboard() {
      if (!navigator.clipboard || !navigator.clipboard.read) {
        setScreenshotUploadStatus("Браузер не дал доступ к чтению буфера. Используйте Ctrl+V на вкладке скриншотов.", "error");
        return;
      }
      const clipboardItems = await navigator.clipboard.read();
      const files = [];
      for (const item of clipboardItems) {
        for (const type of item.types || []) {
          if (!String(type).startsWith("image/")) {
            continue;
          }
          const blob = await item.getType(type);
          files.push(new File([blob], clipboardScreenshotName(type, files.length), {type}));
        }
      }
      await uploadPastedScreenshots(files);
    }

    function dragEventHasFiles(event) {
      return Array.from((event.dataTransfer && event.dataTransfer.types) || []).includes("Files");
    }

    function dragEventHasScreenshot(event) {
      return draggedScreenshot
        || dragCandidateScreenshot
        || Array.from((event.dataTransfer && event.dataTransfer.types) || []).includes("application/x-screenshot-file");
    }

    async function copySelectedScreenshotFolderPath() {
      const folder = selectedScreenshotFolder();
      if (!folder) {
        setScreenshotUploadStatus("Папка не выбрана.", "error");
        return;
      }
      await copyTextToClipboard(folder.path);
      setScreenshotUploadStatus(`Путь скопирован: ${folder.path}`, "ok");
    }

    function folderById(folderId) {
      return screenshotFolders.find((folder) => folder.id === folderId) || null;
    }

    function fileByFolderAndName(folderId, fileName) {
      const folder = folderById(folderId);
      return folder ? (folder.files || []).find((file) => file.name === fileName) || null : null;
    }

    function openScreenshotPreview(folderId, fileName) {
      const folder = folderById(folderId);
      const file = fileByFolderAndName(folderId, fileName);
      if (!folder || !file) {
        setScreenshotFoldersStatus("Скриншот не найден в текущем списке.", "error");
        return;
      }
      screenshotPreviewImageEl.src = screenshotFilePreviewUrl(folderId, fileName);
      screenshotPreviewMetaEl.textContent = `${folder.id} · ${file.name} · ${file.path}`;
      screenshotPreviewModalEl.hidden = false;
      document.getElementById("closeScreenshotPreviewButton").focus();
    }

    function closeScreenshotPreview() {
      screenshotPreviewModalEl.hidden = true;
      screenshotPreviewImageEl.removeAttribute("src");
      screenshotPreviewMetaEl.textContent = "";
    }

    async function deleteScreenshotFile(folderId, fileName) {
      setScreenshotFoldersStatus(`Удаляю ${fileName}...`);
      const response = await fetch(`/screenshot-folders/${encodeURIComponent(folderId)}/files/${encodeURIComponent(fileName)}`, {
        method: "DELETE"
      });
      const data = await response.json();
      if (!response.ok) {
        setScreenshotFoldersStatus(data.detail || "Не удалось удалить скриншот.", "error");
        return;
      }
      await refreshScreenshotFolders(folderId);
      setScreenshotFoldersStatus(`Скриншот удален: ${fileName}`, "ok");
    }

    async function copyScreenshotFile(folderId, fileName, targetFolderId) {
      if (!targetFolderId) {
        setScreenshotFoldersStatus("Выберите папку назначения для копирования.", "error");
        return;
      }
      setScreenshotFoldersStatus(`Копирую ${fileName}...`);
      const response = await fetch(`/screenshot-folders/${encodeURIComponent(folderId)}/files/${encodeURIComponent(fileName)}/copy`, {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({target_folder_id: targetFolderId})
      });
      const data = await response.json();
      if (!response.ok) {
        setScreenshotFoldersStatus(data.detail || "Не удалось скопировать скриншот.", "error");
        return;
      }
      await refreshScreenshotFolders(targetFolderId);
      setScreenshotFoldersStatus(`Скриншот скопирован в ${targetFolderId}: ${data.file.path}`, "ok");
    }

    function screenshotTrashKey(folderId, fileName) {
      return `${folderId}/${fileName}`;
    }

    function renderScreenshotTrash() {
      screenshotTrashItemsEl.innerHTML = screenshotTrash.map((item) => {
        return `<div class="screenshot-trash-item" data-trash-key="${escapeHtml(screenshotTrashKey(item.folderId, item.fileName))}">
          <div>
            <strong>${escapeHtml(item.fileName)}</strong>
            <div class="subtle">${escapeHtml(item.folderId)} · ${escapeHtml(item.path || "")}</div>
          </div>
          <button class="icon-button" data-action="remove-from-screenshot-trash" data-folder-id="${escapeHtml(item.folderId)}" data-file-name="${escapeHtml(item.fileName)}" type="button" title="Убрать из корзины" aria-label="Убрать из корзины">×</button>
        </div>`;
      }).join("") || `<div class="subtle">Корзина пустая.</div>`;
    }

    function addScreenshotToTrash(folderId, fileName) {
      const file = fileByFolderAndName(folderId, fileName);
      if (!file) {
        setScreenshotTrashStatus("Скриншот не найден в текущем списке.", "error");
        return;
      }
      const key = screenshotTrashKey(folderId, fileName);
      if (screenshotTrash.some((item) => screenshotTrashKey(item.folderId, item.fileName) === key)) {
        setScreenshotTrashStatus("Этот скриншот уже в корзине.", "error");
        return;
      }
      if (!window.confirm(`Переместить в корзину ${fileName}?`)) {
        setScreenshotTrashStatus("Перемещение в корзину отменено.");
        return;
      }
      screenshotTrash.push({
        folderId,
        fileName,
        path: file.path
      });
      renderScreenshotTrash();
      setScreenshotTrashStatus(`В корзине: ${screenshotTrash.length}.`, "ok");
    }

    function removeScreenshotFromTrash(folderId, fileName) {
      const key = screenshotTrashKey(folderId, fileName);
      screenshotTrash = screenshotTrash.filter((item) => screenshotTrashKey(item.folderId, item.fileName) !== key);
      renderScreenshotTrash();
      setScreenshotTrashStatus(`В корзине: ${screenshotTrash.length}.`);
    }

    async function clearScreenshotTrash() {
      if (!screenshotTrash.length) {
        setScreenshotTrashStatus("Корзина пустая.", "error");
        return;
      }
      if (!window.confirm(`Окончательно удалить ${screenshotTrash.length} файлов из корзины?`)) {
        setScreenshotTrashStatus("Очистка корзины отменена.");
        return;
      }

      const itemsToDelete = screenshotTrash.slice();
      const failed = [];
      setScreenshotTrashStatus(`Удаляю файлов: ${itemsToDelete.length}...`);
      for (const item of itemsToDelete) {
        const response = await fetch(`/screenshot-folders/${encodeURIComponent(item.folderId)}/files/${encodeURIComponent(item.fileName)}`, {
          method: "DELETE"
        });
        if (!response.ok) {
          const data = await response.json();
          failed.push(`${item.fileName}: ${data.detail || response.status}`);
          continue;
        }
        removeScreenshotFromTrash(item.folderId, item.fileName);
      }

      await refreshScreenshotFolders(selectedScreenshotFolderId);
      if (failed.length) {
        setScreenshotTrashStatus(`Не удалены: ${failed.join("; ")}`, "error");
        return;
      }
      setScreenshotTrashStatus("Корзина очищена.", "ok");
    }

    function evidenceFilePreviewUrl(folderId, fileName) {
      return `/evidence-folders/${encodeURIComponent(folderId)}/files/${encodeURIComponent(fileName)}/preview`;
    }

    function evidenceFolderParams() {
      const params = new URLSearchParams();
      params.set("date_from", evidenceFoldersFromEl.value || todayInputValue());
      if (evidenceFoldersToEl.value) {
        params.set("date_to", evidenceFoldersToEl.value);
      }
      const phone = activeMappedQueuePhone();
      if (phone) {
        params.set("phone", phone);
      }
      return params;
    }

    function selectedEvidenceFolder() {
      return evidenceFolders.find((folder) => folder.id === evidenceFolderSelectEl.value) || null;
    }

    function renderEvidenceFolderSelect(preferredId = selectedEvidenceFolderId) {
      if (!evidenceFolders.length) {
        evidenceFolderSelectEl.innerHTML = `<option value="">Нет папок</option>`;
        evidenceFolderSelectEl.value = "";
        selectedEvidenceFolderId = "";
        return;
      }

      const selectedId = evidenceFolders.some((folder) => folder.id === preferredId)
        ? preferredId
        : evidenceFolders[0].id;
      evidenceFolderSelectEl.innerHTML = evidenceFolders.map((folder) => {
        const selected = folder.id === selectedId ? " selected" : "";
        return `<option value="${escapeHtml(folder.id)}"${selected}>${escapeHtml(folder.id)} · ${escapeHtml(folder.path)}</option>`;
      }).join("");
      evidenceFolderSelectEl.value = selectedId;
      selectedEvidenceFolderId = selectedId;
    }

    function renderEvidenceCopyTargetOptions(currentFolderId) {
      const targets = evidenceFolders.filter((folder) => folder.id !== currentFolderId);
      if (!targets.length) {
        return `<option value="">Нет другой папки</option>`;
      }
      return targets.map((folder) => `<option value="${escapeHtml(folder.id)}">${escapeHtml(folder.id)}</option>`).join("");
    }

    function evidenceFileBadge(file) {
      const extension = String(file.extension || "").replace(".", "") || "file";
      return extension.slice(0, 6);
    }

    function renderEvidencePreviewCell(folder, file) {
      const previewUrl = evidenceFilePreviewUrl(folder.id, file.name);
      if (file.is_image) {
        return `<img class="evidence-thumbnail" src="${escapeHtml(previewUrl)}" alt="${escapeHtml(file.name)}" data-action="preview-evidence-file" draggable="false">`;
      }
      return `<div class="evidence-file-tile" data-action="preview-evidence-file" role="button" tabindex="0" title="Открыть файл">${escapeHtml(evidenceFileBadge(file))}</div>`;
    }

    function renderEvidenceFileRow(folder, file) {
      return `<div class="evidence-file-row" data-folder-id="${escapeHtml(folder.id)}" data-file-name="${escapeHtml(file.name)}" draggable="true">
        ${renderEvidencePreviewCell(folder, file)}
        <div class="evidence-file-name">
          <strong>${escapeHtml(file.name)}</strong>
          <div class="subtle">${escapeHtml(formatBytes(file.size))}</div>
          <div class="subtle">${escapeHtml(file.path)}</div>
        </div>
        <select data-role="copy-target">${renderEvidenceCopyTargetOptions(folder.id)}</select>
        <button class="secondary" data-action="preview-evidence-file" type="button">Открыть</button>
        <button class="secondary" data-action="copy-evidence-path" type="button">Путь</button>
        <button class="secondary" data-action="copy-evidence-file" type="button">Копировать</button>
        <button class="icon-button" data-action="delete-evidence-file" type="button" title="Удалить" aria-label="Удалить">×</button>
      </div>`;
    }

    function renderEvidenceFolders() {
      renderEvidenceFolderSelect();
      const dateToText = evidenceFoldersToEl.value ? ` - ${evidenceFoldersToEl.value}` : "";
      evidenceFoldersCountEl.textContent = `${evidenceFolders.length} папок с ${evidenceFoldersFromEl.value || todayInputValue()}${dateToText}`;
      evidenceFoldersEl.innerHTML = evidenceFolders.map((folder) => {
        const isExpanded = expandedEvidenceFolderIds.has(folder.id);
        const filesHtml = folder.files && folder.files.length
          ? `<div class="evidence-files">${folder.files.map((file) => renderEvidenceFileRow(folder, file)).join("")}</div>`
          : `<div class="subtle evidence-empty">В папке пока нет файлов.</div>`;
        return `<article class="evidence-folder-card" data-folder-id="${escapeHtml(folder.id)}">
          <div class="evidence-folder-head">
            <div>
              <div class="evidence-folder-title">${escapeHtml(folder.id)}</div>
              <div class="subtle">${escapeHtml(folder.file_count || 0)} файлов · создано ${escapeHtml(formatLocalDateTime(folder.created_at))}</div>
            </div>
            <div class="actions" style="margin-top: 0;">
              <button class="secondary" data-action="toggle-evidence-folder" data-folder-id="${escapeHtml(folder.id)}" type="button">${isExpanded ? "Свернуть" : "Развернуть"}</button>
              <button class="secondary" data-action="copy-evidence-folder-path" data-folder-id="${escapeHtml(folder.id)}" type="button">Скопировать путь</button>
            </div>
          </div>
          <div class="evidence-path-row">
            <code class="evidence-path-value">${escapeHtml(folder.path)}</code>
            <button class="secondary" data-action="select-evidence-folder" data-folder-id="${escapeHtml(folder.id)}" type="button">Выбрать</button>
          </div>
          <div class="evidence-folder-files" ${isExpanded ? "" : "hidden"}>${filesHtml}</div>
        </article>`;
      }).join("") || `<div class="panel subtle">Папки с доказательствами пока не созданы.</div>`;
    }

    async function refreshEvidenceFolders(preferredId = selectedEvidenceFolderId) {
      if (!activeMappedQueuePhone()) {
        evidenceFolders = [];
        selectedEvidenceFolderId = "";
        renderEvidenceFolders();
        setEvidenceFoldersFilterStatus(activeGitContextRequiredMessage());
        return;
      }
      const response = await fetch(`/evidence-folders?${evidenceFolderParams().toString()}`);
      const data = await response.json();
      if (!response.ok) {
        setEvidenceFoldersFilterStatus(data.detail || "Не удалось загрузить папки.", "error");
        return;
      }
      evidenceFolders = data.folders || [];
      selectedEvidenceFolderId = preferredId;
      renderEvidenceFolders();
      setEvidenceFoldersFilterStatus("");
    }

    async function createEvidenceFolder() {
      setEvidenceFoldersStatus("Создаю папку...");
      const phone = activeMappedQueuePhone();
      if (!phone) {
        setEvidenceFoldersStatus(activeGitContextRequiredMessage(), "error");
        return;
      }
      const params = new URLSearchParams();
      params.set("phone", phone);
      const response = await fetch(`/evidence-folders${params.toString() ? "?" + params.toString() : ""}`, {method: "POST"});
      const data = await response.json();
      if (!response.ok) {
        setEvidenceFoldersStatus(data.detail || "Не удалось создать папку.", "error");
        return;
      }
      selectedEvidenceFolderId = data.folder.id;
      setEvidenceFoldersStatus(`Создана папка: ${data.folder.path}`, "ok");
      setEvidenceUploadStatus("");
      await refreshEvidenceFolders(data.folder.id);
      refreshAttachmentFolderChoices(attachmentChoiceValue("evidence", data.folder.id))
        .catch((error) => setAttachmentStatus(error.message, "error"));
    }

    async function uploadEvidenceFiles(fileList, folderId = evidenceFolderSelectEl.value) {
      const folder = evidenceFolderById(folderId) || selectedEvidenceFolder();
      const files = Array.from(fileList || []).filter(Boolean);
      if (!folder) {
        setEvidenceUploadStatus("Сначала создайте или выберите папку.", "error");
        return;
      }
      if (!files.length) {
        setEvidenceUploadStatus("Выберите один или несколько файлов.", "error");
        return;
      }

      setEvidenceUploadStatus(`Загружаю файлов: ${files.length}...`);
      for (const file of files) {
        if (file.size > 100 * 1024 * 1024) {
          setEvidenceUploadStatus(`Файл ${file.name} больше 100 MB.`, "error");
          return;
        }
        const contentBase64 = await readFileAsBase64(file);
        const response = await fetch(`/evidence-folders/${encodeURIComponent(folder.id)}/files`, {
          method: "POST",
          headers: {"Content-Type": "application/json"},
          body: JSON.stringify({
            filename: file.name,
            content_base64: contentBase64
          })
        });
        const data = await response.json();
        if (!response.ok) {
          setEvidenceUploadStatus(data.detail || `Не удалось загрузить ${file.name}.`, "error");
          return;
        }
      }

      await refreshEvidenceFolders(folder.id);
      setEvidenceUploadStatus(`Загружено файлов: ${files.length}. Папка: ${folder.path}`, "ok");
    }

    async function uploadEvidence() {
      await uploadEvidenceFiles(evidenceFilesEl.files || []);
      evidenceFilesEl.value = "";
    }

    async function uploadDroppedEvidence(fileList, folderId = evidenceFolderSelectEl.value) {
      const folder = evidenceFolderById(folderId) || selectedEvidenceFolder();
      const files = Array.from(fileList || []);
      if (!folder) {
        setEvidenceUploadStatus("Сначала создайте или выберите папку.", "error");
        return;
      }
      if (!files.length) {
        setEvidenceUploadStatus("В drop нет файлов.", "error");
        return;
      }
      if (!window.confirm(`Загрузить ${files.length} файлов в ${folder.id}?`)) {
        setEvidenceUploadStatus("Загрузка отменена.");
        return;
      }
      selectedEvidenceFolderId = folder.id;
      evidenceFolderSelectEl.value = folder.id;
      await uploadEvidenceFiles(files, folder.id);
    }

    function dragEventHasEvidence(event) {
      return draggedEvidence
        || dragCandidateEvidence
        || Array.from((event.dataTransfer && event.dataTransfer.types) || []).includes("application/x-evidence-file");
    }

    async function copySelectedEvidenceFolderPath() {
      const folder = selectedEvidenceFolder();
      if (!folder) {
        setEvidenceUploadStatus("Папка не выбрана.", "error");
        return;
      }
      await copyTextToClipboard(folder.path);
      setEvidenceUploadStatus(`Путь скопирован: ${folder.path}`, "ok");
    }

    function evidenceFolderById(folderId) {
      return evidenceFolders.find((folder) => folder.id === folderId) || null;
    }

    function evidenceFileByFolderAndName(folderId, fileName) {
      const folder = evidenceFolderById(folderId);
      return folder ? (folder.files || []).find((file) => file.name === fileName) || null : null;
    }

    function openEvidencePreview(folderId, fileName) {
      const folder = evidenceFolderById(folderId);
      const file = evidenceFileByFolderAndName(folderId, fileName);
      if (!folder || !file) {
        setEvidenceFoldersStatus("Файл не найден в текущем списке.", "error");
        return;
      }

      const previewUrl = evidenceFilePreviewUrl(folderId, fileName);
      evidencePreviewMetaEl.textContent = `${folder.id} · ${file.name} · ${file.path}`;
      if (file.is_image) {
        evidencePreviewFrameEl.innerHTML = `<img src="${escapeHtml(previewUrl)}" alt="${escapeHtml(file.name)}">`;
      } else {
        evidencePreviewFrameEl.innerHTML = `<div class="actions" style="margin-top: 0;">
          <a class="secondary" href="${escapeHtml(previewUrl)}" target="_blank" rel="noopener">Открыть в новой вкладке</a>
        </div>
        <iframe src="${escapeHtml(previewUrl)}" title="${escapeHtml(file.name)}"></iframe>`;
      }
      evidencePreviewModalEl.hidden = false;
      document.getElementById("closeEvidencePreviewButton").focus();
    }

    function closeEvidencePreview() {
      evidencePreviewModalEl.hidden = true;
      evidencePreviewFrameEl.innerHTML = "";
      evidencePreviewMetaEl.textContent = "";
    }

    async function deleteEvidenceFile(folderId, fileName) {
      setEvidenceFoldersStatus(`Удаляю ${fileName}...`);
      const response = await fetch(`/evidence-folders/${encodeURIComponent(folderId)}/files/${encodeURIComponent(fileName)}`, {
        method: "DELETE"
      });
      const data = await response.json();
      if (!response.ok) {
        setEvidenceFoldersStatus(data.detail || "Не удалось удалить файл.", "error");
        return;
      }
      await refreshEvidenceFolders(folderId);
      setEvidenceFoldersStatus(`Файл удален: ${fileName}`, "ok");
    }

    async function copyEvidenceFile(folderId, fileName, targetFolderId) {
      if (!targetFolderId) {
        setEvidenceFoldersStatus("Выберите папку назначения для копирования.", "error");
        return;
      }
      setEvidenceFoldersStatus(`Копирую ${fileName}...`);
      const response = await fetch(`/evidence-folders/${encodeURIComponent(folderId)}/files/${encodeURIComponent(fileName)}/copy`, {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({target_folder_id: targetFolderId})
      });
      const data = await response.json();
      if (!response.ok) {
        setEvidenceFoldersStatus(data.detail || "Не удалось скопировать файл.", "error");
        return;
      }
      await refreshEvidenceFolders(targetFolderId);
      setEvidenceFoldersStatus(`Файл скопирован в ${targetFolderId}: ${data.file.path}`, "ok");
    }

    function evidenceTrashKey(folderId, fileName) {
      return `${folderId}/${fileName}`;
    }

    function renderEvidenceTrash() {
      evidenceTrashItemsEl.innerHTML = evidenceTrash.map((item) => {
        return `<div class="evidence-trash-item" data-trash-key="${escapeHtml(evidenceTrashKey(item.folderId, item.fileName))}">
          <div>
            <strong>${escapeHtml(item.fileName)}</strong>
            <div class="subtle">${escapeHtml(item.folderId)} · ${escapeHtml(item.path || "")}</div>
          </div>
          <button class="icon-button" data-action="remove-from-evidence-trash" data-folder-id="${escapeHtml(item.folderId)}" data-file-name="${escapeHtml(item.fileName)}" type="button" title="Убрать из корзины" aria-label="Убрать из корзины">×</button>
        </div>`;
      }).join("") || `<div class="subtle">Корзина пустая.</div>`;
    }

    function addEvidenceToTrash(folderId, fileName) {
      const file = evidenceFileByFolderAndName(folderId, fileName);
      if (!file) {
        setEvidenceTrashStatus("Файл не найден в текущем списке.", "error");
        return;
      }
      const key = evidenceTrashKey(folderId, fileName);
      if (evidenceTrash.some((item) => evidenceTrashKey(item.folderId, item.fileName) === key)) {
        setEvidenceTrashStatus("Этот файл уже в корзине.", "error");
        return;
      }
      if (!window.confirm(`Переместить в корзину ${fileName}?`)) {
        setEvidenceTrashStatus("Перемещение в корзину отменено.");
        return;
      }
      evidenceTrash.push({
        folderId,
        fileName,
        path: file.path
      });
      renderEvidenceTrash();
      setEvidenceTrashStatus(`В корзине: ${evidenceTrash.length}.`, "ok");
    }

    function removeEvidenceFromTrash(folderId, fileName) {
      const key = evidenceTrashKey(folderId, fileName);
      evidenceTrash = evidenceTrash.filter((item) => evidenceTrashKey(item.folderId, item.fileName) !== key);
      renderEvidenceTrash();
      setEvidenceTrashStatus(`В корзине: ${evidenceTrash.length}.`);
    }

    async function clearEvidenceTrash() {
      if (!evidenceTrash.length) {
        setEvidenceTrashStatus("Корзина пустая.", "error");
        return;
      }
      if (!window.confirm(`Окончательно удалить ${evidenceTrash.length} файлов из корзины?`)) {
        setEvidenceTrashStatus("Очистка корзины отменена.");
        return;
      }

      const itemsToDelete = evidenceTrash.slice();
      const failed = [];
      setEvidenceTrashStatus(`Удаляю файлов: ${itemsToDelete.length}...`);
      for (const item of itemsToDelete) {
        const response = await fetch(`/evidence-folders/${encodeURIComponent(item.folderId)}/files/${encodeURIComponent(item.fileName)}`, {
          method: "DELETE"
        });
        if (!response.ok) {
          const data = await response.json();
          failed.push(`${item.fileName}: ${data.detail || response.status}`);
          continue;
        }
        removeEvidenceFromTrash(item.folderId, item.fileName);
      }

      await refreshEvidenceFolders(selectedEvidenceFolderId);
      if (failed.length) {
        setEvidenceTrashStatus(`Не удалены: ${failed.join("; ")}`, "error");
        return;
      }
      setEvidenceTrashStatus("Корзина очищена.", "ok");
    }

    function attachmentDefaultMessage(type) {
      return type === "evidence" ? "Смотри файлы в папке" : "См. скриншоты в папке.";
    }

    function attachmentChoiceValue(type, folderId) {
      return `${type}:${folderId}`;
    }

    function selectedAttachmentFolderChoice() {
      return attachmentFolderChoices.find((choice) => choice.value === attachmentFolderSelectEl.value) || null;
    }

    function renderAttachmentFolderOptions(preferredValue = attachmentFolderSelectEl.value) {
      if (!attachmentFolderChoices.length) {
        attachmentFolderSelectEl.innerHTML = `<option value="">Нет папок</option>`;
        attachmentFolderSelectEl.value = "";
        attachmentDescriptionEl.value = attachmentDefaultMessage("screenshots");
        return;
      }

      const selectedValue = attachmentFolderChoices.some((choice) => choice.value === preferredValue)
        ? preferredValue
        : attachmentFolderChoices[0].value;
      const screenshotOptions = attachmentFolderChoices
        .filter((choice) => choice.type === "screenshots")
        .map((choice) => `<option value="${escapeHtml(choice.value)}">${escapeHtml(choice.label)}</option>`)
        .join("");
      const evidenceOptions = attachmentFolderChoices
        .filter((choice) => choice.type === "evidence")
        .map((choice) => `<option value="${escapeHtml(choice.value)}">${escapeHtml(choice.label)}</option>`)
        .join("");

      attachmentFolderSelectEl.innerHTML = [
        screenshotOptions ? `<optgroup label="Скриншоты">${screenshotOptions}</optgroup>` : "",
        evidenceOptions ? `<optgroup label="Доказательства">${evidenceOptions}</optgroup>` : ""
      ].join("");
      attachmentFolderSelectEl.value = selectedValue;
      updateAttachmentDescriptionForSelection();
    }

    function updateAttachmentDescriptionForSelection() {
      const choice = selectedAttachmentFolderChoice();
      const nextDefault = attachmentDefaultMessage(choice ? choice.type : "screenshots");
      const currentText = attachmentDescriptionEl.value.trim();
      const knownDefaults = [attachmentDefaultMessage("screenshots"), attachmentDefaultMessage("evidence")];
      if (!currentText || knownDefaults.includes(currentText)) {
        attachmentDescriptionEl.value = nextDefault;
      }
    }

    async function fetchFolderChoices(endpoint, type, label) {
      const params = new URLSearchParams();
      const phone = activeMappedQueuePhone();
      if (!phone) {
        return [];
      }
      params.set("phone", phone);
      const response = await fetch(`${endpoint}${params.toString() ? "?" + params.toString() : ""}`);
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.detail || `Не удалось загрузить папки: ${label}.`);
      }
      return (data.folders || []).map((folder) => ({
        type,
        id: folder.id,
        value: attachmentChoiceValue(type, folder.id),
        label: `${folder.id} · ${folder.path}`,
        path: folder.path
      }));
    }

    async function refreshAttachmentFolderChoices(preferredValue = attachmentFolderSelectEl.value) {
      setAttachmentStatus("Загружаю папки...");
      if (!activeMappedQueuePhone()) {
        attachmentFolderChoices = [];
        renderAttachmentFolderOptions("");
        setAttachmentStatus(activeGitContextRequiredMessage());
        return;
      }
      const [screenshotChoices, evidenceChoices] = await Promise.all([
        fetchFolderChoices("/screenshot-folders", "screenshots", "скриншоты"),
        fetchFolderChoices("/evidence-folders", "evidence", "доказательства")
      ]);
      attachmentFolderChoices = [...screenshotChoices, ...evidenceChoices];
      renderAttachmentFolderOptions(preferredValue);
      setAttachmentStatus(`Доступно папок: ${attachmentFolderChoices.length}.`, "ok");
    }

    async function copyAttachmentFolderPath() {
      const choice = selectedAttachmentFolderChoice();
      if (!choice) {
        setAttachmentStatus("Выберите папку.", "error");
        return;
      }
      await copyTextToClipboard(choice.path);
      setAttachmentStatus(`Путь скопирован: ${choice.path}`, "ok");
    }

    function attachmentFolderMessage() {
      const choice = selectedAttachmentFolderChoice();
      if (!choice) {
        return null;
      }
      const message = attachmentDescriptionEl.value.trim() || attachmentDefaultMessage(choice.type);
      return `${message} : ${choice.path}`;
    }

    async function copyAttachmentFolderMessage() {
      const text = attachmentFolderMessage();
      if (!text) {
        setAttachmentStatus("Выберите папку.", "error");
        return;
      }
      await copyTextToClipboard(text);
      setAttachmentStatus(`Сообщение скопировано: ${text}`, "ok");
    }

    async function sendPhoneMessage() {
      let message = phoneMessageEl.value.trim();
      const conversationPhone = conversationPhoneEl.value.trim();
      const sender = agentById(phoneSenderEl.value);
      const receiver = agentById(phoneReceiverEl.value);
      if (!conversationPhone) {
        setPhoneStatus("Введите номер разговора.", "error");
        return;
      }
      if (!sender || !sender.phone) {
        setPhoneStatus("У отправителя нет телефона.", "error");
        return;
      }
      if (!receiver || !receiver.phone) {
        setPhoneStatus("У получателя нет телефона.", "error");
        return;
      }
      if (!message) {
        setPhoneStatus("Введите текст сообщения.", "error");
        return;
      }

      const channelPathByQueue = {
        "worker-all": "worker/all",
        "tester-all": "tester/all",
        "consultant-all": "consultant/all"
      };
      const channelPath = channelPathByQueue[phoneChannelEl.value] || "worker/all";
      setPhoneStatus("Отправляю по телефону...");
      const response = await fetch(`/${channelPath}/${encodeURIComponent(conversationPhone)}`, {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({
          submitted_via: "ui_phone_pair",
          sender: sender.name,
          receiver: receiver.name,
          from_phone: sender.phone,
          to_phone: receiver.phone,
          message
        })
      });
      const data = await response.json();
      if (!response.ok) {
        setPhoneStatus(data.detail || "Ошибка отправки по телефону.", "error");
        return;
      }
      setPhoneStatus(`Отправлено в ${data.queue}: ${sender.phone} -> ${receiver.phone}. Размер очереди: ${data.size}.`, "ok");
      await refresh();
    }

    function consultantQuestionMessage(sender, expert, question) {
      return `TO: ${expert.name}
FROM: ${sender.name}
STATUS: QUESTION

QUESTION:
${question}`;
    }

    async function askConsultant() {
      const conversationPhone = consultantConversationEl.value.trim();
      const sender = agentById(consultantSenderEl.value);
      const expert = agentById(consultantExpertEl.value);
      const question = consultantQuestionEl.value.trim();
      if (!conversationPhone) {
        setConsultantStatus("Введите номер разговора.", "error");
        return;
      }
      if (!sender || !sender.phone) {
        setConsultantStatus("У спрашивающего нет телефона.", "error");
        return;
      }
      if (!expert || !expert.phone) {
        setConsultantStatus("У консультанта нет телефона.", "error");
        return;
      }
      if (!question) {
        setConsultantStatus("Введите вопрос консультанту.", "error");
        return;
      }

      setConsultantStatus("Отправляю вопрос консультанту...");
      const response = await fetch(`/consultant/all/${encodeURIComponent(conversationPhone)}`, {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({
          submitted_via: "ui_consultant_window",
          sender: sender.name,
          receiver: expert.name,
          from_phone: sender.phone,
          to_phone: expert.phone,
          status: "QUESTION",
          message: consultantQuestionMessage(sender, expert, question)
        })
      });
      const data = await response.json();
      if (!response.ok) {
        setConsultantStatus(data.detail || "Ошибка отправки консультанту.", "error");
        return;
      }
      setConsultantStatus(`Вопрос отправлен: ${sender.phone} -> ${expert.phone}. Размер очереди: ${data.size}.`, "ok");
      await refreshQueues();
    }

    async function checkConsultantReply() {
      const conversationPhone = consultantConversationEl.value.trim();
      const sender = agentById(consultantSenderEl.value);
      if (!conversationPhone) {
        setConsultantStatus("Введите номер разговора.", "error");
        return;
      }
      if (!sender || !sender.phone) {
        setConsultantStatus("У спрашивающего нет телефона.", "error");
        return;
      }

      setConsultantStatus("Проверяю ответ консультанта...");
      const response = await fetch(`/consultant/all/${encodeURIComponent(conversationPhone)}?to_phone=${encodeURIComponent(sender.phone)}`);
      const data = await response.json();
      if (!response.ok) {
        setConsultantStatus(data.detail || "Ответа для этого телефона пока нет.", response.status === 404 ? "" : "error");
        return;
      }
      consultantReplyEl.value = formatMessage(data.message);
      setConsultantStatus(`Ответ получен от ${data.from_phone || "unknown"} для ${data.to_phone}.`, "ok");
      await refreshQueues();
    }

    async function refreshQueues() {
      const phone = activeMappedQueuePhone();
      if (!phone) {
        activeQueueItemIds = new Set();
        queueStatsEl.innerHTML = Object.entries(queueConfig).map(([name, config]) => {
          return `<div class="queue-stat ${escapeHtml(config.context)}">
            <span class="subtle">${escapeHtml(config.route)} · ${escapeHtml(config.label)}</span>
            <strong>0</strong>
          </div>`;
        }).join("");
        setStatus(activeGitContextRequiredMessage());
        return;
      }
      const params = new URLSearchParams();
      params.set("phone", phone);
      const response = await fetch(`/queues${params.toString() ? "?" + params.toString() : ""}`);
      const data = await response.json();
      if (!response.ok) {
        setStatus(data.detail || "Не удалось загрузить очереди.", "error");
        return;
      }
      activeQueueItemIds = new Set();
      Object.entries(data.items || {}).forEach(([queueName, items]) => {
        (items || []).forEach((item) => {
          if (item.id) {
            activeQueueItemIds.add(`${queueName}:${item.id}`);
          }
        });
      });
      queueStatsEl.innerHTML = Object.entries(queueConfig).map(([name, config]) => {
        const size = data.queues[name] ?? 0;
        const items = (data.items && data.items[name]) || [];
        const itemHtml = items.length
          ? `<div class="queue-items">${items.map((item) => `
            <div class="queue-item">
              <div>
                ${item.metadata && item.metadata.project_name ? `<div class="queue-item-phone">${escapeHtml(item.metadata.project_name)}${item.metadata.git_commit_short ? ` · ${escapeHtml(item.metadata.git_commit_short)}` : ""}</div>` : ""}
                ${item.metadata && item.metadata.conversation_phone ? `<div class="queue-item-phone">call ${escapeHtml(item.metadata.conversation_phone)} · ${escapeHtml(item.metadata.from_phone || "?")} -> ${escapeHtml(item.metadata.to_phone || "?")}</div>` : ""}
                <div class="queue-item-text">${escapeHtml(item.summary || item.id || "message")}</div>
              </div>
              ${item.id ? `<button class="icon-button" data-action="delete-queued-message" data-queue="${escapeHtml(name)}" data-queue-item-id="${escapeHtml(item.id)}" type="button" title="Удалить из очереди" aria-label="Удалить из очереди">×</button>` : ""}
            </div>`).join("")}</div>`
          : "";
        return `<div class="queue-stat ${escapeHtml(config.context)}">
          <span class="subtle">${escapeHtml(config.route)} · ${escapeHtml(config.label)}</span>
          <strong>${escapeHtml(size)}</strong>
          ${itemHtml}
        </div>`;
      }).join("");
    }

    function scheduledModeLabel(task) {
      if (task.schedule_mode === "delay") {
        return `через ${task.delay_minutes} мин.`;
      }
      if (task.schedule_mode === "pass") {
        return "после STATUS: PASS";
      }
      return task.schedule_mode || "ожидание";
    }

    async function refreshScheduledTasks() {
      const phone = activeMappedQueuePhone();
      if (!phone) {
        scheduledTasksCountEl.textContent = "Git context не выбран";
        scheduledTasksEl.innerHTML = `<div class="subtle">${escapeHtml(activeGitContextRequiredMessage())}</div>`;
        setScheduledTasksStatus("");
        return;
      }
      const params = new URLSearchParams();
      params.set("phone", phone);
      const response = await fetch(`/scheduled-tasks${params.toString() ? "?" + params.toString() : ""}`);
      const data = await response.json();
      if (!response.ok) {
        setScheduledTasksStatus(data.detail || "Не удалось загрузить ожидающие задания.", "error");
        return;
      }
      const tasks = data.tasks || [];
      scheduledTasksCountEl.textContent = tasks.length ? `${tasks.length} ожидает` : "нет ожидающих";
      scheduledTasksEl.innerHTML = tasks.map((task) => {
        const meta = task.metadata || {};
        const context = task.context || "unknown";
        const actor = [meta.sender, meta.receiver].filter(Boolean).join(" -> ") || task.direction || task.route || task.queue;
        const targetTime = task.schedule_mode === "delay"
          ? `Поставить: ${formatLocalDateTime(task.due_at)}`
          : `Условие: ${escapeHtml(task.trigger_text || "STATUS: PASS")}`;
        return `<div class="scheduled-card">
          <div class="scheduled-card-head">
            <div>
              <div class="entry-title">
                <span class="dot ${escapeHtml(context)}"></span>
                <span class="badge ${escapeHtml(context)}">${escapeHtml(context)}</span>
                ${meta.project_name ? `<span class="badge">${escapeHtml(meta.project_name)}</span>` : ""}
                <span class="badge">${escapeHtml(task.route || task.queue)}</span>
                <span class="badge event">${escapeHtml(scheduledModeLabel(task))}</span>
              </div>
              <div class="scheduled-summary"><strong>${escapeHtml(actor)}:</strong> ${escapeHtml(task.summary || "")}</div>
              <div class="subtle">${targetTime} · создано ${escapeHtml(formatLocalDateTime(task.created_at))}</div>
            </div>
            <button class="icon-button" data-action="cancel-scheduled-task" data-task-id="${escapeHtml(task.id)}" type="button" title="Отменить ожидание" aria-label="Отменить ожидание">×</button>
          </div>
        </div>`;
      }).join("") || `<div class="subtle">Ожидающих заданий нет.</div>`;
      setScheduledTasksStatus("");
    }

    async function cancelScheduledTask(taskId) {
      if (!window.confirm("Отменить это ожидающее задание?")) {
        setScheduledTasksStatus("Отмена не выполнена.");
        return;
      }
      setScheduledTasksStatus("Отменяю ожидающее задание...");
      const response = await fetch(`/scheduled-tasks/${encodeURIComponent(taskId)}`, {
        method: "DELETE"
      });
      const data = await response.json();
      if (!response.ok) {
        setScheduledTasksStatus(data.detail || "Ожидающее задание не найдено.", "error");
        return;
      }
      setScheduledTasksStatus("Ожидающее задание отменено.", "ok");
      await Promise.all([refreshScheduledTasks(), refreshHistory()]);
    }

    async function deleteQueuedMessage(queueName, itemId) {
      setStatus("Удаляю сообщение из очереди...");
      const phone = activeMappedQueuePhone();
      const url = phone
        ? `/queues/${encodeURIComponent(queueName)}/${encodeURIComponent(phone)}/${encodeURIComponent(itemId)}`
        : `/queues/${encodeURIComponent(queueName)}/${encodeURIComponent(itemId)}`;
      const response = await fetch(url, {
        method: "DELETE"
      });
      const data = await response.json();
      if (!response.ok) {
        setStatus(data.detail || "Сообщение уже не найдено в очереди.", "error");
        return;
      }
      setStatus(`Сообщение удалено из ${data.queue}. Осталось: ${data.size}.`, "ok");
      await Promise.all([refreshQueues(), refreshHistory()]);
    }

    async function refreshGitConfig() {
      const response = await fetch("/git-config");
      const data = await response.json();
      phoneGitContexts = data.phone_contexts || [];
      gitContexts = data.contexts || [];
      phoneGitContexts = phoneGitContexts.map((context) => {
        const sameGitContextKey = context.git_context_key && context.git_context_key === data.git_context_key;
        const sameGitAddress = context.git_address && data.git_address && context.git_address === data.git_address;
        if (sameGitContextKey || sameGitAddress) {
          return Object.assign({}, context, {
            git_commit: data.git_commit || context.git_commit || "",
            git_commit_short: data.git_commit_short || context.git_commit_short || "",
            git_error: data.git_error || context.git_error || ""
          });
        }
        return context;
      });
      let newlyAssignedPhone = "";
      const previouslyUnmappedContext = projectContextByKey(activeUnmappedGitContextKey);
      if (previouslyUnmappedContext) {
        const contextPhones = Array.isArray(previouslyUnmappedContext.phones)
          ? previouslyUnmappedContext.phones.map((phone) => String(phone || "").trim()).filter(Boolean)
          : [];
        newlyAssignedPhone = contextPhones.find((phone) => phoneContextByPhone(phone)) || "";
        if (newlyAssignedPhone) {
          activeUnmappedGitContextKey = "";
        }
      } else if (activeUnmappedGitContextKey) {
        activeUnmappedGitContextKey = "";
      }
      const activePhone = newlyAssignedPhone || activeQueuePhone();
      const activePhoneHasContext = Boolean(phoneContextByPhone(activePhone));
      const editingGitContext = [gitPhoneEl, gitNewPhoneEl, gitAddressEl, gitProjectNameEl, gitContextKeyEl].includes(document.activeElement);
      const preferredPhone = activeUnmappedGitContextKey
        ? ""
        : activePhone && (activePhoneHasContext || editingGitContext || !phoneGitContexts.length)
          ? activePhone
          : (phoneGitContexts[0] && phoneGitContexts[0].phone) || activePhone || "";
      renderGitPhoneOptions(preferredPhone);
      const activeContext = activeProjectContext();
      const displayGitAddress = (activeContext && activeContext.git_address) || data.git_address || "";
      const displayProjectName = (activeContext && activeContext.project_name) || data.project_name || "";
      const displayGitContextKey = (activeContext && activeContext.git_context_key) || data.git_context_key || "";
      if (document.activeElement !== gitAddressEl) {
        gitAddressEl.value = displayGitAddress;
      }
      if (document.activeElement !== gitProjectNameEl) {
        gitProjectNameEl.value = displayProjectName;
      }
      if (document.activeElement !== gitContextKeyEl) {
        gitContextKeyEl.value = displayGitContextKey;
      }
      currentGitContext = activeContext
        || gitContexts.find((context) => context.is_current_port)
        || (data.git_context_key ? {
          project_name: data.project_name || "LLM Extractor",
          git_context_key: data.git_context_key,
          git_address: data.git_address || "",
          git_commit: data.git_commit || "",
          git_commit_short: data.git_commit_short || "",
          is_current_port: true
        } : null);
      renderGitContextOptions(historyGitContextEl.value || (currentGitContext && currentGitContext.git_context_key) || "");
      updateActiveGitContextDisplay();
      renderGitContextProjectList();
      commitValueEl.textContent = data.git_commit_short || "не задан";
      if (activeUnmappedGitContextKey && activeContext) {
        setGitStatus(`Проект ${activeContext.project_name || activeContext.git_context_key} зарегистрирован без phone.`, "ok");
      } else if (activeContext && activeMappedQueuePhone()) {
        setGitStatus(`Телефон ${activeMappedQueuePhone()}: ${activeContext.project_name || "project"} · ${activeContext.git_context_key}`, "ok");
      } else if (data.git_commit_short) {
        setGitStatus(`Порт ${data.port}: ${data.project_name || "project"} · commit ${data.git_commit_short}. Выберите телефон и сохраните привязку.`, "ok");
      } else if (data.git_error) {
        setGitStatus(`Порт ${data.port}: ${data.git_error}`, "error");
      } else {
        setGitStatus(`Порт ${data.port}: Git address не задан. Файл привязки: ${data.config_path}`);
      }
    }

    function renderGitContextOptions(preferredKey = "") {
      const activePhone = activeQueuePhone();
      const activePhoneContext = phoneContextByPhone(activePhone);
      const selectedKey = activeUnmappedGitContextKey && projectContextByKey(activeUnmappedGitContextKey)
        ? activeUnmappedGitContextKey
        : activePhone && !activePhoneContext
        ? ""
        : gitContexts.some((context) => context.git_context_key === preferredKey)
          ? preferredKey
          : (activePhoneContext && activePhoneContext.git_context_key)
            || (currentGitContext && currentGitContext.git_context_key)
            || (gitContexts[0] && gitContexts[0].git_context_key)
            || "";
      const emptyOption = selectedKey ? "" : `<option value="">Git context не выбран</option>`;
      historyGitContextEl.innerHTML = gitContexts.length
        ? emptyOption + gitContexts.map((context) => {
          const selected = context.git_context_key === selectedKey ? " selected" : "";
          const portText = context.ports && context.ports.length ? ` · port ${context.ports.join(", ")}` : "";
          const phoneText = context.phones && context.phones.length ? ` · phone ${context.phones.join(", ")}` : "";
          const label = `${context.project_name || "Project"} · ${context.git_context_key}${phoneText}${portText}`;
          return `<option value="${escapeHtml(context.git_context_key)}"${selected}>${escapeHtml(label)}</option>`;
        }).join("")
        : `<option value="">Git context не задан</option>`;
      historyGitContextEl.value = selectedKey;
    }

    async function saveGitConfig() {
      const gitAddress = gitAddressEl.value.trim();
      const projectName = gitProjectNameEl.value.trim();
      const gitContextKey = gitContextKeyEl.value.trim();
      const phone = activeQueuePhone();
      if (!phone) {
        setGitStatus("Выберите телефон для Git context.", "error");
        return;
      }
      if (!gitAddress) {
        setGitStatus("Введите Git address.", "error");
        return;
      }
      setGitStatus("Сохраняю...");
      const response = await fetch("/git-config", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({phone, git_address: gitAddress, project_name: projectName, git_context_key: gitContextKey})
      });
      const data = await response.json();
      if (!response.ok) {
        setGitStatus(data.detail || "Ошибка сохранения.", "error");
        return;
      }
      commitValueEl.textContent = data.git_commit_short || "не задан";
      gitContexts = data.contexts || [];
      phoneGitContexts = data.phone_contexts || [];
      phoneGitContexts = phoneGitContexts.map((context) => {
        if (context.phone === phone) {
          return Object.assign({}, context, {
            git_commit: data.git_commit || context.git_commit || "",
            git_commit_short: data.git_commit_short || context.git_commit_short || "",
            git_error: data.git_error || context.git_error || ""
          });
        }
        return context;
      });
      activeUnmappedGitContextKey = "";
      pendingGitContextKey = "";
      renderGitPhoneOptions(phone);
      currentGitContext = phoneContextByPhone(phone) || gitContexts.find((context) => context.is_current_port) || currentGitContext;
      renderGitContextOptions(data.git_context_key || (currentGitContext && currentGitContext.git_context_key) || "");
      if (data.git_commit_short) {
        setGitStatus(`Сохранено для телефона ${phone}. Commit: ${data.git_commit_short}`, "ok");
      } else if (data.git_error) {
        setGitStatus(`Сохранено для порта ${data.port}, но commit не получен: ${data.git_error}`, "error");
      } else {
        setGitStatus(`Сохранено для порта ${data.port}.`, "ok");
      }
      await Promise.all([
        refreshQueues(),
        refreshScheduledTasks(),
        refreshHistory(),
        refreshAttachmentFolderChoices(),
        refreshScreenshotFolders(),
        refreshEvidenceFolders()
      ]);
    }

    function addGitContext() {
      const phone = gitNewPhoneEl.value.trim();
      if (!phone) {
        setGitStatus("Введите новый номер Git context.", "error");
        return;
      }
      const draftContext = activeProjectContext();
      const draftGitAddress = gitAddressEl.value;
      const draftProjectName = gitProjectNameEl.value;
      const draftGitContextKey = gitContextKeyEl.value;
      activeUnmappedGitContextKey = "";
      renderGitPhoneOptions(phone);
      gitPhoneEl.value = phone;
      const existing = phoneContextByPhone(phone);
      if (existing) {
        gitAddressEl.value = existing.git_address || "";
        gitProjectNameEl.value = existing.project_name || "";
        gitContextKeyEl.value = existing.git_context_key || "";
        currentGitContext = existing;
        renderGitContextOptions(existing.git_context_key || "");
        setGitStatus(`Номер ${phone} уже сохранен. Можно изменить Git address и сохранить заново.`, "ok");
      } else {
        gitAddressEl.value = draftGitAddress;
        gitProjectNameEl.value = draftProjectName;
        gitContextKeyEl.value = draftGitContextKey;
        currentGitContext = draftContext;
        renderGitContextOptions(draftGitContextKey);
        const instruction = draftGitAddress
          ? "проверьте данные и сохраните привязку"
          : "заполните Git address и сохраните привязку";
        setGitStatus(`Новый Git context ${phone}: ${instruction}.`);
      }
      gitNewPhoneEl.value = "";
      gitAddressEl.focus();
    }

    async function deleteSelectedGitContext() {
      const phone = activeQueuePhone();
      if (!phone) {
        setGitStatus("Выберите номер Git context.", "error");
        return;
      }
      const existing = phoneContextByPhone(phone);
      if (!existing) {
        setGitStatus(`Номер ${phone} еще не сохранен как Git context. Удалять нечего.`, "error");
        return;
      }
      if (!window.confirm(`Удалить Git context для номера ${phone}? История сохранится.`)) {
        setGitStatus("Удаление отменено.");
        return;
      }

      setGitStatus("Удаляю Git context...");
      const response = await fetch(`/git-config/phone/${encodeURIComponent(phone)}`, {
        method: "DELETE"
      });
      const data = await response.json();
      if (!response.ok) {
        setGitStatus(data.detail || "Не удалось удалить Git context.", "error");
        return;
      }
      phoneGitContexts = data.phone_contexts || [];
      gitContexts = data.contexts || [];
      activeUnmappedGitContextKey = "";
      pendingGitContextKey = "";
      const nextPhone = (phoneGitContexts[0] && phoneGitContexts[0].phone) || "";
      renderGitPhoneOptions(nextPhone);
      const nextContext = phoneContextByPhone(activeQueuePhone());
      gitAddressEl.value = nextContext ? nextContext.git_address || "" : "";
      gitProjectNameEl.value = nextContext ? nextContext.project_name || "" : "";
      gitContextKeyEl.value = nextContext ? nextContext.git_context_key || "" : "";
      currentGitContext = nextContext || null;
      renderGitContextOptions(nextContext ? nextContext.git_context_key || "" : "");
      setGitStatus(`Git context для номера ${phone} удален.`, "ok");
      await Promise.all([
        refreshQueues(),
        refreshScheduledTasks(),
        refreshHistory(),
        refreshAttachmentFolderChoices(),
        refreshScreenshotFolders(),
        refreshEvidenceFolders()
      ]);
    }

    function historyParams(limit) {
      const params = new URLSearchParams();
      params.set("limit", String(limit));
      params.set("date_from", historyFromEl.value || todayInputValue());
      if (historyGitContextEl.value) {
        params.set("git_context", historyGitContextEl.value);
      }
      if (historyToEl.value) {
        params.set("date_to", historyToEl.value);
      }
      return params;
    }

    async function fetchHistoryRecords(limit) {
      const response = await fetch(`/history?${historyParams(limit).toString()}`);
      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.detail || "Ошибка загрузки истории.");
      }
      return await response.json();
    }

    function formatRecordForClipboard(record) {
      const meta = record.metadata || {};
      const timestamp = new Date(record.timestamp).toLocaleString("ru-RU");
      const revision = historyRecordRevision(record);
      const project = meta.project_name || "no-project";
      const actor = [meta.sender, meta.receiver].filter(Boolean).join(" -> ") || meta.direction || record.route || record.queue;
      const message = formatHistoryRecordMessage(record);
      const eventLabel = meta.cycle_event_type || record.event;
      return `[${timestamp}] [Project: ${project}] [${revision.label}: ${revision.value}] [${actor}] [${eventLabel}]
${message}`;
    }

    function fallbackCopyText(text) {
      const textarea = document.createElement("textarea");
      textarea.value = text;
      textarea.setAttribute("readonly", "");
      textarea.style.position = "fixed";
      textarea.style.left = "-9999px";
      document.body.appendChild(textarea);
      textarea.select();
      const copied = document.execCommand("copy");
      document.body.removeChild(textarea);
      if (!copied) {
        throw new Error("Clipboard недоступен.");
      }
    }

    async function copyHistoryToClipboard() {
      setHistoryStatus("Готовлю текст для копирования...");
      const records = await fetchHistoryRecords(10000);
      if (!records.length) {
        setHistoryStatus("За выбранный период сообщений нет.", "error");
        return;
      }
      const text = records.map(formatRecordForClipboard).join("\\n\\n---\\n\\n");
      if (navigator.clipboard && window.isSecureContext) {
        await navigator.clipboard.writeText(text);
      } else {
        fallbackCopyText(text);
      }
      setHistoryStatus(`Скопировано сообщений: ${records.length}.`, "ok");
    }

    function recordCommitInfo(record) {
      const meta = record.metadata || {};
      const full = String(meta.git_commit || meta.git_commit_short || "").trim();
      return {
        full,
        short: String(meta.git_commit_short || (full ? full.slice(0, 12) : "")).trim()
      };
    }

    function commitLabel(commitInfo) {
      return commitInfo.short || (commitInfo.full ? commitInfo.full.slice(0, 12) : "no-commit");
    }

    function sameCommit(left, right) {
      if (!left || !right || !left.full || !right.full) {
        return false;
      }
      return left.full.toLowerCase() === right.full.toLowerCase();
    }

    function patchRequestKey(previousCommit, currentCommit, gitAddress) {
      return [
        gitAddress,
        previousCommit.full.toLowerCase(),
        currentCommit.full.toLowerCase()
      ].join("|");
    }

    function patchBlockHeader(previousCommit, currentCommit, gitAddress, patchData = null) {
      const lines = [
        "[PATCH BETWEEN COMMITS]",
        `From: ${commitLabel(previousCommit)}`,
        `To: ${commitLabel(currentCommit)}`
      ];
      if (gitAddress) {
        lines.push(`Repository: ${gitAddress}`);
      }
      if (patchData && patchData.source) {
        lines.push(`Source: ${patchData.source}`);
      }
      if (patchData && patchData.patch_url) {
        lines.push(`Patch URL: ${patchData.patch_url}`);
      }
      return lines.join("\\n");
    }

    function patchUnavailableBlock(previousCommit, currentCommit, gitAddress, reason) {
      return `${patchBlockHeader(previousCommit, currentCommit, gitAddress)}
Status: unavailable
Reason: ${reason || "Patch could not be loaded"}
[END PATCH]`;
    }

    async function fetchPatchBetweenCommits(previousCommit, currentCommit, gitAddress) {
      if (!gitAddress) {
        return patchUnavailableBlock(previousCommit, currentCommit, gitAddress, "Git address is missing in Git context.");
      }
      const params = new URLSearchParams();
      params.set("git_address", gitAddress);
      params.set("from_commit", previousCommit.full);
      params.set("to_commit", currentCommit.full);
      const response = await fetch(`/git-patch?${params.toString()}`);
      const data = await response.json();
      if (!response.ok) {
        return patchUnavailableBlock(previousCommit, currentCommit, gitAddress, data.detail || `HTTP ${response.status}`);
      }
      return `${patchBlockHeader(previousCommit, currentCommit, gitAddress, data)}

${data.patch || ""}
[END PATCH]`;
    }

    async function historyWithPatchesText(records, gitAddress) {
      const sections = [];
      const usedPatchKeys = new Set();
      let previousCommit = null;
      let patchCount = 0;
      let patchErrorCount = 0;

      for (const record of records) {
        const currentCommit = recordCommitInfo(record);
        if (previousCommit && currentCommit.full && !sameCommit(previousCommit, currentCommit)) {
          const key = patchRequestKey(previousCommit, currentCommit, gitAddress);
          if (!usedPatchKeys.has(key)) {
            usedPatchKeys.add(key);
            const patchBlock = await fetchPatchBetweenCommits(previousCommit, currentCommit, gitAddress);
            if (patchBlock.includes("Status: unavailable")) {
              patchErrorCount += 1;
            } else {
              patchCount += 1;
            }
            sections.push(patchBlock);
          }
        }

        sections.push(formatRecordForClipboard(record));
        if (currentCommit.full) {
          previousCommit = currentCommit;
        }
      }

      return {
        text: sections.join("\\n\\n---\\n\\n"),
        patchCount,
        patchErrorCount
      };
    }

    async function copyHistoryWithPatchesToClipboard() {
      setHistoryStatus("Готовлю историю и патчи между commit...");
      const activeProject = selectedGitContext();
      const gitAddress = (activeProject && activeProject.git_address) || gitAddressEl.value.trim();
      if (!gitAddress) {
        setHistoryStatus("Введите Git address в блоке Git context. Он нужен, чтобы получить patch между commit.", "error");
        return;
      }
      const records = await fetchHistoryRecords(10000);
      if (!records.length) {
        setHistoryStatus("За выбранный период сообщений нет.", "error");
        return;
      }
      const result = await historyWithPatchesText(records, gitAddress);
      await copyTextToClipboard(result.text);
      const patchText = result.patchErrorCount
        ? `Патчей: ${result.patchCount}, не удалось получить: ${result.patchErrorCount}.`
        : `Патчей: ${result.patchCount}.`;
      setHistoryStatus(`Скопировано сообщений: ${records.length}. ${patchText}`, result.patchErrorCount ? "error" : "ok");
    }

    async function copySingleHistoryRecord(recordId) {
      const record = currentHistoryRecords.find((item) => item.id === recordId);
      if (!record) {
        setHistoryStatus("Сообщение не найдено в текущей выборке.", "error");
        return;
      }
      const text = formatRecordForClipboard(record);
      if (navigator.clipboard && window.isSecureContext) {
        await navigator.clipboard.writeText(text);
      } else {
        fallbackCopyText(text);
      }
      setHistoryStatus("Сообщение скопировано.", "ok");
    }

    async function deleteRemovedBackendHistoryRecord(recordId) {
      setHistoryStatus("Стираю запись из истории...");
      const response = await fetch(`/history/${encodeURIComponent(recordId)}`, {
        method: "DELETE"
      });
      const data = await response.json();
      if (!response.ok) {
        setHistoryStatus(data.detail || "Запись нельзя удалить из истории.", "error");
        return;
      }
      setHistoryStatus("Запись removed_from_backend_queue стерта из истории.", "ok");
      await refreshHistory();
    }

    async function restoreRemovedBackendHistoryRecord(recordId) {
      setHistoryStatus("Возвращаю сообщение в очередь...");
      const phone = activeMappedQueuePhone();
      if (!phone) {
        setHistoryStatus(activeGitContextRequiredMessage(), "error");
        return;
      }
      const url = `/history/${encodeURIComponent(recordId)}/restore-to-queue/${encodeURIComponent(phone)}`;
      const response = await fetch(url, {
        method: "POST"
      });
      const data = await response.json();
      if (!response.ok) {
        setHistoryStatus(data.detail || "Сообщение нельзя вернуть в очередь.", "error");
        return;
      }
      setHistoryStatus(`Сообщение возвращено в ${data.queue}. Размер очереди: ${data.size}.`, "ok");
      await refresh();
    }

    function cycleGraphViewIsActive() {
      return Boolean(cycleGraphViewEl && cycleGraphViewEl.classList.contains("active"));
    }

    function setCycleGraphStatus(text, state = "") {
      cycleGraphStatusEl.textContent = text;
      cycleGraphStatusEl.className = "status" + (state ? " " + state : "");
    }

    function cycleGraphProjectPhone(context = activeProjectContext()) {
      return String((context && context.project_phone) || "").trim();
    }

    function cycleGraphStatusLabel(statusValue) {
      const labels = {
        queued: "в очереди",
        in_progress: "в работе",
        completed: "завершён"
      };
      const value = String(statusValue || "").trim();
      return labels[value] || value || "неизвестно";
    }

    function cycleGraphRoleLabel(roleValue) {
      const value = String(roleValue || "").trim();
      const labels = {
        "Project Manager": "Project Manager",
        project_manager: "Project Manager",
        system_analyst: "Системный аналитик",
        backend_developer: "Backend-разработчик",
        frontend_developer: "Frontend-разработчик",
        ux_designer: "UX/UI дизайнер",
        qa_engineer: "QA-инженер",
        project_coordinator: "Координатор"
      };
      return labels[value] || value.replaceAll("_", " ") || "Агент";
    }

    function cycleGraphEventLabel(eventTypeValue) {
      const value = String(eventTypeValue || "").trim();
      const labels = {
        CYCLE_STARTED: "Цикл открыт",
        GROUP_DEPLOYED: "Группа подключена",
        MESSAGE_QUEUED: "Задача поставлена в очередь",
        TASK_STARTED: "Агент начал задачу",
        HANDOFF_TRIGGERED: "Задача передана дальше",
        ARTIFACT_CREATED: "Артефакт создан",
        GROUP_REPORT_SUBMITTED: "Отчёт группы отправлен",
        CYCLE_COMPLETED: "Цикл завершён",
        MESSAGE_REMOVED: "Задача удалена из очереди"
      };
      return labels[value] || value.replaceAll("_", " ") || "Событие";
    }

    function cycleGraphFormatDate(value) {
      if (!value) {
        return "";
      }
      const parsed = new Date(value);
      return Number.isNaN(parsed.getTime()) ? String(value) : parsed.toLocaleString("ru-RU");
    }

    function cycleGraphShortId(value, maxLength = 24) {
      const text = String(value || "");
      if (text.length <= maxLength) {
        return text;
      }
      return text.slice(0, Math.max(6, maxLength - 1)) + "…";
    }

    function cycleGraphCountLabel(value, one, few, many) {
      const count = Number(value || 0);
      const lastTwo = Math.abs(count) % 100;
      const last = lastTwo % 10;
      const form = lastTwo >= 11 && lastTwo <= 14
        ? many
        : (last === 1 ? one : (last >= 2 && last <= 4 ? few : many));
      return `${count} ${form}`;
    }

    async function cycleGraphFetchJson(url, signal) {
      const response = await fetch(url, {signal});
      let data = null;
      try {
        data = await response.json();
      } catch (_error) {
        data = null;
      }
      if (!response.ok) {
        const detail = data && data.detail;
        throw new Error(typeof detail === "string" ? detail : `HTTP ${response.status}`);
      }
      return data || {};
    }

    function resetCycleGraphDetail(message) {
      cycleGraphTitleEl.textContent = "Граф группы";
      cycleGraphSubtitleEl.textContent = "Выберите цикл, чтобы увидеть движение задач.";
      cycleGraphMetricsEl.innerHTML = "";
      cycleGraphCanvasEl.innerHTML = `<div class="cycle-graph-empty">${escapeHtml(message)}</div>`;
      cycleGraphTaskLineageEl.textContent = "";
      cycleGraphEventCountEl.textContent = "";
      cycleGraphHistoryEl.innerHTML = `<div class="subtle">Событий пока нет.</div>`;
    }

    function renderCycleGraphCycleList() {
      const selectedId = cycleGraphSelectedCycleId;
      cycleGraphCycleSelectEl.disabled = !cycleGraphCycles.length;
      cycleGraphCycleSelectEl.innerHTML = cycleGraphCycles.length
        ? cycleGraphCycles.map((cycle) => {
          const cycleId = String(cycle.cycle_id || "");
          const title = cycle.title || cycleId;
          return `<option value="${escapeHtml(cycleId)}"${cycleId === selectedId ? " selected" : ""}>${escapeHtml(title)} · ${escapeHtml(cycleGraphStatusLabel(cycle.status))}</option>`;
        }).join("")
        : `<option value="">Нет доступных циклов</option>`;
      cycleGraphCycleListEl.innerHTML = cycleGraphCycles.map((cycle) => {
        const cycleId = String(cycle.cycle_id || "");
        const statusValue = String(cycle.status || "queued");
        const statusClass = ["queued", "in_progress", "completed"].includes(statusValue) ? statusValue : "queued";
        return `<button class="cycle-list-card${cycleId === selectedId ? " active" : ""}" data-cycle-id="${escapeHtml(cycleId)}" type="button"${cycleId === selectedId ? ' aria-current="true"' : ""}>
          <span class="cycle-list-card-head">
            <span class="cycle-list-card-title">${escapeHtml(cycle.title || cycleId)}</span>
            <span class="cycle-status-chip ${escapeHtml(statusClass)}">${escapeHtml(cycleGraphStatusLabel(statusValue))}</span>
          </span>
          <span class="cycle-list-card-meta">
            <span>${escapeHtml(cycleGraphCountLabel(cycle.task_count, "задача", "задачи", "задач"))}</span>
            <span>·</span>
            <span>${Number(cycle.handoff_count || 0)} handoff</span>
            <span>·</span>
            <span>${escapeHtml(cycleGraphFormatDate(cycle.updated_at))}</span>
          </span>
        </button>`;
      }).join("");
    }

    function deriveCycleGraphActivity(events, cycleStatus) {
      const activeByTask = new Map();
      (events || []).forEach((event) => {
        const eventType = String(event.event_type || "");
        const taskNodeId = String(event.task_node_id || "");
        const parentTaskNodeId = String(event.parent_task_node_id || "");
        const fromAgentId = String(event.from_agent_id || "");
        const toAgentId = String(event.to_agent_id || "");
        if (eventType === "TASK_STARTED" && taskNodeId && toAgentId) {
          activeByTask.set(taskNodeId, toAgentId);
          return;
        }
        if (eventType === "HANDOFF_TRIGGERED") {
          if (parentTaskNodeId) {
            activeByTask.delete(parentTaskNodeId);
          } else if (fromAgentId) {
            const fallbackEntry = Array.from(activeByTask.entries()).reverse().find(([, agentId]) => agentId === fromAgentId);
            if (fallbackEntry) {
              activeByTask.delete(fallbackEntry[0]);
            }
          }
          return;
        }
        if (["GROUP_REPORT_SUBMITTED", "MESSAGE_REMOVED"].includes(eventType)) {
          if (taskNodeId) {
            activeByTask.delete(taskNodeId);
          } else if (fromAgentId && eventType === "GROUP_REPORT_SUBMITTED") {
            const fallbackEntry = Array.from(activeByTask.entries()).reverse().find(([, agentId]) => agentId === fromAgentId);
            if (fallbackEntry) {
              activeByTask.delete(fallbackEntry[0]);
            }
          }
        }
        if (eventType === "CYCLE_COMPLETED") {
          activeByTask.clear();
        }
      });
      if (String(cycleStatus || "") === "completed") {
        activeByTask.clear();
      }
      return {
        agentIds: new Set(activeByTask.values()),
        taskNodeIds: new Set(activeByTask.keys())
      };
    }

    function renderCycleGraphHistory(historyData) {
      const events = Array.isArray(historyData.events) ? historyData.events : [];
      cycleGraphEventCountEl.textContent = cycleGraphCountLabel(events.length, "событие", "события", "событий");
      cycleGraphHistoryEl.innerHTML = events.slice().reverse().slice(0, 18).map((event) => {
        const eventType = String(event.event_type || "");
        const eventClass = eventType === "HANDOFF_TRIGGERED" ? " handoff" : (eventType === "TASK_STARTED" ? " started" : "");
        const fromRole = cycleGraphRoleLabel((event.from_agent || {}).role || event.from_agent_id);
        const toRole = cycleGraphRoleLabel((event.to_agent || {}).role || event.to_agent_id);
        const actorText = [event.from_agent_id ? fromRole : "", event.to_agent_id ? toRole : ""].filter(Boolean).join(" → ");
        const detail = [
          actorText,
          event.connection_id ? `связь ${event.connection_id}` : "",
          event.task_id ? `задача ${cycleGraphShortId(event.task_id, 30)}` : ""
        ].filter(Boolean).join(" · ");
        return `<article class="cycle-event${eventClass}" data-event-id="${escapeHtml(event.event_id || "")}" data-event-type="${escapeHtml(eventType)}">
          <span class="cycle-event-sequence">${Number(event.sequence || 0)}</span>
          <span>
            <span class="cycle-event-title">${escapeHtml(cycleGraphEventLabel(eventType))}</span>
            <span class="cycle-event-meta">${escapeHtml(detail)}</span>
          </span>
          <time class="subtle">${escapeHtml(cycleGraphFormatDate(event.timestamp))}</time>
        </article>`;
      }).join("") || `<div class="subtle">Событий пока нет.</div>`;
    }

    function renderCycleGraphSvg(graphData, historyData) {
      const cycle = graphData.cycle || historyData.cycle || {};
      const nodes = graphData.nodes || {};
      const edges = graphData.edges || {};
      const agents = Array.isArray(nodes.agents) ? nodes.agents.slice() : [];
      const tasks = Array.isArray(nodes.tasks) ? nodes.tasks.slice() : [];
      const communications = Array.isArray(edges.communications) ? edges.communications : [];
      const taskLineage = Array.isArray(edges.task_lineage) ? edges.task_lineage : [];
      const events = Array.isArray(historyData.events) ? historyData.events : [];
      const agentFirstSeen = new Map();
      events.forEach((event) => {
        [event.from_agent_id, event.to_agent_id].filter(Boolean).forEach((agentId) => {
          if (!agentFirstSeen.has(agentId)) {
            agentFirstSeen.set(agentId, agentFirstSeen.size);
          }
        });
      });
      agents.sort((left, right) => (agentFirstSeen.get(left.id) ?? Number.MAX_SAFE_INTEGER) - (agentFirstSeen.get(right.id) ?? Number.MAX_SAFE_INTEGER));
      tasks.sort((left, right) => String(left.first_event_at || "").localeCompare(String(right.first_event_at || "")));
      const activity = deriveCycleGraphActivity(events, cycle.status);
      const handoffs = events.filter((event) => event.event_type === "HANDOFF_TRIGGERED");
      const lastHandoff = handoffs.length ? handoffs[handoffs.length - 1] : null;

      cycleGraphTitleEl.textContent = cycle.title || "Граф группы";
      cycleGraphSubtitleEl.textContent = [cycle.cycle_id, cycleGraphStatusLabel(cycle.status), cycle.group_ids && cycle.group_ids.length ? cycleGraphCountLabel(cycle.group_ids.length, "группа", "группы", "групп") : ""].filter(Boolean).join(" · ");
      cycleGraphMetricsEl.innerHTML = [
        `<span class="badge">${escapeHtml(cycleGraphCountLabel(agents.length, "агент", "агента", "агентов"))}</span>`,
        `<span class="badge">${escapeHtml(cycleGraphCountLabel(tasks.length, "задача", "задачи", "задач"))}</span>`,
        `<span class="badge event">${handoffs.length} handoff</span>`,
        `<span class="badge common">${escapeHtml(cycleGraphCountLabel(activity.agentIds.size, "активный", "активных", "активных"))}</span>`
      ].join("");

      if (!agents.length && !tasks.length) {
        cycleGraphCanvasEl.innerHTML = `<div class="cycle-graph-empty">В выбранном цикле пока нет узлов графа.</div>`;
        cycleGraphTaskLineageEl.textContent = "Граф заполнится после постановки и получения первой задачи.";
        return;
      }

      const width = Math.max(760, agents.length * 190 + 80, tasks.length * 180 + 80);
      const height = tasks.length ? 390 : 215;
      const agentY = 96;
      const taskY = 292;
      const agentPositions = new Map();
      const taskPositions = new Map();
      const spreadX = (index, total, margin) => total <= 1 ? width / 2 : margin + index * ((width - margin * 2) / (total - 1));
      agents.forEach((agent, index) => agentPositions.set(String(agent.id || ""), {x: spreadX(index, agents.length, 112), y: agentY}));
      tasks.forEach((task, index) => taskPositions.set(String(task.id || ""), {x: spreadX(index, tasks.length, 105), y: taskY}));

      const communicationMarkup = communications.map((edge, index) => {
        const from = agentPositions.get(String(edge.from || ""));
        const to = agentPositions.get(String(edge.to || ""));
        if (!from || !to) {
          return "";
        }
        const isLatest = Boolean(lastHandoff && edge.from === lastHandoff.from_agent_id && edge.to === lastHandoff.to_agent_id);
        const bendY = agentY + 78 + (index % 3) * 16;
        const path = from.x === to.x
          ? `M ${from.x + 72} ${from.y} C ${from.x + 135} ${from.y - 75}, ${to.x + 135} ${to.y + 75}, ${to.x + 72} ${to.y}`
          : `M ${from.x} ${from.y + 38} C ${from.x} ${bendY}, ${to.x} ${bendY}, ${to.x} ${to.y + 38}`;
        return `<path class="cycle-communication-edge${isLatest ? " handoff-active" : ""}" data-edge-type="communication" data-edge-id="${escapeHtml(edge.id || "")}" d="${path}" marker-end="url(#${isLatest ? "cycleHandoffArrow" : "cycleAgentArrow"})"><title>${escapeHtml(`${cycleGraphRoleLabel((agents.find((item) => item.id === edge.from) || {}).role)} → ${cycleGraphRoleLabel((agents.find((item) => item.id === edge.to) || {}).role)} · ${Number(edge.count || 0)} передач`)}</title></path>`;
      }).join("");

      const lineageMarkup = taskLineage.map((edge) => {
        const from = taskPositions.get(String(edge.from || ""));
        const to = taskPositions.get(String(edge.to || ""));
        if (!from || !to) {
          return "";
        }
        const direction = to.x >= from.x ? 1 : -1;
        const startX = from.x + direction * 82;
        const endX = to.x - direction * 82;
        const curveY = taskY - 55;
        const path = `M ${startX} ${taskY} C ${startX + direction * 28} ${curveY}, ${endX - direction * 28} ${curveY}, ${endX} ${taskY}`;
        return `<path class="cycle-task-edge" data-edge-type="task-lineage" data-edge-id="${escapeHtml(edge.id || "")}" d="${path}" marker-end="url(#cycleTaskArrow)"><title>${escapeHtml(`${cycleGraphShortId(edge.from)} → ${cycleGraphShortId(edge.to)}`)}</title></path>`;
      }).join("");

      const agentMarkup = agents.map((agent) => {
        const id = String(agent.id || "");
        const position = agentPositions.get(id);
        const isActive = activity.agentIds.has(id);
        return `<g class="cycle-agent-node${isActive ? " active" : ""}" data-node-type="agent" data-node-id="${escapeHtml(id)}">
          <rect class="cycle-agent-halo" x="${position.x - 88}" y="${position.y - 47}" width="176" height="94" rx="18"></rect>
          <rect class="cycle-agent-body" x="${position.x - 80}" y="${position.y - 38}" width="160" height="76" rx="14"></rect>
          <text class="cycle-agent-role" x="${position.x}" y="${position.y - 8}">${escapeHtml(cycleGraphRoleLabel(agent.role))}</text>
          <text class="cycle-agent-meta" x="${position.x}" y="${position.y + 13}">${escapeHtml(agent.phone ? `телефон ${agent.phone}` : cycleGraphShortId(id, 20))}</text>
          <text class="cycle-agent-meta" x="${position.x}" y="${position.y + 29}">${isActive ? "TASK_STARTED · активен" : "ожидает / передал"}</text>
          <title>${escapeHtml(id)}</title>
        </g>`;
      }).join("");

      const taskMarkup = tasks.map((task) => {
        const id = String(task.id || "");
        const position = taskPositions.get(id);
        const isActive = activity.taskNodeIds.has(id);
        const eventTypes = Array.isArray(task.event_types) ? task.event_types : [];
        const lastEventType = eventTypes.length ? eventTypes[eventTypes.length - 1] : "";
        return `<g class="cycle-task-node${isActive ? " active" : ""}" data-node-type="task" data-node-id="${escapeHtml(id)}">
          <rect x="${position.x - 82}" y="${position.y - 35}" width="164" height="70" rx="11"></rect>
          <text class="cycle-task-title" x="${position.x}" y="${position.y - 9}">${escapeHtml(cycleGraphShortId(task.task_id || id, 24))}</text>
          <text class="cycle-task-meta" x="${position.x}" y="${position.y + 10}">${escapeHtml(cycleGraphEventLabel(lastEventType))}</text>
          <text class="cycle-task-meta" x="${position.x}" y="${position.y + 26}">${isActive ? "в работе" : (task.parent_task_node_id ? "дочерняя задача" : "корневая задача")}</text>
          <title>${escapeHtml(id)}</title>
        </g>`;
      }).join("");

      cycleGraphCanvasEl.innerHTML = `<svg id="cycleGraphSvg" role="img" aria-label="Граф агентов и задач цикла" viewBox="0 0 ${width} ${height}" width="${width}" height="${height}">
        <title>${escapeHtml(cycle.title || "Граф цикла разработки")}</title>
        <desc>${escapeHtml(`Фактические связи ${agents.length} агентов и lineage ${tasks.length} задач. Активных агентов: ${activity.agentIds.size}.`)}</desc>
        <defs>
          <marker id="cycleAgentArrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M 0 0 L 10 5 L 0 10 z" fill="#6b8eb5"></path></marker>
          <marker id="cycleHandoffArrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M 0 0 L 10 5 L 0 10 z" fill="#d97706"></path></marker>
          <marker id="cycleTaskArrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M 0 0 L 10 5 L 0 10 z" fill="#7c3aed"></path></marker>
        </defs>
        <text x="22" y="24" fill="#606975" font-size="11" font-weight="700">АГЕНТЫ И ФАКТИЧЕСКИЕ ПЕРЕДАЧИ</text>
        ${communicationMarkup}
        ${agentMarkup}
        ${tasks.length ? `<line x1="22" y1="218" x2="${width - 22}" y2="218" stroke="#d7dce2"></line><text x="22" y="242" fill="#606975" font-size="11" font-weight="700">LINEAGE ЗАДАЧ</text>${lineageMarkup}${taskMarkup}` : ""}
      </svg>`;

      const activeRoles = agents.filter((agent) => activity.agentIds.has(String(agent.id || ""))).map((agent) => cycleGraphRoleLabel(agent.role));
      const handoffText = lastHandoff
        ? `Последний handoff: ${cycleGraphRoleLabel((lastHandoff.from_agent || {}).role || lastHandoff.from_agent_id)} → ${cycleGraphRoleLabel((lastHandoff.to_agent || {}).role || lastHandoff.to_agent_id)}.`
        : "Handoff-событий пока нет.";
      cycleGraphTaskLineageEl.textContent = `${taskLineage.length} связей lineage · ${communications.length} коммуникационных маршрутов. ${activeRoles.length ? `Активны: ${activeRoles.join(", ")}.` : "Активных агентов нет."} ${handoffText}`;
    }

    function renderCycleGraphDetail(graphData, historyData) {
      renderCycleGraphSvg(graphData, historyData);
      renderCycleGraphHistory(historyData);
    }

    async function refreshCycleGraph(options = {}) {
      if (!cycleGraphViewIsActive() && !options.force) {
        return;
      }
      if (cycleGraphRefreshInFlight && !options.force) {
        return;
      }
      const context = activeProjectContext();
      const projectId = cycleGraphProjectPhone(context);
      const projectName = String((context && (context.project_name || context.git_context_key)) || "").trim();
      if (!projectId) {
        if (cycleGraphAbortController) {
          cycleGraphAbortController.abort();
        }
        cycleGraphCycles = [];
        cycleGraphProjectId = "";
        cycleGraphSelectedCycleId = "";
        cycleGraphViewEl.dataset.state = "empty";
        cycleGraphProjectSummaryEl.textContent = "У активного Git context нет канонического project_phone. Сначала зарегистрируйте проект через Project Manager 0001.";
        renderCycleGraphCycleList();
        resetCycleGraphDetail("Выберите зарегистрированный проект с project_phone.");
        setCycleGraphStatus("");
        return;
      }

      if (cycleGraphRefreshInFlight && options.force && cycleGraphAbortController) {
        cycleGraphAbortController.abort();
      }
      const controller = new AbortController();
      cycleGraphAbortController = controller;
      cycleGraphRefreshInFlight = true;
      const requestVersion = ++cycleGraphRequestVersion;
      const projectChanged = projectId !== cycleGraphProjectId;
      if (projectChanged) {
        cycleGraphSelectedCycleId = "";
      }
      cycleGraphProjectId = projectId;
      cycleGraphProjectSummaryEl.innerHTML = `<strong>${escapeHtml(projectName || "Проект")}</strong><br><span class="subtle">project_phone ${escapeHtml(projectId)} · ${escapeHtml((context && context.git_context_key) || "")}</span>`;
      if (!options.silent) {
        cycleGraphViewEl.dataset.state = "loading";
        setCycleGraphStatus("Загружаю циклы и историю...");
      }

      try {
        const cycleData = await cycleGraphFetchJson(`/api/v1/projects/${encodeURIComponent(projectId)}/cycles?limit=100`, controller.signal);
        if (requestVersion !== cycleGraphRequestVersion || projectId !== cycleGraphProjectPhone()) {
          return;
        }
        cycleGraphCycles = Array.isArray(cycleData.cycles) ? cycleData.cycles : [];
        const preferredCycleId = String(options.cycleId || cycleGraphSelectedCycleId || "");
        const preferredExists = cycleGraphCycles.some((cycle) => String(cycle.cycle_id || "") === preferredCycleId);
        const preferredActive = cycleGraphCycles.find((cycle) => cycle.status !== "completed");
        cycleGraphSelectedCycleId = preferredExists
          ? preferredCycleId
          : String(((preferredActive || cycleGraphCycles[0] || {}).cycle_id) || "");
        renderCycleGraphCycleList();

        if (!cycleGraphSelectedCycleId) {
          cycleGraphViewEl.dataset.state = "empty";
          resetCycleGraphDetail("В проекте пока нет циклов разработки.");
          setCycleGraphStatus("Циклов пока нет.");
          return;
        }

        const cycleId = cycleGraphSelectedCycleId;
        const [graphData, historyData] = await Promise.all([
          cycleGraphFetchJson(`/api/v1/cycles/${encodeURIComponent(cycleId)}/graph`, controller.signal),
          cycleGraphFetchJson(`/api/v1/cycles/${encodeURIComponent(cycleId)}/history`, controller.signal)
        ]);
        if (requestVersion !== cycleGraphRequestVersion || projectId !== cycleGraphProjectPhone() || cycleId !== cycleGraphSelectedCycleId) {
          return;
        }
        renderCycleGraphDetail(graphData, historyData);
        cycleGraphViewEl.dataset.state = "ready";
        setCycleGraphStatus(`Обновлено: ${new Date().toLocaleTimeString("ru-RU")}`, "ok");
      } catch (error) {
        if (error.name === "AbortError") {
          return;
        }
        if (requestVersion === cycleGraphRequestVersion) {
          cycleGraphViewEl.dataset.state = "error";
          setCycleGraphStatus(error.message || "Не удалось загрузить граф цикла.", "error");
          if (!cycleGraphCycles.length) {
            resetCycleGraphDetail("Не удалось загрузить данные цикла.");
          }
        }
      } finally {
        if (requestVersion === cycleGraphRequestVersion) {
          cycleGraphRefreshInFlight = false;
        }
      }
    }

    async function refreshHistory() {
      if (!activeMappedQueuePhone()) {
        currentHistoryRecords = [];
        historyCountEl.textContent = "Git context не выбран";
        historyEl.innerHTML = `<div class="panel subtle">${escapeHtml(activeGitContextRequiredMessage())}</div>`;
        setHistoryStatus("");
        return;
      }
      const records = await fetchHistoryRecords(500);
      currentHistoryRecords = records;
      const dateToText = historyToEl.value ? ` - ${historyToEl.value}` : "";
      const activeProject = selectedGitContext();
      const projectText = activeProject ? ` · ${activeProject.project_name || activeProject.git_context_key}` : "";
      historyCountEl.textContent = `${records.length} сообщений с ${historyFromEl.value || todayInputValue()}${dateToText}${projectText}`;
      historyEl.innerHTML = records.slice().reverse().map((record) => {
        const meta = record.metadata || {};
        const context = record.context || meta.context || "unknown";
        const timestamp = new Date(record.timestamp).toLocaleString("ru-RU");
        const revision = historyRecordRevision(record);
        const project = meta.project_name || "no-project";
        const actor = [meta.sender, meta.receiver].filter(Boolean).join(" -> ") || meta.direction || record.route || record.queue;
        const message = formatHistoryRecordMessage(record);
        const statusText = extractStatus(message);
        const summary = extractSummary(message);
        const eventLabel = meta.cycle_event_type || record.event;
        const queueItemId = meta.queue_item_id || "";
        const canDeleteFromQueue = queueItemId && activeQueueItemIds.has(`${record.queue}:${queueItemId}`);
        const canDeleteHistoryRecord = record.event === "removed_from_backend_queue";
        return `<article class="entry">
          <div class="entry-head">
            <div>
              <div class="entry-title">
                <span class="dot ${escapeHtml(context)}"></span>
                <span class="badge ${escapeHtml(context)}">${escapeHtml(context)}</span>
                <span class="badge">${escapeHtml(project)}</span>
                <span class="badge">${escapeHtml(revision.label)}: ${escapeHtml(revision.value)}</span>
                <span class="badge event">${escapeHtml(eventLabel)}</span>
              </div>
              <p class="entry-summary"><strong>${escapeHtml(actor)}:</strong> STATUS: ${escapeHtml(statusText)}${summary ? ". " + escapeHtml(summary) : ""}</p>
            </div>
            <time class="subtle">${escapeHtml(timestamp)}</time>
          </div>
          <details>
            <summary>Полный текст сообщения</summary>
            <pre>${escapeHtml(message)}</pre>
          </details>
          <div class="actions">
            <button class="icon-button" data-action="copy-history-record" data-record-id="${escapeHtml(record.id)}" type="button" title="Скопировать сообщение" aria-label="Скопировать сообщение">⧉</button>
            ${canDeleteFromQueue ? `<button class="icon-button" data-action="delete-queued-message" data-queue="${escapeHtml(record.queue)}" data-queue-item-id="${escapeHtml(queueItemId)}" type="button" title="Удалить из очереди" aria-label="Удалить из очереди">×</button>` : ""}
            ${canDeleteHistoryRecord ? `<button class="icon-button" data-action="restore-history-record" data-record-id="${escapeHtml(record.id)}" type="button" title="Вернуть сообщение в очередь" aria-label="Вернуть сообщение в очередь">↩</button>` : ""}
            ${canDeleteHistoryRecord ? `<button class="icon-button" data-action="delete-history-record" data-record-id="${escapeHtml(record.id)}" type="button" title="Стереть запись removed_from_backend_queue из истории" aria-label="Стереть запись removed_from_backend_queue из истории">⌫</button>` : ""}
          </div>
        </article>`;
      }).join("") || `<div class="panel subtle">История пока пустая.</div>`;
      setHistoryStatus("");
    }

    async function refresh() {
      await refreshGitConfig();
      await refreshAgents();
      const refreshTasks = [refreshQueues(), refreshScheduledTasks(), refreshHistory(), refreshProjectSprints()];
      if (cycleGraphViewIsActive()) {
        refreshTasks.push(refreshCycleGraph({silent: true}));
      }
      await Promise.all(refreshTasks);
    }

    function setActiveView(view) {
      document.querySelectorAll(".page-tab").forEach((button) => {
        button.classList.toggle("active", button.dataset.view === view);
      });
      document.querySelectorAll("main.view").forEach((viewEl) => {
        viewEl.classList.toggle("active", viewEl.dataset.view === view);
      });
      if ((view === "agents" || view === "consultants") && !agents.length) {
        refreshAgents().catch((error) => setAgentsStatus(error.message, "error"));
      }
      if (view === "screenshots") {
        refreshScreenshotFolders().catch((error) => setScreenshotFoldersStatus(error.message, "error"));
      }
      if (view === "evidence") {
        refreshEvidenceFolders().catch((error) => setEvidenceFoldersStatus(error.message, "error"));
      }
      if (view === "cycles") {
        refreshCycleGraph({force: true}).catch((error) => setCycleGraphStatus(error.message, "error"));
      } else if (cycleGraphAbortController) {
        cycleGraphAbortController.abort();
        cycleGraphAbortController = null;
        cycleGraphRefreshInFlight = false;
      }
    }

    queueEl.addEventListener("change", applyQueueDefaults);
    scheduleModeEl.addEventListener("change", updateScheduleUi);
    scheduleDelayMinutesEl.addEventListener("input", () => setStatus(""));
    document.querySelectorAll(".page-tab").forEach((button) => {
      button.addEventListener("click", () => setActiveView(button.dataset.view));
    });
    cycleGraphCycleSelectEl.addEventListener("change", () => {
      cycleGraphSelectedCycleId = cycleGraphCycleSelectEl.value;
      refreshCycleGraph({force: true, cycleId: cycleGraphSelectedCycleId}).catch((error) => setCycleGraphStatus(error.message, "error"));
    });
    cycleGraphCycleListEl.addEventListener("click", (event) => {
      const card = event.target.closest("[data-cycle-id]");
      if (!card) {
        return;
      }
      cycleGraphSelectedCycleId = String(card.dataset.cycleId || "");
      refreshCycleGraph({force: true, cycleId: cycleGraphSelectedCycleId}).catch((error) => setCycleGraphStatus(error.message, "error"));
    });
    document.getElementById("refreshCycleGraphButton").addEventListener("click", () => {
      refreshCycleGraph({force: true, cycleId: cycleGraphSelectedCycleId}).catch((error) => setCycleGraphStatus(error.message, "error"));
    });
    document.querySelectorAll(".tab").forEach((button) => {
      button.addEventListener("click", () => setActiveContext(button.dataset.context));
    });
    document.getElementById("sendButton").addEventListener("click", () => {
      sendMessage().catch((error) => setStatus(error.message, "error"));
    });
    document.getElementById("copyAttachmentFolderPathButton").addEventListener("click", () => {
      copyAttachmentFolderPath().catch((error) => setAttachmentStatus(error.message, "error"));
    });
    document.getElementById("copyAttachmentMessageButton").addEventListener("click", () => {
      copyAttachmentFolderMessage().catch((error) => setAttachmentStatus(error.message, "error"));
    });
    document.getElementById("refreshAttachmentFoldersButton").addEventListener("click", () => {
      refreshAttachmentFolderChoices().catch((error) => setAttachmentStatus(error.message, "error"));
    });
    attachmentFolderSelectEl.addEventListener("change", () => {
      updateAttachmentDescriptionForSelection();
      setAttachmentStatus("");
    });
    document.getElementById("createScreenshotFolderButton").addEventListener("click", () => {
      createScreenshotFolder().catch((error) => setScreenshotFoldersStatus(error.message, "error"));
    });
    document.getElementById("refreshScreenshotFoldersButton").addEventListener("click", () => {
      refreshScreenshotFolders().then(() => setScreenshotFoldersStatus("Список обновлен.", "ok")).catch((error) => setScreenshotFoldersStatus(error.message, "error"));
    });
    document.getElementById("applyScreenshotFolderFilterButton").addEventListener("click", () => {
      expandedScreenshotFolderIds = new Set();
      refreshScreenshotFolders().then(() => setScreenshotFoldersFilterStatus("Фильтр применен.", "ok")).catch((error) => setScreenshotFoldersFilterStatus(error.message, "error"));
    });
    document.getElementById("resetScreenshotFolderFilterButton").addEventListener("click", () => {
      screenshotFoldersFromEl.value = todayInputValue();
      screenshotFoldersToEl.value = "";
      expandedScreenshotFolderIds = new Set();
      refreshScreenshotFolders().then(() => setScreenshotFoldersFilterStatus("Показаны папки за сегодня.", "ok")).catch((error) => setScreenshotFoldersFilterStatus(error.message, "error"));
    });
    document.getElementById("uploadScreenshotsButton").addEventListener("click", () => {
      uploadScreenshots().catch((error) => setScreenshotUploadStatus(error.message, "error"));
    });
    document.getElementById("pasteScreenshotButton").addEventListener("click", () => {
      pasteScreenshotsFromClipboard().catch((error) => setScreenshotUploadStatus(error.message, "error"));
    });
    document.getElementById("copySelectedScreenshotFolderPathButton").addEventListener("click", () => {
      copySelectedScreenshotFolderPath().catch((error) => setScreenshotUploadStatus(error.message, "error"));
    });
    document.getElementById("clearScreenshotTrashButton").addEventListener("click", () => {
      clearScreenshotTrash().catch((error) => setScreenshotTrashStatus(error.message, "error"));
    });
    screenshotFolderSelectEl.addEventListener("change", () => {
      selectedScreenshotFolderId = screenshotFolderSelectEl.value;
      setScreenshotUploadStatus("");
    });
    screenshotDropZoneEl.addEventListener("dragover", (event) => {
      if (!dragEventHasFiles(event)) {
        return;
      }
      event.preventDefault();
      screenshotDropZoneEl.classList.add("drag-over");
      event.dataTransfer.dropEffect = "copy";
    });
    screenshotDropZoneEl.addEventListener("dragleave", () => {
      screenshotDropZoneEl.classList.remove("drag-over");
    });
    screenshotDropZoneEl.addEventListener("drop", (event) => {
      if (!dragEventHasFiles(event)) {
        return;
      }
      event.preventDefault();
      screenshotDropZoneEl.classList.remove("drag-over");
      uploadDroppedScreenshots(event.dataTransfer.files).catch((error) => setScreenshotUploadStatus(error.message, "error"));
    });
    screenshotTrashZoneEl.addEventListener("dragover", (event) => {
      if (!dragEventHasScreenshot(event)) {
        return;
      }
      event.preventDefault();
      screenshotTrashZoneEl.classList.add("drag-over");
      event.dataTransfer.dropEffect = "move";
    });
    screenshotTrashZoneEl.addEventListener("dragleave", () => {
      screenshotTrashZoneEl.classList.remove("drag-over");
    });
    screenshotTrashZoneEl.addEventListener("drop", (event) => {
      if (!dragEventHasScreenshot(event)) {
        return;
      }
      event.preventDefault();
      screenshotTrashZoneEl.classList.remove("drag-over");
      let item = draggedScreenshot || dragCandidateScreenshot;
      if (!item && event.dataTransfer) {
        try {
          item = JSON.parse(event.dataTransfer.getData("application/x-screenshot-file") || "null");
        } catch (error) {
          item = null;
        }
      }
      if (item && item.folderId && item.fileName) {
        addScreenshotToTrash(item.folderId, item.fileName);
      }
      draggedScreenshot = null;
      dragCandidateScreenshot = null;
    });
    screenshotTrashItemsEl.addEventListener("click", (event) => {
      const target = event.target.closest("[data-action]");
      if (!target || target.dataset.action !== "remove-from-screenshot-trash") {
        return;
      }
      removeScreenshotFromTrash(target.dataset.folderId, target.dataset.fileName);
    });
    screenshotFoldersEl.addEventListener("click", (event) => {
      const target = event.target.closest("[data-action]");
      if (!target) {
        return;
      }
      const action = target.dataset.action;
      const row = target.closest(".screenshot-file-row");
      const folderId = target.dataset.folderId || (row && row.dataset.folderId);
      const fileName = target.dataset.fileName || (row && row.dataset.fileName);
      if (action === "select-screenshot-folder") {
        selectedScreenshotFolderId = folderId;
        renderScreenshotFolderSelect(folderId);
        setScreenshotUploadStatus(`Выбрана папка: ${folderId}`, "ok");
      }
      if (action === "toggle-screenshot-folder") {
        if (expandedScreenshotFolderIds.has(folderId)) {
          expandedScreenshotFolderIds.delete(folderId);
        } else {
          expandedScreenshotFolderIds.add(folderId);
        }
        renderScreenshotFolders();
      }
      if (action === "copy-screenshot-folder-path") {
        const folder = folderById(folderId);
        if (folder) {
          copyTextToClipboard(folder.path).then(() => setScreenshotFoldersStatus(`Путь скопирован: ${folder.path}`, "ok")).catch((error) => setScreenshotFoldersStatus(error.message, "error"));
        }
      }
      if (action === "preview-screenshot-file" && folderId && fileName) {
        openScreenshotPreview(folderId, fileName);
      }
      if (action === "copy-screenshot-path" && folderId && fileName) {
        const file = fileByFolderAndName(folderId, fileName);
        if (file) {
          copyTextToClipboard(file.path).then(() => setScreenshotFoldersStatus(`Путь скриншота скопирован: ${file.path}`, "ok")).catch((error) => setScreenshotFoldersStatus(error.message, "error"));
        }
      }
      if (action === "delete-screenshot-file" && folderId && fileName) {
        deleteScreenshotFile(folderId, fileName).catch((error) => setScreenshotFoldersStatus(error.message, "error"));
      }
      if (action === "copy-screenshot-file" && folderId && fileName && row) {
        const targetSelect = row.querySelector('[data-role="copy-target"]');
        copyScreenshotFile(folderId, fileName, targetSelect ? targetSelect.value : "").catch((error) => setScreenshotFoldersStatus(error.message, "error"));
      }
      dragCandidateScreenshot = null;
    });
    screenshotFoldersEl.addEventListener("pointerdown", (event) => {
      if (event.target.closest("button, select")) {
        return;
      }
      const row = event.target.closest(".screenshot-file-row");
      dragCandidateScreenshot = row
        ? {folderId: row.dataset.folderId, fileName: row.dataset.fileName}
        : null;
    });
    screenshotFoldersEl.addEventListener("dragstart", (event) => {
      if (event.target.closest("button, select")) {
        return;
      }
      const row = event.target.closest(".screenshot-file-row");
      if (!row || !event.dataTransfer) {
        return;
      }
      draggedScreenshot = dragCandidateScreenshot || {
        folderId: row.dataset.folderId,
        fileName: row.dataset.fileName
      };
      event.dataTransfer.effectAllowed = "move";
      event.dataTransfer.setData("application/x-screenshot-file", JSON.stringify(draggedScreenshot));
    });
    screenshotFoldersEl.addEventListener("dragend", () => {
      draggedScreenshot = null;
      dragCandidateScreenshot = null;
      screenshotTrashZoneEl.classList.remove("drag-over");
      screenshotFoldersEl.querySelectorAll(".screenshot-folder-card.drag-over").forEach((card) => card.classList.remove("drag-over"));
    });
    screenshotFoldersEl.addEventListener("dragover", (event) => {
      if (!dragEventHasFiles(event)) {
        return;
      }
      const card = event.target.closest(".screenshot-folder-card");
      if (!card) {
        return;
      }
      event.preventDefault();
      card.classList.add("drag-over");
      event.dataTransfer.dropEffect = "copy";
    });
    screenshotFoldersEl.addEventListener("dragleave", (event) => {
      const card = event.target.closest(".screenshot-folder-card");
      if (card) {
        card.classList.remove("drag-over");
      }
    });
    screenshotFoldersEl.addEventListener("drop", (event) => {
      if (!dragEventHasFiles(event)) {
        return;
      }
      const card = event.target.closest(".screenshot-folder-card");
      if (!card) {
        return;
      }
      event.preventDefault();
      card.classList.remove("drag-over");
      uploadDroppedScreenshots(event.dataTransfer.files, card.dataset.folderId).catch((error) => setScreenshotUploadStatus(error.message, "error"));
    });
    document.getElementById("closeScreenshotPreviewButton").addEventListener("click", closeScreenshotPreview);
    screenshotPreviewModalEl.addEventListener("click", (event) => {
      if (event.target === screenshotPreviewModalEl) {
        closeScreenshotPreview();
      }
    });
    document.getElementById("createEvidenceFolderButton").addEventListener("click", () => {
      createEvidenceFolder().catch((error) => setEvidenceFoldersStatus(error.message, "error"));
    });
    document.getElementById("refreshEvidenceFoldersButton").addEventListener("click", () => {
      refreshEvidenceFolders().then(() => setEvidenceFoldersStatus("Список обновлен.", "ok")).catch((error) => setEvidenceFoldersStatus(error.message, "error"));
    });
    document.getElementById("applyEvidenceFolderFilterButton").addEventListener("click", () => {
      expandedEvidenceFolderIds = new Set();
      refreshEvidenceFolders().then(() => setEvidenceFoldersFilterStatus("Фильтр применен.", "ok")).catch((error) => setEvidenceFoldersFilterStatus(error.message, "error"));
    });
    document.getElementById("resetEvidenceFolderFilterButton").addEventListener("click", () => {
      evidenceFoldersFromEl.value = todayInputValue();
      evidenceFoldersToEl.value = "";
      expandedEvidenceFolderIds = new Set();
      refreshEvidenceFolders().then(() => setEvidenceFoldersFilterStatus("Показаны папки за сегодня.", "ok")).catch((error) => setEvidenceFoldersFilterStatus(error.message, "error"));
    });
    document.getElementById("uploadEvidenceButton").addEventListener("click", () => {
      uploadEvidence().catch((error) => setEvidenceUploadStatus(error.message, "error"));
    });
    document.getElementById("copySelectedEvidenceFolderPathButton").addEventListener("click", () => {
      copySelectedEvidenceFolderPath().catch((error) => setEvidenceUploadStatus(error.message, "error"));
    });
    document.getElementById("clearEvidenceTrashButton").addEventListener("click", () => {
      clearEvidenceTrash().catch((error) => setEvidenceTrashStatus(error.message, "error"));
    });
    evidenceFolderSelectEl.addEventListener("change", () => {
      selectedEvidenceFolderId = evidenceFolderSelectEl.value;
      setEvidenceUploadStatus("");
    });
    evidenceDropZoneEl.addEventListener("dragover", (event) => {
      if (!dragEventHasFiles(event)) {
        return;
      }
      event.preventDefault();
      evidenceDropZoneEl.classList.add("drag-over");
      event.dataTransfer.dropEffect = "copy";
    });
    evidenceDropZoneEl.addEventListener("dragleave", () => {
      evidenceDropZoneEl.classList.remove("drag-over");
    });
    evidenceDropZoneEl.addEventListener("drop", (event) => {
      if (!dragEventHasFiles(event)) {
        return;
      }
      event.preventDefault();
      evidenceDropZoneEl.classList.remove("drag-over");
      uploadDroppedEvidence(event.dataTransfer.files).catch((error) => setEvidenceUploadStatus(error.message, "error"));
    });
    evidenceTrashZoneEl.addEventListener("dragover", (event) => {
      if (!dragEventHasEvidence(event)) {
        return;
      }
      event.preventDefault();
      evidenceTrashZoneEl.classList.add("drag-over");
      event.dataTransfer.dropEffect = "move";
    });
    evidenceTrashZoneEl.addEventListener("dragleave", () => {
      evidenceTrashZoneEl.classList.remove("drag-over");
    });
    evidenceTrashZoneEl.addEventListener("drop", (event) => {
      if (!dragEventHasEvidence(event)) {
        return;
      }
      event.preventDefault();
      evidenceTrashZoneEl.classList.remove("drag-over");
      let item = draggedEvidence || dragCandidateEvidence;
      if (!item && event.dataTransfer) {
        try {
          item = JSON.parse(event.dataTransfer.getData("application/x-evidence-file") || "null");
        } catch (error) {
          item = null;
        }
      }
      if (item && item.folderId && item.fileName) {
        addEvidenceToTrash(item.folderId, item.fileName);
      }
      draggedEvidence = null;
      dragCandidateEvidence = null;
    });
    evidenceTrashItemsEl.addEventListener("click", (event) => {
      const target = event.target.closest("[data-action]");
      if (!target || target.dataset.action !== "remove-from-evidence-trash") {
        return;
      }
      removeEvidenceFromTrash(target.dataset.folderId, target.dataset.fileName);
    });
    evidenceFoldersEl.addEventListener("click", (event) => {
      const target = event.target.closest("[data-action]");
      if (!target) {
        return;
      }
      const action = target.dataset.action;
      const row = target.closest(".evidence-file-row");
      const folderId = target.dataset.folderId || (row && row.dataset.folderId);
      const fileName = target.dataset.fileName || (row && row.dataset.fileName);
      if (action === "select-evidence-folder") {
        selectedEvidenceFolderId = folderId;
        renderEvidenceFolderSelect(folderId);
        setEvidenceUploadStatus(`Выбрана папка: ${folderId}`, "ok");
      }
      if (action === "toggle-evidence-folder") {
        if (expandedEvidenceFolderIds.has(folderId)) {
          expandedEvidenceFolderIds.delete(folderId);
        } else {
          expandedEvidenceFolderIds.add(folderId);
        }
        renderEvidenceFolders();
      }
      if (action === "copy-evidence-folder-path") {
        const folder = evidenceFolderById(folderId);
        if (folder) {
          copyTextToClipboard(folder.path).then(() => setEvidenceFoldersStatus(`Путь скопирован: ${folder.path}`, "ok")).catch((error) => setEvidenceFoldersStatus(error.message, "error"));
        }
      }
      if (action === "preview-evidence-file" && folderId && fileName) {
        openEvidencePreview(folderId, fileName);
      }
      if (action === "copy-evidence-path" && folderId && fileName) {
        const file = evidenceFileByFolderAndName(folderId, fileName);
        if (file) {
          copyTextToClipboard(file.path).then(() => setEvidenceFoldersStatus(`Путь файла скопирован: ${file.path}`, "ok")).catch((error) => setEvidenceFoldersStatus(error.message, "error"));
        }
      }
      if (action === "delete-evidence-file" && folderId && fileName) {
        deleteEvidenceFile(folderId, fileName).catch((error) => setEvidenceFoldersStatus(error.message, "error"));
      }
      if (action === "copy-evidence-file" && folderId && fileName && row) {
        const targetSelect = row.querySelector('[data-role="copy-target"]');
        copyEvidenceFile(folderId, fileName, targetSelect ? targetSelect.value : "").catch((error) => setEvidenceFoldersStatus(error.message, "error"));
      }
      dragCandidateEvidence = null;
    });
    evidenceFoldersEl.addEventListener("pointerdown", (event) => {
      if (event.target.closest("button, select")) {
        return;
      }
      const row = event.target.closest(".evidence-file-row");
      dragCandidateEvidence = row
        ? {folderId: row.dataset.folderId, fileName: row.dataset.fileName}
        : null;
    });
    evidenceFoldersEl.addEventListener("dragstart", (event) => {
      if (event.target.closest("button, select")) {
        return;
      }
      const row = event.target.closest(".evidence-file-row");
      if (!row || !event.dataTransfer) {
        return;
      }
      draggedEvidence = dragCandidateEvidence || {
        folderId: row.dataset.folderId,
        fileName: row.dataset.fileName
      };
      event.dataTransfer.effectAllowed = "move";
      event.dataTransfer.setData("application/x-evidence-file", JSON.stringify(draggedEvidence));
    });
    evidenceFoldersEl.addEventListener("dragend", () => {
      draggedEvidence = null;
      dragCandidateEvidence = null;
      evidenceTrashZoneEl.classList.remove("drag-over");
      evidenceFoldersEl.querySelectorAll(".evidence-folder-card.drag-over").forEach((card) => card.classList.remove("drag-over"));
    });
    evidenceFoldersEl.addEventListener("dragover", (event) => {
      if (!dragEventHasFiles(event)) {
        return;
      }
      const card = event.target.closest(".evidence-folder-card");
      if (!card) {
        return;
      }
      event.preventDefault();
      card.classList.add("drag-over");
      event.dataTransfer.dropEffect = "copy";
    });
    evidenceFoldersEl.addEventListener("dragleave", (event) => {
      const card = event.target.closest(".evidence-folder-card");
      if (card) {
        card.classList.remove("drag-over");
      }
    });
    evidenceFoldersEl.addEventListener("drop", (event) => {
      if (!dragEventHasFiles(event)) {
        return;
      }
      const card = event.target.closest(".evidence-folder-card");
      if (!card) {
        return;
      }
      event.preventDefault();
      card.classList.remove("drag-over");
      uploadDroppedEvidence(event.dataTransfer.files, card.dataset.folderId).catch((error) => setEvidenceUploadStatus(error.message, "error"));
    });
    document.getElementById("closeEvidencePreviewButton").addEventListener("click", closeEvidencePreview);
    evidencePreviewModalEl.addEventListener("click", (event) => {
      if (event.target === evidencePreviewModalEl) {
        closeEvidencePreview();
      }
    });
    ["click", "keyup", "select", "input"].forEach((eventName) => {
      messageEl.addEventListener(eventName, rememberMessageCursor);
    });
    document.getElementById("sendPhoneMessageButton").addEventListener("click", () => {
      sendPhoneMessage().catch((error) => setPhoneStatus(error.message, "error"));
    });
    phoneChannelEl.addEventListener("change", () => {
      updatePhoneAgentControls();
      setPhoneStatus("");
    });
    document.getElementById("askConsultantButton").addEventListener("click", () => {
      askConsultant().catch((error) => setConsultantStatus(error.message, "error"));
    });
    document.getElementById("checkConsultantReplyButton").addEventListener("click", () => {
      checkConsultantReply().catch((error) => setConsultantStatus(error.message, "error"));
    });
    consultantSenderEl.addEventListener("change", () => {
      updateConsultantControls();
      setConsultantStatus("");
    });
    consultantExpertEl.addEventListener("change", () => setConsultantStatus(""));
    document.getElementById("taskTemplateButton").addEventListener("click", () => setTemplate("task"));
    document.getElementById("failTemplateButton").addEventListener("click", () => setTemplate("fail"));
    document.getElementById("readyTemplateButton").addEventListener("click", () => setTemplate("ready"));
    document.getElementById("refreshButton").addEventListener("click", refresh);
    document.getElementById("saveGitButton").addEventListener("click", () => {
      saveGitConfig()
        .then(() => Promise.all([refreshQueues(), refreshScheduledTasks(), refreshHistory()]))
        .catch((error) => setGitStatus(error.message, "error"));
    });
    document.getElementById("addGitContextButton").addEventListener("click", addGitContext);
    document.getElementById("deleteGitContextButton").addEventListener("click", () => {
      deleteSelectedGitContext().catch((error) => setGitStatus(error.message, "error"));
    });
    gitNewPhoneEl.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        event.preventDefault();
        addGitContext();
      }
    });
    document.getElementById("checkGitButton").addEventListener("click", () => {
      refreshGitConfig().catch((error) => setGitStatus(error.message, "error"));
    });
    document.getElementById("applyHistoryFilterButton").addEventListener("click", () => {
      refreshHistory().catch((error) => setHistoryStatus(error.message, "error"));
    });
    document.getElementById("copyHistoryButton").addEventListener("click", () => {
      copyHistoryToClipboard().catch((error) => setHistoryStatus(error.message, "error"));
    });
    document.getElementById("copyHistoryWithPatchesButton").addEventListener("click", () => {
      copyHistoryWithPatchesToClipboard().catch((error) => setHistoryStatus(error.message, "error"));
    });
    queueStatsEl.addEventListener("click", (event) => {
      const target = event.target;
      if (target && target.dataset && target.dataset.action === "delete-queued-message") {
        deleteQueuedMessage(target.dataset.queue, target.dataset.queueItemId).catch((error) => setStatus(error.message, "error"));
      }
    });
    scheduledTasksEl.addEventListener("click", (event) => {
      const target = event.target;
      if (target && target.dataset && target.dataset.action === "cancel-scheduled-task") {
        cancelScheduledTask(target.dataset.taskId).catch((error) => setScheduledTasksStatus(error.message, "error"));
      }
    });
    document.getElementById("addEmailRouteButton").addEventListener("click", () => addEmailRoute());
    document.getElementById("saveEmailRoutesButton").addEventListener("click", () => {
      saveEmailRoutes().catch((error) => setEmailRoutesStatus(error.message, "error"));
    });
    applyGitContextProjectButtonEl.addEventListener("click", () => {
      applySelectedGitContext(gitContextListEl.value || "", true);
    });
    gitContextListEl.addEventListener("change", () => {
      pendingGitContextKey = gitContextListEl.value || "";
    });
    gitPhoneEl.addEventListener("change", () => {
      applySelectedGitPhone(activeQueuePhone(), true);
    });
    document.getElementById("addAgentButton").addEventListener("click", () => addAgent());
    attachAgentToProjectButtonEl.addEventListener("click", () => {
      attachExistingAgentToProject().catch((error) => setAgentsStatus(error.message, "error"));
    });
    detachSelectedAgentFromProjectButtonEl.addEventListener("click", () => {
      detachSelectedAgentFromProject().catch((error) => setAgentsStatus(error.message, "error"));
    });
    importProjectActorsButtonEl.addEventListener("click", () => {
      importProjectActorsFromJson().catch((error) => setAgentsStatus(error.message, "error"));
    });
    refreshProjectSprintsButtonEl.addEventListener("click", () => {
      projectSprintsStatusEl.textContent = "Обновляю историю спринтов...";
      refreshProjectSprints().catch((error) => {
        projectSprintsStatusEl.textContent = error.message;
      });
    });
    deleteAllProjectActorsButtonEl.addEventListener("click", () => {
      deleteAllProjectActors().catch((error) => setAgentsStatus(error.message, "error"));
    });
    agentProjectManagerEl.addEventListener("click", (event) => {
      const target = event.target;
      if (!target || !target.dataset) {
        return;
      }
      if (target.dataset.action === "copy-agent-to-project") {
        attachAgentToProjectById(target.dataset.agentId).catch((error) => setAgentsStatus(error.message, "error"));
      }
      if (target.dataset.action === "edit-project-agent") {
        selectAgentForEdit(target.dataset.agentId);
      }
      if (target.dataset.action === "detach-agent-from-project") {
        detachAgentFromProjectById(target.dataset.agentId).catch((error) => setAgentsStatus(error.message, "error"));
      }
      if (target.dataset.action === "download-project-sprint" && target.dataset.sprintUrl) {
        window.location.assign(target.dataset.sprintUrl);
      }
    });
    document.getElementById("cloneAgentButton").addEventListener("click", openCloneAgentModal);
    document.getElementById("removeSelectedAgentButton").addEventListener("click", removeSelectedAgent);
    document.getElementById("copyAgentProfileButton").addEventListener("click", openCopyProfilePhoneModal);
    document.getElementById("saveAgentsButton").addEventListener("click", () => {
      saveAgents().catch((error) => setAgentsStatus(error.message, "error"));
    });

    async function createEmptyAgent() {
      setAgentsStatus("Генерация пустого агента...");
      emptyAgentPromptPanelEl.hidden = true;
      const response = await fetch("/agents/create-empty", {
        method: "POST"
      });
      const data = await response.json();
      if (!response.ok) {
        setAgentsStatus(data.detail || "Ошибка генерации пустого агента.", "error");
        return;
      }
      setAgentsStatus(`Успешно сгенерирован агент: ${data.name} с телефоном ${data.phone}.`, "ok");
      emptyAgentPromptMetaEl.textContent = `${data.name} · телефон ${data.phone}`;
      emptyAgentPromptTextEl.value = data.prompt_text || "";
      emptyAgentPromptPanelEl.hidden = false;
      await refreshAgents();
      selectedAgentId = data.id;
      renderAgentSelector(selectedAgentId);
      renderSelectedAgent();
    }

    async function copyEmptyAgentPrompt() {
      const promptText = emptyAgentPromptTextEl.value;
      if (!promptText) {
        setAgentsStatus("Сначала сгенерируйте пустого агента.", "error");
        return;
      }
      if (navigator.clipboard && window.isSecureContext) {
        await navigator.clipboard.writeText(promptText);
      } else {
        fallbackCopyText(promptText);
      }
      setAgentsStatus("Prompt пустого агента скопирован.", "ok");
    }

    async function sendSpecialization() {
      const phone = specializePhoneEl.value;
      const name = specializeNameEl.value.trim();
      const profile = specializeProfileEl.value.trim();
      if (!phone) {
        specializeStatusEl.textContent = "Выберите пустого агента.";
        specializeStatusEl.className = "status error";
        return;
      }
      if (!name || !profile) {
        specializeStatusEl.textContent = "Заполните имя и профиль специализации.";
        specializeStatusEl.className = "status error";
        return;
      }

      let parameters = {};
      const templateId = specializeTemplateEl.value;
      const template = agentById(templateId);
      if (template && template.parameters) {
        parameters = Object.assign({}, template.parameters);
      }

      specializeStatusEl.textContent = "Отправка специализации...";
      specializeStatusEl.className = "status";

      const response = await fetch(`/agents/specialize/${encodeURIComponent(phone)}`, {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({
          name,
          profile,
          parameters
        })
      });
      const data = await response.json();
      if (!response.ok) {
        specializeStatusEl.textContent = data.detail || "Ошибка отправки специализации.";
        specializeStatusEl.className = "status error";
        return;
      }
      pendingSpecializations[data.phone] = data.spec;
      updateSpecializeAgentState();
      renderAgentStatusSummary();
      specializeStatusEl.textContent = `Специализация для телефона ${phone} отправлена. Ожидается опрос и подтверждение агента.`;
      specializeStatusEl.className = "status ok";
    }

    createEmptyAgentButtonEl.addEventListener("click", () => {
      createEmptyAgent().catch((error) => setAgentsStatus(error.message, "error"));
    });
    specializePhoneEl.addEventListener("change", () => {
      updateSpecializeAgentState();
      specializeStatusEl.textContent = "";
    });
    copyEmptyAgentPromptButtonEl.addEventListener("click", () => {
      copyEmptyAgentPrompt().catch((error) => setAgentsStatus(error.message, "error"));
    });
    specializeTemplateEl.addEventListener("change", () => {
      const templateId = specializeTemplateEl.value;
      const template = agentById(templateId);
      if (template) {
        specializeNameEl.value = template.name + " (Специализированный)";
        specializeProfileEl.value = template.profile;
      } else {
        specializeNameEl.value = "";
        specializeProfileEl.value = "";
      }
      specializeStatusEl.textContent = "";
    });
    sendSpecializationButtonEl.addEventListener("click", () => {
      sendSpecialization().catch((error) => {
        specializeStatusEl.textContent = error.message;
        specializeStatusEl.className = "status error";
      });
    });
    document.getElementById("closeCloneAgentButton").addEventListener("click", closeCloneAgentModal);
    document.getElementById("cancelCloneAgentButton").addEventListener("click", closeCloneAgentModal);
    document.getElementById("loadGeminiModelsButton").addEventListener("click", () => {
      loadGeminiModels().catch((error) => setCloneAgentStatus(error.message, "error"));
    });
    document.getElementById("requestCloneAgentButton").addEventListener("click", () => {
      requestAgentClonePreview().catch((error) => setCloneAgentStatus(error.message, "error"));
    });
    acceptCloneAgentButtonEl.addEventListener("click", acceptAgentClone);
    document.getElementById("closeCopyProfilePhoneButton").addEventListener("click", closeCopyProfilePhoneModal);
    document.getElementById("cancelCopyProfilePhoneButton").addEventListener("click", closeCopyProfilePhoneModal);
    confirmCopyProfilePhoneButtonEl.addEventListener("click", () => {
      copySelectedAgentProfile().catch((error) => setCopyProfilePhoneStatus(error.message, "error"));
    });
    cloneAgentModalEl.addEventListener("click", (event) => {
      if (event.target === cloneAgentModalEl) {
        closeCloneAgentModal();
      }
    });
    copyProfilePhoneModalEl.addEventListener("click", (event) => {
      if (event.target === copyProfilePhoneModalEl) {
        closeCopyProfilePhoneModal();
      }
    });
    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape" && !screenshotPreviewModalEl.hidden) {
        closeScreenshotPreview();
      }
      if (event.key === "Escape" && !evidencePreviewModalEl.hidden) {
        closeEvidencePreview();
      }
      if (event.key === "Escape" && !cloneAgentModalEl.hidden) {
        closeCloneAgentModal();
      }
      if (event.key === "Escape" && !copyProfilePhoneModalEl.hidden) {
        closeCopyProfilePhoneModal();
      }
      if (event.key === "Escape" && !helpModalEl.hidden) {
        closeHelpModal();
      }
    });
    cloneSourceAgentEl.addEventListener("change", () => {
      resetClonePreview();
      setCloneAgentStatus("");
    });
    agentSelectorEl.addEventListener("change", () => {
      captureCurrentAgent();
      selectedAgentId = agentSelectorEl.value;
      renderSelectedAgent();
      renderAgentProjectControls();
      setAgentsStatus("");
    });
    agentsEl.addEventListener("input", () => {
      markAgentEditorDirty();
      captureCurrentAgent();
      renderAgentSelector(selectedAgentId);
      syncActorsFromAgents(agents);
      updateSpecializationPanel();
      renderAgentStatusSummary();
      renderAgentProjectControls();
      updateAgentPreview();
      setAgentsStatus("");
    });
    agentsEl.addEventListener("click", (event) => {
      const target = event.target;
      if (target && target.dataset && target.dataset.action === "add-agent-param") {
        addSelectedAgentParam();
        updateAgentPreview();
      }
      if (target && target.dataset && target.dataset.action === "remove-agent-param") {
        markAgentEditorDirty();
        target.closest(".agent-param-row").remove();
        const paramsEl = agentsEl.querySelector(".agent-params");
        if (paramsEl && !paramsEl.querySelector(".agent-param-row")) {
          paramsEl.innerHTML = `<div class="subtle">Параметры пока не заданы.</div>`;
        }
        captureCurrentAgent();
        updateAgentPreview();
        setAgentsStatus("");
      }
    });
    emailRoutesEl.addEventListener("click", (event) => {
      const target = event.target;
      if (target && target.dataset && target.dataset.action === "remove-email-route") {
        target.closest(".email-route-row").remove();
        if (!emailRoutesEl.querySelector(".email-route-row")) {
          renderEmailRoutes([]);
        }
        setEmailRoutesStatus("");
      }
    });
    historyEl.addEventListener("click", (event) => {
      const target = event.target;
      if (target && target.dataset && target.dataset.action === "copy-history-record") {
        copySingleHistoryRecord(target.dataset.recordId).catch((error) => setHistoryStatus(error.message, "error"));
      }
      if (target && target.dataset && target.dataset.action === "delete-queued-message") {
        deleteQueuedMessage(target.dataset.queue, target.dataset.queueItemId).catch((error) => setHistoryStatus(error.message, "error"));
      }
      if (target && target.dataset && target.dataset.action === "delete-history-record") {
        deleteRemovedBackendHistoryRecord(target.dataset.recordId).catch((error) => setHistoryStatus(error.message, "error"));
      }
      if (target && target.dataset && target.dataset.action === "restore-history-record") {
        restoreRemovedBackendHistoryRecord(target.dataset.recordId).catch((error) => setHistoryStatus(error.message, "error"));
      }
    });
    historyGitContextEl.addEventListener("change", () => {
      setHistoryStatus("");
      refreshHistory().catch((error) => setHistoryStatus(error.message, "error"));
    });
    historyFromEl.addEventListener("change", () => setHistoryStatus(""));
    historyToEl.addEventListener("change", () => setHistoryStatus(""));
    screenshotFoldersFromEl.addEventListener("change", () => setScreenshotFoldersFilterStatus(""));
    screenshotFoldersToEl.addEventListener("change", () => setScreenshotFoldersFilterStatus(""));
    evidenceFoldersFromEl.addEventListener("change", () => setEvidenceFoldersFilterStatus(""));
    evidenceFoldersToEl.addEventListener("change", () => setEvidenceFoldersFilterStatus(""));
    document.addEventListener("paste", (event) => {
      const screenshotsView = document.querySelector('main[data-view="screenshots"]');
      if (!screenshotsView || !screenshotsView.classList.contains("active")) {
        return;
      }
      const files = normalizeClipboardScreenshotFiles(event.clipboardData && event.clipboardData.files);
      if (!files.length) {
        return;
      }
      event.preventDefault();
      uploadPastedScreenshots(files).catch((error) => setScreenshotUploadStatus(error.message, "error"));
    });

    historyFromEl.value = todayInputValue();
    screenshotFoldersFromEl.value = todayInputValue();
    evidenceFoldersFromEl.value = todayInputValue();
    initializeHelpSystem();
    updateScheduleUi();
    renderScreenshotTrash();
    renderEvidenceTrash();
    renderEmailSenderOptions();
    renderQueueOptions();
    setTemplate("task");
    refreshAgents().catch((error) => setAgentsStatus(error.message, "error"));
    refreshEmailRoutes().catch((error) => setEmailRoutesStatus(error.message, "error"));
    refresh()
      .then(() => refreshAttachmentFolderChoices())
      .catch((error) => setStatus(error.message, "error"));
    setInterval(refresh, 5000);
  </script>
</body>
</html>"""
    return (
        template.replace("__HISTORY_PATH__", escaped_history_path)
        .replace("__GIT_CONFIG_PATH__", escaped_git_config_path)
        .replace("__EMAIL_ROUTES_PATH__", escaped_email_routes_path)
        .replace("__AGENTS_PATH__", escaped_agents_path)
        .replace("__SCREENSHOT_FOLDERS_PATH__", escaped_screenshot_folders_path)
        .replace("__EVIDENCE_FOLDERS_PATH__", escaped_evidence_folders_path)
        .replace("__QUEUE_CONFIG_JSON__", queue_config_json)
    )


async def read_message(request: Request) -> Any:
    body = await request.body()
    if not body:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Request body is empty",
        )

    content_type = request.headers.get("content-type", "")
    if "application/json" in content_type:
        try:
            return json.loads(body)
        except json.JSONDecodeError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid JSON body",
            ) from exc

    return body.decode("utf-8")


async def enqueue(
    queue_name: str,
    message: Any,
    metadata: dict[str, Any] | None = None,
    port: int | None = None,
    git_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    queue_meta = queue_definition(queue_name)
    clean_metadata = {key: value for key, value in (metadata or {}).items() if value is not None}
    item_git_context = (
        {key: value for key, value in git_context.items() if value is not None}
        if git_context is not None
        else await git_context_for_port(port)
    )
    item = make_queue_item(message, {**clean_metadata, **item_git_context})
    item_id = item["id"]
    async with locks[queue_name]:
        queues[queue_name].append(item)
        size = len(queues[queue_name])

    event = queue_meta["post_event"]
    await append_history(
        event,
        queue_name,
        message,
        {"queue_item_id": item_id, **clean_metadata},
        port,
        item_git_context,
    )
    return {"status": "queued", "queue": queue_name, "id": item_id, "size": size}


async def dequeue(
    queue_name: str,
    port: int | None = None,
    git_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    queue_meta = queue_definition(queue_name)
    target_context_key = (
        git_context_key_from_metadata(git_context)
        if isinstance(git_context, dict)
        else ""
    )
    delivered_item: Any | None = None
    async with locks[queue_name]:
        kept_items: deque[Any] = deque()
        while queues[queue_name]:
            item = queues[queue_name].popleft()
            if (
                delivered_item is None
                and (
                    not target_context_key
                    or queue_item_matches_git_context(item, target_context_key)
                )
            ):
                delivered_item = item
                continue
            kept_items.append(item)
        queues[queue_name] = kept_items

        if delivered_item is None:
            detail = "Queue is empty"
            if target_context_key:
                detail = "Queue has no message for this phone Git context"
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=detail,
            )

    item_id = queue_item_id(delivered_item)
    message = queue_item_message(delivered_item)
    event = queue_meta["get_event"]
    item_metadata = queue_item_metadata(delivered_item)
    metadata = {"queue_item_id": item_id, **item_metadata} if item_id else item_metadata
    await append_history(event, queue_name, message, metadata, port=port, git_context=git_context)
    return {"message": message, "id": item_id}


def phone_channel_item_matches(
    item: Any,
    conversation_phone: str,
    to_phone: str,
) -> bool:
    metadata = queue_item_metadata(item)
    return (
        str(metadata.get("conversation_phone", "")).strip() == conversation_phone
        and str(metadata.get("to_phone", "")).strip() == to_phone
    )


async def enqueue_phone_channel(
    queue_name: str,
    conversation_phone: str,
    message: Any,
    metadata: dict[str, Any],
    port: int | None = None,
    git_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if queue_name not in {"worker-all", "tester-all", "consultant-all"}:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Unknown phone channel",
        )

    conversation_phone = conversation_phone.strip()
    from_phone = str(metadata.get("from_phone", "")).strip()
    to_phone = str(metadata.get("to_phone", "")).strip()
    if not conversation_phone or not from_phone or not to_phone:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="conversation_phone, from_phone and to_phone are required",
        )

    phone_metadata = {
        **metadata,
        "conversation_phone": conversation_phone,
        "from_phone": from_phone,
        "to_phone": to_phone,
        "phone_channel": queue_name,
    }
    return await enqueue(queue_name, message, phone_metadata, port, git_context)


async def _dequeue_phone_channel_unlocked(
    queue_name: str,
    conversation_phone: str,
    to_phone: str,
    port: int | None = None,
    git_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if queue_name not in {"worker-all", "tester-all", "consultant-all"}:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Unknown phone channel",
        )

    conversation_phone = conversation_phone.strip()
    to_phone = to_phone.strip()
    if not conversation_phone or not to_phone:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="conversation_phone and to_phone are required",
        )

    queue_meta = queue_definition(queue_name)
    target_context_key = (
        git_context_key_from_metadata(git_context)
        if isinstance(git_context, dict)
        else ""
    )
    delivered_item: Any | None = None
    async with locks[queue_name]:
        kept_items: deque[Any] = deque()
        while queues[queue_name]:
            item = queues[queue_name].popleft()
            if (
                delivered_item is None
                and phone_channel_item_matches(item, conversation_phone, to_phone)
                and (
                    not target_context_key
                    or queue_item_matches_git_context(item, target_context_key)
                )
            ):
                delivered_item = item
                continue
            kept_items.append(item)
        queues[queue_name] = kept_items

    if delivered_item is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No message for this phone",
        )

    item_id = queue_item_id(delivered_item)
    message = queue_item_message(delivered_item)
    metadata = {"queue_item_id": item_id, **queue_item_metadata(delivered_item)}
    await append_history(
        queue_meta["get_event"],
        queue_name,
        message,
        metadata,
        port=port,
        git_context=git_context,
    )
    return {
        "message": message,
        "id": item_id,
        "conversation_phone": conversation_phone,
        "from_phone": metadata.get("from_phone"),
        "to_phone": metadata.get("to_phone"),
        "queue": queue_name,
        "cycle_id": metadata.get("cycle_id"),
        "task_id": metadata.get("task_id"),
        "task_node_id": metadata.get("task_node_id"),
        "parent_task_id": metadata.get("parent_task_id"),
        "parent_task_node_id": metadata.get("parent_task_node_id"),
        "metadata": deepcopy(metadata),
    }


async def dequeue_phone_channel(
    queue_name: str,
    conversation_phone: str,
    to_phone: str,
    port: int | None = None,
    git_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    async with group_task_submission_lock:
        return await _dequeue_phone_channel_unlocked(
            queue_name,
            conversation_phone,
            to_phone,
            port,
            git_context,
        )


async def _delete_queued_item_unlocked(
    queue_name: str,
    item_id: str,
    port: int | None = None,
    git_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    queue_meta = queue_definition(queue_name)
    target_context_key = (
        git_context_key_from_metadata(git_context)
        if isinstance(git_context, dict)
        else ""
    )
    deleted_item: Any | None = None
    async with locks[queue_name]:
        kept_items: deque[Any] = deque()
        while queues[queue_name]:
            item = queues[queue_name].popleft()
            if (
                deleted_item is None
                and queue_item_id(item) == item_id
                and (
                    not target_context_key
                    or queue_item_matches_git_context(item, target_context_key)
                )
            ):
                deleted_item = item
                continue
            kept_items.append(item)
        queues[queue_name] = kept_items
        size = len(queues[queue_name])

    if deleted_item is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Queue item not found",
        )

    message = queue_item_message(deleted_item)
    deleted_metadata = queue_item_metadata(deleted_item)
    deleted_git_context = {
        key: deleted_metadata.get(key)
        for key in (
            "fastapi_port",
            "project_name",
            "git_context_key",
            "git_address",
            "git_commit",
            "git_commit_short",
            "git_error",
        )
        if deleted_metadata.get(key) is not None
    }
    await append_history(
        f"removed_from_{queue_meta['context']}_queue",
        queue_name,
        message,
        {
            "queue_item_id": item_id,
            **deleted_metadata,
            "sender": "Owner",
            "receiver": queue_meta["default_receiver"],
            "action": "deleted_from_queue",
        },
        port=port,
        git_context=git_context or deleted_git_context or None,
    )
    return {"status": "deleted", "queue": queue_name, "id": item_id, "size": size}


async def delete_queued_item(
    queue_name: str,
    item_id: str,
    port: int | None = None,
    git_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    async with group_task_submission_lock:
        return await _delete_queued_item_unlocked(
            queue_name,
            item_id,
            port,
            git_context,
        )


async def remove_project_actor_queue_items(
    queue_context: dict[str, Any],
    actor_ids: set[str],
    actor_phones: set[str],
    *,
    action: str,
) -> list[dict[str, Any]]:
    if not actor_ids and not actor_phones:
        return []
    context_key = git_context_key_from_metadata(queue_context)
    removed_items: list[tuple[str, Any]] = []
    acquired: list[asyncio.Lock] = []
    try:
        for queue_name in sorted(GROUP_QUEUE_NAMES):
            await locks[queue_name].acquire()
            acquired.append(locks[queue_name])
        for queue_name in sorted(GROUP_QUEUE_NAMES):
            kept: deque[Any] = deque()
            while queues[queue_name]:
                item = queues[queue_name].popleft()
                metadata = queue_item_metadata(item)
                item_context = git_context_key_from_metadata(metadata)
                item_actor_id = str(metadata.get("to_agent_id") or "").strip()
                item_phone = str(metadata.get("to_phone") or "").strip()
                actor_matches = (
                    item_actor_id in actor_ids
                    if item_actor_id
                    else item_phone in actor_phones
                )
                if actor_matches and (not context_key or item_context == context_key):
                    removed_items.append((queue_name, item))
                    continue
                kept.append(item)
            queues[queue_name] = kept
    finally:
        for queue_lock in reversed(acquired):
            queue_lock.release()

    removed: list[dict[str, Any]] = []
    for queue_name, item in removed_items:
        metadata = deepcopy(queue_item_metadata(item))
        removed.append(
            {
                "queue": queue_name,
                "id": queue_item_id(item),
                "task_id": metadata.get("task_id"),
                "to_agent_id": metadata.get("to_agent_id"),
                "to_phone": metadata.get("to_phone"),
            }
        )
        await append_history(
            f"removed_from_{QUEUE_DEFINITIONS[queue_name]['context']}_queue",
            queue_name,
            deepcopy(queue_item_message(item)),
            {
                "queue_item_id": queue_item_id(item),
                **metadata,
                "action": action,
            },
            git_context=queue_context,
        )
    return removed


async def enqueue_sequential_agent_node(
    agent: dict[str, Any],
    project_phone: str,
    queue_context: dict[str, Any],
    assignment: dict[str, Any],
    *,
    source: str,
    port: int | None = None,
) -> dict[str, Any]:
    agent_id = str(agent.get("id") or "").strip()
    agent_name = str(agent.get("name") or "").strip()
    logical_agent_phone = str(agent.get("phone") or "").strip()
    agent_parameters = (
        agent.get("parameters")
        if isinstance(agent.get("parameters"), dict)
        else {}
    )
    git_branch = str(
        agent.get("git_branch") or agent_parameters.get("git_branch") or ""
    ).strip()
    role_agent_ids = [
        str(agent_id_value).strip()
        for agent_id_value in assignment.get("role_agent_ids", [])
        if str(agent_id_value).strip()
    ]
    try:
        node_index = role_agent_ids.index(agent_id) + 1
    except ValueError:
        node_index = len(assignment.get("completed_agent_ids", [])) + 1
    node_count = len(role_agent_ids)
    assignment_id = str(assignment.get("current_assignment_id") or "").strip()
    tasks = deepcopy(agent.get("tasks") or [])
    task_lines = [
        f"{index}. [{str(task.get('queue') or 'worker-all')}] "
        f"{str(task.get('message') or '').strip()}"
        for index, task in enumerate(tasks, start=1)
        if isinstance(task, dict)
    ]
    if not task_lines:
        task_lines = ["Заданий для этого узла нет."]
    profile = str(agent.get("profile") or "").strip()
    workflow = (
        assignment.get("workflow")
        if isinstance(assignment.get("workflow"), dict)
        else {}
    )
    phase = str(assignment.get("phase") or "node")
    current_node_id = str(assignment.get("current_node_id") or "").strip()
    pending_transition = (
        assignment.get("pending_transition")
        if isinstance(assignment.get("pending_transition"), dict)
        else {}
    )
    graph_nodes = [
        node for node in workflow.get("nodes", []) if isinstance(node, dict)
    ]
    if graph_nodes and current_node_id:
        node_count = len(graph_nodes)
        node_index = next(
            (
                index
                for index, node in enumerate(graph_nodes, start=1)
                if str(node.get("id") or "") == current_node_id
            ),
            node_index,
        )
    graph_node = next(
        (
            node
            for node in graph_nodes
            if str(node.get("id") or "") == current_node_id
        ),
        {},
    )
    if phase == "review":
        reviews = [
            review
            for review in pending_transition.get("reviews", [])
            if isinstance(review, dict)
        ]
        review_number = len(reviews) + 1
        source_task_lines = [
            f"{index}. [{str(task.get('queue') or 'worker-all')}] "
            f"{str(task.get('message') or '').strip()}"
            for index, task in enumerate(
                pending_transition.get("source_tasks", []), start=1
            )
            if isinstance(task, dict)
        ] or ["Задачи исходного узла не указаны."]
        previous_review_lines = [
            f"- {review.get('reviewer_name')}: {review.get('decision')}"
            + (
                f" — {review.get('feedback')}"
                if str(review.get("feedback") or "").strip()
                else ""
            )
            for review in reviews
        ] or ["- Предыдущих решений нет."]
        review_context = (
            pending_transition.get("review_context")
            if isinstance(pending_transition.get("review_context"), dict)
            else {}
        )
        review_submission = (
            review_context.get("submission")
            if isinstance(review_context.get("submission"), dict)
            else {}
        )
        submitted_from_commit = str(
            review_submission.get("from_commit") or ""
        ).casefold()
        submitted_git_commit = str(
            review_submission.get("git_commit") or ""
        ).casefold()
        review_patches = [
            patch
            for patch in review_context.get("patches", [])
            if isinstance(patch, dict)
        ]
        matching_patches = [
            patch
            for patch in review_patches
            if (
                not submitted_from_commit
                or str(
                    (patch.get("from_commit") or {}).get("full")
                    if isinstance(patch.get("from_commit"), dict)
                    else patch.get("from_commit")
                    or ""
                ).casefold()
                == submitted_from_commit
            )
            and (
                not submitted_git_commit
                or str(
                    (patch.get("to_commit") or {}).get("full")
                    if isinstance(patch.get("to_commit"), dict)
                    else patch.get("to_commit")
                    or ""
                ).casefold()
                == submitted_git_commit
            )
        ]
        if (
            not matching_patches
            and review_patches
            and not submitted_from_commit
            and not submitted_git_commit
        ):
            matching_patches = [review_patches[-1]]
        review_patch_blocks: list[str] = []
        for patch in matching_patches:
            from_info = patch.get("from_commit")
            to_info = patch.get("to_commit")
            from_info = from_info if isinstance(from_info, dict) else {
                "full": str(from_info or ""),
                "short": str(from_info or "")[:12],
            }
            to_info = to_info if isinstance(to_info, dict) else {
                "full": str(to_info or ""),
                "short": str(to_info or "")[:12],
            }
            header = history_patch_block_header(
                from_info,
                to_info,
                str(patch.get("git_address") or ""),
                patch,
            )
            if patch.get("status") == "available":
                review_patch_blocks.append(
                    f"{header}\n\n{str(patch.get('patch') or '')}\n[END PATCH]"
                )
            else:
                review_patch_blocks.append(
                    f"{header}\nStatus: unavailable\n"
                    f"Reason: {patch.get('reason') or 'unknown'}\n[END PATCH]"
                )
        review_patch_text = "\n\n---\n\n".join(review_patch_blocks)
        review_context_lines = [
            "Изменения кода для этого перехода:",
            (
                f"Сообщений: {int(review_context.get('record_count') or 0)}; "
                f"patch-блоков: {int(review_context.get('patch_count') or 0)}; "
                f"недоступных patch-блоков: "
                f"{int(review_context.get('patch_error_count') or 0)}."
            ),
            review_patch_text or "Для этого перехода новый patch-блок отсутствует.",
            (
                "Полная история сообщений с дедуплицированными patch-блоками: "
                "project_state.execution.pending_transition.review_context.text."
            ),
        ]
        graph_lines = [
            f"- {node.get('id')} ({node.get('agent_name') or node.get('agent_id')}): "
            + ", ".join(
                f"{outcome} -> {target}"
                for outcome, target in (node.get("transitions") or {}).items()
            )
            for node in graph_nodes
        ]
        decision_example = json.dumps(
            {
                "assignment_id": assignment_id,
                "status": "APPROVE",
                "feedback": "Переход проверен",
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )
        message = "\n".join(
            [
                f"ПОСЛЕДОВАТЕЛЬНЫЙ РЕЖИМ: РЕВЬЮ ПЕРЕХОДА {review_number} ИЗ 2",
                (
                    f"Сейчас вы агент {agent_name} "
                    f"(id={agent_id}, logical_phone={logical_agent_phone})."
                ),
                f"Проект: {queue_context.get('project_name') or project_phone}.",
                f"Git: {queue_context.get('git_address') or 'не указан'}.",
                f"Git context: {queue_context.get('git_context_key') or 'не указан'}.",
                f"Телефон проекта: {project_phone}.",
                "Профиль ревьювера:",
                profile or "Профиль не задан.",
                "Предлагаемый переход:",
                (
                    f"{pending_transition.get('source_node_id')} --"
                    f"{pending_transition.get('outcome')}--> "
                    f"{pending_transition.get('target_node_id')}"
                ),
                f"Исходный агент: {pending_transition.get('source_agent_name')}.",
                f"Ветка исходного агента: {pending_transition.get('source_git_branch') or 'не указана'}.",
                "Профиль исходного агента:",
                str(pending_transition.get("source_agent_profile") or "Профиль не задан."),
                "Задачи исходного узла:",
                *source_task_lines,
                "Результат исходного агента:",
                str(pending_transition.get("result") or "Результат не приложен."),
                *review_context_lines,
                "Полный граф проекта:",
                *graph_lines,
                "Решения предыдущих ревьюверов:",
                *previous_review_lines,
                (
                    "Отправьте решение в текущий whoami endpoint. Допустимы только "
                    "APPROVE или REJECT; при REJECT поле feedback обязательно."
                ),
                f"POST /api/v1/projects/{project_phone}/agents/{logical_agent_phone}/whoami",
                decision_example,
            ]
        )
    else:
        allowed_outcomes = sorted(
            str(outcome).upper()
            for outcome in (graph_node.get("transitions") or {})
        )
        last_transition = (
            assignment.get("last_transition")
            if isinstance(assignment.get("last_transition"), dict)
            else {}
        )
        transition_context: list[str] = []
        if last_transition and str(
            last_transition.get("applied_target_node_id") or ""
        ) == current_node_id:
            transition_context = [
                "Контекст предыдущего перехода/доработки:",
                f"Исходный узел: {last_transition.get('source_node_id')}.",
                f"Результат: {last_transition.get('result') or 'не приложен'}.",
                f"Замечание: {last_transition.get('feedback') or 'нет'}.",
                "Решения ревьюверов: "
                + "; ".join(
                    f"{review.get('reviewer_name')}={review.get('decision')}"
                    + (
                        f" ({review.get('feedback')})"
                        if str(review.get("feedback") or "").strip()
                        else ""
                    )
                    for review in last_transition.get("reviews", [])
                    if isinstance(review, dict)
                ),
            ]
        completion_example = json.dumps(
            {
                "assignment_id": assignment_id,
                "status": allowed_outcomes[0] if allowed_outcomes else "DONE",
                "result": "Описание выполненной работы и проверок",
                "from_commit": (
                    queue_context.get("git_commit")
                    or "<commit до начала работы>"
                ),
                "git_commit": "<commit с выполненной работой>",
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )
        message = "\n".join(
            [
                "ПОСЛЕДОВАТЕЛЬНЫЙ РЕЖИМ: НОВЫЙ УЗЕЛ ГРАФА",
                (
                    f"Сейчас вы агент {agent_name} "
                    f"(id={agent_id}, logical_phone={logical_agent_phone})."
                ),
                f"Проект: {queue_context.get('project_name') or project_phone}.",
                f"Git: {queue_context.get('git_address') or 'не указан'}.",
                f"Узел графа: {current_node_id or node_index} ({node_index} из {node_count}).",
                f"Рабочая Git-ветка: {git_branch or 'не назначена'}.",
                "Профиль текущей роли:",
                profile or "Профиль не задан.",
                "Задания текущего узла:",
                *task_lines,
                *transition_context,
                "Допустимые результаты: " + ", ".join(allowed_outcomes or ["DONE"]),
                (
                    "После выполнения отправьте результат в текущий whoami endpoint. "
                    "Система выдаст две последовательные роли ревьюверов и применит "
                    "переход только после двух APPROVE."
                ),
                (
                    "Для удалённого репозитория передайте from_commit и git_commit; "
                    "для локального клона HEAD определяется автоматически."
                ),
                f"POST /api/v1/projects/{project_phone}/agents/{logical_agent_phone}/whoami",
                completion_example,
            ]
        )
    queued = await enqueue_phone_channel(
        "worker-all",
        project_phone,
        message,
        {
            "submitted_via": source,
            "action": (
                "sequential_transition_review_assigned"
                if phase == "review"
                else "sequential_agent_node_assigned"
            ),
            "assignment_mode": "sequential",
            "sequential_identity_queue": True,
            "identity_kind": (
                "transition_review" if phase == "review" else "graph_node"
            ),
            "assignment_id": assignment_id,
            "sender": PROJECT_MANAGER_AGENT_NAME,
            "receiver": agent_name,
            "from_phone": PROJECT_MANAGER_PHONE,
            "to_phone": project_phone,
            "to_agent_id": agent_id,
            "logical_to_phone": logical_agent_phone,
            "to_agent_git_branch": git_branch,
            "from_commit": queue_context.get("git_commit"),
            "graph_node_index": node_index,
            "graph_node_count": node_count,
            "graph_node_id": current_node_id,
            "phase": phase,
            "workflow": deepcopy(workflow),
            "pending_transition": (
                {
                    **deepcopy(pending_transition),
                    "review_context": {
                        key: deepcopy(review_context.get(key))
                        for key in (
                            "schema_version",
                            "generated_at",
                            "record_count",
                            "patch_count",
                            "patch_error_count",
                            "deduplicated_by_commit_pair",
                            "submission",
                        )
                        if review_context.get(key) is not None
                    },
                }
                if pending_transition
                else None
            ),
            "allowed_outcomes": sorted(
                str(outcome).upper()
                for outcome in (graph_node.get("transitions") or {})
            ),
            "task_ids": [
                str(task.get("task_id") or "").strip()
                for task in tasks
                if isinstance(task, dict) and str(task.get("task_id") or "").strip()
            ],
            "tasks": tasks,
            "agent": {
                "id": agent_id,
                "name": agent_name,
                "phone": logical_agent_phone,
                "git_branch": git_branch,
                "profile": profile,
            },
            "whoami_endpoint": (
                f"/api/v1/projects/{project_phone}/agents/"
                f"{logical_agent_phone}/whoami"
            ),
            "sequential_poll_endpoint": (
                f"/worker/all/{project_phone}?to_phone={project_phone}"
            ),
            "status": "QUEUED",
        },
        port,
        queue_context,
    )
    if isinstance(workflow, dict) and workflow.get("enabled"):
        # The active graph node/review must be consumed before one-time reviewer
        # bootstrap cards that may already be waiting in the shared queue.
        async with locks["worker-all"]:
            promoted_item: Any | None = None
            remaining: deque[Any] = deque()
            while queues["worker-all"]:
                queue_item = queues["worker-all"].popleft()
                if (
                    promoted_item is None
                    and str(queue_item_id(queue_item) or "") == str(queued["id"])
                ):
                    promoted_item = queue_item
                else:
                    remaining.append(queue_item)
            if promoted_item is not None:
                remaining.appendleft(promoted_item)
            queues["worker-all"] = remaining
    return {
        "agent_id": agent_id,
        "agent_name": agent_name,
        "logical_agent_phone": logical_agent_phone,
        "delivery_phone": project_phone,
        "assignment_id": assignment_id,
        "graph_node_index": node_index,
        "graph_node_count": node_count,
        "graph_node_id": current_node_id,
        "phase": phase,
        "task_count": len(tasks),
        "task_ids": [
            str(task.get("task_id") or "").strip()
            for task in tasks
            if isinstance(task, dict) and str(task.get("task_id") or "").strip()
        ],
        "queue": queued["queue"],
        "queue_item_id": queued["id"],
        "poll_endpoint": f"/worker/all/{project_phone}?to_phone={project_phone}",
    }


async def enqueue_sequential_reviewer_bootstrap(
    reviewer: dict[str, Any],
    project_phone: str,
    queue_context: dict[str, Any],
    assignment: dict[str, Any],
    *,
    reviewer_index: int,
    source: str,
    port: int | None = None,
) -> dict[str, Any]:
    reviewer_id = str(reviewer.get("id") or "").strip()
    reviewer_name = str(reviewer.get("name") or "").strip()
    reviewer_phone = str(reviewer.get("phone") or "").strip()
    reviewer_parameters = (
        reviewer.get("parameters")
        if isinstance(reviewer.get("parameters"), dict)
        else {}
    )
    git_branch = str(
        reviewer.get("git_branch")
        or reviewer_parameters.get("git_branch")
        or ""
    ).strip()
    state_endpoint = f"/api/v1/projects/{project_phone}/state.json"
    review_endpoint = (
        f"/api/v1/projects/{project_phone}/agents/{reviewer_phone}/whoami"
    )
    message = "\n".join(
        [
            "ПОСЛЕДОВАТЕЛЬНЫЙ РЕЖИМ: ИНИЦИАЛИЗАЦИЯ РЕВЬЮВЕРА",
            (
                f"Сейчас вы ревьювер {reviewer_name} "
                f"(ревьювер {reviewer_index} из 2, id={reviewer_id}, "
                f"logical_phone={reviewer_phone})."
            ),
            "Вы сохраняете роль ревьювера на всё время проекта.",
            (
                f"Полный актуальный JSON проекта: GET {state_endpoint}. "
                "Он содержит граф, всех агентов, выполненную работу, решения "
                "ревьюверов, очереди и последние события."
            ),
            (
                "Новые переходы проверяйте по своему logical_phone. "
                f"Решение APPROVE или REJECT отправляйте в POST {review_endpoint}."
            ),
            "Профиль ревьювера:",
            str(reviewer.get("profile") or "Профиль не задан.").strip(),
        ]
    )
    queued = await enqueue_phone_channel(
        "worker-all",
        project_phone,
        message,
        {
            "submitted_via": source,
            "action": "sequential_reviewer_bootstrap",
            "assignment_mode": "sequential",
            "sequential_identity_queue": True,
            "identity_kind": "reviewer_bootstrap",
            "reviewer_index": reviewer_index,
            "sender": PROJECT_MANAGER_AGENT_NAME,
            "receiver": reviewer_name,
            "from_phone": PROJECT_MANAGER_PHONE,
            "to_phone": project_phone,
            "to_agent_id": reviewer_id,
            "logical_to_phone": reviewer_phone,
            "to_agent_git_branch": git_branch,
            "project_state_endpoint": state_endpoint,
            "review_endpoint": review_endpoint,
            "agent": {
                "id": reviewer_id,
                "name": reviewer_name,
                "phone": reviewer_phone,
                "git_branch": git_branch,
                "profile": reviewer.get("profile"),
            },
            "workflow": deepcopy(assignment.get("workflow")),
            "status": "READY",
        },
        port,
        queue_context,
    )
    return {
        "agent_id": reviewer_id,
        "agent_name": reviewer_name,
        "logical_agent_phone": reviewer_phone,
        "delivery_phone": project_phone,
        "identity_kind": "reviewer_bootstrap",
        "reviewer_index": reviewer_index,
        "task_count": 0,
        "queue": queued["queue"],
        "queue_item_id": queued["id"],
        "project_state_endpoint": state_endpoint,
        "review_endpoint": review_endpoint,
    }


def sprint_payload_identity(
    payload: dict[str, Any],
    source_filename: str,
    sequence: int,
) -> tuple[str, str]:
    raw_sprint = payload.get("sprint")
    sprint_object = raw_sprint if isinstance(raw_sprint, dict) else {}
    external_id = str(
        sprint_object.get("id")
        or payload.get("sprint_id")
        or (raw_sprint if isinstance(raw_sprint, str) else "")
        or ""
    ).strip()
    title = str(
        sprint_object.get("title")
        or sprint_object.get("name")
        or payload.get("sprint_title")
        or payload.get("sprint_name")
        or ""
    ).strip()
    if not title and source_filename:
        title = Path(source_filename).stem.strip()
    return external_id, title or f"Спринт {sequence}"


def project_state_has_sprint_content(state: dict[str, Any]) -> bool:
    if state.get("agents"):
        return True
    pending = state.get("pending_work")
    if isinstance(pending, dict) and any(pending.values()):
        return True
    execution = state.get("execution")
    if isinstance(execution, dict):
        if execution.get("assignments") or execution.get("workflow"):
            return True
        if execution.get("role_agent_ids") or execution.get("current_agent_id"):
            return True
    return False


def sprint_record_summary(record: dict[str, Any]) -> dict[str, Any]:
    summary = {
        key: deepcopy(record.get(key))
        for key in (
            "id",
            "sequence",
            "external_id",
            "title",
            "status",
            "source",
            "source_filename",
            "imported_at",
            "archived_at",
            "assignment_mode",
            "agent_count",
            "task_count",
            "legacy",
        )
    }
    code_history = record.get("final_code_history")
    summary["code_patch_count"] = (
        int(code_history.get("patch_count") or 0)
        if isinstance(code_history, dict)
        else None
    )
    summary["code_patch_error_count"] = (
        int(code_history.get("patch_error_count") or 0)
        if isinstance(code_history, dict)
        else None
    )
    return summary


def record_project_sprint_import_file(
    *,
    context_key: str,
    project_phone: str,
    project_name: str,
    payload: dict[str, Any],
    source: str,
    source_filename: str,
    previous_state: dict[str, Any],
    previous_code_history: dict[str, Any],
    current_state: dict[str, Any],
    assignment_mode: str,
    agent_count: int,
    task_count: int,
) -> dict[str, Any]:
    with sprint_history_file_lock():
        history = read_sprint_history_file()
        raw_projects = history.get("projects")
        projects = dict(raw_projects) if isinstance(raw_projects, dict) else {}
        raw_project = projects.get(context_key)
        project = dict(raw_project) if isinstance(raw_project, dict) else {}
        raw_records = project.get("sprints")
        records = [
            deepcopy(record)
            for record in raw_records
            if isinstance(record, dict)
        ] if isinstance(raw_records, list) else []
        now = utc_now()
        current_record = next(
            (
                record
                for record in reversed(records)
                if str(record.get("status") or "") == "current"
            ),
            None,
        )
        if current_record is not None:
            current_record["status"] = "archived"
            current_record["archived_at"] = now
            current_record["final_state"] = deepcopy(previous_state)
            current_record["final_code_history"] = deepcopy(previous_code_history)
        elif project_state_has_sprint_content(previous_state):
            legacy_sequence = max(
                (int(record.get("sequence") or 0) for record in records),
                default=0,
            ) + 1
            records.append(
                {
                    "id": f"sprint-{legacy_sequence:04d}-{uuid4().hex[:8]}",
                    "sequence": legacy_sequence,
                    "external_id": "",
                    "title": "Состояние до первого сохранённого импорта",
                    "status": "archived",
                    "source": "legacy-runtime",
                    "source_filename": "",
                    "imported_at": None,
                    "archived_at": now,
                    "assignment_mode": str(
                        (previous_state.get("execution") or {}).get("mode")
                        or "parallel"
                    ),
                    "agent_count": len(previous_state.get("agents") or []),
                    "task_count": sum(
                        len(agent.get("tasks") or [])
                        for agent in previous_state.get("agents") or []
                        if isinstance(agent, dict)
                    ),
                    "legacy": True,
                    "import_payload": None,
                    "initial_state": None,
                    "final_state": deepcopy(previous_state),
                    "final_code_history": deepcopy(previous_code_history),
                }
            )

        sequence = max(
            (int(record.get("sequence") or 0) for record in records),
            default=0,
        ) + 1
        external_id, title = sprint_payload_identity(
            payload,
            source_filename,
            sequence,
        )
        new_record = {
            "id": f"sprint-{sequence:04d}-{uuid4().hex[:8]}",
            "sequence": sequence,
            "external_id": external_id,
            "title": title,
            "status": "current",
            "source": source,
            "source_filename": source_filename,
            "imported_at": now,
            "archived_at": None,
            "assignment_mode": assignment_mode,
            "agent_count": agent_count,
            "task_count": task_count,
            "legacy": False,
            "import_payload": deepcopy(payload),
            "initial_state": deepcopy(current_state),
            "final_state": None,
            "final_code_history": None,
        }
        records.append(new_record)
        project.update(
            {
                "project_phone": project_phone,
                "project_name": project_name,
                "git_context_key": context_key,
                "current_sprint_id": new_record["id"],
                "updated_at": now,
                "sprints": records,
            }
        )
        projects[context_key] = project
        history["schema_version"] = 1
        history["projects"] = projects
        write_sprint_history_file(history)
        return sprint_record_summary(new_record)


async def record_project_sprint_import(
    *,
    context_key: str,
    project_phone: str,
    project_name: str,
    payload: dict[str, Any],
    source: str,
    source_filename: str,
    previous_state: dict[str, Any],
    current_state: dict[str, Any],
    assignment_mode: str,
    agent_count: int,
    task_count: int,
) -> dict[str, Any]:
    previous_project = previous_state.get("project")
    previous_git_address = str(
        previous_project.get("git_address")
        if isinstance(previous_project, dict)
        else ""
    ).strip()
    previous_records = previous_state.get("recent_activity")
    if not isinstance(previous_records, list):
        previous_records = []
    previous_code_history = previous_state.get("history_with_patches")
    if not isinstance(previous_code_history, dict):
        previous_code_history = await asyncio.to_thread(
            history_with_patches_context,
            previous_records,
            previous_git_address,
        )
    async with sprint_history_lock:
        return await asyncio.to_thread(
            record_project_sprint_import_file,
            context_key=context_key,
            project_phone=project_phone,
            project_name=project_name,
            payload=payload,
            source=source,
            source_filename=source_filename,
            previous_state=previous_state,
            previous_code_history=previous_code_history,
            current_state=current_state,
            assignment_mode=assignment_mode,
            agent_count=agent_count,
            task_count=task_count,
        )


async def import_project_actors_data(
    project_id: str,
    payload: Any,
    *,
    source: str = "api",
    source_filename: str = "",
    port: int | None = None,
    activate_sequential: bool = True,
    expected_git_context_key: str = "",
    expected_repository_key: str = "",
) -> dict[str, Any]:
    options = actor_import_options(payload)
    options["source"] = source
    options["expected_git_context_key"] = expected_git_context_key
    options["expected_repository_key"] = expected_repository_key
    async with group_task_submission_lock:
        previous_state = await project_state_json(project_id, history_limit=10000)
        result = await run_group_write_transaction(
            project_actor_mutation_transaction,
            project_id,
            options,
        )
        removed_tasks = await remove_project_actor_queue_items(
            result["queue_context"],
            set(result["removed_actor_ids"]),
            set(result["removed_actor_phones"]),
            action="removed_by_actor_import_overwrite",
        )
        queued_tasks: list[dict[str, Any]] = []
        if (
            activate_sequential
            and result.get("assignment_mode") == "sequential"
            and result["imported_agents"]
        ):
            initial_state = result.get("assignment") or {}
            initial_workflow = initial_state.get("workflow")
            first_agent_id = ""
            if isinstance(initial_workflow, dict) and initial_workflow.get("enabled"):
                start_node_id = str(
                    initial_workflow.get("start_node_id") or ""
                ).strip()
                start_node = next(
                    (
                        node
                        for node in initial_workflow.get("nodes", [])
                        if isinstance(node, dict)
                        and str(node.get("id") or "").strip() == start_node_id
                    ),
                    None,
                )
                first_agent_id = str((start_node or {}).get("agent_id") or "").strip()
            if not first_agent_id:
                first_agent_id = next(
                    (
                        str(agent_id).strip()
                        for agent_id in initial_state.get("role_agent_ids", [])
                        if str(agent_id).strip()
                    ),
                    "",
                )
            first_agent = next(
                (
                    agent
                    for agent in result["imported_agents"]
                    if str(agent.get("id") or "").strip() == first_agent_id
                ),
                result["imported_agents"][0],
            )
            activated = await run_group_write_transaction(
                sequential_agent_assignment_transaction,
                project_id,
                str(first_agent.get("phone") or "").strip(),
                False,
                utc_now(),
            )
            active_agent = activated.get("agent")
            if not isinstance(active_agent, dict):
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Sequential graph could not activate its first node",
                )
            workflow = activated.get("assignment", {}).get("workflow")
            if (
                isinstance(workflow, dict)
                and workflow.get("enabled")
                and workflow.get("initialize_reviewers") is True
            ):
                agents_by_id = {
                    str(agent.get("id") or "").strip(): agent
                    for agent in activated.get("agents", [])
                    if isinstance(agent, dict)
                }
                for reviewer_index, reviewer_id in enumerate(
                    workflow.get("reviewer_agent_ids", []),
                    start=1,
                ):
                    reviewer = agents_by_id.get(str(reviewer_id).strip())
                    if reviewer is None:
                        raise HTTPException(
                            status_code=status.HTTP_409_CONFLICT,
                            detail="A conditional graph reviewer was not imported",
                        )
                    queued_tasks.append(
                        await enqueue_sequential_reviewer_bootstrap(
                            reviewer,
                            activated["project_phone"],
                            activated["queue_context"],
                            activated["assignment"],
                            reviewer_index=reviewer_index,
                            source=f"agent_json_import:{source}",
                            port=port,
                        )
                    )
            node = await enqueue_sequential_agent_node(
                active_agent,
                activated["project_phone"],
                activated["queue_context"],
                activated["assignment"],
                source=f"agent_json_import:{source}",
                port=port,
            )
            queued_tasks.append(node)
            result["assignment"] = deepcopy(activated["assignment"])
            result["active_agent"] = deepcopy(active_agent)
        immediately_queued_agents = (
            result["imported_agents"]
            if result.get("assignment_mode") != "sequential"
            else []
        )
        for actor in immediately_queued_agents:
            actor_id = str(actor.get("id") or "").strip()
            actor_name = str(actor.get("name") or "").strip()
            actor_phone = str(actor.get("phone") or "").strip()
            actor_git_branch = str(
                actor.get("git_branch")
                or actor.get("parameters", {}).get("git_branch")
                or ""
            ).strip()
            for task in actor.get("tasks", []):
                metadata = task.get("metadata") if isinstance(task.get("metadata"), dict) else {}
                task_id = str(task.get("task_id") or "").strip()
                queued = await enqueue_phone_channel(
                    str(task.get("queue") or "worker-all"),
                    result["project_phone"],
                    str(task.get("message") or "").strip(),
                    {
                        **metadata,
                        "submitted_via": f"actor_json_import:{source}",
                        "sender": "Project Manager",
                        "receiver": actor_name,
                        "from_phone": PROJECT_MANAGER_PHONE,
                        "to_phone": actor_phone,
                        "from_agent_id": PROJECT_MANAGER_AGENT_ID,
                        "to_agent_id": actor_id,
                        "to_agent_git_branch": actor_git_branch,
                        "to_agent_profile_endpoint": (
                            f"/api/v1/projects/{result['project_phone']}/agents/{actor_phone}"
                        ),
                        "task_id": task_id,
                        "request_id": task.get("request_id"),
                        "status": task.get("status") or "QUEUED",
                    },
                    port,
                    result["queue_context"],
                )
                queued_tasks.append(
                    {
                        "agent_id": actor_id,
                        "agent_name": actor_name,
                        "agent_phone": actor_phone,
                        "agent_git_branch": actor_git_branch,
                        "actor_id": actor_id,
                        "actor_name": actor_name,
                        "actor_phone": actor_phone,
                        "task_id": task_id,
                        "queue": queued["queue"],
                        "queue_item_id": queued["id"],
                    }
                )
        response = {
            key: value
            for key, value in result.items()
            if key not in {"queue_context", "removed_actor_ids", "removed_actor_phones"}
        } | {
            "source": source,
            "removed_agent_count": len(result["removed_agents"]),
            "imported_agent_count": len(result["imported_agents"]),
            "removed_actor_count": len(result["removed_agents"]),
            "imported_actor_count": len(result["imported_agents"]),
            "removed_task_count": len(removed_tasks),
            "queued_task_count": (
                sum(int(item.get("task_count") or 0) for item in queued_tasks)
                if result.get("assignment_mode") == "sequential"
                else len(queued_tasks)
            ),
            "queued_queue_item_count": len(queued_tasks),
            "deferred_task_count": (
                max(
                    0,
                    sum(
                        len(agent.get("tasks") or [])
                        for agent in result["imported_agents"]
                    )
                    - sum(int(item.get("task_count") or 0) for item in queued_tasks),
                )
                if result.get("assignment_mode") == "sequential"
                else 0
            ),
            "sequential_poll_endpoint": (
                f"/worker/all/{result['project_phone']}?to_phone={result['project_phone']}"
                if result.get("assignment_mode") == "sequential"
                else None
            ),
            "removed_tasks": removed_tasks,
            "queued_tasks": queued_tasks,
        }
        current_state = await project_state_json(project_id, history_limit=10000)
        context_key = git_context_key_from_metadata(result["queue_context"])
        response["sprint"] = await record_project_sprint_import(
            context_key=context_key,
            project_phone=result["project_phone"],
            project_name=str(result["project"].get("project_name") or "").strip(),
            payload=payload,
            source=source,
            source_filename=safe_attachment_filename(source_filename)
            if source_filename
            else "",
            previous_state=previous_state,
            current_state=current_state,
            assignment_mode=str(result.get("assignment_mode") or "parallel"),
            agent_count=len(result["imported_agents"]),
            task_count=sum(
                len(agent.get("tasks") or [])
                for agent in result["imported_agents"]
            ),
        )
        return response


async def delete_project_actors_data(
    project_id: str,
    *,
    include_managed: bool,
) -> dict[str, Any]:
    options = {
        "overwrite": True,
        "include_managed": include_managed,
        "actors": [],
        "source": "bulk-delete",
    }
    async with group_task_submission_lock:
        result = await run_group_write_transaction(
            project_actor_mutation_transaction,
            project_id,
            options,
            True,
        )
        removed_tasks = await remove_project_actor_queue_items(
            result["queue_context"],
            set(result["removed_actor_ids"]),
            set(result["removed_actor_phones"]),
            action="removed_by_project_actor_delete",
        )
    return {
        key: value
        for key, value in result.items()
        if key not in {"queue_context", "removed_actor_ids", "removed_actor_phones"}
    } | {
        "deleted_agent_count": len(result["removed_agents"]),
        "deleted_actor_count": len(result["removed_agents"]),
        "removed_task_count": len(removed_tasks),
        "removed_tasks": removed_tasks,
    }


def json_object_from_text(raw_text: str) -> dict[str, Any]:
    text = raw_text.lstrip("\ufeff").strip()
    fenced = re.fullmatch(r"```(?:json)?\s*(.*?)\s*```", text, flags=re.DOTALL | re.IGNORECASE)
    if fenced:
        text = fenced.group(1).strip()
    if len(text.encode("utf-8")) > MAX_ACTOR_IMPORT_BYTES:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail="Agent import JSON is larger than 1 MB",
        )
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Telegram message does not contain valid JSON: {exc.msg}",
        ) from exc
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Agent import JSON must contain an object",
        )
    return payload


def telegram_api_json(token: str, method: str, data: dict[str, Any]) -> dict[str, Any]:
    encoded = urllib.parse.urlencode(data).encode("utf-8")
    request = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/{method}",
        data=encoded,
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read(MAX_ACTOR_IMPORT_BYTES + 1)
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Telegram Bot API request '{method}' failed",
        ) from exc
    if len(raw) > MAX_ACTOR_IMPORT_BYTES:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail="Telegram response is larger than 1 MB",
        )
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Telegram Bot API request '{method}' returned invalid JSON",
        ) from exc
    if not isinstance(payload, dict) or not payload.get("ok"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Telegram Bot API request '{method}' was rejected",
        )
    return payload


def telegram_document_payload(document: dict[str, Any]) -> dict[str, Any]:
    file_id = str(document.get("file_id") or "").strip()
    filename = str(document.get("file_name") or "agents.json").strip()
    if not file_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Telegram document file_id is missing",
        )
    if filename and not filename.lower().endswith(".json"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Telegram document must be a .json file",
        )
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not token:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="TELEGRAM_BOT_TOKEN is required to download Telegram documents",
        )
    file_result = telegram_api_json(token, "getFile", {"file_id": file_id})
    result = file_result.get("result")
    file_path = str(result.get("file_path") or "").strip() if isinstance(result, dict) else ""
    if not file_path:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Telegram did not return a document path",
        )
    url = f"https://api.telegram.org/file/bot{token}/{urllib.parse.quote(file_path, safe='/')}"
    try:
        with urllib.request.urlopen(url, timeout=30) as response:
            raw = response.read(MAX_ACTOR_IMPORT_BYTES + 1)
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Telegram JSON document download failed",
        ) from exc
    if len(raw) > MAX_ACTOR_IMPORT_BYTES:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail="Telegram JSON document is larger than 1 MB",
        )
    return json_object_from_text(raw.decode("utf-8"))


def actor_payload_from_telegram_update(update: Any) -> tuple[dict[str, Any], dict[str, Any]]:
    if not isinstance(update, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Telegram update must be a JSON object",
        )
    if "agents" in update or "actors" in update:
        return update, {}
    message = update.get("message") or update.get("channel_post")
    if not isinstance(message, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Telegram update has no message or channel_post",
        )
    text = message.get("text") or message.get("caption")
    if isinstance(text, str) and text.strip().startswith(("{", "```")):
        payload = json_object_from_text(text)
    elif isinstance(message.get("document"), dict):
        payload = telegram_document_payload(message["document"])
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Send agent JSON as message text or as a .json document",
        )
    return payload, message


def actor_import_reference_value(payload: dict[str, Any], key: str) -> Any:
    if key in payload:
        return payload.get(key)
    for section_name in ("agents", "actors"):
        section = payload.get(section_name)
        if isinstance(section, dict) and key in section:
            return section.get(key)
    return None


def actor_import_reference_items(
    payload: dict[str, Any],
    key: str,
) -> list[tuple[str, Any]]:
    references: list[tuple[str, Any]] = []
    if key in payload:
        references.append((key, payload.get(key)))
    for section_name in ("agents", "actors"):
        section = payload.get(section_name)
        if isinstance(section, dict) and key in section:
            references.append((f"{section_name}.{key}", section.get(key)))
    return references


async def project_phone_for_actor_import(
    payload: dict[str, Any],
    current_port: int | None = None,
) -> str:
    raw_project_id = (
        actor_import_reference_value(payload, "project_id")
        or actor_import_reference_value(payload, "project_phone")
    )
    raw_git_address = actor_import_reference_value(payload, "git_address")
    raw_context_key = actor_import_reference_value(payload, "git_context_key")

    context: dict[str, Any] | None = None
    if raw_git_address is not None:
        _, repository_key = normalize_project_git_address(raw_git_address)
        requested_context_key = normalize_requested_project_context_key(
            raw_context_key,
            repository_key,
        )
        config = await read_git_config()
        context = resolve_project_context_from_config(
            config,
            repository_key,
            requested_context_key,
            current_port,
        )
    elif raw_context_key is not None:
        if not isinstance(raw_context_key, str):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="git_context_key must be a string",
            )
        requested_context_key = normalize_project_context_reference(raw_context_key)
        if not requested_context_key:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="git_context_key must not be blank when provided",
            )
        config = await read_git_config()
        context = configured_git_context_for_key(
            config,
            requested_context_key,
            current_port,
        )

    if raw_git_address is not None or raw_context_key is not None:
        if context is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error": "project_not_found",
                    "message": (
                        "No registered project matches git_address/git_context_key; "
                        "resolve the project through Project Manager 0001 first"
                    ),
                },
            )
        project_phone = normalize_project_phone(context.get("project_phone"))
        if not project_phone:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "error": "project_phone_required",
                    "message": "Resolve the project through Project Manager 0001 first",
                },
            )
        if raw_project_id is not None:
            supplied_project_phone = normalize_project_phone(raw_project_id)
            if supplied_project_phone != project_phone:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "error": "project_reference_mismatch",
                        "message": (
                            "project_id/project_phone does not match the project "
                            "registered for git_address/git_context_key"
                        ),
                    },
                )
        return project_phone

    project_phone = str(raw_project_id or "").strip()
    if not project_phone:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "git_address (optionally git_context_key) or legacy "
                "project_id/project_phone is required in Telegram agent JSON"
            ),
        )
    return project_phone


def telegram_id_allowlist(variable_name: str) -> set[str]:
    return {
        value.strip()
        for value in os.getenv(variable_name, "").split(",")
        if value.strip()
    }


def ensure_telegram_sender_allowed(message: dict[str, Any]) -> None:
    allowed_chat_ids = telegram_id_allowlist("TELEGRAM_ALLOWED_CHAT_IDS")
    allowed_user_ids = telegram_id_allowlist("TELEGRAM_ALLOWED_USER_IDS")
    chat = message.get("chat") if isinstance(message, dict) else None
    sender = message.get("from") if isinstance(message, dict) else None
    chat_id = str(chat.get("id") if isinstance(chat, dict) else "").strip()
    user_id = str(sender.get("id") if isinstance(sender, dict) else "").strip()
    if allowed_chat_ids and chat_id not in allowed_chat_ids:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Telegram chat is not allowed to import project agents",
        )
    if allowed_user_ids and user_id not in allowed_user_ids:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Telegram user is not allowed to import project agents",
        )


def send_telegram_import_reply(message: dict[str, Any], result: dict[str, Any]) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat = message.get("chat") if isinstance(message, dict) else None
    chat_id = chat.get("id") if isinstance(chat, dict) else None
    if not token or chat_id is None:
        return
    reply = (
        f"Agents imported: {result['imported_agent_count']}; "
        f"removed: {result['removed_agent_count']}; "
        f"tasks queued: {result['queued_task_count']}."
    )
    try:
        telegram_api_json(token, "sendMessage", {"chat_id": chat_id, "text": reply})
    except HTTPException:
        return


@app.get("/", response_class=HTMLResponse)
async def index() -> HTMLResponse:
    return HTMLResponse(
        render_index_v2(),
        headers={
            "Cache-Control": "no-store, max-age=0",
            "Pragma": "no-cache",
        },
    )


@app.get("/queues")
async def get_queues(phone: str | None = None) -> dict[str, Any]:
    git_context = await git_context_for_phone(phone) if phone else None
    git_context_key = (
        git_context_key_from_metadata(git_context)
        if isinstance(git_context, dict)
        else ""
    )
    result: dict[str, int] = {}
    items: dict[str, list[dict[str, Any]]] = {}
    for queue_name in queues:
        async with locks[queue_name]:
            queue_items = queue_items_for_git_context(queues[queue_name], git_context_key)
            result[queue_name] = len(queue_items)
            items[queue_name] = [queue_item_snapshot(item) for item in queue_items]
    return {"queues": result, "items": items}


@app.get("/scheduled-tasks")
async def get_scheduled_tasks(phone: str | None = None) -> dict[str, Any]:
    git_context = await git_context_for_phone(phone) if phone else None
    git_context_key = (
        git_context_key_from_metadata(git_context)
        if isinstance(git_context, dict)
        else None
    )
    tasks = await list_scheduled_tasks(git_context_key)
    return {"tasks": tasks, "count": len(tasks)}


@app.delete("/scheduled-tasks/{task_id}")
async def delete_scheduled_task(task_id: str, request: Request) -> dict[str, Any]:
    return await cancel_scheduled_task(task_id, request_port(request))


@app.delete("/queues/{queue_name}/{item_id}")
async def delete_queue_item(
    queue_name: str,
    item_id: str,
    request: Request,
) -> dict[str, Any]:
    return await delete_queued_item(queue_name, item_id, request_port(request))


@app.delete("/queues/{queue_name}/{phone}/{item_id}")
async def delete_queue_item_for_phone(
    queue_name: str,
    phone: str,
    item_id: str,
    request: Request,
) -> dict[str, Any]:
    git_context = await git_context_for_phone(phone)
    return await delete_queued_item(
        queue_name,
        item_id,
        request_port(request),
        git_context,
    )


@app.get("/history")
async def get_history(
    limit: int = 200,
    date_from: str | None = None,
    date_to: str | None = None,
    git_context: str | None = None,
) -> list[dict[str, Any]]:
    try:
        if date_from:
            date.fromisoformat(date_from)
        if date_to:
            date.fromisoformat(date_to)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Dates must use YYYY-MM-DD format",
        ) from exc

    return await read_history(limit, date_from, date_to, git_context)


@app.delete("/history/{record_id}")
async def delete_history(record_id: str) -> dict[str, Any]:
    result = await delete_removed_backend_history_record(record_id)
    if result["deleted"]:
        return {"status": "deleted", "id": record_id, "remaining": result["remaining"]}

    if result["reason"] == "protected_event":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only removed_from_backend_queue history records can be deleted",
        )

    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="History record not found",
    )


@app.post("/history/{record_id}/restore-to-queue", status_code=status.HTTP_201_CREATED)
async def restore_history_to_queue(
    record_id: str,
    request: Request,
) -> dict[str, Any]:
    return await restore_history_record_to_queue(record_id, request, None)


@app.post("/history/{record_id}/restore-to-queue/{phone}", status_code=status.HTTP_201_CREATED)
async def restore_history_to_queue_for_phone(
    record_id: str,
    phone: str,
    request: Request,
) -> dict[str, Any]:
    return await restore_history_record_to_queue(record_id, request, phone)


async def restore_history_record_to_queue(
    record_id: str,
    request: Request,
    phone: str | None,
) -> dict[str, Any]:
    record = await find_history_record(record_id)
    if record is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="History record not found",
        )

    if record.get("event") != "removed_from_backend_queue":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only removed_from_backend_queue history records can be restored",
        )

    queue_name = record.get("queue")
    if queue_name not in queues:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Original queue is unknown",
        )

    record_metadata = git_context_metadata_from_record(record)
    metadata = {
        **record_metadata,
        "sender": "Owner",
        "receiver": queue_definition(queue_name)["default_receiver"],
        "action": "restored_to_original_queue",
        "restored_from_history_id": record_id,
    }
    phone_git_context = await git_context_for_phone(phone) if phone else None
    record_git_context = {
        key: record_metadata.get(key)
        for key in (
            "fastapi_port",
            "project_name",
            "git_context_key",
            "git_address",
            "git_commit",
            "git_commit_short",
            "git_error",
        )
        if record_metadata.get(key) is not None
    }
    if phone_git_context and record_git_context:
        record_context_key = git_context_key_from_metadata(record_git_context)
        phone_context_key = git_context_key_from_metadata(phone_git_context)
        if record_context_key and phone_context_key and record_context_key != phone_context_key:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Selected phone is mapped to a different Git context than this history record",
            )
    return await enqueue(
        queue_name,
        record.get("message"),
        metadata,
        request_port(request),
        phone_git_context or record_git_context or None,
    )


def project_agent_identity_snapshot_transaction(
    project_id: str,
    agent_phone: str,
) -> dict[str, Any]:
    with git_config_file_lock():
        with agents_file_lock():
            config = read_git_config_file()
            _, context_key, project_entry, context = project_for_group_api(
                config,
                project_id,
            )
            agents = read_agents_file()
            project_agents = full_agents_for_project(
                agents,
                context_key,
                phone_git_contexts_from_config(config),
            )
            clean_phone = agent_phone.strip()
            target_agent = next(
                (
                    agent
                    for agent in project_agents
                    if str(agent.get("phone") or "").strip() == clean_phone
                ),
                None,
            )
            if target_agent is None:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Agent phone was not found in this project",
                )
            project_phone = normalize_project_phone(project_entry.get("project_phone"))
            return {
                "project_id": project_phone,
                "project_phone": project_phone,
                "project": public_project_context(context),
                "context_key": context_key,
                "assignment_mode": str(
                    project_entry.get(PROJECT_AGENT_ASSIGNMENT_KEY, {}).get("mode")
                    if isinstance(
                        project_entry.get(PROJECT_AGENT_ASSIGNMENT_KEY), dict
                    )
                    else "parallel"
                )
                or "parallel",
                "assignment": deepcopy(
                    project_entry.get(PROJECT_AGENT_ASSIGNMENT_KEY, {})
                    if isinstance(
                        project_entry.get(PROJECT_AGENT_ASSIGNMENT_KEY), dict
                    )
                    else {}
                ),
                "agent": deepcopy(target_agent),
                "agents": deepcopy(project_agents),
                "queue_context": {
                    "queue_phone": project_phone,
                    "git_context_phone": project_phone,
                    "project_phone": project_phone,
                    "project_name": project_entry.get("project_name"),
                    "git_context_key": context_key,
                    "git_address": project_entry.get("git_address"),
                },
            }


def conditional_graph_assignment_transaction_locked(
    config: dict[str, Any],
    raw_key: str,
    context_key: str,
    project_entry: dict[str, Any],
    context: dict[str, Any],
    state: dict[str, Any],
    previous_agents: list[dict[str, Any]],
    original_agents: list[dict[str, Any]],
    project_agents: list[dict[str, Any]],
    requested_phone: str,
    complete_current: bool,
    seen_at: str,
    submitted_outcome: str,
    submitted_feedback: str,
    submitted_result: str,
    expected_assignment_id: str,
) -> dict[str, Any]:
    workflow = state.get("workflow")
    if not isinstance(workflow, dict) or not workflow.get("enabled"):
        raise RuntimeError("Conditional graph workflow is not configured")
    nodes = [
        deepcopy(node)
        for node in workflow.get("nodes", [])
        if isinstance(node, dict)
    ]
    nodes_by_id = {
        str(node.get("id") or "").strip(): node
        for node in nodes
        if str(node.get("id") or "").strip()
    }
    terminal_nodes = {
        str(node_id): deepcopy(node)
        for node_id, node in (workflow.get("terminal_nodes") or {}).items()
        if isinstance(node, dict)
    }
    reviewer_agent_ids = [
        str(agent_id).strip()
        for agent_id in workflow.get("reviewer_agent_ids", [])
        if str(agent_id).strip()
    ]
    if len(reviewer_agent_ids) != 2:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Conditional graph requires exactly two configured reviewers",
        )
    agents_by_id = {
        str(agent.get("id") or "").strip(): agent
        for agent in project_agents
        if str(agent.get("id") or "").strip()
    }
    missing_agents = [
        agent_id
        for agent_id in reviewer_agent_ids
        + [str(node.get("agent_id") or "").strip() for node in nodes]
        if agent_id not in agents_by_id
    ]
    if missing_agents:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"error": "workflow_agents_missing", "agent_ids": missing_agents},
        )

    assignments = [
        deepcopy(item)
        for item in state.get("assignments", [])
        if isinstance(item, dict)
    ]
    state["assignments"] = assignments
    visit_counts = {
        str(node_id): max(0, int(count or 0))
        for node_id, count in (state.get("visit_counts") or {}).items()
        if str(node_id)
    }
    state["visit_counts"] = visit_counts

    def update_agent_parameters(agent_id: str, **changes: Any) -> None:
        for index, stored_agent in enumerate(previous_agents):
            if str(stored_agent.get("id") or "").strip() != agent_id:
                continue
            updated = deepcopy(stored_agent)
            parameters = dict(updated.get("parameters") or {})
            parameters.update(changes)
            updated["parameters"] = normalize_agent_parameters(parameters)
            previous_agents[index] = updated
            return

    def finish_current_assignment(
        assignment_status: str,
        *,
        outcome: str = "",
        feedback: str = "",
    ) -> dict[str, Any] | None:
        state["active_task"] = None
        assignment_id = str(state.get("current_assignment_id") or "").strip()
        for assignment in assignments:
            if str(assignment.get("assignment_id") or "") != assignment_id:
                continue
            assignment["status"] = assignment_status
            assignment["completed_at"] = seen_at
            if outcome:
                assignment["outcome"] = outcome
            if feedback:
                assignment["feedback"] = feedback
            return deepcopy(assignment)
        return None

    def activate_agent(
        agent_id: str,
        *,
        phase: str,
        node_id: str,
        review_index: int | None = None,
    ) -> dict[str, Any]:
        agent = agents_by_id[agent_id]
        assignment_id = str(uuid4())
        assignment = {
            "assignment_id": assignment_id,
            "kind": "transition_review" if phase == "review" else "graph_node",
            "phase": phase,
            "node_id": node_id,
            "agent_id": agent_id,
            "agent_name": agent.get("name"),
            "agent_phone": agent.get("phone"),
            "git_branch": agent.get("git_branch")
            or agent.get("parameters", {}).get("git_branch"),
            "task_ids": [
                str(task.get("task_id") or "").strip()
                for task in agent.get("tasks", [])
                if isinstance(task, dict) and str(task.get("task_id") or "").strip()
            ],
            "review_index": review_index,
            "status": "active",
            "started_at": seen_at,
            "completed_at": None,
        }
        assignments.append(assignment)
        state["phase"] = phase
        state["current_node_id"] = node_id
        state["current_agent_id"] = agent_id
        state["current_assignment_id"] = assignment_id
        state["current_started_at"] = seen_at
        state["active_task"] = None
        state["status"] = "active"
        if phase == "node":
            visit_counts[node_id] = visit_counts.get(node_id, 0) + 1
        update_agent_parameters(
            agent_id,
            created_at=agent.get("parameters", {}).get("created_at") or seen_at,
            first_seen_at=agent.get("parameters", {}).get("first_seen_at") or seen_at,
            last_seen_at=seen_at,
            alive_until=(
                (parse_utc_datetime(seen_at) or datetime.now(timezone.utc))
                + timedelta(seconds=AGENT_HEARTBEAT_TTL_SECONDS)
            ).isoformat(),
            presence_status="alive",
            assignment_status="active",
            current_assignment_id=assignment_id,
        )
        return assignment

    current_agent_id = str(state.get("current_agent_id") or "").strip()
    current_agent = agents_by_id.get(current_agent_id)
    current_node_id = str(state.get("current_node_id") or "").strip()
    newly_assigned = False
    completed_agent: dict[str, Any] | None = None
    completed_assignment: dict[str, Any] | None = None
    transition_applied: dict[str, Any] | None = None

    if current_agent is None:
        if str(state.get("status") or "") in {"completed", "blocked"}:
            selected_agent = None
        else:
            start_node_id = str(workflow.get("start_node_id") or "").strip()
            start_node = nodes_by_id.get(start_node_id)
            if start_node is None:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="The configured graph start node no longer exists",
                )
            current_node_id = start_node_id
            current_agent_id = str(start_node.get("agent_id") or "").strip()
            current_agent = agents_by_id[current_agent_id]
            activate_agent(current_agent_id, phase="node", node_id=current_node_id)
            newly_assigned = True
            selected_agent = current_agent
    else:
        selected_agent = current_agent

    clean_requested_phone = requested_phone.strip()
    if selected_agent is not None:
        expected_phone = str(selected_agent.get("phone") or "").strip()
        if clean_requested_phone != expected_phone:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "error": "sequential_assignment_in_progress",
                    "current_agent_id": current_agent_id,
                    "current_agent_name": selected_agent.get("name"),
                    "expected_phone": expected_phone,
                    "phase": state.get("phase"),
                    "message": "Another sequential role is still active",
                },
            )
        if expected_assignment_id and expected_assignment_id != str(
            state.get("current_assignment_id") or ""
        ):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "error": "stale_sequential_assignment",
                    "expected_assignment_id": state.get("current_assignment_id"),
                },
            )

    if complete_current and selected_agent is not None:
        phase = str(state.get("phase") or "node")
        completed_agent = deepcopy(selected_agent)
        if phase == "node":
            node = nodes_by_id.get(current_node_id)
            if node is None:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="The active graph node no longer exists",
                )
            transitions = {
                str(key).upper(): str(value)
                for key, value in (node.get("transitions") or {}).items()
            }
            outcome = submitted_outcome.strip().upper()
            if not outcome and len(transitions) == 1:
                outcome = next(iter(transitions))
            if outcome not in transitions:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail={
                        "error": "invalid_graph_outcome",
                        "node_id": current_node_id,
                        "outcome": outcome,
                        "allowed": sorted(transitions),
                    },
                )
            target_node_id = transitions[outcome]
            completed_assignment = finish_current_assignment(
                "awaiting_review",
                outcome=outcome,
                feedback=submitted_feedback,
            )
            pending_transition = {
                "transition_id": str(uuid4()),
                "source_node_id": current_node_id,
                "source_agent_id": current_agent_id,
                "source_agent_name": selected_agent.get("name"),
                "source_agent_phone": selected_agent.get("phone"),
                "source_agent_profile": selected_agent.get("profile"),
                "source_git_branch": selected_agent.get("git_branch")
                or selected_agent.get("parameters", {}).get("git_branch"),
                "source_tasks": deepcopy(selected_agent.get("tasks") or []),
                "source_assignment_id": state.get("current_assignment_id"),
                "outcome": outcome,
                "target_node_id": target_node_id,
                "result": submitted_result or submitted_feedback,
                "feedback": submitted_feedback,
                "proposed_at": seen_at,
                "reviews": [],
                "status": "reviewing",
            }
            state["pending_transition"] = pending_transition
            update_agent_parameters(
                current_agent_id,
                last_seen_at=seen_at,
                presence_status="awaiting_review",
                assignment_status="awaiting_review",
                assignment_completed_at=seen_at,
            )
            reviewer_id = reviewer_agent_ids[0]
            activate_agent(
                reviewer_id,
                phase="review",
                node_id=current_node_id,
                review_index=1,
            )
            current_agent_id = reviewer_id
            selected_agent = agents_by_id[reviewer_id]
            newly_assigned = True
        elif phase == "review":
            decision = submitted_outcome.strip().upper()
            if decision not in SEQUENTIAL_REVIEW_DECISIONS:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail={
                        "error": "invalid_transition_review_decision",
                        "decision": decision,
                        "allowed": sorted(SEQUENTIAL_REVIEW_DECISIONS),
                    },
                )
            if decision == "REJECT" and not submitted_feedback.strip():
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="A REJECT decision requires non-empty feedback",
                )
            pending_transition = state.get("pending_transition")
            if not isinstance(pending_transition, dict):
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="There is no transition waiting for review",
                )
            reviews = [
                deepcopy(review)
                for review in pending_transition.get("reviews", [])
                if isinstance(review, dict)
            ]
            review = {
                "reviewer_index": len(reviews) + 1,
                "reviewer_agent_id": current_agent_id,
                "reviewer_name": selected_agent.get("name"),
                "decision": decision,
                "feedback": submitted_feedback,
                "reviewed_at": seen_at,
                "assignment_id": state.get("current_assignment_id"),
            }
            reviews.append(review)
            pending_transition["reviews"] = reviews
            if decision == "REJECT":
                pending_transition["feedback"] = submitted_feedback
            completed_assignment = finish_current_assignment(
                "approved" if decision == "APPROVE" else "rejected",
                outcome=decision,
                feedback=submitted_feedback,
            )
            update_agent_parameters(
                current_agent_id,
                last_seen_at=seen_at,
                alive_until=seen_at,
                presence_status="completed",
                assignment_status=(
                    "review_approved" if decision == "APPROVE" else "review_rejected"
                ),
                assignment_completed_at=seen_at,
            )
            if decision == "APPROVE" and len(reviews) < 2:
                next_reviewer_id = reviewer_agent_ids[len(reviews)]
                activate_agent(
                    next_reviewer_id,
                    phase="review",
                    node_id=str(pending_transition.get("source_node_id") or ""),
                    review_index=len(reviews) + 1,
                )
                current_agent_id = next_reviewer_id
                selected_agent = agents_by_id[next_reviewer_id]
                newly_assigned = True
            else:
                source_node_id = str(pending_transition.get("source_node_id") or "")
                proposed_target_id = str(pending_transition.get("target_node_id") or "")
                applied_target_id = source_node_id if decision == "REJECT" else proposed_target_id
                rework = (
                    decision == "REJECT"
                    or str(pending_transition.get("outcome") or "").upper()
                    in {"FAIL", "FAILED", "REWORK", "REJECT"}
                    or (
                        applied_target_id in nodes_by_id
                        and visit_counts.get(applied_target_id, 0) > 0
                    )
                )
                rework_cycle_count = max(0, int(state.get("rework_cycle_count") or 0))
                if rework:
                    rework_cycle_count += 1
                state["rework_cycle_count"] = rework_cycle_count
                max_rework_cycles = max(
                    0, int(workflow.get("max_rework_cycles") or 0)
                )
                pending_transition["status"] = (
                    "rejected" if decision == "REJECT" else "approved"
                )
                pending_transition["resolved_at"] = seen_at
                pending_transition["applied_target_node_id"] = applied_target_id
                transition_applied = deepcopy(pending_transition)
                for assignment in assignments:
                    if str(assignment.get("assignment_id") or "") == str(
                        pending_transition.get("source_assignment_id") or ""
                    ):
                        assignment["status"] = (
                            "review_rejected" if decision == "REJECT" else "transitioned"
                        )
                        assignment["transition_resolved_at"] = seen_at
                        assignment["reviews"] = deepcopy(reviews)
                        break
                state["last_transition"] = deepcopy(pending_transition)
                state["pending_transition"] = None
                if rework_cycle_count > max_rework_cycles:
                    state["status"] = "blocked"
                    state["blocked_at"] = seen_at
                    state["blocked_reason"] = "max_rework_cycles_exceeded"
                    state["current_agent_id"] = None
                    state["current_assignment_id"] = None
                    state["current_started_at"] = None
                    selected_agent = None
                    current_agent_id = ""
                elif applied_target_id in terminal_nodes:
                    terminal = terminal_nodes[applied_target_id]
                    state["status"] = "completed"
                    state["completed_at"] = seen_at
                    state["terminal_node"] = deepcopy(terminal)
                    state["current_node_id"] = applied_target_id
                    state["current_agent_id"] = None
                    state["current_assignment_id"] = None
                    state["current_started_at"] = None
                    selected_agent = None
                    current_agent_id = ""
                else:
                    target_node = nodes_by_id.get(applied_target_id)
                    if target_node is None:
                        raise HTTPException(
                            status_code=status.HTTP_409_CONFLICT,
                            detail="The approved transition target no longer exists",
                        )
                    target_agent_id = str(target_node.get("agent_id") or "")
                    activate_agent(
                        target_agent_id,
                        phase="node",
                        node_id=applied_target_id,
                    )
                    current_agent_id = target_agent_id
                    selected_agent = agents_by_id[target_agent_id]
                    newly_assigned = True
        else:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Unsupported sequential graph phase: {phase}",
            )

    if selected_agent is not None and not complete_current:
        parameters = selected_agent.get("parameters", {})
        try:
            heartbeat_count = max(0, int(parameters.get("heartbeat_count") or 0)) + 1
        except (TypeError, ValueError):
            heartbeat_count = 1
        update_agent_parameters(
            str(selected_agent.get("id") or ""),
            last_seen_at=seen_at,
            heartbeat_count=str(heartbeat_count),
            presence_status="alive",
            assignment_status="active",
        )

    completed_agent_ids = [
        str(agent_id).strip()
        for agent_id in state.get("completed_agent_ids", [])
        if str(agent_id).strip()
    ]
    if completed_agent is not None:
        completed_id = str(completed_agent.get("id") or "").strip()
        if completed_id and completed_id not in completed_agent_ids:
            completed_agent_ids.append(completed_id)
    state["completed_agent_ids"] = completed_agent_ids
    state["visit_counts"] = visit_counts
    state["updated_at"] = seen_at
    state["revision"] = int(state.get("revision") or 0) + 1
    project_entry[PROJECT_AGENT_ASSIGNMENT_KEY] = state
    project_entry["updated_at"] = seen_at
    raw_projects = config.get(PROJECTS_KEY)
    projects = dict(raw_projects) if isinstance(raw_projects, dict) else {}
    projects[raw_key] = project_entry
    config[PROJECTS_KEY] = projects

    updated_agents = normalize_agents(previous_agents)
    write_agents_file_unlocked(updated_agents)
    try:
        write_git_config_file(config)
    except Exception:
        write_agents_file_unlocked(original_agents)
        raise
    updated_project_agents = full_agents_for_project(
        updated_agents,
        context_key,
        phone_git_contexts_from_config(config),
    )
    updated_by_id = {
        str(agent.get("id") or "").strip(): agent for agent in updated_project_agents
    }
    selected_agent = updated_by_id.get(current_agent_id)
    project_phone = normalize_project_phone(project_entry.get("project_phone"))
    return {
        "sequential": True,
        "conditional_graph": True,
        "project_id": project_phone,
        "project_phone": project_phone,
        "project": public_project_context(context),
        "context_key": context_key,
        "agent": deepcopy(selected_agent) if selected_agent else None,
        "agents": deepcopy(updated_project_agents),
        "newly_assigned": newly_assigned,
        "completed_agent": completed_agent,
        "completed_assignment": completed_assignment,
        "transition_applied": transition_applied,
        "all_completed": state.get("status") in {"completed", "blocked"},
        "blocked": state.get("status") == "blocked",
        "assignment": deepcopy(state),
        "queue_context": {
            "queue_phone": project_phone,
            "git_context_phone": project_phone,
            "project_phone": project_phone,
            "project_name": project_entry.get("project_name"),
            "git_context_key": context_key,
            "git_address": project_entry.get("git_address"),
        },
    }


def sequential_agent_assignment_transaction(
    project_id: str,
    requested_phone: str,
    complete_current: bool,
    seen_at: str,
    submitted_outcome: str = "",
    submitted_feedback: str = "",
    submitted_result: str = "",
    expected_assignment_id: str = "",
) -> dict[str, Any]:
    with git_config_file_lock():
        with agents_file_lock():
            config = read_git_config_file()
            raw_key, context_key, project_entry, context = project_for_group_api(
                config,
                project_id,
            )
            raw_state = project_entry.get(PROJECT_AGENT_ASSIGNMENT_KEY)
            state = deepcopy(raw_state) if isinstance(raw_state, dict) else {}
            if str(state.get("mode") or "parallel") != "sequential":
                return {"sequential": False}

            previous_agents = read_agents_file()
            original_agents = deepcopy(previous_agents)
            phone_contexts = phone_git_contexts_from_config(config)
            project_agents = full_agents_for_project(
                previous_agents,
                context_key,
                phone_contexts,
            )
            workflow = state.get("workflow")
            if isinstance(workflow, dict) and workflow.get("enabled"):
                return conditional_graph_assignment_transaction_locked(
                    config,
                    raw_key,
                    context_key,
                    project_entry,
                    context,
                    state,
                    previous_agents,
                    original_agents,
                    project_agents,
                    requested_phone,
                    complete_current,
                    seen_at,
                    submitted_outcome,
                    submitted_feedback,
                    submitted_result,
                    expected_assignment_id,
                )
            clean_requested_phone = requested_phone.strip()
            if not any(
                str(agent.get("phone") or "").strip() == clean_requested_phone
                for agent in project_agents
            ):
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Agent phone was not found in this project",
                )

            agents_by_id = {
                str(agent.get("id") or "").strip(): agent
                for agent in project_agents
            }
            role_agent_ids = [
                str(agent_id).strip()
                for agent_id in state.get("role_agent_ids", [])
                if str(agent_id).strip() in agents_by_id
            ]
            if not role_agent_ids:
                role_agent_ids = [
                    str(agent.get("id") or "").strip()
                    for agent in sorted(
                        (
                            agent
                            for agent in project_agents
                            if str(
                                agent.get("parameters", {}).get("assignment_order")
                                or ""
                            ).strip()
                        ),
                        key=lambda agent: int(
                            agent.get("parameters", {}).get("assignment_order") or 0
                        ),
                    )
                ]
                state["role_agent_ids"] = role_agent_ids

            completed_agent_ids = [
                str(agent_id).strip()
                for agent_id in state.get("completed_agent_ids", [])
                if str(agent_id).strip()
            ]
            completed_set = set(completed_agent_ids)
            current_agent_id = str(state.get("current_agent_id") or "").strip()
            current_agent = agents_by_id.get(current_agent_id)
            completed_agent: dict[str, Any] | None = None
            completed_assignment: dict[str, Any] | None = None

            if complete_current:
                if current_agent is None:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "error": "no_active_sequential_assignment",
                            "message": "There is no active role to complete",
                        },
                    )
                current_phone = str(current_agent.get("phone") or "").strip()
                if clean_requested_phone != current_phone:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "error": "sequential_assignment_phone_mismatch",
                            "current_agent_id": current_agent_id,
                            "expected_phone": current_phone,
                        },
                    )
                completed_agent = deepcopy(current_agent)
                if current_agent_id not in completed_set:
                    completed_agent_ids.append(current_agent_id)
                    completed_set.add(current_agent_id)
                for assignment in state.get("assignments", []):
                    if not isinstance(assignment, dict):
                        continue
                    if (
                        str(assignment.get("assignment_id") or "")
                        == str(state.get("current_assignment_id") or "")
                    ):
                        assignment["status"] = "completed"
                        assignment["completed_at"] = seen_at
                        completed_assignment = deepcopy(assignment)
                        break
                for index, stored_agent in enumerate(previous_agents):
                    if str(stored_agent.get("id") or "").strip() != current_agent_id:
                        continue
                    updated_completed = deepcopy(stored_agent)
                    completed_parameters = dict(updated_completed.get("parameters") or {})
                    completed_parameters.update(
                        {
                            "presence_status": "completed",
                            "last_seen_at": seen_at,
                            "alive_until": seen_at,
                            "assignment_status": "completed",
                            "assignment_completed_at": seen_at,
                        }
                    )
                    updated_completed["parameters"] = normalize_agent_parameters(
                        completed_parameters
                    )
                    previous_agents[index] = updated_completed
                    break
                state["current_agent_id"] = None
                state["current_assignment_id"] = None
                state["current_started_at"] = None
                current_agent = None
                current_agent_id = ""

            if current_agent is not None and not complete_current:
                expected_phone = str(current_agent.get("phone") or "").strip()
                if clean_requested_phone != expected_phone:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "error": "sequential_assignment_in_progress",
                            "current_agent_id": current_agent_id,
                            "current_agent_name": current_agent.get("name"),
                            "expected_phone": expected_phone,
                            "message": "Another sequential role is still active",
                        },
                    )

            newly_assigned = False
            if current_agent is None:
                next_agent_id = next(
                    (
                        agent_id
                        for agent_id in role_agent_ids
                        if agent_id not in completed_set
                    ),
                    "",
                )
                current_agent = agents_by_id.get(next_agent_id)
                current_agent_id = next_agent_id
                if current_agent is not None:
                    newly_assigned = True
                    assignment_id = str(uuid4())
                    assignment = {
                        "assignment_id": assignment_id,
                        "agent_id": current_agent_id,
                        "agent_name": current_agent.get("name"),
                        "agent_phone": current_agent.get("phone"),
                        "git_branch": current_agent.get("git_branch")
                        or current_agent.get("parameters", {}).get("git_branch"),
                        "task_ids": [
                            str(task.get("task_id") or "").strip()
                            for task in current_agent.get("tasks", [])
                            if str(task.get("task_id") or "").strip()
                        ],
                        "status": "active",
                        "started_at": seen_at,
                        "completed_at": None,
                    }
                    assignments = [
                        deepcopy(item)
                        for item in state.get("assignments", [])
                        if isinstance(item, dict)
                    ]
                    assignments.append(assignment)
                    state["assignments"] = assignments
                    state["current_agent_id"] = current_agent_id
                    state["current_assignment_id"] = assignment_id
                    state["current_started_at"] = seen_at
                    state["status"] = "active"
                else:
                    state["status"] = "completed"
                    state["completed_at"] = seen_at

            if current_agent is not None:
                for index, stored_agent in enumerate(previous_agents):
                    if str(stored_agent.get("id") or "").strip() != current_agent_id:
                        continue
                    updated_current = deepcopy(stored_agent)
                    parameters = dict(updated_current.get("parameters") or {})
                    try:
                        heartbeat_count = max(
                            0,
                            int(parameters.get("heartbeat_count") or 0),
                        ) + 1
                    except (TypeError, ValueError):
                        heartbeat_count = 1
                    seen_time = parse_utc_datetime(seen_at) or datetime.now(timezone.utc)
                    parameters.update(
                        {
                            "created_at": parameters.get("created_at") or seen_at,
                            "first_seen_at": parameters.get("first_seen_at") or seen_at,
                            "last_seen_at": seen_at,
                            "alive_until": (
                                seen_time
                                + timedelta(seconds=AGENT_HEARTBEAT_TTL_SECONDS)
                            ).isoformat(),
                            "heartbeat_count": str(heartbeat_count),
                            "presence_status": "alive",
                            "assignment_status": "active",
                            "current_assignment_id": state.get(
                                "current_assignment_id"
                            ),
                        }
                    )
                    updated_current["parameters"] = normalize_agent_parameters(
                        parameters
                    )
                    previous_agents[index] = updated_current
                    break

            state["completed_agent_ids"] = completed_agent_ids
            state["updated_at"] = seen_at
            state["revision"] = int(state.get("revision") or 0) + 1
            project_entry[PROJECT_AGENT_ASSIGNMENT_KEY] = state
            project_entry["updated_at"] = seen_at
            raw_projects = config.get(PROJECTS_KEY)
            projects = dict(raw_projects) if isinstance(raw_projects, dict) else {}
            projects[raw_key] = project_entry
            config[PROJECTS_KEY] = projects

            updated_agents = normalize_agents(previous_agents)
            write_agents_file_unlocked(updated_agents)
            try:
                write_git_config_file(config)
            except Exception:
                write_agents_file_unlocked(original_agents)
                raise
            updated_project_agents = full_agents_for_project(
                updated_agents,
                context_key,
                phone_git_contexts_from_config(config),
            )
            updated_by_id = {
                str(agent.get("id") or "").strip(): agent
                for agent in updated_project_agents
            }
            selected_agent = updated_by_id.get(current_agent_id)
            project_phone = normalize_project_phone(project_entry.get("project_phone"))
            return {
                "sequential": True,
                "project_id": project_phone,
                "project_phone": project_phone,
                "project": public_project_context(context),
                "context_key": context_key,
                "agent": deepcopy(selected_agent) if selected_agent else None,
                "agents": deepcopy(updated_project_agents),
                "newly_assigned": newly_assigned,
                "completed_agent": completed_agent,
                "completed_assignment": completed_assignment,
                "all_completed": selected_agent is None,
                "assignment": deepcopy(state),
                "queue_context": {
                    "queue_phone": project_phone,
                    "git_context_phone": project_phone,
                    "project_phone": project_phone,
                    "project_name": project_entry.get("project_name"),
                    "git_context_key": context_key,
                    "git_address": project_entry.get("git_address"),
                },
            }


def earliest_iso_timestamp(*values: Any) -> str:
    parsed = [
        timestamp
        for value in values
        if (timestamp := parse_utc_datetime(value)) is not None
    ]
    return min(parsed).isoformat() if parsed else ""


def mark_project_agent_alive_transaction(
    project_id: str,
    agent_phone: str,
    seen_at: str,
    suggested_created_at: str,
) -> dict[str, Any]:
    with git_config_file_lock():
        with agents_file_lock():
            config = read_git_config_file()
            _, context_key, project_entry, context = project_for_group_api(
                config,
                project_id,
            )
            previous_agents = read_agents_file()
            project_agents = full_agents_for_project(
                previous_agents,
                context_key,
                phone_git_contexts_from_config(config),
            )
            clean_phone = agent_phone.strip()
            project_agent = next(
                (
                    agent
                    for agent in project_agents
                    if str(agent.get("phone") or "").strip() == clean_phone
                ),
                None,
            )
            if project_agent is None:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Agent phone was not found in this project",
                )
            agent_id = str(project_agent.get("id") or "").strip()
            target_index = next(
                (
                    index
                    for index, agent in enumerate(previous_agents)
                    if str(agent.get("id") or "").strip() == agent_id
                ),
                None,
            )
            if target_index is None:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Project agent disappeared while heartbeat was being recorded",
                )

            target = deepcopy(previous_agents[target_index])
            parameters = (
                dict(target.get("parameters"))
                if isinstance(target.get("parameters"), dict)
                else {}
            )
            try:
                heartbeat_count = max(0, int(parameters.get("heartbeat_count") or 0)) + 1
            except (TypeError, ValueError):
                heartbeat_count = 1
            seen_time = parse_utc_datetime(seen_at) or datetime.now(timezone.utc)
            alive_until = seen_time + timedelta(seconds=AGENT_HEARTBEAT_TTL_SECONDS)
            created_at = earliest_iso_timestamp(
                parameters.get("created_at"),
                suggested_created_at,
                seen_at,
            )
            project_phone = normalize_project_phone(project_entry.get("project_phone"))
            parameters.update(
                {
                    "created_at": created_at or seen_at,
                    "first_seen_at": parameters.get("first_seen_at") or seen_at,
                    "last_seen_at": seen_at,
                    "alive_until": alive_until.isoformat(),
                    "heartbeat_count": str(heartbeat_count),
                    "presence_status": "alive",
                    "heartbeat_interval_seconds": str(
                        AGENT_HEARTBEAT_INTERVAL_SECONDS
                    ),
                    "whoami_endpoint": (
                        f"/api/v1/projects/{project_phone}/agents/{clean_phone}/whoami"
                    ),
                }
            )
            target["parameters"] = normalize_agent_parameters(parameters)
            previous_agents[target_index] = target
            updated_agents = normalize_agents(previous_agents)
            write_agents_file_unlocked(updated_agents)
            updated_project_agents = full_agents_for_project(
                updated_agents,
                context_key,
                phone_git_contexts_from_config(config),
            )
            updated_agent = next(
                agent
                for agent in updated_project_agents
                if str(agent.get("id") or "").strip() == agent_id
            )
            return {
                "project_id": project_phone,
                "project_phone": project_phone,
                "project": public_project_context(context),
                "context_key": context_key,
                "agent": deepcopy(updated_agent),
                "agents": deepcopy(updated_project_agents),
                "queue_context": {
                    "queue_phone": project_phone,
                    "git_context_phone": project_phone,
                    "project_phone": project_phone,
                    "project_name": project_entry.get("project_name"),
                    "git_context_key": context_key,
                    "git_address": project_entry.get("git_address"),
                },
            }


def attach_transition_review_context_transaction(
    project_id: str,
    transition_id: str,
    review_context: dict[str, Any],
    seen_at: str,
) -> dict[str, Any]:
    with git_config_file_lock():
        config = read_git_config_file()
        raw_key, _, project_entry, _ = project_for_group_api(config, project_id)
        raw_state = project_entry.get(PROJECT_AGENT_ASSIGNMENT_KEY)
        state = deepcopy(raw_state) if isinstance(raw_state, dict) else {}
        pending_transition = state.get("pending_transition")
        if not isinstance(pending_transition, dict) or str(
            pending_transition.get("transition_id") or ""
        ) != transition_id:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="The transition awaiting review changed before context was attached",
            )
        if not isinstance(pending_transition.get("review_context"), dict):
            pending_transition["review_context"] = deepcopy(review_context)
            state["pending_transition"] = pending_transition
            state["updated_at"] = seen_at
            state["revision"] = int(state.get("revision") or 0) + 1
            project_entry[PROJECT_AGENT_ASSIGNMENT_KEY] = state
            project_entry["updated_at"] = seen_at
            raw_projects = config.get(PROJECTS_KEY)
            projects = dict(raw_projects) if isinstance(raw_projects, dict) else {}
            projects[raw_key] = project_entry
            config[PROJECTS_KEY] = projects
            write_git_config_file(config)
        return deepcopy(state)


async def identify_sequential_project_agent(
    project_id: str,
    requested_phone: str,
    request_message: str,
    complete_current: bool,
    request: Request,
    submitted_outcome: str = "",
    submitted_feedback: str = "",
    submitted_result: str = "",
    expected_assignment_id: str = "",
    identity_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    seen_at = utc_now()
    submitted_from_commit = ""
    submitted_git_commit = ""
    if isinstance(identity_payload, dict):
        submitted_outcome = str(
            identity_payload.get("outcome")
            or identity_payload.get("status")
            or submitted_outcome
            or ""
        )
        submitted_feedback = str(
            identity_payload.get("feedback") or submitted_feedback or ""
        )
        submitted_result = str(
            identity_payload.get("result") or submitted_result or ""
        )
        expected_assignment_id = str(
            identity_payload.get("assignment_id")
            or expected_assignment_id
            or ""
        )
        submitted_from_commit = str(
            identity_payload.get("from_commit")
            or identity_payload.get("base_commit")
            or ""
        ).strip()
        submitted_git_commit = str(
            identity_payload.get("git_commit")
            or identity_payload.get("commit")
            or identity_payload.get("git_commit_short")
            or ""
        ).strip()
    submitted_outcome = submitted_outcome.strip().upper()
    submitted_feedback = submitted_feedback.strip()
    submitted_result = submitted_result.strip()
    expected_assignment_id = expected_assignment_id.strip()
    if submitted_from_commit:
        submitted_from_commit = normalize_commit_ref(submitted_from_commit)
    if submitted_git_commit:
        submitted_git_commit = normalize_commit_ref(submitted_git_commit)
    if len(submitted_feedback) > 20000 or len(submitted_result) > 100000:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail="feedback or result is too large",
        )
    if submitted_outcome:
        complete_current = True
    async with group_task_submission_lock:
        result = await run_group_write_transaction(
            sequential_agent_assignment_transaction,
            project_id,
            requested_phone,
            complete_current,
            seen_at,
            submitted_outcome,
            submitted_feedback,
            submitted_result,
            expected_assignment_id,
        )
        completed_agent = result.get("completed_agent")
        completed_assignment = result.get("completed_assignment") or {}
        graph_node_completed = (
            isinstance(completed_assignment, dict)
            and completed_assignment.get("kind") == "graph_node"
        )
        completion_git_context = deepcopy(result["queue_context"])
        if submitted_git_commit:
            completion_git_context.update(
                {
                    "git_commit": submitted_git_commit,
                    "git_commit_short": submitted_git_commit[:12],
                }
            )
        elif isinstance(completed_agent, dict) and graph_node_completed:
            local_git_reference = await asyncio.to_thread(
                resolve_local_git_reference,
                str(completion_git_context.get("git_address") or ""),
            )
            if local_git_reference.get("git_commit"):
                completion_git_context.update(local_git_reference)
        result["queue_context"] = completion_git_context
        removed_tasks: list[dict[str, Any]] = []
        if isinstance(completed_agent, dict):
            completed_id = str(completed_agent.get("id") or "").strip()
            completed_phone = str(completed_agent.get("phone") or "").strip()
            removed_tasks = await remove_project_actor_queue_items(
                result["queue_context"],
                {completed_id} if completed_id else set(),
                {completed_phone} if completed_phone else set(),
                action="removed_by_sequential_assignment_completion",
            )
            if graph_node_completed and submitted_from_commit:
                await append_history(
                    "agent_assignment_base_commit",
                    "worker-all",
                    f"Начальный commit задания: {submitted_from_commit}",
                    {
                        "submitted_via": "agent_whoami",
                        "action": "sequential_role_base_commit",
                        "sender": completed_agent.get("name"),
                        "receiver": completed_agent.get("name"),
                        "from_phone": completed_phone,
                        "to_phone": completed_phone,
                        "from_agent_id": completed_id,
                        "to_agent_id": completed_id,
                        "project_phone": result["project_phone"],
                        "assignment_id": completed_assignment.get("assignment_id"),
                    },
                    request_port(request),
                    {
                        **result["queue_context"],
                        "git_commit": submitted_from_commit,
                        "git_commit_short": submitted_from_commit[:12],
                    },
                )
            await append_history(
                "agent_assignment_completed",
                "worker-all",
                submitted_result or request_message or "Задание выполнено. Кто я?",
                {
                    "submitted_via": "agent_whoami",
                    "action": "sequential_role_completed",
                    "sender": completed_agent.get("name"),
                    "receiver": "Project Manager",
                    "from_phone": completed_phone,
                    "to_phone": PROJECT_MANAGER_PHONE,
                    "from_agent_id": completed_id,
                    "to_agent_id": PROJECT_MANAGER_AGENT_ID,
                    "project_phone": result["project_phone"],
                    "assignment_id": (
                        result.get("completed_assignment") or {}
                    ).get("assignment_id"),
                    "outcome": submitted_outcome,
                    "result": submitted_result,
                    "status": "COMPLETED",
                },
                request_port(request),
                completion_git_context,
            )
            pending_transition = (result.get("assignment") or {}).get(
                "pending_transition"
            )
            if graph_node_completed and isinstance(pending_transition, dict):
                review_context = await project_history_with_patches_context(
                    result["context_key"],
                    str(result["project"].get("git_address") or ""),
                    assignment_id=str(
                        completed_assignment.get("assignment_id") or ""
                    ).strip()
                    or None,
                )
                review_context.update(
                    {
                        "schema_version": 1,
                        "generated_at": seen_at,
                        "semantics": (
                            "A patch is inserted before the first message at a "
                            "different commit; identical commit pairs are not repeated."
                        ),
                        "submission": {
                            "assignment_id": completed_assignment.get(
                                "assignment_id"
                            ),
                            "from_commit": submitted_from_commit or None,
                            "git_commit": completion_git_context.get("git_commit"),
                        },
                    }
                )
                updated_assignment = await run_group_write_transaction(
                    attach_transition_review_context_transaction,
                    project_id,
                    str(pending_transition.get("transition_id") or ""),
                    review_context,
                    seen_at,
                )
                result["assignment"] = updated_assignment

        agent = result.get("agent")
        if not isinstance(agent, dict):
            assignment = result.get("assignment") or {}
            blocked = bool(result.get("blocked")) or assignment.get("status") == "blocked"
            return {
                "answer": (
                    "Последовательный граф заблокирован: превышено допустимое "
                    "число циклов доработки."
                    if blocked
                    else "Все последовательные роли, проверки и задания выполнены."
                ),
                "identity_request": request_message or "Кто я?",
                "project_id": result["project_id"],
                "project_phone": result["project_phone"],
                "project": result["project"],
                "assignment_mode": "sequential",
                "all_completed": True,
                "blocked": blocked,
                "agent": None,
                "presence": {
                    "status": "blocked" if blocked else "completed",
                    "is_alive": False,
                },
                "assigned_tasks": [],
                "work_summary": {
                    "assigned_task_count": 0,
                    "history_event_count": 0,
                },
                "work_history": [],
                "assignment": assignment,
                "terminal_node": assignment.get("terminal_node"),
                "last_transition": assignment.get("last_transition"),
                "transition_applied": result.get("transition_applied"),
                "completed_assignments": assignment.get("assignments", []),
                "removed_pending_tasks": removed_tasks,
            }

        assignment = result.get("assignment") or {}
        assignment_id = str(assignment.get("current_assignment_id") or "").strip()
        queued_tasks: list[dict[str, Any]] = []
        if result.get("newly_assigned"):
            queued_tasks = [await enqueue_sequential_agent_node(
                agent,
                result["project_phone"],
                result["queue_context"],
                assignment,
                source="sequential_agent_node_switch",
                port=request_port(request),
            )]

        agent_id = str(agent.get("id") or "").strip()
        agent_name = str(agent.get("name") or "").strip()
        agent_phone = str(agent.get("phone") or "").strip()
        heartbeat_event = (
            "agent_assignment_started"
            if result.get("newly_assigned")
            else "agent_identity_heartbeat"
        )
        await append_history(
            heartbeat_event,
            "worker-all",
            request_message or "Кто я?",
            {
                "submitted_via": "agent_whoami",
                "action": (
                    "sequential_role_assigned"
                    if result.get("newly_assigned")
                    else "agent_marked_alive"
                ),
                "sender": agent_name,
                "receiver": agent_name,
                "from_phone": agent_phone,
                "to_phone": agent_phone,
                "from_agent_id": agent_id,
                "to_agent_id": agent_id,
                "project_phone": result["project_phone"],
                "presence_status": "alive",
                "assignment_id": assignment_id,
            },
            request_port(request),
            result["queue_context"],
        )

    created_at = str(agent.get("parameters", {}).get("created_at") or seen_at)
    async with history_lock:
        work_history = await asyncio.to_thread(
            read_agent_work_history_file,
            agent,
            result["context_key"],
            created_at,
        )
    assigned_tasks = deepcopy(agent.get("tasks") or [])
    work_summary = agent_work_history_summary(work_history, len(assigned_tasks))
    git_branch = str(
        agent.get("git_branch")
        or agent.get("parameters", {}).get("git_branch")
        or ""
    ).strip()
    completed_count = len(assignment.get("completed_agent_ids", []))
    total_count = len(assignment.get("role_agent_ids", []))
    role_number = min(completed_count + 1, total_count) if total_count else 0
    phase = str(assignment.get("phase") or "node")
    if phase == "review":
        pending_transition = assignment.get("pending_transition") or {}
        review_number = len(pending_transition.get("reviews", [])) + 1
        answer = (
            f"Сейчас вы ревьювер перехода №{review_number} из 2 — {agent_name} "
            f"(id={agent_id}, phone={agent_phone}). Проверьте переход "
            f"{pending_transition.get('source_node_id')} --"
            f"{pending_transition.get('outcome')}--> "
            f"{pending_transition.get('target_node_id')} и ответьте APPROVE или REJECT."
        )
    else:
        answer = (
            f"Ваша текущая последовательная роль — {agent_name} "
            f"(роль {role_number} из {total_count}, id={agent_id}, phone={agent_phone}). "
            f"Ветка: {git_branch or 'не назначена'}. Задач в этой роли: "
            f"{len(assigned_tasks)}. Каждый переход подтвердят два ревьювера."
        )
    return {
        "answer": answer,
        "identity_request": request_message or "Кто я?",
        "project_id": result["project_id"],
        "project_phone": result["project_phone"],
        "project": result["project"],
        "assignment_mode": "sequential",
        "all_completed": False,
        "blocked": False,
        "phase": phase,
        "newly_assigned": bool(result.get("newly_assigned")),
        "agent": agent,
        "profile": agent.get("profile"),
        "git_branch": git_branch,
        "presence": agent.get("presence") or agent_presence_snapshot(agent),
        "assigned_tasks": assigned_tasks,
        "work_summary": work_summary,
        "work_history_since": created_at,
        "work_history": work_history,
        "assignment": assignment,
        "pending_transition": assignment.get("pending_transition"),
        "last_transition": assignment.get("last_transition"),
        "transition_applied": result.get("transition_applied"),
        "current_assignment_id": assignment_id,
        "next_whoami_endpoint": (
            f"/api/v1/projects/{result['project_phone']}/agents/{agent_phone}/whoami"
        ),
        "sequential_poll_endpoint": (
            f"/worker/all/{result['project_phone']}"
            f"?to_phone={result['project_phone']}"
        ),
        "queued_tasks": queued_tasks,
        "removed_pending_tasks": removed_tasks,
        "completed_assignment": result.get("completed_assignment"),
        "completed_assignments": [
            item
            for item in assignment.get("assignments", [])
            if isinstance(item, dict)
            and item.get("status")
            in {"completed", "transitioned", "review_rejected", "approved", "rejected"}
        ],
    }


def sequential_runtime_project_snapshot_transaction(
    project_id: str,
) -> dict[str, Any]:
    with git_config_file_lock():
        with agents_file_lock():
            config = read_git_config_file()
            _, context_key, project_entry, context = project_for_group_api(
                config,
                project_id,
            )
            assignment = (
                deepcopy(project_entry.get(PROJECT_AGENT_ASSIGNMENT_KEY))
                if isinstance(project_entry.get(PROJECT_AGENT_ASSIGNMENT_KEY), dict)
                else {"mode": "parallel", "strategy": "parallel"}
            )
            project_phone = normalize_project_phone(
                project_entry.get("project_phone")
            )
            project_agents = full_agents_for_project(
                read_agents_file(),
                context_key,
                phone_git_contexts_from_config(config),
            )
            return {
                "project_id": project_phone,
                "project_phone": project_phone,
                "project": public_project_context(context),
                "context_key": context_key,
                "assignment": assignment,
                "agents": deepcopy(project_agents),
                "queue_context": {
                    "queue_phone": project_phone,
                    "git_context_phone": project_phone,
                    "project_phone": project_phone,
                    "project_name": project_entry.get("project_name"),
                    "git_context_key": context_key,
                    "git_address": project_entry.get("git_address"),
                },
            }


async def project_state_json(
    project_id: str,
    *,
    history_limit: int = 200,
) -> dict[str, Any]:
    snapshot = await run_group_write_transaction(
        sequential_runtime_project_snapshot_transaction,
        project_id,
    )
    recent_activity = await read_history(
        limit=history_limit,
        date_from="1970-01-01",
        git_context=snapshot["context_key"],
    )
    history_with_patches = await asyncio.to_thread(
        history_with_patches_context,
        recent_activity,
        str(snapshot["project"].get("git_address") or ""),
    )
    pending_work: dict[str, list[dict[str, Any]]] = {}
    acquired: list[asyncio.Lock] = []
    try:
        for queue_name in sorted(GROUP_QUEUE_NAMES):
            await locks[queue_name].acquire()
            acquired.append(locks[queue_name])
        for queue_name in sorted(GROUP_QUEUE_NAMES):
            pending_work[queue_name] = [
                queue_item_snapshot(item)
                for item in queues[queue_name]
                if str(
                    queue_item_metadata(item).get("conversation_phone") or ""
                ).strip()
                == snapshot["project_phone"]
                and queue_item_matches_git_context(item, snapshot["context_key"])
            ]
    finally:
        for queue_lock in reversed(acquired):
            queue_lock.release()
    assignment = snapshot["assignment"]
    return {
        "schema_version": 1,
        "generated_at": utc_now(),
        "project_id": snapshot["project_id"],
        "project_phone": snapshot["project_phone"],
        "project": snapshot["project"],
        "execution": assignment,
        "workflow": deepcopy(assignment.get("workflow")),
        "agents": snapshot["agents"],
        "pending_work": pending_work,
        "recent_activity": recent_activity,
        "history_with_patches": history_with_patches,
        "activity_with_patches": history_with_patches["timeline"],
        "code_patches": history_with_patches["patches"],
        "code_patch_summary": {
            "activity_count": history_with_patches["record_count"],
            "unique_commit_transition_count": (
                history_with_patches["patch_count"]
                + history_with_patches["patch_error_count"]
            ),
            "available_patch_count": history_with_patches["patch_count"],
            "unavailable_patch_count": history_with_patches["patch_error_count"],
        },
    }


def sequential_runtime_target_agent(
    metadata: dict[str, Any],
    project_agents: list[dict[str, Any]],
    project_phone: str,
) -> dict[str, Any] | None:
    target_agent_id = str(metadata.get("to_agent_id") or "").strip()
    nested_agent = metadata.get("agent")
    if not target_agent_id and isinstance(nested_agent, dict):
        target_agent_id = str(nested_agent.get("id") or "").strip()
    if target_agent_id:
        matched_by_id = next(
            (
                agent
                for agent in project_agents
                if str(agent.get("id") or "").strip() == target_agent_id
            ),
            None,
        )
        if matched_by_id is not None:
            return matched_by_id

    target_phone = str(
        metadata.get("logical_to_phone") or metadata.get("to_phone") or ""
    ).strip()
    if target_phone == project_phone and isinstance(nested_agent, dict):
        target_phone = str(nested_agent.get("phone") or "").strip()
    return next(
        (
            agent
            for agent in project_agents
            if str(agent.get("phone") or "").strip() == target_phone
        ),
        None,
    )


def sequential_runtime_task_snapshot(
    queue_name: str,
    queue_item: Any,
) -> dict[str, Any]:
    return {
        "id": str(queue_item_id(queue_item) or ""),
        "queue": queue_name,
        "queued_at": queue_item_queued_at(queue_item),
        "message": deepcopy(queue_item_message(queue_item)),
        "metadata": deepcopy(queue_item_metadata(queue_item)),
    }


def sequential_runtime_from_active_task(
    snapshot: dict[str, Any],
    task: Any,
    *,
    recovered_from_history: bool = False,
) -> dict[str, Any] | None:
    assignment = snapshot.get("assignment")
    if not isinstance(assignment, dict) or assignment.get("status") != "active":
        return None
    if not isinstance(task, dict) or not isinstance(task.get("metadata"), dict):
        return None
    metadata = task["metadata"]
    if str(metadata.get("identity_kind") or "") == "reviewer_bootstrap":
        return None

    current_agent_id = str(assignment.get("current_agent_id") or "").strip()
    current_assignment_id = str(
        assignment.get("current_assignment_id") or ""
    ).strip()
    task_assignment_id = str(metadata.get("assignment_id") or "").strip()
    if current_assignment_id and task_assignment_id != current_assignment_id:
        return None
    agent = next(
        (
            candidate
            for candidate in snapshot.get("agents", [])
            if str(candidate.get("id") or "").strip() == current_agent_id
        ),
        None,
    )
    if not isinstance(agent, dict):
        return None
    return {
        **snapshot,
        "agent": deepcopy(agent),
        "task": deepcopy(task),
        "identity_reused": True,
        "active_task_recovered_from_history": recovered_from_history,
    }


async def recover_sequential_runtime_identity(
    snapshot: dict[str, Any],
) -> dict[str, Any] | None:
    assignment = snapshot.get("assignment")
    if not isinstance(assignment, dict):
        return None
    cached = sequential_runtime_from_active_task(
        snapshot,
        assignment.get("active_task"),
    )
    if cached is not None:
        return cached

    current_assignment_id = str(
        assignment.get("current_assignment_id") or ""
    ).strip()
    current_agent_id = str(assignment.get("current_agent_id") or "").strip()
    if (
        assignment.get("status") != "active"
        or not current_assignment_id
        or not current_agent_id
    ):
        return None
    records = await read_history(
        limit=10000,
        date_from="1970-01-01",
        git_context=snapshot.get("context_key"),
    )
    for record in reversed(records):
        metadata = (
            record.get("metadata")
            if isinstance(record.get("metadata"), dict)
            else {}
        )
        if metadata.get("action") != "sequential_graph_node_delivered":
            continue
        if str(metadata.get("assignment_id") or "").strip() != current_assignment_id:
            continue
        target_agent_id = str(
            metadata.get("to_agent_id")
            or (
                metadata.get("agent", {}).get("id")
                if isinstance(metadata.get("agent"), dict)
                else ""
            )
            or ""
        ).strip()
        if target_agent_id and target_agent_id != current_agent_id:
            continue
        task = {
            "id": str(metadata.get("queue_item_id") or record.get("id") or ""),
            "queue": str(record.get("queue") or "worker-all"),
            "queued_at": metadata.get("queued_at") or record.get("timestamp"),
            "message": deepcopy(record.get("message")),
            "metadata": deepcopy(metadata),
        }
        recovered = sequential_runtime_from_active_task(
            snapshot,
            task,
            recovered_from_history=True,
        )
        if recovered is not None:
            return recovered
    return None


def record_conditional_graph_identity_delivery_transaction(
    project_id: str,
    agent: dict[str, Any],
    queue_name: str,
    queue_item: Any,
    seen_at: str,
) -> dict[str, Any]:
    with git_config_file_lock():
        config = read_git_config_file()
        raw_key, context_key, project_entry, context = project_for_group_api(
            config,
            project_id,
        )
        raw_state = project_entry.get(PROJECT_AGENT_ASSIGNMENT_KEY)
        state = deepcopy(raw_state) if isinstance(raw_state, dict) else {}
        workflow = state.get("workflow")
        if not (
            str(state.get("mode") or "parallel") == "sequential"
            and str(state.get("strategy") or "") == "conditional_graph"
            and isinstance(workflow, dict)
            and workflow.get("enabled")
        ):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={"error": "conditional_graph_not_active"},
            )

        metadata = queue_item_metadata(queue_item)
        agent_id = str(agent.get("id") or "").strip()
        identity_kind = str(metadata.get("identity_kind") or "graph_node").strip()
        delivery = {
            "queue_item_id": queue_item_id(queue_item),
            "queue": queue_name,
            "identity_kind": identity_kind,
            "agent_id": agent_id,
            "agent_name": agent.get("name"),
            "agent_phone": agent.get("phone"),
            "assignment_id": metadata.get("assignment_id"),
            "delivered_at": seen_at,
        }
        deliveries = [
            deepcopy(item)
            for item in state.get("identity_deliveries", [])
            if isinstance(item, dict)
        ]
        deliveries.append(delivery)
        state["identity_deliveries"] = deliveries[-500:]

        if identity_kind == "reviewer_bootstrap":
            initialized = [
                deepcopy(item)
                for item in state.get("reviewer_initializations", [])
                if isinstance(item, dict)
                and str(item.get("agent_id") or "").strip() != agent_id
            ]
            initialized.append(
                {
                    "agent_id": agent_id,
                    "agent_name": agent.get("name"),
                    "agent_phone": agent.get("phone"),
                    "reviewer_index": metadata.get("reviewer_index"),
                    "initialized_at": seen_at,
                    "project_state_endpoint": metadata.get(
                        "project_state_endpoint"
                    ),
                }
            )
            state["reviewer_initializations"] = sorted(
                initialized,
                key=lambda item: int(item.get("reviewer_index") or 0),
            )
        else:
            state["active_task"] = sequential_runtime_task_snapshot(
                queue_name,
                queue_item,
            )
            assignment_id = str(metadata.get("assignment_id") or "").strip()
            for assignment in state.get("assignments", []):
                if not isinstance(assignment, dict):
                    continue
                if str(assignment.get("assignment_id") or "") == assignment_id:
                    assignment["delivered_at"] = seen_at
                    assignment["queue_item_id"] = queue_item_id(queue_item)
                    assignment["queue"] = queue_name
                    break

        state["updated_at"] = seen_at
        state["revision"] = int(state.get("revision") or 0) + 1
        project_entry[PROJECT_AGENT_ASSIGNMENT_KEY] = state
        project_entry["updated_at"] = seen_at
        raw_projects = config.get(PROJECTS_KEY)
        projects = dict(raw_projects) if isinstance(raw_projects, dict) else {}
        projects[raw_key] = project_entry
        config[PROJECTS_KEY] = projects
        write_git_config_file(config)
        return {
            "assignment": deepcopy(state),
            "project": public_project_context(
                configured_git_context_for_key(config, context_key) or context
            ),
        }


def record_sequential_runtime_identity_transaction(
    project_id: str,
    agent: dict[str, Any],
    queue_name: str,
    queue_item: Any,
    seen_at: str,
) -> dict[str, Any]:
    with git_config_file_lock():
        config = read_git_config_file()
        raw_key, context_key, project_entry, context = project_for_group_api(
            config,
            project_id,
        )
        raw_state = project_entry.get(PROJECT_AGENT_ASSIGNMENT_KEY)
        state = deepcopy(raw_state) if isinstance(raw_state, dict) else {}
        if str(state.get("mode") or "parallel") != "sequential":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={"error": "project_not_sequential"},
            )

        metadata = queue_item_metadata(queue_item)
        assignments = [
            deepcopy(item)
            for item in state.get("assignments", [])
            if isinstance(item, dict)
        ]
        previous_assignment_id = str(
            state.get("current_assignment_id") or ""
        ).strip()
        for assignment in assignments:
            if (
                str(assignment.get("assignment_id") or "")
                == previous_assignment_id
                and assignment.get("status") == "active"
            ):
                assignment["status"] = "advanced"
                assignment["completed_at"] = seen_at
                break

        assignment_id = str(metadata.get("assignment_id") or uuid4()).strip()
        agent_id = str(agent.get("id") or "").strip()
        agent_phone = str(agent.get("phone") or "").strip()
        active_task = sequential_runtime_task_snapshot(queue_name, queue_item)
        active_task["metadata"]["assignment_id"] = assignment_id
        assignments.append(
            {
                "assignment_id": assignment_id,
                "agent_id": agent_id,
                "agent_name": agent.get("name"),
                "agent_phone": agent_phone,
                "git_branch": agent.get("git_branch")
                or agent.get("parameters", {}).get("git_branch"),
                "queue": queue_name,
                "queue_item_id": queue_item_id(queue_item),
                "task_id": metadata.get("task_id"),
                "status": "active",
                "started_at": seen_at,
                "completed_at": None,
            }
        )
        state.update(
            {
                "mode": "sequential",
                "strategy": "queue_graph",
                "status": "active",
                "current_agent_id": agent_id,
                "current_assignment_id": assignment_id,
                "current_started_at": seen_at,
                "active_task": active_task,
                "assignments": assignments,
                "updated_at": seen_at,
                "revision": int(state.get("revision") or 0) + 1,
            }
        )
        project_entry[PROJECT_AGENT_ASSIGNMENT_KEY] = state
        project_entry["updated_at"] = seen_at
        raw_projects = config.get(PROJECTS_KEY)
        projects = dict(raw_projects) if isinstance(raw_projects, dict) else {}
        projects[raw_key] = project_entry
        config[PROJECTS_KEY] = projects
        write_git_config_file(config)
        return {
            "assignment": deepcopy(state),
            "project": public_project_context(
                configured_git_context_for_key(config, context_key) or context
            ),
        }


async def dequeue_sequential_runtime_task(
    project_id: str,
    port: int | None,
) -> dict[str, Any]:
    async with group_task_submission_lock:
        snapshot = await run_group_write_transaction(
            sequential_runtime_project_snapshot_transaction,
            project_id,
        )
        assignment = snapshot["assignment"]
        if str(assignment.get("mode") or "parallel") != "sequential":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "error": "project_not_sequential",
                    "message": (
                        "The common who-am-I queue is available only for a "
                        "project imported with agents.assignment_mode=sequential"
                    ),
                },
            )

        acquired: list[asyncio.Lock] = []
        selected_queue = ""
        selected_item: Any | None = None
        try:
            for queue_name in sorted(GROUP_QUEUE_NAMES):
                await locks[queue_name].acquire()
                acquired.append(locks[queue_name])

            candidates: list[tuple[int, str, str, str, Any]] = []
            for queue_name in sorted(GROUP_QUEUE_NAMES):
                for item in queues[queue_name]:
                    metadata = queue_item_metadata(item)
                    if (
                        str(metadata.get("conversation_phone") or "").strip()
                        != snapshot["project_phone"]
                    ):
                        continue
                    if not queue_item_matches_git_context(
                        item,
                        snapshot["context_key"],
                    ):
                        continue
                    if assignment.get("strategy") == "conditional_graph":
                        if metadata.get("sequential_identity_queue") is not True:
                            continue
                        if metadata.get("identity_kind") != "reviewer_bootstrap":
                            current_assignment_id = str(
                                assignment.get("current_assignment_id") or ""
                            ).strip()
                            if current_assignment_id and str(
                                metadata.get("assignment_id") or ""
                            ).strip() != current_assignment_id:
                                continue
                    candidates.append(
                        (
                            (
                                0
                                if metadata.get("identity_kind")
                                == "reviewer_bootstrap"
                                else 1
                            ),
                            str(queue_item_queued_at(item) or ""),
                            str(queue_item_id(item) or ""),
                            queue_name,
                            item,
                        )
                    )

            if candidates:
                _, _, selected_id, selected_queue, selected_item = min(
                    candidates,
                    key=lambda candidate: (candidate[0], candidate[1], candidate[2]),
                )
                selected_metadata = queue_item_metadata(selected_item)
                target_agent = sequential_runtime_target_agent(
                    selected_metadata,
                    snapshot["agents"],
                    snapshot["project_phone"],
                )
                if target_agent is None:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail={
                            "error": "queued_target_agent_not_found",
                            "message": (
                                "The next queue item is not addressed to an agent "
                                "from the imported project team"
                            ),
                            "queue_item_id": selected_id,
                            "to_agent_id": selected_metadata.get("to_agent_id"),
                            "to_phone": selected_metadata.get("logical_to_phone")
                            or selected_metadata.get("to_phone"),
                        },
                    )

                kept: deque[Any] = deque()
                removed = False
                while queues[selected_queue]:
                    item = queues[selected_queue].popleft()
                    if not removed and str(queue_item_id(item) or "") == selected_id:
                        removed = True
                        continue
                    kept.append(item)
                queues[selected_queue] = kept
        finally:
            for queue_lock in reversed(acquired):
                queue_lock.release()

        if selected_item is None:
            recovered = await recover_sequential_runtime_identity(snapshot)
            if recovered is not None:
                return recovered
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error": "sequential_graph_queue_empty",
                    "message": (
                        "There is no active identity and no next graph node in "
                        "the project queues yet. Send the current result or task "
                        "and ask again."
                    ),
                    "project_phone": snapshot["project_phone"],
                },
            )

        metadata = deepcopy(queue_item_metadata(selected_item))
        item_id = str(queue_item_id(selected_item) or "")
        message = deepcopy(queue_item_message(selected_item))
        target_agent = sequential_runtime_target_agent(
            metadata,
            snapshot["agents"],
            snapshot["project_phone"],
        )
        if target_agent is None:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="The selected sequential target agent disappeared",
            )
        try:
            identity_transaction = (
                record_conditional_graph_identity_delivery_transaction
                if assignment.get("strategy") == "conditional_graph"
                else record_sequential_runtime_identity_transaction
            )
            identity_state = await run_group_write_transaction(
                identity_transaction,
                project_id,
                target_agent,
                selected_queue,
                deepcopy(selected_item),
                utc_now(),
            )
        except Exception:
            async with locks[selected_queue]:
                queues[selected_queue].appendleft(selected_item)
            raise
        snapshot.update(identity_state)
        await append_history(
            QUEUE_DEFINITIONS[selected_queue]["get_event"],
            selected_queue,
            message,
            {
                "queue_item_id": item_id,
                **metadata,
                "submitted_via": "sequential_agent_runtime",
                "action": "sequential_graph_node_delivered",
            },
            port,
            snapshot["queue_context"],
        )
        return {
            **snapshot,
            "agent": deepcopy(target_agent),
            "task": sequential_runtime_task_snapshot(
                selected_queue,
                selected_item,
            ),
            "identity_reused": False,
            "active_task_recovered_from_history": False,
        }


def sequential_runtime_team_directory(
    agents: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return [
        {
            "id": agent.get("id"),
            "name": agent.get("name"),
            "phone": agent.get("phone"),
            "git_branch": agent.get("git_branch")
            or agent.get("parameters", {}).get("git_branch"),
            "profile": agent.get("profile"),
            "status": agent.get("status"),
            "presence": agent.get("presence")
            or agent_presence_snapshot(agent),
        }
        for agent in agents
    ]


def sequential_runtime_outgoing_connections(
    project: dict[str, Any],
    agent: dict[str, Any],
) -> list[dict[str, Any]]:
    agent_id = str(agent.get("id") or "").strip()
    agent_phone = str(agent.get("phone") or "").strip()
    outgoing: list[dict[str, Any]] = []
    for group in project.get("groups", []):
        if not isinstance(group, dict):
            continue
        for connection in group.get("connections", []):
            if not isinstance(connection, dict):
                continue
            if (
                str(connection.get("from_agent_id") or "").strip() == agent_id
                or str(connection.get("from_phone") or "").strip() == agent_phone
            ):
                outgoing.append(deepcopy(connection))
    return outgoing


def sequential_runtime_response(
    runtime: dict[str, Any],
    request: Request,
) -> dict[str, Any]:
    base_url = str(request.base_url).rstrip("/")
    agent = runtime["agent"]
    agent_id = str(agent.get("id") or "").strip()
    agent_name = str(agent.get("name") or "").strip()
    agent_phone = str(agent.get("phone") or "").strip()
    git_branch = str(
        agent.get("git_branch")
        or agent.get("parameters", {}).get("git_branch")
        or ""
    ).strip()
    project_phone = runtime["project_phone"]
    reply_path = "/api/v1/agents/whoami/repository"
    git_address = runtime["project"].get("git_address")
    metadata = runtime["task"]["metadata"]
    identity_kind = str(metadata.get("identity_kind") or "graph_node")
    reviewer_bootstrap = identity_kind == "reviewer_bootstrap"
    assignment = runtime.get("assignment") or {}
    assignment_strategy = str(assignment.get("strategy") or "queue_graph")
    team = sequential_runtime_team_directory(runtime["agents"])
    send_payload = {
        "from_phone": agent_phone,
        "to_phone": "<phone из team>",
        "sender": agent_name,
        "receiver": "<name из team>",
        "message": "<результат или следующее задание>",
    }
    return {
        "answer": (
            (
                f"Сейчас вы ревьювер №{metadata.get('reviewer_index')} — "
                f"{agent_name} (id={agent_id}, phone={agent_phone}). "
                "Эта роль закреплена за вами на всё время проекта."
            )
            if reviewer_bootstrap
            else (
                f"Сейчас вы агент {agent_name} (id={agent_id}, phone={agent_phone}). "
                "Эта рабочая роль выбрана текущим узлом графа."
            )
        ),
        "assignment_mode": "sequential",
        "assignment_strategy": assignment_strategy,
        "identity_reused": bool(runtime.get("identity_reused")),
        "active_task_recovered_from_history": bool(
            runtime.get("active_task_recovered_from_history")
        ),
        "identity_kind": identity_kind,
        "identity_persistent": reviewer_bootstrap,
        "execution_authorized": True,
        "requires_additional_confirmation": False,
        "reviewer_index": metadata.get("reviewer_index"),
        "phase": metadata.get("phase") or assignment.get("phase") or "node",
        "project_id": project_phone,
        "project_phone": project_phone,
        "project": runtime["project"],
        "agent": agent,
        "profile": agent.get("profile"),
        "git_branch": git_branch,
        "active_task": runtime["task"],
        "graph_position": {
            "queue": runtime["task"]["queue"],
            "queue_item_id": runtime["task"]["id"],
            "cycle_id": metadata.get("cycle_id"),
            "group_id": metadata.get("group_id"),
            "connection_id": metadata.get("connection_id"),
            "task_id": metadata.get("task_id"),
            "task_node_id": metadata.get("task_node_id"),
            "parent_task_id": metadata.get("parent_task_id"),
            "parent_task_node_id": metadata.get("parent_task_node_id"),
        },
        "team": team,
        "communication": {
            "project_phone": project_phone,
            "send_endpoints": {
                queue_name: f"{base_url}{group_queue_route(queue_name, project_phone)}"
                for queue_name in sorted(GROUP_QUEUE_NAMES)
            },
            "send_payload_template": send_payload,
            "outgoing_graph_connections": sequential_runtime_outgoing_connections(
                runtime["project"],
                agent,
            ),
            "instructions": (
                (
                    "Вы постоянный ревьювер. Читайте полный project_state и новые "
                    "запросы по своему logical phone; решение отправляйте в "
                    "review_endpoint из active_task.metadata."
                )
                if reviewer_bootstrap
                else (
                    "Отправьте результат или решение ревью в whoami_endpoint из "
                    "active_task.metadata, затем запросите следующий рабочий узел."
                )
                if assignment_strategy == "conditional_graph"
                else (
                    "Отправьте результат или новое задание нужному агенту из team, "
                    "затем снова запросите следующую личность. После получения "
                    "agent и active_task сразу начинайте работу и не ждите "
                    "дополнительного подтверждения. Повторный запрос до handoff "
                    "вернёт эту же активную identity/task."
                )
            ),
        },
        "next_identity_request": (
            None
            if reviewer_bootstrap
            else {
                "method": "POST",
                "url": f"{base_url}{reply_path}",
                "json": {"git_address": git_address},
            }
        ),
    }


@app.get("/api/v1/agents/whoami")
@app.post("/api/v1/agents/whoami")
async def begin_sequential_agent_identity(request: Request) -> dict[str, Any]:
    base_url = str(request.base_url).rstrip("/")
    reply_url = f"{base_url}/api/v1/agents/whoami/repository"
    return {
        "status": "repository_required",
        "question": "Из какого Git-репозитория вы пришли?",
        "instructions": (
            "Отправьте git_address на reply_url один раз и сохраните ответ. "
            "Если ответ содержит agent и active_task, покажите требуемую сводку "
            "и сразу приступайте к работе без дополнительного подтверждения."
        ),
        "reply": {
            "method": "POST",
            "url": reply_url,
            "content_type": "application/json",
            "json": {
                "git_address": "https://github.com/owner/repository.git",
                "git_context_key": "optional-for-repository-with-several-projects",
            },
        },
        "reply_url": reply_url,
    }


@app.post("/api/v1/agents/whoami/repository")
async def identify_sequential_agent_from_repository(
    request: Request,
) -> dict[str, Any]:
    payload = await read_message(request)
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Expected JSON object with git_address",
        )
    if payload.get("git_address") is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="git_address is required",
        )
    project_phone = await project_phone_for_actor_import(
        payload,
        request_port(request),
    )
    runtime = await dequeue_sequential_runtime_task(
        project_phone,
        port=request_port(request),
    )
    response = sequential_runtime_response(runtime, request)
    project_state = await project_state_json(project_phone)
    response["communication"]["instructions"] += (
        " Полная история сообщений и межкоммитные diff-блоки находятся в "
        "project_state.activity_with_patches. Для одного commit patch не "
        "повторяется; при смене commit patch расположен перед первым сообщением "
        "нового commit. Сводка находится в project_state.code_patch_summary."
    )
    response["project_state"] = project_state
    response["project_state_url"] = (
        f"{str(request.base_url).rstrip('/')}"
        f"/api/v1/projects/{project_phone}/state.json"
    )
    return response


@app.get("/api/v1/projects/{project_id}/state")
@app.get("/api/v1/projects/{project_id}/state.json")
async def get_project_state_json(
    project_id: str,
    history_limit: int = 200,
) -> dict[str, Any]:
    return await project_state_json(project_id, history_limit=history_limit)


def project_sprint_history_file_snapshot(
    context_key: str,
) -> dict[str, Any] | None:
    with sprint_history_file_lock():
        history = read_sprint_history_file()
        project = history.get("projects", {}).get(context_key)
        return deepcopy(project) if isinstance(project, dict) else None


async def project_sprint_history_snapshot(
    project_id: str,
) -> tuple[str, str, dict[str, Any] | None]:
    config = await read_git_config()
    _, context_key, project_entry, _ = project_for_group_api(config, project_id)
    project_phone = normalize_project_phone(project_entry.get("project_phone"))
    async with sprint_history_lock:
        project = await asyncio.to_thread(
            project_sprint_history_file_snapshot,
            context_key,
        )
    return project_phone, context_key, project


@app.get("/api/v1/projects/{project_id}/sprints")
async def get_project_sprints(project_id: str) -> dict[str, Any]:
    project_phone, context_key, project = await project_sprint_history_snapshot(
        project_id
    )
    raw_records = project.get("sprints") if isinstance(project, dict) else []
    records = [
        sprint_record_summary(record)
        for record in raw_records
        if isinstance(record, dict)
    ] if isinstance(raw_records, list) else []
    records.sort(
        key=lambda record: int(record.get("sequence") or 0),
        reverse=True,
    )
    for record in records:
        record["download_url"] = (
            f"/api/v1/projects/{project_phone}/sprints/{record['id']}/download"
        )
    return {
        "project_id": project_phone,
        "project_phone": project_phone,
        "git_context_key": context_key,
        "current_sprint_id": (
            project.get("current_sprint_id") if isinstance(project, dict) else None
        ),
        "sprint_count": len(records),
        "sprints": records,
    }


@app.get("/api/v1/projects/{project_id}/sprints/{sprint_id}/download")
async def download_project_sprint(
    project_id: str,
    sprint_id: str,
) -> JSONResponse:
    project_phone, context_key, project = await project_sprint_history_snapshot(
        project_id
    )
    raw_records = project.get("sprints") if isinstance(project, dict) else []
    record = next(
        (
            deepcopy(item)
            for item in raw_records
            if isinstance(item, dict)
            and str(item.get("id") or "").strip() == sprint_id.strip()
        ),
        None,
    ) if isinstance(raw_records, list) else None
    if record is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Sprint archive was not found for this project",
        )
    runtime_state = record.get("final_state") or record.get("initial_state")
    if str(record.get("status") or "") == "current":
        runtime_state = await project_state_json(project_phone, history_limit=10000)
    code_history = record.get("final_code_history")
    if not isinstance(code_history, dict) and isinstance(runtime_state, dict):
        runtime_code_history = runtime_state.get("history_with_patches")
        if isinstance(runtime_code_history, dict):
            code_history = runtime_code_history
    if not isinstance(code_history, dict):
        runtime_project = (
            runtime_state.get("project")
            if isinstance(runtime_state, dict)
            and isinstance(runtime_state.get("project"), dict)
            else {}
        )
        runtime_records = (
            runtime_state.get("recent_activity")
            if isinstance(runtime_state, dict)
            and isinstance(runtime_state.get("recent_activity"), list)
            else []
        )
        code_history = await asyncio.to_thread(
            history_with_patches_context,
            runtime_records,
            str(runtime_project.get("git_address") or "").strip(),
        )
    metadata = sprint_record_summary(record)
    archive = {
        "schema_version": 1,
        "archive_type": "nginx-qa-project-sprint",
        "exported_at": utc_now(),
        "project_id": project_phone,
        "project_phone": project_phone,
        "git_context_key": context_key,
        "sprint": metadata,
        "import_payload": deepcopy(record.get("import_payload")),
        "initial_state": deepcopy(record.get("initial_state")),
        "runtime_state": deepcopy(runtime_state),
        "code_history": deepcopy(code_history),
    }
    sequence = int(record.get("sequence") or 0)
    filename = f"project-{project_phone}-sprint-{sequence:04d}.json"
    return JSONResponse(
        content=archive,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/api/v1/projects/{project_id}/agents")
@app.get("/api/v1/projects/{project_id}/actors")
async def get_project_actors(project_id: str) -> dict[str, Any]:
    async with git_config_lock:
        async with agents_lock:
            config = await asyncio.to_thread(read_git_config_file)
            _, context_key, project_entry, context = project_for_group_api(
                config,
                project_id,
            )
            agents = await asyncio.to_thread(read_agents_file)
    project_agents = full_agents_for_project(
        agents,
        context_key,
        phone_git_contexts_from_config(config),
    )
    assignment = (
        deepcopy(project_entry.get(PROJECT_AGENT_ASSIGNMENT_KEY))
        if isinstance(project_entry.get(PROJECT_AGENT_ASSIGNMENT_KEY), dict)
        else {"mode": "parallel", "status": "parallel"}
    )
    return {
        "project_id": normalize_project_phone(project_entry.get("project_phone")),
        "project_phone": normalize_project_phone(project_entry.get("project_phone")),
        "project": public_project_context(context),
        "assignment_mode": assignment.get("mode") or "parallel",
        "assignment": assignment,
        "agents": project_agents,
        "agent_count": len(project_agents),
        "actors": project_agents,
        "actor_count": len(project_agents),
    }


async def ensure_actor_import_matches_route_project(
    project_id: str,
    payload: Any,
    current_port: int | None,
) -> dict[str, str]:
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Expected JSON object",
        )

    reference_items = {
        key: actor_import_reference_items(payload, key)
        for key in ("project_id", "project_phone", "git_address", "git_context_key")
    }
    if not any(reference_items.values()):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "project_reference_required",
                "message": (
                    "Agent JSON must identify its project with project_id/"
                    "project_phone, git_context_key, or git_address"
                ),
            },
        )

    config = await read_git_config()
    _, route_context_key, project_entry, route_context = project_for_group_api(
        config,
        project_id,
    )
    route_phone = normalize_project_phone(project_entry.get("project_phone"))
    route_repository_key = project_repository_key_for_context(route_context)

    def mismatch(field: str, supplied: Any) -> None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "project_reference_mismatch",
                "message": (
                    f"JSON field {field} does not match the project selected in "
                    "the import URL"
                ),
                "route_project_phone": route_phone,
                "route_git_context_key": route_context_key,
                "route_repository_key": route_repository_key,
                "json_field": field,
                "json_value": supplied,
            },
        )

    has_unique_project_reference = False
    for key in ("project_id", "project_phone"):
        for field, supplied in reference_items[key]:
            supplied_phone = normalize_project_phone(supplied)
            if not supplied_phone:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail={
                        "error": "invalid_project_reference",
                        "field": field,
                        "message": "Project phone must be a registered four-digit value",
                    },
                )
            if supplied_phone != route_phone:
                mismatch(field, supplied)
            has_unique_project_reference = True

    for field, supplied in reference_items["git_address"]:
        _, supplied_repository_key = normalize_project_git_address(supplied)
        if supplied_repository_key != route_repository_key:
            mismatch(field, supplied)

    for field, supplied in reference_items["git_context_key"]:
        if not isinstance(supplied, str) or not supplied.strip():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "error": "invalid_project_reference",
                    "field": field,
                    "message": "git_context_key must be a non-empty string",
                },
            )
        supplied_context_key = normalize_project_context_reference(supplied)
        if supplied_context_key != route_context_key:
            mismatch(field, supplied)
        has_unique_project_reference = True

    if not has_unique_project_reference:
        matching_contexts = [
            context
            for context in configured_git_contexts_from_config(config, current_port)
            if project_repository_key_for_context(context) == route_repository_key
        ]
        if len(matching_contexts) != 1:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "error": "ambiguous_project_reference",
                    "message": (
                        "git_address identifies a repository used by several projects; "
                        "add project_id/project_phone or exact git_context_key"
                    ),
                    "candidates": [
                        public_project_context(context)
                        for context in matching_contexts
                    ],
                },
            )

    return {
        "project_phone": route_phone,
        "git_context_key": route_context_key,
        "repository_key": route_repository_key,
    }


@app.post(
    "/api/v1/projects/{project_id}/agents/import",
    status_code=status.HTTP_201_CREATED,
)
@app.post(
    "/api/v1/projects/{project_id}/actors/import",
    status_code=status.HTTP_201_CREATED,
)
async def import_project_actors(
    project_id: str,
    request: Request,
) -> dict[str, Any]:
    payload = await read_message(request)
    validated_project = await ensure_actor_import_matches_route_project(
        project_id,
        payload,
        request_port(request),
    )
    encoded_filename = request.headers.get("x-nginx-qa-sprint-filename", "").strip()
    source_filename = urllib.parse.unquote(encoded_filename) if encoded_filename else ""
    return await import_project_actors_data(
        project_id,
        payload,
        source="api",
        source_filename=source_filename,
        port=request_port(request),
        activate_sequential=True,
        expected_git_context_key=validated_project["git_context_key"],
        expected_repository_key=validated_project["repository_key"],
    )


@app.delete("/api/v1/projects/{project_id}/agents")
@app.delete("/api/v1/projects/{project_id}/actors")
async def delete_project_actors(
    project_id: str,
    include_managed: bool = False,
) -> dict[str, Any]:
    return await delete_project_actors_data(
        project_id,
        include_managed=include_managed,
    )


@app.get("/api/v1/projects/{project_id}/agents/{agent_phone}")
async def get_project_agent(
    project_id: str,
    agent_phone: str,
) -> dict[str, Any]:
    async with git_config_lock:
        async with agents_lock:
            config = await asyncio.to_thread(read_git_config_file)
            _, context_key, project_entry, context = project_for_group_api(
                config,
                project_id,
            )
            agents = await asyncio.to_thread(read_agents_file)
    project_agents = full_agents_for_project(
        agents,
        context_key,
        phone_git_contexts_from_config(config),
    )
    clean_phone = agent_phone.strip()
    target_agent = next(
        (
            agent
            for agent in project_agents
            if str(agent.get("phone") or "").strip() == clean_phone
        ),
        None,
    )
    if target_agent is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Agent phone was not found in this project",
        )
    return {
        "project_id": normalize_project_phone(project_entry.get("project_phone")),
        "project_phone": normalize_project_phone(project_entry.get("project_phone")),
        "project": public_project_context(context),
        "agent": target_agent,
        "profile": target_agent.get("profile"),
        "git_branch": target_agent.get("git_branch")
        or target_agent.get("parameters", {}).get("git_branch"),
        "presence": target_agent.get("presence")
        or agent_presence_snapshot(target_agent),
    }


@app.post("/api/v1/projects/{project_id}/agents/{agent_phone}/whoami")
async def identify_project_agent(
    project_id: str,
    agent_phone: str,
    request: Request,
) -> dict[str, Any]:
    request_message = "Кто я?"
    identity_payload: dict[str, Any] = {}
    raw_body = await request.body()
    if raw_body:
        try:
            payload = json.loads(raw_body)
        except json.JSONDecodeError as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid JSON body",
            ) from exc
        if not isinstance(payload, dict):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Expected JSON object",
            )
        identity_payload = payload
        request_message = str(payload.get("message") or request_message).strip()
        if len(request_message) > 1000:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Identity request message must not exceed 1000 characters",
            )

    normalized_message = request_message.casefold()
    complete_current = bool(
        identity_payload.get("completed") is True
        or str(identity_payload.get("status") or "").strip().upper()
        in {"COMPLETED", "DONE"}
        or re.search(
            r"(?:задани\w*|работ\w*|роль)\s+(?:выполн\w*|заверш\w*)|\b(?:completed|done)\b",
            normalized_message,
        )
    )

    snapshot = await run_group_write_transaction(
        project_agent_identity_snapshot_transaction,
        project_id,
        agent_phone,
    )
    if snapshot.get("assignment_mode") == "sequential":
        workflow = snapshot.get("assignment", {}).get("workflow")
        if isinstance(workflow, dict) and workflow.get("enabled"):
            submitted_outcome = str(
                identity_payload.get("outcome")
                or identity_payload.get("status")
                or ""
            ).strip()
            response = await identify_sequential_project_agent(
                project_id,
                agent_phone,
                request_message,
                complete_current or bool(submitted_outcome),
                request,
                submitted_outcome=submitted_outcome,
                submitted_feedback=str(
                    identity_payload.get("feedback") or ""
                ),
                submitted_result=str(identity_payload.get("result") or ""),
                expected_assignment_id=str(
                    identity_payload.get("assignment_id") or ""
                ),
                identity_payload=identity_payload,
            )
            response["project_state"] = await project_state_json(project_id)
            response["project_state_url"] = (
                f"{str(request.base_url).rstrip('/')}"
                f"/api/v1/projects/{snapshot['project_phone']}/state.json"
            )
            return response
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "use_sequential_queue_graph_identity",
                "message": (
                    "A sequential executor is not permanently bound to this "
                    "phone. Start at /api/v1/agents/whoami and use the returned "
                    "reply_url; the next queued item selects the current role."
                ),
                "whoami_url": (
                    f"{str(request.base_url).rstrip('/')}/api/v1/agents/whoami"
                ),
            },
        )
    async with history_lock:
        previous_history = await asyncio.to_thread(
            read_agent_work_history_file,
            snapshot["agent"],
            snapshot["context_key"],
            None,
        )
    parameters = (
        snapshot["agent"].get("parameters")
        if isinstance(snapshot["agent"].get("parameters"), dict)
        else {}
    )
    earliest_history_at = (
        str(previous_history[0].get("timestamp") or "")
        if previous_history
        else ""
    )
    seen_at = utc_now()
    suggested_created_at = earliest_iso_timestamp(
        parameters.get("created_at"),
        earliest_history_at,
        seen_at,
    )
    updated = await run_group_write_transaction(
        mark_project_agent_alive_transaction,
        project_id,
        agent_phone,
        seen_at,
        suggested_created_at,
    )
    agent = updated["agent"]
    agent_id = str(agent.get("id") or "").strip()
    agent_name = str(agent.get("name") or "").strip()
    clean_phone = str(agent.get("phone") or "").strip()
    await append_history(
        "agent_identity_heartbeat",
        "worker-all",
        request_message or "Кто я?",
        {
            "submitted_via": "agent_whoami",
            "action": "agent_marked_alive",
            "sender": agent_name,
            "receiver": agent_name,
            "from_phone": clean_phone,
            "to_phone": clean_phone,
            "from_agent_id": agent_id,
            "to_agent_id": agent_id,
            "project_phone": updated["project_phone"],
            "presence_status": "alive",
        },
        request_port(request),
        updated["queue_context"],
    )
    created_at = str(agent.get("parameters", {}).get("created_at") or seen_at)
    async with history_lock:
        work_history = await asyncio.to_thread(
            read_agent_work_history_file,
            agent,
            updated["context_key"],
            created_at,
        )
    assigned_tasks = deepcopy(agent.get("tasks") or [])
    work_summary = agent_work_history_summary(
        work_history,
        len(assigned_tasks),
    )
    git_branch = str(
        agent.get("git_branch")
        or agent.get("parameters", {}).get("git_branch")
        or ""
    ).strip()
    project_directory = [
        {
            "id": project_agent.get("id"),
            "name": project_agent.get("name"),
            "phone": project_agent.get("phone"),
            "git_branch": project_agent.get("git_branch")
            or project_agent.get("parameters", {}).get("git_branch"),
            "presence": project_agent.get("presence")
            or agent_presence_snapshot(project_agent),
        }
        for project_agent in updated["agents"]
    ]
    answer = (
        f"Вы — {agent_name} (id={agent_id}, phone={clean_phone}). "
        f"Ваша рабочая ветка: {git_branch or 'не назначена'}. "
        f"Назначено задач: {len(assigned_tasks)}; событий работы с момента создания: "
        f"{len(work_history)}. Вы отмечены как живой агент."
    )
    return {
        "answer": answer,
        "identity_request": request_message or "Кто я?",
        "project_id": updated["project_id"],
        "project_phone": updated["project_phone"],
        "project": updated["project"],
        "agent": agent,
        "profile": agent.get("profile"),
        "git_branch": git_branch,
        "presence": agent.get("presence") or agent_presence_snapshot(agent),
        "assigned_tasks": assigned_tasks,
        "work_summary": work_summary,
        "work_history_since": created_at,
        "work_history": work_history,
        "project_agents": project_directory,
    }


def actor_payload_with_assignment_mode(
    payload: dict[str, Any],
    assignment_mode: str,
) -> dict[str, Any]:
    if assignment_mode not in AGENT_ASSIGNMENT_MODES:
        raise ValueError(f"Unsupported agent assignment mode: {assignment_mode}")
    normalized_payload = deepcopy(payload)
    normalized_payload["assignment_mode"] = assignment_mode
    for section_name in ("agents", "actors"):
        section = normalized_payload.get(section_name)
        if isinstance(section, dict):
            section["assignment_mode"] = assignment_mode
    return normalized_payload


async def telegram_actor_import_for_mode(
    request: Request,
    assignment_mode: str,
) -> dict[str, Any]:
    expected_secret = os.getenv("TELEGRAM_WEBHOOK_SECRET", "").strip()
    supplied_secret = request.headers.get("x-telegram-bot-api-secret-token", "").strip()
    if expected_secret and not hmac.compare_digest(expected_secret, supplied_secret):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid Telegram webhook secret",
        )
    update = await read_message(request)
    payload, message = await asyncio.to_thread(actor_payload_from_telegram_update, update)
    ensure_telegram_sender_allowed(message)
    payload = actor_payload_with_assignment_mode(payload, assignment_mode)
    project_id = await project_phone_for_actor_import(
        payload,
        request_port(request),
    )
    validated_project = await ensure_actor_import_matches_route_project(
        project_id,
        payload,
        request_port(request),
    )
    document = message.get("document") if isinstance(message, dict) else None
    source_filename = (
        str(document.get("file_name") or "").strip()
        if isinstance(document, dict)
        else ""
    )
    result = await import_project_actors_data(
        project_id,
        payload,
        source="telegram",
        source_filename=source_filename or "telegram-message.json",
        port=request_port(request),
        activate_sequential=assignment_mode == "sequential",
        expected_git_context_key=validated_project["git_context_key"],
        expected_repository_key=validated_project["repository_key"],
    )
    await asyncio.to_thread(send_telegram_import_reply, message, result)
    return {"ok": True, **result}


@app.post("/api/v1/telegram/agents/sequential")
async def telegram_sequential_agent_import(request: Request) -> dict[str, Any]:
    return await telegram_actor_import_for_mode(request, "sequential")


@app.post("/api/v1/telegram/agents/parallel")
async def telegram_parallel_agent_import(request: Request) -> dict[str, Any]:
    return await telegram_actor_import_for_mode(request, "parallel")


@app.post("/api/v1/telegram/agents")
@app.post("/api/v1/telegram/actors")
async def telegram_actor_import(request: Request) -> dict[str, Any]:
    return await telegram_actor_import_for_mode(request, "parallel")


@app.get("/api/v1/group-templates")
async def get_group_templates() -> dict[str, Any]:
    registry = await asyncio.to_thread(read_group_templates_file)
    validated_templates: dict[str, dict[str, Any]] = {}
    for template_id in registry["group_templates"]:
        blueprint = await asyncio.to_thread(
            group_blueprint_from_payload,
            registry,
            {
                "template_id": template_id,
                "group_key": template_id,
            },
        )
        validated_templates[template_id] = {
            **deepcopy(registry["group_templates"][template_id]),
            "template_fingerprint": blueprint["template_fingerprint"],
        }
    return {
        "schema_version": registry.get("schema_version", 1),
        "registry_path": str(group_templates_path),
        "agent_specs": deepcopy(registry["agent_specs"]),
        "group_templates": validated_templates,
        "group_topologies": deepcopy(registry.get("group_topologies", {})),
    }


@app.get("/api/v1/group-templates/{template_id}")
async def get_group_template(template_id: str) -> dict[str, Any]:
    registry = await asyncio.to_thread(read_group_templates_file)
    clean_template_id, template = group_template_for_id(registry, template_id)
    blueprint = await asyncio.to_thread(
        group_blueprint_from_payload,
        registry,
        {
            "template_id": clean_template_id,
            "group_key": clean_template_id,
        },
    )
    referenced_specs = {
        definition["spec"]: deepcopy(registry["agent_specs"][definition["spec"]])
        for definition in blueprint["definitions"]
    }
    return {
        "template": {
            **template,
            "template_fingerprint": blueprint["template_fingerprint"],
        },
        "agent_specs": referenced_specs,
    }


@app.get("/api/v1/projects/{project_id}/groups")
async def get_project_groups(
    project_id: str,
    include_archived: bool = True,
) -> dict[str, Any]:
    config = await read_git_config()
    _, context_key, project_entry, context = project_for_group_api(config, project_id)
    registry = await asyncio.to_thread(read_group_templates_file)
    refresh_project_group_relationships(project_entry, registry)
    raw_groups = project_entry.get("groups")
    groups = [
        deepcopy(group)
        for group in raw_groups
        if isinstance(group, dict)
        and (
            include_archived
            or str(group.get("status") or "active").strip() != "archived"
        )
    ] if isinstance(raw_groups, list) else []
    public_context = {
        **context,
        "groups": deepcopy(project_entry.get("groups", [])),
        "group_relationships": deepcopy(
            project_entry.get("group_relationships", [])
        ),
        "customer_reporting": deepcopy(
            project_entry.get("customer_reporting", {})
        ),
    }
    return {
        "project_id": normalize_project_phone(project_entry.get("project_phone")),
        "project": public_project_context(public_context),
        "group_count": len(groups),
        "groups": groups,
        "group_relationships": deepcopy(
            project_entry.get("group_relationships", [])
        ),
        "customer_reporting": deepcopy(
            project_entry.get("customer_reporting", {})
        ),
        "git_context_key": context_key,
    }


@app.post("/api/v1/projects/{project_id}/groups")
async def post_project_group(
    project_id: str,
    request: Request,
) -> dict[str, Any]:
    payload = await read_message(request)
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Expected JSON object",
        )
    return await run_group_write_transaction(
        create_group_transaction,
        project_id,
        payload,
    )


@app.get("/api/v1/groups/{group_id}")
async def get_group(group_id: str) -> dict[str, Any]:
    group_data = await read_group_with_agents(group_id)
    queue_tasks = await queued_group_tasks(group_id)
    return {
        "project": group_data["project"],
        "group": group_data["group"],
        "agent_profiles": group_data["agents"],
        "queue_size": len(queue_tasks),
        "queue_tasks": queue_tasks,
    }


@app.put("/api/v1/groups/{group_id}")
async def put_group(group_id: str, request: Request) -> dict[str, Any]:
    payload = await read_message(request)
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Expected JSON object",
        )
    return await run_group_write_transaction(
        update_group_transaction,
        group_id,
        payload,
    )


@app.delete("/api/v1/groups/{group_id}")
async def delete_group(group_id: str) -> dict[str, Any]:
    return await run_group_write_transaction(
        delete_group_transaction,
        group_id,
    )


@app.post(
    "/api/v1/groups/{group_id}/tasks",
    status_code=status.HTTP_201_CREATED,
)
async def post_group_task(
    group_id: str,
    request: Request,
) -> dict[str, Any]:
    payload = await read_message(request)
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Expected JSON object",
        )
    return await enqueue_external_group_task(
        normalized_group_id(group_id),
        payload,
        request_port(request),
    )


@app.post(
    "/api/v1/groups/{group_id}/connections/{connection_id}/tasks",
    status_code=status.HTTP_201_CREATED,
)
async def post_group_connection_task(
    group_id: str,
    connection_id: str,
    request: Request,
) -> dict[str, Any]:
    payload = await read_message(request)
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Expected JSON object",
        )
    return await enqueue_group_connection_task(
        normalized_group_id(group_id),
        normalized_group_id(connection_id),
        payload,
        request_port(request),
    )


@app.get("/api/v1/groups/{group_id}/agents/{agent_id}/tasks")
async def get_group_agent_task(
    group_id: str,
    agent_id: str,
    request: Request,
) -> dict[str, Any]:
    return await dequeue_group_agent_task(
        normalized_group_id(group_id),
        str(agent_id or "").strip(),
        request_port(request),
    )


@app.get("/api/v1/projects/{project_id}/cycles")
async def get_project_cycles(
    project_id: str,
    cycle_status: str | None = None,
    group_id: str | None = None,
    limit: int = 200,
) -> dict[str, Any]:
    config = await read_git_config()
    _, context_key, project_entry, _ = project_for_group_api(config, project_id)
    project_phone = normalize_project_phone(project_entry.get("project_phone"))
    clean_status = str(cycle_status or "").strip().lower()
    if clean_status and clean_status not in {"queued", "in_progress", "completed"}:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="cycle_status must be queued, in_progress, or completed",
        )
    clean_group_id = str(group_id or "").strip()
    safe_limit = max(1, min(limit, 10000))
    records = await read_cycle_records(project_phone=project_phone)
    summaries = cycle_summaries_from_records(records)
    if clean_status:
        summaries = [
            summary
            for summary in summaries
            if summary.get("status") == clean_status
        ]
    if clean_group_id:
        summaries = [
            summary
            for summary in summaries
            if clean_group_id in summary.get("group_ids", [])
        ]
    summaries = summaries[:safe_limit]
    return {
        "project_id": project_phone,
        "project_phone": project_phone,
        "git_context_key": context_key,
        "cycle_count": len(summaries),
        "cycles": summaries,
    }


@app.get("/api/v1/cycles/{cycle_id}/history")
async def get_cycle_history(
    cycle_id: str,
    event_type: str | None = None,
    limit: int = 10000,
) -> dict[str, Any]:
    records, summary = await cycle_records_and_summary(cycle_id)
    events = cycle_events_from_records(records)
    clean_event_type = str(event_type or "").strip().upper()
    if clean_event_type:
        events = [
            event
            for event in events
            if event.get("event_type") == clean_event_type
        ]
    safe_limit = max(1, min(limit, 50000))
    events = events[-safe_limit:]
    return {
        "cycle": summary,
        "event_count": len(events),
        "events": events,
    }


@app.get("/api/v1/cycles/{cycle_id}/graph")
async def get_cycle_graph(cycle_id: str) -> dict[str, Any]:
    records, summary = await cycle_records_and_summary(cycle_id)
    events = cycle_events_from_records(records)
    return cycle_graph_from_events(summary, events)


@app.post(
    "/api/v1/cycles/{cycle_id}/events",
    status_code=status.HTTP_201_CREATED,
)
async def post_cycle_event(
    cycle_id: str,
    request: Request,
) -> dict[str, Any]:
    clean_cycle_id = normalized_cycle_id(cycle_id, required=True)
    payload = await read_message(request)
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Expected JSON object",
        )
    normalized_payload = normalize_cycle_lifecycle_payload(payload)
    async with group_task_submission_lock:
        records, summary = await cycle_records_and_summary(clean_cycle_id)
        canonicalize_cycle_task_reference(records, normalized_payload)
        deduplicated = deduplicated_cycle_lifecycle_result(
            records,
            summary,
            normalized_payload,
        )
        if deduplicated is not None:
            if normalized_payload["event_type"] == "CYCLE_COMPLETED":
                cancelled_tasks = await remove_queued_cycle_items(clean_cycle_id)
                refreshed_records = await read_cycle_records(
                    cycle_id=clean_cycle_id
                )
                deduplicated["cycle"] = cycle_summary_from_records(
                    refreshed_records
                )
                deduplicated["cancelled_queue_task_count"] = int(
                    (deduplicated["cycle"] or {}).get("cancelled_task_count") or 0
                )
                deduplicated["newly_cancelled_queue_task_count"] = len(
                    cancelled_tasks
                )
            return deduplicated
        group_data: dict[str, Any] | None = None
        group_id = normalized_payload["group_id"]
        if group_id:
            group_data = await read_group_with_agents(group_id)
            group_project_phone = str(
                group_data["group"].get("project_phone") or ""
            ).strip()
            if group_project_phone != str(summary.get("project_id") or ""):
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "error": "cycle_group_project_conflict",
                        "cycle_id": clean_cycle_id,
                        "group_id": group_id,
                    },
                )
            if group_id not in summary.get("group_ids", []):
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "error": "group_not_in_cycle",
                        "cycle_id": clean_cycle_id,
                        "group_id": group_id,
                    },
                )

        actor = cycle_lifecycle_actor(normalized_payload, group_data)
        if normalized_payload["event_type"] == "GROUP_REPORT_SUBMITTED":
            reporting_rule = (
                group_data["group"].get("reporting_rule", {})
                if group_data is not None
                else {}
            )
            expected_reporter_role = str(
                reporting_rule.get("report_from") or ""
            ).strip()
            if (
                expected_reporter_role
                and actor.get("from_role") != expected_reporter_role
            ):
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail={
                        "error": "cycle_reporter_role_forbidden",
                        "expected_role": expected_reporter_role,
                        "received_role": actor.get("from_role"),
                    },
                )
            report_to = str(
                reporting_rule.get("report_to") or "project_manager"
            ).strip()
            if report_to == "project_manager":
                actor.update(
                    {
                        "to_agent_id": PROJECT_MANAGER_AGENT_ID,
                        "to_role": "project_manager",
                        "to_phone": PROJECT_MANAGER_PHONE,
                        "receiver": PROJECT_MANAGER_AGENT_NAME,
                    }
                )
            else:
                actor.update(
                    {
                        "to_role": report_to or "customer",
                        "receiver": report_to or "Customer",
                    }
                )
        elif normalized_payload["event_type"] == "CYCLE_COMPLETED":
            actor.update({"receiver": "Customer"})
        git_context = (
            group_git_context_for_queue(group_data)
            if group_data is not None
            else {
                "git_context_key": summary.get("git_context_key"),
                "project_phone": summary.get("project_id"),
            }
        )
        result = await append_cycle_lifecycle_event(
            clean_cycle_id,
            summary,
            normalized_payload,
            actor,
            git_context,
        )
        if (
            normalized_payload["event_type"] == "CYCLE_COMPLETED"
        ):
            cancelled_tasks = await remove_queued_cycle_items(clean_cycle_id)
            refreshed_records = await read_cycle_records(cycle_id=clean_cycle_id)
            result["cycle"] = cycle_summary_from_records(refreshed_records)
            result["cancelled_queue_task_count"] = int(
                (result["cycle"] or {}).get("cancelled_task_count") or 0
            )
            result["newly_cancelled_queue_task_count"] = len(cancelled_tasks)
        return result


@app.post("/project-manager/0001")
async def post_project_manager_0001(request: Request) -> dict[str, Any]:
    payload = await read_message(request)
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Expected JSON object",
        )

    git_address, repository_key = normalize_project_git_address(
        payload.get("git_address")
    )
    requested_context_key = normalize_requested_project_context_key(
        payload.get("git_context_key"),
        repository_key,
    )

    project_name = payload.get("project_name")
    if project_name is not None and not isinstance(project_name, str):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="project_name must be a string",
        )

    created, project_context, project_phone, phone_assigned = await resolve_or_create_project(
        git_address,
        repository_key,
        requested_context_key,
        project_name,
        request_port(request),
    )
    config, agents, cycle_records = await asyncio.gather(
        read_git_config(),
        read_agents(),
        read_cycle_records(project_phone=project_phone),
    )
    target_context_key = str(project_context.get("git_context_key") or "").strip()
    project_agents = full_agents_for_project(
        agents,
        target_context_key,
        phone_git_contexts_from_config(config),
    )
    return {
        "project_manager_phone": PROJECT_MANAGER_PHONE,
        "project_phone": project_phone,
        "phone_assigned": phone_assigned,
        "created": created,
        "project": public_project_context(project_context),
        "agent_count": len(project_agents),
        "agents": project_agents,
        "group_count": len(project_context.get("groups", []))
        if isinstance(project_context.get("groups"), list)
        else 0,
        "cycle_count": len(cycle_summaries_from_records(cycle_records)),
    }


@app.get("/git-config")
async def get_git_config(request: Request) -> dict[str, Any]:
    port = request_port(request)
    config = await read_git_config()
    entry = config.get(str(port), {}) if port is not None else {}
    phone_contexts = phone_git_contexts_from_config(config)
    git_address = entry.get("git_address") if isinstance(entry, dict) else None
    project_name = normalize_project_name(
        entry.get("project_name") if isinstance(entry, dict) else None,
        git_address if isinstance(git_address, str) else "",
    ) if git_address else ""
    git_context_key = (
        normalize_project_context_reference(
            str(entry.get("git_context_key") or "").strip()
        )
        if isinstance(entry, dict)
        else ""
    )
    if not git_context_key and isinstance(git_address, str) and git_address.strip():
        git_context_key = normalize_project_context_reference(git_address)
    git_context = await git_context_for_port(port)
    return {
        "port": port,
        "config_path": str(git_config_path),
        "git_address": git_address or "",
        "project_name": project_name,
        "git_context_key": git_context_key,
        "contexts": configured_git_contexts_from_config(config, port),
        "phone_contexts": [
            phone_contexts[phone]
            for phone in sorted(phone_contexts)
        ],
        **git_context,
    }


@app.post("/git-config", status_code=status.HTTP_201_CREATED)
async def post_git_config(request: Request) -> dict[str, Any]:
    port = request_port(request)
    if port is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot determine FastAPI port from request",
        )

    payload = await read_message(request)
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Expected JSON object",
        )

    git_address = payload.get("git_address")
    if not isinstance(git_address, str) or not git_address.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Git address is empty",
        )

    project_name = payload.get("project_name")
    if project_name is not None and not isinstance(project_name, str):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="project_name must be a string",
        )

    phone = payload.get("phone")
    if phone is not None and not isinstance(phone, str):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="phone must be a string",
        )

    git_context_key = payload.get("git_context_key")
    if git_context_key is not None and not isinstance(git_context_key, str):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="git_context_key must be a string",
        )

    entry = await save_git_address(
        port,
        git_address.strip(),
        project_name,
        phone,
        git_context_key,
    )
    git_context = await git_context_for_port(port)
    config = await read_git_config()
    phone_contexts = phone_git_contexts_from_config(config)
    return {
        "port": port,
        "config_path": str(git_config_path),
        "contexts": configured_git_contexts_from_config(config, port),
        "phone_contexts": [
            phone_contexts[phone_key]
            for phone_key in sorted(phone_contexts)
        ],
        **entry,
        **git_context,
    }


@app.delete("/git-config/phone/{phone}")
async def delete_git_config_phone(phone: str, request: Request) -> dict[str, Any]:
    port = request_port(request)
    removed = await delete_git_context_phone(phone)
    config = await read_git_config()
    phone_contexts = phone_git_contexts_from_config(config)
    return {
        "status": "deleted",
        "phone": normalize_phone_key(phone),
        "removed": removed,
        "contexts": configured_git_contexts_from_config(config, port),
        "phone_contexts": [
            phone_contexts[phone_key]
            for phone_key in sorted(phone_contexts)
        ],
    }


@app.get("/git-patch")
async def get_git_patch(
    git_address: str,
    to_commit: str,
    from_commit: str | None = None,
) -> dict[str, Any]:
    safe_to_commit = normalize_commit_ref(to_commit)
    safe_from_commit = normalize_commit_ref(from_commit) if from_commit else None
    patch_data = await asyncio.to_thread(
        resolve_git_patch,
        git_address,
        safe_to_commit,
        safe_from_commit,
    )
    return {
        "git_address": git_address,
        "from_commit": safe_from_commit,
        "to_commit": safe_to_commit,
        **patch_data,
    }


@app.get("/agents")
async def get_agents() -> dict[str, Any]:
    agents = await read_agents()
    async with specialization_lock:
        queues_data = await asyncio.to_thread(read_specialization_queues_file)
    return {
        "config_path": str(agents_path),
        "agents": [agent_with_presence(agent) for agent in agents],
        "pending_specializations": {
            phone: queue[0]
            for phone, queue in queues_data.items()
            if queue
        },
    }


@app.post("/agents", status_code=status.HTTP_201_CREATED)
async def post_agents(request: Request) -> dict[str, Any]:
    payload = await read_message(request)
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Expected JSON object",
        )

    agents = await save_agents(payload.get("agents", []))
    return {
        "config_path": str(agents_path),
        "agents": agents,
    }


@app.post("/attachments", status_code=status.HTTP_201_CREATED)
async def post_attachment(request: Request) -> dict[str, Any]:
    payload = await read_message(request)
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Expected JSON object",
        )

    return await asyncio.to_thread(save_attachment_file, payload)


@app.get("/screenshot-folders")
async def get_screenshot_folders(
    date_from: str | None = None,
    date_to: str | None = None,
    phone: str | None = None,
    git_context: str | None = None,
) -> dict[str, Any]:
    try:
        if date_from:
            date.fromisoformat(date_from)
        if date_to:
            date.fromisoformat(date_to)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Dates must use YYYY-MM-DD format",
        ) from exc

    phone_git_context = await git_context_for_phone(phone) if phone else None
    git_context_key = (
        git_context_key_from_metadata(phone_git_context)
        if phone_git_context
        else normalize_history_git_context_filter(git_context)
    )

    async with screenshot_folders_lock:
        return await asyncio.to_thread(
            list_screenshot_folders_file,
            date_from,
            date_to,
            git_context_key,
        )


@app.post("/screenshot-folders", status_code=status.HTTP_201_CREATED)
async def post_screenshot_folder(phone: str | None = None) -> dict[str, Any]:
    git_context = await git_context_for_phone(phone) if phone else None
    async with screenshot_folders_lock:
        return await asyncio.to_thread(create_screenshot_folder_file, git_context)


@app.post("/screenshot-folders/{folder_id}/files", status_code=status.HTTP_201_CREATED)
async def post_screenshot_file(folder_id: str, request: Request) -> dict[str, Any]:
    payload = await read_message(request)
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Expected JSON object",
        )

    async with screenshot_folders_lock:
        return await asyncio.to_thread(save_screenshot_file_to_folder, folder_id, payload)


@app.delete("/screenshot-folders/{folder_id}/files/{filename}")
async def delete_screenshot_file(folder_id: str, filename: str) -> dict[str, Any]:
    async with screenshot_folders_lock:
        return await asyncio.to_thread(delete_screenshot_file_from_folder, folder_id, filename)


@app.get("/screenshot-folders/{folder_id}/files/{filename}/preview")
async def preview_screenshot_file(folder_id: str, filename: str) -> FileResponse:
    async with screenshot_folders_lock:
        file_path = await asyncio.to_thread(resolve_screenshot_file_path, folder_id, filename)
    return FileResponse(file_path, filename=file_path.name, content_disposition_type="inline")


@app.post("/screenshot-folders/{folder_id}/files/{filename}/copy", status_code=status.HTTP_201_CREATED)
async def copy_screenshot_file(folder_id: str, filename: str, request: Request) -> dict[str, Any]:
    payload = await read_message(request)
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Expected JSON object",
        )

    target_folder_id = str(payload.get("target_folder_id", "")).strip()
    if not target_folder_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="target_folder_id is required",
        )

    async with screenshot_folders_lock:
        return await asyncio.to_thread(
            copy_screenshot_file_between_folders,
            folder_id,
            filename,
            target_folder_id,
        )


@app.get("/evidence-folders")
async def get_evidence_folders(
    date_from: str | None = None,
    date_to: str | None = None,
    phone: str | None = None,
    git_context: str | None = None,
) -> dict[str, Any]:
    try:
        if date_from:
            date.fromisoformat(date_from)
        if date_to:
            date.fromisoformat(date_to)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Dates must use YYYY-MM-DD format",
        ) from exc

    phone_git_context = await git_context_for_phone(phone) if phone else None
    git_context_key = (
        git_context_key_from_metadata(phone_git_context)
        if phone_git_context
        else normalize_history_git_context_filter(git_context)
    )

    async with evidence_folders_lock:
        return await asyncio.to_thread(
            list_evidence_folders_file,
            date_from,
            date_to,
            git_context_key,
        )


@app.post("/evidence-folders", status_code=status.HTTP_201_CREATED)
async def post_evidence_folder(phone: str | None = None) -> dict[str, Any]:
    git_context = await git_context_for_phone(phone) if phone else None
    async with evidence_folders_lock:
        return await asyncio.to_thread(create_evidence_folder_file, git_context)


@app.post("/evidence-folders/{folder_id}/files", status_code=status.HTTP_201_CREATED)
async def post_evidence_file(folder_id: str, request: Request) -> dict[str, Any]:
    payload = await read_message(request)
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Expected JSON object",
        )

    async with evidence_folders_lock:
        return await asyncio.to_thread(save_evidence_file_to_folder, folder_id, payload)


@app.delete("/evidence-folders/{folder_id}/files/{filename}")
async def delete_evidence_file(folder_id: str, filename: str) -> dict[str, Any]:
    async with evidence_folders_lock:
        return await asyncio.to_thread(delete_evidence_file_from_folder, folder_id, filename)


@app.get("/evidence-folders/{folder_id}/files/{filename}/preview")
async def preview_evidence_file(folder_id: str, filename: str) -> FileResponse:
    async with evidence_folders_lock:
        file_path = await asyncio.to_thread(resolve_evidence_file_path, folder_id, filename)
    return FileResponse(file_path, filename=file_path.name, content_disposition_type="inline")


@app.post("/evidence-folders/{folder_id}/files/{filename}/copy", status_code=status.HTTP_201_CREATED)
async def copy_evidence_file(folder_id: str, filename: str, request: Request) -> dict[str, Any]:
    payload = await read_message(request)
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Expected JSON object",
        )

    target_folder_id = str(payload.get("target_folder_id", "")).strip()
    if not target_folder_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="target_folder_id is required",
        )

    async with evidence_folders_lock:
        return await asyncio.to_thread(
            copy_evidence_file_between_folders,
            folder_id,
            filename,
            target_folder_id,
        )


def create_empty_agent_transaction(base_url: str) -> dict[str, Any]:
    with agents_file_lock():
        agents = read_agents_file()
        phone = unique_random_phone_for_agents(agents)
        agent_id = f"agent-empty-{phone}"
        name = f"Empty Agent {phone}"
        specialization_endpoint = f"{base_url}/agents/poll/{phone}"
        created_at = datetime.now(timezone.utc)
        expires_at = created_at + timedelta(hours=2)
        parameters = {
            "agent_phone": phone,
            "poll_interval": EMPTY_AGENT_POLL_INTERVAL,
            "wait_window": EMPTY_AGENT_WAIT_WINDOW,
            "specialization_endpoint": specialization_endpoint,
            "created_at": created_at.isoformat(),
            "expires_at": expires_at.isoformat(),
        }
        prompt_text = profile_template_text(EMPTY_AGENT_PROMPT_TEMPLATE, parameters)
        new_agent = {
            "id": agent_id,
            "name": name,
            "phone": phone,
            "profile": EMPTY_AGENT_PROMPT_TEMPLATE,
            "parameters": parameters,
            "status": "empty",
            "template_source": "system:empty_agent",
        }
        agents.append(new_agent)
        write_agents_file_unlocked(normalize_agents(agents))

    return {
        "id": agent_id,
        "phone": phone,
        "name": name,
        "prompt_text": prompt_text,
    }


@app.post("/agents/create-empty", status_code=status.HTTP_201_CREATED)
async def create_empty_agent(request: Request) -> dict[str, Any]:
    base_url = str(request.base_url).rstrip("/")
    async with agents_lock:
        return await asyncio.to_thread(create_empty_agent_transaction, base_url)


@app.get("/agents/poll/{phone}")
async def poll_specialization(phone: str) -> dict[str, Any]:
    phone = phone.strip()
    agents = await read_agents()
    target_agent = next((agent for agent in agents if str(agent.get("phone", "")).strip() == phone), None)
    if target_agent is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Agent phone not found",
        )
    if agent_status_value(target_agent) != "empty":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Agent is not waiting for specialization",
        )
    if empty_agent_is_expired(target_agent):
        raise HTTPException(
            status_code=status.HTTP_410_GONE,
            detail="Empty agent wait window expired",
        )

    async with specialization_lock:
        queues_data = await asyncio.to_thread(read_specialization_queues_file)
        if phone not in queues_data or not queues_data[phone]:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No specialization assigned yet",
            )
        spec = queues_data[phone][0]
    return spec


@app.post("/agents/specialize/{phone}")
async def specialize_agent(phone: str, request: Request) -> dict[str, Any]:
    phone = phone.strip()
    agents = await read_agents()
    target_agent = next((agent for agent in agents if str(agent.get("phone", "")).strip() == phone), None)
    if target_agent is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Empty agent phone not found",
        )
    if agent_status_value(target_agent) != "empty":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Agent is not empty or is already specialized",
        )
    if empty_agent_is_expired(target_agent):
        raise HTTPException(
            status_code=status.HTTP_410_GONE,
            detail="Empty agent wait window expired",
        )

    payload = await read_message(request)
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Expected JSON object",
        )

    name = str(payload.get("name", "")).strip()
    profile = str(payload.get("profile", "")).strip()
    parameters = normalize_agent_parameters(payload.get("parameters"))

    if not name or not profile:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Fields 'name' and 'profile' are required",
        )

    spec = {
        "name": name,
        "profile": profile,
        "parameters": parameters,
        "status": "active",
    }

    async with specialization_lock:
        queues_data = await asyncio.to_thread(read_specialization_queues_file)
        queues_data[phone] = [spec]
        specialization_queues.clear()
        specialization_queues.update(queues_data)
        await asyncio.to_thread(write_specialization_queues_file, queues_data)

    return {"status": "enqueued", "phone": phone, "spec": spec}


@app.post("/agents/poll/{phone}/confirm")
async def confirm_specialization(phone: str, request: Request) -> dict[str, Any]:
    phone = phone.strip()
    payload = await read_message(request)
    if not isinstance(payload, dict) or payload.get("status") != "ACCEPTED":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Expected JSON object with status 'ACCEPTED'",
        )

    agents = await read_agents()
    target_index = next(
        (index for index, agent in enumerate(agents) if str(agent.get("phone", "")).strip() == phone),
        None,
    )
    if target_index is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Agent phone not found",
        )
    if agent_status_value(agents[target_index]) != "empty":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Agent is not waiting for specialization",
        )

    async with specialization_lock:
        queues_data = await asyncio.to_thread(read_specialization_queues_file)
        if phone not in queues_data or not queues_data[phone]:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No specialization found to confirm",
            )
        spec = queues_data[phone].pop(0)
        if queues_data[phone]:
            queues_data[phone] = queues_data[phone]
        else:
            queues_data.pop(phone, None)
        specialization_queues.clear()
        specialization_queues.update(queues_data)
        await asyncio.to_thread(write_specialization_queues_file, queues_data)

    agents[target_index]["name"] = spec["name"]
    agents[target_index]["profile"] = spec["profile"]
    agents[target_index]["parameters"] = {
        **normalize_agent_parameters(agents[target_index].get("parameters")),
        **normalize_agent_parameters(spec.get("parameters")),
    }
    agents[target_index]["status"] = "active"
    await save_agents(agents)

    return {"status": "confirmed", "phone": phone, "updated_agent": agents[target_index]}


@app.post("/agents/clone-preview")
async def post_agent_clone_preview(request: Request) -> dict[str, Any]:
    payload = await read_message(request)
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Expected JSON object",
        )

    return await asyncio.to_thread(build_agent_clone_preview, payload)


@app.post("/gemini/models")
async def post_gemini_models(request: Request) -> dict[str, Any]:
    payload = await read_message(request)
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Expected JSON object",
        )

    return await asyncio.to_thread(build_gemini_models_response, payload)


@app.get("/email-routes")
async def get_email_routes() -> dict[str, Any]:
    routes = await read_email_routes()
    return {
        "config_path": str(email_routes_path),
        "routes": routes,
    }


@app.post("/email-routes", status_code=status.HTTP_201_CREATED)
async def post_email_routes(request: Request) -> dict[str, Any]:
    payload = await read_message(request)
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Expected JSON object",
        )

    routes = await save_email_routes(payload.get("routes", []))
    return {
        "config_path": str(email_routes_path),
        "routes": routes,
    }


@app.post("/ui/send", status_code=status.HTTP_201_CREATED)
async def ui_send(request: Request) -> dict[str, Any]:
    payload = await read_message(request)
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Expected JSON object",
        )

    queue_name = payload.get("queue")
    if queue_name not in queues:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Unknown queue",
        )

    message = payload.get("message")
    if not isinstance(message, str) or not message.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Message is empty",
        )

    phone = payload.get("phone")
    if not isinstance(phone, str) or not phone.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="phone is required for queue Git context",
        )
    git_context = await git_context_for_phone(phone)
    message = message.strip()
    metadata = {
        "submitted_via": "ui",
        "sender": payload.get("sender"),
        "receiver": payload.get("receiver"),
        "status": payload.get("status"),
        "queue_phone": normalize_phone_key(phone),
    }
    schedule = payload.get("schedule")
    if schedule is not None and not isinstance(schedule, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="schedule must be an object.",
        )
    return await schedule_message(
        queue_name,
        message,
        metadata,
        request_port(request),
        schedule,
        git_context,
    )


async def read_phone_channel_payload(request: Request) -> tuple[Any, dict[str, Any]]:
    payload = await read_message(request)
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Expected JSON object",
        )

    message = payload.get("message")
    if not isinstance(message, str) or not message.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Message is empty",
        )

    metadata = {
        "submitted_via": payload.get("submitted_via") or "phone_channel",
        "sender": payload.get("sender"),
        "receiver": payload.get("receiver"),
        "from_phone": payload.get("from_phone"),
        "to_phone": payload.get("to_phone"),
        "status": payload.get("status"),
    }
    return message.strip(), metadata


@app.post("/worker/all/{conversation_phone}", status_code=status.HTTP_201_CREATED)
async def post_worker_all(
    conversation_phone: str,
    request: Request,
) -> dict[str, Any]:
    message, metadata = await read_phone_channel_payload(request)
    git_context = await git_context_for_phone_if_mapped(conversation_phone)
    return await enqueue_phone_channel(
        "worker-all",
        conversation_phone,
        message,
        metadata,
        request_port(request),
        git_context,
    )


@app.get("/worker/all/{conversation_phone}")
async def get_worker_all(
    conversation_phone: str,
    request: Request,
    to_phone: str,
) -> dict[str, Any]:
    git_context = await git_context_for_phone_if_mapped(conversation_phone)
    return await dequeue_phone_channel(
        "worker-all",
        conversation_phone,
        to_phone,
        request_port(request),
        git_context,
    )


@app.post("/tester/all/{conversation_phone}", status_code=status.HTTP_201_CREATED)
async def post_tester_all(
    conversation_phone: str,
    request: Request,
) -> dict[str, Any]:
    message, metadata = await read_phone_channel_payload(request)
    git_context = await git_context_for_phone_if_mapped(conversation_phone)
    return await enqueue_phone_channel(
        "tester-all",
        conversation_phone,
        message,
        metadata,
        request_port(request),
        git_context,
    )


@app.get("/tester/all/{conversation_phone}")
async def get_tester_all(
    conversation_phone: str,
    request: Request,
    to_phone: str,
) -> dict[str, Any]:
    git_context = await git_context_for_phone_if_mapped(conversation_phone)
    return await dequeue_phone_channel(
        "tester-all",
        conversation_phone,
        to_phone,
        request_port(request),
        git_context,
    )


@app.post("/consultant/all/{conversation_phone}", status_code=status.HTTP_201_CREATED)
async def post_consultant_all(
    conversation_phone: str,
    request: Request,
) -> dict[str, Any]:
    message, metadata = await read_phone_channel_payload(request)
    git_context = await git_context_for_phone_if_mapped(conversation_phone)
    return await enqueue_phone_channel(
        "consultant-all",
        conversation_phone,
        message,
        metadata,
        request_port(request),
        git_context,
    )


@app.get("/consultant/all/{conversation_phone}")
async def get_consultant_all(
    conversation_phone: str,
    request: Request,
    to_phone: str,
) -> dict[str, Any]:
    git_context = await git_context_for_phone_if_mapped(conversation_phone)
    return await dequeue_phone_channel(
        "consultant-all",
        conversation_phone,
        to_phone,
        request_port(request),
        git_context,
    )


@app.post("/work/{phone}", status_code=status.HTTP_201_CREATED)
async def post_work_for_phone(phone: str, request: Request) -> dict[str, Any]:
    message = await read_message(request)
    git_context = await git_context_for_phone(phone)
    return await enqueue(
        "work",
        message,
        {"queue_phone": normalize_phone_key(phone)},
        port=request_port(request),
        git_context=git_context,
    )


@app.get("/work/{phone}")
async def get_work_for_phone(phone: str, request: Request) -> dict[str, Any]:
    git_context = await git_context_for_phone(phone)
    return await dequeue("work", request_port(request), git_context)


@app.post("/test/{phone}", status_code=status.HTTP_201_CREATED)
async def post_test_for_phone(phone: str, request: Request) -> dict[str, Any]:
    message = await read_message(request)
    git_context = await git_context_for_phone(phone)
    return await enqueue(
        "test",
        message,
        {"queue_phone": normalize_phone_key(phone)},
        port=request_port(request),
        git_context=git_context,
    )


@app.get("/test/{phone}")
async def get_test_for_phone(phone: str, request: Request) -> dict[str, Any]:
    git_context = await git_context_for_phone(phone)
    return await dequeue("test", request_port(request), git_context)


@app.post("/work-design/{phone}", status_code=status.HTTP_201_CREATED)
async def post_work_design_for_phone(phone: str, request: Request) -> dict[str, Any]:
    message = await read_message(request)
    git_context = await git_context_for_phone(phone)
    return await enqueue(
        "work-design",
        message,
        {"queue_phone": normalize_phone_key(phone)},
        port=request_port(request),
        git_context=git_context,
    )


@app.get("/work-design/{phone}")
async def get_work_design_for_phone(phone: str, request: Request) -> dict[str, Any]:
    git_context = await git_context_for_phone(phone)
    return await dequeue("work-design", request_port(request), git_context)


@app.post("/test-design/{phone}", status_code=status.HTTP_201_CREATED)
async def post_test_design_for_phone(phone: str, request: Request) -> dict[str, Any]:
    message = await read_message(request)
    git_context = await git_context_for_phone(phone)
    return await enqueue(
        "test-design",
        message,
        {"queue_phone": normalize_phone_key(phone)},
        port=request_port(request),
        git_context=git_context,
    )


@app.get("/test-design/{phone}")
async def get_test_design_for_phone(phone: str, request: Request) -> dict[str, Any]:
    git_context = await git_context_for_phone(phone)
    return await dequeue("test-design", request_port(request), git_context)


def legacy_queue_route_error(route: str) -> None:
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=f"Legacy queue URL {route} has no phone-bound Git context. Use {route}/{{phone}}.",
    )


@app.post("/work", status_code=status.HTTP_400_BAD_REQUEST)
async def post_work_legacy() -> None:
    legacy_queue_route_error("/work")


@app.get("/work", status_code=status.HTTP_400_BAD_REQUEST)
async def get_work_legacy() -> None:
    legacy_queue_route_error("/work")


@app.post("/test", status_code=status.HTTP_400_BAD_REQUEST)
async def post_test_legacy() -> None:
    legacy_queue_route_error("/test")


@app.get("/test", status_code=status.HTTP_400_BAD_REQUEST)
async def get_test_legacy() -> None:
    legacy_queue_route_error("/test")


@app.post("/work-design", status_code=status.HTTP_400_BAD_REQUEST)
async def post_work_design_legacy() -> None:
    legacy_queue_route_error("/work-design")


@app.get("/work-design", status_code=status.HTTP_400_BAD_REQUEST)
async def get_work_design_legacy() -> None:
    legacy_queue_route_error("/work-design")


@app.post("/test-design", status_code=status.HTTP_400_BAD_REQUEST)
async def post_test_design_legacy() -> None:
    legacy_queue_route_error("/test-design")


@app.get("/test-design", status_code=status.HTTP_400_BAD_REQUEST)
async def get_test_design_legacy() -> None:
    legacy_queue_route_error("/test-design")


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8025)
