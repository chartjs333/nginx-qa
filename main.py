from __future__ import annotations

import asyncio
import base64
import binascii
import html
import json
import os
import random
import re
import shutil
import subprocess
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

import uvicorn
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import FileResponse, HTMLResponse


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
scheduled_tasks: dict[str, dict[str, Any]] = {}
scheduled_timer_tasks: dict[str, asyncio.Task[Any]] = {}
base_dir = Path(__file__).resolve().parent
history_path = base_dir / "conversation_log.jsonl"
git_config_path = base_dir / "port_git_map.json"
email_routes_path = base_dir / "email_routes.json"
agents_path = base_dir / "agents.json"
specializations_path = base_dir / "specializations.json"
attachments_path = base_dir / "attachments"
screenshot_folders_path = base_dir / "screenshot_folders"
evidence_folders_path = base_dir / "evidence_folders"
SCREENSHOT_FOLDER_PREFIX = "screenshot_folder_"
EVIDENCE_FOLDER_PREFIX = "evidence_folder_"
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
DEFAULT_AGENT_PHONES: dict[str, str] = {
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


def write_history_line(record: dict[str, Any]) -> None:
    history_path.parent.mkdir(parents=True, exist_ok=True)
    with history_path.open("a", encoding="utf-8") as file:
        file.write(json.dumps(record, ensure_ascii=False) + "\n")


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


def write_git_config_file(data: dict[str, Any]) -> None:
    git_config_path.parent.mkdir(parents=True, exist_ok=True)
    with git_config_path.open("w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)
        file.write("\n")


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
    agent = {
        "id": agent_id,
        "name": name,
        "phone": phone,
        "profile": profile,
        "parameters": normalize_agent_parameters(raw_agent.get("parameters")),
        "template_source": template_source,
    }
    agent_status = normalize_agent_status(raw_agent)
    if agent_status:
        agent["status"] = agent_status
    return agent


def normalize_agents(raw_agents: Any) -> list[dict[str, Any]]:
    if not isinstance(raw_agents, list):
        return []

    agents: list[dict[str, Any]] = []
    seen_names: set[str] = set()
    for raw_agent in raw_agents:
        agent = normalize_agent(raw_agent)
        if agent is None:
            continue

        name_key = agent["name"].casefold()
        if name_key in seen_names:
            continue

        seen_names.add(name_key)
        agents.append(agent)

    return agents


def read_agents_file() -> list[dict[str, Any]]:
    if not agents_path.exists():
        return default_agents()

    with agents_path.open("r", encoding="utf-8") as file:
        try:
            data = json.load(file)
        except json.JSONDecodeError:
            return default_agents()

    if isinstance(data, dict):
        raw_agents = data.get("agents", [])
    else:
        raw_agents = data

    if not isinstance(raw_agents, list):
        return default_agents()

    return normalize_agents(raw_agents)


def write_agents_file(agents: list[dict[str, Any]]) -> None:
    agents_path.parent.mkdir(parents=True, exist_ok=True)
    with agents_path.open("w", encoding="utf-8") as file:
        json.dump({"agents": agents}, file, ensure_ascii=False, indent=2)
        file.write("\n")


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
        if file_path.is_file()
    ]
    return {
        "id": folder_path.name,
        "number": folder_number,
        "path": str(folder_path.resolve()),
        "created_at": datetime.fromtimestamp(stat.st_ctime, timezone.utc).isoformat(),
        "file_count": len(files),
        "files": files,
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
) -> dict[str, Any]:
    root = ensure_screenshot_folders_root()
    folders = [
        path
        for path in root.iterdir()
        if (
            path.is_dir()
            and screenshot_folder_number(path.name) is not None
            and screenshot_folder_in_date_range(path, date_from, date_to)
        )
    ]
    folders.sort(key=lambda path: screenshot_folder_number(path.name) or 0)
    return {
        "root_path": str(root),
        "date_from": date_from,
        "date_to": date_to,
        "folders": [screenshot_folder_snapshot(path) for path in folders],
    }


def create_screenshot_folder_file() -> dict[str, Any]:
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
            break
        next_number += 1

    return {
        "root_path": str(root),
        "folder": screenshot_folder_snapshot(folder_path),
        "folders": list_screenshot_folders_file()["folders"],
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
        if file_path.is_file()
    ]
    return {
        "id": folder_path.name,
        "number": folder_number,
        "path": str(folder_path.resolve()),
        "created_at": datetime.fromtimestamp(stat.st_ctime, timezone.utc).isoformat(),
        "file_count": len(files),
        "files": files,
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
) -> dict[str, Any]:
    root = ensure_evidence_folders_root()
    folders = [
        path
        for path in root.iterdir()
        if (
            path.is_dir()
            and evidence_folder_number(path.name) is not None
            and evidence_folder_in_date_range(path, date_from, date_to)
        )
    ]
    folders.sort(key=lambda path: evidence_folder_number(path.name) or 0)
    return {
        "root_path": str(root),
        "date_from": date_from,
        "date_to": date_to,
        "folders": [evidence_folder_snapshot(path) for path in folders],
    }


def create_evidence_folder_file() -> dict[str, Any]:
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
            break
        next_number += 1

    return {
        "root_path": str(root),
        "folder": evidence_folder_snapshot(folder_path),
        "folders": list_evidence_folders_file()["folders"],
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


async def save_agents(raw_agents: Any) -> list[dict[str, Any]]:
    if not isinstance(raw_agents, list):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Expected agents list",
        )

    agents = normalize_agents(raw_agents)
    async with agents_lock:
        await asyncio.to_thread(write_agents_file, agents)

    return agents


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


async def save_git_address(port: int, git_address: str) -> dict[str, Any]:
    async with git_config_lock:
        config = await asyncio.to_thread(read_git_config_file)
        config[str(port)] = {
            "git_address": git_address,
            "updated_at": utc_now(),
        }
        await asyncio.to_thread(write_git_config_file, config)
        return config[str(port)]


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
        if path.endswith(".git"):
            path = path[:-4]
        return f"{parsed.netloc.lower()}/{path.lower()}"

    if address.endswith(".git"):
        address = address[:-4]
    return address


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

    git_context = await asyncio.to_thread(resolve_git_reference, git_address)
    return {
        "fastapi_port": port,
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
) -> dict[str, Any]:
    queue_meta = queue_definition(queue_name)
    submitted_metadata = {
        key: value for key, value in (metadata or {}).items() if value is not None
    }
    enriched_metadata = {
        "route": queue_meta["route"],
        "context": queue_meta["context"],
        "context_label": queue_meta["context_label"],
        "sender": submitted_metadata.get("sender") or queue_meta["default_sender"],
        "receiver": submitted_metadata.get("receiver") or queue_meta["default_receiver"],
        "direction": queue_meta["label"],
        **submitted_metadata,
        **(await git_context_for_port(port)),
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


def summarize_message(message: Any) -> str:
    if isinstance(message, str):
        text = " ".join(message.split())
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
            for key in ("sender", "receiver", "status", "submitted_via")
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
    )


async def release_pass_scheduled_tasks(
    trigger_record: dict[str, Any],
    port: int | None = None,
) -> list[dict[str, Any]]:
    async with scheduled_tasks_lock:
        task_ids = [
            task_id
            for task_id, task in scheduled_tasks.items()
            if task.get("schedule_mode") == "pass"
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
) -> dict[str, Any]:
    schedule = schedule if isinstance(schedule, dict) else {}
    mode = str(schedule.get("mode") or "now").strip().lower()
    if mode in {"", "now", "immediate"}:
        return await enqueue(queue_name, message, metadata, port)

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
        "metadata": {key: value for key, value in (metadata or {}).items() if value is not None},
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
    )
    return {
        "status": "scheduled",
        "queue": queue_name,
        "id": task_id,
        "task": scheduled_task_snapshot(task),
    }


async def list_scheduled_tasks() -> list[dict[str, Any]]:
    async with scheduled_tasks_lock:
        tasks = [scheduled_task_snapshot(task) for task in scheduled_tasks.values()]

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


def read_history_file(
    limit: int,
    date_from: str | None = None,
    date_to: str | None = None,
) -> list[dict[str, Any]]:
    if not history_path.exists():
        return []

    with history_path.open("r", encoding="utf-8") as file:
        lines = file.readlines()

    start_at = parse_date_boundary(date_from or local_today_iso(), end=False)
    end_before = parse_date_boundary(date_to, end=True) if date_to else None
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
            records.append(record)

    return records[-limit:]


async def read_history(
    limit: int = 200,
    date_from: str | None = None,
    date_to: str | None = None,
) -> list[dict[str, Any]]:
    safe_limit = max(1, min(limit, 10000))
    async with history_lock:
        return await asyncio.to_thread(read_history_file, safe_limit, date_from, date_to)


def delete_removed_backend_history_record_file(record_id: str) -> dict[str, Any]:
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
            if record.get("event") == "removed_from_backend_queue":
                deleted = True
                continue
            found_protected = True

        kept_lines.append(line if line.endswith("\n") else f"{line}\n")

    if deleted:
        temp_path = history_path.with_suffix(".jsonl.tmp")
        with temp_path.open("w", encoding="utf-8") as file:
            file.writelines(kept_lines)
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
      .agent-row-head {
        grid-template-columns: 1fr;
      }
      .agent-toolbar-actions {
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
        <h2>Git context</h2>
        <label for="gitAddress">Git address для текущего порта</label>
        <input id="gitAddress" placeholder="D:\\nginx или https://github.com/org/repo.git">
        <div class="actions">
          <button class="primary" id="saveGitButton" type="button">Сохранить Git address</button>
          <button class="secondary" id="checkGitButton" type="button">Проверить commit</button>
        </div>
        <div class="status" id="gitStatus">Файл привязки: __GIT_CONFIG_PATH__</div>
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
        purpose: "Связывает текущий порт приложения с репозиторием, где лежит код проекта.",
        logic: "После сохранения система может показывать текущий commit, чтобы участники понимали, какую версию проверяют."
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
      "field:gitAddress": {
        title: "Git address",
        purpose: "Адрес папки или репозитория, где лежит код проекта.",
        logic: "По этому адресу система пытается определить текущий commit, чтобы связать проверку с версией кода."
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
        title: "Сохранить Git address",
        purpose: "Сохраняет адрес репозитория или папки проекта.",
        logic: "После сохранения система сможет определять commit для текущего порта."
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
        purpose: "Копирует профиль агента уже с подставленными значениями.",
        logic: "Такой текст можно передать агенту или проверить вручную."
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
    const historyEl = document.getElementById("history");
    const historyCountEl = document.getElementById("historyCount");
    const historyFromEl = document.getElementById("historyFrom");
    const historyToEl = document.getElementById("historyTo");
    const historyStatusEl = document.getElementById("historyStatus");
    const queueStatsEl = document.getElementById("queueStats");
    const scheduledTasksEl = document.getElementById("scheduledTasks");
    const scheduledTasksCountEl = document.getElementById("scheduledTasksCount");
    const scheduledTasksStatusEl = document.getElementById("scheduledTasksStatus");
    const gitAddressEl = document.getElementById("gitAddress");
    const gitStatusEl = document.getElementById("gitStatus");
    const emailRoutesEl = document.getElementById("emailRoutes");
    const emailRoutesStatusEl = document.getElementById("emailRoutesStatus");
    const emailSenderOptionsEl = document.getElementById("emailSenderOptions");
    const agentSelectorEl = document.getElementById("agentSelector");
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
    let pendingSpecializations = {};
    let selectedAgentId = "";
    let cloneCandidateAgent = null;
    let messageCursor = {start: 0, end: 0};
    let helpRenderScheduled = false;

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
      if (typeof message === "string") {
        return message;
      }
      return JSON.stringify(message, null, 2);
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
      return `<li>${escapeHtml(agent.name || "Без имени")}${escapeHtml(phone)}${escapeHtml(status)}${escapeHtml(extra)}</li>`;
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
        template_source: existingAgent.template_source || ""
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
    }

    function renderAgentSelector(preferredId = selectedAgentId) {
      if (!agents.length) {
        selectedAgentId = "";
        agentSelectorEl.innerHTML = `<option value="">Нет агентов</option>`;
        agentSelectorEl.value = "";
        return;
      }

      if (!agents.some((agent) => agent.id === preferredId)) {
        preferredId = agents[0].id;
      }
      selectedAgentId = preferredId;
      agentSelectorEl.innerHTML = agents.map((agent, index) => {
        const label = agent.name || `Agent ${index + 1}`;
        const selected = agent.id === selectedAgentId ? " selected" : "";
        return `<option value="${escapeHtml(agent.id)}"${selected}>${escapeHtml(label)}</option>`;
      }).join("");
      agentSelectorEl.value = selectedAgentId;
    }

    function renderSelectedAgent() {
      const agent = agents.find((item) => item.id === selectedAgentId);
      agentsEl.innerHTML = agent
        ? agentRow(agent)
        : `<div class="subtle">Список агентов пока пуст.</div>`;
      updateAgentPreview();
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

    function renderAgents(agentList, preferredId = selectedAgentId) {
      agents = (agentList || []).map((agent) => ({
        id: agent.id || agentId(),
        name: agent.name || "",
        phone: agent.phone || "",
        profile: agent.profile || "",
        parameters: agent.parameters || {},
        status: agent.status || (agent.parameters && agent.parameters.status) || "",
        template_source: agent.template_source || ""
      }));
      renderAgentSelector(preferredId);
      renderSelectedAgent();
      updateSpecializationPanel();
      renderAgentStatusSummary();
    }

    function collectAgents() {
      captureCurrentAgent();
      const cleanAgents = [];
      const seen = new Set();
      agents.forEach((agent) => {
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
          template_source: agent.template_source || ""
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
      const nextAgent = agent || {
        id: agentId(),
        name: nextAgentName(),
        phone: "",
        profile: "",
        parameters: {},
        template_source: ""
      };
      agents.push(nextAgent);
      renderAgents(agents, nextAgent.id);
      syncActorsFromAgents(agents);
      setAgentsStatus("");
    }

    function addSelectedAgentParam() {
      const paramsEl = agentsEl.querySelector(".agent-params");
      if (!paramsEl) {
        return;
      }
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
      const currentIndex = agents.findIndex((agent) => agent.id === selectedAgentId);
      agents = agents.filter((agent) => agent.id !== selectedAgentId);
      const nextAgent = agents[Math.min(currentIndex, agents.length - 1)];
      renderAgents(agents, nextAgent ? nextAgent.id : "");
      syncActorsFromAgents(agents);
      setAgentsStatus("Агент удален из списка. Нажмите «Сохранить агентов», чтобы записать изменение.", "ok");
    }

    async function copySelectedAgentProfile() {
      captureCurrentAgent();
      const agent = agents.find((item) => item.id === selectedAgentId);
      if (!agent) {
        setAgentsStatus("Агент не выбран.", "error");
        return;
      }
      const result = profileWithParameters(agent.profile, agent.parameters || {});
      if (navigator.clipboard && window.isSecureContext) {
        await navigator.clipboard.writeText(result.text);
      } else {
        fallbackCopyText(result.text);
      }
      if (result.missing.length) {
        setAgentsStatus(`Скопировано, но не заполнены placeholders: ${result.missing.join(", ")}.`, "error");
      } else {
        setAgentsStatus("Профиль скопирован с подставленными параметрами.", "ok");
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

    async function refreshAgents() {
      const response = await fetch("/agents");
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.detail || "Ошибка загрузки агентов.");
      }
      pendingSpecializations = data.pending_specializations || {};
      renderAgents(data.agents || [], selectedAgentId);
      syncActorsFromAgents(data.agents || []);
      setAgentsStatus(`Загружено агентов: ${(data.agents || []).length}. Файл: ${data.config_path}`);
    }

    async function saveAgents() {
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
      setAgentsStatus(`Сохранено агентов: ${(data.agents || []).length}.`, "ok");
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
      const lines = String(message)
        .split("\\n")
        .map((line) => line.trim())
        .filter(Boolean)
        .filter((line) => !line.startsWith("TO:") && !line.startsWith("FROM:") && !line.startsWith("STATUS:"));
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

    async function sendMessage() {
      const message = messageEl.value.trim();
      if (!message) {
        setStatus("Введите текст сообщения.", "error");
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
      const response = await fetch("/screenshot-folders", {method: "POST"});
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
      const response = await fetch("/evidence-folders", {method: "POST"});
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
      const response = await fetch(endpoint);
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
      const message = phoneMessageEl.value.trim();
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
      const response = await fetch("/queues");
      const data = await response.json();
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
      const response = await fetch("/scheduled-tasks");
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
      const response = await fetch(`/queues/${encodeURIComponent(queueName)}/${encodeURIComponent(itemId)}`, {
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
      if (document.activeElement !== gitAddressEl) {
        gitAddressEl.value = data.git_address || "";
      }
      commitValueEl.textContent = data.git_commit_short || "не задан";
      if (data.git_commit_short) {
        setGitStatus(`Порт ${data.port}: commit ${data.git_commit_short}`, "ok");
      } else if (data.git_error) {
        setGitStatus(`Порт ${data.port}: ${data.git_error}`, "error");
      } else {
        setGitStatus(`Порт ${data.port}: Git address не задан. Файл привязки: ${data.config_path}`);
      }
    }

    async function saveGitConfig() {
      const gitAddress = gitAddressEl.value.trim();
      if (!gitAddress) {
        setGitStatus("Введите Git address.", "error");
        return;
      }
      setGitStatus("Сохраняю...");
      const response = await fetch("/git-config", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({git_address: gitAddress})
      });
      const data = await response.json();
      if (!response.ok) {
        setGitStatus(data.detail || "Ошибка сохранения.", "error");
        return;
      }
      commitValueEl.textContent = data.git_commit_short || "не задан";
      if (data.git_commit_short) {
        setGitStatus(`Сохранено для порта ${data.port}. Commit: ${data.git_commit_short}`, "ok");
      } else if (data.git_error) {
        setGitStatus(`Сохранено для порта ${data.port}, но commit не получен: ${data.git_error}`, "error");
      } else {
        setGitStatus(`Сохранено для порта ${data.port}.`, "ok");
      }
    }

    function historyParams(limit) {
      const params = new URLSearchParams();
      params.set("limit", String(limit));
      params.set("date_from", historyFromEl.value || todayInputValue());
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
      const commit = meta.git_commit_short || "no-commit";
      const actor = [meta.sender, meta.receiver].filter(Boolean).join(" -> ") || meta.direction || record.route || record.queue;
      const message = formatMessage(record.message);
      return `[${timestamp}] [Commit: ${commit}] [${actor}] [${record.event}]
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
        short: String(meta.git_commit_short || (full ? full.slice(0, 12) : "")).trim(),
        gitAddress: String(meta.git_address || "").trim()
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

    function patchRequestKey(previousCommit, currentCommit) {
      const gitAddress = currentCommit.gitAddress || previousCommit.gitAddress || "";
      return [
        gitAddress,
        previousCommit.full.toLowerCase(),
        currentCommit.full.toLowerCase()
      ].join("|");
    }

    function patchBlockHeader(previousCommit, currentCommit, patchData = null) {
      const lines = [
        "[PATCH BETWEEN COMMITS]",
        `From: ${commitLabel(previousCommit)}`,
        `To: ${commitLabel(currentCommit)}`
      ];
      const gitAddress = currentCommit.gitAddress || previousCommit.gitAddress;
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

    function patchUnavailableBlock(previousCommit, currentCommit, reason) {
      return `${patchBlockHeader(previousCommit, currentCommit)}
Status: unavailable
Reason: ${reason || "Patch could not be loaded"}
[END PATCH]`;
    }

    async function fetchPatchBetweenCommits(previousCommit, currentCommit) {
      const gitAddress = currentCommit.gitAddress || previousCommit.gitAddress || "";
      if (!gitAddress) {
        return patchUnavailableBlock(previousCommit, currentCommit, "Git address is missing in history metadata.");
      }
      const params = new URLSearchParams();
      params.set("git_address", gitAddress);
      params.set("from_commit", previousCommit.full);
      params.set("to_commit", currentCommit.full);
      const response = await fetch(`/git-patch?${params.toString()}`);
      const data = await response.json();
      if (!response.ok) {
        return patchUnavailableBlock(previousCommit, currentCommit, data.detail || `HTTP ${response.status}`);
      }
      return `${patchBlockHeader(previousCommit, currentCommit, data)}

${data.patch || ""}
[END PATCH]`;
    }

    async function historyWithPatchesText(records) {
      const sections = [];
      const usedPatchKeys = new Set();
      let previousCommit = null;
      let patchCount = 0;
      let patchErrorCount = 0;

      for (const record of records) {
        const currentCommit = recordCommitInfo(record);
        if (previousCommit && currentCommit.full && !sameCommit(previousCommit, currentCommit)) {
          const key = patchRequestKey(previousCommit, currentCommit);
          if (!usedPatchKeys.has(key)) {
            usedPatchKeys.add(key);
            const patchBlock = await fetchPatchBetweenCommits(previousCommit, currentCommit);
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
      const records = await fetchHistoryRecords(10000);
      if (!records.length) {
        setHistoryStatus("За выбранный период сообщений нет.", "error");
        return;
      }
      const result = await historyWithPatchesText(records);
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
      const response = await fetch(`/history/${encodeURIComponent(recordId)}/restore-to-queue`, {
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

    async function refreshHistory() {
      const records = await fetchHistoryRecords(500);
      currentHistoryRecords = records;
      const dateToText = historyToEl.value ? ` - ${historyToEl.value}` : "";
      historyCountEl.textContent = `${records.length} сообщений с ${historyFromEl.value || todayInputValue()}${dateToText}`;
      historyEl.innerHTML = records.slice().reverse().map((record) => {
        const meta = record.metadata || {};
        const context = record.context || meta.context || "unknown";
        const timestamp = new Date(record.timestamp).toLocaleString("ru-RU");
        const commit = meta.git_commit_short || "no-commit";
        const actor = [meta.sender, meta.receiver].filter(Boolean).join(" -> ") || meta.direction || record.route || record.queue;
        const message = formatMessage(record.message);
        const statusText = extractStatus(message);
        const summary = extractSummary(message);
        const queueItemId = meta.queue_item_id || "";
        const canDeleteFromQueue = queueItemId && activeQueueItemIds.has(`${record.queue}:${queueItemId}`);
        const canDeleteHistoryRecord = record.event === "removed_from_backend_queue";
        return `<article class="entry">
          <div class="entry-head">
            <div>
              <div class="entry-title">
                <span class="dot ${escapeHtml(context)}"></span>
                <span class="badge ${escapeHtml(context)}">${escapeHtml(context)}</span>
                <span class="badge">Commit: ${escapeHtml(commit)}</span>
                <span class="badge event">${escapeHtml(record.event)}</span>
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
      await refreshQueues();
      await Promise.all([refreshScheduledTasks(), refreshHistory(), refreshGitConfig()]);
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
    }

    queueEl.addEventListener("change", applyQueueDefaults);
    scheduleModeEl.addEventListener("change", updateScheduleUi);
    scheduleDelayMinutesEl.addEventListener("input", () => setStatus(""));
    document.querySelectorAll(".page-tab").forEach((button) => {
      button.addEventListener("click", () => setActiveView(button.dataset.view));
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
      saveGitConfig().then(refreshHistory).catch((error) => setGitStatus(error.message, "error"));
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
    document.getElementById("addAgentButton").addEventListener("click", () => addAgent());
    document.getElementById("cloneAgentButton").addEventListener("click", openCloneAgentModal);
    document.getElementById("removeSelectedAgentButton").addEventListener("click", removeSelectedAgent);
    document.getElementById("copyAgentProfileButton").addEventListener("click", () => {
      copySelectedAgentProfile().catch((error) => setAgentsStatus(error.message, "error"));
    });
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
    cloneAgentModalEl.addEventListener("click", (event) => {
      if (event.target === cloneAgentModalEl) {
        closeCloneAgentModal();
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
      setAgentsStatus("");
    });
    agentsEl.addEventListener("input", () => {
      captureCurrentAgent();
      renderAgentSelector(selectedAgentId);
      syncActorsFromAgents(agents);
      updateSpecializationPanel();
      renderAgentStatusSummary();
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
    refreshAttachmentFolderChoices().catch((error) => setAttachmentStatus(error.message, "error"));
    refreshAgents().catch((error) => setAgentsStatus(error.message, "error"));
    refreshEmailRoutes().catch((error) => setEmailRoutesStatus(error.message, "error"));
    refresh();
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
) -> dict[str, Any]:
    queue_meta = queue_definition(queue_name)
    item = make_queue_item(message, metadata)
    item_id = item["id"]
    async with locks[queue_name]:
        queues[queue_name].append(item)
        size = len(queues[queue_name])

    event = queue_meta["post_event"]
    await append_history(
        event,
        queue_name,
        message,
        {"queue_item_id": item_id, **(metadata or {})},
        port,
    )
    return {"status": "queued", "queue": queue_name, "id": item_id, "size": size}


async def dequeue(queue_name: str, port: int | None = None) -> dict[str, Any]:
    queue_meta = queue_definition(queue_name)
    async with locks[queue_name]:
        if not queues[queue_name]:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Queue is empty",
            )
        item = queues[queue_name].popleft()

    item_id = queue_item_id(item)
    message = queue_item_message(item)
    event = queue_meta["get_event"]
    metadata = {"queue_item_id": item_id} if item_id else None
    await append_history(event, queue_name, message, metadata, port=port)
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
    return await enqueue(queue_name, message, phone_metadata, port)


async def dequeue_phone_channel(
    queue_name: str,
    conversation_phone: str,
    to_phone: str,
    port: int | None = None,
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
    delivered_item: Any | None = None
    async with locks[queue_name]:
        kept_items: deque[Any] = deque()
        while queues[queue_name]:
            item = queues[queue_name].popleft()
            if delivered_item is None and phone_channel_item_matches(item, conversation_phone, to_phone):
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
    await append_history(queue_meta["get_event"], queue_name, message, metadata, port=port)
    return {
        "message": message,
        "id": item_id,
        "conversation_phone": conversation_phone,
        "from_phone": metadata.get("from_phone"),
        "to_phone": metadata.get("to_phone"),
        "queue": queue_name,
    }


async def delete_queued_item(
    queue_name: str,
    item_id: str,
    port: int | None = None,
) -> dict[str, Any]:
    queue_meta = queue_definition(queue_name)
    deleted_item: Any | None = None
    async with locks[queue_name]:
        kept_items: deque[Any] = deque()
        while queues[queue_name]:
            item = queues[queue_name].popleft()
            if deleted_item is None and queue_item_id(item) == item_id:
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
    await append_history(
        f"removed_from_{queue_meta['context']}_queue",
        queue_name,
        message,
        {
            "queue_item_id": item_id,
            "sender": "Owner",
            "receiver": queue_meta["default_receiver"],
            "action": "deleted_from_queue",
        },
        port=port,
    )
    return {"status": "deleted", "queue": queue_name, "id": item_id, "size": size}


@app.get("/", response_class=HTMLResponse)
async def index() -> str:
    return render_index_v2()


@app.get("/queues")
async def get_queues() -> dict[str, Any]:
    result: dict[str, int] = {}
    items: dict[str, list[dict[str, Any]]] = {}
    for queue_name in queues:
        async with locks[queue_name]:
            result[queue_name] = len(queues[queue_name])
            items[queue_name] = [queue_item_snapshot(item) for item in queues[queue_name]]
    return {"queues": result, "items": items}


@app.get("/scheduled-tasks")
async def get_scheduled_tasks() -> dict[str, Any]:
    tasks = await list_scheduled_tasks()
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


@app.get("/history")
async def get_history(
    limit: int = 200,
    date_from: str | None = None,
    date_to: str | None = None,
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

    return await read_history(limit, date_from, date_to)


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

    metadata = {
        "sender": "Owner",
        "receiver": queue_definition(queue_name)["default_receiver"],
        "action": "restored_to_original_queue",
        "restored_from_history_id": record_id,
    }
    return await enqueue(
        queue_name,
        record.get("message"),
        metadata,
        request_port(request),
    )


@app.get("/git-config")
async def get_git_config(request: Request) -> dict[str, Any]:
    port = request_port(request)
    config = await read_git_config()
    entry = config.get(str(port), {}) if port is not None else {}
    git_address = entry.get("git_address") if isinstance(entry, dict) else None
    git_context = await git_context_for_port(port)
    return {
        "port": port,
        "config_path": str(git_config_path),
        "git_address": git_address or "",
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

    entry = await save_git_address(port, git_address.strip())
    git_context = await git_context_for_port(port)
    return {
        "port": port,
        "config_path": str(git_config_path),
        **entry,
        **git_context,
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
        "agents": agents,
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

    async with screenshot_folders_lock:
        return await asyncio.to_thread(list_screenshot_folders_file, date_from, date_to)


@app.post("/screenshot-folders", status_code=status.HTTP_201_CREATED)
async def post_screenshot_folder() -> dict[str, Any]:
    async with screenshot_folders_lock:
        return await asyncio.to_thread(create_screenshot_folder_file)


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

    async with evidence_folders_lock:
        return await asyncio.to_thread(list_evidence_folders_file, date_from, date_to)


@app.post("/evidence-folders", status_code=status.HTTP_201_CREATED)
async def post_evidence_folder() -> dict[str, Any]:
    async with evidence_folders_lock:
        return await asyncio.to_thread(create_evidence_folder_file)


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


@app.post("/agents/create-empty", status_code=status.HTTP_201_CREATED)
async def create_empty_agent(request: Request) -> dict[str, Any]:
    base_url = str(request.base_url).rstrip("/")

    async with agents_lock:
        agents = await asyncio.to_thread(read_agents_file)
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
        await asyncio.to_thread(write_agents_file, normalize_agents(agents))

    return {
        "id": agent_id,
        "phone": phone,
        "name": name,
        "prompt_text": prompt_text,
    }


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

    metadata = {
        "submitted_via": "ui",
        "sender": payload.get("sender"),
        "receiver": payload.get("receiver"),
        "status": payload.get("status"),
    }
    schedule = payload.get("schedule")
    if schedule is not None and not isinstance(schedule, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="schedule must be an object.",
        )
    return await schedule_message(
        queue_name,
        message.strip(),
        metadata,
        request_port(request),
        schedule,
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
    return await enqueue_phone_channel(
        "worker-all",
        conversation_phone,
        message,
        metadata,
        request_port(request),
    )


@app.get("/worker/all/{conversation_phone}")
async def get_worker_all(
    conversation_phone: str,
    request: Request,
    to_phone: str,
) -> dict[str, Any]:
    return await dequeue_phone_channel("worker-all", conversation_phone, to_phone, request_port(request))


@app.post("/tester/all/{conversation_phone}", status_code=status.HTTP_201_CREATED)
async def post_tester_all(
    conversation_phone: str,
    request: Request,
) -> dict[str, Any]:
    message, metadata = await read_phone_channel_payload(request)
    return await enqueue_phone_channel(
        "tester-all",
        conversation_phone,
        message,
        metadata,
        request_port(request),
    )


@app.get("/tester/all/{conversation_phone}")
async def get_tester_all(
    conversation_phone: str,
    request: Request,
    to_phone: str,
) -> dict[str, Any]:
    return await dequeue_phone_channel("tester-all", conversation_phone, to_phone, request_port(request))


@app.post("/consultant/all/{conversation_phone}", status_code=status.HTTP_201_CREATED)
async def post_consultant_all(
    conversation_phone: str,
    request: Request,
) -> dict[str, Any]:
    message, metadata = await read_phone_channel_payload(request)
    return await enqueue_phone_channel(
        "consultant-all",
        conversation_phone,
        message,
        metadata,
        request_port(request),
    )


@app.get("/consultant/all/{conversation_phone}")
async def get_consultant_all(
    conversation_phone: str,
    request: Request,
    to_phone: str,
) -> dict[str, Any]:
    return await dequeue_phone_channel("consultant-all", conversation_phone, to_phone, request_port(request))


@app.post("/work", status_code=status.HTTP_201_CREATED)
async def post_work(request: Request) -> dict[str, Any]:
    return await enqueue("work", await read_message(request), port=request_port(request))


@app.get("/work")
async def get_work(request: Request) -> dict[str, Any]:
    return await dequeue("work", request_port(request))


@app.post("/test", status_code=status.HTTP_201_CREATED)
async def post_test(request: Request) -> dict[str, Any]:
    return await enqueue("test", await read_message(request), port=request_port(request))


@app.get("/test")
async def get_test(request: Request) -> dict[str, Any]:
    return await dequeue("test", request_port(request))


@app.post("/work-design", status_code=status.HTTP_201_CREATED)
async def post_work_design(request: Request) -> dict[str, Any]:
    return await enqueue(
        "work-design",
        await read_message(request),
        port=request_port(request),
    )


@app.get("/work-design")
async def get_work_design(request: Request) -> dict[str, Any]:
    return await dequeue("work-design", request_port(request))


@app.post("/test-design", status_code=status.HTTP_201_CREATED)
async def post_test_design(request: Request) -> dict[str, Any]:
    return await enqueue(
        "test-design",
        await read_message(request),
        port=request_port(request),
    )


@app.get("/test-design")
async def get_test_design(request: Request) -> dict[str, Any]:
    return await dequeue("test-design", request_port(request))


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8025)
