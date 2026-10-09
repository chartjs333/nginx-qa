"""Durable delivery metadata, never an approval or execution state machine.

Links contain public opaque identifiers, not capabilities. Existing operator
authentication and the authoritative scope workflow remain mandatory. SMTP
acceptance is at-least-once: a crash before recording acceptance can duplicate
mail, but cannot create a semantic decision. Read methods do not initialize,
reconcile, claim, audit, or modify the outbox.
"""
from __future__ import annotations

from contextlib import closing, contextmanager
from dataclasses import dataclass, field
from email.message import EmailMessage
from email.utils import parseaddr
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import secrets
import smtplib
import sqlite3
import ssl
import time
from typing import Callable, Mapping, Protocol
from urllib.parse import urlencode, urlsplit


BINDING_FIELDS = (
    "runtime_type", "project_id", "sprint_id", "request_id", "request_sha256",
    "content_sha256", "identity_sha256", "assignment_id", "base_scope_revision",
    "execution_revision",
)
DISPLAY_FIELDS = ("project_name", "sprint_name", "node_id", "role_id", "summary", "reason")
_ERROR_CODES = frozenset({"SMTP_AUTH_FAILED", "SMTP_TLS_FAILED", "SMTP_DELIVERY_FAILED",
    "DELIVERY_FAILED", "SNAPSHOT_STALE", "LINK_EXPIRED", "LEASE_EXPIRED"})
_REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


class NotificationError(ValueError):
    """Static codes only: never echo provider errors, configuration or secrets."""
    def __init__(self, code: str, status_code: int = 400):
        super().__init__(code)
        self.code, self.status_code = code, status_code


def _digest(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
        separators=(",", ":"), allow_nan=False).encode("utf-8")).hexdigest()


def _credential_values(env: Mapping[str, str] | None = None) -> tuple[str, ...]:
    source = os.environ if env is None else env
    return tuple(sorted({str(value) for name, value in source.items()
        if value and len(str(value)) >= 4 and any(part in name.upper()
            for part in ("TOKEN", "PASSWORD", "SECRET", "API_KEY", "SMTP_USERNAME"))}, key=len, reverse=True))


def known_secret_values(env: Mapping[str, str] | None = None) -> tuple[str, ...]:
    """For in-process rejection/redaction only; callers must never log values."""
    return _credential_values(env)


def safe_text(value, *, limit: int = 512, credentials: tuple[str, ...] = ()) -> str:
    text = str(value or "")
    # Locate every match in the ORIGINAL text. Sequential replacement can leak
    # a password suffix when a username is contained in it, or when two secrets
    # overlap without either containing the other. Merge all original spans.
    spans = []
    for secret in set((*credentials, *_credential_values())):
        if not secret:
            continue
        start = text.find(secret)
        while start >= 0:
            spans.append((start, start + len(secret)))
            start = text.find(secret, start + 1)
    merged = []
    for start, end in sorted(spans):
        if merged and start <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(end, merged[-1][1]))
        else:
            merged.append((start, end))
    pieces, offset = [], 0
    for start, end in merged:
        pieces.extend((text[offset:start], "[REDACTED]"))
        offset = end
    pieces.append(text[offset:])
    text = "".join(pieces)
    # Defense in depth for pasted credentials that are not in this process.
    text = re.sub(r"(?i)\bbearer\s+\S+", "Bearer [REDACTED]", text)
    text = re.sub(r"(?i)\b(?:password|secret|token|api[_-]?key)\s*[:=]\s*\S+", "credential=[REDACTED]", text)
    text = re.sub(r"\b[0-9a-fA-F]{64}\b", "[REDACTED]", text)
    text = " ".join(text.split())
    return text[:limit]


def _email_address(value) -> str:
    if not isinstance(value, str) or len(value) > 254 or "\r" in value or "\n" in value:
        raise NotificationError("EMAIL_CONFIG_INVALID")
    _, parsed = parseaddr(value)
    if parsed != value or not re.fullmatch(r"[^\s<>@,;]+@[^\s<>@,;]+", value):
        raise NotificationError("EMAIL_CONFIG_INVALID")
    return value


def _integer(value, minimum, maximum):
    if type(value) is not int or not minimum <= value <= maximum:
        raise NotificationError("EMAIL_CONFIG_INVALID")
    return value


@dataclass(frozen=True)
class EmailConfig:
    enabled: bool = False
    pending_decisions: bool = False
    destination: str = ""
    sender: str = ""
    public_base_url: str = ""
    database_path: Path | None = None
    smtp_host: str = ""
    smtp_port: int = 587
    smtp_security: str = "starttls"
    smtp_username: str = field(default="", repr=False)
    smtp_password: str = field(default="", repr=False)
    link_ttl_seconds: int = 3600
    poll_interval_seconds: int = 30
    lease_seconds: int = 120
    max_attempts: int = 5

    @classmethod
    def from_environment(cls, env: Mapping[str, str] | None = None) -> "EmailConfig":
        values = os.environ if env is None else env
        path = values.get("NGINX_QA_NOTIFICATION_CONFIG")
        if not path:
            return cls()
        if not Path(path).is_absolute():
            raise NotificationError("EMAIL_CONFIG_PATH_INVALID")
        try:
            raw = json.loads(Path(path).read_text(encoding="utf-8"))
        except (OSError, ValueError, TypeError):
            raise NotificationError("EMAIL_CONFIG_UNREADABLE") from None
        return cls.from_dict(raw, env=values)

    @classmethod
    def from_dict(cls, raw: dict, *, env: Mapping[str, str] | None = None) -> "EmailConfig":
        values = os.environ if env is None else env
        if not isinstance(raw, dict) or set(raw) != {"schema_version", "notifications"} or type(raw["schema_version"]) is not int or raw["schema_version"] != 1:
            raise NotificationError("EMAIL_CONFIG_INVALID")
        notifications = raw["notifications"]
        if not isinstance(notifications, dict) or set(notifications) != {"email"} or not isinstance(notifications["email"], dict):
            raise NotificationError("EMAIL_CONFIG_INVALID")
        email = notifications["email"]
        allowed = {"enabled", "pending_decisions", "destination", "sender", "public_base_url",
            "database_path", "smtp", "link_ttl_seconds", "poll_interval_seconds", "lease_seconds", "max_attempts", "allow_loopback_http"}
        if set(email) - allowed or type(email.get("enabled")) is not bool or type(email.get("pending_decisions", True)) is not bool:
            raise NotificationError("EMAIL_CONFIG_INVALID")
        # Disabling delivery does not make inline credentials an acceptable
        # configuration format. Do not silently preserve such a future hazard.
        if "smtp" in email and (not isinstance(email["smtp"], dict) or set(email["smtp"]) != {"host", "port", "security"}):
            raise NotificationError("EMAIL_CONFIG_INVALID")
        if not email["enabled"]:
            return cls()
        smtp = email.get("smtp")
        if not isinstance(smtp, dict) or set(smtp) != {"host", "port", "security"}:
            raise NotificationError("EMAIL_CONFIG_INVALID")
        host = smtp["host"]
        if not isinstance(host, str) or not host or len(host) > 253 or re.search(r"[\s/@\\]", host):
            raise NotificationError("EMAIL_CONFIG_INVALID")
        if not isinstance(smtp["security"], str) or smtp["security"] not in {"starttls", "ssl"}:
            raise NotificationError("EMAIL_CONFIG_TLS_REQUIRED")
        base = email.get("public_base_url")
        if not isinstance(base, str):
            raise NotificationError("EMAIL_CONFIG_URL_INVALID")
        try:
            parsed = urlsplit(base)
            parsed.port
        except ValueError:
            raise NotificationError("EMAIL_CONFIG_URL_INVALID") from None
        try:
            loopback = parsed.hostname == "localhost" or ipaddress.ip_address(parsed.hostname or "").is_loopback
        except ValueError:
            loopback = False
        if type(email.get("allow_loopback_http", False)) is not bool or re.search(r"[\s\\]", base):
            raise NotificationError("EMAIL_CONFIG_URL_INVALID")
        if (parsed.scheme != "https" and not (parsed.scheme == "http" and loopback and email.get("allow_loopback_http") is True)) or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment or parsed.path not in {"", "/"}:
            raise NotificationError("EMAIL_CONFIG_URL_INVALID")
        if any(value in base for value in _credential_values(values)):
            raise NotificationError("EMAIL_CONFIG_URL_INVALID")
        database = email.get("database_path")
        if not isinstance(database, str) or not Path(database).is_absolute():
            raise NotificationError("EMAIL_CONFIG_DATABASE_INVALID")
        dbpath = Path(database).resolve()
        if dbpath == _REPOSITORY_ROOT or dbpath.is_relative_to(_REPOSITORY_ROOT) or dbpath.is_dir():
            raise NotificationError("EMAIL_CONFIG_DATABASE_INVALID")
        username = values.get("NGINX_QA_SMTP_USERNAME", "")
        password = values.get("NGINX_QA_SMTP_PASSWORD", "")
        if bool(username) != bool(password) or any("\r" in value or "\n" in value for value in (username, password)):
            raise NotificationError("EMAIL_CONFIG_CREDENTIALS_INVALID")
        return cls(enabled=True, pending_decisions=email.get("pending_decisions", True),
            destination=_email_address(email.get("destination")), sender=_email_address(email.get("sender")),
            public_base_url=base.rstrip("/"), database_path=dbpath, smtp_host=host,
            smtp_port=_integer(smtp["port"], 1, 65535), smtp_security=smtp["security"],
            smtp_username=username, smtp_password=password,
            link_ttl_seconds=_integer(email.get("link_ttl_seconds", 3600), 60, 86400),
            poll_interval_seconds=_integer(email.get("poll_interval_seconds", 30), 1, 3600),
            lease_seconds=_integer(email.get("lease_seconds", 120), 60, 3600),
            max_attempts=_integer(email.get("max_attempts", 5), 1, 100))


def binding_matches(record: dict, snapshot: dict) -> bool:
    binding = record.get("binding", record)
    other = snapshot.get("binding", snapshot)
    return all(key in binding and key in other and binding[key] == other[key] for key in BINDING_FIELDS)


def is_expired(record: dict, now: float | None = None) -> bool:
    return float(record["expires_at"]) <= (time.time() if now is None else now)


def _snapshot(raw: dict, credentials=()) -> dict:
    if not isinstance(raw, dict) or any(key not in raw for key in BINDING_FIELDS):
        raise NotificationError("NOTIFICATION_SNAPSHOT_INVALID")
    result = {}
    for key in BINDING_FIELDS:
        value = raw[key]
        if key in {"base_scope_revision", "execution_revision"}:
            if type(value) is not int or value < 0:
                raise NotificationError("NOTIFICATION_SNAPSHOT_INVALID")
        elif not isinstance(value, str) or not value or len(value) > 512 or re.search(r"[\x00-\x1f\x7f]", value):
            raise NotificationError("NOTIFICATION_SNAPSHOT_INVALID")
        if key.endswith("sha256") and not re.fullmatch(r"[a-f0-9]{64}", value):
            raise NotificationError("NOTIFICATION_SNAPSHOT_INVALID")
        if isinstance(value, str) and any(secret in value for secret in (*credentials, *_credential_values())):
            raise NotificationError("CREDENTIAL_IN_NOTIFICATION_FORBIDDEN")
        result[key] = value
    for key in DISPLAY_FIELDS:
        result[key] = safe_text(raw.get(key), credentials=credentials)
    return result


class NotificationStore:
    def __init__(self, path: str | Path, *, link_ttl_seconds: int = 3600,
                 lease_seconds: int = 120, max_attempts: int = 5, credentials: tuple[str, ...] = ()):
        self.path = Path(path)
        if not self.path.is_absolute():
            raise NotificationError("NOTIFICATION_DATABASE_PATH_INVALID")
        self.link_ttl_seconds, self.lease_seconds, self.max_attempts = link_ttl_seconds, lease_seconds, max_attempts
        self._credentials = credentials

    @contextmanager
    def _connection(self, *, write=False):
        uri = self.path.resolve().as_uri() + ("?mode=rw" if write else "?mode=ro")
        connection = sqlite3.connect(uri, uri=True, timeout=10, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=10000")
        if not write:
            connection.execute("PRAGMA query_only=ON")
        try:
            if write:
                connection.execute("BEGIN IMMEDIATE")
            yield connection
            if write:
                connection.commit()
        except BaseException:
            if write:
                connection.rollback()
            raise
        finally:
            connection.close()

    def initialize(self) -> None:
        """Startup/worker only. GET paths must never call this method."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(self.path, timeout=10)) as connection:
            connection.execute("PRAGMA journal_mode=DELETE")
            connection.execute("PRAGMA synchronous=FULL")
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            if version not in {0, 1}:
                raise NotificationError("NOTIFICATION_DATABASE_VERSION_UNSUPPORTED", 503)
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS notifications (
                    notification_id TEXT PRIMARY KEY, semantic_key TEXT UNIQUE NOT NULL,
                    snapshot_json TEXT NOT NULL, status TEXT NOT NULL, created_at REAL NOT NULL,
                    expires_at REAL NOT NULL, updated_at REAL NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
                    error_code TEXT, lease_id TEXT, lease_until REAL, delivered_at REAL);
                CREATE TABLE IF NOT EXISTS notification_events (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT, notification_id TEXT NOT NULL,
                    kind TEXT NOT NULL, timestamp REAL NOT NULL, error_code TEXT);
                CREATE TABLE IF NOT EXISTS notification_retries (
                    notification_id TEXT NOT NULL, idempotency_hash TEXT NOT NULL,
                    PRIMARY KEY(notification_id, idempotency_hash));
                CREATE TRIGGER IF NOT EXISTS immutable_notification_events_update
                    BEFORE UPDATE ON notification_events BEGIN SELECT RAISE(ABORT, 'APPEND_ONLY'); END;
                CREATE TRIGGER IF NOT EXISTS immutable_notification_events_delete
                    BEFORE DELETE ON notification_events BEGIN SELECT RAISE(ABORT, 'APPEND_ONLY'); END;
                PRAGMA user_version=1;
            """)
            connection.commit()

    @staticmethod
    def _record(row) -> dict | None:
        if row is None:
            return None
        item = dict(row)
        snapshot = json.loads(item.pop("snapshot_json"))
        item.pop("semantic_key", None)
        item.pop("lease_id", None)
        item.pop("lease_until", None)
        return {**item, **snapshot, "binding": {key: snapshot[key] for key in BINDING_FIELDS}}

    @staticmethod
    def _event(connection, notification_id, kind, now, error_code=None):
        connection.execute("INSERT INTO notification_events(notification_id,kind,timestamp,error_code) VALUES(?,?,?,?)",
            (notification_id, kind, now, error_code))

    def get(self, notification_id: str) -> dict | None:
        if not self.path.is_file():
            return None
        with self._connection() as connection:
            return self._record(connection.execute("SELECT * FROM notifications WHERE notification_id=?", (notification_id,)).fetchone())

    def list_for(self, runtime_type: str, project_id: str, sprint_id: str, request_id: str | None = None) -> list[dict]:
        if not self.path.is_file():
            return []
        with self._connection() as connection:
            records = [self._record(row) for row in connection.execute("SELECT * FROM notifications ORDER BY created_at,notification_id")]
        return [item for item in records if item["runtime_type"] == str(runtime_type)
            and item["project_id"] == str(project_id) and item["sprint_id"] == str(sprint_id)
            and (request_id is None or item["request_id"] == request_id)]

    def events(self, notification_id: str) -> list[dict]:
        if not self.path.is_file():
            return []
        with self._connection() as connection:
            return [dict(row) for row in connection.execute("SELECT * FROM notification_events WHERE notification_id=? ORDER BY sequence", (notification_id,))]

    def enqueue(self, snapshot: dict, *, now: float | None = None) -> dict:
        now = time.time() if now is None else now
        clean = _snapshot(snapshot, self._credentials)
        key = _digest({name: clean[name] for name in ("runtime_type", "project_id", "sprint_id", "request_id")})
        with self._connection(write=True) as connection:
            previous = connection.execute("SELECT * FROM notifications WHERE semantic_key=?", (key,)).fetchone()
            if previous:
                return self._record(previous)
            notification_id = secrets.token_hex(16)
            connection.execute("INSERT INTO notifications(notification_id,semantic_key,snapshot_json,status,created_at,expires_at,updated_at) VALUES(?,?,?,'pending',?,?,?)",
                (notification_id, key, json.dumps(clean, ensure_ascii=False, sort_keys=True), now, now + self.link_ttl_seconds, now))
            self._event(connection, notification_id, "notification_requested", now)
            self._event(connection, notification_id, "delivery_pending", now)
            return self._record(connection.execute("SELECT * FROM notifications WHERE notification_id=?", (notification_id,)).fetchone())

    def retry(self, notification_id: str, *, idempotency_key: str, now: float | None = None) -> dict:
        """Retry failures only. Expired/stale mail must use normal /execution.

        Retry does not refresh a link or snapshot. Existing mail can never be
        rebound to a different request, assignment or revision.
        """
        now = time.time() if now is None else now
        if not isinstance(idempotency_key, str) or not 1 <= len(idempotency_key) <= 200:
            raise NotificationError("NOTIFICATION_IDEMPOTENCY_REQUIRED")
        key = _digest(idempotency_key)
        with self._connection(write=True) as connection:
            row = connection.execute("SELECT * FROM notifications WHERE notification_id=?", (notification_id,)).fetchone()
            if row is None:
                raise NotificationError("NOTIFICATION_NOT_FOUND", 404)
            if connection.execute("SELECT 1 FROM notification_retries WHERE notification_id=? AND idempotency_hash=?", (notification_id, key)).fetchone():
                return {**self._record(row), "deduplicated": True}
            if is_expired(dict(row), now):
                raise NotificationError("NOTIFICATION_LINK_EXPIRED", 409)
            if row["status"] != "failed" or row["attempts"] >= self.max_attempts:
                raise NotificationError("NOTIFICATION_RETRY_CONFLICT", 409)
            connection.execute("INSERT INTO notification_retries VALUES(?,?)", (notification_id, key))
            connection.execute("UPDATE notifications SET status='retry_scheduled',updated_at=?,error_code=NULL WHERE notification_id=?", (now, notification_id))
            self._event(connection, notification_id, "retry_scheduled", now)
            return {**self._record(connection.execute("SELECT * FROM notifications WHERE notification_id=?", (notification_id,)).fetchone()), "deduplicated": False}

    def claim(self, *, now: float | None = None) -> tuple[dict, str] | None:
        now = time.time() if now is None else now
        with self._connection(write=True) as connection:
            # Recover a crashed delivery process; never change semantic state.
            for row in connection.execute("SELECT * FROM notifications WHERE status='sending' AND lease_until<=?", (now,)).fetchall():
                exhausted = row["attempts"] >= self.max_attempts
                status = "failed" if exhausted else "pending"
                connection.execute("UPDATE notifications SET status=?,lease_id=NULL,lease_until=NULL,updated_at=?,error_code='LEASE_EXPIRED' WHERE notification_id=?", (status, now, row["notification_id"]))
                self._event(connection, row["notification_id"], "lease_recovered", now, "LEASE_EXPIRED")
            row = connection.execute("SELECT * FROM notifications WHERE status IN ('pending','retry_scheduled') ORDER BY created_at,notification_id LIMIT 1").fetchone()
            if row is None:
                return None
            notification_id, lease = row["notification_id"], secrets.token_hex(16)
            connection.execute("UPDATE notifications SET status='sending',attempts=attempts+1,lease_id=?,lease_until=?,updated_at=? WHERE notification_id=?", (lease, now + self.lease_seconds, now, notification_id))
            self._event(connection, notification_id, "delivery_started", now)
            return self._record(connection.execute("SELECT * FROM notifications WHERE notification_id=?", (notification_id,)).fetchone()), lease

    def finish(self, notification_id: str, lease: str, *, status: str, error_code: str | None = None, now: float | None = None) -> bool:
        now = time.time() if now is None else now
        if status not in {"delivered", "failed", "suppressed"}:
            raise NotificationError("NOTIFICATION_RESULT_INVALID")
        if error_code is not None and error_code not in _ERROR_CODES:
            error_code = "DELIVERY_FAILED"
        with self._connection(write=True) as connection:
            result = connection.execute("UPDATE notifications SET status=?,updated_at=?,error_code=?,lease_id=NULL,lease_until=NULL,delivered_at=? WHERE notification_id=? AND status='sending' AND lease_id=?",
                (status, now, error_code, now if status == "delivered" else None, notification_id, lease))
            if result.rowcount != 1:
                return False
            self._event(connection, notification_id, "delivery_" + status, now, error_code)
            return True


class NotificationProvider(Protocol):
    def send(self, notification: dict) -> None: ...


class SmtpEmailProvider:
    def __init__(self, config: EmailConfig):
        self.config = config

    def message(self, notification: dict) -> EmailMessage:
        config = self.config
        credentials = tuple(value for value in (config.smtp_username, config.smtp_password) if value)
        clean = {key: safe_text(notification.get(key), credentials=credentials) for key in DISPLAY_FIELDS}
        notification_id = notification.get("notification_id", "")
        if not re.fullmatch(r"[a-f0-9]{32}", notification_id):
            raise NotificationError("NOTIFICATION_ID_INVALID")
        url = config.public_base_url + "/execution?" + urlencode({"notification": notification_id})
        message = EmailMessage()
        message["From"], message["To"] = config.sender, config.destination
        message["Subject"] = "nginx-qa — требуется ваше решение"
        message["Message-ID"] = f"<nginx-qa-{notification_id}@notifications.invalid>"
        message.set_content("\n".join([
            "nginx-qa — требуется ваше решение", "",
            "Project: " + clean["project_name"], "Sprint: " + clean["sprint_name"],
            "Node / role: " + clean["node_id"] + " / " + clean["role_id"], "",
            "Требуется: " + clean["summary"], "Причина: " + clean["reason"], "",
            "Открыть решение: " + url, "",
            "Ссылка только открывает карточку. Для решения требуется штатная авторизация оператора.",
            "Если ссылка устарела, откройте актуальное Pending decision через /execution.",
            "Содержание карточки и точный diff являются authoritative; письмо — только уведомление.",
        ]))
        return message

    def send(self, notification: dict) -> None:
        config = self.config
        message = self.message(notification)
        try:
            context = ssl.create_default_context()
            if config.smtp_security == "ssl":
                client = smtplib.SMTP_SSL(config.smtp_host, config.smtp_port, timeout=20, context=context)
            elif config.smtp_security == "starttls":
                client = smtplib.SMTP(config.smtp_host, config.smtp_port, timeout=20)
            else:
                raise NotificationError("SMTP_TLS_FAILED")
            with client:
                client.ehlo()
                if config.smtp_security == "starttls":
                    client.starttls(context=context)
                    client.ehlo()
                if config.smtp_username:
                    client.login(config.smtp_username, config.smtp_password)
                refused = client.send_message(message)
                if refused:
                    raise NotificationError("SMTP_DELIVERY_FAILED")
        except smtplib.SMTPAuthenticationError:
            raise NotificationError("SMTP_AUTH_FAILED") from None
        except (ssl.SSLError, smtplib.SMTPNotSupportedError):
            raise NotificationError("SMTP_TLS_FAILED") from None
        except NotificationError:
            raise
        except Exception:
            raise NotificationError("SMTP_DELIVERY_FAILED") from None


class NotificationWorker:
    def __init__(self, config: EmailConfig, store: NotificationStore, provider: NotificationProvider | None = None):
        self.config, self.store = config, store
        self.provider = provider if provider is not None else SmtpEmailProvider(config)

    def run_once(self, is_current: Callable[[dict], bool], *, now: float | None = None) -> dict:
        if not self.config.enabled or not self.config.pending_decisions:
            return {"status": "disabled"}
        current_time = time.time() if now is None else now
        claimed = self.store.claim(now=current_time)
        if claimed is None:
            return {"status": "idle"}
        record, lease = claimed
        status, error = "delivered", None
        try:
            if is_expired(record, current_time):
                status, error = "suppressed", "LINK_EXPIRED"
            elif not is_current(record):
                status, error = "suppressed", "SNAPSHOT_STALE"
            else:
                self.provider.send(record)
        except NotificationError as exc:
            status, error = "failed", exc.code if exc.code in _ERROR_CODES else "DELIVERY_FAILED"
        except Exception:
            status, error = "failed", "DELIVERY_FAILED"
        completed = self.store.finish(record["notification_id"], lease, status=status,
            error_code=error, now=time.time() if now is None else now)
        return {"notification_id": record["notification_id"], "status": status if completed else "lease_lost", "error_code": error}
