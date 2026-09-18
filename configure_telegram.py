from __future__ import annotations

import ipaddress
import json
import os
import re
import socket
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


TRUE_VALUES = {"1", "true", "yes", "on"}
FALSE_VALUES = {"", "0", "false", "no", "off"}
WEBHOOK_SECRET_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,256}$")
TRANSIENT_WEBHOOK_ERROR_MARKERS = (
    "failed to resolve host",
    "failed to connect",
    "connection timed out",
    "connection refused",
    "temporary failure",
)


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = raw.strip().casefold()
    if value in TRUE_VALUES:
        return True
    if value in FALSE_VALUES:
        return False
    raise ValueError(f"{name} must be 1/0, true/false, yes/no, or on/off")


def validate_id_allowlist(name: str) -> None:
    raw = os.getenv(name, "").strip()
    if not raw:
        return
    values = [value.strip() for value in raw.split(",") if value.strip()]
    if not values or any(not re.fullmatch(r"-?\d+", value) for value in values):
        raise ValueError(f"{name} must contain comma-separated numeric Telegram IDs")


def validate_optional_numeric_id(name: str, *, allow_negative: bool) -> None:
    raw = os.getenv(name, "").strip()
    if not raw:
        return
    pattern = r"-?\d+" if allow_negative else r"\d+"
    if not re.fullmatch(pattern, raw):
        qualifier = "numeric" if allow_negative else "a positive numeric"
        raise ValueError(f"{name} must be {qualifier} Telegram ID")


def telegram_request(token: str, method: str, data: dict[str, Any]) -> dict[str, Any]:
    request = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/{method}",
        data=urllib.parse.urlencode(data).encode("utf-8"),
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read(64 * 1024)
    except urllib.error.HTTPError as exc:
        raw_error = exc.read(64 * 1024)
        description = ""
        try:
            error_payload = json.loads(raw_error)
            if isinstance(error_payload, dict):
                description = str(error_payload.get("description") or "").strip()
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass
        suffix = f": {description}" if description else ""
        raise RuntimeError(f"Telegram {method} failed with HTTP {exc.code}{suffix}") from exc
    except (urllib.error.URLError, TimeoutError) as exc:
        raise RuntimeError(f"Telegram {method} request failed") from exc
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Telegram {method} returned invalid JSON") from exc
    if not isinstance(payload, dict) or not payload.get("ok"):
        description = payload.get("description") if isinstance(payload, dict) else None
        raise RuntimeError(description or f"Telegram {method} was rejected")
    return payload


def webhook_retry_seconds() -> float:
    raw = os.getenv("TELEGRAM_WEBHOOK_REGISTER_RETRY_SECONDS", "").strip()
    if not raw:
        return 120.0 if env_bool("CLOUDFLARED_QUICK_TUNNEL") else 0.0
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError("TELEGRAM_WEBHOOK_REGISTER_RETRY_SECONDS must be numeric") from exc
    if value < 0 or value > 600:
        raise ValueError("TELEGRAM_WEBHOOK_REGISTER_RETRY_SECONDS must be between 0 and 600")
    return value


def webhook_ip_address(webhook_url: str) -> str:
    configured = os.getenv("TELEGRAM_WEBHOOK_IP_ADDRESS", "").strip()
    if configured:
        try:
            return str(ipaddress.ip_address(configured))
        except ValueError as exc:
            raise ValueError("TELEGRAM_WEBHOOK_IP_ADDRESS must be a valid IP address") from exc
    if not env_bool("CLOUDFLARED_QUICK_TUNNEL"):
        return ""
    hostname = urllib.parse.urlparse(webhook_url).hostname or ""
    if not hostname.casefold().endswith(".trycloudflare.com"):
        return ""
    try:
        addresses = socket.getaddrinfo(
            hostname,
            443,
            family=socket.AF_INET,
            type=socket.SOCK_STREAM,
        )
    except socket.gaierror:
        return ""
    for address in addresses:
        if address[4]:
            return str(address[4][0])
    return ""


def register_webhook_with_retry(
    token: str,
    data: dict[str, Any],
    *,
    retry_seconds: float,
) -> None:
    deadline = time.monotonic() + retry_seconds
    attempt = 0
    while True:
        attempt += 1
        try:
            request_data = dict(data)
            resolved_ip = webhook_ip_address(str(request_data.get("url") or ""))
            if resolved_ip:
                request_data["ip_address"] = resolved_ip
            telegram_request(token, "setWebhook", request_data)
            return
        except RuntimeError as exc:
            message = str(exc).casefold()
            is_transient = any(marker in message for marker in TRANSIENT_WEBHOOK_ERROR_MARKERS)
            remaining = deadline - time.monotonic()
            if not is_transient or remaining <= 0:
                raise
            delay = min(5.0, remaining)
            print(
                f"Telegram: webhook address is not ready yet; retrying in {delay:.0f}s "
                f"(attempt {attempt}).",
                file=sys.stderr,
            )
            time.sleep(delay)


def configure() -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    secret = os.getenv("TELEGRAM_WEBHOOK_SECRET", "").strip()
    webhook_url = os.getenv("TELEGRAM_WEBHOOK_URL", "").strip()
    auto_register = env_bool("TELEGRAM_WEBHOOK_AUTO_REGISTER")
    drop_pending = env_bool("TELEGRAM_DROP_PENDING_UPDATES")
    validate_id_allowlist("TELEGRAM_ALLOWED_CHAT_IDS")
    validate_id_allowlist("TELEGRAM_ALLOWED_USER_IDS")
    validate_optional_numeric_id("TELEGRAM_HISTORY_CHAT_ID", allow_negative=True)
    validate_optional_numeric_id(
        "TELEGRAM_HISTORY_MESSAGE_THREAD_ID",
        allow_negative=False,
    )

    if secret and not WEBHOOK_SECRET_PATTERN.fullmatch(secret):
        raise ValueError(
            "TELEGRAM_WEBHOOK_SECRET must contain 1-256 letters, digits, underscores, or hyphens"
        )
    if not token:
        if auto_register:
            raise ValueError("TELEGRAM_BOT_TOKEN is required when webhook auto-registration is enabled")
        print("Telegram: TELEGRAM_BOT_TOKEN is not set; integration is disabled.")
        return
    if token.startswith("CHANGE_ME_"):
        raise ValueError("Replace the TELEGRAM_BOT_TOKEN placeholder with a newly rotated token")
    if not auto_register:
        print("Telegram: token loaded; webhook auto-registration is disabled.")
        return
    if not webhook_url:
        raise ValueError("TELEGRAM_WEBHOOK_URL is required when webhook auto-registration is enabled")
    parsed_url = urllib.parse.urlparse(webhook_url)
    if parsed_url.scheme != "https" or not parsed_url.netloc:
        raise ValueError("TELEGRAM_WEBHOOK_URL must be a public HTTPS URL")
    if not secret or secret.startswith("CHANGE_ME_"):
        raise ValueError(
            "Set a non-placeholder TELEGRAM_WEBHOOK_SECRET before registering the webhook"
        )

    register_webhook_with_retry(
        token,
        {
            "url": webhook_url,
            "secret_token": secret,
            "allowed_updates": json.dumps(["message", "channel_post"]),
            "drop_pending_updates": "true" if drop_pending else "false",
        },
        retry_seconds=webhook_retry_seconds(),
    )
    print(f"Telegram: webhook registered at {webhook_url}")


def main() -> int:
    try:
        configure()
    except (ValueError, RuntimeError) as exc:
        print(f"Telegram configuration error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
