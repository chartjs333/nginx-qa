from __future__ import annotations

import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


TRUE_VALUES = {"1", "true", "yes", "on"}
FALSE_VALUES = {"", "0", "false", "no", "off"}
WEBHOOK_SECRET_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,256}$")


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
        raise RuntimeError(f"Telegram {method} failed with HTTP {exc.code}") from exc
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


def configure() -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    secret = os.getenv("TELEGRAM_WEBHOOK_SECRET", "").strip()
    webhook_url = os.getenv("TELEGRAM_WEBHOOK_URL", "").strip()
    auto_register = env_bool("TELEGRAM_WEBHOOK_AUTO_REGISTER")
    drop_pending = env_bool("TELEGRAM_DROP_PENDING_UPDATES")
    validate_id_allowlist("TELEGRAM_ALLOWED_CHAT_IDS")
    validate_id_allowlist("TELEGRAM_ALLOWED_USER_IDS")

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

    telegram_request(
        token,
        "setWebhook",
        {
            "url": webhook_url,
            "secret_token": secret,
            "allowed_updates": json.dumps(["message", "channel_post"]),
            "drop_pending_updates": "true" if drop_pending else "false",
        },
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
