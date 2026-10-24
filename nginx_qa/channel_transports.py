"""Opt-in human inbox / consented live-session transport for Hub composition.

Provider endpoints are operator-configured HTTPS URLs. They MUST deduplicate the
Idempotency-Key before starting a call. Gateway owns presence, consent and fallback.
"""

from __future__ import annotations

from urllib.parse import urlsplit

import httpx


class HumanChannelAdapter:
    def __init__(self, endpoint=None, token=None, media="audio", client=None):
        if endpoint is not None:
            url = urlsplit(endpoint)
            if (
                url.scheme != "https"
                or not url.hostname
                or url.username
                or url.password
                or url.query
                or url.fragment
            ):
                raise ValueError("Configured HTTPS live-session endpoint required")
        if media not in ("audio", "video"):
            raise ValueError("Unsupported live session media")
        self.endpoint, self.token, self.media = endpoint, token, media
        self.client = client or httpx.Client(
            timeout=10, trust_env=False, follow_redirects=False
        )

    def send(self, request, operation):
        if operation in ("send_message", "request_data", "request_observation"):
            # The unchanged Hub mobile inbox reads durable delivered requests.
            return
        if operation != "request_live_session":
            raise PermissionError("Unsupported human operation")
        if not self.endpoint:
            raise OSError("No live provider; Gateway must apply bounded async fallback")
        headers = {
            "Idempotency-Key": request.request_id,
            "X-Correlation-ID": request.correlation_id,
        }
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        try:
            with self.client.stream(
                "POST",
                self.endpoint,
                headers=headers,
                json={
                    "schema_version": 1,
                    "request_id": request.request_id,
                    "channel_id": request.channel_id,
                    "correlation_id": request.correlation_id,
                    "media": self.media,
                    "text": request.payload.get("text", ""),
                },
                follow_redirects=False,
            ) as response:
                if response.status_code not in (200, 202, 204):
                    raise OSError("Provider did not accept live session")
        except httpx.HTTPError as exc:
            raise OSError("Live provider unavailable") from exc
        return
