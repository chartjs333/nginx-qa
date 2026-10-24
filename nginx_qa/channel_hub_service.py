"""Retired embedded Hub launcher; kept only to reject old deployment commands.

Run Inbound Hub independently using its own documented entry point. Configure
NGINX_QA_CHANNEL_BRIDGE_CONFIG.hub_url with an HTTPS service implementing 1.1.
Stock Hub PR #2 lacks that contract; there is intentionally no internal fallback.
"""


def main():
    raise SystemExit(
        "Embedded Hub composition is disabled: nginx-qa uses HTTP/HTTPS only. "
        "Run Hub independently and provide its remote 1.1 API. "
        "Stock Hub PR #2 does not yet expose the required service endpoints."
    )


if __name__ == "__main__":
    main()
