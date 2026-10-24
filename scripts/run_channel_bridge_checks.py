"""HTTP-only Bridge checks, with explicit real-Hub and simulator results.

Usage: python scripts/run_channel_bridge_checks.py --hub-wheel /installed-artifacts/inbound_agent_channel_bridge-0.1.0-py3-none-any.whl
The wheel is provisioned from pinned unmodified Hub PR #2; no source checkout or
PYTHONPATH injection into nginx is accepted. No live service endpoints are used.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hub-wheel", required=True, type=Path)
    args = parser.parse_args()
    wheel = args.hub_wheel.resolve(strict=True)
    env = {**os.environ, "HUB_HTTP_WHEEL": str(wheel)}
    with tempfile.TemporaryDirectory(prefix="http-checks-") as tmp:
        for suite in (
            "tests/test_channel_http_contract.py",
            "integration_tests/test_channel_bridge.py",
            "integration_tests/test_hub_public_http.py",
        ):
            report = Path(tmp) / (Path(suite).stem + ".xml")
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "pytest",
                    "-q",
                    suite,
                    "--junitxml=" + str(report),
                ],
                cwd=ROOT,
                env=env,
                check=False,
            )
            if result.returncode:
                raise SystemExit(result.returncode)
            suites = ET.parse(report).findall(".//testsuite")
            if not suites or any(
                int(item.get(key, "0"))
                for item in suites
                for key in ("failures", "errors", "skipped")
            ):
                raise SystemExit("HTTP qualification requires no failures/errors/skips")
    print(
        "Targeted tests passed. Stock Hub v1 is NOT a remote 1.1 implementation; see VALIDATION.md."
    )


if __name__ == "__main__":
    main()
