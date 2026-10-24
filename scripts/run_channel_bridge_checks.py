"""Required Bridge qualification; no skip-as-pass and no service deployment.

Usage: python scripts/run_channel_bridge_checks.py /absolute/inbound-hub/channel_bridge/src
Install requirements-channel-tests.txt and the pinned Hub package first.
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
HUB_SHA = "d31f6bd54f70a7e7e2e4295b43424b81dc272155"


def run(argv, env):
    result = subprocess.run(argv, cwd=ROOT, env=env, check=False)
    if result.returncode:
        raise SystemExit(result.returncode)


def main():
    if len(sys.argv) != 2:
        raise SystemExit(__doc__)
    source = Path(sys.argv[1]).resolve(strict=True)
    repo = source.parent.parent
    head = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if head != HUB_SHA:
        raise SystemExit("Hub checkout is not the qualified PR #2 commit")
    if subprocess.run(
        ["git", "-C", str(repo), "diff", "--quiet", "HEAD", "--", "channel_bridge"],
        check=False,
    ).returncode:
        raise SystemExit("Hub source is modified")
    env = {**os.environ, "HUB_BRIDGE_SOURCE": str(source)}
    with tempfile.TemporaryDirectory(prefix="channel-checks-") as tmp:
        result = Path(tmp) / "results.xml"
        run(
            [
                sys.executable,
                "-m",
                "pytest",
                "integration_tests/test_channel_bridge.py",
                "-q",
                "--junitxml=" + str(result),
            ],
            env,
        )
        tree = ET.parse(result)
        suites = tree.findall(".//testsuite")
        if not suites or any(
            int(s.get(k, "0"))
            for s in suites
            for k in ("failures", "errors", "skipped")
        ):
            raise SystemExit("Mandatory integration did not pass without skips")
    paths = [
        str(p.relative_to(ROOT))
        for p in sorted((ROOT / "nginx_qa").glob("channel*.py"))
    ]
    run(
        [
            sys.executable,
            "-m",
            "ruff",
            "check",
            *paths,
            "integration_tests/test_channel_bridge.py",
            "scripts/export_channel_schemas.py",
            "scripts/run_channel_bridge_checks.py",
        ],
        env,
    )
    run(
        [
            sys.executable,
            "-m",
            "mypy",
            "--follow-imports=skip",
            "--ignore-missing-imports",
            "--check-untyped-defs",
            *paths,
        ],
        env,
    )
    run([sys.executable, "scripts/export_channel_schemas.py", "--check"], env)
    run([sys.executable, "-m", "bandit", "-q", *paths], env)
    run(["git", "diff", "--check"], env)


if __name__ == "__main__":
    main()
