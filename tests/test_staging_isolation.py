from __future__ import annotations

import json
import unittest
from pathlib import Path

from nginx_qa.managed_import import normalize_managed_runtime_config
from nginx_qa.sprint_types import (
    managed_runtime_config_invariant_issues,
    windows_paths_overlap,
)


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
ENVIRONMENT_EXAMPLE = REPOSITORY_ROOT / ".env.staging.example"
LAUNCHER = REPOSITORY_ROOT / "run_staging.ps1"


def parse_environment_example() -> dict[str, str]:
    result: dict[str, str] = {}
    for raw_line in ENVIRONMENT_EXAMPLE.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        name, separator, value = line.partition("=")
        if not separator:
            raise AssertionError(f"invalid environment line: {raw_line}")
        if name in result:
            raise AssertionError(f"duplicate environment variable: {name}")
        result[name] = value
    return result


class StagingIsolationTests(unittest.TestCase):
    def test_environment_example_normalizes_to_the_frozen_staging_config(self) -> None:
        environment = parse_environment_example()
        config = normalize_managed_runtime_config(environment)

        self.assertEqual((), managed_runtime_config_invariant_issues(config))
        self.assertEqual("127.0.0.1", config["http_host"])
        self.assertEqual(18025, config["http_port"])
        self.assertEqual(18100, config["child_port_start"])
        self.assertEqual(18199, config["child_port_end"])
        self.assertEqual(
            "universal-managed-sprint-engine-staging", config["instance_id"]
        )
        self.assertTrue(config["disable_telegram"])
        self.assertTrue(config["disable_tunnel"])
        self.assertEqual(120, config["git_fetch_timeout_seconds"])

    def test_mutable_roots_are_siblings_not_service_children(self) -> None:
        config = normalize_managed_runtime_config(parse_environment_example())
        roots = [
            config["service_root"],
            config["runtime_root"],
            config["prompt_root"],
            config["managed_root"],
        ]

        for index, first in enumerate(roots):
            for second in roots[index + 1 :]:
                self.assertFalse(
                    windows_paths_overlap(first, second),
                    f"staging roots overlap: {first!r}, {second!r}",
                )

    def test_live_and_previous_work_roots_are_explicitly_protected(self) -> None:
        environment = parse_environment_example()
        protected = json.loads(environment["NGINX_QA_PROTECTED_ROOTS"])

        self.assertEqual(
            ["D:/nginx-qa", "D:/nginx-qa-umse", "D:/Prompt"], protected
        )
        for staging_name in (
            "NGINX_QA_SERVICE_ROOT",
            "NGINX_QA_RUNTIME_ROOT",
            "NGINX_QA_PROMPT_ROOT",
            "NGINX_QA_MANAGED_ROOT",
        ):
            for protected_root in protected:
                self.assertFalse(
                    windows_paths_overlap(environment[staging_name], protected_root)
                )

    def test_launcher_is_fail_closed_and_uses_the_staging_endpoint(self) -> None:
        launcher = LAUNCHER.read_text(encoding="utf-8")

        required_fragments = (
            '$expectedServiceRoot = "D:\\nginx-qa-staging\\universal-managed-sprint-engine"',
            '$expectedBranch = "agent/umse-07-staging-qualification"',
            '$expectedOrigin = "https://github.com/chartjs333/nginx-qa.git"',
            "status --porcelain=v1 --untracked-files=normal",
            "Test-PathOverlap",
            "ReparsePoint",
            "Get-NetTCPConnection -LocalPort 18025 -State Listen",
            "-m uvicorn main:app --host $env:NGINX_QA_HTTP_HOST --port $env:NGINX_QA_HTTP_PORT",
            "Remove-Item Env:TELEGRAM_BOT_TOKEN",
            'directory_template = "D:\\nginx-qa-staging\\prompt\\{repository}"',
        )
        for fragment in required_fragments:
            self.assertIn(fragment, launcher)

        for unsafe_fragment in (
            "python main.py",
            "configure_telegram.py",
            "cloudflared_quick_tunnel.ps1",
            "run.bat",
        ):
            self.assertNotIn(unsafe_fragment, launcher)

    def test_real_environment_file_stays_ignored_but_template_is_versioned(self) -> None:
        ignore_rules = (REPOSITORY_ROOT / ".gitignore").read_text(
            encoding="utf-8"
        )

        self.assertIn(".env.*", ignore_rules.splitlines())
        self.assertIn("!.env.staging.example", ignore_rules.splitlines())


if __name__ == "__main__":
    unittest.main()
