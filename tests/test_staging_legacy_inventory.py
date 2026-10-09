from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
LAUNCHER = REPOSITORY_ROOT / "run_staging.ps1"
POWERSHELL = shutil.which("powershell.exe")


def launcher_testable_functions() -> str:
    launcher = LAUNCHER.read_text(encoding="utf-8")
    begin = launcher.index("# BEGIN TESTABLE FUNCTIONS")
    end_marker = "# END TESTABLE FUNCTIONS"
    end = launcher.index(end_marker) + len(end_marker)
    return launcher[begin:end]


def powershell_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def run_powershell(body: str, *, timeout: int = 60) -> subprocess.CompletedProcess[str]:
    if POWERSHELL is None:
        raise unittest.SkipTest("Windows PowerShell is unavailable")
    script = "\n".join(
        (
            "Set-StrictMode -Version Latest",
            '$ErrorActionPreference = "Stop"',
            launcher_testable_functions(),
            textwrap.dedent(body),
        )
    )
    with tempfile.TemporaryDirectory(prefix="nginx-qa-legacy-inventory-ps-") as raw_temp:
        script_path = Path(raw_temp) / "test.ps1"
        script_path.write_text(script, encoding="utf-8-sig")
        return subprocess.run(
            [
                POWERSHELL,
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                str(script_path),
            ],
            cwd=REPOSITORY_ROOT,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )


def assert_powershell_success(
    test: unittest.TestCase, completed: subprocess.CompletedProcess[str]
) -> None:
    test.assertEqual(
        0,
        completed.returncode,
        f"PowerShell failed\nstdout:\n{completed.stdout}\nstderr:\n{completed.stderr}",
    )


@unittest.skipUnless(os.name == "nt", "PowerShell behavior is Windows-specific")
class StagingLegacyInventoryTests(unittest.TestCase):
    def test_staging_host_rebinds_every_legacy_mutable_path(self) -> None:
        with tempfile.TemporaryDirectory(prefix="nginx-qa-legacy-host-") as raw_temp:
            root = Path(raw_temp)
            state = root / "state"
            legacy = state / "legacy"
            legacy.mkdir(parents=True)
            (state / ".nginx-qa-staging-owner.json").write_text(
                "{}", encoding="utf-8"
            )
            environment = os.environ.copy()
            environment.update(
                {
                    "NGINX_QA_SERVICE_ROOT": str(REPOSITORY_ROOT),
                    "NGINX_QA_STAGING_STATE_BASE": str(state),
                    "NGINX_QA_PROTECTED_ROOTS": json.dumps([str(root / "live")]),
                }
            )
            script = """
import json
import staging_host_app as host

payload = {name: str(path) for name, path in host.STAGING_MUTABLE_PATHS.items()}
payload["runtime_state_directory"] = str(host._main.runtime_state_directory())
print(json.dumps(payload, sort_keys=True))
"""
            completed = subprocess.run(
                [sys.executable, "-B", "-c", script],
                cwd=REPOSITORY_ROOT,
                env=environment,
                capture_output=True,
                text=True,
                timeout=60,
                check=False,
            )
            self.assertEqual(
                0,
                completed.returncode,
                f"host import failed\nstdout:\n{completed.stdout}\nstderr:\n{completed.stderr}",
            )
            paths = json.loads(completed.stdout.strip().splitlines()[-1])
            expected_names = {
                "history_path",
                "git_config_path",
                "email_routes_path",
                "agents_path",
                "sprint_history_path",
                "pending_sprints_path",
                "specializations_path",
                "attachments_path",
                "screenshot_folders_path",
                "evidence_folders_path",
            }
            self.assertEqual(expected_names, set(paths) - {"runtime_state_directory"})
            for name in expected_names:
                self.assertEqual(legacy, Path(paths[name]).parent)
                self.assertFalse(Path(paths[name]).is_relative_to(REPOSITORY_ROOT))
            self.assertEqual(
                legacy / "runtime_state", Path(paths["runtime_state_directory"])
            )

    def test_staging_host_rejects_reparse_mutable_binding_before_import(self) -> None:
        with tempfile.TemporaryDirectory(prefix="nginx-qa-legacy-binding-") as raw_temp:
            root = Path(raw_temp)
            state = root / "state"
            legacy = state / "legacy"
            protected = root / "protected-live"
            binding = legacy / "attachments"
            legacy.mkdir(parents=True)
            protected.mkdir()
            (state / ".nginx-qa-staging-owner.json").write_text(
                "{}", encoding="utf-8"
            )
            created = run_powershell(
                f"""
                New-Item -ItemType Junction `
                    -Path {powershell_literal(binding)} `
                    -Target {powershell_literal(protected)} | Out-Null
                "junction created"
                """
            )
            assert_powershell_success(self, created)
            environment = os.environ.copy()
            environment.update(
                {
                    "NGINX_QA_SERVICE_ROOT": str(REPOSITORY_ROOT),
                    "NGINX_QA_STAGING_STATE_BASE": str(state),
                    "NGINX_QA_PROTECTED_ROOTS": json.dumps([str(protected)]),
                }
            )
            completed = subprocess.run(
                [sys.executable, "-B", "-c", "import staging_host_app"],
                cwd=REPOSITORY_ROOT,
                env=environment,
                capture_output=True,
                text=True,
                timeout=60,
                check=False,
            )
            self.assertNotEqual(0, completed.returncode)
            self.assertIn("reparse point", completed.stderr)
            self.assertFalse((protected / "conversation_log.jsonl").exists())

    def test_staging_host_rejects_nested_runtime_queue_junction(self) -> None:
        with tempfile.TemporaryDirectory(prefix="nginx-qa-legacy-queue-") as raw_temp:
            root = Path(raw_temp)
            state = root / "state"
            runtime = state / "legacy" / "runtime_state"
            protected = root / "protected-live-runtime"
            queue_binding = runtime / "queues"
            runtime.mkdir(parents=True)
            protected.mkdir()
            (state / ".nginx-qa-staging-owner.json").write_text(
                "{}", encoding="utf-8"
            )
            created = run_powershell(
                f"""
                New-Item -ItemType Junction `
                    -Path {powershell_literal(queue_binding)} `
                    -Target {powershell_literal(protected)} | Out-Null
                "queue junction created"
                """
            )
            assert_powershell_success(self, created)
            environment = os.environ.copy()
            environment.update(
                {
                    "NGINX_QA_SERVICE_ROOT": str(REPOSITORY_ROOT),
                    "NGINX_QA_STAGING_STATE_BASE": str(state),
                    "NGINX_QA_PROTECTED_ROOTS": json.dumps([str(protected)]),
                }
            )
            completed = subprocess.run(
                [sys.executable, "-B", "-c", "import staging_host_app"],
                cwd=REPOSITORY_ROOT,
                env=environment,
                capture_output=True,
                text=True,
                timeout=60,
                check=False,
            )
            self.assertNotEqual(0, completed.returncode)
            self.assertIn("reparse point", completed.stderr)
            self.assertEqual([], list(protected.iterdir()))

    def test_top_level_legacy_paths_remain_absent_after_ownership(self) -> None:
        with tempfile.TemporaryDirectory(prefix="nginx-qa-legacy-known-") as raw_temp:
            service = Path(raw_temp) / "service"
            runtime = service / "runtime_state"
            completed = run_powershell(
                f"""
                $service = {powershell_literal(service)}
                $runtime = {powershell_literal(runtime)}
                $expected = [ordered]@{{
                    directory_template = (Join-Path $service "prompt\\{{repository}}")
                    agent_latest_file_template = (Join-Path $service "prompt\\{{repository}}_{{agent_phone}}-latest.prompt")
                }}
                New-Item -ItemType Directory -Path $service | Out-Null

                Assert-LegacyStateInventory `
                    -ServiceRoot $service `
                    -LegacyRuntimeRoot $runtime `
                    -OwnershipEstablished $true `
                    -ExpectedPromptSettings $expected

                foreach ($name in @("agents.json", "attachments")) {{
                    $path = Join-Path $service $name
                    if ($name -eq "attachments") {{
                        New-Item -ItemType Directory -Path $path | Out-Null
                    }} else {{
                        [System.IO.File]::WriteAllText($path, "{{}}")
                    }}
                    foreach ($ownershipEstablished in @($false, $true)) {{
                        $rejected = $false
                        try {{
                            Assert-LegacyStateInventory `
                                -ServiceRoot $service `
                                -LegacyRuntimeRoot $runtime `
                                -OwnershipEstablished $ownershipEstablished `
                                -ExpectedPromptSettings $expected
                        }} catch {{
                            $rejected = $true
                        }}
                        if (-not $rejected) {{
                            throw "top-level legacy state was accepted: $name"
                        }}
                    }}
                    Remove-Item -LiteralPath $path -Recurse -Force
                }}
                "absent legacy inventory behavior OK"
                """
            )
            assert_powershell_success(self, completed)

    def test_runtime_inventory_and_prompt_schema_are_exact(self) -> None:
        with tempfile.TemporaryDirectory(prefix="nginx-qa-legacy-runtime-") as raw_temp:
            service = Path(raw_temp) / "service"
            runtime = service / "runtime_state"
            completed = run_powershell(
                f"""
                $service = {powershell_literal(service)}
                $runtime = {powershell_literal(runtime)}
                $settings = Join-Path $runtime "sequential-prompt-settings.json"
                $expected = [ordered]@{{
                    directory_template = (Join-Path $service "prompt\\{{repository}}")
                    agent_latest_file_template = (Join-Path $service "prompt\\{{repository}}_{{agent_phone}}-latest.prompt")
                }}
                New-Item -ItemType Directory -Path $runtime | Out-Null
                [System.IO.File]::WriteAllText(
                    $settings,
                    ([pscustomobject]$expected | ConvertTo-Json),
                    [System.Text.UTF8Encoding]::new($false)
                )

                Assert-LegacyStateInventory `
                    -ServiceRoot $service `
                    -LegacyRuntimeRoot $runtime `
                    -OwnershipEstablished $true `
                    -ExpectedPromptSettings $expected

                $extra = Join-Path $runtime "extra.json"
                [System.IO.File]::WriteAllText($extra, "{{}}")
                $rejected = $false
                try {{
                    Assert-LegacyStateInventory `
                        -ServiceRoot $service `
                        -LegacyRuntimeRoot $runtime `
                        -OwnershipEstablished $true `
                        -ExpectedPromptSettings $expected
                }} catch {{ $rejected = $true }}
                if (-not $rejected) {{ throw "extra runtime file was accepted" }}
                Remove-Item -LiteralPath $extra -Force

                $nested = Join-Path $runtime "nested"
                New-Item -ItemType Directory -Path $nested | Out-Null
                $rejected = $false
                try {{
                    Assert-LegacyStateInventory `
                        -ServiceRoot $service `
                        -LegacyRuntimeRoot $runtime `
                        -OwnershipEstablished $true `
                        -ExpectedPromptSettings $expected
                }} catch {{ $rejected = $true }}
                if (-not $rejected) {{ throw "nested runtime directory was accepted" }}
                Remove-Item -LiteralPath $nested -Force

                $tampered = [ordered]@{{
                    directory_template = $expected.directory_template
                    agent_latest_file_template = $expected.agent_latest_file_template
                    unexpected = "value"
                }}
                [System.IO.File]::WriteAllText(
                    $settings,
                    ([pscustomobject]$tampered | ConvertTo-Json),
                    [System.Text.UTF8Encoding]::new($false)
                )
                $rejected = $false
                try {{
                    Assert-LegacyStateInventory `
                        -ServiceRoot $service `
                        -LegacyRuntimeRoot $runtime `
                        -OwnershipEstablished $true `
                        -ExpectedPromptSettings $expected
                }} catch {{ $rejected = $true }}
                if (-not $rejected) {{ throw "extra prompt-settings key was accepted" }}
                "exact runtime inventory behavior OK"
                """
            )
            assert_powershell_success(self, completed)

    def test_stale_artifacts_and_reparse_runtime_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory(prefix="nginx-qa-legacy-unsafe-") as raw_temp:
            root = Path(raw_temp)
            service = root / "service"
            runtime = service / "runtime_state"
            target = root / "runtime-target"
            completed = run_powershell(
                f"""
                $service = {powershell_literal(service)}
                $runtime = {powershell_literal(runtime)}
                $target = {powershell_literal(target)}
                $expected = [ordered]@{{
                    directory_template = (Join-Path $service "prompt\\{{repository}}")
                    agent_latest_file_template = (Join-Path $service "prompt\\{{repository}}_{{agent_phone}}-latest.prompt")
                }}
                New-Item -ItemType Directory -Path $service | Out-Null
                foreach ($name in @(
                    "queue_backup_before_restart_case.json",
                    ".port_git_map.json.case.tmp",
                    ".pending_project_sprints.json.case.tmp"
                )) {{
                    $artifact = Join-Path $service $name
                    [System.IO.File]::WriteAllText($artifact, "{{}}")
                    $rejected = $false
                    try {{
                        Assert-LegacyStateInventory `
                            -ServiceRoot $service `
                            -LegacyRuntimeRoot $runtime `
                            -OwnershipEstablished $true `
                            -ExpectedPromptSettings $expected
                    }} catch {{ $rejected = $true }}
                    if (-not $rejected) {{ throw "stale artifact was accepted: $name" }}
                    Remove-Item -LiteralPath $artifact -Force
                }}

                New-Item -ItemType Directory -Path $target | Out-Null
                try {{
                    New-Item -ItemType Junction -Path $runtime -Target $target | Out-Null
                    $rejected = $false
                    try {{
                        Assert-LegacyStateInventory `
                            -ServiceRoot $service `
                            -LegacyRuntimeRoot $runtime `
                            -OwnershipEstablished $true `
                            -ExpectedPromptSettings $expected
                    }} catch {{ $rejected = $true }}
                    if (-not $rejected) {{ throw "reparse runtime root was accepted" }}
                }} finally {{
                    if (Test-Path -LiteralPath $runtime) {{
                        Remove-Item -LiteralPath $runtime -Force
                    }}
                }}
                "unsafe legacy inventory behavior OK"
                """
            )
            assert_powershell_success(self, completed)


if __name__ == "__main__":
    unittest.main()
