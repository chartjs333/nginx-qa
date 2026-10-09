from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

from nginx_qa.managed_import import normalize_managed_runtime_config
from nginx_qa.sprint_types import (
    managed_runtime_config_invariant_issues,
    windows_paths_overlap,
)


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
ENVIRONMENT_EXAMPLE = REPOSITORY_ROOT / ".env.staging.example"
PREMERGE_ENVIRONMENT_EXAMPLE = (
    REPOSITORY_ROOT
    / "orchestration"
    / "sprints"
    / "premerge-inbound-hub-e2e-v1"
    / "staging-18027.env.example"
)
SHORT_PREMERGE_ENVIRONMENT_EXAMPLE = (
    REPOSITORY_ROOT
    / "orchestration"
    / "sprints"
    / "premerge-inbound-hub-e2e-v1"
    / "staging-18030-short.env.example"
)
LAUNCHER = REPOSITORY_ROOT / "run_staging.ps1"
E2E_MANIFEST = (
    REPOSITORY_ROOT
    / "orchestration"
    / "sprints"
    / "universal-managed-sprint-engine"
    / "staging-e2e-manifest.json"
)
LIVE_SNAPSHOT_TOOL = (
    REPOSITORY_ROOT
    / "orchestration"
    / "sprints"
    / "universal-managed-sprint-engine"
    / "capture_live_snapshot.ps1"
)
STAGING_PYTHON = (
    "C:/nginx-qa-staging-state/umse-007/.venv/Scripts/python.exe"
)
POWERSHELL = shutil.which("powershell.exe")


def parse_environment_file(path: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
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


def parse_environment_example() -> dict[str, str]:
    return parse_environment_file(ENVIRONMENT_EXAMPLE)


def launcher_testable_functions() -> str:
    launcher = LAUNCHER.read_text(encoding="utf-8")
    begin = launcher.index("# BEGIN TESTABLE FUNCTIONS")
    end_marker = "# END TESTABLE FUNCTIONS"
    end = launcher.index(end_marker) + len(end_marker)
    return launcher[begin:end]


def snapshot_testable_functions() -> str:
    snapshot_tool = LIVE_SNAPSHOT_TOOL.read_text(encoding="utf-8")
    begin = snapshot_tool.index("# BEGIN TESTABLE SNAPSHOT FUNCTIONS")
    end_marker = "# END TESTABLE SNAPSHOT FUNCTIONS"
    end = snapshot_tool.index(end_marker) + len(end_marker)
    return snapshot_tool[begin:end]


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
    with tempfile.TemporaryDirectory(prefix="nginx-qa-staging-ps-") as raw_temp:
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


def run_snapshot_powershell(
    body: str, *, timeout: int = 60
) -> subprocess.CompletedProcess[str]:
    if POWERSHELL is None:
        raise unittest.SkipTest("Windows PowerShell is unavailable")
    script = "\n".join(
        (
            "Set-StrictMode -Version Latest",
            '$ErrorActionPreference = "Stop"',
            snapshot_testable_functions(),
            textwrap.dedent(body),
        )
    )
    with tempfile.TemporaryDirectory(prefix="nginx-qa-snapshot-ps-") as raw_temp:
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

    @unittest.skipUnless(os.name == "nt", "PowerShell behavior is Windows-specific")
    def test_premerge_inbound_hub_profile_is_isolated_and_valid(self) -> None:
        completed = run_powershell(
            r"""
            Get-ExpectedStagingEnvironment `
                -Profile "premerge-inbound-hub-e2e-v1" |
                ConvertTo-Json -Compress
            """
        )
        assert_powershell_success(self, completed)
        environment = json.loads(completed.stdout.strip().splitlines()[-1])
        self.assertEqual(
            parse_environment_file(PREMERGE_ENVIRONMENT_EXAMPLE),
            environment,
        )
        config = normalize_managed_runtime_config(environment)

        self.assertEqual((), managed_runtime_config_invariant_issues(config))
        self.assertEqual(18027, config["http_port"])
        self.assertEqual(18300, config["child_port_start"])
        self.assertEqual(18399, config["child_port_end"])
        self.assertEqual(
            "premerge-inbound-hub-e2e-v1-staging-18027",
            config["instance_id"],
        )
        self.assertEqual(
            Path("D:/nginx-qa-staging/premerge-inbound-hub-e2e-v1"),
            Path(config["service_root"]),
        )
        state_base = Path(environment["NGINX_QA_STAGING_STATE_BASE"])
        for name in (
            "NGINX_QA_STAGING_VENV_ROOT",
            "NGINX_QA_RUNTIME_ROOT",
            "NGINX_QA_PROMPT_ROOT",
            "NGINX_QA_MANAGED_ROOT",
        ):
            self.assertEqual(state_base, Path(environment[name]).parent)
        for protected_root in json.loads(environment["NGINX_QA_PROTECTED_ROOTS"]):
            self.assertFalse(
                windows_paths_overlap(config["service_root"], protected_root)
            )
            self.assertFalse(
                windows_paths_overlap(str(state_base), protected_root)
            )

    @unittest.skipUnless(os.name == "nt", "PowerShell behavior is Windows-specific")
    def test_short_premerge_profile_is_isolated_and_valid(self) -> None:
        completed = run_powershell(
            r"""
            Get-ExpectedStagingEnvironment `
                -Profile "premerge-inbound-hub-e2e-v1-short" |
                ConvertTo-Json -Compress
            """
        )
        assert_powershell_success(self, completed)
        environment = json.loads(completed.stdout.strip().splitlines()[-1])
        self.assertEqual(
            parse_environment_file(SHORT_PREMERGE_ENVIRONMENT_EXAMPLE),
            environment,
        )
        config = normalize_managed_runtime_config(environment)

        self.assertEqual((), managed_runtime_config_invariant_issues(config))
        self.assertEqual(18030, config["http_port"])
        self.assertEqual(18400, config["child_port_start"])
        self.assertEqual(18499, config["child_port_end"])
        self.assertEqual(
            "premerge-inbound-hub-e2e-v1-short-18030-01",
            config["instance_id"],
        )
        self.assertEqual(Path("D:/nq-e2e-18027"), Path(config["service_root"]))
        state_base = Path(environment["NGINX_QA_STAGING_STATE_BASE"])
        for name in (
            "NGINX_QA_STAGING_VENV_ROOT",
            "NGINX_QA_RUNTIME_ROOT",
            "NGINX_QA_PROMPT_ROOT",
            "NGINX_QA_MANAGED_ROOT",
        ):
            self.assertEqual(state_base, Path(environment[name]).parent)
        for protected_root in json.loads(environment["NGINX_QA_PROTECTED_ROOTS"]):
            self.assertFalse(
                windows_paths_overlap(config["service_root"], protected_root)
            )
            self.assertFalse(
                windows_paths_overlap(str(state_base), protected_root)
            )

    @unittest.skipUnless(os.name == "nt", "PowerShell behavior is Windows-specific")
    def test_launcher_rejects_each_mutable_boundary_override(self) -> None:
        completed = run_powershell(
            r"""
            $expected = Get-ExpectedStagingEnvironment
            foreach ($entry in $expected.GetEnumerator()) {
                [System.Environment]::SetEnvironmentVariable(
                    [string]$entry.Key,
                    [string]$entry.Value,
                    [System.EnvironmentVariableTarget]::Process
                )
            }
            Assert-ExactStagingEnvironment -ExpectedValues $expected
            $mutableNames = @(
                "NGINX_QA_STAGING_STATE_BASE",
                "NGINX_QA_STAGING_VENV_ROOT",
                "NGINX_QA_STAGING_BASE_PYTHON",
                "NGINX_QA_RUNTIME_ROOT",
                "NGINX_QA_PROMPT_ROOT",
                "NGINX_QA_MANAGED_ROOT"
            )
            foreach ($name in $mutableNames) {
                $original = [string]$expected[$name]
                [System.Environment]::SetEnvironmentVariable(
                    $name,
                    ($original + "-tampered"),
                    [System.EnvironmentVariableTarget]::Process
                )
                $rejected = $false
                try {
                    Assert-ExactStagingEnvironment -ExpectedValues $expected
                }
                catch {
                    $rejected = $true
                }
                if (-not $rejected) {
                    throw "Mutable boundary override was accepted: $name"
                }
                [System.Environment]::SetEnvironmentVariable(
                    $name,
                    $original,
                    [System.EnvironmentVariableTarget]::Process
                )
            }
            $expected | ConvertTo-Json -Compress
            """
        )
        assert_powershell_success(self, completed)
        self.assertEqual(
            parse_environment_example(), json.loads(completed.stdout.strip().splitlines()[-1])
        )

    def test_mutable_roots_are_dedicated_state_base_children(self) -> None:
        environment = parse_environment_example()
        config = normalize_managed_runtime_config(environment)
        state_base = Path(environment["NGINX_QA_STAGING_STATE_BASE"])
        mutable_roots = [
            Path(environment["NGINX_QA_STAGING_VENV_ROOT"]),
            Path(config["runtime_root"]),
            Path(config["prompt_root"]),
            Path(config["managed_root"]),
        ]

        self.assertEqual("umse-007", state_base.name)
        self.assertNotEqual("C:/nginx-qa-staging-state", state_base.as_posix())
        for root in mutable_roots:
            self.assertEqual(state_base, root.parent)
            self.assertFalse(windows_paths_overlap(config["service_root"], str(root)))
        for index, first in enumerate(mutable_roots):
            for second in mutable_roots[index + 1 :]:
                self.assertFalse(windows_paths_overlap(str(first), str(second)))

    def test_live_and_previous_work_roots_are_explicitly_protected(self) -> None:
        environment = parse_environment_example()
        protected = json.loads(environment["NGINX_QA_PROTECTED_ROOTS"])

        self.assertEqual(
            ["D:/nginx-qa", "D:/nginx-qa-umse", "D:/Prompt"], protected
        )
        for staging_name in (
            "NGINX_QA_SERVICE_ROOT",
            "NGINX_QA_STAGING_STATE_BASE",
            "NGINX_QA_STAGING_VENV_ROOT",
            "NGINX_QA_RUNTIME_ROOT",
            "NGINX_QA_PROMPT_ROOT",
            "NGINX_QA_MANAGED_ROOT",
        ):
            for protected_root in protected:
                self.assertFalse(
                    windows_paths_overlap(environment[staging_name], protected_root)
                )

    def test_launcher_contains_all_fail_closed_gates(self) -> None:
        launcher = LAUNCHER.read_text(encoding="utf-8")

        required_fragments = (
            '$expectedServiceRoot = "D:\\nginx-qa-staging\\universal-managed-sprint-engine"',
            '$expectedBranch = "agent/umse-07-staging-qualification"',
            '$expectedOrigin = "https://github.com/chartjs333/nginx-qa.git"',
            "status --porcelain=v1 --untracked-files=all",
            "ls-remote --exit-code origin",
            "ExpectedCommit",
            "ReparsePoint",
            "[System.Security.Cryptography.SHA256]::Create()",
            "base_python_version",
            "base_python_sha256",
            "initialization_id",
            "First-use staging state base must be empty",
            "Unknown legacy staging state exists before ownership initialization",
            "Get-NetTCPConnection -State Listen",
            "port_owner_pids(host, int(port))",
            "job.contains(listener_pid)",
            "owned.append(",
            '"leader_pid": pid',
            '"pid": int(listener_pid)',
            "ManagedWorkspaceManager.validate_isolated_root",
            "-m uvicorn staging_host_app:app",
        )
        for fragment in required_fragments:
            self.assertIn(fragment, launcher)

        for unsafe_fragment in (
            "python main.py",
            "configure_telegram.py",
            "cloudflared_quick_tunnel.ps1",
            "run.bat",
            "or identity.cwd is None",
        ):
            self.assertNotIn(unsafe_fragment, launcher)

        self.assertIn("identity.cwd is not None", launcher)

    @unittest.skipUnless(os.name == "nt", "PowerShell behavior is Windows-specific")
    def test_ownership_marker_is_atomic_random_and_exact(self) -> None:
        with tempfile.TemporaryDirectory(prefix="nginx-qa-owner-") as raw_temp:
            root = Path(raw_temp)
            base_a = root / "state-a"
            base_b = root / "state-b"
            unknown = root / "unknown"
            completed = run_powershell(
                f"""
                $baseA = {powershell_literal(base_a)}
                $baseB = {powershell_literal(base_b)}
                $unknown = {powershell_literal(unknown)}
                $markerA = Join-Path $baseA ".nginx-qa-staging-owner.json"
                $markerB = Join-Path $baseB ".nginx-qa-staging-owner.json"
                $expectedA = [ordered]@{{
                    instance_id = "isolation-test"
                    state_base = Resolve-NormalizedPath $baseA
                    base_python_sha256 = ("a" * 64)
                }}
                $expectedB = [ordered]@{{
                    instance_id = "isolation-test"
                    state_base = Resolve-NormalizedPath $baseB
                    base_python_sha256 = ("a" * 64)
                }}

                $first = Get-OwnershipState -StateBase $baseA -MarkerPath $markerA -ExpectedRecord $expectedA
                if ($first.Mode -cne "FirstUse" -or $null -ne $first.InitializationId) {{
                    throw "unexpected first-use state"
                }}
                $restartA = Initialize-OwnershipMarker -StateBase $baseA -MarkerPath $markerA -ExpectedRecord $expectedA
                $restartB = Initialize-OwnershipMarker -StateBase $baseB -MarkerPath $markerB -ExpectedRecord $expectedB
                if ($restartA.Mode -cne "Restart" -or $restartB.Mode -cne "Restart") {{
                    throw "ownership was not established"
                }}
                if ($restartA.InitializationId -ceq $restartB.InitializationId) {{
                    throw "initialization IDs were reused"
                }}
                $parsed = [guid]::Empty
                if (-not [guid]::TryParseExact($restartA.InitializationId, "D", [ref]$parsed)) {{
                    throw "initialization ID is not a canonical GUID"
                }}
                if (@(Get-ChildItem -LiteralPath $baseA -Filter "*.tmp" -Force).Count -ne 0) {{
                    throw "atomic marker left a temporary file"
                }}
                $again = Get-OwnershipState -StateBase $baseA -MarkerPath $markerA -ExpectedRecord $expectedA
                if ($again.InitializationId -cne $restartA.InitializationId) {{
                    throw "restart changed initialization identity"
                }}

                $changed = [ordered]@{{
                    instance_id = "isolation-test"
                    state_base = Resolve-NormalizedPath $baseA
                    base_python_sha256 = ("b" * 64)
                }}
                $rejected = $false
                try {{
                    $null = Get-OwnershipState -StateBase $baseA -MarkerPath $markerA -ExpectedRecord $changed
                }} catch {{
                    $rejected = $true
                }}
                if (-not $rejected) {{ throw "changed static ownership was accepted" }}

                New-Item -ItemType Directory -Path $unknown | Out-Null
                [System.IO.File]::WriteAllText((Join-Path $unknown "unknown.txt"), "x")
                $rejected = $false
                try {{
                    $null = Get-OwnershipState `
                        -StateBase $unknown `
                        -MarkerPath (Join-Path $unknown ".nginx-qa-staging-owner.json") `
                        -ExpectedRecord $expectedA
                }} catch {{
                    $rejected = $true
                }}
                if (-not $rejected) {{ throw "unowned non-empty base was adopted" }}

                $tampered = Get-Content -LiteralPath $markerA -Raw | ConvertFrom-Json
                $tampered.initialization_id = "not-a-guid"
                [System.IO.File]::WriteAllText(
                    $markerA,
                    ($tampered | ConvertTo-Json -Depth 12),
                    [System.Text.UTF8Encoding]::new($false)
                )
                $rejected = $false
                try {{
                    $null = Get-OwnershipState -StateBase $baseA -MarkerPath $markerA -ExpectedRecord $expectedA
                }} catch {{
                    $rejected = $true
                }}
                if (-not $rejected) {{ throw "invalid initialization ID was accepted" }}
                "ownership behavior OK"
                """
            )
            assert_powershell_success(self, completed)

    @unittest.skipUnless(os.name == "nt", "PowerShell behavior is Windows-specific")
    def test_canonical_guard_rejects_a_junction(self) -> None:
        with tempfile.TemporaryDirectory(prefix="nginx-qa-reparse-") as raw_temp:
            root = Path(raw_temp)
            target = root / "target"
            junction = root / "junction"
            completed = run_powershell(
                f"""
                $target = {powershell_literal(target)}
                $junction = {powershell_literal(junction)}
                New-Item -ItemType Directory -Path $target | Out-Null
                try {{
                    New-Item -ItemType Junction -Path $junction -Target $target | Out-Null
                    $rejected = $false
                    try {{
                        $null = Assert-CanonicalNonReparsePath -Path $junction -PathKind Container
                    }} catch {{
                        $rejected = $true
                    }}
                    if (-not $rejected) {{ throw "junction was accepted" }}
                }} finally {{
                    if (Test-Path -LiteralPath $junction) {{
                        Remove-Item -LiteralPath $junction -Force
                    }}
                }}
                "reparse behavior OK"
                """
            )
            assert_powershell_success(self, completed)

    @unittest.skipUnless(os.name == "nt", "PowerShell behavior is Windows-specific")
    def test_base_interpreter_provenance_includes_version_and_hash(self) -> None:
        environment = parse_environment_example()
        base_python = Path(environment["NGINX_QA_STAGING_BASE_PYTHON"])
        if not base_python.is_file():
            self.skipTest(f"configured base Python is unavailable: {base_python}")
        with tempfile.TemporaryDirectory(prefix="nginx-qa-python-proof-") as raw_temp:
            completed = run_powershell(
                f"""
                $probe = Assert-TrustedBaseInterpreter `
                    -PythonPath {powershell_literal(base_python)} `
                    -ProtectedRoots @("D:\\nginx-qa", "D:\\nginx-qa-umse") `
                    -ForbiddenRoots @({powershell_literal(raw_temp)})
                if ($probe.version -notmatch '^\\d+\\.\\d+\\.\\d+') {{
                    throw "missing interpreter version"
                }}
                if ($probe.sha256 -notmatch '^[0-9a-f]{{64}}$') {{
                    throw "missing interpreter SHA256"
                }}
                $rejected = $false
                try {{
                    $null = Assert-TrustedBaseInterpreter `
                        -PythonPath {powershell_literal(base_python)} `
                        -ProtectedRoots @($probe.base_prefix) `
                        -ForbiddenRoots @({powershell_literal(raw_temp)})
                }} catch {{
                    $rejected = $true
                }}
                if (-not $rejected) {{ throw "protected interpreter was accepted" }}
                "interpreter provenance OK"
                """
            )
            assert_powershell_success(self, completed)

    @unittest.skipUnless(os.name == "nt", "PowerShell behavior is Windows-specific")
    def test_git_checkout_requires_standalone_directory_and_exact_remote_sha(self) -> None:
        with tempfile.TemporaryDirectory(prefix="nginx-qa-git-proof-") as raw_temp:
            root = Path(raw_temp)
            remote = root / "remote.git"
            checkout = root / "checkout"
            indirected = root / "indirected"

            def git(*arguments: str, cwd: Path | None = None) -> str:
                result = subprocess.run(
                    ["git", *arguments],
                    cwd=cwd,
                    capture_output=True,
                    text=True,
                    check=True,
                )
                return result.stdout.strip()

            git("init", "--bare", str(remote))
            git("init", str(checkout))
            git("config", "user.name", "Isolation Test", cwd=checkout)
            git("config", "user.email", "isolation@example.invalid", cwd=checkout)
            git("checkout", "-b", "staging", cwd=checkout)
            (checkout / "tracked.txt").write_text("proof\n", encoding="utf-8")
            git("add", "tracked.txt", cwd=checkout)
            git("commit", "-m", "proof", cwd=checkout)
            origin = remote.as_posix()
            git("remote", "add", "origin", origin, cwd=checkout)
            git("push", "-u", "origin", "staging", cwd=checkout)
            commit = git("rev-parse", "HEAD", cwd=checkout)
            indirected.mkdir()
            (indirected / ".git").write_text(
                f"gitdir: {(checkout / '.git').as_posix()}\n", encoding="utf-8"
            )

            completed = run_powershell(
                f"""
                $actual = Assert-StandaloneGitCheckout `
                    -ServiceRoot {powershell_literal(checkout)} `
                    -ExpectedOrigin {powershell_literal(origin)} `
                    -ExpectedBranch "staging" `
                    -ExpectedCommit "{commit}"
                if ($actual -cne "{commit}") {{ throw "exact commit was not returned" }}

                $dirtyPath = Join-Path {powershell_literal(checkout)} "dirty-untracked.txt"
                [System.IO.File]::WriteAllText($dirtyPath, "dirty")
                $rejected = $false
                try {{
                    $null = Assert-StandaloneGitCheckout `
                        -ServiceRoot {powershell_literal(checkout)} `
                        -ExpectedOrigin {powershell_literal(origin)} `
                        -ExpectedBranch "staging" `
                        -ExpectedCommit "{commit}"
                }} catch {{
                    $rejected = $true
                }} finally {{
                    [System.IO.File]::Delete($dirtyPath)
                }}
                if (-not $rejected) {{ throw "dirty checkout was accepted" }}

                $rejected = $false
                try {{
                    $null = Assert-StandaloneGitCheckout `
                        -ServiceRoot {powershell_literal(checkout)} `
                        -ExpectedOrigin {powershell_literal(origin)} `
                        -ExpectedBranch "staging" `
                        -ExpectedCommit ("0" * 40)
                }} catch {{
                    $rejected = $true
                }}
                if (-not $rejected) {{ throw "wrong approved SHA was accepted" }}

                $rejected = $false
                try {{
                    $null = Assert-StandaloneGitCheckout `
                        -ServiceRoot {powershell_literal(indirected)} `
                        -ExpectedOrigin {powershell_literal(origin)} `
                        -ExpectedBranch "staging" `
                        -ExpectedCommit "{commit}"
                }} catch {{
                    $rejected = $true
                }}
                if (-not $rejected) {{ throw ".git indirection was accepted" }}
                "Git provenance OK"
                """
            )
            assert_powershell_success(self, completed)

            (checkout / "tracked.txt").write_text("diverged\n", encoding="utf-8")
            git("add", "tracked.txt", cwd=checkout)
            git("commit", "-m", "local divergence", cwd=checkout)
            diverged_commit = git("rev-parse", "HEAD", cwd=checkout)
            completed = run_powershell(
                f"""
                $rejected = $false
                try {{
                    $null = Assert-StandaloneGitCheckout `
                        -ServiceRoot {powershell_literal(checkout)} `
                        -ExpectedOrigin {powershell_literal(origin)} `
                        -ExpectedBranch "staging" `
                        -ExpectedCommit "{diverged_commit}"
                }} catch {{
                    $rejected = $true
                }}
                if (-not $rejected) {{ throw "remote-diverged checkout was accepted" }}
                "Git divergence behavior OK"
                """
            )
            assert_powershell_success(self, completed)

    @unittest.skipUnless(os.name == "nt", "PowerShell behavior is Windows-specific")
    def test_full_port_gate_uses_actual_listener_pid_not_durable_leader(self) -> None:
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        http_guard = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            listener.bind(("127.0.0.1", 0))
            listener.listen(1)
            child_port = listener.getsockname()[1]
            http_guard.bind(("127.0.0.1", 0))
            http_port = http_guard.getsockname()[1]
            owner_pid = os.getpid()
            leader_pid = owner_pid + 1_000_000

            completed = run_powershell(
                f"""
                $rejected = $false
                try {{
                    Assert-PortPool -HttpPort {http_port} -ChildStart {child_port} -ChildEnd {child_port}
                }} catch {{
                    $rejected = $true
                }}
                if (-not $rejected) {{ throw "unknown listener was accepted" }}

                $records = @([pscustomobject]@{{
                    port = {child_port}
                    pid = {owner_pid}
                    leader_pid = {leader_pid}
                    process_id = "redirector-proof"
                    job_object_id = "Global\\redirector-proof"
                }})
                $allowed = ConvertTo-AllowedChildOwnerMap -AuthenticatedListeners $records
                $entry = $allowed["{child_port}/{owner_pid}"]
                if ($entry.OwnerPid -ne {owner_pid} -or $entry.LeaderPid -ne {leader_pid}) {{
                    throw "listener/leader identity was collapsed"
                }}
                if ($entry.OwnerPid -eq $entry.LeaderPid) {{
                    throw "test did not exercise redirector descendant ownership"
                }}
                $emptyRecords = @(
                    ConvertFrom-AuthenticatedListenerJson -Json "[]"
                )
                if ($emptyRecords.Count -ne 0) {{
                    throw "empty listener JSON did not stay empty"
                }}
                $emptyAllowed = ConvertTo-AllowedChildOwnerMap `
                    -AuthenticatedListeners $emptyRecords
                if ($emptyAllowed.Count -ne 0) {{
                    throw "empty listener JSON produced an allowed owner"
                }}
                Assert-PortPool `
                    -HttpPort {http_port} `
                    -ChildStart {child_port} `
                    -ChildEnd {child_port} `
                    -AllowedChildOwners $allowed
                "listener-owner behavior OK"
                """
            )
            assert_powershell_success(self, completed)
        finally:
            http_guard.close()
            listener.close()

    def test_real_environment_file_stays_ignored_but_template_is_versioned(self) -> None:
        ignore_rules = (REPOSITORY_ROOT / ".gitignore").read_text(
            encoding="utf-8"
        )

        self.assertIn(".env.*", ignore_rules.splitlines())
        self.assertIn("!.env.staging.example", ignore_rules.splitlines())

    def test_e2e_children_use_the_owned_staging_interpreter(self) -> None:
        manifest = json.loads(E2E_MANIFEST.read_text(encoding="utf-8"))
        commands = [
            node["workspace"]["process"]["command"]
            for node in manifest["nodes"]
            if "process" in node.get("workspace", {})
        ]

        self.assertEqual(4, len(commands))
        self.assertEqual({STAGING_PYTHON}, {command[0] for command in commands})

    def test_live_snapshot_tool_is_read_only_and_fail_closed(self) -> None:
        snapshot_tool = LIVE_SNAPSHOT_TOOL.read_text(encoding="utf-8")

        required_fragments = (
            "Get-NetTCPConnection -State Listen -LocalPort $LivePort",
            "$listeners.Count -ne 1",
            "$listeners[0].LocalAddress -cne '0.0.0.0'",
            "GIT_OPTIONAL_LOCKS",
            "'symbolic-ref',",
            "$LiveHealthUri = 'http://127.0.0.1:8025/'",
            "Add-Type -AssemblyName System.Net.Http",
            "[System.Net.Http.HttpClientHandler]::new()",
            "$handler.AllowAutoRedirect = $false",
            "$handler.UseProxy = $false",
            "[System.Net.Http.HttpCompletionOption]::ResponseHeadersRead",
            "schema_version = 2",
            "health         = $health",
            ".nginx-qa-staging-owner.json",
            "Assert-SnapshotCanonicalNonReparsePath",
            "Assert-SafeSnapshotLeafName",
            "Refusing to replace an existing snapshot",
        )
        for fragment in required_fragments:
            self.assertIn(fragment, snapshot_tool)
        for forbidden_live_fragment in (
            "runtime_state",
            "pending_project_sprints.json",
            "project_sprints.json",
            "agents.json",
            "conversation_log.jsonl",
            "port_git_map.json",
            "Get-ChildItem",
            "--untracked-files",
            "Set-Content -LiteralPath $ResolvedLiveRoot",
            "Out-File -LiteralPath $ResolvedLiveRoot",
            "Remove-Item -LiteralPath $ResolvedLiveRoot",
            "Invoke-WebRequest",
            "Invoke-RestMethod",
        ):
            self.assertNotIn(forbidden_live_fragment, snapshot_tool)

    @unittest.skipUnless(os.name == "nt", "PowerShell behavior is Windows-specific")
    def test_snapshot_output_rejects_junction_ancestor_before_write(self) -> None:
        with tempfile.TemporaryDirectory(prefix="nginx-qa-snapshot-junction-") as raw_temp:
            root = Path(raw_temp)
            state_base = root / "state"
            evidence_root = state_base / "evidence"
            junction_target = root / "redirect-target"
            output = evidence_root / "capture.json"
            completed = run_snapshot_powershell(
                f"""
                $stateBase = {powershell_literal(state_base)}
                $evidenceRoot = {powershell_literal(evidence_root)}
                $junctionTarget = {powershell_literal(junction_target)}
                $output = {powershell_literal(output)}
                New-Item -ItemType Directory -Path $stateBase | Out-Null
                New-Item -ItemType Directory -Path $junctionTarget | Out-Null
                [System.IO.File]::WriteAllText(
                    (Join-Path $stateBase ".nginx-qa-staging-owner.json"),
                    "{{}}"
                )
                try {{
                    New-Item `
                        -ItemType Junction `
                        -Path $evidenceRoot `
                        -Target $junctionTarget | Out-Null
                    $rejected = $false
                    try {{
                        $null = Initialize-SnapshotOutputBoundary `
                            -RequestedOutput $output `
                            -StateBase $stateBase `
                            -EvidenceRoot $evidenceRoot
                    }} catch {{
                        $rejected = $true
                    }}
                    if (-not $rejected) {{
                        throw "junction output ancestor was accepted"
                    }}
                    if (Test-Path -LiteralPath (Join-Path $junctionTarget "capture.json")) {{
                        throw "snapshot was written through a junction"
                    }}
                }} finally {{
                    if (Test-Path -LiteralPath $evidenceRoot) {{
                        Remove-Item -LiteralPath $evidenceRoot -Force
                    }}
                }}
                "snapshot junction behavior OK"
                """
            )
            assert_powershell_success(self, completed)

    @unittest.skipUnless(os.name == "nt", "PowerShell behavior is Windows-specific")
    def test_snapshot_output_rejects_ads_device_and_unsafe_leaf_before_write(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory(prefix="nginx-qa-snapshot-leaf-") as raw_temp:
            root = Path(raw_temp)
            state_base = root / "state"
            evidence_root = state_base / "evidence"
            ads_output = str(evidence_root / "capture.json") + ":secret"
            reserved_output = evidence_root / "NUL.json"
            completed = run_snapshot_powershell(
                f"""
                $stateBase = {powershell_literal(state_base)}
                $evidenceRoot = {powershell_literal(evidence_root)}
                New-Item -ItemType Directory -Path $stateBase | Out-Null
                [System.IO.File]::WriteAllText(
                    (Join-Path $stateBase ".nginx-qa-staging-owner.json"),
                    "{{}}"
                )
                $unsafeOutputs = @(
                    {powershell_literal(ads_output)},
                    {powershell_literal(reserved_output)},
                    "\\\\?\\C:\\snapshot.json",
                    "\\\\server\\share\\snapshot.json"
                )
                foreach ($unsafeOutput in $unsafeOutputs) {{
                    $rejected = $false
                    try {{
                        $null = Initialize-SnapshotOutputBoundary `
                            -RequestedOutput $unsafeOutput `
                            -StateBase $stateBase `
                            -EvidenceRoot $evidenceRoot
                    }} catch {{
                        $rejected = $true
                    }}
                    if (-not $rejected) {{
                        throw "unsafe output was accepted: $unsafeOutput"
                    }}
                }}
                if (Test-Path -LiteralPath $evidenceRoot) {{
                    throw "unsafe output validation performed a write"
                }}
                "snapshot leaf behavior OK"
                """
            )
            assert_powershell_success(self, completed)


if __name__ == "__main__":
    unittest.main()
