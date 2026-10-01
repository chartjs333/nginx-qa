import os
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest

from nginx_qa.git_provider import (
    GitCommandRunner,
    ManagedFileLock,
    ManagedGitError,
    ManagedGitProvider,
    RepositorySpec,
    canonical_remote_from_address,
)


class GitCommandRunnerTests(unittest.TestCase):
    def test_output_is_captured_bounded_and_redacted(self) -> None:
        secret = "credential-value-that-must-not-survive"
        runner = GitCommandRunner(executable=sys.executable, timeout=5)
        result = runner.run(
            (
                "-c",
                (
                    "import os,sys; "
                    "print(os.environ['TEST_CREDENTIAL']); "
                    "print('Bearer abcdefghijklmnop', file=sys.stderr)"
                ),
            ),
            environment={"TEST_CREDENTIAL": secret},
        )
        self.assertEqual(result.returncode, 0)
        self.assertNotIn(secret, result.stdout)
        self.assertIn("[REDACTED]", result.stdout)
        self.assertNotIn("abcdefghijklmnop", result.stderr)
        self.assertIn("[REDACTED]", result.stderr)

    def test_large_stdout_and_stderr_are_drained_with_bounded_retention(self) -> None:
        runner = GitCommandRunner(executable=sys.executable, timeout=10)
        result = runner.run(
            (
                "-c",
                (
                    "import sys; payload='x'*(4*1024*1024); "
                    "sys.stdout.write(payload); sys.stderr.write(payload)"
                ),
            )
        )
        self.assertLess(len(result.stdout), 70_000)
        self.assertLess(len(result.stderr), 70_000)
        self.assertIn("[OUTPUT TRUNCATED]", result.stdout)
        self.assertIn("[OUTPUT TRUNCATED]", result.stderr)

    def test_process_timeout_is_finite_and_structured(self) -> None:
        runner = GitCommandRunner(executable=sys.executable, timeout=0.1)
        started = time.monotonic()
        with self.assertRaises(ManagedGitError) as raised:
            runner.run(("-c", "import time; time.sleep(5)"))
        self.assertEqual(raised.exception.code, "GIT_COMMAND_TIMEOUT")
        self.assertLess(time.monotonic() - started, 3)

    def test_timeout_terminates_descendant_with_inherited_pipes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            marker = Path(temporary) / "descendant-survived.txt"
            child_code = (
                "import pathlib,sys,time; time.sleep(0.8); "
                "pathlib.Path(sys.argv[1]).write_text('alive', encoding='utf-8')"
            )
            parent_code = (
                "import subprocess,sys,time; "
                "subprocess.Popen([sys.executable, '-c', sys.argv[1], sys.argv[2]]); "
                "time.sleep(30)"
            )
            runner = GitCommandRunner(executable=sys.executable, timeout=0.1)
            started = time.monotonic()
            with self.assertRaises(ManagedGitError) as raised:
                runner.run(("-c", parent_code, child_code, str(marker)))
            self.assertEqual(raised.exception.code, "GIT_COMMAND_TIMEOUT")
            self.assertLess(time.monotonic() - started, 3)
            time.sleep(1)
            self.assertFalse(marker.exists())


class GitProviderValidationTests(unittest.TestCase):
    def test_remote_identity_normalization_matches_existing_rules(self) -> None:
        expected = "github.com/example/repository"
        for value in (
            "https://github.com/Example/Repository.git",
            "ssh://git@github.com/Example/Repository.git",
            "git@github.com:Example/Repository.git",
            "github.com/Example/Repository.git",
        ):
            with self.subTest(value=value):
                self.assertEqual(canonical_remote_from_address(value), expected)
        self.assertIsNone(
            canonical_remote_from_address(
                "https://user:password@github.com/example/repository.git"
            )
        )

    def test_local_transport_requires_explicit_test_provider(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            remote = base / "remote.git"
            remote.mkdir()
            spec = RepositorySpec(
                "repo", "example.invalid/acme/repo", str(remote)
            )
            provider = ManagedGitProvider(base / "managed", {"repo": spec})
            with self.assertRaises(ManagedGitError) as raised:
                provider.resolve_repository("repo")
            self.assertEqual(raised.exception.code, "REPOSITORY_TRANSPORT_INVALID")

            test_provider = ManagedGitProvider(
                base / "test-managed",
                {"repo": spec},
                allow_local_transport=True,
            )
            self.assertEqual(test_provider.resolve_repository("repo"), spec)

    def test_raw_token_disguised_as_reference_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            spec = RepositorySpec(
                "repo",
                "example.invalid/acme/repo",
                str(base / "remote.git"),
                "vault:ghp_abcdefghijklmnopqrstuvwxyz",
            )
            provider = ManagedGitProvider(
                base / "managed",
                {"repo": spec},
                allow_local_transport=True,
            )
            with self.assertRaises(ManagedGitError) as raised:
                provider.resolve_repository("repo")
            self.assertEqual(raised.exception.code, "CREDENTIAL_REFERENCE_INVALID")

    def test_managed_file_lock_serializes_threads(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            lock_path = Path(temporary) / "locks" / "repository.lock"
            entered: list[str] = []
            first_inside = threading.Event()
            release_first = threading.Event()

            def first() -> None:
                with ManagedFileLock(lock_path, timeout=3):
                    entered.append("first")
                    first_inside.set()
                    release_first.wait(2)

            def second() -> None:
                first_inside.wait(2)
                with ManagedFileLock(lock_path, timeout=3):
                    entered.append("second")

            first_thread = threading.Thread(target=first)
            second_thread = threading.Thread(target=second)
            first_thread.start()
            second_thread.start()
            self.assertTrue(first_inside.wait(2))
            time.sleep(0.05)
            self.assertEqual(entered, ["first"])
            release_first.set()
            first_thread.join(3)
            second_thread.join(3)
            self.assertFalse(first_thread.is_alive())
            self.assertFalse(second_thread.is_alive())
            self.assertEqual(entered, ["first", "second"])


if __name__ == "__main__":
    unittest.main()
