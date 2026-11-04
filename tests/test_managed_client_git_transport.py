"""Actual Windows git:// publication in a disposable loopback-only daemon."""
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import threading
import time
import unittest
import uuid

from nginx_qa.control_secrets import sanitized_child_environment
from nginx_qa.managed_codex_client import ManagedCodexClient
from nginx_qa.process_supervisor import _WindowsJob, _spawn_suspended_in_windows_job


@unittest.skipUnless(os.name == "nt", "Git for Windows side-band regression")
class GitDaemonPublication(unittest.TestCase):
    def test_actual_git_protocol_push_with_owned_jobs_and_exact_remote_commit(self):
        git = shutil.which("git")
        with tempfile.TemporaryDirectory(prefix="nq-git-transport-") as temporary:
            base = Path(temporary)
            repo, bare = base / "repo", base / "remote.git"
            def command(*args):
                return subprocess.run([git, *map(str, args)], capture_output=True,
                    check=True, timeout=10).stdout.decode().strip()
            command("init", repo)
            command("init", "--bare", bare)
            command("-C", repo, "config", "user.name", "Fixture")
            command("-C", repo, "config", "user.email", "fixture@example.invalid")
            (repo / "README.md").write_text("Disposable network transport fixture.\n")
            command("-C", repo, "add", "README.md")
            command("-C", repo, "commit", "-m", "fixture")
            commit = command("-C", repo, "rev-parse", "HEAD")
            with socket.socket() as listener:
                listener.bind(("127.0.0.1", 0))
                port = listener.getsockname()[1]
            job = _WindowsJob.create("Local\\nq-git-fixture-" + uuid.uuid4().hex,
                {"memory_bytes": 536870912, "process_count": 12, "cpu_percent": 50})
            daemon = None
            try:
                with tempfile.TemporaryFile() as log:
                    daemon = _spawn_suspended_in_windows_job(job, [git, "daemon", "--reuseaddr",
                        "--export-all", "--enable=receive-pack", "--listen=127.0.0.1", "--port=" + str(port),
                        "--base-path=" + str(base)], cwd=str(base), environment=sanitized_child_environment(),
                        stdout_stream=log, stderr_stream=log, inherit_recovery_job_handle=False)
                    _WindowsJob.resume(daemon)
                    deadline = time.monotonic() + 5
                    while True:
                        try:
                            with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                                break
                        except OSError:
                            if time.monotonic() > deadline or daemon.poll() is not None:
                                self.fail("Disposable Git daemon did not become reachable")
                            time.sleep(0.02)
                    origin = "git://127.0.0.1:" + str(port) + "/remote.git"
                    command("-C", repo, "remote", "add", "origin", origin)
                    config_before = (repo / ".git/config").read_bytes()
                    client = object.__new__(ManagedCodexClient)
                    client.root, client.thread_context = base / "client", threading.local()
                    client.root.mkdir()
                    client.thread_context.stop = threading.Event()
                    timer = threading.Timer(12, client.thread_context.stop.set)
                    timer.start()
                    try:
                        client._publish_commit(repo, "fixture", commit, origin)
                        self.assertEqual(command("--git-dir", bare, "rev-parse", "refs/heads/fixture"), commit)
                        self.assertEqual(config_before, (repo / ".git/config").read_bytes())
                        # An exit-zero push alone cannot prove a different expected commit.
                        with self.assertRaisesRegex(ValueError, "CLIENT_REMOTE_COMMIT_UNPROVEN"):
                            client._publish_commit(repo, "fixture", "f" * 40, origin)
                    finally:
                        timer.cancel()
            finally:
                job.terminate()
                if daemon:
                    daemon.wait(timeout=5)
                    daemon.close()
                job.close()


if __name__ == "__main__":
    unittest.main()
