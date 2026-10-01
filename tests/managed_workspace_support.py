from pathlib import Path
import subprocess
import tempfile

from nginx_qa.branch_leases import BranchLeaseStore
from nginx_qa.git_provider import ManagedGitProvider, RepositorySpec
from nginx_qa.workspace_manager import ManagedWorkspaceManager


class ManagedWorkspaceFixture:
    canonical_remote = "example.invalid/acme/repository"

    def setUp(self) -> None:
        super().setUp()
        self.temporary = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary.name)
        self.source = self.base / "source"
        self.source.mkdir()
        self.git("init", "--initial-branch=main", cwd=self.source)
        self.git("config", "user.name", "Managed Test", cwd=self.source)
        self.git("config", "user.email", "managed@example.invalid", cwd=self.source)
        (self.source / "tracked.txt").write_text("one\n", encoding="utf-8")
        self.git("add", "tracked.txt", cwd=self.source)
        self.git("commit", "-m", "initial", cwd=self.source)
        self.source_commit = self.git("rev-parse", "HEAD", cwd=self.source)

        self.remote = self.base / "upstream.git"
        self.git("clone", "--bare", str(self.source), str(self.remote))
        self.git("remote", "add", "upstream", str(self.remote), cwd=self.source)

        self.managed_root = self.base / "managed"
        primary = RepositorySpec(
            repository_id="primary",
            canonical_remote=self.canonical_remote,
            transport_url=str(self.remote),
        )
        alias = RepositorySpec(
            repository_id="alias",
            canonical_remote=self.canonical_remote,
            transport_url=str(self.remote),
        )
        self.provider = ManagedGitProvider(
            self.managed_root,
            {"primary": primary, "alias": alias},
            allow_local_transport=True,
            command_timeout=30,
            lock_timeout=30,
        )
        self.leases = BranchLeaseStore(self.managed_root / "leases")
        self.manager = ManagedWorkspaceManager(
            self.managed_root,
            self.provider,
            self.leases,
            install_roots=(),
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()
        super().tearDown()

    @staticmethod
    def git(*arguments: str, cwd: Path | None = None, check: bool = True) -> str:
        completed = subprocess.run(
            ("git", *arguments),
            cwd=str(cwd) if cwd is not None else None,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        if check and completed.returncode != 0:
            raise AssertionError(
                f"git {' '.join(arguments)} failed ({completed.returncode}): "
                f"{completed.stderr}"
            )
        return completed.stdout.strip()

    def commit(self, text: str, message: str) -> str:
        (self.source / "tracked.txt").write_text(text, encoding="utf-8")
        self.git("add", "tracked.txt", cwd=self.source)
        self.git("commit", "-m", message, cwd=self.source)
        return self.git("rev-parse", "HEAD", cwd=self.source)
