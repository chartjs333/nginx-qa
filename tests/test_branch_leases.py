import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import threading
import unittest

from nginx_qa.branch_leases import (
    BranchHeads,
    BranchLeaseError,
    BranchLeaseRequest,
    BranchLeaseStore,
    deterministic_branch_lease_id,
    select_initial_head,
)
from nginx_qa.sprint_types import mirror_storage_key


REMOTE = "example.invalid/acme/repository"
MIRROR_KEY = mirror_storage_key(REMOTE)
SOURCE = "1" * 40
DESCENDANT = "2" * 40
OTHER = "3" * 40


class BranchPolicyTests(unittest.TestCase):
    def test_create_and_reject_if_exists_require_absence(self) -> None:
        for policy in ("create", "reject_if_exists"):
            with self.subTest(policy=policy):
                self.assertEqual(
                    select_initial_head(
                        policy=policy,
                        source_commit=SOURCE,
                        expected_branch_head=None,
                        heads=BranchHeads(),
                        is_ancestor=lambda _a, _b: False,
                    ),
                    SOURCE,
                )
                with self.assertRaises(BranchLeaseError) as raised:
                    select_initial_head(
                        policy=policy,
                        source_commit=SOURCE,
                        expected_branch_head=None,
                        heads=BranchHeads(remote_head=DESCENDANT),
                        is_ancestor=lambda _a, _b: True,
                    )
                self.assertEqual(raised.exception.code, "BRANCH_ALREADY_EXISTS")

    def test_resume_accepts_only_a_source_descendant(self) -> None:
        self.assertEqual(
            select_initial_head(
                policy="resume",
                source_commit=SOURCE,
                expected_branch_head=None,
                heads=BranchHeads(local_head=DESCENDANT, remote_head=DESCENDANT),
                is_ancestor=lambda ancestor, descendant: (
                    ancestor == SOURCE and descendant == DESCENDANT
                ),
            ),
            DESCENDANT,
        )
        with self.assertRaises(BranchLeaseError) as raised:
            select_initial_head(
                policy="resume",
                source_commit=SOURCE,
                expected_branch_head=None,
                heads=BranchHeads(remote_head=OTHER),
                is_ancestor=lambda _a, _b: False,
            )
        self.assertEqual(raised.exception.code, "BRANCH_DIVERGED")

    def test_split_local_and_remote_heads_fail_closed(self) -> None:
        with self.assertRaises(BranchLeaseError) as raised:
            select_initial_head(
                policy="resume",
                source_commit=SOURCE,
                expected_branch_head=None,
                heads=BranchHeads(local_head=DESCENDANT, remote_head=OTHER),
                is_ancestor=lambda _a, _b: True,
            )
        self.assertEqual(raised.exception.code, "BRANCH_DIVERGED")

    def test_require_exact_head_requires_an_existing_exact_match(self) -> None:
        self.assertEqual(
            select_initial_head(
                policy="require_exact_head",
                source_commit=SOURCE,
                expected_branch_head=DESCENDANT,
                heads=BranchHeads(remote_head=DESCENDANT),
                is_ancestor=lambda _a, _b: False,
            ),
            DESCENDANT,
        )
        for heads in (BranchHeads(), BranchHeads(remote_head=OTHER)):
            with self.subTest(heads=heads):
                with self.assertRaises(BranchLeaseError) as raised:
                    select_initial_head(
                        policy="require_exact_head",
                        source_commit=SOURCE,
                        expected_branch_head=DESCENDANT,
                        heads=heads,
                        is_ancestor=lambda _a, _b: True,
                    )
                self.assertEqual(raised.exception.code, "BRANCH_DIVERGED")


class BranchLeaseStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.store = BranchLeaseStore(Path(self.temporary.name) / "leases")

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def request(
        self,
        assignment_id: str,
        *,
        repository_id: str = "primary",
        branch: str = "agent/work",
        lease_id: str | None = None,
    ) -> BranchLeaseRequest:
        return BranchLeaseRequest(
            lease_id=lease_id
            or deterministic_branch_lease_id(MIRROR_KEY, branch, assignment_id),
            repository_id=repository_id,
            repository_key=REMOTE,
            mirror_storage_key=MIRROR_KEY,
            branch=branch,
            assignment_id=assignment_id,
            source_commit=SOURCE,
            initial_head_commit=SOURCE,
        )

    def test_exact_replay_is_idempotent_and_aliases_conflict(self) -> None:
        request = self.request("assignment-a")
        first = self.store.acquire_write(request)
        self.assertEqual(self.store.acquire_write(request), first)

        alias = self.request("assignment-b", repository_id="alias")
        with self.assertRaises(BranchLeaseError) as raised:
            self.store.acquire_write(alias)
        self.assertEqual(raised.exception.code, "BRANCH_ALREADY_LEASED")

    def test_branch_identity_is_casefolded(self) -> None:
        self.store.acquire_write(self.request("assignment-a", branch="Agent/Work"))
        with self.assertRaises(BranchLeaseError) as raised:
            self.store.acquire_write(self.request("assignment-b", branch="agent/work"))
        self.assertEqual(raised.exception.code, "BRANCH_ALREADY_LEASED")

    def test_only_one_concurrent_writer_wins(self) -> None:
        barrier = threading.Barrier(8)

        def acquire(index: int) -> str:
            barrier.wait()
            try:
                self.store.acquire_write(self.request(f"assignment-{index}"))
                return "acquired"
            except BranchLeaseError as exc:
                return exc.code

        with ThreadPoolExecutor(max_workers=8) as pool:
            outcomes = list(pool.map(acquire, range(8)))
        self.assertEqual(outcomes.count("acquired"), 1)
        self.assertEqual(outcomes.count("BRANCH_ALREADY_LEASED"), 7)
        self.assertEqual(len(self.store.active_writers()), 1)

    def test_release_requires_owner_settlement_and_stopped_processes(self) -> None:
        lease = self.store.acquire_write(self.request("assignment-a"))
        for kwargs in (
            {},
            {"assignment_settled": True},
            {"all_processes_stopped": True},
        ):
            with self.subTest(kwargs=kwargs):
                with self.assertRaises(BranchLeaseError) as raised:
                    self.store.release(lease.lease_id, "assignment-a", **kwargs)
                self.assertEqual(
                    raised.exception.code, "BRANCH_LEASE_RELEASE_BLOCKED"
                )
        with self.assertRaises(BranchLeaseError) as raised:
            self.store.release(
                lease.lease_id,
                "not-owner",
                assignment_settled=True,
                all_processes_stopped=True,
            )
        self.assertEqual(raised.exception.code, "BRANCH_LEASE_OWNER_MISMATCH")

        released = self.store.release(
            lease.lease_id,
            "assignment-a",
            assignment_settled=True,
            all_processes_stopped=True,
        )
        self.assertEqual(released.status, "released")
        self.assertEqual(
            self.store.release(
                lease.lease_id,
                "assignment-a",
                assignment_settled=True,
                all_processes_stopped=True,
            ),
            released,
        )
        with self.assertRaises(BranchLeaseError) as replay:
            self.store.acquire_write(self.request("assignment-a"))
        self.assertEqual(replay.exception.code, "BRANCH_LEASE_RELEASED")


if __name__ == "__main__":
    unittest.main()
