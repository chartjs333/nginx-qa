from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import socket
import tempfile
import threading
import unittest

from nginx_qa.port_leases import (
    ManagedPortReservationError,
    ManagedPortReservationRegistry,
)


class ManagedPortReservationRegistryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.base = Path(self.temporary_directory.name)
        self.registry = ManagedPortReservationRegistry()

    def tearDown(self) -> None:
        self.registry.close_all()
        self.temporary_directory.cleanup()

    def database(self, name: str) -> Path:
        path = self.base / name
        path.touch()
        return path

    @staticmethod
    def available_port() -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.bind(("127.0.0.1", 0))
            return int(probe.getsockname()[1])

    @staticmethod
    def lease(lease_id: str, port: int, **extra: object) -> dict[str, object]:
        return {
            "lease_id": lease_id,
            "network_namespace_id": "host",
            "host": "127.0.0.1",
            "port": port,
            "status": "reserved",
            **extra,
        }

    @staticmethod
    def bind_competitor(port: int) -> socket.socket:
        competitor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            competitor.bind(("127.0.0.1", port))
        except BaseException:
            competitor.close()
            raise
        return competitor

    def test_compatible_idempotent_acquire_and_transaction_rollback(self) -> None:
        database = self.database("runtime.sqlite3")
        port = self.available_port()
        lease = self.lease("lease-1", port)

        acquired = self.registry.acquire_many(database, [lease])
        repeated = self.registry.acquire_many(database, [lease])

        self.assertEqual(
            acquired.acquired_keys,
            ((acquired.database_key, "lease-1"),),
        )
        self.assertEqual(repeated.acquired_keys, ())
        self.registry.rollback(repeated)
        self.assertTrue(self.registry.owns_endpoint(database, "127.0.0.1", port))
        self.assertTrue(self.registry.holds_endpoint(database, "127.0.0.1", port))
        with self.assertRaises(OSError):
            self.bind_competitor(port)

        self.registry.rollback(acquired)
        with self.bind_competitor(port):
            pass

    def test_stale_rollback_token_cannot_release_reacquired_lease(self) -> None:
        database = self.database("runtime.sqlite3")
        port = self.available_port()
        lease = self.lease("lease-1", port)

        stale = self.registry.acquire_many(database, [lease])
        self.registry.release(database, "lease-1")
        current = self.registry.acquire_many(database, [lease])

        self.registry.rollback(stale)
        self.assertTrue(self.registry.holds_endpoint(database, "127.0.0.1", port))
        with self.assertRaises(OSError):
            self.bind_competitor(port)

        self.registry.rollback(current)
        with self.bind_competitor(port):
            pass

    def test_rollback_token_cannot_release_durable_or_handed_off_lease(self) -> None:
        database = self.database("runtime.sqlite3")
        port = self.available_port()
        lease = self.lease("lease-1", port)

        provisional = self.registry.acquire_many(database, [lease])
        self.registry.mark_durable_many(database, [lease])
        self.registry.rollback(provisional)
        self.assertTrue(self.registry.holds_endpoint(database, "127.0.0.1", port))

        self.registry.release(database, "lease-1")
        durable = self.registry.acquire_durable_many(database, [lease])
        handoff = self.registry.begin_handoff(database, "lease-1")
        self.registry.rollback(durable)
        owner = self.registry.endpoint_owner("127.0.0.1", port)
        self.assertIsNotNone(owner)
        assert owner is not None
        self.assertEqual(owner.state, "handing_off")
        self.registry.reacquire_handoff(handoff)
        self.registry.release(database, "lease-1")

    def test_endpoint_claim_is_global_across_databases_and_namespaces(self) -> None:
        first_database = self.database("first.sqlite3")
        second_database = self.database("second.sqlite3")
        port = self.available_port()
        first = self.lease("lease-a", port)
        second = self.lease(
            "lease-b",
            port,
            network_namespace_id="different-durable-label",
        )

        self.registry.acquire_many(first_database, [first])
        with self.assertRaises(ManagedPortReservationError):
            self.registry.acquire_many(second_database, [second])

        owner = self.registry.endpoint_owner("127.0.0.1", port)
        self.assertIsNotNone(owner)
        assert owner is not None
        self.assertEqual(owner.lease_id, "lease-a")
        self.registry.release(first_database, "lease-a")
        token = self.registry.acquire_many(second_database, [second])
        self.assertEqual(len(token.acquired_keys), 1)

    def test_durable_reconciliation_releases_cross_instance_stale_claim(self) -> None:
        database = self.database("runtime.sqlite3")
        port = self.available_port()
        lease = self.lease(
            "lease-1",
            port,
            instance_id="instance-1",
            assignment_id="assignment-1",
            process_id=None,
            bind_verified=False,
            acquired_at="2026-01-01T00:00:00+00:00",
            released_at=None,
        )
        other = ManagedPortReservationRegistry()
        try:
            self.registry.acquire_durable_many(database, [lease])
            with self.assertRaises(ManagedPortReservationError):
                other.acquire_durable_many(database, [lease])

            bound = {
                **lease,
                "status": "bound",
                "process_id": "process-1",
                "bind_verified": True,
            }
            bound_fence = self.registry.reconciliation_fence(database)
            self.registry.reconcile_durable(
                database, [bound], fence=bound_fence
            )
            self.assertTrue(
                self.registry.owns_endpoint(database, "127.0.0.1", port)
            )

            # A different service instance committed the terminal transition.
            released_fence = self.registry.reconciliation_fence(database)
            self.registry.reconcile_durable(
                database, [], fence=released_fence
            )
            self.assertFalse(
                self.registry.owns_endpoint(database, "127.0.0.1", port)
            )
            acquired = other.acquire_durable_many(database, [lease])
            self.assertEqual(len(acquired.acquired_keys), 1)
        finally:
            other.close_all()

    def test_durable_reconciliation_does_not_drop_uncommitted_holder(self) -> None:
        database = self.database("runtime.sqlite3")
        port = self.available_port()
        lease = self.lease("lease-provisional", port)
        self.registry.acquire_many(database, [lease])

        fence = self.registry.reconciliation_fence(database)
        self.registry.reconcile_durable(database, [], fence=fence)

        self.assertTrue(
            self.registry.holds_endpoint(database, "127.0.0.1", port)
        )

    def test_stale_snapshot_cannot_drop_newly_committed_reservation(self) -> None:
        database = self.database("runtime.sqlite3")
        port = self.available_port()
        lease = self.lease("lease-new-commit", port)

        stale_fence = self.registry.reconciliation_fence(database)
        self.registry.acquire_many(database, [lease])
        self.registry.mark_durable_many(database, [lease])

        # This empty observation began before the reservation existed.
        self.registry.reconcile_durable(database, [], fence=stale_fence)
        self.assertTrue(
            self.registry.holds_endpoint(database, "127.0.0.1", port)
        )

        # A later empty durable snapshot is authoritative and releases it.
        current_fence = self.registry.reconciliation_fence(database)
        self.registry.reconcile_durable(database, [], fence=current_fence)
        self.assertFalse(
            self.registry.owns_endpoint(database, "127.0.0.1", port)
        )

    def test_handoff_is_fenced_and_retains_internal_endpoint_claim(self) -> None:
        database = self.database("runtime.sqlite3")
        other_database = self.database("other.sqlite3")
        port = self.available_port()
        lease = self.lease("lease-1", port)
        self.registry.acquire_many(database, [lease])
        signature = self.registry.reservation(database, "lease-1").signature  # type: ignore[union-attr]

        first_handoff = self.registry.begin_handoff(
            database,
            "lease-1",
            expected_signature=signature,
        )
        during_handoff = self.registry.reservation(database, "lease-1")
        self.assertIsNotNone(during_handoff)
        assert during_handoff is not None
        self.assertEqual(during_handoff.state, "handing_off")
        self.assertFalse(during_handoff.holds_socket)
        with self.assertRaises(ManagedPortReservationError):
            self.registry.acquire_many(
                other_database,
                [self.lease("lease-2", port)],
            )

        reacquired = self.registry.reacquire_handoff(first_handoff)
        self.assertEqual(reacquired.state, "reserved")
        self.assertTrue(reacquired.holds_socket)
        with self.assertRaises(ManagedPortReservationError):
            self.registry.reacquire_handoff(first_handoff)

        second_handoff = self.registry.begin_handoff(database, "lease-1")
        with self.bind_competitor(port) as child_listener:
            child_listener.listen(1)
            bound = self.registry.complete_handoff(second_handoff)
            self.assertEqual(bound.state, "bound")
            self.assertFalse(bound.holds_socket)
            with self.assertRaises(ManagedPortReservationError):
                self.registry.reacquire_bound(
                    database,
                    "lease-1",
                    expected_signature=bound.signature,
                    expected_generation=bound.generation,
                )

        restored = self.registry.reacquire_bound(
            database,
            "lease-1",
            expected_signature=bound.signature,
            expected_generation=bound.generation,
        )
        self.assertEqual(restored.state, "reserved")
        self.assertTrue(restored.holds_socket)

    def test_exact_recovery_rejects_extras_and_changed_identity(self) -> None:
        database = self.database("runtime.sqlite3")
        port = self.available_port()
        lease = self.lease("lease-1", port)
        self.registry.acquire_many(database, [lease])

        repeated = self.registry.recover_exact(database, [lease])
        self.assertEqual(repeated.acquired_keys, ())
        with self.assertRaises(ManagedPortReservationError):
            self.registry.recover_exact(database, [])
        with self.assertRaises(ManagedPortReservationError):
            self.registry.recover_exact(
                database,
                [{**lease, "assignment_id": "changed-signature"}],
            )

        self.registry.release(database, "lease-1")
        recovered = self.registry.reacquire_many(database, [lease])
        self.assertEqual(len(recovered.acquired_keys), 1)

    def test_concurrent_claims_have_one_winner(self) -> None:
        contenders = 8
        port = self.available_port()
        databases = [self.database(f"runtime-{index}.sqlite3") for index in range(contenders)]
        barrier = threading.Barrier(contenders)

        def acquire(index: int) -> bool:
            barrier.wait()
            try:
                token = self.registry.acquire_many(
                    databases[index],
                    [self.lease(f"lease-{index}", port)],
                )
            except ManagedPortReservationError:
                return False
            return bool(token.acquired_keys)

        with ThreadPoolExecutor(max_workers=contenders) as executor:
            results = list(executor.map(acquire, range(contenders)))

        self.assertEqual(results.count(True), 1)
        self.assertEqual(len(self.registry.reservations()), 1)


if __name__ == "__main__":
    unittest.main()
