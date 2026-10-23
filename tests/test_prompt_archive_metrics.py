import json
import threading
import unittest

from nginx_qa.prompt_archive_metrics import (
    PROMPT_ARCHIVE_FORMAT_VERSION,
    PromptArchiveMetrics,
)


class PromptArchiveMetricsTests(unittest.TestCase):
    def test_snapshot_reports_closed_required_metrics(self) -> None:
        metrics = PromptArchiveMetrics()
        metrics.record_publication(
            logical_bytes=1_024,
            new_physical_bytes=256,
            dedup_hits=7,
        )
        metrics.record_hydration(latency_ns=11_000_000, succeeded=True)
        metrics.record_hydration(latency_ns=17_000_000, succeeded=False)
        metrics.record_corruption_failure()
        metrics.record_recovery_failure()

        self.assertEqual(
            metrics.snapshot().as_dict(),
            {
                "schema_version": 1,
                "kind": "prompt_archive_metrics",
                "format_version": PROMPT_ARCHIVE_FORMAT_VERSION,
                "publication_count": 1,
                "logical_bytes": 1_024,
                "new_physical_bytes": 256,
                "dedup_hits": 7,
                "hydration": {
                    "attempt_count": 2,
                    "success_count": 1,
                    "failure_count": 1,
                    "latency_ns_total": 28_000_000,
                    "latency_ns_max": 17_000_000,
                },
                "failures": {"corruption": 1, "recovery": 1},
                "saturated": False,
            },
        )

    def test_concurrent_updates_are_atomic_and_lossless_below_limit(self) -> None:
        metrics = PromptArchiveMetrics()
        barrier = threading.Barrier(9)

        def update() -> None:
            barrier.wait()
            for _ in range(2_000):
                metrics.record_publication(
                    logical_bytes=3,
                    new_physical_bytes=2,
                    dedup_hits=1,
                )
                metrics.record_hydration(latency_ns=5, succeeded=True)
                metrics.record_corruption_failure()
                metrics.record_recovery_failure()

        threads = [threading.Thread(target=update) for _ in range(8)]
        for thread in threads:
            thread.start()
        barrier.wait()
        for thread in threads:
            thread.join(timeout=20)
            self.assertFalse(thread.is_alive())

        snapshot = metrics.snapshot()
        self.assertEqual(snapshot.publication_count, 16_000)
        self.assertEqual(snapshot.logical_bytes, 48_000)
        self.assertEqual(snapshot.new_physical_bytes, 32_000)
        self.assertEqual(snapshot.dedup_hits, 16_000)
        self.assertEqual(snapshot.hydration_attempt_count, 16_000)
        self.assertEqual(snapshot.hydration_success_count, 16_000)
        self.assertEqual(snapshot.hydration_failure_count, 0)
        self.assertEqual(snapshot.hydration_latency_ns_total, 80_000)
        self.assertEqual(snapshot.hydration_latency_ns_max, 5)
        self.assertEqual(snapshot.corruption_failures, 16_000)
        self.assertEqual(snapshot.recovery_failures, 16_000)
        self.assertFalse(snapshot.saturated)

    def test_invalid_event_cannot_partially_mutate_metrics(self) -> None:
        metrics = PromptArchiveMetrics()
        before = metrics.snapshot()

        invalid_publications = (
            {"logical_bytes": True, "new_physical_bytes": 1, "dedup_hits": 1},
            {"logical_bytes": 1, "new_physical_bytes": -1, "dedup_hits": 1},
            {"logical_bytes": 1, "new_physical_bytes": 1, "dedup_hits": 1 << 63},
        )
        for event in invalid_publications:
            with self.assertRaises(ValueError):
                metrics.record_publication(**event)
            self.assertEqual(metrics.snapshot(), before)

        with self.assertRaises(ValueError):
            metrics.record_hydration(latency_ns=1, succeeded=1)  # type: ignore[arg-type]
        self.assertEqual(metrics.snapshot(), before)

    def test_counters_saturate_and_signal_loss_of_precision(self) -> None:
        metrics = PromptArchiveMetrics()
        maximum = (1 << 63) - 1
        metrics.record_publication(
            logical_bytes=maximum,
            new_physical_bytes=maximum,
            dedup_hits=maximum,
        )
        metrics.record_publication(
            logical_bytes=1,
            new_physical_bytes=1,
            dedup_hits=1,
        )
        metrics.record_hydration(latency_ns=maximum, succeeded=False)
        metrics.record_hydration(latency_ns=1, succeeded=True)

        snapshot = metrics.snapshot()
        self.assertEqual(snapshot.logical_bytes, maximum)
        self.assertEqual(snapshot.new_physical_bytes, maximum)
        self.assertEqual(snapshot.dedup_hits, maximum)
        self.assertEqual(snapshot.hydration_latency_ns_total, maximum)
        self.assertEqual(snapshot.hydration_latency_ns_max, maximum)
        self.assertTrue(snapshot.saturated)

    def test_export_cannot_contain_caller_values(self) -> None:
        metrics = PromptArchiveMetrics()
        canaries = (
            "D:/sensitive/prompt/latest-response.json",
            "Bearer secret-token",
            "raw-payload-canary",
        )
        metrics.record_publication(
            logical_bytes=len(canaries[0]),
            new_physical_bytes=len(canaries[1]),
            dedup_hits=len(canaries[2]),
        )
        exported = json.dumps(metrics.snapshot().as_dict(), sort_keys=True)
        for canary in canaries:
            self.assertNotIn(canary, exported)


if __name__ == "__main__":
    unittest.main()
