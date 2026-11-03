"""Synthetic cache contracts: no main, sockets, credentials or persisted state."""
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import gc
import math
import threading
import unittest
from unittest.mock import patch
import weakref

from nginx_qa import execution_observability as projection
from nginx_qa.execution_read_cache import HistoricalProjectionCache
from tests.test_execution_observability import delta_fixture, managed_fixture


def history(source=None):
    source = source or delta_fixture()
    ledger = {}
    for revision in range(2):
        source["execution"]["revision"] = revision
        projection.append_execution_checkpoint(ledger, source, recorded_at="2026-01-01T00:00:00Z")
    return source, ledger["checkpoints"]


class ProjectionCacheTests(unittest.TestCase):
    def test_canonical_digest_equivalence_including_scalars_and_errors(self):
        values = [None, True, False, 1, -0.0, 1e100, 1e-100, "é\\\nПривет😀",
                  {"z": (1, 2), "a": [None, {"q": "\""}]}, "я" * 1000000,
                  {1: "integer key", 2: False}, [delta_fixture(), managed_fixture()]]
        for value in values:
            self.assertEqual(projection._checkpoint_digest(value), projection._hash(value))
        for value in (math.nan, math.inf, {"nan": math.nan}):
            with self.assertRaises(ValueError):
                projection._checkpoint_digest(value)
        circular = []
        circular.append(circular)
        with self.assertRaises(ValueError):
            projection._checkpoint_digest(circular)

    def test_cached_and_uncached_full_response_equivalence_and_no_mutation(self):
        for fixture in (delta_fixture(), managed_fixture()):
            source, points = history(fixture)
            before = deepcopy((source, points))
            cache = HistoricalProjectionCache()
            for options in ({}, {"at_checkpoint": points[0]["checkpoint_id"]}, {"at_revision": 1}):
                expected = projection.project_execution(source, checkpoints=points, **options)
                for _ in range(2):
                    actual = projection.project_execution(source, checkpoints=points, history_cache=cache, **options)
                    self.assertEqual(actual, expected)
            self.assertEqual((source, points), before)

    def test_warm_read_skips_history_projection_but_validates_every_checkpoint(self):
        source, points = history()
        cache = HistoricalProjectionCache()
        first = projection.project_execution(source, checkpoints=points, history_cache=cache)
        with patch.object(projection, "_timeline", wraps=projection._timeline) as timeline, patch.object(
                projection, "_checkpoint_digest", wraps=projection._checkpoint_digest) as digest:
            second = projection.project_execution(source, checkpoints=points, history_cache=cache, cursor=first["cursor"])
        self.assertEqual(timeline.call_count, 1)  # fresh current view, not historical snapshots
        self.assertEqual(digest.call_count, len(points))
        self.assertTrue(second["unchanged"])

    def test_same_label_content_tamper_and_corrupt_future_fail_after_warmup(self):
        source, points = history()
        cache = HistoricalProjectionCache()
        projection.project_execution(source, checkpoints=points, history_cache=cache)
        points[-1]["snapshot"]["execution"]["status"] = "corrupt"
        for options in ({}, {"at_checkpoint": points[0]["checkpoint_id"]}):
            with self.assertRaisesRegex(ValueError, "digest"):
                projection.project_execution(source, checkpoints=points, history_cache=cache, **options)

    def test_scope_ack_queue_and_requests_change_without_revision(self):
        for change in ("scope", "ack", "queue", "requests", "activity"):
            source, points = history()
            cache = HistoricalProjectionCache()
            old = projection.project_execution(source, checkpoints=points, history_cache=cache)
            control = source["execution"]["scope_control"]
            aid = source["execution"]["current_assignment_id"]
            if change == "scope":
                control["assignment_bindings"][aid]["effective_core"]["profile"] = "new"
            elif change == "ack":
                control["acknowledgements"] = {}
            elif change == "queue":
                source["pending_work"]["worker"].append({"id": "fresh-queue"})
            elif change == "requests":
                source["scope_requests"] = [{"request_id": "new", "status": "pending"}]
            else:
                source["recent_activity"] = [{"event_id": "new-activity"}]
            expected = projection.project_execution(source, checkpoints=points)
            actual = projection.project_execution(source, checkpoints=points, history_cache=cache)
            self.assertEqual(actual, expected)
            self.assertNotEqual(actual["cursor"], old["cursor"])

    def test_metadata_decoration_and_append_invalidate_history(self):
        source, points = history()
        cache = HistoricalProjectionCache()
        projection.project_execution(source, checkpoints=points, history_cache=cache)
        points[0]["execution_revision"] = 900
        actual = projection.project_execution(source, checkpoints=points, history_cache=cache)
        self.assertEqual(actual, projection.project_execution(source, checkpoints=points))
        self.assertTrue(any(e.get("observed_at_execution_revision") == 900 for e in actual["timeline"]))
        source["execution"]["revision"] = 3
        source["execution"]["last_transition"] = {"transition_id": "new", "resolved_at": "2026-01-02T00:00:00Z"}
        projection.append_execution_checkpoint({"checkpoints": points}, source, recorded_at="2026-01-02T00:00:00Z")
        self.assertEqual(projection.project_execution(source, checkpoints=points, history_cache=cache),
                         projection.project_execution(source, checkpoints=points))

    def test_corrected_snapshot_with_valid_new_digest_is_not_old_cache(self):
        source, points = history()
        cache = HistoricalProjectionCache()
        projection.project_execution(source, checkpoints=points, history_cache=cache)
        points[0]["snapshot"]["recent_activity"] = [{"event_id": "correction"}]
        points[0]["snapshot_sha256"] = projection._hash(points[0]["snapshot"])
        self.assertEqual(projection.project_execution(source, checkpoints=points, history_cache=cache),
                         projection.project_execution(source, checkpoints=points))

    def test_project_sprint_namespace_and_cache_restart(self):
        source, points = history()
        cache = HistoricalProjectionCache()
        for project, sprint in (("p1", "s1"), ("p2", "s1"), ("p1", "s2")):
            source.update(project_id=project, sprint_id=sprint)
            result = projection.project_execution(source, checkpoints=points, history_cache=cache)
            self.assertEqual(result, projection.project_execution(source, checkpoints=points))
        self.assertEqual(cache.info()["misses"], 3)
        self.assertEqual(HistoricalProjectionCache().info()["entries"], 0)

    def test_returned_mutations_do_not_poison_cache_or_retain_source(self):
        source, points = history()
        class Tracked(dict):
            pass
        points[0]["snapshot"] = Tracked(points[0]["snapshot"])
        source_ref = weakref.ref(points[0]["snapshot"])
        cache = HistoricalProjectionCache()
        output = projection.project_execution(source, checkpoints=points, history_cache=cache)
        expected = deepcopy(output)
        output["timeline"][0]["metadata"]["injected"] = True
        self.assertEqual(projection.project_execution(source, checkpoints=points, history_cache=cache), expected)
        del points
        gc.collect()
        self.assertIsNone(source_ref())

    def test_eviction_entry_limit_oversize_disable_and_type_preservation(self):
        cache = HistoricalProjectionCache(max_bytes=3000, max_entry_bytes=2000, max_entries=2)
        for i in range(10):
            cache.get_or_build(str(i), lambda: {"data": "x" * 500, "tuple": (1, 2), 5: None})
            self.assertLessEqual(cache.info()["retained_payload_bytes"], 3000)
            self.assertLessEqual(cache.info()["entries"], 2)
        cache.get_or_build("large", lambda: "x" * 4000)
        before = cache.info()["misses"]
        cache.get_or_build("large", lambda: "x" * 4000)
        self.assertEqual(cache.info()["misses"], before + 1)
        item = cache.get_or_build("types", lambda: {5: (1, 2)})
        self.assertEqual(cache.get_or_build("types", lambda: self.fail("cache miss")), item)
        for option in ({"max_bytes": 0}, {"max_entry_bytes": 0}, {"max_entries": 0}):
            disabled = HistoricalProjectionCache(**option)
            disabled.get_or_build("key", lambda: {"a": []})
            self.assertEqual(disabled.info()["entries"], 0)

    def test_single_flight_threads_and_failed_build_recovery(self):
        cache = HistoricalProjectionCache()
        entered, release = threading.Event(), threading.Event()
        calls = []
        def build():
            calls.append(1)
            entered.set()
            if not release.wait(3):
                raise AssertionError("release not signalled")
            return {"complete": True}
        with ThreadPoolExecutor(max_workers=4) as pool:
            first = pool.submit(cache.get_or_build, "same", build)
            self.assertTrue(entered.wait(2))
            others = [pool.submit(cache.get_or_build, "same", build) for _ in range(3)]
            release.set()
            self.assertTrue(all(f.result()["complete"] for f in [first, *others]))
        self.assertEqual(len(calls), 1)
        with self.assertRaises(ValueError):
            cache.get_or_build("broken", lambda: (_ for _ in ()).throw(ValueError("synthetic")))
        self.assertEqual(cache.get_or_build("broken", lambda: 42), 42)


if __name__ == "__main__":
    unittest.main()
