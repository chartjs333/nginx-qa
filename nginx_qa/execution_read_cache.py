"""Process-local derived data only; never an authority or a validation cache."""
from __future__ import annotations

import asyncio
from collections import OrderedDict
from copy import deepcopy
import sys
import threading


def _retained_size(value, limit):
    """Bounded walk of Python payload size; preserve types instead of JSON round-trip."""
    seen = set()
    pending = [value]
    total = 0
    while pending:
        item = pending.pop()
        identity = id(item)
        if identity in seen:
            continue
        seen.add(identity)
        total += sys.getsizeof(item)
        if total > limit:
            return total
        if isinstance(item, dict):
            pending.extend(item.keys())
            pending.extend(item.values())
        elif isinstance(item, (list, tuple)):
            pending.extend(item)
    return total


class HistoricalProjectionCache:
    """LRU of detached historical projections, not source snapshots/HTTP responses.

    The caller MUST validate actual checkpoint contents before every lookup.
    Payload accounting excludes the small, entry-count-bounded LRU/lock overhead;
    it is not a cap on response size or total process peak memory.
    """

    def __init__(self, *, max_bytes=8 * 1024 * 1024, max_entry_bytes=2 * 1024 * 1024,
                 max_entries=8):
        if min(max_bytes, max_entry_bytes, max_entries) < 0:
            raise ValueError("Cache bounds must be non-negative")
        self.max_bytes = max_bytes
        self.max_entry_bytes = min(max_entry_bytes, max_bytes)
        self.max_entries = max_entries
        self._entries = OrderedDict()
        self._lock = threading.Lock()
        self._bytes = self._hits = self._misses = 0

    def get_or_build(self, key, build):
        # Single-flight also for direct threaded callers. No host/file locks are
        # acquired by build. Failed builds are never cached; no stale fallback.
        with self._lock:
            if key in self._entries:
                value, size = self._entries.pop(key)
                self._entries[key] = (value, size)
                self._hits += 1
                return deepcopy(value)
            self._misses += 1
            value = build()
            size = _retained_size(value, self.max_entry_bytes)
            if self.max_entries and size <= self.max_entry_bytes:
                owned = deepcopy(value)
                # deepcopy may change container capacity/sharing, so account for
                # the object actually retained rather than only the source.
                size = _retained_size(owned, self.max_entry_bytes)
                if size <= self.max_entry_bytes:
                    while self._entries and (self._bytes + size > self.max_bytes
                                             or len(self._entries) >= self.max_entries):
                        _, (_, removed_size) = self._entries.popitem(last=False)
                        self._bytes -= removed_size
                    self._entries[key] = (owned, size)
                    self._bytes += size
            return value

    def info(self):
        with self._lock:
            return {"entries": len(self._entries), "retained_payload_bytes": self._bytes,
                    "hits": self._hits, "misses": self._misses,
                    "max_bytes": self.max_bytes, "max_entry_bytes": self.max_entry_bytes,
                    "max_entries": self.max_entries}


class ExecutionReadGate:
    """One in-flight execution read per app, admitted BEFORE snapshot capture.

    Cancelling a waiter must not admit another giant read while its thread is
    still running. The shielded operation owns the permit until real completion.
    No response is shared with the next caller: it obtains a fresh snapshot.
    """

    def __init__(self):
        self._permit = asyncio.Semaphore(1)
        self._loop = None
        self._users = 0
        self._lifecycle = threading.Lock()

    async def run(self, operation):
        # TestClient and application restarts can reuse a registered app on a
        # new loop. Rebind only when idle, never split an active gate across loops.
        loop = asyncio.get_running_loop()
        with self._lifecycle:
            if loop is not self._loop:
                if self._users:
                    raise RuntimeError("Execution read gate is active on another event loop")
                self._permit = asyncio.Semaphore(1)
                self._loop = loop
            permit = self._permit
            self._users += 1

        def release_user():
            with self._lifecycle:
                self._users -= 1

        try:
            await permit.acquire()
        except BaseException:
            release_user()
            raise
        try:
            task = asyncio.create_task(operation())
        except BaseException:
            permit.release()
            release_user()
            raise

        def finished(completed):
            permit.release()
            release_user()
            # Retrieve failures even when the HTTP waiter has disconnected.
            if not completed.cancelled():
                completed.exception()

        task.add_done_callback(finished)
        return await asyncio.shield(task)
