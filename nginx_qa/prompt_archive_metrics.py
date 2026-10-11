"""Bounded, payload-free process metrics for prompt archive v1.

The archive runtime handles opaque evidence and authorization metadata.  None
of that data belongs in process metrics.  This module therefore exposes only a
closed set of integer counters plus the fixed PAS1 format identifier.  It does
not accept labels, paths, refs, correlation IDs, exception messages, or
payloads, so callers cannot accidentally retain those values here.

The counters are process-local observability, not durable publication proof.
Qualification that claims exact physical accounting must still inventory every
test-owned artifact.  ``new_physical_bytes`` should be supplied from accepted
write outcomes or a bounded before/after inventory, rather than inferred from
filenames.
"""

from __future__ import annotations

import dataclasses
import threading
from typing import Final


PROMPT_ARCHIVE_FORMAT_VERSION: Final[str] = "PAS1"
PROMPT_ARCHIVE_METRICS_SCHEMA_VERSION: Final[int] = 1

_MAX_COUNTER: Final[int] = (1 << 63) - 1


def _bounded_counter_input(value: object, *, name: str) -> int:
    """Return a valid non-negative signed-64-bit metric input.

    ``bool`` is deliberately rejected even though it is an ``int`` subclass.
    The fixed error text never reflects the supplied value.
    """

    if type(value) is not int or value < 0 or value > _MAX_COUNTER:
        raise ValueError(f"{name} must be a non-negative signed-64-bit integer")
    return value


@dataclasses.dataclass(frozen=True, slots=True)
class PromptArchiveMetricsSnapshot:
    """One immutable, secret-free view of the in-process PAS1 counters."""

    publication_count: int
    logical_bytes: int
    new_physical_bytes: int
    dedup_hits: int
    hydration_attempt_count: int
    hydration_success_count: int
    hydration_failure_count: int
    hydration_latency_ns_total: int
    hydration_latency_ns_max: int
    corruption_failures: int
    recovery_failures: int
    saturated: bool

    def as_dict(self) -> dict[str, object]:
        """Return the closed JSON-ready evidence shape.

        The shape intentionally has no extension/label map.  Adding arbitrary
        metadata here would reopen a path for payloads, paths, or credentials
        to leak into metrics exports.
        """

        return {
            "schema_version": PROMPT_ARCHIVE_METRICS_SCHEMA_VERSION,
            "kind": "prompt_archive_metrics",
            "format_version": PROMPT_ARCHIVE_FORMAT_VERSION,
            "publication_count": self.publication_count,
            "logical_bytes": self.logical_bytes,
            "new_physical_bytes": self.new_physical_bytes,
            "dedup_hits": self.dedup_hits,
            "hydration": {
                "attempt_count": self.hydration_attempt_count,
                "success_count": self.hydration_success_count,
                "failure_count": self.hydration_failure_count,
                "latency_ns_total": self.hydration_latency_ns_total,
                "latency_ns_max": self.hydration_latency_ns_max,
            },
            "failures": {
                "corruption": self.corruption_failures,
                "recovery": self.recovery_failures,
            },
            "saturated": self.saturated,
        }


class PromptArchiveMetrics:
    """Thread-safe, monotonic, bounded PAS1 process counters.

    Values saturate at signed 64-bit maximum instead of wrapping.  A sticky
    ``saturated`` bit makes the loss of exact aggregate precision explicit.
    Inputs are validated before the lock is acquired, so an invalid multi-field
    event cannot partially update the snapshot.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._publication_count = 0
        self._logical_bytes = 0
        self._new_physical_bytes = 0
        self._dedup_hits = 0
        self._hydration_attempt_count = 0
        self._hydration_success_count = 0
        self._hydration_failure_count = 0
        self._hydration_latency_ns_total = 0
        self._hydration_latency_ns_max = 0
        self._corruption_failures = 0
        self._recovery_failures = 0
        self._saturated = False

    def _add_locked(self, attribute: str, increment: int) -> None:
        current = getattr(self, attribute)
        if increment > _MAX_COUNTER - current:
            setattr(self, attribute, _MAX_COUNTER)
            self._saturated = True
            return
        setattr(self, attribute, current + increment)

    def record_publication(
        self,
        *,
        logical_bytes: int,
        new_physical_bytes: int,
        dedup_hits: int,
    ) -> None:
        """Record one successfully verified publication or idempotent replay."""

        logical = _bounded_counter_input(logical_bytes, name="logical_bytes")
        physical = _bounded_counter_input(
            new_physical_bytes, name="new_physical_bytes"
        )
        deduplicated = _bounded_counter_input(dedup_hits, name="dedup_hits")
        with self._lock:
            self._add_locked("_publication_count", 1)
            self._add_locked("_logical_bytes", logical)
            self._add_locked("_new_physical_bytes", physical)
            self._add_locked("_dedup_hits", deduplicated)

    def record_hydration(self, *, latency_ns: int, succeeded: bool) -> None:
        """Record one completed hydration attempt without retaining its target."""

        latency = _bounded_counter_input(latency_ns, name="latency_ns")
        if type(succeeded) is not bool:
            raise ValueError("succeeded must be a boolean")
        with self._lock:
            self._add_locked("_hydration_attempt_count", 1)
            if succeeded:
                self._add_locked("_hydration_success_count", 1)
            else:
                self._add_locked("_hydration_failure_count", 1)
            self._add_locked("_hydration_latency_ns_total", latency)
            self._hydration_latency_ns_max = max(
                self._hydration_latency_ns_max, latency
            )

    def record_corruption_failure(self) -> None:
        """Record one fail-closed corruption outcome."""

        with self._lock:
            self._add_locked("_corruption_failures", 1)

    def record_recovery_failure(self) -> None:
        """Record one unsuccessful recovery attempt."""

        with self._lock:
            self._add_locked("_recovery_failures", 1)

    def snapshot(self) -> PromptArchiveMetricsSnapshot:
        """Copy all counters atomically."""

        with self._lock:
            return PromptArchiveMetricsSnapshot(
                publication_count=self._publication_count,
                logical_bytes=self._logical_bytes,
                new_physical_bytes=self._new_physical_bytes,
                dedup_hits=self._dedup_hits,
                hydration_attempt_count=self._hydration_attempt_count,
                hydration_success_count=self._hydration_success_count,
                hydration_failure_count=self._hydration_failure_count,
                hydration_latency_ns_total=self._hydration_latency_ns_total,
                hydration_latency_ns_max=self._hydration_latency_ns_max,
                corruption_failures=self._corruption_failures,
                recovery_failures=self._recovery_failures,
                saturated=self._saturated,
            )


__all__ = [
    "PROMPT_ARCHIVE_FORMAT_VERSION",
    "PROMPT_ARCHIVE_METRICS_SCHEMA_VERSION",
    "PromptArchiveMetrics",
    "PromptArchiveMetricsSnapshot",
]
