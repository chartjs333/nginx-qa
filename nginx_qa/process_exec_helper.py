"""POSIX exec gate for managed-child resource limits.

This helper runs in the child process, before the repository-owned executable.
It exists because ``subprocess.Popen(preexec_fn=...)`` is unsafe when nginx-qa
has background threads.  No shell is involved and the helper immediately
replaces itself with the exact argv supplied by the supervisor.
"""

from __future__ import annotations

import json
import math
import os
import resource
import sys
from typing import Any, Mapping


def _positive_integer(value: Any, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def apply_limits(raw: Mapping[str, Any]) -> None:
    memory = _positive_integer(raw.get("memory_bytes"), "memory_bytes")
    processes = _positive_integer(raw.get("process_count"), "process_count")
    wall_seconds = _positive_integer(
        raw.get("wall_time_seconds"), "wall_time_seconds"
    )
    cpu_percent = _positive_integer(raw.get("cpu_percent"), "cpu_percent")
    cpu_seconds = max(1, math.ceil(wall_seconds * cpu_percent / 100))
    resource.setrlimit(resource.RLIMIT_AS, (memory, memory))
    if hasattr(resource, "RLIMIT_NPROC"):
        resource.setrlimit(resource.RLIMIT_NPROC, (processes, processes))
    resource.setrlimit(resource.RLIMIT_CPU, (cpu_seconds, cpu_seconds))


def main(arguments: list[str] | None = None) -> int:
    values = list(sys.argv[1:] if arguments is None else arguments)
    if len(values) < 3:
        raise SystemExit(
            "usage: process_exec_helper.py LIMITS_JSON GATE_FD EXECUTABLE [ARG ...]"
        )
    limits = json.loads(values[0])
    if not isinstance(limits, dict):
        raise ValueError("resource limits must be a JSON object")
    gate_fd = _positive_integer(int(values[1]), "gate fd")
    executable = values[2]
    argv = values[2:]
    try:
        released = os.read(gate_fd, 1)
    finally:
        os.close(gate_fd)
    if released != b"G":
        # Parent exit closes the pipe.  Until the durable PID/group receipt is
        # published the helper can only wait here or exit; it cannot exec the
        # assignment or create descendants.
        return 125
    apply_limits(limits)
    os.execve(executable, argv, dict(os.environ))
    raise AssertionError("os.execve returned unexpectedly")


if __name__ == "__main__":
    raise SystemExit(main())
