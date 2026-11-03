"""Offline synthetic projection benchmark. No main import, sockets or real state.

Run from repository root: python -m tools.benchmark_execution_projection.
The optional output is a new evidence file, never a runtime registry.
"""
from __future__ import annotations

import argparse
import asyncio
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import statistics
import subprocess
import time
import tracemalloc
import types
from unittest import mock

from nginx_qa import execution_observability as execution


def fixture(revisions=12, assignments=30, payload_bytes=8192):
    snapshot = {"project_id": "benchmark-project", "sprint_id": "benchmark-sprint",
                "pending_work": {"worker": []}, "recent_activity": [],
                "execution": {"mode": "sequential", "status": "active", "revision": 0,
                              "assignments": [], "current_assignment_id": "active"}}
    payload = "synthetic-" + "x" * payload_bytes
    snapshot["execution"]["assignments"] = [
        {"assignment_id": "task-" + str(i), "node_id": "build", "status": "completed",
         "created_at": "2026-01-01T00:00:00Z", "completed_at": "2026-01-01T00:01:00Z",
         "result": payload + str(i), "outcome": "DONE"} for i in range(assignments)]
    snapshot["execution"]["assignments"].append(
        {"assignment_id": "active", "node_id": "build", "status": "active"})
    ledger = {}
    for i in range(revisions):
        snapshot["execution"]["revision"] = i + 1
        execution.append_execution_checkpoint(ledger, snapshot, recorded_at="2026-01-01T00:02:00Z")
    return snapshot, ledger["checkpoints"]


def timed(call):
    start_cpu = time.process_time()
    start = time.perf_counter()
    value = call()
    return value, {"wall_seconds": time.perf_counter() - start,
                   "cpu_seconds": time.process_time() - start_cpu}


async def responsiveness(call):
    gaps = []
    done = False
    async def heartbeat():
        previous = time.perf_counter()
        while not done:
            await asyncio.sleep(.002)
            now = time.perf_counter()
            gaps.append(now - previous)
            previous = now
    task = asyncio.create_task(heartbeat())
    await asyncio.sleep(.004)
    await asyncio.to_thread(call)
    done = True
    await task
    return {"heartbeat_max_gap_seconds": max(gaps), "heartbeat_samples": len(gaps)}


async def http_responsiveness(snapshot, points, reference_commit):
    """Real ASGI route + synthetic managed host, no socket/main/lifespan/state."""
    from tests import test_execution_read_scheduling as harness
    from nginx_qa import scope_api
    if reference_commit:
        code = subprocess.check_output(["git", "show", reference_commit + ":nginx_qa/scope_api.py"],
                                       cwd=Path(__file__).resolve().parents[1])
        scope_api = types.ModuleType("nginx_qa._benchmark_scope_baseline")
        scope_api.__package__ = "nginx_qa"
        scope_api.__file__ = str(Path(__file__).resolve().parents[1] / "nginx_qa" / "scope_api.py")
        exec(compile(code, reference_commit + ":scope_api.py", "exec"), scope_api.__dict__)
        scope_api.project_execution = execution.project_execution
    state = deepcopy(snapshot["execution"])
    state["sprint_id"] = harness.SPRINT
    state["execution_audit"] = {"checkpoints": deepcopy(points)}
    with mock.patch.object(harness, "scope_api", scope_api):
        host = harness.SyntheticHost(state)
    reports = []
    for phase in ("cold", "warm"):
        done = False
        gaps = []
        async def heartbeat():
            previous = time.perf_counter()
            while not done:
                await asyncio.sleep(.002)
                now = time.perf_counter()
                gaps.append(now - previous)
                previous = now
        watcher = asyncio.create_task(heartbeat())
        await asyncio.sleep(.004)
        start = time.perf_counter()
        status, view, _ = await harness.request(host.app)
        elapsed = time.perf_counter() - start
        done = True
        await watcher
        reports.append({"phase": phase, "http_status": status, "wall_seconds": elapsed,
                        "heartbeat_max_gap_seconds": max(gaps), "heartbeat_samples": len(gaps),
                        "output_sha256": hashlib.sha256(json.dumps(view, sort_keys=True,
                             ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()})
    return reports


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--revisions", type=int, default=12)
    parser.add_argument("--assignments", type=int, default=30)
    parser.add_argument("--payload-bytes", type=int, default=8192)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--reference-commit", help="Load only the pure projector from a local Git commit")
    parser.add_argument("--http-responsiveness", action="store_true", help="Also exercise real ASGI routes with synthetic host; no sockets")
    args = parser.parse_args()
    global execution
    runtime_files = ("nginx_qa/execution_observability.py", "nginx_qa/execution_read_cache.py", "nginx_qa/scope_api.py")
    root = Path(__file__).resolve().parents[1]
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    if args.reference_commit:
        ref = subprocess.check_output(["git", "rev-parse", "--verify", args.reference_commit + "^{commit}"], cwd=root, text=True).strip()
        code = subprocess.check_output(["git", "show", ref + ":nginx_qa/execution_observability.py"], cwd=root)
        execution = types.ModuleType("nginx_qa._benchmark_baseline")
        execution.__package__ = "nginx_qa"
        exec(compile(code, ref + ":execution_observability.py", "exec"), execution.__dict__)
        source = {"commit": ref, "projector_sha256": hashlib.sha256(code).hexdigest(), "mode": "pure-projector-from-git"}
    else:
        source = {"commit": head, "mode": "working-tree",
                  "runtime_file_sha256": {name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in runtime_files}}
    snapshot, points = fixture(args.revisions, args.assignments, args.payload_bytes)
    serialized = json.dumps({"snapshot": snapshot, "checkpoints": points}, ensure_ascii=False)
    cache_type = getattr(execution, "HistoricalProjectionCache", None)
    cache = cache_type() if cache_type else None
    def project():
        kwargs = {"history_cache": cache} if cache else {}
        return execution.project_execution(snapshot, checkpoints=points, **kwargs)
    first, cold = timed(project)
    warm = [timed(project)[1] for _ in range(args.repeats)]
    with mock.patch.object(execution, "_timeline", wraps=execution._timeline) as timeline:
        project()
        recomputes = {"warm_timeline_calls_including_current": timeline.call_count,
                      "checkpoint_count": len(points)}
    stages = {
        "synthetic_json_parse": timed(lambda: json.loads(serialized))[1],
        "snapshot_copy": timed(lambda: deepcopy(snapshot))[1],
        "checkpoint_validation_hashes": timed(lambda: [getattr(execution, "_checkpoint_digest", execution._hash)(p["snapshot"]) for p in points])[1],
        "uncached_historical_timeline": timed(lambda: [execution._timeline(p["snapshot"],
            execution._assignments(execution._state(p["snapshot"])),
            execution._records(p["snapshot"].get("scope_requests"))) for p in points])[1],
        "output_cursor_hash": timed(lambda: execution._hash(first))[1],
        "output_json": timed(lambda: json.dumps(first, ensure_ascii=False))[1],
    }
    tracemalloc.start()
    project()
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    report = {"synthetic_only": True, "live_access": False, "source": source,
              "workload": {k: getattr(args, k) for k in ("revisions", "assignments", "payload_bytes", "repeats")},
              "synthetic_json_bytes": len(serialized.encode()), "history_cache_enabled": cache is not None,
              "output_sha256": hashlib.sha256(json.dumps(first, sort_keys=True, ensure_ascii=False,
                    separators=(",", ":"), allow_nan=False).encode()).hexdigest(),
              "cold": cold, "warm": warm,
              "instrumented_recompute_counts": recomputes,
              "warm_wall_median_seconds": statistics.median(s["wall_seconds"] for s in warm),
              "stages": stages, "warm_tracemalloc_peak_bytes": peak,
              "off_thread_responsiveness": asyncio.run(responsiveness(project))}
    if cache:
        report["cache"] = cache.info()
    if args.http_responsiveness:
        report["synthetic_asgi_responsiveness"] = asyncio.run(http_responsiveness(
            snapshot, points, source["commit"] if args.reference_commit else None))
    text = json.dumps(report, indent=2)
    if args.output:
        with args.output.open("x", encoding="utf-8") as handle:
            handle.write(text + "\n")
    print(text)


if __name__ == "__main__":
    main()
