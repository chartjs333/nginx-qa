"""Explicit real-provider component check; never starts a sprint or setup action."""
import argparse
import asyncio
import hashlib
import json
from pathlib import Path
import sys
import time
import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from nginx_qa.setup_advisor import SetupAdvisor, snapshot_current
from nginx_qa.setup_ollama_advisor import _BACKGROUND


async def main(args):
    snapshot = json.loads(Path(args.snapshot).read_bytes())
    if not snapshot_current(snapshot):
        raise ValueError("A fresh real issued snapshot is required")
    async with httpx.AsyncClient(timeout=5, follow_redirects=False, trust_env=False) as client:
        async with client.stream("GET", "http://127.0.0.1:11434/api/ps",
                                 headers={"Accept-Encoding": "identity"}) as response:
            if response.status_code != 200 or response.headers.get("content-encoding", "identity") != "identity":
                raise ValueError("Unable to observe local loaded-model metadata")
            raw = bytearray()
            async for chunk in response.aiter_bytes(chunk_size=4096):
                raw.extend(chunk)
                if len(raw) > 65536:
                    raise ValueError("Loaded-model metadata too large")
    loaded = any(m.get("name", m.get("model")) == args.model for m in json.loads(raw).get("models", []))
    if args.require_unloaded and loaded:
        raise ValueError("Selected model is still loaded; do not claim a cold-model check")
    advisor = SetupAdvisor(environment={"NGINX_QA_SETUP_ADVISOR_PROVIDER": "ollama",
        "NGINX_QA_SETUP_ADVISOR_MODEL": args.model}, state_dir=Path(args.state_dir))
    request = {"schema_version": 1, "snapshot_id": snapshot["snapshot_id"],
               "question": args.question, "idempotency_key": "live-" + str(time.time_ns())}
    start = time.monotonic()
    scope = {"actor_id": "qualification-operator", "project_id": snapshot["project_id"]}
    cancellation = {}
    if args.cancel_caller:
        marker = Path(args.state_dir) / "ollama-request.json"
        if marker.exists() or _BACKGROUND:
            raise ValueError("Cancellation qualification requires idle owned admission")
        caller = asyncio.create_task(advisor.get_advice(snapshot, request, **scope))
        deadline = time.monotonic() + 10
        while not marker.exists():
            if caller.done() or time.monotonic() > deadline:
                raise ValueError("No active model request observed")
            await asyncio.sleep(.02)
        if len(_BACKGROUND) != 1:
            raise ValueError("Ambiguous model request ownership")
        background = next(iter(_BACKGROUND))
        await asyncio.sleep(.2)
        if background.done():
            raise ValueError("Model completed before cancellation; no cancellation acceptance")
        caller.cancel()
        try:
            await caller
        except asyncio.CancelledError:
            pass
        else:
            raise AssertionError("Caller was not cancelled")
        cancelled_replay = await advisor.get_advice(snapshot, request, **scope)
        contender = await advisor.get_advice(snapshot, dict(request,
            idempotency_key=request["idempotency_key"] + "-contender"), **scope)
        if ("REQUEST_CANCELLED" not in cancelled_replay["explanation"]
                or "LOCAL_PROVIDER_ACTIVE_OR_UNRESOLVED" not in contender["explanation"]):
            raise AssertionError("Cancellation/re-admission contract failed")
        result = advisor.validate_model_advice(await background, snapshot)
        cancellation = {"caller_cancelled": True, "cancelled_key_replays_cancelled": True,
                        "new_request_blocked_while_inflight": True,
                        "actual_background_model_response_validated": True,
                        "provider_process_kill_claimed": False}
    else:
        result = await advisor.get_advice(snapshot, request, **scope)
    elapsed = round(time.monotonic() - start, 3)
    replay = await advisor.get_advice(snapshot, request, **scope)
    root = Path(__file__).resolve().parents[1]
    files = ("nginx_qa/setup_advisor.py", "nginx_qa/setup_ollama_advisor.py",
             "nginx_qa/setup_diagnostics_api.py", "nginx_qa/setup_diagnostics.py", "nginx_qa/static/setup.js")
    report = {"kind": "real_ollama_production_component_on_issued_snapshot",
        "model": args.model, "duration_seconds": elapsed,
        "model_reported_loaded_before": loaded,
        "status": "PASS" if result["explanation"].startswith("Ответ модели") else "FALLBACK",
        "facts": advisor.project_minimal_facts(snapshot), "result": result,
        "same_request_replay_equal": replay == (cancelled_replay if args.cancel_caller else result),
        "admission_marker_retained": (Path(args.state_dir) / "ollama-request.json").exists(),
        "source_sha256": {p: hashlib.sha256((root / p).read_bytes()).hexdigest() for p in files},
        "native_sprint_started": False, "setup_action_executed": False,
        "browser_end_to_end": "NOT_RUN_SEPARATE_QUALIFICATION_STAGE", **cancellation}
    Path(args.output).write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({k: report[k] for k in ("status", "model", "duration_seconds", "same_request_replay_equal", "admission_marker_retained")}, ensure_ascii=False))
    if report["status"] != "PASS" or not report["same_request_replay_equal"] or report["admission_marker_retained"]:
        raise SystemExit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot", required=True)
    parser.add_argument("--model", default="qwen2.5:7b")
    parser.add_argument("--state-dir", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--question", default="Что проверить следующим шагом? Объясни кратко по диагностике.")
    parser.add_argument("--run-model", action="store_true", required=True)
    parser.add_argument("--cancel-caller", action="store_true")
    parser.add_argument("--require-unloaded", action="store_true")
    asyncio.run(main(parser.parse_args()))
