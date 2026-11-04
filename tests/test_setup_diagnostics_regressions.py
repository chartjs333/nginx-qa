
"""Runtime regression tests for passive diagnostic isolation and capture admission."""
from __future__ import annotations

import asyncio
from contextlib import closing
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from nginx_qa.setup_diagnostics import (
    DiagnosticSnapshotCollector, DiagnosticReadGate, open_readonly_snapshot, DiagnosticSourceUnavailable,
)


def native_fixture(project_id: str) -> tuple[dict, dict]:
    """Use the engine's real contract fixture, re-keyed for a canonical project."""
    from tests.test_sprint_type_contract import (
        active_runtime_fixture, project_control_fixture, SPRINT_ID, SPRINT_PATH_SEGMENT,
    )
    from nginx_qa.sprint_types import SprintProvenance, managed_occurrence_id, managed_sprint_path_segment, canonical_tuple_sha256
    state, control = active_runtime_fixture(), project_control_fixture()
    identity = dict(state["identity"], project_id=project_id)
    new_id = SprintProvenance(**identity).stable_sprint_id()
    old_occurrence = managed_occurrence_id(SPRINT_ID, 1, "build", 1, [])
    new_occurrence = managed_occurrence_id(new_id, 1, "build", 1, [])
    raw = json.dumps([control, state])
    for old, new in (("project-id", project_id), (old_occurrence, new_occurrence),
                     (SPRINT_PATH_SEGMENT, managed_sprint_path_segment(new_id)), (SPRINT_ID, new_id)):
        raw = raw.replace(old, new)
    control, state = json.loads(raw)
    state["import_attempts"][0]["request_fingerprint"] = canonical_tuple_sha256(
        project_id, "main", state["requested_ref"], state["identity"]["manifest_path"])
    return control, state


def owned_tunnel_fixture(root: Path, now: float) -> tuple[dict, Path]:
    state = root / "owned-state"
    receipt = state / "legacy" / "runtime_state" / "launcher-origin.json"
    receipt.parent.mkdir(parents=True)
    marker = {
        "schema_version": 1, "initialization_id": "8048d5c4-703b-4c4d-b59b-1e289206dfde",
        "ownership": {"instance_id": "fixture-instance", "gateway_port": 18035,
                      "state_base": str(state), "gateway_origin_state": str(receipt)},
    }
    (state / ".nginx-qa-staging-owner.json").write_text(json.dumps(marker), encoding="utf-8")
    receipt.write_text(json.dumps({
        "schema_version": 1, "initialization_id": marker["initialization_id"],
        "instance_id": "fixture-instance", "phase": "READY", "heartbeat": now - 2,
        "upstream": "http://127.0.0.1:18035", "public_origin": "https://fixture.trycloudflare.com",
    }), encoding="utf-8")
    return {
        "NGINX_QA_LAUNCHER_ORIGIN_STATE": str(receipt), "NGINX_QA_STAGING_STATE_BASE": str(state),
        "NGINX_QA_INSTANCE_ID": "fixture-instance", "NGINX_QA_GATEWAY_PORT": "18035",
    }, receipt


class DiagnosticProjectionRegressions(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def collector(self, **kwargs):
        return DiagnosticSnapshotCollector(code_root=self.root / "not-app-git", environment={}, **kwargs)

    @staticmethod
    def checks(snapshot):
        return {c["check_id"]: c for c in snapshot["checks"]}

    def database(self, *, active=False):
        # Native initialization is allowed only for temporary test fixture preparation.
        from nginx_qa.managed_import import ManagedImportStore, empty_project_control
        from nginx_qa.branch_leases import BranchLeaseStore
        database = self.root / "state.sqlite3"
        store = ManagedImportStore(database)
        store._ensure_initialized()
        BranchLeaseStore(self.root, database_name=database.name).ensure_initialized()
        control, state = native_fixture("9000") if active else (empty_project_control("9000"), None)
        with closing(sqlite3.connect(database)) as conn, conn:
            conn.execute("INSERT INTO managed_projects VALUES (?, ?, ?)", ("9000", json.dumps(control), 1))
            if state:
                conn.execute("INSERT INTO managed_sprints VALUES (?, ?, ?, ?, ?)",
                             ("9000", state["sprint_id"], state["status"], 1, json.dumps(state)))
        return database

    def test_missing_store_git_and_registry_never_report_ready(self):
        snapshot = self.collector().collect_snapshot("9000")
        checks = self.checks(snapshot)
        for key in ("project.slot", "engine.runtime", "engine.resources", "engine.delivery",
                    "engine.result_reviews", "engine.recovery", "git.source", "git.publication_authority"):
            self.assertEqual(checks[key]["status"], "UNKNOWN", key)
        self.assertEqual(checks["git.publication_authority"]["evidence_kind"], "NONE")
        self.assertEqual(list(self.root.iterdir()), [])

    def test_no_implicit_store_from_state_base(self):
        # A state root alone is not permission to guess a database layout.
        store = self.database()
        self.assertTrue(store.is_file())
        with patch("nginx_qa.setup_diagnostics.sqlite3.connect", side_effect=AssertionError("unexpected store")):
            self.collector(state_base=self.root)._read_sqlite_store("9000")

    def test_unregistered_control_is_unavailable_even_if_schema_exists(self):
        store = self.database()
        data = self.collector(store_path=store)._read_sqlite_store("9001")
        self.assertFalse(data["available"])
        self.assertIsNone(data["slot_busy"])

    def test_corrupt_and_mismatched_control_fail_closed(self):
        store = self.database()
        for value in ('{broken', json.dumps({"project_id": "9001"}), '{"project_id":"9000","project_id":"9001"}'):
            with closing(sqlite3.connect(store)) as conn, conn:
                conn.execute("UPDATE managed_projects SET control_json=?", (value,))
            data = self.collector(store_path=store)._read_sqlite_store("9000")
            self.assertFalse(data["available"])
            self.assertEqual(data["error_code"], "STATE_CORRUPT")

    def test_native_active_state_is_read_and_no_files_change(self):
        store = self.database(active=True)
        before = {p.relative_to(self.root): p.read_bytes() for p in self.root.rglob("*") if p.is_file()}
        with patch("socket.socket.connect", side_effect=AssertionError("passive network")),              patch("subprocess.run", side_effect=AssertionError("passive process")):
            data = self.collector(store_path=store)._read_sqlite_store("9000")
        after = {p.relative_to(self.root): p.read_bytes() for p in self.root.rglob("*") if p.is_file()}
        self.assertEqual(data["error_code"], None, data)
        self.assertTrue(data["available"])
        self.assertTrue(data["slot_busy"])
        self.assertEqual(data["active_assignment_id"], "assignment-1")
        self.assertEqual(before, after)

    def test_native_corrupt_runtime_and_index_are_rejected(self):
        store = self.database(active=True)
        with closing(sqlite3.connect(store)) as conn, conn:
            state = json.loads(conn.execute("SELECT state_json FROM managed_sprints").fetchone()[0])
            state["identity"]["project_id"] = "9001"
            conn.execute("UPDATE managed_sprints SET state_json=?", (json.dumps(state),))
        data = self.collector(store_path=store)._read_sqlite_store("9000")
        self.assertFalse(data["available"])
        self.assertEqual(data["error_code"], "STATE_CORRUPT")

    def test_uncheckpointed_wal_is_unavailable_without_sidecar_writes(self):
        store = self.database()
        with closing(sqlite3.connect(store)) as writer:
            writer.execute("UPDATE managed_projects SET revision=2")
            writer.commit()
            before = {p.name: p.read_bytes() for p in self.root.iterdir() if p.is_file()}
            with self.assertRaises(DiagnosticSourceUnavailable) as error:
                open_readonly_snapshot(store)
            self.assertEqual(error.exception.error_code, "SOURCE_STALE")
            data = self.collector(store_path=store)._read_sqlite_store("9000")
            self.assertFalse(data["available"])
            self.assertEqual(data["error_code"], "SOURCE_STALE")
            after = {p.name: p.read_bytes() for p in self.root.iterdir() if p.is_file()}
            self.assertEqual(before, after)
        # Closing the fixture writer checkpoints naturally; the collector never does.
        data = self.collector(store_path=store)._read_sqlite_store("9000")
        self.assertTrue(data["available"], data)
        self.assertEqual(data["control_revision"], 2)

    def test_memory_snapshot_is_readonly_and_size_bounded(self):
        store = self.database()
        with self.assertRaises(ValueError):
            open_readonly_snapshot(store, max_bytes=32)
        with closing(open_readonly_snapshot(store)) as reader:
            self.assertEqual(reader.execute("SELECT revision FROM managed_projects").fetchone()[0], 1)
            with self.assertRaises(sqlite3.OperationalError):
                reader.execute("UPDATE managed_projects SET revision=99")

    def test_missing_projection_table_is_unknown_even_without_assignments(self):
        store = self.database()
        with closing(sqlite3.connect(store)) as writer, writer:
            writer.execute("DROP TABLE branch_leases")
        checks = self.checks(self.collector(store_path=store).collect_snapshot("9000"))
        self.assertEqual(checks["engine.resources"]["status"], "UNKNOWN")
        self.assertEqual(checks["engine.delivery"]["status"], "PASS")

    def test_older_store_without_launcher_table_keeps_native_facts_but_no_free_slot_claim(self):
        store = self.database(active=True)
        with closing(sqlite3.connect(store)) as writer, writer:
            writer.execute("DROP TABLE telegram_launcher_operations")
        collector = self.collector(store_path=store)
        checks = self.checks(collector.collect_snapshot("9000"))
        self.assertEqual(checks["engine.runtime"]["status"], "PASS")
        self.assertEqual(checks["project.slot"]["error_code"], "PROJECT_BUSY")
        from nginx_qa.managed_import import empty_project_control
        with closing(sqlite3.connect(store)) as writer, writer:
            writer.execute("DELETE FROM managed_sprints")
            writer.execute("UPDATE managed_projects SET control_json=?", (json.dumps(empty_project_control("9000")),))
        checks = self.checks(collector.collect_snapshot("9000"))
        self.assertEqual(checks["engine.runtime"]["status"], "PASS")
        self.assertEqual(checks["project.slot"]["status"], "UNKNOWN")

    def test_large_terminal_blocked_history_does_not_occupy_new_active_or_free_slot(self):
        store = self.database(active=True)
        _, history = native_fixture("9000")
        # History is not the indexed current runtime and its body is intentionally
        # larger than the capture limit. No current-state claim depends on parsing it.
        history["sprint_id"] = "msv1-" + "f"*64
        history["status"] = "blocked"
        history["diagnostic_test_padding"] = "x"*2097152
        with closing(sqlite3.connect(store)) as writer, writer:
            writer.execute("INSERT INTO managed_sprints VALUES(?,?,?,?,?)",
                ("9000", history["sprint_id"], "blocked", 1, json.dumps(history)))
        collector = self.collector(store_path=store)
        data = collector._read_sqlite_store("9000")
        self.assertTrue(data["available"], data)
        self.assertTrue(data["slot_busy"])
        from nginx_qa.managed_import import empty_project_control
        with closing(sqlite3.connect(store)) as writer, writer:
            writer.execute("DELETE FROM managed_sprints WHERE status='active'")
            writer.execute("UPDATE managed_projects SET control_json=?", (json.dumps(empty_project_control("9000")),))
        data = collector._read_sqlite_store("9000")
        self.assertTrue(data["available"], data)
        self.assertFalse(data["slot_busy"])
        checks = self.checks(collector.collect_snapshot("9000"))
        facts = {f["fact_id"]:f["value"] for f in checks["engine.resources"]["evidence"]}
        self.assertFalse(facts["engine.historical_resources_observed"])
        self.assertEqual(facts["engine.resource_scope"], "indexed_and_preparing_sprints")

    def test_launcher_reservation_occupies_project_slot(self):
        store = self.database()
        with closing(sqlite3.connect(store)) as conn, conn:
            conn.execute("INSERT INTO telegram_launcher_operations VALUES(?,?,?,?,?,?)",
                         ("launch-fixture", "9000", "dedupe", "message", 1, "{}"))
        data = self.collector(store_path=store)._read_sqlite_store("9000")
        self.assertTrue(data["available"], data)
        self.assertTrue(data["slot_busy"])
        checks = self.checks(self.collector(store_path=store).collect_snapshot("9000"))
        self.assertEqual(checks["project.slot"]["error_code"], "PROJECT_BUSY")

    def test_project_resource_counts_ignore_foreign_rows(self):
        store = self.database(active=True)
        with closing(sqlite3.connect(store)) as conn, conn:
            for suffix, assignment in (("own", "assignment-1"), ("foreign", "assignment-other")):
                conn.execute("INSERT INTO managed_port_leases VALUES(?,?,?,?,?,?,?,?,?)",
                             ("port-" + suffix, "instance", "namespace", assignment, None,
                              "127.0.0.1", 18001 if suffix == "own" else 18002, "reserved", "{}"))
                conn.execute("INSERT INTO branch_leases VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                             ("branch-lease-1" if suffix == "own" else "branch-foreign", "main", "example.invalid/x", "mirror-" + suffix,
                              "agent/" + suffix, "agent/" + suffix, assignment, "a"*40, "a"*40,
                              "write", "active", "2026-11-04T00:00:00Z", None))
                conn.execute("INSERT INTO managed_queue_items VALUES(?,?,?,?,?,?)",
                             ("queue-" + suffix, "event-1" if suffix == "own" else "event-other",
                              "ASSIGNMENT_ENQUEUE", "{}", "receipt-" + suffix, "2026-11-04T00:00:00Z"))
        data = self.collector(store_path=store)._read_sqlite_store("9000")
        self.assertTrue(data["available"], data)
        # A lease sharing an assignment ID is insufficient provenance: its ID must
        # also belong to the selected project's native resource state.
        self.assertEqual((data["port_leases_count"], data["branch_leases_count"], data["queue_items_count"]), (0, 1, 1))
        snapshot = json.dumps(self.collector(store_path=store).collect_snapshot("9000"))
        self.assertNotIn("assignment-other", snapshot)
        self.assertNotIn("foreign", snapshot)

    def test_overlarge_state_is_unknown_not_partially_ready(self):
        store = self.database(active=True)
        with closing(sqlite3.connect(store)) as conn, conn:
            conn.execute("UPDATE managed_sprints SET state_json=?", ('{"padding":"' + "x"*2097152 + '"}',))
        data = self.collector(store_path=store)._read_sqlite_store("9000")
        self.assertFalse(data["available"])
        self.assertEqual(data["error_code"], "STATE_CORRUPT")

    def test_canonical_registry_does_not_accept_phone_alias(self):
        path = self.root / "registry.json"
        path.write_text(json.dumps({"projects": {}, "phone_git_contexts": {"9000": {"git_context_key": "wrong"}}}))
        checks = self.checks(self.collector(git_config_path=path).collect_snapshot("9000"))
        self.assertEqual(checks["project.identity"]["status"], "BLOCKED")
        path.write_text(json.dumps({"projects": {"github.com/example/actual": {
            "project_phone": "9000", "git_address": "https://github.com/example/actual.git"}}}))
        checks = self.checks(self.collector(git_config_path=path).collect_snapshot("9000"))
        self.assertEqual(checks["project.identity"]["status"], "PASS")

    def test_project_git_uses_explicit_worktree_pointer_not_application(self):
        app_root, repo_root, common = self.root / "app", self.root / "repo", self.root / "common"
        (app_root / ".git").mkdir(parents=True)
        (app_root / ".git" / "HEAD").write_text("a"*40)
        repo_root.mkdir()
        per_tree = common / "worktrees" / "repo"
        per_tree.mkdir(parents=True)
        (repo_root / ".git").write_text("gitdir: " + str(per_tree))
        (per_tree / "commondir").write_text("../..")
        (per_tree / "HEAD").write_text("ref: refs/heads/feature")
        (common / "packed-refs").write_text("b"*40 + " refs/heads/feature\n")
        kwargs = dict(code_root=app_root, project_metadata={"project_id": "9000"}, environment={})
        missing = DiagnosticSnapshotCollector(**kwargs).collect_snapshot("9000")
        self.assertEqual(self.checks(missing)["git.source"]["status"], "UNKNOWN")
        snapshot = DiagnosticSnapshotCollector(**kwargs, repository_root=repo_root).collect_snapshot("9000")
        check = self.checks(snapshot)["git.source"]
        self.assertEqual(check["status"], "PASS")
        self.assertIn("b"*40, [f["value"] for f in check["evidence"]])
        self.assertEqual(snapshot["source"]["code_commit"], "a"*40)

    def test_invalid_git_head_never_passes(self):
        repo = self.root / "repo"
        (repo / ".git").mkdir(parents=True)
        (repo / ".git" / "HEAD").write_text("garbage" * 20)
        snapshot = self.collector(repository_root=repo, project_metadata={"project_id": "9000"}).collect_snapshot("9000")
        self.assertEqual(self.checks(snapshot)["git.source"]["status"], "UNKNOWN")

    def test_tunnel_requires_matching_owned_marker_and_receipt(self):
        now = time.time()
        environment, receipt = owned_tunnel_fixture(self.root, now)
        collector = DiagnosticSnapshotCollector(environment=environment, now=datetime.fromtimestamp(now, timezone.utc))
        good = self.checks(collector.collect_snapshot("9000", instance_id="fixture-instance"))
        self.assertEqual(good["tunnel.ownership"]["status"], "PASS")
        bad = self.checks(collector.collect_snapshot("9000", instance_id="other-instance"))
        self.assertEqual(bad["tunnel.ownership"]["status"], "BLOCKED")
        body = json.loads(receipt.read_text())
        body["initialization_id"] = "other-init"
        receipt.write_text(json.dumps(body))
        bad = self.checks(collector.collect_snapshot("9000"))
        self.assertEqual(bad["tunnel.ownership"]["status"], "BLOCKED")

    def test_binding_evidence_is_project_instance_scoped(self):
        binding = {"project_id": "9000", "instance_id": "i-1", "binding_id": "bind-1"}
        snapshot = self.collector(binding_metadata=binding).collect_snapshot("9000", instance_id="i-1")
        self.assertEqual(self.checks(snapshot)["worker.binding"]["status"], "PASS")
        snapshot = self.collector(binding_metadata=binding).collect_snapshot("9001", instance_id="i-1")
        self.assertEqual(self.checks(snapshot)["worker.binding"]["status"], "BLOCKED")

    def test_action_lists_and_fingerprint_are_authorization_scoped(self):
        collector = self.collector()
        a = collector.collect_snapshot("9000", actor_id="alice", authorized_action_ids=["diagnostics.refresh"])
        b = collector.collect_snapshot("9000", actor_id="bob", authorized_action_ids=[])
        self.assertEqual(a["available_action_ids"], ["diagnostics.refresh"])
        self.assertEqual(b["available_action_ids"], [])
        self.assertNotEqual(a["authority_fingerprint"], b["authority_fingerprint"])
        self.assertTrue(all(not c["next_action_ids"] for c in b["checks"]))


class SharedDatabaseAdmission(unittest.TestCase):
    def test_all_read_paths_share_image_admission_until_connection_close(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'native.sqlite3'
            with closing(sqlite3.connect(path)) as connection:
                connection.execute('CREATE TABLE marker(value TEXT)')
                connection.commit()
            first = open_readonly_snapshot(path)
            try:
                with self.assertRaises(DiagnosticSourceUnavailable) as raised:
                    open_readonly_snapshot(path)
                self.assertEqual('SOURCE_BUSY', raised.exception.error_code)
            finally:
                first.close()
            with closing(open_readonly_snapshot(path)) as second:
                self.assertEqual(0, second.execute('SELECT COUNT(*) FROM marker').fetchone()[0])
            with self.assertRaises(Exception):
                open_readonly_snapshot(Path(directory) / 'missing.sqlite3')
            with closing(open_readonly_snapshot(path)):
                pass  # failed captures must release admission too


class DiagnosticGateRegressions(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.kwargs = dict(code_root=self.root, environment={}, actor_id="alice", authority_version="policy-v1")

    async def asyncTearDown(self):
        self.temp.cleanup()

    async def test_cancelled_capture_holds_admission_until_thread_exits(self):
        gate = DiagnosticReadGate()
        started, finish = threading.Event(), threading.Event()
        original = DiagnosticSnapshotCollector.collect_snapshot
        captures = 0
        def slow(collector, *args, **kwargs):
            nonlocal captures
            captures += 1
            started.set()
            finish.wait(3)
            return original(collector, *args, **kwargs)
        try:
            with patch.object(DiagnosticSnapshotCollector, "collect_snapshot", slow):
                task = asyncio.create_task(gate.get_snapshot("9000", **self.kwargs))
                self.assertTrue(await asyncio.to_thread(started.wait, 1))
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                with self.assertRaisesRegex(RuntimeError, "DIAGNOSTIC_CAPTURE_BUSY"):
                    await gate.get_snapshot("9001", **self.kwargs)
                self.assertEqual(captures, 1)
                self.assertFalse(await gate.drain(timeout=0.01))
                finish.set()
                self.assertTrue(await gate.drain(timeout=2))
        finally:
            finish.set()
            await gate.drain(timeout=3)

    async def test_timeout_does_not_release_capture(self):
        gate = DiagnosticReadGate()
        finish = threading.Event()
        original = DiagnosticSnapshotCollector.collect_snapshot
        def slow(collector, *args, **kwargs):
            finish.wait(3)
            return original(collector, *args, **kwargs)
        try:
            with patch.object(DiagnosticSnapshotCollector, "collect_snapshot", slow):
                with self.assertRaisesRegex(RuntimeError, "DIAGNOSTIC_CAPTURE_TIMEOUT"):
                    await gate.get_snapshot("9000", **self.kwargs, timeout=0.01)
                with self.assertRaisesRegex(RuntimeError, "DIAGNOSTIC_CAPTURE_BUSY"):
                    await gate.get_snapshot("9001", **self.kwargs)
                finish.set()
                self.assertTrue(await gate.drain(timeout=2))
        finally:
            finish.set()
            await gate.drain(timeout=3)

    async def test_issued_snapshot_is_immutable_and_owner_policy_bound(self):
        gate = DiagnosticReadGate()
        snapshot = await gate.get_snapshot("9000", **self.kwargs)
        ident = snapshot["snapshot_id"]
        snapshot["checks"].clear()
        issued = gate.get_issued_snapshot(ident, "9000", actor_id="alice", authority_version="policy-v1")
        self.assertTrue(issued["checks"])
        for changes in ({"actor_id": "bob"}, {"policy_generation": 2}, {"authority_version": "policy-v2"}, {"instance_id": "other"}):
            context = dict(actor_id="alice", policy_generation=1, authority_version="policy-v1")
            context.update(changes)
            with self.assertRaises(LookupError):
                gate.get_issued_snapshot(ident, "9000", **context)
        with self.assertRaises(LookupError):
            gate.get_issued_snapshot(ident, "9001", actor_id="alice", authority_version="policy-v1")
        await gate.drain()

    async def test_source_change_invalidates_cache_and_issued_id(self):
        config = self.root / "registry.json"
        config.write_text('{"projects":{}}')
        gate = DiagnosticReadGate()
        first = await gate.get_snapshot("9000", **self.kwargs, git_config_path=config)
        config.write_text('{"projects":{},"changed":true}')
        with self.assertRaises(LookupError):
            gate.get_issued_snapshot(first["snapshot_id"], "9000", actor_id="alice", authority_version="policy-v1")
        second = await gate.get_snapshot("9000", **self.kwargs, git_config_path=config)
        self.assertNotEqual(first["snapshot_id"], second["snapshot_id"])
        await gate.drain()

    async def test_cache_is_bounded_and_invalidatable(self):
        gate = DiagnosticReadGate(max_cache_entries=2)
        first = await gate.get_snapshot("9000", **self.kwargs)
        repeat = await gate.get_snapshot("9000", **self.kwargs)
        self.assertEqual(first["snapshot_id"], repeat["snapshot_id"])
        await gate.get_snapshot("9001", **self.kwargs)
        await gate.get_snapshot("9002", **self.kwargs)
        self.assertEqual(len(gate._issued), 2)
        with self.assertRaises(LookupError):
            gate.get_issued_snapshot(first["snapshot_id"], "9000", actor_id="alice", authority_version="policy-v1")
        gate.invalidate("9002")
        self.assertTrue(all(key[0] != "9002" for key in gate._issued))
        await gate.drain()

    async def test_issued_expiry_and_changed_grants_do_not_reuse_cache(self):
        gate = DiagnosticReadGate(issued_ttl=0.02)
        first = await gate.get_snapshot("9000", **self.kwargs, authorized_action_ids=["diagnostics.refresh"])
        revoked = await gate.get_snapshot("9000", **self.kwargs, authorized_action_ids=[])
        self.assertEqual(revoked["available_action_ids"], [])
        self.assertNotEqual(first["snapshot_id"], revoked["snapshot_id"])
        await asyncio.sleep(0.03)
        with self.assertRaises(LookupError):
            gate.get_issued_snapshot(first["snapshot_id"], "9000", actor_id="alice", authority_version="policy-v1")
        await gate.drain()

    async def test_invalidation_during_capture_cannot_repopulate_cache(self):
        gate = DiagnosticReadGate()
        started, finish = threading.Event(), threading.Event()
        original = DiagnosticSnapshotCollector.collect_snapshot
        def slow(collector, *args, **kwargs):
            started.set()
            finish.wait(3)
            return original(collector, *args, **kwargs)
        try:
            with patch.object(DiagnosticSnapshotCollector, "collect_snapshot", slow):
                task = asyncio.create_task(gate.get_snapshot("9000", **self.kwargs))
                self.assertTrue(await asyncio.to_thread(started.wait, 1))
                gate.invalidate("9000", actor_id="alice")
                finish.set()
                with self.assertRaisesRegex(RuntimeError, "DIAGNOSTIC_SOURCE_CHANGED"):
                    await task
                self.assertEqual(gate._issued, {})
        finally:
            finish.set()
            await gate.drain(timeout=3)


if __name__ == "__main__":
    unittest.main()
