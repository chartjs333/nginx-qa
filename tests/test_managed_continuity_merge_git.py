import hashlib
import sqlite3
import subprocess
import tempfile
import unittest
from contextlib import nullcontext
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from nginx_qa.branch_leases import BranchLeaseStore
from nginx_qa.managed_continuity import (
    ManagedContinuityError,
    ManagedContinuityRuntime,
)
from nginx_qa.managed_import import ManagedImportStore, TransactionalSprintImporter
from nginx_qa.sprint_types import (
    canonical_json_bytes,
    canonical_json_sha256,
    managed_activation_invariant_issues,
    managed_integration_id,
    managed_integration_workspace_id,
    managed_result_key,
    managed_transition_token_id,
    mirror_storage_key,
)
from nginx_qa.workspace_manager import WorkspaceRequest
from tests.test_sprint_type_contract import (
    MIRROR_KEY,
    REPOSITORY_KEY,
    SPRINT_ID,
    prepared_integration_runtime_fixture,
    project_control_fixture,
)


FIXED_CLOCK = datetime(2026, 10, 3, 1, 2, 3, 456789, tzinfo=timezone.utc)


class InjectedPostCommitCrash(RuntimeError):
    pass


def _git(repository: Path, *arguments: str, check: bool = True) -> str:
    completed = subprocess.run(
        ["git", "-C", str(repository), *arguments],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if check and completed.returncode != 0:
        raise AssertionError(
            f"git {' '.join(arguments)} failed ({completed.returncode}): "
            f"{completed.stderr}"
        )
    return completed.stdout.strip()


def _replace_exact(value, replacements: dict[str, str]):
    if isinstance(value, dict):
        return {key: _replace_exact(item, replacements) for key, item in value.items()}
    if isinstance(value, list):
        return [_replace_exact(item, replacements) for item in value]
    if isinstance(value, str):
        return replacements.get(value, value)
    return value


class ManagedContinuityMergeGitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.root = Path(self.temporary.name)
        self.origin = self.root / "origin"
        self.origin.mkdir()
        _git(self.origin, "init", "--quiet", "--initial-branch=main")
        _git(self.origin, "config", "user.name", "Fixture Author")
        _git(self.origin, "config", "user.email", "fixture@example.invalid")

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _write_and_commit(self, relative_path: str, content: str, message: str) -> str:
        path = self.origin / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        _git(self.origin, "add", "--", relative_path)
        _git(self.origin, "commit", "--quiet", "-m", message)
        return _git(self.origin, "rev-parse", "HEAD").lower()

    def _divergent_commits(self, *, conflict: bool) -> tuple[str, str, str]:
        base = self._write_and_commit("shared.txt", "base\n", "base")

        _git(self.origin, "switch", "--quiet", "-c", "lint")
        if conflict:
            lint = self._write_and_commit("shared.txt", "lint\n", "lint")
        else:
            lint = self._write_and_commit("lint.txt", "lint\n", "lint")

        _git(self.origin, "switch", "--quiet", "-c", "build", base)
        if conflict:
            build = self._write_and_commit("shared.txt", "build\n", "build")
        else:
            build = self._write_and_commit("build.txt", "build\n", "build")
        return base, build, lint

    def _ancestor_parent_commits(self) -> tuple[str, str, str]:
        base = self._write_and_commit("shared.txt", "base\n", "base")
        _git(self.origin, "switch", "--quiet", "-c", "lint")
        lint = self._write_and_commit("lint.txt", "lint\n", "lint")
        return base, base, lint

    def _runtime(
        self, *, conflict: bool, second_parent_is_ancestor: bool = False
    ) -> tuple[ManagedContinuityRuntime, ManagedImportStore]:
        base_commit, build_commit, lint_commit = (
            self._ancestor_parent_commits()
            if second_parent_is_ancestor
            else self._divergent_commits(conflict=conflict)
        )
        managed_root = self.root / "managed"
        runtime_root = self.root / "runtime"
        config = {
            "http_host": "127.0.0.1",
            "http_port": 18025,
            "service_root": str(self.root / "service"),
            "protected_roots": [str(self.root / "protected")],
            "runtime_root": str(runtime_root),
            "process_runtime_root": str(runtime_root / "processes"),
            "log_root": str(runtime_root / "logs"),
            "pid_root": str(runtime_root / "pids"),
            "lease_root": str(runtime_root / "leases"),
            "prompt_root": str(self.root / "prompts"),
            "managed_root": str(managed_root),
            "git_fetch_timeout_seconds": 120,
            "child_port_start": 18100,
            "child_port_end": 18199,
            "instance_id": "merge-git-test",
            "disable_telegram": True,
            "disable_tunnel": True,
        }
        canonical_remote = "github.com/example/managed-continuity-merge"
        repository_registry = {
            "main": {
                "repository_id": "main",
                "canonical_remote": canonical_remote,
                "transport_url": str(self.origin),
                "credential_reference": None,
            }
        }
        database = runtime_root / "leases" / "managed.sqlite3"
        store = ManagedImportStore(database)
        store._ensure_initialized()
        branch_leases = BranchLeaseStore(database.parent, database_name=database.name)
        branch_leases.ensure_initialized()
        importer = TransactionalSprintImporter(
            config,
            repository_registry,
            store=store,
            branch_leases=branch_leases,
            allow_local_transport=True,
        )
        repository = importer.git_provider.ensure_mirror("main")

        state, _trigger_ids = prepared_integration_runtime_fixture()
        state = _replace_exact(
            state,
            {
                REPOSITORY_KEY: canonical_remote,
                MIRROR_KEY: mirror_storage_key(canonical_remote),
            },
        )
        state["runtime_config"] = deepcopy(config)
        state["repository"].update(
            {
                "repository_key": canonical_remote,
                "canonical_remote": canonical_remote,
                "mirror_storage_key": repository.mirror_storage_key,
                "mirror_path": str(repository.mirror_path.resolve(strict=True)),
            }
        )
        state["graph_revisions"][0]["definition"]["git_address"] = (
            "https://github.com/example/managed-continuity-merge.git"
        )
        state["graph_revisions"][0]["definition_sha256"] = canonical_json_sha256(
            state["graph_revisions"][0]["definition"]
        )

        assignments = {
            assignment["assignment_id"]: assignment
            for assignment in state["assignments"]
        }
        assignments["assignment-1"]["result_commit"] = build_commit
        assignments["lint-assignment"]["result_commit"] = lint_commit
        build_result_key = managed_result_key("assignment-1", "DONE", build_commit)
        lint_result_key = managed_result_key(
            "lint-assignment", "DONE", lint_commit
        )
        result_changes = {
            "assignment-1": (build_result_key, build_commit),
            "lint-assignment": (lint_result_key, lint_commit),
        }
        for receipt in state["result_receipts"]:
            result_key, result_commit = result_changes[receipt["assignment_id"]]
            receipt.update(
                {"result_key": result_key, "result_commit": result_commit}
            )
            receipt["response"].update(
                {"result_key": result_key, "result_commit": result_commit}
            )
        for review in [*state["review_assignments"], *state["reviews"]]:
            result_key, result_commit = result_changes[
                review["source_assignment_id"]
            ]
            review.update(
                {"result_key": result_key, "result_commit": result_commit}
            )
            review["response"]["result_key"] = result_key
        for event in state["outbox"]:
            if event["event_type"] != "REVIEW_ENQUEUE":
                continue
            result_key, _result_commit = result_changes[
                event["payload"]["source_assignment_id"]
            ]
            reviewer_index = event["payload"]["reviewer_index"]
            event["payload"]["result_key"] = result_key
            event["dedupe_key"] = (
                f"enqueue:review:{SPRINT_ID}:{result_key}:{reviewer_index}"
            )

        occurrences = {
            occurrence["node_id"]: occurrence
            for occurrence in state["workflow"]["occurrences"]
        }
        build_token_id = managed_transition_token_id(
            SPRINT_ID,
            occurrences["build"]["occurrence_id"],
            build_result_key,
            "join",
        )
        lint_token_id = managed_transition_token_id(
            SPRINT_ID,
            occurrences["lint"]["occurrence_id"],
            lint_result_key,
            "join",
        )
        token_changes = {
            "build": (build_token_id, build_result_key, build_commit),
            "lint": (lint_token_id, lint_result_key, lint_commit),
        }
        for token in state["workflow"]["transition_tokens"]:
            token_id, result_key, _result_commit = token_changes[
                token["source_node_id"]
            ]
            token.update({"token_id": token_id, "result_key": result_key})
        for journal in state["transition_journal"]:
            node_id = (
                "build" if journal["assignment_id"] == "assignment-1" else "lint"
            )
            token_id, result_key, result_commit = token_changes[node_id]
            journal.update(
                {
                    "result_key": result_key,
                    "result_commit": result_commit,
                    "transition_token_ids": [token_id],
                }
            )
        state["integrations"] = []
        state["integration_workspaces"] = []

        requests = {
            "workspace-1": WorkspaceRequest(
                project_id="project-id",
                sprint_id=SPRINT_ID,
                node_id="build",
                assignment_id="assignment-1",
                repository_id="main",
                source_commit=base_commit,
                access="write",
                assigned_branch="agent/example",
                existing_branch_policy="resume",
            ),
            "workspace-lint": WorkspaceRequest(
                project_id="project-id",
                sprint_id=SPRINT_ID,
                node_id="lint",
                assignment_id="lint-assignment",
                repository_id="main",
                source_commit=base_commit,
                access="read",
            ),
        }
        for workspace in state["workspaces"]:
            root = importer.workspace_manager.expected_root(
                requests[str(workspace["workspace_id"])]
            )
            workspace.update(
                {
                    "expected_root": str(root),
                    "actual_git_toplevel": str(root),
                    "actual_git_dir": str(root / ".git"),
                }
            )

        runtime = ManagedContinuityRuntime(
            importer,
            clock=lambda: FIXED_CLOCK,
            result_verifier=lambda *_: nullcontext(),
        )
        selected = runtime._ready_tokens(state, "join")
        receipts = {
            receipt["result_key"]: receipt for receipt in state["result_receipts"]
        }
        result_keys = [token["result_key"] for token in selected]
        commits = [receipts[result_key]["result_commit"] for result_key in result_keys]
        integration, artifact = runtime._prepare_integration_pair(
            "project-id",
            state,
            target_node_id="join",
            graph_revision=1,
            tokens=selected,
            result_keys=result_keys,
            commits=commits,
            now=FIXED_CLOCK.isoformat(),
        )
        state["integrations"] = [integration]
        state["integration_workspaces"] = [artifact]

        self.assertEqual(managed_activation_invariant_issues(state), ())
        control = project_control_fixture()
        with sqlite3.connect(database) as connection:
            connection.execute(
                "INSERT INTO managed_projects(project_id,control_json,revision) "
                "VALUES(?,?,?)",
                (
                    "project-id",
                    canonical_json_bytes(control).decode("utf-8"),
                    1,
                ),
            )
            connection.execute(
                "INSERT INTO managed_sprints"
                "(project_id,sprint_id,status,fencing_token,state_json) "
                "VALUES(?,?,?,?,?)",
                (
                    "project-id",
                    SPRINT_ID,
                    "active",
                    1,
                    canonical_json_bytes(state).decode("utf-8"),
                ),
            )
            connection.commit()

        return runtime, store

    def _pending_failed_join_repair(
        self,
        runtime: ManagedContinuityRuntime,
        store: ManagedImportStore,
        *,
        suffix: str,
    ) -> tuple[dict, dict, list[str]]:
        content = "repair artifact\n"
        self._write_and_commit(
            "orchestration/README.md", content, f"repair readme {suffix}"
        )
        repair_commit = self._write_and_commit(
            "service.py", content, f"repair service {suffix}"
        )
        digest = hashlib.sha256(content.encode("utf-8")).hexdigest()
        repair_request = {
            "expected_revision": 1,
            "repair_source_commit": repair_commit,
            "idempotency_key": f"direct-repair-{suffix}",
            "patch": {
                "checksum_metadata": [
                    {"path": "orchestration/README.md", "sha256": digest},
                    {"path": "service.py", "sha256": digest},
                ]
            },
        }
        created_at = FIXED_CLOCK.isoformat()

        def fail_prepared_join(state, _connection):
            integration = state["integrations"][0]
            artifact = state["integration_workspaces"][0]
            integration.update(
                {
                    "status": "FAILED",
                    "normalized_error": {"code": "INTEGRATION_FAILED"},
                    "updated_at": created_at,
                }
            )
            artifact.update(
                {
                    "artifact_status": "preserved",
                    "working_tree_state": "clean",
                    "verified_at": created_at,
                }
            )
            runtime._append_integration_failure_context(
                state,
                integration,
                artifact,
                reason_code="JOIN_INTEGRATION_FAILED",
                normalized_error={"code": "INTEGRATION_FAILED"},
                now=created_at,
            )

        store.mutate_runtime_state("project-id", SPRINT_ID, fail_prepared_join)
        runtime.drain_outbox("project-id", SPRINT_ID)
        failed = store.runtime_state("project-id", SPRINT_ID)
        self.assertEqual(managed_activation_invariant_issues(failed), ())
        context = failed["coordinator_contexts"][0]
        trigger_ids = list(context["join_target"]["trigger_token_ids"])
        recovery_request = {
            "assignment_id": context["context_id"],
            "idempotency_key": f"pending-recovery-{suffix}",
            "action": "APPLY_REPAIR",
            "parameters": {"request": repair_request},
        }

        def fault(point: str, _context: dict) -> None:
            if point == "after_recovery_pending":
                raise InjectedPostCommitCrash("after recovery pending")

        runtime.fault_injector = fault
        try:
            with self.assertRaises(InjectedPostCommitCrash):
                runtime.submit_if_managed(
                    "project-id",
                    "2860",
                    canonical_json_bytes(recovery_request),
                    f"stage-pending-{suffix}",
                )
        finally:
            runtime.fault_injector = None
        pending = store.runtime_state("project-id", SPRINT_ID)
        self.assertEqual(pending["recovery_records"][0]["status"], "pending")
        return repair_request, recovery_request, trigger_ids

    def test_merge_no_ff_accepts_microsecond_clock_and_commits_both_parents(self) -> None:
        runtime, store = self._runtime(conflict=False)

        self.assertGreater(runtime.reconcile("project-id", SPRINT_ID), 0)

        state = store.runtime_state("project-id", SPRINT_ID)
        integration = state["integrations"][0]
        artifact = state["integration_workspaces"][0]
        self.assertEqual(integration["status"], "COMMITTED")
        self.assertEqual(integration["author_timestamp"], "2026-10-03T01:02:03Z")
        self.assertEqual(integration["committer_timestamp"], "2026-10-03T01:02:03Z")
        commit = integration["integration_commit"]
        parents = _git(
            Path(artifact["expected_root"]),
            "rev-list",
            "--parents",
            "-n",
            "1",
            commit,
        ).split()
        self.assertEqual(
            parents,
            [commit, *[parent["commit"] for parent in integration["parents"]]],
        )
        assignment = next(
            item
            for item in state["assignments"]
            if item.get("integration_id") == integration["integration_id"]
        )
        self.assertEqual(assignment["source_commit"], commit)
        self.assertEqual(managed_activation_invariant_issues(state), ())

    def test_merge_no_ff_keeps_exact_order_when_second_parent_is_ancestor(self) -> None:
        runtime, store = self._runtime(
            conflict=False,
            second_parent_is_ancestor=True,
        )
        prepared = store.runtime_state("project-id", SPRINT_ID)
        prepared_integration = prepared["integrations"][0]
        expected_parents = [
            parent["commit"] for parent in prepared_integration["parents"]
        ]
        self.assertNotEqual(expected_parents[0], expected_parents[1])
        integration_root = Path(
            prepared["integration_workspaces"][0]["expected_root"]
        )
        self.assertEqual(
            _git(
                integration_root,
                "merge-base",
                expected_parents[0],
                expected_parents[1],
            ),
            expected_parents[1],
        )

        self.assertGreater(runtime.reconcile("project-id", SPRINT_ID), 0)

        state = store.runtime_state("project-id", SPRINT_ID)
        integration = state["integrations"][0]
        artifact = state["integration_workspaces"][0]
        commit = integration["integration_commit"]
        root = Path(artifact["expected_root"])
        self.assertEqual(integration["status"], "COMMITTED")
        self.assertEqual(
            _git(root, "rev-list", "--parents", "-n", "1", commit).split(),
            [commit, *expected_parents],
        )
        self.assertEqual(
            _git(root, "rev-parse", f"{commit}^{{tree}}"),
            _git(root, "rev-parse", f"{expected_parents[0]}^{{tree}}"),
        )
        self.assertEqual(
            _git(root, "status", "--porcelain=v1", "--untracked-files=all"),
            "",
        )
        self.assertEqual(artifact["working_tree_state"], "clean")
        self.assertEqual(managed_activation_invariant_issues(state), ())

    def test_frozen_commit_preserves_partial_duplicate_parent_order(self) -> None:
        runtime, store = self._runtime(conflict=False)
        state = store.runtime_state("project-id", SPRINT_ID)
        integration = deepcopy(state["integrations"][0])
        artifact = state["integration_workspaces"][0]
        root = Path(artifact["expected_root"])
        provider = runtime.importer.git_provider

        first_parent = integration["parents"][0]["commit"]
        second_parent = integration["parents"][1]["commit"]
        self.assertNotEqual(first_parent, second_parent)
        integration["parents"].append(deepcopy(integration["parents"][0]))
        expected_parents = [first_parent, second_parent, first_parent]
        expected_tree = _git(root, "write-tree")

        candidate = runtime._create_frozen_integration_commit(
            provider,
            root,
            integration,
            expected_tree=expected_tree,
        )
        runtime._verify_frozen_integration_commit(
            provider,
            root,
            integration,
            candidate,
            expected_tree=expected_tree,
        )
        provider.runner.run(
            (
                "-C",
                str(root),
                "update-ref",
                "HEAD",
                candidate,
                first_parent,
            ),
            error_code="INTEGRATION_WORKSPACE_FAILED",
        )

        self.assertEqual(
            _git(root, "rev-list", "--parents", "-n", "1", candidate).split(),
            [candidate, *expected_parents],
        )
        self.assertEqual(_git(root, "rev-parse", "HEAD"), candidate)
        self.assertEqual(
            [
                line.removeprefix("parent ")
                for line in _git(root, "cat-file", "-p", candidate).splitlines()
                if line.startswith("parent ")
            ],
            expected_parents,
        )

    def test_merge_conflict_preserves_artifact_and_routes_coordinator(self) -> None:
        runtime, store = self._runtime(conflict=True)

        self.assertGreater(runtime.reconcile("project-id", SPRINT_ID), 0)

        state = store.runtime_state("project-id", SPRINT_ID)
        integration = state["integrations"][0]
        artifact = state["integration_workspaces"][0]
        self.assertEqual(integration["status"], "CONFLICT")
        self.assertEqual(integration["normalized_error"], {"code": "MERGE_CONFLICT"})
        self.assertEqual(artifact["artifact_status"], "preserved")
        self.assertEqual(artifact["working_tree_state"], "conflicted")
        root = Path(artifact["expected_root"])
        self.assertTrue(root.is_dir())
        self.assertEqual(_git(root, "diff", "--name-only", "--diff-filter=U"), "shared.txt")
        context = state["coordinator_contexts"][0]
        self.assertEqual(context["failure_scope"], "integration")
        self.assertEqual(context["reason_code"], "JOIN_MERGE_CONFLICT")
        self.assertEqual(
            context["join_target"]["integration_id"], integration["integration_id"]
        )
        coordinator_events = [
            event
            for event in state["outbox"]
            if event["event_type"] == "COORDINATOR_ENQUEUE"
        ]
        self.assertEqual(len(coordinator_events), 1)
        self.assertEqual(coordinator_events[0]["status"], "delivered")
        self.assertEqual(managed_activation_invariant_issues(state), ())

    def test_reconcile_resumes_after_git_commit_before_runtime_commit(self) -> None:
        runtime, store = self._runtime(conflict=False)
        original_prepare_assignment = runtime._prepare_assignment
        crashed = False

        def crash_after_git_commit(*args, **kwargs):
            nonlocal crashed
            if not crashed:
                crashed = True
                raise InjectedPostCommitCrash("after integration Git commit")
            return original_prepare_assignment(*args, **kwargs)

        with patch.object(
            runtime, "_prepare_assignment", side_effect=crash_after_git_commit
        ):
            with self.assertRaises(InjectedPostCommitCrash):
                runtime.reconcile("project-id", SPRINT_ID)

        interrupted = store.runtime_state("project-id", SPRINT_ID)
        integration = interrupted["integrations"][0]
        artifact = interrupted["integration_workspaces"][0]
        self.assertEqual(integration["status"], "PREPARED")
        committed_head = _git(Path(artifact["expected_root"]), "rev-parse", "HEAD")
        self.assertNotEqual(committed_head, integration["parents"][0]["commit"])
        self.assertEqual(
            len(
                _git(
                    Path(artifact["expected_root"]),
                    "rev-list",
                    "--parents",
                    "-n",
                    "1",
                    committed_head,
                ).split()
            ),
            3,
        )

        restarted = ManagedContinuityRuntime(
            runtime.importer,
            clock=lambda: FIXED_CLOCK,
            result_verifier=lambda *_: nullcontext(),
        )
        self.assertGreater(restarted.reconcile("project-id", SPRINT_ID), 0)
        recovered = store.runtime_state("project-id", SPRINT_ID)
        recovered_integration = recovered["integrations"][0]
        self.assertEqual(recovered_integration["status"], "COMMITTED")
        self.assertEqual(recovered_integration["integration_commit"], committed_head)
        self.assertEqual(len(recovered["integrations"]), 1)
        self.assertEqual(managed_activation_invariant_issues(recovered), ())

    def test_prepared_integration_successor_failure_routes_once(self) -> None:
        runtime, store = self._runtime(conflict=False)

        with patch.object(
            runtime,
            "_prepare_assignment",
            side_effect=ManagedContinuityError(
                "CONTINUITY_RUNTIME_FAILED",
                503,
                "integration-successor-preparation",
            ),
        ):
            self.assertGreater(runtime.reconcile("project-id", SPRINT_ID), 0)

        self.assertEqual(runtime.reconcile("project-id", SPRINT_ID), 0)
        staged = store.runtime_state("project-id", SPRINT_ID)
        integration = staged["integrations"][0]
        contexts = [
            item
            for item in staged["coordinator_contexts"]
            if item["reason_code"] == "SUCCESSOR_PREPARATION_FAILED"
        ]

        self.assertEqual(integration["status"], "PREPARED")
        self.assertIsNone(integration["assignment_id"])
        self.assertEqual(len(contexts), 1)
        self.assertEqual(contexts[0]["failure_scope"], "prepare")
        self.assertEqual(
            contexts[0]["normalized_error"]["phase"],
            "ASSIGNMENT_PREPARE",
        )
        self.assertEqual(
            contexts[0]["normalized_error"]["trigger_token_ids"],
            integration["trigger_token_ids"],
        )
        trigger_ids = set(integration["trigger_token_ids"])
        tokens = {
            item["token_id"]: item
            for item in staged["workflow"]["transition_tokens"]
        }
        self.assertTrue(
            all(tokens[token_id]["status"] == "available" for token_id in trigger_ids)
        )
        self.assertFalse(
            any(
                item.get("integration_id") == integration["integration_id"]
                for item in staged["assignments"]
            )
        )
        self.assertEqual(managed_activation_invariant_issues(staged), ())

    def test_prepared_integration_from_old_revision_is_historical(self) -> None:
        runtime, store = self._runtime(conflict=False)
        current = store.runtime_state("project-id", SPRINT_ID)
        current["graph_revision"] = 2
        current["workflow"]["graph_revision"] = 2

        with patch.object(
            runtime.importer.workspace_manager,
            "integration_workspace_root",
        ) as integration_workspace_root:
            self.assertFalse(
                runtime._resume_one_prepared_integration(
                    "project-id",
                    SPRINT_ID,
                    current,
                )
            )

        integration_workspace_root.assert_not_called()

    def test_blocked_prepared_integration_does_not_starve_later_one(self) -> None:
        runtime, store = self._runtime(conflict=False)
        current = store.runtime_state("project-id", SPRINT_ID)
        blocked = deepcopy(current["integrations"][0])
        blocked["integration_id"] = "blocked-integration"
        blocked["trigger_token_ids"] = ["blocked-token"]
        current["integrations"].insert(0, blocked)

        def is_blocked(_state, *, target_node_id, trigger_token_ids):
            self.assertEqual(target_node_id, "join")
            return tuple(trigger_token_ids) == ("blocked-token",)

        with (
            patch.object(
                runtime,
                "_has_successor_preparation_failure",
                side_effect=is_blocked,
            ),
            patch.object(
                runtime.importer.workspace_manager,
                "integration_workspace_root",
                side_effect=AssertionError("resumable integration selected"),
            ) as integration_workspace_root,
        ):
            with self.assertRaisesRegex(
                AssertionError,
                "resumable integration selected",
            ):
                runtime._resume_one_prepared_integration(
                    "project-id",
                    SPRINT_ID,
                    current,
                )

        integration_workspace_root.assert_called_once()

    def test_mirror_pin_failure_keeps_prepared_and_reuses_exact_head(self) -> None:
        runtime, store = self._runtime(conflict=False)

        with patch.object(
            runtime.importer.git_provider,
            "pin_commit",
            side_effect=InjectedPostCommitCrash("during integration mirror pin"),
        ):
            with self.assertRaises(InjectedPostCommitCrash):
                runtime.reconcile("project-id", SPRINT_ID)

        interrupted = store.runtime_state("project-id", SPRINT_ID)
        integration = interrupted["integrations"][0]
        artifact = interrupted["integration_workspaces"][0]
        root = Path(artifact["expected_root"])
        committed_head = _git(root, "rev-parse", "HEAD")
        expected_parents = [parent["commit"] for parent in integration["parents"]]
        self.assertEqual(integration["status"], "PREPARED")
        self.assertIsNone(integration["integration_commit"])
        self.assertEqual(
            _git(root, "rev-list", "--parents", "-n", "1", committed_head).split(),
            [committed_head, *expected_parents],
        )
        self.assertEqual(interrupted["coordinator_contexts"], [])

        restarted = ManagedContinuityRuntime(
            runtime.importer,
            clock=lambda: FIXED_CLOCK,
            result_verifier=lambda *_: nullcontext(),
        )
        self.assertGreater(restarted.reconcile("project-id", SPRINT_ID), 0)

        recovered = store.runtime_state("project-id", SPRINT_ID)
        recovered_integration = recovered["integrations"][0]
        self.assertEqual(recovered_integration["status"], "COMMITTED")
        self.assertEqual(recovered_integration["integration_commit"], committed_head)
        self.assertEqual(_git(root, "rev-parse", "HEAD"), committed_head)
        self.assertEqual(len(recovered["integrations"]), 1)
        self.assertEqual(recovered["coordinator_contexts"], [])
        self.assertEqual(managed_activation_invariant_issues(recovered), ())

    def test_pending_repair_adopts_direct_prepared_integration_effect(self) -> None:
        runtime, store = self._runtime(conflict=False)
        repair_request, recovery_request, trigger_ids = (
            self._pending_failed_join_repair(
                runtime, store, suffix="prepared-adoption"
            )
        )

        with patch.object(
            runtime, "_resume_one_prepared_integration", return_value=False
        ):
            direct = runtime.repair(
                SPRINT_ID, repair_request, "direct-prepared-repair"
            )
            interrupted = store.runtime_state("project-id", SPRINT_ID)
            expected_integration_id = managed_integration_id(
                SPRINT_ID, 2, "join", trigger_ids
            )
            expected_workspace_id = managed_integration_workspace_id(
                SPRINT_ID, 2, "join", trigger_ids
            )
            prepared = next(
                item
                for item in interrupted["integrations"]
                if item["integration_id"] == expected_integration_id
            )
            self.assertEqual(prepared["status"], "PREPARED")
            self.assertEqual(interrupted["recovery_records"][0]["status"], "pending")

            resumed = runtime.submit_if_managed(
                "project-id",
                "2860",
                canonical_json_bytes(recovery_request),
                "resume-prepared-repair",
            )
            replay = runtime.submit_if_managed(
                "project-id",
                "2860",
                canonical_json_bytes(recovery_request),
                "replay-prepared-repair",
            )

        recovered = store.runtime_state("project-id", SPRINT_ID)
        repair_id = recovered["repairs"][0]["repair_id"]
        expected_ids = [
            repair_id,
            expected_integration_id,
            expected_workspace_id,
        ]
        self.assertFalse(direct.response["deduplicated"])
        self.assertEqual(resumed.response["produced_record_ids"], expected_ids)
        self.assertFalse(resumed.response["deduplicated"])
        self.assertEqual(replay.response["produced_record_ids"], expected_ids)
        self.assertTrue(replay.response["deduplicated"])
        self.assertEqual(recovered["recovery_records"][0]["status"], "completed")
        self.assertEqual(len(recovered["repairs"]), 1)
        self.assertEqual(
            sum(
                item["integration_id"] == expected_integration_id
                for item in recovered["integrations"]
            ),
            1,
        )
        self.assertEqual(
            sum(
                item["workspace_artifact_id"] == expected_workspace_id
                for item in recovered["integration_workspaces"]
            ),
            1,
        )
        recovered_prepared = next(
            item
            for item in recovered["integrations"]
            if item["integration_id"] == expected_integration_id
        )
        self.assertEqual(recovered_prepared["status"], "PREPARED")
        self.assertEqual(managed_activation_invariant_issues(recovered), ())

    def test_repair_adoption_recognizes_committed_integration_effect(self) -> None:
        runtime, store = self._runtime(conflict=False)
        repair_request, recovery_request, trigger_ids = (
            self._pending_failed_join_repair(
                runtime, store, suffix="committed-adoption"
            )
        )

        expected_integration_id = managed_integration_id(
            SPRINT_ID, 2, "join", trigger_ids
        )
        expected_workspace_id = managed_integration_workspace_id(
            SPRINT_ID, 2, "join", trigger_ids
        )
        with patch.object(
            runtime, "_resume_one_prepared_integration", return_value=False
        ):
            direct = runtime.repair(
                SPRINT_ID, repair_request, "direct-committed-repair"
            )
            resumed = runtime.submit_if_managed(
                "project-id",
                "2860",
                canonical_json_bytes(recovery_request),
                "resume-committed-repair",
            )
            replay = runtime.submit_if_managed(
                "project-id",
                "2860",
                canonical_json_bytes(recovery_request),
                "replay-committed-repair",
            )

        self.assertGreater(runtime.reconcile("project-id", SPRINT_ID), 0)

        recovered = store.runtime_state("project-id", SPRINT_ID)
        repair_id = recovered["repairs"][0]["repair_id"]
        expected_ids = [
            repair_id,
            expected_integration_id,
            expected_workspace_id,
        ]
        committed = next(
            item
            for item in recovered["integrations"]
            if item["integration_id"] == expected_integration_id
        )
        context = recovered["coordinator_contexts"][0]
        self.assertFalse(direct.response["deduplicated"])
        self.assertEqual(resumed.response["produced_record_ids"], expected_ids)
        self.assertFalse(resumed.response["deduplicated"])
        self.assertEqual(replay.response["produced_record_ids"], expected_ids)
        self.assertTrue(replay.response["deduplicated"])
        self.assertEqual(recovered["recovery_records"][0]["status"], "completed")
        self.assertEqual(len(recovered["repairs"]), 1)
        self.assertEqual(
            sum(
                item["integration_id"] == expected_integration_id
                for item in recovered["integrations"]
            ),
            1,
        )
        self.assertEqual(
            sum(
                item["workspace_artifact_id"] == expected_workspace_id
                for item in recovered["integration_workspaces"]
            ),
            1,
        )
        self.assertEqual(committed["status"], "COMMITTED")
        self.assertEqual(
            runtime._existing_repaired_join_effect(
                "project-id",
                SPRINT_ID,
                recovered,
                context,
                to_revision=2,
                correlation="recognize-committed-effect",
            ),
            (expected_integration_id, expected_workspace_id),
        )
        self.assertEqual(managed_activation_invariant_issues(recovered), ())


if __name__ == "__main__":
    unittest.main()
