# Delta scope-control preparation verification

Date: 2026-10-05 (Europe/Berlin). Status: prepared and tested in an isolated
checkout; no deployment or live amendment was performed.

Base: `9375675cdcc56fd3d05861de9375ceb19a71400c`.
Publication branch: `agent/legacy-scope-control-v1`.
The release SHA is the commit containing this report; the publication handoff
must supply its full immutable SHA for `<FINAL_SHA>` in the
[limited rollout runbook](LEGACY_SCOPE_CONTROL_LIMITED_ROLLOUT.md).

## Tested scope and result

The user narrowed the requested regression to scope-control plus affected
existing paths. The completed release-environment run was:

```powershell
.\.venv\Scripts\python.exe -m unittest `
    tests.test_legacy_scope_control `
    tests.test_project_actor_import `
    tests.test_queue_persistence `
    tests.test_groups `
    tests.test_sequential_prompt_ui `
    tests.test_sprint_type_contract.SprintSchemaContractTests.test_exact_schema_set_meta_validates_and_refs_resolve_offline `
    tests.test_sprint_type_contract.SprintSchemaContractTests.test_every_public_api_schema_has_a_positive_instance `
    -v
```

**118 tests, 114.936 seconds, OK. Failures: 0. Errors: 0. Skips: 0.**

| Suite | Count | Reason |
|---|---:|---|
| Scope control | 7 | Apply, Git binding, credentials, CAS/replay, ACK/context, tamper rejection and continuation |
| Existing actor/import API | 57 | Identity, conditional graph, transitions/reviews, history and legacy compatibility |
| Queue persistence | 7 | Shared delivery and persistence paths |
| Groups | 31 | Shared transaction and queue primitives |
| Sequential prompt/API | 14 | Effective delivery, saved responses and prompt projection compatibility |
| Schema registration/positive contracts | 2 | Both new public schemas and offline references |

Three existing module-level checks in `tests/test_windows_launcher.py` were
called directly (unittest discovery does not collect those functions): all
passed. All 20 PowerShell code blocks in the runbook passed parser validation.
Python compilation and `git diff --check` passed.

Local detailed log (ignored by Git):
`runtime_state/scope-control-targeted-regression.log`.
SHA-256 of the log bytes:
`2c88cd6c1be2d922d5d89638df76c34735744e9d0f24fc0a56bad389057b17b8`.

The isolated `.venv` was copied from the serving release without modifying it.
The copied interpreter confirmed `sys.prefix` is
`D:\nginx-qa-scope-control-prep\.venv`. The serving and copied environments
have the same installed distribution versions, including Python 3.12.1,
FastAPI 0.142.2, Uvicorn 0.54.0, Starlette 1.7.0, Pydantic 2.13.5 and
jsonschema 4.26.0. No dependency upgrade is part of this change.

## Required continuation evidence

`tests.test_legacy_scope_control.LegacyScopeControlTests.test_end_to_end_amendment_survives_reviewed_round_trip`
passed in the completed 118-test run. It starts with historical completed
assignments and a prior formal `NO_GO`, then exercises:

1. Amendment of the already issued coordinator assignment, preserving that
   assignment record and persisted issued active task exactly.
2. Effective instruction retrieval, exact context and ACK, ordinary handoff.
3. Two newly issued reviewer assignments, each with the amended reviewer
   profile, its own context and ACK, and two new `APPROVE` decisions.
4. A new formal-linkage occurrence with effective scope, ACK and `NO_GO`.
5. Two further amended reviews, then a new coordinator occurrence with the
   current effective scope and its own ACK.

Assertions cover new assignment ID uniqueness, historical assignments/results/
approvals remaining equal, unchanged workflow and sprint archive,
`required_approvals=2`, seven new scope bindings/ACKs and append-only history.
Negative checks reject direct queue consumption/deletion, pre-delivery scope
ACK/submission, missing/wrong role tokens, stale context, actor drift and
corruption of the cached effective active task. Seventeen negative manifest
mutations cover topology, transitions, review policy/order, identities,
phones/branches, task IDs, queues and metadata.

## Delta source and copied-state rehearsal

The serving checkout is `D:\nginx-qa-release\umse-9375675`, on the base SHA.
Its only local Git entries remain `M run.bat` and `?? QUICKSTART.md`; both were
inspected and preserved. At the final read-only check, listener PIDs were
34324 on 18025 and 51384 on 8025. These are observations, not constants for
later process termination.

Delta canonical remote and local branch both named
`65074657df5f9ba3f16b7ac4912a563128d902ee`. The base manifest commit was
`0c8fe0426586810c1e1a342d11f0d5c42ce2de16`; Git ancestry and exact source blob
SHA-256 values were verified. The explicit repository map resolves the
non-adjacent source checkout to `D:\delta`.

An isolated copied-state rehearsal applied the exact runbook request:
revision 52 to 53, effective revision 1, sprint `sprint-0001-783ef52c`,
assignment `898cbe80-1f5f-4348-ae65-4e6fc83239bc` preserved, graph not advanced,
queue unchanged. The original active task, all 22 assignment records and the
workflow remained equal. Live `port_git_map.json` SHA-256 before and after
that rehearsal and at the final read-only check was:
`3cd67058f1eb3d5d79f708ec4fcf5165e5f2125b2f68cadc3be1e4ff0c03cb19`.

A retained, access-restricted preparation snapshot is at
`D:\nginx-qa-scope-control-prep\runtime_state\delta-preparation-snapshot-20261005`.
Its five existing authoritative/configuration files were copied with matching
source-before/copy/source-after hashes. The runbook's copied-state validator
passed: project 9000, sprint/assignment/node/phase, revision 52, 22 assignments,
14 decisions (13 APPROVE, 1 REJECT), two-review policy and issued hashes.
Manifest SHA-256:
`ab66725ea032f485ad500f6fa17a560cbaf194d9b36f2fb770309231047c04c1`.
This is a preparation snapshot, not the coherent rollback checkpoint for a
future maintenance window: all writers must first stop and a fresh complete
backup including prompt pointers must be verified as specified by the runbook.
Neither copied state nor credentials are committed.

## Checks not completed or not selected

The broad `unittest discover -s tests -v` run using the older system package
environment was intentionally stopped when the user narrowed verification.
Its log contains 150 completed `ok` results; the next test was interrupted.
It is **not** a full-regression pass and is not the release-environment result.
An earlier detached full run has no recoverable final result and is not claimed.

The final release-environment selection did not run the remaining tests in
`test_sprint_type_contract.py`, nor these unrelated wider suites:

```text
test_auto_refresh_ui.py
test_branch_leases.py
test_configure_telegram.py
test_cycle_graph_ui.py
test_git_provider.py
test_history_ui.py
test_managed_continuity.py
test_managed_continuity_import_recovery.py
test_managed_continuity_merge_git.py
test_managed_import.py
test_managed_workspaces.py
test_pending_sprints_ui.py
test_port_leases.py
test_process_supervisor.py
test_project_manager.py
test_staging_cleanup_reproof.py
test_staging_isolation.py
test_staging_legacy_inventory.py
test_staging_qualification_e2e_runner.py
test_workspace_safety.py
```

Live restart, deployment, scope apply, ACK, handoff, reviews, graph transitions,
real Telegram/Cloudflare operations and live rollback were not executed. They
remain subject to the separate maintenance/apply authorization, not evidence
of an untested automatic deployment. There were no unavailable or skipped
checks in the selected 118-test run.
