# Versioned scope and human decisions: delivery verification

## Release identity and isolation

- Requirements: Delta `e72e6f88abf81037a2afec408f8d023980607d66`.
- nginx-qa base: `d28726bbcbb552dd21f8cda1042f39f2ee2a21ec`.
- Checkout: `D:\nginx-qa-scope-v2`.
- Branch: `codex/scope-decisions-graph-ui`.
- Final release identity is the Git commit containing this report, recorded in
  the delivery message. Do not substitute a later moving branch tip.

No live service installation, restart, migration, amendment, ACK, handoff, import
or graph advance was performed by this work. Test mutations use temporary stores,
temporary Git repositories and explicitly isolated ephemeral HTTP servers.

## Delivered behavior

Versioned effective scope retains original issued instructions, amendments,
binding history and exact-context ACK history. Human requests, validations,
authoritative decisions and losing stale attempts are append-only. Decisions use
execution/scope CAS and content-bound validation; retries cannot create another
decision or scope revision. A raw v2 amendment cannot bypass human decisions.

Executors submit semantic boundaries and an immutable Git reference; the server
resolves and verifies its blob hash. The operator uses the common UI to approve,
reject or edit boundaries with an exact server-generated diff. Prior human
authorization is displayed as awaiting application, not a new consent request;
its registration still requires the authenticated operator and exact content.

The universal `/execution` page includes Graph, assignments/visits, Pending
decisions and Timeline, with cursor polling, retained historical selection and
exact saved checkpoints. Execution state, operator attention and optional project
qualification are distinct. Unknown historical facts are not invented.

Normal legacy conditional, queue graph, structured parallel and managed execution
retain their own review/transition gates. Every governed task and new reviewer
requires its role token, effective instruction and exact ACK/context. ACK updates
authorization/audit only: it cannot transition, complete, approve or dequeue.

See [workflow and credential contract](SCOPE_V2_WORKFLOW.md),
[runtime boundaries](SCOPE_V2_RUNTIME_CAPABILITIES.md), and
[deployment/migration plan](SCOPE_V2_DEPLOYMENT.md).

## Reproducible affected regression

Run from the isolated checkout, using the existing pinned test interpreter:

```powershell
& 'D:\nginx-qa-scope-control-prep\.venv\Scripts\python.exe' -B scripts\run_scope_v2_checks.py --output docs\evidence\scope-v2\affected-tests.json
```

The runner records every test outcome, timing and an exact SHA-256 map of source,
tests, schemas and wrappers, and verifies that these inputs did not change during
the run. It excludes runtime state, credentials and recursively self-hashed
evidence. Final results are in [affected-tests.json](evidence/scope-v2/affected-tests.json).

**Final affected run: 293 tests, 292 passed, 1 skipped, 0 failures/errors;
415.476 seconds (6m 55s). Source fingerprint was unchanged throughout.**
The skipped check is the strict RFC3339 format check described below. This is
not a claim that the entire repository regression suite was executed.
Tested source fingerprint:
`491573540364f3e26e0aadbf5ec7bf0ef0ba6743d9ca328f36f40af1524c0c22`.

The first diagnostic run found three failures: the expected schema registry was
missing the three new schemas; a conflict test incorrectly forbade the required
append-only stale-attempt audit; the test environment lacked its declared optional
RFC3339 checker. The first two checks were corrected and retained. The date check
is explicitly unavailable, not replaced by a permissive substitute.

The final requirement review also corrected prior-authorization attention and
server-side Git hash resolution, and prohibited managed child processes from
requesting the protected scope-token namespace through generic secret references.

## Specific acceptance evidence

| Requirement | Concrete verification |
| --- | --- |
| Full continuation through human workflow | `ScopeRevisionTests.test_versioned_round_trip_new_reviews_formal_no_go_reentry_and_next_amendment`: v1 fixture/migration, structured request without client hash, exact validation, registration of existing authorization, scope 2, current coordinator effective GET/ACK, normal handoff, two new reviewer GET/ACK/APPROVE, formal assignment/NO_GO, two more reviews, coordinator re-entry with scope 2, later revisions 3/4. Checks request/decision/amendment/context/ACK linkage and unchanged old history. |
| Concurrent operator decisions | `DecisionLedgerTests.test_every_pair_of_concurrent_decisions_has_one_winner_and_durable_loser`: all nine approve/reject/edit pairs, including different edits; one winner and retained stale attempt. HTTP race in `HumanScopeHTTPTests.test_http_human_decision_race_ack_and_read_only_observability`; managed transaction race in `ScopeRuntimeAdapterTests.test_managed_operator_race_single_authoritative_decision`. |
| Immutable lineage, reload and stale ACK | `ScopeRevisionTests.test_versioned_second_third_scope_preserve_history_ack_and_failure_atomicity`; migration tests; managed multi-revision tests. Persisted JSON reload and prestart validation retain lineage and reject tampering. |
| Future reviewer amendment vs original reviewed result | `ScopeRevisionTests.test_versioned_amend_active_reviewer_next_review_uses_new_scope_same_result`: new reviewer scope, separately hash-linked original reviewed-source context, unchanged exact result and two required reviews. |
| ACK is not execution | Real conditional/parallel/queue/managed API checks preserve assignments, queues and graph pointer; projection test `test_ack_exact_clears_scope_only_preserves_other_gate` retains other pending gates. |
| Failure atomicity / recovery | `test_apply_failure_publishes_neither_decision_nor_partial_scope`, managed transaction rollback, parallel/queue handoff publication-intent recovery and deduplicated retry tests. |
| Credential separation | Operator session/CSRF/origin/pairing tests, credential-body rejection including escaped JSON, managed child environment tests, role-specific result/review/ACK checks. |
| Shared UI | [UI acceptance and screenshots](evidence/scope-v2/UI_ACCEPTANCE.md): actual backend edit/validate/apply in a browser; historical mode, reconnect and another managed fixture. Test-only pairing routes exist only in the isolated harness. |

Additional static checks: `git diff --check`, JavaScript `node --check`, and
PowerShell parser checks for both credential helpers. No dependency was installed
or upgraded for this work.

[Fresh-process evidence](evidence/scope-v2/fresh-process-reopen.json): an additional
run of the complete continuation test (73.181 seconds) then opened its persisted
scope-4 state in two new pinned Python processes. The prestart CLI reported
compatible; the independent reader verified every amendment, binding/ACK history,
current context and complete document/execution hashes against the parent.
Persisted bytes and the tested source fingerprint remained unchanged. Child
processes received no scope-control credential variables. This qualifies
fresh-process state reopening, not a full service restart or deployed lifecycle.

[Credential helper evidence](evidence/scope-v2/CREDENTIAL_HELPER_CHECK.md) records
a real isolated DPAPI CurrentUser roundtrip and four authenticated HTTP requests
on an ephemeral test listener, including two sequential roles and the explicit
mutation guard. No credential appeared in captured output. The test process and
synthetic vault were removed. Source-only inspection confirmed the existing
local wrapper's ciphertext format and header names match; its real vault was
not read or decrypted. Actual launch-account/ACL/decryptability checks remain a
future authorized deployment preflight.

## Real-state compatibility, not a live mutation

[Compatibility evidence](SCOPE_V2_COMPATIBILITY_EVIDENCE.json) is reproducible with
`tools/verify_scope_compatibility.py` against the ignored read-only observation
copy. It performs envelope migration and synthetic revisions 2/3 **in memory**,
verifies old amendment/bindings/ACKs/assignments/reviews and graph topology, and
checks its input bytes remain unchanged. It makes zero HTTP requests.

Observed fixture: project `9000`, sprint `sprint-0001-783ef52c`, revision `74`,
scope `1`, assignment `89e70d7d-7b35-43ff-b5c2-cfe96f8526c6`, 28 assignments and
18 review assignments. This observation is neither current-state CAS nor a
consistent recovery backup. Raw live prompts/configuration are not committed.

Read-only confirmation on 2026-10-05 at 14:10:43 UTC still reported revision 74,
the same active coordinator assignment, scope 1, 28 assignments, 18 reviews and
zero pending queue items. Listener observations: 18025 PID 47132/start
11:07:27.7161375 UTC; 8025 PID 51384/start 2026-10-04 12:12:46.7202721 UTC.
18025 remained at `d28726bbcbb552dd21f8cda1042f39f2ee2a21ec`, with only the
pre-existing modified `run.bat` and untracked `QUICKSTART.md`. Protected 8025
remained at `3c42efc1701a0297d3563d7d31e66e532b072394`. Neither process was stopped
or restarted by this work. Other actors' future actions are not controlled by
this observation.

## Explicitly not qualified / deployment gates

- Strict RFC3339 schema format test: missing declared `jsonschema[format]` dev
  extra in the reused isolated test interpreter. Its strict test remains and
  must run in a fully provisioned qualification environment; object-ID checks
  are separate and still run.
- Full repository regression, unrelated integrations and production load/storage
  campaigns were not requested or executed. The affected set is selected because
  this change touches persistence, identity, queue handoff, managed result/review
  and the shared UI/schema contracts.
- Real deployment, service restart, live migration and live scope application
  remain unauthorized and untested. The existing local pre-amendment launcher is
  not v2-capable: the future deployment checklist requires a reviewed pinned
  replacement, a freeze of all writers, a verified full backup and copy-out
  migration before starting the target service.
- Legacy deployment remains single-serving-writer/single-worker. Unstructured
  historical messages without durable assignment identity cannot be retroactively
  amended. Old timestamps without persisted checkpoints are explicitly unavailable.
- Full append-only checkpoints require disk budgeting; no automatic retention or
  destructive downgrade is supplied. After accepted v2 work, rollback must not
  run v1 code on v2 state or discard accepted history.

Deployment is a separate approval stage. The plan does not authorize taking it.
