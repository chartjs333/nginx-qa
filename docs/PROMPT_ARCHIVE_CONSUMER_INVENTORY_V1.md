# Prompt Archive authoritative response-path consumer inventory V1

## 1. Status and scope

This is the PSC-A10 inventory for the two compatibility paths returned by a
legacy sequential response:

- `response_file_path`, the immutable response-specific path; and
- `latest_response_file_path`, the ordered project-latest path whose normative
  leaf is `latest-response.json`.

The inventory covers checked-in runtime instructions, launch prompts, the web
UI, legacy actor/reviewer flows, operator tooling, and tests. It distinguishes
a **semantic consumer**, which interprets the JSON stored at one of those
paths, from a **path-only consumer**, which displays, copies, hashes, deletes,
or asserts the existence of bytes without interpreting them as an inline
response.

Inventory status is **complete for PSC-004 implementation and focused
qualification**. The authoritative writer, bounded authorized reader, runtime
instructions, UI labels, actor/reviewer consumers, legacy migration boundary,
reference-prototype boundary, and PAS1 backup/recovery procedure are covered
below. This document does not relabel listed legacy-only or path-only consumers
as authoritative consumers.

The inventory was built with the repository-wide searches:

```text
rg -n --hidden --glob '!.git/**' \
  'response_file_path|latest_response_file_path|latest-response\.json|transition-response'
rg -n --hidden --glob '!.git/**' \
  'read_text\(|read_bytes\(|json\.load|Get-Content|ConvertFrom-Json'
```

Generated evidence, schemas that merely declare a field, storage writers, and
CAS/control-file readers are not semantic compatibility-path consumers. They
are still called out where their tests can otherwise look like a consumer.

## 2. Required dispatch for every semantic consumer

A consumer must perform the following bounded top-level dispatch before it
looks for `project_state`, review context, history, or any other inline field:

1. Parse one bounded UTF-8 JSON object while preserving enough information to
   reject duplicate top-level `kind` members.
2. If the object has **no** top-level `kind`, treat the original bytes as a
   legacy inline response. Preserve the bytes as-is; duplicate members other
   than `kind` are legacy data and are not canonicalized by this decision.
3. If the unique top-level `kind` is
   `prompt_archive_descriptor_stub`, validate the closed canonical stub and
   dispatch only through the project/sprint-scoped authorized PAS1 reader.
   Authorization must complete before locator or archive-filesystem lookup.
   A local path is not an authorization grant.
4. If `kind` is duplicated, non-string, or any other value, fail closed. It is
   not legacy inline content.

The reference implementation is
`PromptArchiveRuntime.read_inline_or_archive()` in
`nginx_qa/prompt_archive_runtime.py:5212-5285`. It returns no-kind legacy bytes
unchanged, rejects duplicate/unknown `kind`, authorizes before lookup, validates
the stub against the selected pointer/manifest, and hydrates under the runtime
limit. Its focused coverage is
`tests/test_prompt_archive_runtime.py:1241-1306`.

For HTTP clients the supported descriptor branch is not direct local-file
hydration. The compact response supplies the authorized route-specific
`prompt_archive.content_url`, `state_url`, and `manifest_url`: content returns
the exact head occurrence, while state performs the bounded root-to-head cold
reconstruction. The operator and managed route families are registered in
`nginx_qa/prompt_archive_api.py`. The
route-neutral file stub intentionally contains no bearer token, caller phone,
correlation ID, or request-specific URL.

## 3. Runtime, prompt, UI, and workflow consumers

| ID | Actual consumer and location | Present behavior | Coverage and status |
| --- | --- | --- | --- |
| R-01 | Runtime recovery text embedded in every compact legacy-sequential response: `main.py`, attached as `full_response_instructions` | States the exact bounded dispatch: absent `kind` is legacy; one string `prompt_archive_descriptor_stub` uses the authorized content URL or bounded reader; duplicate, non-string, or unknown `kind` fails closed. | Migrated and covered by `tests/test_sequential_prompt_ui.py`. **B-01 closed.** |
| R-02 | Standard launch prompt used by the launch-prompt API: `prompts/sequential_sprint_launch_standard.txt`, loaded by `main.py` | States the same no-kind/known-stub/invalid-kind rule and directs descriptor consumers to the authorized bounded route. | Migrated and covered by launch-prompt assertions. **B-01 closed.** |
| R-03 | README operator/developer instruction: `README.md:258-272` | States the complete rule: no `kind` is legacy, the known stub is authorized dispatch, and unknown `kind` fails closed. | Documentation-complete; not executable enforcement. |
| R-04 | Launch-prompt web UI path field and copy action in `main.py` | Path-only. The browser never fetches or parses the file. The mode-neutral label says that the path can name an archive or descriptor, and copy status preserves that distinction. | DOM/path-copy coverage exists in `tests/test_sequential_prompt_ui.py`. **B-02 closed.** |
| R-05 | Common legacy sequential identity/repository flow: `main.py:33649-33707` | Produces a compact HTTP body and `.prompt`, then delegates storage publication to `attach_sequential_graph_response_storage()`. It does not reopen either compatibility path. In authoritative mode it supplies route-specific archive URLs only after a durable occurrence. | Runtime publication is covered by `tests/test_prompt_archive_runtime_integration.py:247-373` and hot-path tests at `:1893-2039`. It depends on R-01/R-02 for a human or agent that elects to inspect a path. |
| R-06 | Project-phone legacy actor and reviewer result/identity flow: `main.py:36784-36827` | Worker and both reviewer transitions share the same compact materializer and recovery instructions. Runtime workflow code does not parse the compatibility files. | Flow coverage exists in `tests/test_project_actor_import.py:2067-2336`; that test's direct archive assertion is separately listed as T-03 because it assumes inline JSON. |
| R-07 | Compatibility materializer and publisher: `main.py:10762-11495` and `main.py:11498-11532` | Producer, not downstream semantic consumer. Legacy/shadow writes no-kind inline JSON. Authoritative writes a route-neutral stub to both compatibility aliases and puts the route-specific descriptor in the compact response. Read-only authoritative identity omits nonexistent response paths. | Mode-specific publication and rollback tests are in `tests/test_prompt_archive_runtime_integration.py:219-417`. Safe writer behavior does not by itself migrate downstream readers. |
| R-08 | Shadow source/retry readers: `_read_prompt_archive_source_file()` and `read_prompt_archive_legacy_source()` at `main.py:422-610`, plus reads at `main.py:11216-11249` and `main.py:11387-11480` | These are bounded internal readers of a transition file selected from trusted configuration and a mode-specific writer plan. They compare or preserve opaque committed bytes; they do not interpret fields as an inline response. They are not a general response-path API. | Covered by source-race and shadow failure/recovery cases in `tests/test_prompt_archive_runtime_integration.py:763-812`, `:979-1058`, and `:1060-1310`. Safe only under their closed legacy/shadow call sites. |
| R-09 | `ExternalLatestCoordinator` in `nginx_qa/prompt_archive_latest.py` | Storage-protocol consumer. It binds bytes to explicit `content_kind=legacy_inline` or `descriptor_stub`, ordered intent, target authority, and (for stubs) archive-owner proof. It does not expose inline response fields. | `tests/test_prompt_archive_latest.py:216-1400` exercises publication, recovery, content-kind binding, owner proof, and target aliases. This is protocol coverage, not proof that UI/agent/script readers dispatch safely. |
| R-10 | Managed phone-specific identity | Does not return or consume these legacy sequential response paths; its workspace/identity contract is separate. | Archive-mode invariance is covered by `tests/test_prompt_archive_runtime_integration.py:2040-2100`. Not a response-path consumer. |

There is no checked-in production call to
`PromptArchiveRuntime.read_inline_or_archive()` other than its focused tests.
That is acceptable only while production consumers use compact HTTP fields and
the authorized archive URLs instead of opening the local files. It must not be
represented as a transparently wired filesystem reader.

## 4. Operator scripts and documented procedures

| ID | Actual consumer and location | Present behavior | Compatibility status |
| --- | --- | --- | --- |
| O-01 | Offline migration CLI `tools/prompt_archive_migrate.py`, backed by `nginx_qa/prompt_archive_migration.py` | Before storage bootstrap, planning, or CAS writes, performs bounded top-level discrimination on every selected response file. No-kind and opaque-invalid legacy bytes remain raw-preserved; recognized descriptor, unknown/non-string, duplicate `kind`, and resource-limit inputs fail with stable errors. | Enforced isolated **legacy-inline input** boundary, including analyze mode; CLI says a path is not authorization. **B-03 closed.** |
| O-02 | Supplied prototype snapshot `orchestration/sprints/prompt-storage-compaction-v1/reference/prompt_storage_compactor_prototype.py` | Preserved byte-exactly at the audited SHA-256. It assumes inline JSON and has no descriptor dispatch. | Explicitly reference-only and prohibited as an operational migration/runtime tool by the sprint README and prototype audit; the supported CLI is O-01. **B-04 closed without altering the evidence snapshot.** |
| O-03 | Legacy Scope Control rollout backup example `docs/LEGACY_SCOPE_CONTROL_LIMITED_ROLLOUT.md:483-490` and `:549-612` | Path-only byte copy/hash of `D:\Prompt\delta\latest-response.json`; it does not parse the response. Its backup set predates PAS1 and omits the authoritative CAS and target-colocated control namespace. | Valid only for the documented legacy deployment. It is not a PAS1 rollback procedure and must not be used as one. The PAS1-specific procedure is [`PROMPT_ARCHIVE_AUTHORITATIVE_BACKUP_RECOVERY_V1.md`](PROMPT_ARCHIVE_AUTHORITATIVE_BACKUP_RECOVERY_V1.md). **B-05 documentation supplied; execution/qualification remains separately gated.** |

No other checked-in `.py`, `.ps1`, `.bat`, `.sh`, JavaScript, or TypeScript
operator script reads either compatibility-path field or the normative
`latest-response.json` leaf. `tools/prompt_archive_migrate.py` is the only
checked-in executable operator entry point found by the inventory search.

## 5. Tests that consume or name compatibility paths

| ID | Test consumer | What it proves and what it does not prove | Status |
| --- | --- | --- | --- |
| T-01 | `tests/test_prompt_archive_runtime.py:1241-1306` | Directly proves raw no-kind legacy preservation (including duplicate non-`kind` members), duplicate-`kind` rejection, authorization before locator lookup, authorized descriptor hydration, denial, and unknown-kind rejection. | Descriptor-aware and complete for the core dispatch primitive. |
| T-02 | `tests/test_prompt_archive_runtime_integration.py:247-373` | Opens the authoritative response and latest paths, verifies identical descriptor-stub bytes and absence of `project_state`, then hydrates through the runtime using the archive identity from the compact response. `:374-417` checks that an explicit legacy write after authoritative enrollment reuses the same latest authority. | Mode-aware integration coverage. The test uses trusted runtime internals, not an end-user local-path authorization bypass. |
| T-03 | `tests/test_project_actor_import.py` actor/reviewer graph coverage | Legacy inline assertions explicitly pin and assert `mode="legacy"`. | **B-06 closed.** |
| T-04 | `tests/test_sequential_prompt_ui.py` | Legacy writer assertions explicitly pin legacy mode. Descriptor instructions and UI wording are covered separately. | **B-06 closed.** |
| T-05 | `tests/test_prompt_archive_response_shape_contract.py` | Freezes exact full legacy JSON aliases/bytes under an explicit legacy registry. | **B-06 closed.** |
| T-06 | `tests/test_prompt_archive_runtime_integration.py:219-246`, `:763-812`, and `:1060-1458` | Reads, deletes, or snapshots legacy/shadow compatibility bytes to prove exact reconstruction and failure isolation. Each relevant test configures `legacy` or `shadow`; it never interprets an authoritative stub as inline state. | Safe mode-specific fixture coverage. Path deletion is isolated test fault injection, not a runtime consumer. |
| T-07 | `tests/test_prompt_archive_latest.py:216-1400` | Opens physical `latest-response.json`, transition stubs, sidecar intents, slots, and journals to test the external latest protocol. Payload semantics are explicitly bound by `content_kind`; descriptor cases use closed descriptor refs and owner proof. | Storage-protocol tests, not general inline consumers. Safe for their fixture role. |
| T-08 | `tests/test_prompt_archive_migration.py` | Exercises filename-based offline migration, exact raw reconstruction, opaque-invalid legacy preservation, and pre-side-effect rejection of descriptor, unknown/non-string, duplicate-kind, and over-limit inputs. | Complete evidence for the supported CLI's enforced legacy-input boundary. **B-03 closed.** The byte-exact prototype remains non-operational by contract (B-04). |
| T-09 | `tests/test_prompt_archive_api.py:250-700` and `tests/test_prompt_archive_auth.py` | Exercise the authorized operator/managed descriptor, manifest, and content routes, including scope failures and stable bounded errors. They do not open a local compatibility path. | Covers the required authorized destination after descriptor dispatch. |
| T-10 | `tests/test_prompt_archive_occurrence_store.py:517`, `tests/test_prompt_archive_metrics.py:129`, contract/schema fixtures, and remaining `response_file_path` assertions | Carry a path as redacted data, validate a schema, or check path existence only. They do not parse the compatibility file as an inline response. | Not semantic consumers; no kind branch is needed unless their behavior expands to reading content. |

## 6. Exact enablement gate

The following enablement gates were closed in PSC-004. Authoritative mode still
remains disabled by default and requires fresh isolated exact-candidate
qualification before any operational enablement:

1. **B-01 — closed:** runtime and launch instructions state and test the exact
   no-kind/known-stub/duplicate-or-unknown fail-closed rule.
2. **B-02 — closed:** the UI uses mode-neutral archive/descriptor wording and
   remains path-copy-only.
3. **B-03 — closed:** offline migration and analyze preflight reject any
   `kind`-bearing or resource-exhausting input before durable side effects.
4. **B-04 — closed:** the supplied prototype remains a byte-exact,
   inspection-only evidence snapshot and is expressly excluded from supported
   operational tooling; only the preflighted O-01 CLI is supported.
5. **B-05 — backup/recovery (documentation closed):**
   [`PROMPT_ARCHIVE_AUTHORITATIVE_BACKUP_RECOVERY_V1.md`](PROMPT_ARCHIVE_AUTHORITATIVE_BACKUP_RECOVERY_V1.md)
   defines one cold recovery unit covering CAS, immutable manifests,
   pointers/locators, occurrence receipts, plans/journals, both fixed-shard
   pending registries, transition stubs, and the external-target-colocated
   latest content/control authority. It requires full-inventory SHA-256 proof,
   exact path/volume/ownership validation, and fail-closed restore. The old
   legacy pointer-copy example is explicitly not a PAS1 backup. No backup or
   restore execution is claimed; an isolated drill and exact-candidate
   qualification remain required before operational enablement.
6. **B-06 — closed:** legacy-only readers explicitly pin legacy mode, and an
   authoritative actor-to-reviewer test hydrates through the authorized shared
   reader with exact project/sprint/archive authorization.
7. Focused consumer, runtime, API/auth, actor/reviewer, UI, migration,
   latest-publication, and contract suites are the exact-candidate release
   evidence gate; their results are recorded in the managed assignment result.

The configuration remains disabled by default (`legacy`) at
`nginx_qa/prompt_archive_runtime.py:290-373`. Even after these blockers close,
authoritative mode additionally requires an isolated archive root and exact
`NGINX_QA_PROMPT_ARCHIVE_OPERATOR_GATE=prompt-archive-authoritative-v1`; that
configuration mechanism is not permission to enable production/live roots.

## 7. Compatibility conclusion

- Existing no-kind inline archives remain readable byte-for-byte through the
  core dispatch primitive.
- Authoritative compatibility paths contain route-neutral descriptor stubs and
  have an authorized bounded hydration destination.
- Compact `.prompt`, active-task, and managed identity paths do not need these
  files to execute work.
- The checked-in PSC-A10 consumer inventory is complete and its identified
  implementation blockers are closed. Legacy remains the default; enabling
  authoritative mode still requires the isolated roots, exact operator gate,
  and exact-candidate qualification described above.
