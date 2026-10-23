# Prompt Storage Compaction v1 acceptance matrix

PSC-006 may return `PASS` only when every mandatory row below has inspectable
evidence tied to the exact candidate commit.  Tests use generated data or
isolated copies only; live prompt roots are never inputs or outputs.

| ID | Mandatory proof | Pass condition |
| --- | --- | --- |
| PSC-A01 | Exact legacy reconstruction | For archives with whitespace, reordered keys, Unicode, varied numeric lexemes, and duplicate JSON members, deterministic chunk/tree construction and the frozen renderer reproduce the original raw length and SHA-256. Unsupported renderer/codec/input fails before publication. |
| PSC-A02 | Semantic digest contract | Canonical algorithm/version is documented and deterministic; canonical equality is never reported as raw-byte equality. |
| PSC-A03 | Reference namespace | User data resembling the reference object round-trips unchanged; unknown ref/schema versions fail closed. |
| PSC-A04 | Deduplication | Identical immutable values produce one verified physical blob with the frozen stored-byte encoding and multiple durable references, including concurrent writers; different retries converge on identical refs and tree roots. |
| PSC-A05 | Delta growth | A long append-only sequence and a deeply nested managed `execution_audit.checkpoints` series demonstrate newly written bytes proportional to the delta, not the entire accumulated history. The nested case includes an unknown/moved alias so the proof depends on recursive exact-byte Merkle JSON rather than a finite path allowlist. Accounting includes JSON value/edge nodes, syntax chunks, records, manifests, indexes, journals, compatibility control artifacts, and filesystem allocation. Middle insert/delete is reported separately as the explicit V1 `O(N)` ordinal-index metadata case and is never presented as the append bound. |
| PSC-A06 | Corruption handling | Missing, truncated, wrong-hash, wrong-size, noncanonical codec/base64, unsupported-compression, active-stack cycles, invalid index level/kind, deeply nested, and oversized content produce stable bounded errors and no partial publication; shared DAG nodes remain valid and are budgeted once. |
| PSC-A07 | Concurrent publication | Multiple writers racing on the same and different content leave valid immutable blobs/manifests with no shared-temp collision. |
| PSC-A08 | Fault injection | Failures before/after durable plan, pending-registry insertion, generation-zero journal intent, every journal CAS, blob write/flush/rename, manifest verification, immutable pointer create, archive-locator create, transition descriptor stub, colocated latest-content bytes, current-intent CAS, external latest bytes, latest-slot CAS, terminal intent receipt, and completion are recoverable. Crash-after-pointer-create/before-journal is recognized as success only when exact planned bytes match. |
| PSC-A09 | Idempotent recovery | Restart/replay covers plan-without-journal and every journal state under cross-process shard/operation/response/latest locks; retained predecessor hashes are verified, forks are rejected, exact intended outputs converge, and abort is possible only before immutable pointer publication. The external-target-colocated latest namespace resolves at most one durable current intent without scanning, reconciles it before allocating another revision, preserves a monotonic high-water mark across process/runtime-root resets, treats a greater ordered intent as superseding an older one, and never regresses latest content. Registry journal refs are lower-bound fences, so a crash between journal CAS and registry rebinding cannot strand a valid deterministic successor. |
| PSC-A10 | Legacy compatibility | Existing inline archives remain readable; legacy/shadow paths retain full JSON; gated authoritative response paths contain route-neutral descriptor stubs; an exact inventory proves every runtime instruction, prompt, UI, actor/reviewer, script, and test consumer branches safely on top-level kind. Every legacy identity/state/transition write and operator rollback uses the same external-target-colocated ordered-intent protocol as authoritative latest publication. Fault injection before/between/after transition stub and latest intent/content/slot publication replays to one committed archive without exposing a partial stub or clobbering a newer write. Disable/rollback reads both legacy and already-created v1 records. Runtime shadow/native publication rejects a missing durable sprint as specified. |
| PSC-A11 | Hot-path isolation | Instrumented managed identity/task/`.prompt` tests in every mode and the explicitly gated PAS1-authoritative writer prove the full project-state/history builder and blob hydration are not invoked. Required task/result fields remain present. Legacy diagnostic compatibility is measured separately and is not misreported as the compact PAS1 path. |
| PSC-A12 | Authorized hydration | Managed hydration uses only phone-specific routes: the protected `X-Nginx-QA-Scope-Token`, exact phone, current active durable binding, project/sprint equality, and recomputed effective revision/hash must all agree before locator lookup. Free-form profile/tasks and manifest provenance are never grants. Authenticated loopback operator routes retain their separate boundary. Missing/invalid/stale/cross-scope/absent cases are indistinguishable 404s with non-enumerating logs/latency. |
| PSC-A13 | Filesystem and decompression security | Traversal, aliases, ADS, case collisions, symlink/junction/reparse escape, decompression bombs, and configured-root overlap are rejected. |
| PSC-A14 | API bounds and secrecy | OpenAPI covers operator and managed route families, versioned schemas, bounded locator/pointer lookup, maximum response/resource limits, and fixed errors. `/content` is only `application/json` with attachment, `nosniff`, and `private, no-store`; active/manifest-selected media is rejected. Native source metadata is closed, redacted, data-only, canary-scanned, and cannot reach adapter/executor/network/filesystem/subprocess sinks. Payloads and secrets do not appear in paths, errors, logs, or metrics; opaque legacy raw evidence is explicitly not claimed secret-free. |
| PSC-A15 | Regression suites | Focused prompt/archive tests plus existing sequential prompt, actor import, managed identity, continuity, proposal, Telegram, and schema/contract suites pass. |
| PSC-A16 | Isolated benchmark | Benchmark records logical input, all physical storage, dedup ratio, write/read latency, peak memory, and per-transition amplification on isolated fixtures. |
| PSC-A17 | Live-root invariance | Deny/audit instrumentation and command/input inventory prove zero access to actual live roots; hashing a live file counts as a read and is forbidden without separate operator authority. Full pre/post path, size, mtime, and full-file SHA-256 inventories prove invariance of test-owned isolated protected-root fixtures. |
| PSC-A18 | Disabled-by-default rollout | A fresh isolated instance starts with legacy-authoritative behavior; shadow writing requires explicit configuration, and PAS1-authoritative writing additionally requires the frozen operator gate plus isolated non-overlapping roots. Both are observable; this sprint does not authorize production enablement. |
| PSC-A19 | No deletion or GC | Candidate contains no automatic deletion/compaction of legacy archives or unreferenced blobs and qualification performs none. |
| PSC-A20 | Exact-head release evidence | Final evidence names source commit, candidate commit/tree, changed files, commands, tests, residual risks, enable/disable procedure, and rollback. No merge/deploy occurs. |

## Required negative terminal checks

- A reconstruction mismatch is a hard failure, not a warning.
- Safety-baseline mutation is a hard failure with a non-zero test/tool result.
- A missing or corrupt blob never falls back to unrelated inline or cross-scope
  content.
- A failed shadow write does not corrupt the legacy authoritative response.
- A continuation/retry cannot silently change format version or integrity
  policy.
- A plan-only crash must create exactly one intent (then complete or safely
  abort); a journal fork, false predecessor hash, or concurrent generation
  writer is a hard failure.
- A native complete JSON record stored as opaque chunks, a recursive JSON
  node/edge digest mismatch, a cycle, or an O(total-history) nested checkpoint
  write is a hard failure even if all top-level promoted aliases reconstruct.
- After an uncertain immutable pointer create, only exact intended bytes count
  as prior success; a different document at the same response key is corruption
  and is never overwritten.
- A missing/invalid scope token, inactive or stale binding, effective-scope
  revision/hash mismatch, or cross-project/sprint request returns the same 404
  as absence before any archive locator or filesystem lookup.
- Active content media, missing attachment/nosniff/no-store headers, native
  source-metadata secret canaries, or any adapter/executor side effect is a hard
  failure.
- A crash at either descriptor-stub boundary or any colocated latest
  content/current-intent/external-content/slot/terminal boundary must replay to
  the exact committed bytes. An unresolved current intent blocks later writers
  rather than permitting a clobber, revision reuse, or partial latest stub.
- Every latest writer must use the one lock and control namespace derived from
  the resolved external latest target. A different archive root, an alias of
  that target, runtime-root reset, or missing/corrupt high-water evidence must
  not create a second authority; it fails closed.
