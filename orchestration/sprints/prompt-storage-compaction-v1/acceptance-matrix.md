# Prompt Storage Compaction v1 acceptance matrix

PSC-006 may return `PASS` only when every mandatory row below has inspectable
evidence tied to the exact candidate commit.  Tests use generated data or
isolated copies only; live prompt roots are never inputs or outputs.

| ID | Mandatory proof | Pass condition |
| --- | --- | --- |
| PSC-A01 | Exact legacy reconstruction | For archives with whitespace, reordered keys, Unicode, varied numeric lexemes, and duplicate JSON members, reconstructed raw length and SHA-256 equal the original bytes. Unsupported input fails before publication. |
| PSC-A02 | Semantic digest contract | Canonical algorithm/version is documented and deterministic; canonical equality is never reported as raw-byte equality. |
| PSC-A03 | Reference namespace | User data resembling the reference object round-trips unchanged; unknown ref/schema versions fail closed. |
| PSC-A04 | Deduplication | Identical immutable values produce one verified physical blob and multiple durable references, including concurrent writers. |
| PSC-A05 | Delta growth | A long append-only sequence demonstrates newly written bytes proportional to the delta, not the entire accumulated history; accounting includes manifests/indexes/filesystem overhead. |
| PSC-A06 | Corruption handling | Missing, truncated, wrong-hash, wrong-size, unsupported-compression, cyclic, deeply nested, and oversized content produce stable bounded errors and no partial publication. |
| PSC-A07 | Concurrent publication | Multiple writers racing on the same and different content leave valid immutable blobs/manifests with no shared-temp collision. |
| PSC-A08 | Fault injection | Failures before/after blob write, flush, rename, manifest write, and authoritative-pointer publication are recoverable and never expose dangling refs. |
| PSC-A09 | Idempotent recovery | Restart/replay completes or safely rejects an interrupted operation without duplicate logical records or mutation of verified blobs. |
| PSC-A10 | Legacy compatibility | Existing inline archives remain readable; shadow/dual-write can be disabled; rollback reads both legacy and already-created v1 records. |
| PSC-A11 | Hot-path isolation | Instrumented identity/task/`.prompt` tests prove the full project-state/history builder and blob hydration are not invoked on the default path. Required task/result fields remain present. |
| PSC-A12 | Authorized hydration | Full evidence can be requested on demand only in the correct project/sprint context; cross-project/sprint refs are rejected. |
| PSC-A13 | Filesystem and decompression security | Traversal, aliases, ADS, case collisions, symlink/junction/reparse escape, decompression bombs, and configured-root overlap are rejected. |
| PSC-A14 | API bounds and secrecy | OpenAPI schemas are versioned; maximum response/resource limits are enforced; payloads and secrets do not appear in paths, errors, logs, or metrics. |
| PSC-A15 | Regression suites | Focused prompt/archive tests plus existing sequential prompt, actor import, managed identity, continuity, proposal, Telegram, and schema/contract suites pass. |
| PSC-A16 | Isolated benchmark | Benchmark records logical input, all physical storage, dedup ratio, write/read latency, peak memory, and per-transition amplification on isolated fixtures. |
| PSC-A17 | Live-root invariance | Full pre/post inventory (path, size, mtime, full-file SHA-256) proves no live prompt/runtime/state file was read as test input or changed. |
| PSC-A18 | Disabled-by-default rollout | A fresh isolated instance starts with legacy-authoritative behavior; shadow/new-format writing requires explicit configuration and is observable. |
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
