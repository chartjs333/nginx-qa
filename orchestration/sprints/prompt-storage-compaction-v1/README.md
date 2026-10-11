# Prompt Storage Compaction v1 managed sprint

This sprint fixes unbounded prompt archive growth without discarding audit,
review, patch-provenance, or recovery evidence.

The source prototype established that large sequential transition archives can
be reduced substantially by extracting repeated values into content-addressed
storage.  It is an analysis prototype, not production code.  In particular,
its canonical JSON equality check is not proof that the original file bytes can
be reconstructed, and whole-field blobs do not prevent quadratic growth when a
history array changes on every transition.

## Sprint artifacts

- `sequential-sprint.json` is the managed sprint definition.
- `requirements.md` is the normative implementation and safety contract.
- `acceptance-matrix.md` is the mandatory qualification matrix.
- `prototype-audit.md` records what the supplied prototype does and does not
  prove.
- `reference/prompt_storage_compactor_prototype.py` is an exact byte snapshot
  of the supplied prototype for inspection only; do not execute it on live data.

## Managed graph

| Task | Node | Purpose |
| --- | --- | --- |
| PSC-001 | `storage-contract` | Freeze the versioned storage/API, integrity, compatibility, and threat-model contract. |
| PSC-002 | `cas-core` | Implement crash-safe immutable CAS primitives and schemas. |
| PSC-003 | `incremental-archive` | Store append-only history as event/chunk deltas instead of whole growing snapshots. |
| PSC-004 | `runtime-integration` | Keep identity/task hot paths light and expose authorized on-demand hydration. |
| PSC-005 | `migration-recovery` | Add dry-run/shadow migration, recovery, rollback, and legacy compatibility. |
| PSC-006 | `qualification` | Run fault, concurrency, security, compatibility, and isolated performance qualification. |
| PSC-007 | `release-plan` | Produce the exact merge/rollout/rollback plan without merging or deploying. |

Every non-terminal result requires two independent managed reviews: one for
runtime/storage correctness and one for evidence/security/operations.

## Hard boundaries

- Work only in the managed workspace and assigned branch returned by managed
  identity.
- Do not read, compact, rewrite, delete, or rename live `D:\\Prompt` data.
- Do not touch live services or their runtime/state roots.
- Use generated fixtures or explicitly isolated copies for all tests and
  benchmarks.
- Do not enable migration or compaction by default.
- Do not implement garbage collection or deletion in v1.
- Do not merge to `main`, deploy, or promote live without a separate operator
  decision after the sprint completes.
