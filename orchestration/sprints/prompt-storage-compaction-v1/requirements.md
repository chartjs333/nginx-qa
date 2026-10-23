# Prompt Storage Compaction v1 requirements

## 1. Problem statement

Sequential transition diagnostics currently retain repeated full snapshots.
Observed isolated samples contained six archives totalling about 297.76 MiB,
with one response near 71 MiB.  In the largest response, `project_state` was
about 66 MiB and the dominant fields were cumulative history, activity, code
patches, and recent activity.  Exact duplicates also occurred between
`history_with_patches.timeline` and `activity_with_patches`, and between
`history_with_patches.patches` and `code_patches`.

The compact per-agent `.prompt` response already omits the full project state,
but the current response path can still build the heavy state and persist full
`latest-response.json` and transition archives before publishing the compact
file.  Reducing bytes after that work is not sufficient: the hot path must stop
materializing data it does not need.

## 2. Required outcome

Introduce a versioned, lossless, content-addressed archive representation in
which immutable history is stored once, each transition adds storage
proportional to its delta, and ordinary identity/task reads do not hydrate the
full history.  Full evidence remains authorized, integrity-checked, auditable,
and reconstructable on demand.

## 3. Integrity terminology

The implementation and documentation must keep these claims distinct:

- **Structural equality**: decoded JSON values compare equal.
- **Canonical equality**: a specified canonical serialization has the same
  digest.
- **Raw-byte equality**: reconstructed bytes have the same SHA-256 and length
  as the exact legacy archive bytes, including whitespace, key order, numeric
  lexemes, and duplicate object-member spelling.

Any migration or publication that claims lossless preservation of an existing
archive MUST prove raw-byte equality.  If it cannot, it MUST fail closed before
publishing the compact manifest or changing any authoritative pointer.  A
canonical digest may be stored as additional semantic evidence, but it cannot
replace the raw digest.

## 4. Versioned storage contract

The design produced by PSC-001 and the implementation that follows MUST define:

1. An explicit archive-manifest media/schema version and an explicit blob/ref
   version.  A plain singleton object named `$cas_ref` is not sufficient because
   it can collide with user payloads.
2. Immutable blobs addressed by the digest of precisely specified uncompressed
   bytes.  References include algorithm, digest, logical media type, encoding,
   uncompressed size, compressed size when applicable, and schema version.
3. A deterministic, repository-independent shard/path mapping.  Callers never
   supply physical blob paths and cannot escape the configured archive root.
4. A durable manifest/reachability record that binds archive, project, sprint,
   transition, and authorization provenance without copying the heavy value.
5. Limits for reference depth, referenced bytes, compression ratio, nesting,
   and total hydrated response size.
6. Backward-compatible reading of legacy inline archives.  Unknown versions,
   missing blobs, malformed references, size mismatches, and digest mismatches
   fail closed with stable errors.

## 5. Incremental growth contract

CAS of a whole cumulative `timeline`, `patches`, large text transcript, or full
`project_state` is not an acceptable final design.  Appending one event must
not write a new copy of all previous events.

The implementation MUST use immutable event/patch/result records or bounded
chunks with an append-only index (a Merkle or equivalent structure is allowed).
For a fixed-size event series, bytes newly written by transition N must be
bounded by the new payload plus fixed or logarithmic metadata.  Qualification
must demonstrate delta growth over a long synthetic sequence and must account
for blobs, manifests, indexes, and filesystem overhead.

## 6. Crash safety and concurrency

Blob and manifest publication MUST be safe under concurrent writers and fault
injection:

- unique temporary files or exclusive creation;
- complete write, flush, and platform-appropriate fsync before publication;
- atomic replace/rename within the same filesystem;
- verification of an already existing blob before reuse;
- immutable blobs published before a manifest or authoritative pointer;
- no manifest may reference an unverified or unreachable blob;
- idempotent replay/recovery after failure at every publication boundary;
- directory durability where the platform supports it, with explicit Windows
  behavior and tested fallback where it does not;
- no shared predictable temp name for competing writers.

The v1 implementation MUST NOT delete blobs or legacy archives.  Garbage
collection is a separately designed future feature requiring durable
reachability and its own operator gate.

## 7. Runtime and API integration

1. Agent identity, `whoami`, active-task, and compact `.prompt` paths MUST NOT
   call or serialize the full project-state/history builder unless the caller
   explicitly requests an authorized full view.
2. Existing response fields needed to execute and submit work remain complete.
   The compact response exposes a stable URL/reference for full evidence.
3. Full archive hydration is explicit, bounded, project-scoped, and subject to
   the existing authorization model.  Cross-project or cross-sprint references
   are rejected.
4. OpenAPI/response schemas describe compact manifests, hydration errors, and
   compatibility behavior.  Unrelated legacy and managed identity contracts
   remain compatible.
5. Source adapter metadata remains data; it must not become executable adapter
   configuration or leak secrets into blobs, paths, logs, or errors.
6. Metrics/evidence report logical bytes, new physical bytes, dedup hits,
   hydration latency, corruption/recovery failures, and format version without
   logging payload content or secrets.

## 8. Compatibility, migration, and rollback

Rollout starts disabled and uses isolated fixtures.  The implementation MUST
support:

- legacy inline read;
- feature-flagged shadow/dual write where legacy output remains authoritative;
- comparison of the compact reconstruction with the exact legacy raw bytes;
- idempotent dry-run and resume after interruption;
- a rollback that disables new writes while preserving the ability to read all
  data already produced;
- explicit evidence that failures do not replace `latest-response.json`,
  transition archives, or `.prompt` with partial output.

Migration tools require explicit input and output roots, reject overlap with
live/configured prompt roots unless a future separately authorized mode exists,
reject symlink/junction/reparse-point escapes, and never delete inputs.

## 9. Security and resource boundaries

Tests and implementation MUST cover malformed hashes, traversal, alternate
data streams, Windows aliases, case-fold collisions, symlink/junction/reparse
escapes, gzip/decompression bombs, deeply nested references, cycles, oversized
hydration, truncated data, and untrusted user values that resemble references.
All errors and evidence are payload-safe and secret-safe.

## 10. Scope and non-goals

In scope:

- prompt/transition archive storage, compact publication, authorized hydration;
- focused OpenAPI/schema changes;
- migration/analyzer tooling and tests;
- documentation, rollout, and rollback evidence.

Out of scope:

- deleting or rewriting live prompt data;
- live migration, deployment, or promotion;
- garbage collection;
- unrelated orchestration/state/SQLite changes;
- changing managed graph semantics;
- merging to `main` during the sprint.
