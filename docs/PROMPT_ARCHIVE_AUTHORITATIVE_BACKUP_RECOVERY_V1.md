# PAS1 authoritative backup and recovery runbook

Status: operator procedure for a future, separately authorized, isolated PAS1
authoritative deployment. This document is not evidence that a backup, restore,
rollout, production enablement, or disaster-recovery exercise has run. It does
not authorize access to a live root, a deployment, or a mode change.

This procedure deliberately contains no runnable live-root commands. An
operator must translate it into an environment-specific, reviewed change plan
only after receiving authority for the exact instance and exact resolved paths.

## 1. Recovery unit and non-goals

A PAS1 authoritative backup is one cold, coherent recovery unit containing all
of the following authorities from the same quiescent instant:

1. the complete isolated `NGINX_QA_PROMPT_ARCHIVE_ROOT` tree;
2. the complete durable occurrence authority, normally the isolated
   `NGINX_QA_RUNTIME_ROOT/prompt-archive-occurrences/v1` tree, including both
   occurrence receipts and immutable source-metadata projection proofs; and
3. for every enrolled project, the resolved project-specific external
   `latest-response.json`, every immutable transition-specific descriptor stub
   referenced by its archive plans, and the complete sibling
   `latest-response.json.nginx-qa-v1` authority.

The three parts are not independent backup sets. A manifest/pointer without
its occurrence proof, a latest file without its sidecar, or a sidecar without
the owning archive plan is not restorable PAS1 evidence. Never combine parts
from different backup generations.

The copy procedure in
[`LEGACY_SCOPE_CONTROL_LIMITED_ROLLOUT.md`](LEGACY_SCOPE_CONTROL_LIMITED_ROLLOUT.md)
is **not** a PAS1 backup. It predates PAS1 and copies a legacy prompt pointer;
it does not preserve the isolated archive CAS, occurrence receipts,
publication recovery state, fixed pending registries, or the external latest
authority. It remains applicable only to the legacy rollout that it names.

PAS1 V1 performs no garbage collection. Backup or restore must not deduplicate,
prune, normalize, reserialize, decompress, recompress, or otherwise rewrite an
artifact. Temporary files and failed/pending-operation evidence are copied as
opaque filesystem entries because their presence is part of the state that a
fail-closed recovery must classify.

## 2. Required inventory

### 2.1 Isolated archive authority

Copy the complete configured archive root, including empty directories and all
regular files. The required inventory includes, without being limited to:

- project-scoped content-addressed blobs, including record, index/value/edge,
  manifest, plan, pointer, locator, and other typed CAS objects;
- immutable manifest objects and their complete reachable graphs;
- stable generation-one response pointers and immutable archive locators;
- immutable publication plans, `compatibility-result.json` receipts, current
  publication journals, all retained journal generations, and operation lock
  receipts;
- the archive-root pending-operation registry: all 256 stable shard names
  `00` through `ff`, every retained shard generation, and all shard lock
  receipts;
- isolated migration inventories and any other stable artifact reached by a
  plan, journal, pointer, locator, or pending entry; and
- reserved temporary entries and unreachable immutable objects. Their
  authority may be “none”, but backup is not a garbage-collection decision.

The backup manifest must distinguish a missing shard from a present empty
shard. Once a project registry is initialized, the fixed set of 256 shard
names is part of restore validation; a recursive directory listing is not a
substitute for that check.

### 2.2 Durable occurrence authority

Copy the complete configured occurrence-receipt root, including:

- every immutable legacy or native receipt below `receipts/v1`;
- every immutable pre-plan source-metadata projection proof below
  `source-metadata-projections/v1`;
- every corresponding occurrence/projection lock receipt below `locks/v1`;
  and
- all directories and reserved temporary entries below that authority.

Occurrence-receipt bytes bind the durable runtime occurrence. Projection-proof
bytes separately bind the runtime-occurrence locator, canonical screened
projection, and closed projection receipt before an archive plan exists. Both
artifact kinds must stay byte-identical and at the same authority paths. A
backup that keeps `receipts/v1` but omits
`source-metadata-projections/v1` is incomplete even when the same projected
values are visible in a later manifest. Reconstructing an occurrence receipt
from runtime state, a manifest, a projection proof, or a response is forbidden.

### 2.3 External latest authority

For each enrolled durable project, record and copy as one unit:

- the resolved project-specific `latest-response.json` file;
- each immutable `transition-response-<id>.json` descriptor-stub leaf named by
  a backed-up authoritative publication plan; and
- the entire sibling `latest-response.json.nginx-qa-v1` directory.

The sibling directory includes the exact target lock receipt, intended-content
CAS, immutable and current intent plans, immutable and current latest slots,
operation plans, current and retained operation journals, operation locks, and
its own fixed 256-shard pending registry with retained generations and shard
locks. In contract terms this includes:

```text
latest.lock
content/v1/sha256/...
intents/v1/... and intent.json
slots/v1/... and slot.json
operations/v1/.../plan.json
operations/v1/.../journal-generations/...
operations/v1/.../journal.json and operation.lock
pending/v1/shards/<00..ff>.json
pending/v1/generations/<00..ff>/...
pending/v1/locks/<00..ff>.lock
```

The external file, transition stubs, and sidecar are one authority even though
they are not under the PAS1 archive root. A backup that contains only
`latest-response.json`, only the descriptor stubs, or only the sidecar is
invalid.

## 3. Pre-backup gate and quiescence

Before reading or copying any scoped path, the approved change record must name
the exact service instance, project set, code commit/tree, archive mode,
canonical archive root, canonical occurrence root, every canonical external
latest target, and backup destination. The operator must prove that these are
the configured paths rather than a remembered, inferred, aliased, or
caller-supplied path.

The following gate is mandatory:

1. Deny new ingress and new scheduler/publication work for the named instance.
2. Gracefully stop that instance and all of its child writers. Do not stop a
   different instance merely because it uses a similar directory name.
3. Prove that no service, child, migration process, recovery process, or manual
   tool can write any member of the recovery unit. A quiet HTTP endpoint or an
   empty active-assignment list alone is not quiescence evidence.
4. Record any nonterminal publication and latest-operation state without
   editing it. Pending work is expected recovery evidence, not a reason to
   manufacture a terminal journal or remove a registry entry.
5. Keep writers stopped until the source inventory, copy, backup inventory,
   and second source inventory all agree.

If writer quiescence cannot be proved, do not take or label a PAS1 backup. A
filesystem snapshot is acceptable only when its documented consistency model
atomically covers all three authorities; independently timed snapshots do not
create a coherent recovery unit.

## 4. Filesystem identity, volume, and ownership

Resolve every root and target without following a symlink, junction, reparse
point, hard-link alias, alternate data stream, or case-folding alias. Record
the canonical directory-entry spelling, filesystem/volume identity, file type,
link count, owner, and access-control policy. Any ambiguous identity or a
regular file with an unexpected link count fails the backup gate.

Preserve the relationship between each external latest target and its sibling
sidecar. A restore must use the same canonical target and parent, so the
derived target-authority ID remains valid. It must not redirect a sidecar to a
new prompt directory or silently fall back from a project-specific target to a
shared legacy target.

Copying to backup media may cross a volume boundary, but publication-style
temporary files, validation staging, and the final installation of each
restored root must be on the same volume as that root's final canonical path.
This preserves the contract's same-volume atomic replacement boundary. Do not
perform a file-by-file overlay across volumes or across authority paths.

Record and restore the service account ownership and restrictive ACLs before
the service can reopen the data. The backup repository itself must be
restricted and encrypted according to the operator's evidence policy. PAS1
control documents are designed not to contain credentials, but opaque legacy
chunks can contain historical sensitive material; “content addressed” does
not mean “safe to disclose.”

## 5. Full inventory and copy verification

For every entry in all three authorities, the restricted backup manifest must
record at least:

- recovery-unit ID and one frozen backup-generation timestamp;
- source authority and normalized relative path (never a user-supplied path);
- filesystem type, link count, byte size, and complete-file SHA-256;
- owner/ACL identity and filesystem/volume identity needed to validate restore;
- source code commit/tree, configured PAS1 mode, and schema/protocol versions;
- the 256-shard presence bitmap for every initialized archive and sidecar
  pending registry; and
- an aggregate digest over the ordered inventory plus the backup manifest's
  own protected integrity evidence.

Do not put credentials, environment values, bearer tokens, cookies, private
keys, raw payload excerpts, or secret-bearing absolute paths in logs, tickets,
or portable evidence. The restricted manifest may name the approved canonical
roots needed for restoration; a separately shared report must use authority
labels and digests instead. Do not print file contents merely to calculate or
demonstrate a hash.

While the service remains quiescent:

1. Produce a complete source inventory of the exact recovery unit.
2. Copy every entry byte-for-byte to a new empty backup generation. Preserve
   relative names and required metadata; never merge into an older generation.
3. Produce a complete backup inventory and independently read every regular
   file in the copy to calculate its SHA-256.
4. Produce a second complete source inventory.
5. Require exact equality of path set, type, size, and full-file SHA-256 among
   the first source inventory, backup inventory, and second source inventory.
   Also require the recorded ownership, link, reparse, volume, shard-set, and
   target/sidecar relationship checks to pass.
6. Seal the backup generation against modification before allowing writers to
   restart.

Sampling, metadata-only checks, directory timestamps, file counts, CAS names,
or a hash of only JSON control files are insufficient. A digest-shaped filename
does not prove the file bytes. Any mismatch makes the entire generation
unacceptable; do not “repair” the backup by recopying individual files after
the consistency point.

## 6. Fail-closed restore validation

A restore is a cold replacement of one complete recovery unit, never an
in-place repair. Keep the service and every writer stopped. Preserve the entire
pre-restore unit as a separate immutable rollback generation before changing
any target.

Restore the chosen generation into new empty staging directories on the same
volumes and at the same canonical ownership boundaries as the intended final
paths. Before installation, require all of the following:

1. The restored path/type/size/SHA-256 inventory exactly equals the sealed
   backup inventory and contains no unexpected entry, reparse point, hard-link
   alias, alternate stream, or ownership/ACL drift.
2. Each initialized pending registry contains exactly the stable shard names
   `00` through `ff`; every stable shard equals its named retained generation,
   preserves unrelated entries, and has no skipped or forked generation.
3. Every immutable plan and journal validates against its schema, identifier,
   CAS ref, predecessor chain, frozen timestamps, scope, occurrence locator,
   planned reachable set, pointer precondition, locator, compatibility output,
   and pending-entry lower-bound fence.
4. Every occurrence locator resolves to the exact immutable occurrence receipt
   and, when source metadata is present, the exact immutable proof below
   `source-metadata-projections/v1`. The proof's locator and locator digest must
   equal the plan's runtime-occurrence locator; its canonical projection and
   projection receipt must equal the corresponding fields in the occurrence
   receipt. Those same canonical projection bytes and receipt must equal the
   one-record manifest `roots.source_metadata` sidecar and
   `source_metadata_receipt`. Presence is biconditional across the occurrence,
   proof, root, and manifest receipt; mixed presence or any digest, size,
   projected-field, source-type, or counter mismatch is corruption. No
   occurrence is synthesized from another artifact.
5. Every pointer, locator, and manifest agrees in project, sprint, response,
   archive, manifest ref, and complete reachable graph. Every reachable object
   passes stored and uncompressed size/digest checks and bounded hydration.
6. Every external latest current intent and slot equals its retained immutable
   counterpart. Intended content, actual `latest-response.json`, transition
   stub, descriptor digest, source ordering tuple, target-authority ID, and
   archive-owner proof must cross-bind to the exact archive plan, pointer,
   locator, and journal state.
7. Every sidecar and archive pending operation is either terminal and eligible
   for idempotent protocol cleanup or recoverable from its exact registered
   plan and journal lower bound. Unknown, missing, mixed-generation, or
   divergent evidence is corruption, not a request to scan for a replacement.
8. The candidate binary/configuration understands every restored protocol and
   schema version and resolves exactly the recorded roots and target authorities.

Run these checks through the supported read-only validators for the exact
candidate. Do not edit JSON, rebuild an index, rename a retained generation,
advance a journal, remove a pending entry, or overwrite latest content to make
validation pass. Do not allow network-facing readiness or writers until all
checks succeed.

After installation, validate the same inventory and cross-authority bindings
again before a controlled service start. A recovery that needs to resume a
valid pending operation must do so only through the product's normal journaled
recovery path after startup authorization; it is not a manual restore step.

## 7. Disable, rollback, and failed-restore semantics

Changing `NGINX_QA_PROMPT_ARCHIVE_MODE` from `authoritative` to `legacy` or
`shadow` only disables new authoritative archive publication. It does not:

- delete or invalidate an existing archive;
- turn a descriptor stub back into an inline response;
- remove occurrence receipts, plans, journals, pointers, locators, or pending
  operations;
- move an enrolled project back to a shared base latest target; or
- permit a legacy writer to bypass the enrolled target's lock, sidecar,
  archive-owner proof, and journaled latest protocol.

A corrupt or absent sidecar/owner proof fails closed; the runtime must not fall
back to a shared `latest-response.json`. Likewise, restoring the archive root
without its matching external authority must not be presented as a successful
disable or rollback.

Restoring selected legacy bytes to an enrolled latest target is a distinct,
explicitly authorized `legacy_latest_publish` operation with
`write_kind=rollback`. It must carry verified payload root/hash/size, a pending
registry entry, a journal, an ordered latest intent, and the normal colocated
content/slot protocol. A mode toggle, filesystem copy, or manual overwrite is
never that rollback operation.

If validation fails before the service accepts new work, leave the candidate
offline and atomically return the complete preserved pre-restore recovery unit;
never retain a mixture. Revalidate that rollback unit before restart. If any
write or externally accepted result occurred after restore, do not rewind it
with an older backup: stop writers, preserve both generations, and request a
new explicit forward-recovery decision.

## 8. Acceptance record

A future backup or restore evidence package is complete only when it names the
operator authorization, exact instance and code tree, recovery-unit generation,
all authority labels, full inventories and SHA-256 results, quiescence proof,
filesystem identity/ownership checks, semantic validator results, and final
service disposition. The shareable summary must contain no secret or payload
content.

This runbook closes the documentation shape of PSC-A10/B-05 only. Execution on
an isolated candidate, qualification of the procedure, authoritative rollout,
production restore, merge, deploy, and live promotion each remain separately
authorized actions.
