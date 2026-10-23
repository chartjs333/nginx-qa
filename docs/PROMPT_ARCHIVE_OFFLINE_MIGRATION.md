# Prompt Archive V1 offline migration and recovery

Status: opt-in offline tooling. It is disabled unless an operator supplies the
exact gate `prompt-archive-offline-migration-v1` and a non-secret operator
principal ID. It is not a runtime startup migration, deletion, retention, or
garbage-collection mechanism.

The tool preserves legacy `latest-response.json` and
`transition-response-*.json` files as opaque bytes. It inventories every
regular file under the explicitly selected source, but `.prompt` files and
other non-candidates remain inventory-only. The source is never written,
renamed, or deleted.

## Isolation and authorization

Use three separate locations:

- an offline source copy;
- a new Prompt Archive root;
- a private evidence file outside both roots.

Source, archive, reconstruction, private-evidence, repository, configured
runtime/managed/prompt/service roots, and every `--protected-root` must not
overlap. On Windows `D:\Prompt` is always protected. Extended/device/UNC/8.3,
SUBST, mapped-drive, reparse-point, symlink, hard-link, alternate-data-stream,
case-collision, reserved-device, and trailing-dot/space ambiguity fails closed.
Do not point this tool at a live nginx-qa root.

The deny set also includes the owning bases from the persisted
`runtime_state/sequential-prompt-settings.json`, even when launcher environment
variables are unset. Existing explicitly configured deny roots are checked for
alias/reparse ambiguity without resolving through an alias. Existing source,
archive, private-evidence parent, and reconstruction directories reject NTFS
alternate streams before publication. A lexical overlap is rejected before
the protected tree is probed.

A deterministic partial-temp name is not proof of ownership by itself. On
Windows the exact temp handle rejects named streams before any truncate/write,
is held without write/delete sharing, and is rechecked immediately before the
handle-relative rename. An unsafe pre-existing temp is left unchanged and is
never promoted to the final witness or reconstruction path.

The public archive stores only path-free hashes, IDs, counts, provenance, and
journal state. The exact pre/post paths and filesystem identities exist only in
the explicitly selected private evidence file. Protect that file as sensitive
operator evidence; do not publish it with the archive. Its safe parent
directory must already exist; the migration tool never creates that
operator-selected directory.

## PowerShell commands

Set explicit absolute, isolated paths first:

```powershell
$Source = 'C:\nq-prompt-migration\source-copy'
$Archive = 'C:\nq-prompt-migration\archive-v1'
$PrivateEvidence = 'C:\nq-prompt-migration-private\batch-01.json'
$Reconstruction = 'C:\nq-prompt-migration\reconstructed'
$Project = '9000'
$Handle = 'offline-source-copy-20261011-01'
$Principal = 'operator-example'
$Gate = 'prompt-archive-offline-migration-v1'
```

Analyze is genuinely read-only: it does not open or create the archive or
private-evidence output.

```powershell
python tools\prompt_archive_migrate.py analyze `
  --input-root $Source --archive-root $Archive `
  --project-id $Project --source-root-handle-id $Handle `
  --operator-gate $Gate --operator-principal-id $Principal `
  --protected-root 'C:\nginx-qa-live' `
  --protected-root 'D:\nginx-qa-live'
```

Migrate publishes an immutable batch reservation, an append-only journal chain,
accepted per-entry PAS1 artifacts, byte-exact content, public evidence, and the
separate private witness. A repeated command is idempotent.

`tool_source_sha256` is immutable writer provenance. Compatible recovery is
gated by the exact `contract_version` and `tool_version`, not by requiring the
current Python file to have the writer's digest; an incompatible implementation
must bump one of those explicit versions. This permits a code-only safety fix
to finish or verify an already durable v1 journal without weakening plan,
inventory, reservation, or raw-byte validation. A source with no selected
legacy response candidates fails before archive storage is initialized.

Each accepted offline PAS1 `source_locator` contains a `migration_binding` with
the batch ID, canonical SHA-256 of the immutable batch plan, and the full exact
entry reservation. Runtime validation reloads that outer plan and verifies its
contract/tool version, operator gate and principal, inventory binding, operation
and occurrence IDs, and the raw byte size/SHA-256 in both directions. A caller
cannot publish a null-sprint offline archive by constructing only a synthetic
runtime occurrence.

```powershell
python tools\prompt_archive_migrate.py migrate `
  --input-root $Source --archive-root $Archive `
  --project-id $Project --source-root-handle-id $Handle `
  --operator-gate $Gate --operator-principal-id $Principal `
  --private-evidence-file $PrivateEvidence `
  --protected-root 'C:\nginx-qa-live' `
  --protected-root 'D:\nginx-qa-live'
```

If a process stopped after a per-entry PAS1 operation reached its durable
`blobs_verified` boundary, the exact retained `response_id` and `operation_id`
may be resumed without opening the source tree:

```powershell
python tools\prompt_archive_migrate.py resume-operation `
  --input-root $Source --archive-root $Archive `
  --project-id $Project --source-root-handle-id $Handle `
  --operator-gate $Gate --operator-principal-id $Principal `
  --response-id 'parresp1-<64 lowercase hex>' `
  --operation-id 'parop1-<32 lowercase hex>' `
  --protected-root 'C:\nginx-qa-live' `
  --protected-root 'D:\nginx-qa-live'
```

This source-free action can finish only that already-reserved entry. It cannot
declare the batch complete: rerun `migrate` with the original isolated source so
the full post-inventory can be compared.

At runtime startup, retained offline journal chains and the registry lower bound
are validated. Startup does not advance or abort a nonterminal offline operation
without source/operator authority; terminal registry entries may be cleaned by
the normal recovery path.

Reconstruct uses the accepted archive plus private witness. It does not read the
source tree and never overwrites a different existing file.

Before writing its first file, reconstruction inventories the complete target.
The target must be empty or contain only byte-exact already-published outputs
and deterministic partial temporary files owned by operation IDs in the durable
batch plan. Any unrelated file or directory fails closed before additional
output is published. This makes a retry idempotent without treating an
arbitrary pre-existing file as migration-owned.

```powershell
python tools\prompt_archive_migrate.py reconstruct `
  --input-root $Source --archive-root $Archive `
  --project-id $Project --source-root-handle-id $Handle `
  --operator-gate $Gate --operator-principal-id $Principal `
  --private-evidence-file $PrivateEvidence `
  --reconstruction-root $Reconstruction `
  --protected-root 'C:\nginx-qa-live' `
  --protected-root 'D:\nginx-qa-live'
```

Rollback verification is also read-only. It proves the retained accepted
artifacts hydrate to the exact pre/post inventory and verifies that the source
still has the same paths, sizes, mtimes, identities, and SHA-256 values.

```powershell
python tools\prompt_archive_migrate.py rollback-verify `
  --input-root $Source --archive-root $Archive `
  --project-id $Project --source-root-handle-id $Handle `
  --operator-gate $Gate --operator-principal-id $Principal `
  --private-evidence-file $PrivateEvidence `
  --protected-root 'C:\nginx-qa-live' `
  --protected-root 'D:\nginx-qa-live'
```

## Evidence and exit status

Success writes one compact JSON document to stdout and exits `0`. A stable,
payload-free migration error document is written to stdout and exits `2`.
Argument-parser errors also exit `2`. No error authorizes a fallback write,
in-place repair, deletion, or relaxed path check.

The machine-readable contracts are:

- `schemas/prompt-archive-migration-analysis-v1.schema.json`;
- `schemas/prompt-archive-migration-batch-plan-v1.schema.json`;
- `schemas/prompt-archive-migration-batch-journal-v1.schema.json`;
- `schemas/prompt-archive-migration-evidence-v1.schema.json`;
- `schemas/prompt-archive-migration-private-evidence-v1.schema.json`;
- `schemas/prompt-archive-migration-resume-evidence-v1.schema.json`;
- `schemas/prompt-archive-migration-reconstruction-evidence-v1.schema.json`;
- `schemas/prompt-archive-migration-verification-v1.schema.json`;
- `schemas/prompt-archive-migration-error-v1.schema.json`.

Keep the source copy, archive root, and private witness until an independently
authorized retention decision. This release performs no source cleanup, archive
GC, live cutover, or deletion.

## Qualification note

The selected legacy file is bounded, but the v1 top-level `kind` preflight
currently materializes its bytes and uses the standard JSON decoder to
discriminate valid control documents. PSC-006 must measure peak RSS with a
realistic generated archive (including the motivating approximately 71 MiB
class) before making a performance PASS claim. This is not permission to use
`D:\Prompt`; generate the fixture under an isolated test root. A future
streaming discriminator may reduce that peak, but it must retain duplicate-key,
unknown-kind, malformed opaque legacy, and fail-closed resource semantics.
