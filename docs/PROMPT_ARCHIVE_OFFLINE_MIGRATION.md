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

The public archive stores only path-free hashes, IDs, counts, provenance, and
journal state. The exact pre/post paths and filesystem identities exist only in
the explicitly selected private evidence file. Protect that file as sensitive
operator evidence; do not publish it with the archive.

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
