# Managed JSON intake

The opt-in launcher accepts its versioned envelope as JSON text, a single `json`
code fence, or an explicitly selected document. For a document, use the reserved
filename **managed-sprint.json**, or attach any `.json` with the exact caption
`/sprint_import 9000` (substitute the canonical project phone). Do not combine a
text body and attachment, or add a different caption to the reserved filename.

The marker selects the new adapter before download. The private Telegram user
needs `prepare`; a caption's project is checked before download and must match
the envelope afterward. With only the reserved filename, the sender needs a
preparation grant for at least one project before download, followed by the
specific envelope project's grant after parsing. All grants are rechecked after
async work. Unknown launcher versions fail in the new adapter.

Unmarked documents and unrelated legacy text/channel posts retain their original
dispatch, permissions and parsing. An unmarked launcher envelope read by the old
parser is rejected with `LAUNCHER_TRANSPORT_REQUIRED` before any runtime mutation.
That old download path retains its existing transport behavior; it is not the
new launcher ingress. Marked channel/edited requests are rejected. The enabled
webhook requires its configured secret; disabling the feature preserves the old
Telegram path (except reserved launcher envelopes cannot bypass into legacy).

The envelope requires `kind: telegram_managed_sprint_v1`, `schema_version: 1`, the
canonical project phone, boolean `autonomous_mode`, and a complete `manifest`.
An unknown launcher version is rejected. `true` does not grant Start permission.
Unwrapped managed manifests retain the existing `MANAGED_GIT_START_REQUIRED`
refusal on the legacy import path.

Intake enforces 1 MiB UTF-8 JSON, no duplicate keys/nonfinite numbers, maximum
depth 64, 32,768 structural values, 256 nodes and 1,024 checksum entries. Schema
and graph checks precede any operation reservation. Source bytes are checked
against Git at the operator-selected ref and exact expected commit, with at most
16 MiB per blob and 64 MiB total checksum input. Source contents are not returned.
Invalid requests create no operation, execution workspace, queue or process.

Launcher write/review topic branches must use `agent/` or `review/`; write nodes
need distinct branches. Uploaded roots/operator settings/credentials are rejected.
Uploaded process commands are disabled in v1. Execution uses the existing external
agent-client integration described in launch-modes.md.

The operator policy can grant `manifest_publish` separately from `prepare`.
Without it the saved operation remains `Created`, blocked by
`PUBLICATION_REQUIRED`. Re-sending the same delivery after granting the capability
resumes that operation. No instance is advertised as prepared or running.

Publication uses the existing Git provider and its scoped credential resolver.
The service generates a `launcher/manifests/launch-<random id>` topic ref. Its
commit preserves the original source tree and adds a generated manifest file;
the source commit is the parent. Deterministic commit metadata and a durable
intent bind exact commit/path/blob digest before publication. Creation uses an
empty expected-old Git lease, which cannot overwrite an existing remote ref.
Lost push acknowledgements are reconciled against the exact intended remote head.
There is no main update or automatic PR. Failed publication remains resumable.

Transport identities (bot/update and bot/chat/message) and payload digest are
persisted in the existing ManagedImportStore database. Exact retries reuse the
operation; changed payload/identity conflicts. Another user cannot read its
receipt. A new delivery is a new intent even if its JSON is identical.

One SQLite writer transaction reserves the canonical project slot. Both old
managed import registration and import recovery consult this same reservation.
An existing active sprint or unfinished import prevents reservation. At most 100
pending operations across projects may hold slots; historical records are kept.

A published operation proceeds through the existing importer to parked preparation.
Manual mode then reports Waiting without assignments; autonomous mode requests the
same authorized launch path. Missing publication, executor or other capability
retains the last proved state and a recoverable blocked reason. Start/Stop and
recovery are described in launch-modes.md. Automated publication tests use
disposable local remotes; they are separate from real GitHub/Telegram evidence.
