# Managed JSON intake

The opt-in launcher accepts its versioned envelope as a complete JSON text, a
single `json` code fence, or a `.json` document without a caption. The private
Telegram user needs the project's `prepare` grant. A document is downloaded only
after sender authorization; project permission is checked again after download.
The enabled webhook always requires its configured secret. Disabling the feature
preserves the previous Telegram path. With it enabled, document downloads require
launcher preparation permission; non-launcher JSON still uses legacy dispatch.

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
agent-client integration, whose preparation/activation is a subsequent stage.

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

At this stage a published operation reports `Created/PREPARATION_REQUIRED`.
Instance preparation and Start are implemented by the following graph stages.
Automated publication tests use disposable local remotes, not GitHub or Telegram.
