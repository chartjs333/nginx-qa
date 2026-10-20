# Prompt archive storage v1

Status: contract frozen for the prompt-storage-compaction-v1 managed sprint.
Implementation, rollout, migration, deletion, and garbage collection are not
authorized by this document.

## 1. Purpose and invariants

Prompt archive storage v1 (PAS1) stores sequential diagnostic evidence without
copying the complete cumulative project state for every transition. It keeps
the existing compact agent prompt usable, retains legacy evidence, and permits
bounded, project-scoped hydration on demand.

The following invariants are normative:

1. Legacy archive bytes are opaque. A lossless claim means the reconstructed
   byte length and SHA-256 equal the original bytes. Parsing and reserializing
   JSON is never evidence of raw-byte equality.
2. Structural equality, canonical equality, and raw-byte equality are distinct
   claims. Evidence and metrics must name the exact claim they prove.
3. A new runtime transition is represented by new immutable records plus a
   persistent sequence update. It must not write a new blob containing the
   complete prior timeline, patches, transcript, or project state.
4. Blob locations are derived only from validated digests and encoding. A
   request, payload, manifest, or reference never supplies a filesystem path.
5. Blobs, index nodes, manifests, and pointers are versioned and closed. User
   payload bytes are never recursively scanned for a magic reference marker.
6. Managed identity, active-task, `whoami`, and compact `.prompt` paths do not
   build full project state and do not hydrate PAS1 content. The unchanged
   legacy diagnostic mode is explicitly excluded from this performance claim.
7. V1 rollout is `legacy` by default. `shadow` keeps legacy output
   authoritative; the separately gated `authoritative` mode is the only mode
   that may replace cumulative runtime writes with PAS1, and it is restricted
   to isolated qualification until a later operator authorizes deployment.
8. PAS1 never deletes or rewrites a legacy archive and implements no garbage
   collection. A later retention design requires a separate operator gate.

## 2. Integrity vocabulary

### 2.1 Raw-byte equality

For an existing legacy archive, the source is an opaque byte string `B`.
Publication records:

```text
raw_length = len(B)
raw_sha256 = lowercase_hex(SHA-256(B))
```

Hydration concatenates the ordered raw chunks and must reproduce exactly `B`.
It succeeds only if both values match. Whitespace, object-member order, Unicode
escape spelling, numeric lexemes, line endings, BOM bytes, and duplicate JSON
members are therefore preserved. Duplicate members are not collapsed by a JSON
decoder. A mismatch is `PROMPT_ARCHIVE_INTEGRITY_MISMATCH`; no manifest or
pointer is published.

### 2.2 Canonical equality

`nginx-qa-canonical-json-v1` is the control-artifact encoding and optional
semantic evidence for payload values. It is UTF-8 without a BOM; object keys
are sorted lexicographically by Unicode scalar value; no whitespace is emitted;
duplicate keys, unpaired surrogates, floating-point values, and values outside
the schema are rejected. `null`, booleans, arrays, and objects use their JSON
tokens. Integers use minimal base-10 (`0`, never `-0`, otherwise optional `-`
and digits with no leading zero). Strings use `\"` and `\\` for quote and
backslash, lowercase `\u00xx` for every U+0000..U+001F control, and literal
UTF-8 for every other scalar; `/` and non-ASCII scalars are not escaped. No
Unicode normalization is performed. The encoder operates on a duplicate-aware
parser result and never on a map produced after duplicate collapse.

Frozen vector (the displayed bytes are UTF-8):

```text
value:  {"b":"Привет","a":[true,null,-2,"\n"]}
bytes:  {"a":[true,null,-2,"\u000a"],"b":"Привет"}
sha256: 8f5516fc3f959f64d4077f58bbb4f9a2b97c14d072ae3373c46942b52b4aea10
```

The digest never substitutes for a raw digest. Structural, canonical, and
raw-byte claims remain different even when their digests happen to match.

Before native response serialization, the builder recursively requires every
object key to be a string; coercion is forbidden, so `{1: "a", "1": "b"}`
cannot collapse into duplicate emitted member names. The JSON encoder is called
with non-finite values disabled (`allow_nan=False`). The exact emitted bytes are
then parsed once with a duplicate-aware parser that rejects duplicate member
names and the non-standard `NaN`, `Infinity`, and `-Infinity` tokens before any
recursive Merkle record is planned. Passing a language-runtime object or a
post-collapse map is not evidence that these checks occurred.

### 2.3 Native-byte equality

A native PAS1 record is created from its exact, already serialized bytes. The
blob digest binds those bytes. Hydrating a native event stream produces the
versioned PAS1 representation, not a claim that an unmaterialized legacy JSON
snapshot once had identical bytes.

## 3. Artifact model

The normative schemas are:

- `schemas/prompt-archive-ref-v1.schema.json`;
- `schemas/prompt-archive-blob-v1.schema.json`;
- `schemas/prompt-archive-record-v1.schema.json`;
- `schemas/prompt-archive-json-value-v1.schema.json`;
- `schemas/prompt-archive-json-edge-v1.schema.json`;
- `schemas/prompt-archive-source-metadata-v1.schema.json`;
- `schemas/prompt-archive-index-node-v1.schema.json`;
- `schemas/prompt-archive-envelope-v1.schema.json`;
- `schemas/prompt-archive-manifest-v1.schema.json`;
- `schemas/prompt-archive-pointer-v1.schema.json`;
- `schemas/prompt-archive-publication-plan-v1.schema.json`;
- `schemas/prompt-archive-journal-v1.schema.json`;
- `schemas/prompt-archive-locator-v1.schema.json`;
- `schemas/prompt-archive-latest-slot-v1.schema.json`;
- `schemas/prompt-archive-pending-operation-registry-v1.schema.json`;
- `schemas/prompt-archive-source-inventory-v1.schema.json`;
- `schemas/prompt-archive-descriptor-stub-v1.schema.json`;
- `schemas/prompt-archive-descriptor-v1.schema.json`;
- `schemas/prompt-archive-error-v1.schema.json`.

All JSON control artifacts use `nginx-qa-canonical-json-v1` exactly.
Opaque legacy chunks and payload blobs retain their declared media type and
exact uncompressed bytes.

### 3.1 Blob reference

A `prompt_archive_blob_ref` describes the exact uncompressed bytes and their
stored representation. It contains the SHA-256 of both, their sizes, logical
kind, media type, encoding, and reference version. It contains no path, URL,
project identifier, credential, or arbitrary metadata.

The physical blob is deliberately binary rather than JSON/base64: `identity`
stores exactly the referenced uncompressed bytes, while `gzip` stores one
RFC 1952 member. `prompt-archive-blob-v1.schema.json` is the versioned,
path-free metadata envelope (`kind`, `media_type`, and `ref`) used at module
boundaries; it does not wrap the physical bytes. A blob is valid only when the
metadata/ref shape, stored-byte digest/size, encoding rules, and uncompressed
byte digest/size all validate together.

The uncompressed digest is the content identity. The physical mapping is:

```text
<archive_root>/projects/<project_storage_key>/blobs/v1/sha256/<hex[0:2]>/<hex[2:4]>/<hex>.blob
```

`<project_storage_key>` is the lower-case digest from the `project-storage-key`
identifier preimage in section 3.3. It depends only on the durable project ID,
not a mutable registry Git address/context; changing project configuration
therefore cannot orphan existing archives. `<hex>` is exactly the validated
lowercase uncompressed content digest. No caller-controlled text is interpolated.
Project scoping prevents cross-project deduplication and its timing/enumeration
side channel. There is exactly one physical path per project/content digest.

Encoding selection and stored bytes are deterministic. Values below 1024
uncompressed bytes use `identity`; values of 1024 bytes or more use exactly the
writer codec `gzip-rfc1952-zlib-1.2.13-level9-v1`. That codec emits the ten-byte
RFC 1952 header `1f8b08000000000002ff`, followed by one raw DEFLATE stream from
zlib 1.2.13 configured as `level=9`, `method=DEFLATED`, `wbits=-15`,
`memLevel=8`, `strategy=Z_DEFAULT_STRATEGY`, supplied as one byte sequence and
finished once, followed by little-endian CRC32 and ISIZE. It emits no optional
FEXTRA/FNAME/FCOMMENT/FHCRC fields. A writer unable to reproduce that codec
exactly fails closed for inputs that require gzip; it must not silently choose
a different encoding. Frozen stored-byte vectors are:

| uncompressed bytes | complete stored bytes (hex) | stored SHA-256 |
| --- | --- | --- |
| empty | `1f8b08000000000002ff03000000000000000000` | `9ceffb7310338057cfe71a4ae1e2c98d2c485d81cdef906532a801f457a38d64` |
| UTF-8 `hello` | `1f8b08000000000002ffcb48cdc9c9070086a6103605000000` | `3223ab15154bd181c783e5b434b108c68f5798bf10be7eb681faba9b6668bc65` |
| 1024 zero bytes | `1f8b08000000000002ff63601805a360148c5400002eafb5ef00040000` | `a1b1b81d3d1afdb8fe119b002318c12c20934713b9754a40f702adb18a2540b9` |

Publication is first-writer-wins: the winner exclusively creates the single
digest path; a loser discards its temporary bytes, fully verifies the winner,
and only then publishes a ref. Deterministic encoding means every valid winner
has identical stored size and digest. Readers verify stored SHA-256, bounded
streaming decompression, uncompressed length, compression-ratio policy, CRC32,
and uncompressed SHA-256.

### 3.2 Records and user data

An immutable record binds one ordinal, scope, safe provenance, record type, and
a typed payload representation. Event, patch, result, feedback, review-context,
execution-audit, source-metadata, and current-field records are independent.
User data remains inert data even when its exact value is
`{"$cas_ref":{"sha256":"..."}}`; only fields that a PAS1 schema explicitly
types as `prompt_archive_blob_ref` are dereferenced.

Every complete JSON record payload uses
`representation=recursive_json_merkle_v1` and a typed `json_value` root. The
closed value and edge schemas describe exact source bytes, not a normalized
semantic value:

- a scalar value stores its exact lexical token in a bounded `literal_chunks`
  byte span;
- an array or object stores exact prefix and suffix byte spans and an ordered
  persistent `json_edges` index;
- an array edge stores the exact separator before the item plus its typed child
  value ref;
- an object edge additionally stores the exact member-name token and exact
  key/value separator before its typed child value ref.

The deterministic parser consumes the complete payload. For a container, the
prefix runs from the value start through `[` or `{` and all bytes before the
first item/member; the first edge has an empty separator. Each later edge owns
all bytes after the preceding child through the next child/member token,
including comma and whitespace. An object edge's key token is only the quoted
name and its key/value separator includes the colon and surrounding whitespace.
The suffix owns all bytes after the final child through `]` or `}`, plus any
outer document whitespace. For a root scalar, the token owns the complete
document span, including outer whitespace. Empty byte spans are represented by
one verified zero-byte literal leaf. Reconstructing each edge and then each
value must match its declared byte length and SHA-256 exactly.

Value/edge artifacts are canonical closed control documents with no project,
sprint, response, provenance, source path, absolute offset, or logical ID.
Their typed content ref digest is their identity, so identical nested syntax can
be reused across records, aliases, and occurrences. Object member order and
exact numeric/string spelling remain byte-preserved. A duplicate-aware parser
must consume the full input; trusted native output rejects duplicate member
names, while opaque legacy migration remains governed by raw-byte equality.
The recursive builder never interprets a user object as a PAS1 reference.

Native `source_metadata` is the canonical bytes of
`prompt-archive-source-metadata-v1`, never an opaque JSON bag and never adapter
or executor configuration. Its allowlist is the closed provenance vocabulary
`source_type`, `conversation_id`, `thread_id`, `message_id`, `sender_label`,
`title`, and `observed_at`; an owning source contract may omit fields but cannot
add one. `source_type` is mapped to the closed enum, with unrecognized values
becoming `unknown`. Conversation/thread/message identifiers are never stored
raw: non-empty exact UTF-8 input becomes `sha256:<lowercase digest>`; invalid or
suspected-secret input becomes `[REDACTED]`.

The frozen `prompt-archive-source-redaction-v1` screen examines every raw value
that may be persisted before hashing, truncation, or storage. Matching is over
the exact Unicode input and an ASCII-case-folded view. It rejects controls,
values above 256 Unicode scalars, any URL, URL user-info, a PEM private-key
marker, JWT-shaped text, `Bearer` or `Basic` credentials, token shapes beginning
`sk-`, `sk-proj-`, `ghp_`, `github_pat_`, `glpat-`, or an `xox*` prefix, and a
credential word at a `^`, `-`, `_`, `.`, or whitespace boundary. The closed
credential words are `authorization`, `cookie`, `access-token`, `api-key`,
`apikey`, `bearer-token`, `credential`, `credentials`, `password`, `passwd`,
`private-key`, `secret`, `token`, `pending-token`, `producer-token`, `dpapi`,
and `webhook` (hyphen may be `_` where shown).

The matcher is Python regular-expression search with Unicode strings and the
flags `IGNORECASE|ASCII`; it is not locale-sensitive and performs no Unicode
normalization. These exact V1 alternatives are compiled separately and a match
in any one rejects the whole value:

```text
[\x00-\x1f\x7f]
[A-Za-z][A-Za-z0-9+.-]*://[^\s<>"']+
-----BEGIN(?: [A-Z0-9]+)? PRIVATE KEY-----
(?<![A-Za-z0-9_-])eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}(?![A-Za-z0-9_-])
\b(?:Bearer|Basic)[ \t]+\S+
\b(?:sk-(?:proj-)?|ghp_|github_pat_|glpat-|xox[A-Za-z0-9]{1,8}-)[A-Za-z0-9_-]{8,}
(?:^|[-_.\s])(?:authorization|cookie|access[-_]?token|api[-_]?key|apikey|bearer[-_]?token|credentials?|password|passwd|private[-_]?key|secrets?|token|pending[-_]?token|producer[-_]?token|dpapi|webhook)(?:$|[-_.\s:=])
(?:^|[\s'"(])(?:[A-Za-z]:[\\/]|\\\\[^\\/\s]+[\\/]|//[^/\s]+/|/(?:[^/\s]+(?:/[^/\s]*)*))
```

The length check is `len(value) > 256` Unicode scalars before matching. The URL
alternative intentionally rejects query/fragment credential names by rejecting
the whole URL. The last alternative rejects absolute Windows, UNC, and POSIX
filesystem paths; relative display words such as `docs/readme` are not paths in
this contract. Configured-secret comparison is case-sensitive and has one
explicit short-value rule: a non-empty configured value of one through three
Unicode scalars rejects only an exactly equal whole candidate value; a
configured value of four or more scalars rejects any candidate containing it as
a substring. Empty configured values are ignored. Registered-token candidates
use maximal ASCII token-character runs matched by
`(?<![A-Za-z0-9_-])[A-Za-z0-9_-]{43,}(?![A-Za-z0-9_-])`. An exactly
43-character run is SHA-256 compared in constant time against every digest in
the frozen set. When that set is non-empty, a longer maximal run is rejected
fail-closed: the digest-only registry cannot prove that a registered token was
not embedded with adjacent token characters.

Before projection, the owner also takes one ephemeral immutable snapshot of
all non-empty configured credential values, partitioned into the exact-whole
one-to-three-scalar set and the substring four-or-more-scalar set, plus the
registered token SHA-256 digests. The frozen rule above is applied without
Unicode normalization or locale/case folding beyond the stated patterns. For
configured credentials, both the exact configured representation and the
operational `strip()` form are protected, because credential consumers remove
surrounding whitespace before use. Neither snapshot nor a raw secret is
persisted. Instead the fully screened, closed source-metadata
projection is frozen in the durable occurrence before archive planning; its
canonical bytes/digest are reused on every retry and recovery and raw metadata
is never re-screened against a later environment. This prevents credential
rotation from changing a durable occurrence.

The V1 credential authority inventory is closed: the six fixed environment
names `GEMINI_API_KEY`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_WEBHOOK_SECRET`,
`NGINX_QA_SCOPE_CONTROL_ADMIN_TOKEN`, `NGINX_QA_SMTP_USERNAME`, and
`NGINX_QA_SMTP_PASSWORD`; numeric
`NGINX_QA_SCOPE_CONTROL_ROLE_TOKEN_<phone>` names; `env:` references from the
current project's configured project/repository credential fields; `env:`
references from that project's one indexed active managed runtime repository
and redacted process environments; durable pending-sprint access tokens; and
the producer registry's token digests. Historical terminal runtimes and other
projects are not credential authorities for the current occurrence, and the
hot path reads the indexed active runtime rather than traversing history.
Arbitrary payload keys that merely look like secret references are not scanned
or treated as credential configuration.

A rejected optional identifier or display value is replaced wholly by
`[REDACTED]`; partial masking is forbidden. `source_type` is mapped to its
closed enum, but becomes `unknown` when invalid or when it collides with the
credential screen. `observed_at` is copied only when it is a valid durable RFC
3339 occurrence timestamp and passes the same credential screen; otherwise it
is omitted. Neither field can appear in `redacted_fields`. `redacted_fields` is
the lexicographically sorted unique
list drawn only from `conversation_id`, `thread_id`, `message_id`,
`sender_label`, and `title`; `redaction_count` equals its length.
`discarded_field_count` records rejected non-allowlisted input fields (bounded
at 1024; a larger input fails). The exact canonical payload must validate the
dedicated schema and use its dedicated media type before its record is
published.

Semantic validation is biconditional: `redacted_fields` equals exactly the
sorted names of present optional properties whose value is `[REDACTED]`; each
listed property must be present with that exact token, and an unlisted property
must not carry it. `redaction_count` equals that list length. Before raw input
is discarded, the occurrence writer also binds `source_type`,
`discarded_field_count`, and the projected-field presence set to the projection
receipt; recovery verifies the canonical projection digest against that receipt
instead of trusting schema-valid self-reported counters.

Frozen negative vectors include `https://alice:hunter2@example.test/x`,
`https://example.test/?api_key=x`, `eyJabcdefgh.abcdefgh.abcdefgh`,
`-----BEGIN PRIVATE KEY-----`, `xoxb-abcdefgh`, `C:\\secret\\x`,
`\\\\server\\share\\x`, and `/var/lib/secret`; each projects to the one fixed
token and names its field in `redacted_fields`. `release/docs`, `Alice`, and
`tokenizer` are frozen non-matches. Tests additionally inject an ephemeral
configured value and a registered-token digest and prove neither raw canary nor
screen context is present in record, plan, journal, descriptor, error, or log.

Authorization headers, cookies, credentials, raw headers, filesystem roots,
commands, URLs, and executable adapter settings are therefore either absent or
the fixed redaction token. Neither a source type nor any hydrated value may
cause network, filesystem, subprocess, adapter, import, or executor behavior;
readers return it as inert JSON only. Negative tests use secret canaries and
side-effect traps to enforce both properties.

`legacy_raw_chunks_v1` is the narrow exception: its bytes are an opaque copy of
already-existing isolated legacy evidence and are not parsed, redacted, or
claimed secret-free, because doing so would violate raw-byte equality. That
exception does not authorize new secret ingestion, logging, public exposure,
or use as executable configuration. It remains behind the same bounded reader
and authorization checks as every other archive.

The history-text string fragment is the one native record representation that
is not a complete JSON value. It uses `opaque_chunks_v1` and bounded
`payload_chunk` blobs behind a `payload_chunks` index. Every other native record
uses the recursive value root above. In both cases the record carries the total
payload length and SHA-256. Large syntax spans and render plans are independently
chunked, so the per-blob 16 MiB ceiling never truncates a larger result,
feedback item, or active task.

### 3.3 Identifier byte grammar

PAS1 integrity identifiers never depend on JSON serialization. `PAS1-ID-v1`
uses the following domain-separated framing:

```text
preimage = ASCII("nginx-qa-pas1-id-v1") || 0x00 || frame(domain)
           || frame(field_name_1) || value(field_1) || ...
frame(s) = 0x00 || uint64_be(len(UTF8(s))) || UTF8(s)
value(null) = 0xff
value(string-or-decimal-integer) = frame(exact_ASCII_or_UTF8_text)
digest = lowercase_hex(SHA-256(preimage))
```

Field names and their order are part of the domain contract. Integers use the
minimal decimal rule from section 2.2. Identifier inputs below are schema-safe
ASCII except that no implementation may silently normalize a future Unicode
input.

| Output/domain | Ordered fields |
| --- | --- |
| `project_storage_key` / `project-storage-key` | `project_id` |
| `sprint_storage_key` / `sprint-storage-key` | `project_id`, `sprint_id` (nullable only for offline migration) |
| `parresp1-…` / `response-id` | `project_id`, `sprint_id`, `response_kind`, `occurrence_kind`, `occurrence_id` |
| `parrec1-…` / `record-id` | `project_id`, `sprint_id`, `record_type`, `origin_response_id`, `origin_transition_id`, `ordinal`, `payload_size`, `payload_sha256`, `payload_root_digest` |
| `par1-…` / `archive-id` | `archive_format`, `project_id`, `sprint_id`, `response_id`, `envelope_sha256`, then root digests in this exact order: `raw_bytes`, `current_fields`, `recent_activity`, `history_text`, `history_timeline`, `activity_with_patches`, `history_patches`, `code_patches`, `work_history`, `review_context`, `results`, `feedback`, `execution_audit`, `source_metadata` (each field is named `<root>_root_digest` and is null when absent), then `hydrated_size`, `hydrated_sha256` |
| `parptr1-…` / `pointer-id` | `project_id`, `sprint_id`, `response_id`, `generation`, `archive_id`, `manifest_sha256`, `previous_pointer_id`, `published_at` |
| `pararc1-…` / `archive-locator-id` | `project_id`, `sprint_id`, `response_id`, `archive_id`, `pointer_id`, `manifest_sha256`, `published_at` |
| `parstub1-…` / `descriptor-stub-id` | `project_id`, `sprint_id`, `response_id`, `archive_id`, `pointer_id`, `manifest_sha256`, `created_at`, `published_at` |
| `parlatestintent1-…` / `latest-content-intent-id` | `project_id`, `slot_id`, `revision_protocol`, `revision_epoch`, `revision_reset_policy`, `source_revision`, `occurrence_id`, `operation_id`, `bootstrap_kind`, `bootstrap_sha256`, `bootstrap_byte_size`, `content_kind`, `content_sha256`, `content_size`, `descriptor_digest` (null for legacy), `source_sprint_id`, `source_response_id`, `published_at` |
| `parlatest1-…` / `latest-slot-id` | `project_id`, `slot_id`, `intent_id`, `revision_protocol`, `revision_epoch`, `revision_reset_policy`, `source_revision`, `occurrence_id`, `operation_id`, `generation`, `previous_slot_id`, `content_kind`, `content_sha256`, `content_size`, `descriptor_digest` (null for legacy), `source_sprint_id`, `source_response_id`, `published_at` |
| `parinv1-…` / `source-inventory-id` | `project_id`, `source_root_handle_id`, `source_root_identity_sha256`, `inventory_sha256`, `entry_count`, `created_at` |
| `parpending1-…` / `pending-operation-registry-id` | `project_id`, `shard_id`, `generation`, `previous_registry_id`, `entries_sha256`, `published_at` |
| `parloc1-…` / `publication-locator` | `project_id`, `sprint_id`, `response_id`, `occurrence_id`, `operation_id` |
| `parjournal1-…` / `publication-journal` | `project_id`, `sprint_id`, `response_id`, `operation_id` |

`json_value` and `json_edge` deliberately have no PAS1 logical identifier and
therefore no separate ID vector. Their canonical control-document bytes are
addressed only by the typed SHA-256 content ref. This omission is what permits
the same exact nested value or edge to be reused under different records and
occurrences without relabelling provenance.

`occurrence_kind` is one of `transition`, `identity-state`, or `migration`; its
`occurrence_id` is the durable runtime transition/state/migration occurrence ID
already journaled before response construction and is not derived from the
response body, target phone, clock, or retry. `envelope_sha256` is null for raw
legacy manifests. V1 pointers always have generation one and a null
`previous_pointer_id`; mutable generations belong only to the project-global
latest-slot and pending-registry wrappers.
`published_at` is copied from the durable occurrence before the plan is
persisted; it is never a retry wall clock. Including it in the pointer and
archive-locator identifiers prevents two different canonical documents from
sharing one logical ID. An
operation ID is not an integrity digest: it is
`parop1-` plus 32 lowercase hex characters from 128 cryptographically random
bits, persisted before any object write and reused during recovery. Other Git,
assignment, review, and result identifiers retain their owning subsystem's
durable format. IDs and digests are never authorization grants.

The contract tests contain executable golden vectors for every table row. Any
different byte framing, field omission, order, normalization, or digest is a
version change, not an implementation choice.

For the exact sample fields used by those tests, frozen digests are:

```text
project-storage-key d23f4ba66de9f6b13715a9dedafdbc65fe54c2fd121bbc788bf671745ab42531
sprint-storage-key  52c984a54d25d125de23576c14d0d99f038af5c35cba1b3f2d835f341b3f2dff
sprint-storage-null f1b65187a5e2e5fefed336defbf94faa63fdb4c572ed69d3933cf8534dbe4ef7
response-id        7dec908e702d975e49a84abbb17140b876a0b70de06a78fd745751030abb25a8
record-id          82e6999cb68ddd46f7aad412f35ed80852341bb6149bcf8be5d143ca92a37133
archive-id         68adb2078c8a76895b65a9a3c0799bcd8148c3c9b657aeef724584c019b60b6f
pointer-id         e5d9c58e7327d00a10397724f920c9e75fe2e4e2c8a0506db9761e1d2d435649
archive-locator-id 2774305b7b2edcec5015264b8c0ff67f3630b66232fa41986618b3ab143c99f4
descriptor-stub-id  337b0a743457de538ba51140e248f3efebcdc8cdd4890a47e4d967ee4d877714
latest-content-intent-id a930dd73e2a2dccc3e8d0e3e37c5c04272b6e729fa847efd738b11a2c658e734
latest-slot-id      344fa50c4d3efd72cd2561c8b9ed845ca4a1d53125f6a9fe1b8e90d8a80ace09
source-inventory-id f869634a4debc8433f0b7b5c33bd8a6642cc0761d0b426a6bc4aae4a764bf69f
pending-registry-id 6395e842a7492bb36f6e44a7ac5e0ab9653e09f6484d04a961d94e02c482277a
publication-locator c7aad3e0d806c2f97d5249fbdde565849d66d9696508ba5f6c67a41f93585481
publication-journal c291f580c6c8db505d2538e3cca8f0f308be74ed9078ab6d0cdfa3c4293ec2a2
```

The `sprint-storage-key` runtime vector uses `project_id=9000` and
`sprint_id=sprint-example-1`. The null vector uses the same project and the
single PAS1 null byte for `sprint_id`; it is the exact bucket for sprintless
offline migration. Thus every pointer/locator uses the same
`sprints/<sprint_storage_key>/...` grammar, while no literal `null`, empty
component, sprint ID, or caller text enters a filesystem path.

The source-inventory vector uses `project_id=9000`,
`source_root_handle_id=isolated-root-1`, `source_root_identity_sha256=a×64`,
`inventory_sha256=b×64`, `entry_count=2`, and
`created_at=2026-10-11T11:20:00Z`.
The archive vector preserves the prior sample values and maps its `2×64` root
to `current_fields`, its `3×64` root to `recent_activity`, and all other root
digests to null; this sentence is part of the golden-vector fixture.
The descriptor-stub vector uses the same sample project/sprint/response/archive,
the displayed sample pointer, manifest digest `d×64`, and timestamps
`2026-10-11T11:29:00Z`/`11:31:00Z`. The latest-content-intent vector uses
`project_id=9000`, `slot_id=latest-response`, revision protocol
`prompt-archive-durable-project-latest-revision-v1`, revision epoch `1`, reset
policy `restore_high_water_mark_or_fail_closed`, source revision `42`,
occurrence `transition-example-1`, operation `parop1-` followed by 32 zeroes,
bootstrap kind `absent` with null SHA-256 and byte size, `descriptor_stub`,
content digest `a×64`, size `512`, descriptor digest `b×64`, sprint
`sprint-example-1`, response `parresp1-` followed by 64 ones, and
`published_at=2026-10-11T11:31:00Z`. The latest-slot vector projects that exact
intent and ordering, uses its displayed intent ID, generation one, and a null
predecessor. The complete frozen IDs are
`parlatestintent1-a930dd73e2a2dccc3e8d0e3e37c5c04272b6e729fa847efd738b11a2c658e734`
and `parlatest1-344fa50c4d3efd72cd2561c8b9ed845ca4a1d53125f6a9fe1b8e90d8a80ace09`.
The pending-registry vector uses shard `ab`, generation one, null
predecessor, entries digest `c×64`, and `11:27:00Z`.

`entries_sha256` is SHA-256 of the canonical JSON bytes of the complete sorted
`entries` array. `descriptor_digest` is the referenced descriptor stub's
uncompressed digest. These derived fields participate in PAS1-ID-v1 but are not
duplicated as schema properties where the enclosing document already carries
the source value; semantic validation recomputes them before accepting an ID.

### 3.4 Persistent sequences

Ordered records, chunks, and JSON edges are reachable through immutable
bounded-fanout index nodes. An index node has at most 64 children and records
total item count and logical bytes. Level zero points to the one typed leaf kind
declared by `sequence_kind`; higher levels point only to index nodes of that
same kind. Ordinal ranges must be contiguous, non-overlapping, and agree with
child metadata.

Every byte stream (`raw_bytes`, an opaque history-text payload, render-plan
bytes, or a recursive-model syntax span) is split from offset zero into exact
8 MiB (8,388,608-byte) chunks. Every non-final chunk is exactly 8 MiB; the final
chunk is 1..8 MiB. An empty stream has exactly one zero-byte chunk so its digest
remains explicit. Leaves are packed left-to-right in consecutive groups of at
most 64. Each higher level is formed by packing the previous level's consecutive
nodes in groups of at most 64 until one root remains; alternative balancing is
invalid. Records and JSON edges are likewise packed by consecutive ordinal in
groups of 64, while an empty record family omits its root and an empty JSON
container has `entries_root=null`. A parent is serialized only after every child
has won or verified its final CAS representation, so retries and concurrent
writers converge on identical node bytes and root digests.

Append copies only the rightmost index path and reuses every unchanged node.
For `N` existing fixed-size records, adding a record writes its changed recursive
JSON nodes/edges, one record envelope, and at most
`ceil(log_64(N + 1)) + 1` record-index nodes plus a constant-size
manifest/pointer update. Updating a nested JSON value copies only the changed
scalar/container, its ancestor edges/value nodes, and one bounded-fanout path
per affected container; unchanged siblings and their indexes are reused. It
never serializes the prior sequence or unchanged subtree into a new cumulative
value. Qualification measures all new physical bytes, including manifests,
indexes, journals, and filesystem allocation.

`legacy_raw_chunks_v1` uses ordered opaque raw chunks solely for byte-preserving
migration/shadow comparison. It is not the native delta-growth representation
and cannot be used to claim that repeatedly chunking complete cumulative files
has O(delta) write amplification. Runtime delta evidence must use
`incremental_transition_v1` records with
`json_value_model=pas1-recursive-json-merkle-v1`.

An incremental manifest always binds one closed
`prompt-archive-envelope-v1` render plan with
`builder_version=pas1-render-plan-builder-v1` and
`renderer_version=pas1-concat-renderer-v1`; both envelope and manifest bind
`json_value_model=pas1-recursive-json-merkle-v1`. These builders are frozen parts
of the format, not implementation choices. Native/shadow input is the exact
destination-byte buffer emitted by nginx-qa's trusted duplicate-free JSON
encoder. That buffer is constructed once before any hash, archive build, or
destination write; legacy output and PAS1 consume the same bytes. Its explicit
newline policy preserves the existing destination contract (`CRLF` on Windows
text destinations, `LF` otherwise), and binary publication must not rely on a
later implicit text-I/O translation. Shadow comparison binds the bytes actually
committed by the legacy destination, not a pre-translation Python string. The
builder must nevertheless use a duplicate-aware parser with byte offsets; any
duplicate member makes native/shadow construction fail before publication.
Offline raw migration never invokes this builder and preserves duplicate
members opaquely.

The builder walks the parsed response in document/pre-order. When a value is at
one of the following exact member paths, it emits one `sequence` span with the
frozen mapping shown here:

| slot path | manifest root | sequence kind |
| --- | --- | --- |
| `project_state.recent_activity` | `recent_activity` | `events` |
| `project_state.history_with_patches.text` | `history_text` | `history_text` |
| `project_state.history_with_patches.timeline` | `history_timeline` | `events` |
| `project_state.activity_with_patches` | `activity_with_patches` | `events` |
| `project_state.history_with_patches.patches` | `history_patches` | `patches` |
| `project_state.code_patches` | `code_patches` | `patches` |
| `work_history` | `work_history` | `events` |
| `assignment.execution_audit.checkpoints` | `execution_audit` | `execution_audit` |

Each path appears at most once because duplicate object names are forbidden.
Missing optional paths omit both their step and manifest root. Event/patch
duplicates may deliberately reference the same immutable root. The history
text sequence consists of exact escaped JSON-string fragments; its prefix and
suffix are the opening and closing quote and its separator is empty. It does
not decode and re-escape cumulative text.

This promotion table is an optimization for known high-volume fields, not the
completeness argument for delta growth. In particular, the sequential response
has no top-level `results`, `feedback`, or `source_metadata` member, so those
names are not sequence slots. Result/feedback/provenance values at their actual
nested paths are ordinary recursive JSON values. Unknown fields and future
paths are also handled by the recursive model without an allowlist change.

The current assembly has these repeated transition aliases. They are recorded
as ordinary `current_field` values. Nested value/edge refs are interned only
when the exact reconstructed byte slice and digest match; indentation at a
different response depth may make semantically equal aliases byte-distinct.
They do not receive a special sequence root:

| logical value | exact response paths |
| --- | --- |
| pending transition | `assignment.pending_transition`; `project_state.execution.pending_transition`; top-level `pending_transition` |
| last transition | `assignment.last_transition`; `project_state.execution.last_transition`; top-level `last_transition` |
| just-applied transition, when present | top-level `transition_applied`; it may reuse the matching last-transition value only after exact byte equality is verified |
| pending review context | `.review_context` below each present pending-transition path above |
| historical/applied review context | `.review_context` below each present last-transition path and `transition_applied` above |
| execution checkpoints | `assignment.execution_audit.checkpoints` is the sole promoted slot; `project_state.execution.execution_audit.checkpoints` remains a recursive `current_field` subtree |

The full `assignment` and `project_state.execution` objects are not declared
identical: project-state assembly may add effective-task and scope fields. Only
byte-identical nested refs are reused. Semantic equality alone is insufficient;
a whitespace, indentation, or punctuation mismatch always produces a different
digest rather than trusting the alias name.

For every maximal non-target JSON value subtree, the builder emits a
`current_field` span in pre-order, including an empty object/array as one span.
If a subtree contains a promoted target, the builder descends until its
remaining maximal non-target values can be emitted separately. Each span is
stored as a `current_field` record whose `value_root` uses the recursive model
in section 3.2; `value_ordinal` starts at zero, is contiguous, and equals that
record's ordinal and its leaf ordinal in the manifest `current_fields` root.
Object member names, punctuation, separators, and whitespace between selected
value spans form structural gaps. Each gap is deterministically split from the
left into 65,536-byte `literal` spans plus one final remainder, omitting empty
spans. No plan literal may contain any JSON value span or promoted target. Total
plan-literal bytes are bounded by a fixed aggregate 8 MiB, while each literal
step is at most 65,536 bytes and the whole plan is bounded by 250,000 steps and
the response limits in section 5. The builder and semantic validator sum every
literal step's declared and observed byte size before publication.

Every step carries its exact half-open `[source_start, source_end)` byte range.
The first starts at zero, every end equals the next start, no span is empty,
and the last ends at `source_byte_size`; the original bytes must hash to
`source_sha256`. This rule plus the deterministic traversal/chunking above
makes the partition unique. A `literal` root resolves only to a
`literal_chunks` index and `literal_chunk` leaves. A sequence root resolves to
the exact per-slot manifest root and its declared `sequence_kind`. A
`current_field` reference resolves to the corresponding leaf of
`current_fields`. Each sequence's canonical RFC 4648 base64 prefix, separator,
and suffix decodes to at most 4096 bytes; decoding must consume the whole
string and re-encoding must reproduce it byte-for-byte.

The `current_fields` root is present exactly when this traversal emits at least
one `current_field` span.  A response whose only value spans are non-empty
promoted targets omits `current_fields`; it must not de-promote the whole
document merely to manufacture a non-empty index.  Empty promoted values still
follow the frozen empty-family rule above and are represented by their exact
recursive value span while the corresponding historical root is omitted.

The renderer visits steps in source order. It streams verified literal bytes,
recursively rendered current-field values, or the decoded sequence framing and
verified record payloads as appropriate. The render plan itself is reconstructed
through `render_plan_chunks`; opaque history fragments use `payload_chunks`;
recursive JSON syntax uses `literal_chunks`, `json_edge` leaves, and
`json_value` nodes. Envelope response identity/version and every step/root
mapping must equal the manifest, and every present native root must be consumed
exactly as frozen. The final stream must match both envelope source length/hash
and manifest hydrated length/hash before any byte is returned or pointer is
published. An unused root, unknown promoted path, alternative partition, wrong
root or sequence kind, duplicate name, ordinal mismatch, or alternate assembly
algorithm is invalid.

Consequently the normative write bound does not depend on keeping the promotion
table exhaustive for the workload this format claims: right-appends and
fixed-cardinality scalar/subtree replacements at bounded JSON depth `D`. Such a
change of `delta` source bytes writes the changed syntax, affected rightmost or
replacement `json_edges` index paths, and at most `D` ancestor edge/value
documents; it never copies unchanged sibling subtrees. With fanout 64 this is
`O(delta + D log_64 N)` physical content, and `D` is independently bounded by
section 5. Because V1 indexes use consecutive ordinals, inserting or deleting
in the middle of a sequence can renumber following leaves and is explicitly an
`O(N)` metadata case; qualification reports it separately and never presents it
as the append bound. Known promoted append-only sequences reduce the constant
and preserve stable named roots, but are not used to claim correctness.

The outer compact runtime response receives its `prompt_archive` descriptor
only after publication. That descriptor is not part of the hashed envelope or
hydrated archived bytes, preventing a self-referential archive-ID cycle.

Record authorization scope is only `(project_id, sprint_id)`. Origin response
and transition IDs are provenance, not consuming-manifest scope. A descendant
manifest in the same authorization scope may therefore reuse immutable records
and index roots from an earlier response; it must preserve each record's origin
provenance and cannot relabel it. Reuse across project or sprint scope is
forbidden even when content digests match.

### 3.5 Manifest, reachability, and pointer

An immutable manifest binds:

- project, response identity, response kind, and a sprint that is mandatory for
  runtime shadow/authoritative publication and null only for offline migration;
- source mode, safe Git provenance, and non-secret authorization provenance;
- a bounded response envelope and the exact recursive JSON value model for
  native incremental archives;
- archive format and root index references;
- hydrated byte digest, length, and precise integrity semantics;
- record counts and the effective safety limits;
- publication mode, writer version, verification time, and whether legacy or
  PAS1 is authoritative for that occurrence.

`archive_id`, `response_id`, record IDs, project storage keys, and pointer IDs
use the exact PAS1-ID-v1 preimages in section 3.3. The manifest is itself stored
as an immutable verified blob. Replaying the same durable occurrence converges
on the same response ID; attempting to bind that ID to different hydrated bytes
is
`PROMPT_ARCHIVE_GENERATION_CONFLICT`.

`created_at`, `verified_at`, `published_at`, every journal event/update time,
and authorization-decision time are copied from immutable durable occurrence
facts into the publication plan before the first journal generation. A retry
never substitutes its wall clock and therefore cannot change control bytes or
identifiers for the same occurrence.

A small immutable pointer binds one scope/response to that manifest. V1 permits
only `generation=1` and `previous_pointer_id=null`. Its entire intended
canonical document and CAS ref, including frozen `published_at`, are stored in
the durable plan. An absent response-key target is exclusively created, exact
planned bytes are idempotent success, and any different existing bytes are a
conflict; a response ID is never retargeted or replaced. Its project, sprint,
response, archive, and manifest-ref fields must exactly match the hydrated
manifest. After pointer publication an immutable archive locator binds the
archive ID to that exact pointer and manifest; its entire document/ref are
likewise frozen by the plan. Reachability is
manifest -> render/index nodes -> records -> recursive JSON values/edges and
syntax/payload chunks. Orphan blobs after a crash are safe and retained; V1 has
no collector.

For a native shadow manifest, `legacy_comparison` is mandatory and closed. It
records the exact legacy byte length/SHA-256, the renderer version, and
`matched=true`; publication is forbidden when the comparison fails. Native
authoritative manifests do not invent legacy equality and omit this block.

The publication matrix is closed:

| publication mode | source kind | format | legacy authoritative | extra proof |
| --- | --- | --- | --- | --- |
| `shadow` | `runtime_shadow` | `incremental_transition_v1` | `true` | matching `legacy_comparison` required |
| `migration` | `offline_migration` | `legacy_raw_chunks_v1` | `true` | isolated-input inventory required |
| `authoritative` | `native_runtime` | `incremental_transition_v1` | `false` | exact operator gate required; comparison absent |

No caller-supplied source label may weaken these combinations. For shadow
native publication, `legacy_comparison.size`, `.sha256`, and `.renderer_version`
must equal the hydrated integrity and renderer fields and `matched` must be
true; schema-shape validation is followed by this semantic equality check.

## 4. Publication and recovery

Publication order is mandatory for archive-producing operations:

1. Validate configured roots using resolved filesystem identities. Reject
   overlap with service, runtime, managed, prompt, protected, input, or output
   roots and reject symlink/junction/reparse traversal.
2. Before any externally visible publication, freeze a closed
   `prompt-archive-publication-plan-v1`. It binds the durable occurrence
   locator, authorization scope, response/archive identity, exact manifest,
   exact intended pointer, exact immutable archive locator, pointer
   create precondition, compatibility outputs, every control timestamp, and the
   sorted reachable-ref set digest/count. Each embedded document has its exact
   deterministic CAS ref. The plan contains no payload text, filesystem path,
   credential, or secret.
3. Publish/verify the canonical plan at its operation path and CAS ref. Then,
   with no latest/operation/response lock held, take only the pending-registry
   shard lock and apply the plan's key-level
   `upsert_planned` mutation. A crash before registry acceptance leaves only an
   unreachable immutable plan orphan and no externally visible effect. Once
   registered, the operation is durable and discoverable without a directory
   scan. A full 1024-entry shard rejects admission before registry acceptance
   or any externally visible publication; the prewritten plan may remain as an
   unreachable no-effect orphan.
4. Under the cross-process operation lock, exclusively create journal
   generation zero (`intent`), then release that lock. With no other lock held,
   take only its pending-shard lock and bind that exact journal ref into the
   same entry. The bind is completed before another journal generation may be
   published. Either ordering crash is recovered from the planned entry or the
   journal lower-bound rule below.
5. Write each new blob to a unique same-directory temporary file created
   exclusively (`CREATE_NEW`/`O_EXCL`). Flush userspace buffers, fsync or
   `FlushFileBuffers`, verify stored bytes, then publish by atomic same-volume
   rename. Never share a predictable temporary name.

   The reserved storage temporary grammar is exactly
   `.<target-name>.<uuid4-hex>.tmp`, where `uuid4-hex` is 32 lowercase
   hexadecimal digits with the RFC 4122 version-4 and variant bits. A hard
   process exit may leave this sibling at any byte length from zero through the
   target's publication bound. It has no journal authority: it is never read,
   adopted, promoted, or deleted by recovery. The offline migration
   journal-generation scanner may classify one only when its embedded target
   is an otherwise valid generation filename and the entry is regular,
   single-link, non-symlink, non-reparse, free of NTFS named streams, and within
   the immutable control-byte bound. It includes every such entry and its
   metadata in both stable directory snapshots and in the finite
   2,000,004-child scan bound, but excludes it from the retained journal chain.
   Every unknown or near-match name and every unsafe, named-stream, or oversized
   entry remains a fail-closed state conflict. This narrow non-authoritative
   classification permits retry after death without cleanup racing a live
   publisher or weakening final-generation fork detection.
6. If the final blob already exists, verify stored digest, bounds,
   decompression, uncompressed size, and uncompressed digest before reuse.
   Mismatch is corruption, not a dedup hit.
7. Publish record and index blobs bottom-up, then the immutable manifest blob.
8. Rehydrate and verify the declared final bytes before publishing a pointer.
   For legacy preservation, compare the original full-file length and SHA-256.
9. Publish or verify the exact planned pointer, then exclusively publish or
   verify the immutable archive locator.
10. In authoritative mode publish/verify the immutable route-neutral transition
    stub and apply the ordered project-global latest-content intent through the
    external-target-colocated protocol below. Shadow and migration use
    `compatibility_publication.mode=none`. Every legacy latest write, before as
    well as after first sidecar enrollment, takes that same physical lock; after
    enrollment it is a journaled `legacy_latest_publish` operation and never an
    untracked overwrite.
11. Durably mark the journal complete, release every non-shard lock, then take
    only its shard lock and remove this exact terminal pending-registry entry.
    A crash before removal is
    cleaned by replay of the terminal journal. Verified immutable objects are
    reused and dangling pre-registration objects are retained; V1 performs no
    deletion or collection.

POSIX implementations fsync parent directories after rename. Windows uses
same-volume replace plus `FlushFileBuffers` on files and documents whether
directory-handle flushing is supported. If the platform cannot provide a
claimed durability guarantee, evidence must say so and the writer remains in
shadow mode; it must not silently report full durability.

The journal state machine is one of the strict paths:

```text
intent -> blobs_verified -> manifest_verified -> pointer_published
       -> locator_published -> compatibility_published -> complete
intent -> aborted
intent -> blobs_verified -> aborted
intent -> blobs_verified -> manifest_verified -> aborted
```

The success states are journal generations zero through six respectively.
Only the three pre-pointer states may transition to `aborted`; pointer,
locator, or compatibility publication is never rolled back or relabelled as an
abort. `mode=none` still records `compatibility_published` as the verified
no-op, so all successful operations share one replay path.

`planned_reachable_set.digest` uses
`prompt-archive-reachable-set-digest-v1`: for every unique reachable ref,
encode `logical_kind`, digest, stored SHA-256, minimal-decimal uncompressed
size, and minimal-decimal stored size separated by NUL and terminated by LF;
sort those UTF-8 lines lexicographically, then SHA-256 the ASCII version label,
LF, and all lines. The count is the number of unique lines and includes the
manifest. The plan's canonical uncompressed bytes are published through the
normal deterministic CAS encoding and retained uncompressed at the discoverable
operation path before intent; the uncompressed identity in `plan_ref` must
verify both, while its stored fields verify the CAS representation.

Every journal transition holds one cross-process per-operation lock from
reading the current generation through publishing the next. Under that lock it
re-reads the current canonical bytes, verifies their generation and SHA-256,
verifies that `previous_journal_sha256` in the proposed next generation equals
those exact retained bytes, and exclusively creates the fixed generation slot
`<generation>-<sha256>.json` before replacing `journal.json`. Prior generation
copies are retained. If a crash leaves current at generation `g` and exactly
one retained, fully valid deterministic successor at `g+1`, recovery verifies
its predecessor hash and intended state bytes and installs that successor as
current; it does not manufacture another successor. More than one distinct
hash for the same generation,
missing predecessor bytes, a wrong predecessor hash, skipped/reordered state,
or a current file unequal to its retained generation is a journal fork and
fails closed. An in-process `asyncio.Lock`, last-writer-wins rename, or
schema-valid caller-supplied predecessor hash is not a CAS.

All event `occurred_at` and `updated_at` values come from the plan's
`frozen_timestamps`; each state has exactly one named value. Replayers never
sample a new clock, so the same next generation converges on the same bytes.

The contract-test reachable-set vector contains the `manifest` tuple
`(a×64,b×64,100,80)`, the `payload_chunk` tuple
`(c×64,d×64,5,5)`, and a duplicate of the first tuple. It has two unique
lines and digest
`5854e85a1e9da65f758bf957f402c7379de848c08c9420b95148e3536e8c8ffe`.

Stable locations are exactly
`projects/<project_storage_key>/operations/v1/<response_id>/<operation_id>/plan.json`,
`.../compatibility-result.json`,
`.../journal-generations/<generation>-<journal_sha256>.json`, the CAS-updated
`.../journal.json`,
`.../operation.lock`,
`projects/<project_storage_key>/pending/v1/shards/<00..ff>.json`,
`projects/<project_storage_key>/pending/v1/generations/<00..ff>/<generation>-<registry_id>.json`,
`projects/<project_storage_key>/pending/v1/locks/<00..ff>.lock`,
`projects/<project_storage_key>/sprints/<sprint_storage_key>/pointers/v1/<response_id>.json`,
`projects/<project_storage_key>/sprints/<sprint_storage_key>/archives/v1/<archive_id>.json`,
`projects/<project_storage_key>/inventories/v1/<inventory_id>.json`, and
`projects/<project_storage_key>/locks/v1/<response_id>.lock`, where every
component is already validated PAS1 output. They never include caller paths.
The pointer and locator also have deterministic CAS refs, but the stable small
files are the bounded lookup/fencing entries; API lookup never scans blobs or
operation directories. Every pending-shard update first exclusively creates
and flushes its immutable retained generation, then atomically replaces the
stable shard file with those exact bytes. The stable file must equal its named
retained generation; missing, mismatched, skipped, or forked generations fail
closed.

For authoritative publication, `compatibility-result.json` is an immutable,
payload-free receipt written only after the target-colocated latest protocol
has reached a terminal `published` or `superseded` result. It binds the exact
latest intent to the retained latest-slot ID/ref/generation and is the
deterministic input to archive journal generations five and six. Creation and
replay both ask the target authority to re-prove the immutable external plan,
terminal journal, typed retained slot, transition stub, and current external
bytes; a locally plausible receipt alone is never authority. Absence or an
unverifiable receipt leaves the operation at `locator_published`; divergent
replay is corruption.

The compatibility authority is deliberately not under the archive root. Let
`<external_latest>` be the resolved external `latest-response.json` target used
by the existing writer. Its one sidecar directory is exactly
`<external_latest>.nginx-qa-v1` in the same parent directory. Resolution and
filesystem-identity checks make supported aliases of one target converge on
this one namespace; a different runtime root or archive root cannot create
another authority. Before deriving the sidecar, the writer opens and
canonicalizes the parent directory, rejects symlink/junction/reparse traversal,
and, when the target exists, opens it without following a reparse point and
proves that it is a regular file with link count exactly one. A link count
greater than one, or inability to prove a unique directory entry, is hard-link
ambiguity and fails closed. Case-only aliases use the canonical parent and
actual directory-entry spelling; an absent target uses the closed normative
basename. Thus arbitrary hard-link paths cannot create a second sidecar
authority. For the normative target basename, the sibling directory basename
is therefore exactly `latest-response.json.nginx-qa-v1`. The closed relative
names in that directory are:

```text
latest.lock
content/v1/sha256/<digest[0:2]>/<digest>.json
intents/v1/<intent_id>-<plan_sha256>.json
intent.json
slots/v1/<generation>-<latest_slot_id>.json
slot.json
operations/v1/<response_id>/<operation_id>/plan.json
operations/v1/<response_id>/<operation_id>/journal-generations/<generation>-<journal_sha256>.json
operations/v1/<response_id>/<operation_id>/journal.json
operations/v1/<response_id>/<operation_id>/operation.lock
pending/v1/shards/<00..ff>.json
pending/v1/generations/<00..ff>/<generation>-<registry_id>.json
pending/v1/locks/<00..ff>.lock
```

`latest.lock` is the sole cross-process lock for every writer of that external
target. `content/...` is the exclusively created, fully verified intended-byte
CAS; its bytes, SHA-256, and length must equal the latest-content intent before
the external file can change. `intents/...` retains the exact canonical
publication plan containing that intent, and `intent.json` is its atomically
replaced current copy. `slots/...` retains each immutable latest-slot wrapper,
and `slot.json` is its atomically replaced current copy. A current file must
equal its retained file byte for byte. None of these paths is selected from a
caller value, and recovery reads only these fixed current files plus the exact
retained names and operation/recovery key they contain; it never enumerates any
directory.

The ordered intent is predecessor-independent. While holding `latest.lock`, a
writer first reconciles the current intent to a terminal journal state, then
compares `(source_revision, occurrence_id, operation_id)` under the exact same
revision authority. "Same intent" means byte-for-byte equality of the complete
canonical retained plan and its recomputed intent ID; equal IDs with divergent
plan bytes are corruption, never idempotence. A current slot with a
greater tuple makes the older intent `superseded`; a lower tuple permits a new
slot derived as `generation=current+1` and
`previous_slot_id=current.latest_slot_id`. A different equal-order intent is
corruption. External content is atomically replaced from the verified intended
CAS before the derived retained/current slot is installed. A crash after any
one of intended-content, retained/current intent, external-content, retained
slot, current slot, or terminal-journal publication is resolved from the exact
current intent: already-equal artifacts count as success, a missing next
artifact is completed, and any unrelated bytes fail closed. An unresolved
current intent blocks allocation by later writers rather than being skipped.
If that intent belongs to an authoritative native archive and its owning
archive plan/journal cannot be resolved and verified, a default or legacy
writer fails closed; it may not partially advance external content or the slot.

Every descriptor intent carries a closed, path-free `archive_owner` and a
`target_authority_id`. The target ID is derived from the canonical external
latest parent plus its normative leaf and is frozen in both plans. The owner
binds project, sprint, response ID/kind, operation/recovery key, archive,
pointer, locator, exact transition-stub leaf, descriptor-stub ID, and target.
`outer_plan_identity_sha256` is SHA-256 of the canonical owner projection (all
those fields except protocol and the digest itself); it is intentionally not a
circular hash of the outer plan that contains the latest intent. While holding
`latest.lock`, the coordinator calls a read-only owner verifier with both the
owner and exact intent. That verifier may take only the archive operation lock,
does not recover or mutate a journal, and proves the exact outer plan plus
current journal at or beyond `locator_published` and exact immutable pointer and
locator bytes. Target drift, a wrong safe transition leaf, mixed owner/intent,
or missing owner authority fails before any compatibility mutation.

The colocated `operations` and fixed 256-shard `pending` trees are the complete
recovery authority for `legacy_latest_publish` after enrollment, so a default
legacy launcher does not need an archive root to finish or discover such work.
They use the same retained-generation and journal-lower-bound rules as the
archive-root registry above. Startup reads exactly the 256 current shard names
and exact refs reached from them; it never scans operation, journal-generation,
intent, content, or slot directories. Native archive publication retains its
archive-root plan/journal for archive artifacts, while its exact latest intent
plan is also retained/current in this colocated namespace and cross-bound to
that journal.

The durable high-water mark is the greatest `source_revision` in the valid
current intent plan or current slot under revision protocol
`prompt-archive-durable-project-latest-revision-v1`, epoch `1`. Allocation under
`latest.lock` is strictly greater than that value; the retained/current intent
is published before the allocation can become externally relevant. A process,
launcher, runtime-root, or archive-root restart never resets it. Missing or
corrupt current/retained high-water evidence requires restoration of those
exact bytes or fail-closed publication; V1 has no epoch reset.

Before the first valid slot exists, an authoritative writer takes
`latest.lock`, observes `absent` or the exact legacy byte digest/size, and holds
the lock continuously while it exclusively creates the namespace, intended
content, retained intent plan, and current intent enrollment marker. That
marker immediately blocks every later latest writer. The writer then releases
the lock and registers the plan while holding only
the one shard lock, then reacquires `latest.lock`, proves the current intent and
external bytes still equal the frozen observation, and holds it through
intended-content publication, external reconciliation, and first-slot install.
If the observation changed, recovery updates no external content and fails
closed. Every legacy writer, including default legacy mode without an archive
root and the legacy writer accompanying shadow mode, locates and takes this
same lock and must reconcile a current intent before writing. Thus the necessary
non-nested registry phase cannot race an uncoordinated legacy overwrite.
Before any valid current-intent enrollment marker or slot exists, an ordinary
legacy writer still takes `latest.lock` but may retain its existing unjournaled
atomic-write semantics. Creating the lock-only control directory is not
enrollment. Once a valid current-intent marker or slot exists, every transition,
identity, state, and rollback write is a colocated, registered, journaled
`legacy_latest_publish`; there is no fallback to the pre-enrollment overwrite.

Authoritative outputs use the trusted project-specific target below
`.nginx-qa-projects/<project_id>`. After that target's sidecar exists, disabling
authoritative mode or selecting legacy/shadow mode does not return the project
to a shared base target: every later writer for that durable project resolves
the same project target first. A corrupt sidecar or unavailable archive-owner
proof fails closed and never causes fallback to the base
`latest-response.json`. Projects which have never enrolled a project-specific
target retain the pre-enrollment legacy base-path behavior.

Startup reads at most the 256 fixed pending-shard paths for each configured
project and never enumerates historical operation directories. Registry entries
are sorted by `(recovery_key, operation_id)` and hard-bounded to 1024 per shard.
The shard is a generation-CAS wrapper, but an immutable plan/journal freezes
only its own key-level precondition and mutation, never the whole shard
generation. Under the shard lock a writer validates the current wrapper,
requires this key to be absent or exactly the expected entry, preserves every
unrelated entry, deterministically applies its mutation, and publishes the next
wrapper. If an unrelated writer advanced the shard, it re-reads and rebases;
only a different value for the same key conflicts.

A registered `planned` entry with no journal causes recovery to acquire
`operation.lock`, canonical/ref-validate its exact plan, verify scope,
occurrence, source locator, immutable-pointer create precondition, and intended
outputs, and create the exact generation-zero intent once. A registered
`journaled` entry's journal ref is a lower-bound fence, not an assertion that
the registry update and journal CAS were atomic. Recovery loads that exact
retained generation and, under `operation.lock`, the fixed `journal.json`. The
current journal may equal the bound generation or be exactly its one
deterministic successor whose `previous_journal_generation` and
`previous_journal_sha256` bind the retained lower bound. If it is the successor,
recovery releases `operation.lock`, takes only the shard lock to rebind the
entry to that current journal, releases it, and only then may reacquire
`operation.lock` for another transition. A second journal transition is
forbidden until this rebind completes, so a gap greater than one, a different
successor, or any broken predecessor proof is corruption. This closes the crash
between journal CAS and registry rebinding without a generation-directory scan.
If a valid registered plan's source can no longer be proved, it
creates/continues the deterministic pre-pointer abort path. A malformed plan,
wrong entry/ref, registry fork, or same-key collision is storage corruption and
produces a safe fixed error without publication. A terminal journal generation
is persisted before its exact entry is removed idempotently. Plan files not
accepted into the registry are unreachable CAS orphans and are not scanned or
executed.

At `intent`, recovery reloads the durable occurrence through the plan's closed
runtime-occurrence locator (whose `runtime_occurrence_id` must equal the plan
occurrence ID), or its isolated inventory locator for migration, and verifies
the same planned set. An inventory locator includes the exact typed ref for the
immutable descriptor at the stable `inventories/v1/<inventory_id>.json` path;
it is never resolved by scanning. Each accepted offline per-entry plan also
stores a closed `migration_binding`: the derived outer batch ID, the canonical
SHA-256 of that immutable operator-gated batch plan, and its complete reserved
entry (ordinal, path digest, source length/digest, random operation ID, and
occurrence/response/decision IDs). Runtime validation reopens the stable outer
plan, requires the exact contract/tool/gate/principal/inventory semantics,
re-derives every reservation identity, compares the canonical batch-plan SHA,
and binds the manifest raw length/digest in both directions. An aggregate
inventory or caller-constructed authorization alone therefore cannot publish
null-sprint bytes. Offline resume must be supplied the same
explicit isolated root handle, whose resolved filesystem-identity digest and
full inventory digest must equal that descriptor before any source byte is
read. Missing or different input aborts before pointer publication, and no raw
path is persisted in a plan. At `blobs_verified` and later the plan/manifest
graph is sufficient; source response construction is not repeated. Startup
still validates the retained offline plan, journal chain, and registry
lower-bound fence without source authority. It leaves a nonterminal operation
pending for explicit operator resume, but may remove an exact terminal pending
entry idempotently because no further source read or publication is required.

`prompt-archive-isolated-inventory-v1` is a frozen binary digest grammar, not
an implementation-defined directory listing. Only regular files below the
already opened isolated root handle are accepted; symlink, junction, reparse,
device, socket, alternate-data-stream, hard-link ambiguity, root escape, and
case-fold collision fail before inventory creation. Each relative path is its
exact Unicode-scalar name with `/` separators, no leading slash, empty, `.`, or
`..` component, no Unicode normalization, and UTF-8 encoding. Unpaired
surrogates and platform aliases such as Windows trailing-dot/space names are
rejected. Entries sort lexicographically by those UTF-8 path bytes and duplicate
encoded paths are invalid.

The inventory preimage is exactly:

```text
ASCII("prompt-archive-isolated-inventory-v1") || 0x00 || uint64_be(entry_count)
for each sorted regular-file entry:
  uint64_be(len(path_utf8)) || path_utf8 || uint64_be(byte_size)
  || raw_32_byte_SHA256(file_bytes)
```

`inventory_sha256` is SHA-256 of that preimage. Every file is opened relative
to the retained root handle without following links; its identity is checked
before and after the streaming hash, and a size/identity/mtime change aborts the
attempt. The frozen two-entry vector (`a.json` containing `one` and
`nested/b.json` containing `two`) has digest
`aa06ae62c2f278fa7e29ffbc0a886effac09967afadd3915bd14c36a34db27b6`.

The root identity is canonical JSON `prompt-archive-root-identity-v1` from the
opened handle, never its path. Windows uses `platform=windows-file-id-v1`, a
16-lowercase-hex zero-padded volume serial, and a 32-lowercase-hex zero-padded
file ID. POSIX uses `platform=posix-dev-inode-v1` with minimal-decimal unsigned
`st_dev` and `st_ino`; unsupported or unstable identities reject migration.
For the canonical Windows vector
`{"file_id_hex":"00000000000000000000000000000042","identity_version":"prompt-archive-root-identity-v1","platform":"windows-file-id-v1","volume_serial_hex":"000000001234abcd"}`,
`source_root_identity_sha256` is
`d5c7b276d395844bf716355dde6c1ef2d4373f82668bc81f59445784da38681f`.
`source_root_handle_id` is a durable random non-secret handle label; recovery
requires the operator to supply the same label and a handle with the same
identity and inventory digest. A schema-valid descriptor alone never proves
the source.

`expected_pointer_generation` is always zero, `previous_pointer_id` is always
null, and the only response-pointer generation is one. Pointer publication is
immutable create-or-verify using the exact intended pointer document/ref fixed
in the plan: absence permits exclusive creation, identical canonical bytes and
ref are replay success, and any different bytes at that response key are a
generation conflict that is never replaced. This closes the
crash-after-create/before-journal window without treating a response pointer as
a mutable register.

The archive locator is immutable-create-or-verify: absence permits exclusive
creation of the exact planned bytes; identical bytes are replay success; any
different bytes at the archive-ID path are corruption. Descriptor publication
uses the same durable state machine. The transition-specific stub is immutable
create-or-verify and is the route-neutral
`prompt-archive-descriptor-stub-v1`, never the request-specific API descriptor.
Latest publication follows the predecessor-independent intent protocol above;
the plan never freezes a mutable slot predecessor. Exact intent/content is
idempotent success and a verified greater ordered slot is a recorded
`superseded` success, never regression. Only then may the journal advance
through `locator_published`, `compatibility_published`, and `complete`.

Concurrency uses exclusive immutable creation and compare-and-swap, not only an
in-process `asyncio.Lock`. Pointer publication additionally holds a
cross-process, per `(project,sprint,response)` lock derived from PAS1 IDs. The
latest, operation, and response locks use exclusive receipts recording the
operation ID, PID, process-start identity, and creation time but no
payload/path. A lock can be reclaimed only after OS-backed proof that the exact
process instance is dead and replay of its matching durable journal; elapsed
time alone is never sufficient. When a path needs both latest and operation
locks, it always acquires `latest.lock` before `operation.lock`; the inverse is
forbidden. A pending-registry shard lock is never nested with latest,
operation, response, or another shard lock in either direction. Before a
registry bind/update, the writer releases latest/operation/response locks,
takes exactly one shard lock for one key-level mutation, releases it, then
reacquires and revalidates any other lock/state it still needs. A competing
writer either converges on the same verified content, is safely
superseded by a greater ordered intent, or receives
`PROMPT_ARCHIVE_GENERATION_CONFLICT`.

Schema validation alone is insufficient. Before publication or replay, one
semantic validator enforces all of these cross-bindings:

- plan scope/occurrence/operation values equal its recovery locator, pending
  mutation, source locator, intended manifest, immutable pointer, archive
  locator, transition stub, and every corresponding journal value;
- every embedded document's PAS1 ID and typed ref are recomputed from its exact
  canonical bytes, and manifest/archive/pointer/locator IDs, refs, integrity,
  timestamps, and source provenance agree in both directions;
- the pending shard is derived from the recovery key; its entry operation,
  recovery key, plan ref, current journal lower bound, and timestamps equal the
  enclosing plan/journal mutation, while unrelated shard entries are preserved;
- `latest_intent.intent_id` is recomputed from the exact flattened fields in
  section 3.3; its ordering operation/occurrence equal the enclosing plan, its
  project/sprint/response provenance equals the descriptor or legacy payload,
  and its content hash/size equal the intended-content CAS;
- a latest-slot ID is recomputed from the exact intent projection plus the
  dynamically selected generation/predecessor; its `intent_id`, ordering,
  content, descriptor ref, source IDs, and timestamp equal that intent, and the
  journal result ID/ref/outcome equal the retained/current slot or the proven
  greater superseding slot;
- descriptor content requires a non-null route-neutral descriptor-stub ref and
  matching sprint/response; legacy content requires a null descriptor ref and
  matches the verified legacy payload root. The transition stub, intended
  content, external `latest-response.json`, and slot therefore cannot name
  different archives or bytes.

Any mismatch fails closed before the next visible boundary; IDs, schema-valid
documents, and caller-provided hashes never substitute for these equalities.

## 5. Resource and security bounds

The effective limits are copied into every manifest. V1 defaults and hard
ceilings are:

| Limit | Default | Hard ceiling |
| --- | ---: | ---: |
| Blob uncompressed bytes | 8 MiB | 16 MiB |
| Blob stored bytes | 8 MiB + 64 KiB | 16 MiB + 64 KiB |
| Hydrated response bytes | 256 MiB | 512 MiB |
| Render-plan literal bytes | 8 MiB | 8 MiB |
| Referenced uncompressed bytes | 384 MiB | 768 MiB |
| Referenced stored bytes | 384 MiB | 768 MiB |
| Reference depth | 512 | 512 |
| Referenced objects | 250,000 | 1,000,000 |
| Index fanout | 64 | 64 |
| JSON nesting for control artifacts | 64 | 128 |
| Recursive JSON value depth | 64 | 128 |
| Recursive JSON value nodes | 250,000 | 1,000,000 |
| Recursive JSON edges | 250,000 | 1,000,000 |
| Compression ratio | 2048:1 | 4096:1 |
| Manifest bytes | 256 KiB | 1 MiB |
| Concurrent hydrations, global | 4 | 32 |
| Concurrent hydrations, per project | 2 | 8 |
| Queued hydration requests, global | 16 | 128 |
| Aggregate hydration spool bytes | 1 GiB | 8 GiB |
| Hydration wall time | 120 s | 600 s |

Bounds are enforced before allocation where possible and continuously during
streaming/decompression. Content exceeding a bound fails with
`PROMPT_ARCHIVE_LIMIT_EXCEEDED`; no partial pointer is published and no
unbounded fallback is attempted.

Hydration acquires the authenticated project's semaphore and then the global
semaphore before opening any graph object. The FIFO wait queue and reserved
spool-byte counter are process-wide, use atomic admission, and never allocate a
spool before capacity is reserved. A full queue, semaphore limit, or aggregate
spool limit returns HTTP 429 `PROMPT_ARCHIVE_BUSY` with `Retry-After: 1` and no
lookup-dependent detail. A request that exceeds its admitted wall time closes
and removes only its isolated spool and returns the existing HTTP 503
`PROMPT_ARCHIVE_UNAVAILABLE` before response headers. Disconnect cancellation
releases all reservations. Implementations may configure lower positive values
for limits that are not fixed at both columns; they never exceed the hard
ceilings, and OpenAPI/evidence records the effective profile. Reference depth
and index fanout are fixed V1 structural constants. Limits apply across both
operator and managed route families.

JSON Schema validates the closed transport shape. The semantic validator also
requires `identity` exactly below 1024 bytes and `gzip` exactly at or above
1024, identity `stored_size == uncompressed_size` and `stored_sha256 ==
digest`, positive compressed size for non-empty gzip data, and the exact frozen
codec representation. The 2048:1 default intentionally admits the mandatory
8 MiB all-zero chunk (whose frozen encoding is 8,175 bytes, about 1026.13:1)
while remaining below the 4096:1 hard ceiling.

Graph validation is a bounded depth-first traversal over `(project storage
key, digest)`. The manifest is depth zero and every serialized ref edge
increments depth by exactly one; a target above the effective maximum is
rejected before it is opened. For every incoming edge the validator first
checks the expected logical kind, media type, parent scope, and edge-specific
sequence/record role, even when that digest is already in the completed-node
memo. Only then may a completed node be reused. A digest on the active ancestor
stack is a cycle; a completed digest is valid DAG reuse.

The fixed 512-edge V1 depth ceiling includes value-to-index, index-to-edge, and
edge-to-child-value hops. It is intentionally distinct from the 128-level JSON
syntax ceiling; both limits, the one-million-object/edge ceilings, and cycle
detection must pass independently.

Each manifest root is bound to the sequence kind in the table in section 3.4;
`raw_bytes` binds `raw_bytes` and `current_fields` binds `current_fields`.
`envelope.chunks_root` binds `render_plan_chunks`. Each literal step binds
`literal_chunks`. An `opaque_chunks_v1` record payload binds `payload_chunks`.
A `recursive_json_merkle_v1` record payload instead binds a typed `json_value`
root whose media type is exact. Every value token/prefix/suffix and edge
separator/key span binds `literal_chunks`; every non-empty container
`entries_root` binds `json_edges`, whose level-zero leaves are typed
`json_edge` refs. Each edge's typed child is a `json_value` ref. Empty
containers have no entries root. Value, edge, syntax-span, and record aggregate
sizes/hashes must equal the exact recursively reconstructed bytes.
At level zero every child has `item_count=1`; chunk leaf `ordinal_start` equals
its chunk ordinal, and a record leaf ordinal equals both `ordinal_start` and the
loaded record's `ordinal`. A root starts at ordinal zero. A chunk child's
`logical_bytes` equals its ref's uncompressed size; a record child's equals the
record payload byte size. A higher-level child resolves to an index with
`level=parent.level-1`, the same `sequence_kind`, and `item_count` and
`logical_bytes` exactly equal to the child metadata. All child ranges are
contiguous, canonical left-packed groups of at most 64, and parent totals equal
their sums. A record ref's typed logical kind must match the loaded record's
`record_type`; its payload media type must match every payload chunk ref.

Manifest counts are computed over the complete graph reachable from manifest
roots and the render-plan root after the plan has been parsed and its step refs
validated. `unique_referenced_objects` counts each unique referenced digest
once and excludes the manifest itself; `records`, `index_nodes`, and each chunk
subtype count the corresponding unique objects; `json_value_nodes` and
`json_edges` count the corresponding unique recursive-model documents.
`graph_edges` counts every
serialized ref occurrence, including edges that reuse an already-counted DAG
node. `referenced_stored_bytes` and `referenced_uncompressed_bytes` sum each
unique referenced object once. `logical_bytes` equals the final hydrated
response size and may count reused emitted content more than once. The durable
plan's reachable-set count is exactly
`manifest.counts.unique_referenced_objects + 1`, because that set additionally
contains the manifest ref. Hydrated-byte limits count every emitted byte.
Manifest counts/digests/limits, plan reachable-set digest, all roots, index
metadata, records, chunks, and the final render must agree; a mismatch fails
before headers or pointer publication.

Synthesized descriptor URLs must match their declared route kind, project,
phone when managed, sprint, and archive; the persisted route-neutral stub has
no URLs. Plan scope/occurrence/recovery/source locators, manifest, immutable
pointer create precondition, intended pointer, archive locator,
compatibility outputs, and frozen timestamps must agree exactly, including
`runtime_occurrence_id == occurrence_id`. Every embedded canonical document
must match its CAS ref and PAS1 identifier. Every journal operation, scope,
occurrence, locator, archive/ref, immutable pointer create precondition, and intended output must
equal its durable plan; each predecessor hash must equal the actual retained
prior generation bytes. These invariants fail closed even though Draft 2020-12
cannot express cross-field arithmetic or equality between sibling values.

| logical kind | required media type |
| --- | --- |
| `raw_chunk` | `application/json` |
| `payload_chunk` | the closed media type of its parent record payload |
| `render_plan_chunk` | `application/vnd.nginx-qa.prompt-response-envelope-v1+json` |
| `literal_chunk` | `application/vnd.nginx-qa.prompt-archive-json-fragment-v1` |
| `event_record` and other `*_record` kinds | `application/vnd.nginx-qa.prompt-archive-record-v1+json` |
| `json_value` | `application/vnd.nginx-qa.prompt-archive-json-value-v1+json` |
| `json_edge` | `application/vnd.nginx-qa.prompt-archive-json-edge-v1+json` |
| `index_node` | `application/vnd.nginx-qa.prompt-archive-index-node-v1+json` |
| `manifest` | `application/vnd.nginx-qa.prompt-archive-manifest-v1+json` |
| `pointer` | `application/vnd.nginx-qa.prompt-archive-pointer-v1+json` |
| `archive_locator` | `application/vnd.nginx-qa.prompt-archive-locator-v1+json` |
| `descriptor_stub` | `application/vnd.nginx-qa.prompt-archive-descriptor-stub-v1+json` |
| `latest_slot` | `application/vnd.nginx-qa.prompt-archive-latest-slot-v1+json` |
| `source_inventory` | `application/vnd.nginx-qa.prompt-archive-source-inventory-v1+json` |
| `pending_operation_registry` | `application/vnd.nginx-qa.prompt-archive-pending-operation-registry-v1+json` |
| `publication_plan` | `application/vnd.nginx-qa.prompt-archive-publication-plan-v1+json` |
| `journal` | `application/vnd.nginx-qa.prompt-archive-journal-v1+json` |

Threats in scope include untrusted payloads, malformed refs/hashes, unknown
versions, path traversal, Windows drive/UNC/ADS/case aliases, symlinks,
junctions/reparse points, decompression bombs, cyclic/deep graphs, concurrent
writers, process/power failure at every publication boundary, truncated or
tampered storage, cross-project/sprint lookup, and sensitive data appearing in
paths/logs/errors/metrics. A malicious kernel, filesystem administrator, or
compromised process identity is outside this boundary and must not be claimed
as defended.

Readers derive paths from the configured root plus validated closed components,
open without following unexpected links where supported, and re-check the
opened handle's resolved identity remains below the root. A colon in any
derived Windows component, case-fold collision, alternate data stream, or
reparse component is rejected.

Logs and metrics may contain correlation ID, format version, logical/stored
byte counts, dedup count, durations, and stable error code. They must not
contain payload bytes, source adapter secrets, credentials, authorization
headers, arbitrary filenames, user-supplied paths, or full refs.

## 6. Runtime and API contract

### 6.1 Compact and authoritative hot paths

The compact agent response remains the execution contract. Existing fields stay
unchanged. The optional additive field is exactly `prompt_archive`; it is absent
in `legacy`, is a `prompt-archive-descriptor-v1` object only after successful
shadow publication, and is required after successful `authoritative`
publication. It is never added to or required by the managed phone-specific
identity response. `null` is not emitted. This response/API descriptor is
synthesized only after the current request has been authorized; the persisted
compatibility artifact is always the route-neutral descriptor stub and never
contains a request correlation ID, agent phone, authorization material, or URL.

Managed identity, active-task, compact `.prompt`, and authoritative PAS1 paths
must not call `project_state_json()`, `history_with_patches_context()`, a PAS1
hydrator, or any equivalent full-state builder. Instrumented tests must prove
those functions are not invoked. Unchanged `legacy` diagnostic serialization
may still construct the cumulative response; shadow mode necessarily exercises
that path for exact comparison and makes no hot-path performance claim. The
gated authoritative mode is what stops cumulative construction and writes.

Archive shadow failure is recorded safely but cannot replace or corrupt the
legacy response, transition archive, `latest-response.json`, or `.prompt`, and
cannot make managed phone-specific identity fail.

### 6.2 Endpoints

The authenticated local operator/UI endpoints are:

```http
GET /api/v1/projects/{project_id}/sprints/{sprint_id}/prompt-archives/{archive_id}
GET /api/v1/projects/{project_id}/sprints/{sprint_id}/prompt-archives/{archive_id}/manifest
GET /api/v1/projects/{project_id}/sprints/{sprint_id}/prompt-archives/{archive_id}/content
GET /api/v1/projects/{project_id}/sprints/{sprint_id}/prompt-archives/{archive_id}/state
```

They retain the existing authenticated loopback operator boundary, including
its Host/Origin, session or admin/project authorization, and audit checks; mere
loopback reachability is not authentication. A managed agent never uses those
operator routes. Its routes are explicitly phone-specific:

```http
GET /api/v1/projects/{project_id}/agents/{agent_phone}/sprints/{sprint_id}/prompt-archives/{archive_id}
GET /api/v1/projects/{project_id}/agents/{agent_phone}/sprints/{sprint_id}/prompt-archives/{archive_id}/manifest
GET /api/v1/projects/{project_id}/agents/{agent_phone}/sprints/{sprint_id}/prompt-archives/{archive_id}/content
GET /api/v1/projects/{project_id}/agents/{agent_phone}/sprints/{sprint_id}/prompt-archives/{archive_id}/state
X-Nginx-QA-Scope-Token: <protected role token for agent_phone>
```

The base route returns `prompt-archive-descriptor-v1`; `/manifest` returns the
committed `prompt-archive-manifest-v1`; `/content` returns the exact declared
JSON bytes after verifying the complete reachable graph before response headers
are committed. `/state` is the explicit cold path for a native authoritative
archive: it follows only the immutable, directly addressed predecessor chain,
verifies every locator/pointer/manifest/occurrence cross-binding, and returns a
schema-versioned root-to-head `prompt_archive_state_view` with the complete
occurrence documents, current execution/evidence state, and ordered history.
Chain length is capped at 256 occurrences; aggregate hydrated bytes,
individual hydration, and final output share the configured response bound.
Cycles, legacy/shadow artifacts, scope drift, and broken lineage fail closed.
It is never invoked by identity, active-task, `.prompt`, or archive publication
paths. Authentication and bounded admission precede every archive lookup;
disconnect or timeout cooperatively cancels traversal before its work/byte
lease is released. HTTP-exposed hydrated content is restricted to
`application/json`; an arbitrary manifest media type is never reflected into a
header. A successful `/content` response sets
`Content-Disposition: attachment; filename="prompt-archive.json"`; a successful
`/state` response sets
`Content-Disposition: attachment; filename="prompt-archive-state.json"`.
Both set exactly
`X-Content-Type-Options: nosniff`, and
`Cache-Control: private, no-store`. The server may use an isolated bounded spool
to avoid partial delivery after late corruption.

A sprint-scoped route must match the manifest sprint exactly. Both
`runtime_shadow` and `native_runtime` publication require a non-null durable
sprint ID before a plan is created. A sprintless shadow occurrence is skipped
with bounded observability while its legacy output remains authoritative; a
sprintless authoritative occurrence fails closed without a cumulative fallback.
Null-sprint offline migration artifacts remain readable by the internal
compatibility reader but are not exposed by the V1 HTTP API; a future project-
only route requires a separate authorization contract.

Lookup is bounded and never scans operation directories. The server derives
the validated project storage key and reads the immutable canonical archive
locator at
`projects/<project_storage_key>/sprints/<sprint_storage_key>/archives/v1/<archive_id>.json`.
Both storage keys are PAS1-ID-v1 digests derived from the already-authorized
route project/sprint, so authorization and scope selection occur before any
lookup without exposing a caller-controlled path. The locator is
at most 64 KiB and contains only its schema/kind, project/sprint/response/archive
IDs, pointer ID, and manifest ref. It leads to the immutable create-or-verify
generation-one pointer at
`projects/<project_storage_key>/sprints/<sprint_storage_key>/pointers/v1/<response_id>.json`;
locator,
pointer, manifest, route, and requested IDs must all agree before CAS access.
Neither file contains a caller path or credential. No endpoint accepts a
filesystem path, blob digest, manifest path, remote URL, or locator override
from the caller.

`HEAD` may later expose safe length/ETag metadata, but arbitrary blob download,
recursive expansion, and caller-selected roots are not part of V1. Range
requests are rejected until their integrity semantics are separately frozen.

The descriptor exposes distinct `descriptor_url`, `manifest_url`,
`content_url`, and `state_url`; a semantic validator requires their operator or phone-specific
route kind, project, agent phone when present, sprint, and archive values to
match the authenticated request and descriptor fields. The descriptor and
errors use the committed schemas. OpenAPI must declare both route families,
descriptor/manifest/state-view JSON, `application/json` attachment content, mandatory
security/cache headers, bounded size behavior, and every stable error status.
It must not describe refs as general JSON values.

### 6.3 Authorization and non-enumeration

Hydration first authenticates the selected route family and only then performs
archive-locator or filesystem lookup. For a managed route, authorization is the
following executable predicate, evaluated against durable managed state:

1. verify `X-Nginx-QA-Scope-Token` with the protected role-token verifier for
   the exact `{agent_phone}` path identity;
2. resolve that phone through the normal phone-specific managed identity rules
   to one current active assignment/review/coordinator binding in the requested
   project and sprint; corrupt, stale, terminal, or ambiguous ownership fails;
3. require a current Scope Control binding and recompute its effective-scope
   hash from the stored `effective_core`; require its effective revision/hash
   and `scope_context` revision/hash to agree exactly; and
4. require the request route, active binding, locator, pointer, and manifest to
   have exactly the same project and sprint.

This predicate grants bounded read access to evidence in that exact active
binding's project/sprint. It never interprets natural-language `profile` or
`tasks`, never treats an ID/digest as a capability, and never derives authority
from the manifest's historical provenance. Absence of Scope Control, a missing
role token, or a stale revision/hash does not invent access. The local operator
route instead reuses its authenticated loopback operator/UI context and then
applies the same project/sprint equality checks.

Missing or invalid authentication, no current active binding, missing Scope
Control, a revision/hash mismatch, cross-project/sprint scope, and an absent
archive all return the identical `PROMPT_ARCHIVE_NOT_FOUND` envelope. The check
occurs before locator/filesystem lookup and emits no distinguishing log, metric,
latency branch, `401`, or `403`. IDs and digests are not bearer capabilities;
authorization is never inferred from a physical path.

Every manifest contains closed, non-secret authorization provenance: auth
model version, principal kind/ID, binding ID when one exists, scope source,
effective-scope revision/digest when one exists, and the authorization decision
occurrence ID. It records why publication was allowed but is not a credential or
a grant. Tokens, cookies, headers, DPAPI references, and credential material are
forbidden. All missing/invalid authentication, forbidden binding/scope, and
cross-project/sprint cases deliberately use the same 404 envelope as absence;
the V1 API never emits 401/403 and never reveals which check failed.

The provenance matrix is also closed. `managed_agent` requires a non-null
binding ID, `managed_effective_scope`, and non-null effective-scope revision and
digest. `legacy_local_principal` requires `legacy_project_context` and null
binding/scope proof. `offline_operator` requires `offline_operator_gate` and
null scope proof. `synthetic_test` requires `synthetic_test` and null scope
proof. These facts are copied from the authorization decision and never
selected by the request payload.

### 6.4 Stable errors

| HTTP | Code | Meaning / retry |
| ---: | --- | --- |
| 400 | `PROMPT_ARCHIVE_REQUEST_INVALID` | Malformed ID/query; not retryable unchanged. |
| 404 | `PROMPT_ARCHIVE_NOT_FOUND` | Absent or outside authorized scope; not retryable unchanged. |
| 409 | `PROMPT_ARCHIVE_VERSION_UNSUPPORTED` | Unknown schema/ref/encoding version. |
| 409 | `PROMPT_ARCHIVE_REFERENCE_INVALID` | Closed ref/index invariant failed. |
| 409 | `PROMPT_ARCHIVE_BLOB_MISSING` | Reachable object is absent; not automatically retryable. |
| 409 | `PROMPT_ARCHIVE_BLOB_CORRUPT` | Stored/uncompressed digest or size failed. |
| 409 | `PROMPT_ARCHIVE_INTEGRITY_MISMATCH` | Final reconstructed digest/length failed. |
| 409 | `PROMPT_ARCHIVE_GENERATION_CONFLICT` | Immutable target or fenced mutable slot differs from the planned state. |
| 429 | `PROMPT_ARCHIVE_BUSY` | Bounded hydration admission is full; retry per `Retry-After`. |
| 413 | `PROMPT_ARCHIVE_LIMIT_EXCEEDED` | Declared or observed resource bound exceeded. |
| 503 | `PROMPT_ARCHIVE_UNAVAILABLE` | Store unavailable without legacy corruption; retryable. |

Only `PROMPT_ARCHIVE_GENERATION_CONFLICT`, `PROMPT_ARCHIVE_BUSY`, and
`PROMPT_ARCHIVE_UNAVAILABLE` have `retryable=true`; every other V1 code has
`retryable=false`, enforced by schema conditionals. The JSON envelope is
`prompt-archive-error-v1`. Messages are fixed safe text;
tracebacks, paths, payload excerpts, secrets, and raw storage errors stay out of
the response.

## 7. Compatibility and rollout

Configuration names reserved by this contract are:

```text
NGINX_QA_PROMPT_ARCHIVE_MODE=legacy|shadow|authoritative
NGINX_QA_PROMPT_ARCHIVE_ROOT=<isolated absolute root>
NGINX_QA_PROMPT_ARCHIVE_MAX_HYDRATED_BYTES=<bounded integer>
NGINX_QA_PROMPT_ARCHIVE_OPERATOR_GATE=prompt-archive-authoritative-v1
```

Unset mode means `legacy`. `shadow` requires a non-overlapping validated root.
`authoritative` additionally requires the exact operator-gate value and refuses
startup unless the archive/runtime/prompt/managed/service/protected roots are
resolved, isolated, and non-overlapping. This sprint may enable it only in a
fresh isolated qualification profile. The flag's existence is not permission
to enable it on production/live roots. There is no V1 `migrate-live`, `delete`,
or `gc` mode.

In shadow mode the existing legacy bytes are written first through the current
authoritative path. PAS1 then records native delta evidence and, when a legacy
raw-preservation claim is made, reconstructs and compares the exact legacy
bytes before publishing its shadow pointer. A PAS1 failure leaves legacy files
and compact prompts unchanged. Metrics identify the failure without payload
content.

In authoritative mode PAS1 writes the bounded response envelope and delta
records directly, publishes only after verification, and returns the compact
response plus descriptor. It never builds or writes a cumulative legacy JSON
snapshot as part of that occurrence. This mode therefore supplies the claimed
O(delta + log N) hot path for the Section 3.4 right-append and
fixed-cardinality-replacement workload because every complete JSON record uses
the recursive Merkle model; named sequence promotion alone is not accepted as
that proof. The documented consecutive-ordinal middle insert/delete case
remains an explicit O(N) metadata rewrite and is outside that hot-path claim.
If publication fails, the occurrence fails closed; there is no silent legacy
fallback that would restart snowball growth.

Existing transition response paths remain API-compatible names. In legacy and
shadow modes, `response_file_path` and `latest_response_file_path` continue to
name the full legacy JSON files. In authoritative mode, those paths instead
name small JSON descriptor stubs that validate as
`prompt-archive-descriptor-stub-v1`, carry
`kind=prompt_archive_descriptor_stub`, and contain no inline history,
request-specific URL, correlation ID, agent phone, or authorization material.
The immutable transition-specific stub and the ordered external
`latest-response.json` content identify the same committed archive; full
content is hydrated only through the bounded authorized API/reader, which
synthesizes `prompt-archive-descriptor-v1` after authorization. A PAS1-aware
consumer branches on `kind`. Older readers
that require an inline full response are not compatible with authoritative
mode, which is why that mode is disabled by default and guarded by the exact
operator gate. `.prompt`, managed identity, and active-task contracts remain
compact and unchanged.

Authoritative enablement requires an exact consumer inventory covering runtime
instructions, launch prompts, web/UI readers, actor-import and reviewer flows,
operator scripts, and tests that open either response path. Every inventoried
reader must parse the bounded top-level object first: legacy response objects
continue through the inline reader, `kind=prompt_archive_descriptor_stub` goes
only through the authorized PAS1 reader, and an unknown kind fails closed. No reader
may assume that either path always contains inline `project_state`, and the
runtime recovery instruction must describe the active mode accurately.

The exact candidate inventory is
[`PROMPT_ARCHIVE_CONSUMER_INVENTORY_V1.md`](PROMPT_ARCHIVE_CONSUMER_INVENTORY_V1.md).
Its PSC-004 implementation inventory is **complete**: bounded dispatch and the
authorized API are joined by exact runtime instructions, mode-neutral UI,
fail-closed offline-tool boundaries, explicitly pinned legacy fixtures, and an
authoritative actor/reviewer consumer regression. Operational enablement still
requires isolated exact-candidate qualification, including the documented
backup/restore drill; it must not be inferred from writer-shape tests alone.

The PAS1-specific cold-backup and fail-closed restore procedure is
[`PROMPT_ARCHIVE_AUTHORITATIVE_BACKUP_RECOVERY_V1.md`](PROMPT_ARCHIVE_AUTHORITATIVE_BACKUP_RECOVERY_V1.md).
It defines the complete cross-authority recovery unit; the legacy Scope Control
rollout copy is explicitly not a PAS1 backup. The procedure is documentation,
not evidence that a backup/restore drill ran and not authorization to access a
live root or enable authoritative mode.

The immutable transition-specific stub and ordered latest intent/content/slot
are the durable authoritative handoff after locator/pointer commit. Fault
injection covers every boundary among the transition stub, retained/current
intent, intended-content CAS, external content, retained/current slot, terminal
journal, and compact response. Replay uses the same durable plan and converges
on one committed archive; it never exposes a stub for an uncommitted pointer,
advances latest to a partial archive, rewrites an older transition stub,
regresses a greater ordered latest intent, or rebuilds cumulative history. An
occurrence is not reported successfully published until its required outputs
are durable.

Readers accept both legacy inline archives and PAS1 descriptors. Changing back
to `legacy` stops new PAS1 writes but does not remove the ability to read
already published PAS1 artifacts and does not itself rewrite
`latest-response.json`. Any actual operator rollback to selected legacy bytes is
an explicit `legacy_latest_publish` plan with `write_kind=rollback`, verified
payload root/hash/size, pending-registry entry, journal, ordered latest intent,
and the same colocated lock/content/slot protocol. It is never an overwrite
caused merely by a mode toggle and never deletes CAS data or rewrites an
immutable authoritative occurrence. Offline migration requires explicit
isolated input/output roots, a durable journal, full-file pre/post inventory,
idempotent resume, and the same raw digest proof. Live roots are rejected in V1.
It accepts only opaque legacy response bytes: a PAS1 descriptor or any other
top-level `kind` is rejected before durable migration output. Native
`source_metadata` sidecars, projection receipts, and occurrence projection
proofs are not migration inputs and are never inferred from a property named
`source_metadata` inside legacy JSON. Such a property remains opaque source
bytes and is reproduced only as part of the exact raw legacy file. Preservation
of native source metadata instead belongs to the cold recovery unit, which
copies the complete archive graph and the occurrence authority's immutable
`source-metadata-projections/v1` proof tree and validates their cross-binding.
The batch plan's `tool_source_sha256` is writer provenance; compatible resume,
reconstruction, and rollback are gated by the exact contract/tool versions so
that a code-only safety fix cannot strand a durable v1 operation. Reconstruction
preflights the complete destination before its first file write and accepts only
exact outputs or deterministic plan-owned temporary leaves.

## 8. Required evidence for later nodes

Implementation and qualification must cover every PSC-A01..PSC-A20 row. In
particular:

- raw fixtures preserve whitespace, ordering, numeric spelling, line endings,
  Unicode and duplicate members;
- user data shaped like a reference round-trips unchanged;
- PAS1-ID and canonical-control golden vectors are byte-exact across retries;
- zero-history native envelopes, chunked oversized fields, descendant-record
  reuse, and mixed-root rejection are exercised;
- deeply nested current fields and pending/last/`transition_applied` aliases
  reconstruct byte-exactly while an inner-leaf change writes only changed
  value/edge documents and bounded ancestor/index paths; an unrecognized future
  field proves that promotion is optional rather than the delta-growth seam;
- recursive JSON cycles, wrong typed refs, depth/node/edge limits, malformed
  separators, duplicate native member names, and aggregate hash/size mismatches
  fail closed;
- long fixed-size event series demonstrate O(delta + log N) new physical bytes;
- every blob/manifest/journal/pointer publication boundary is fault-injected;
- same-content and different-content concurrent writers are exercised;
- missing, corrupt, truncated, wrong-size, cyclic, deep, oversized and
  unsupported data fail with stable bounded errors;
- legacy read, shadow failure, disable/rollback, and managed/legacy identity
  regressions pass;
- OpenAPI authorization and cross-project/sprint non-enumeration are tested;
- deny/audit instrumentation plus the complete command/input inventory proves
  zero access to actual live roots; this sprint does not hash those roots
  because hashing is itself a read;
- full pre/post path, size, mtime and full-file SHA-256 inventories prove
  invariance of test-owned isolated protected-root fixtures; any future live
  inventory requires separate operator authorization;
- evidence is tied to the exact commit and tree and accounts for every physical
  artifact.

Nothing in this contract authorizes reading `D:\\Prompt`, changing live state,
merging to `main`, deploying, enabling shadow or authoritative mode in
production, migrating live archives, deleting originals, or running garbage
collection.
