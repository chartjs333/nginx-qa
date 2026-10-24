# Portable path configuration

The existing `normalize_managed_runtime_config` remains the configuration entry
point. Its `MANAGED_DIRECTORY_ENV` mapping binds logical roots to the existing
environment variables. The existing path validators in `sprint_types` are extended
with DOS/POSIX identities, joins and containment; Windows validators stay in place.
The small `nginx_qa.paths` module shares native boundary checks and default parsing
between consumers, avoiding circular imports between provider/workspace/importer.
`path-defaults.properties` owns platform-specific prompt defaults. There are no
new dependencies and the frozen managed runtime schema remains v1.

## Configuration and compatibility

| Setting | Meaning / precedence |
| --- | --- |
| `NGINX_QA_SERVICE_ROOT` | Native absolute checkout/service directory |
| `NGINX_QA_RUNTIME_ROOT` | Native absolute runtime directory; `processes`, `logs`, `pids`, `leases` are derived children |
| `NGINX_QA_MANAGED_ROOT` | Dedicated managed workspace/repository root |
| `NGINX_QA_PROMPT_ROOT` | Managed prompt directory, unchanged semantics |
| `NGINX_QA_PROTECTED_ROOTS` | Prefer a nonempty JSON string array; legacy native path lists and Windows semicolon lists remain accepted |
| `NGINX_QA_SEQUENTIAL_PROMPT_ROOT` | New optional default for legacy sequential prompts, separate from managed prompts |

Existing valid persisted `sequential-prompt-settings.json` templates take priority.
For missing/invalid fields, the sequential environment setting overrides the
properties default: Windows retains `D:\Prompt`; POSIX uses
`~/.local/state/nginx-qa/prompts`. Defaults are frozen when the application module
loads. Reading configuration creates no directories. Empty/relative overrides
fail closed. Only the legacy prompt convenience setting expands `~` and environment
variables; managed roots remain explicit absolute paths.

See `.env.paths.example` for Linux directory configuration and the unchanged
`.env.staging.example` for the frozen Windows staging profile. The Linux example
does not provision staging ownership or authorize deployment. The existing
PowerShell launchers and installation provenance/ownership checks are unchanged.

## Resolution and security

* `absolute_path_key`, `join_path`, and containment helpers inspect stored paths
  without host-dependent interpretation. POSIX identities preserve case; DOS
  identities retain Windows case-insensitive, device-name and namespace rules.
* `native_path` validates host syntax without opening directories. It is suitable
  for constructing deny lists that must be checked before filesystem probes.
* `resolve_native_path` additionally inspects every existing ancestor with
  `lstat`, rejects symlinks and reparse points (including junctions), and compares
  the resolved identity with the submitted identity. Missing tails are allowed;
  permission errors, non-directory ancestors and aliases fail closed.
* `normalize_configured_path` permits lexical inspection of a foreign persisted
  configuration. Foreign paths never become relative native paths. Filesystem
  consumers require `resolve_native_path` and reject them before access.
* Windows-specific public helpers remain defined in `sprint_types` with
  their original strict Windows semantics. Generic managed invariants use the
  new helpers for configuration, mirror/workspace bindings, integration workspace
  paths and process cwd/log/runtime/executable paths. Mixed DOS/POSIX root sets
  fail validation.
* Workspace protected/home/install/Git-parent exclusions, mutation-time checks,
  locks, ownership proofs, and migration descriptor checks remain enforced.
  The resolver is not an atomic filesystem sandbox and does not replace those
  checks. Prompt output resolution also rejects redirected ancestors.
* Offline migration protects both the legacy Windows deny root and the configured
  sequential default, plus persisted custom templates, before opening a protected
  tree. No persisted state is rewritten and no automatic path relocation occurs.

## Audit matrix

| Finding | Change | Coverage |
| --- | --- | --- |
| POSIX managed environment rejected as non-DOS | Shared normalization | Native config, missing tails, malformed roots |
| POSIX saved workspace/process facts rejected by Windows-only invariants | Flavor-aware identities, joins and containment | Translated snapshot path bindings and negative tampering |
| Provider silently follows a root symlink | Native resolver at provider/workspace/staging boundaries | Real symlinks, dangling links, reparse attributes, Windows junction test |
| `D:\Prompt` hardcoded in runtime and migration protection | Properties plus environment override | Windows compatibility, POSIX default, persisted custom settings |
| Prompt output resolution accepts redirected ancestors | Shared native resolver after placeholder expansion | Directory and file paths, no target mutation |
| Windows protected-root lists split at drive colon on Linux | Central strict parser | JSON validation and legacy separators |
| Test temporaries placed beneath a Git repository | Use a dedicated temporary root outside all checkouts | Existing Git-parent security checks remain enabled |

Run only the explicit selection in `tests/path-portability.txt`:

```sh
python -m pytest -q @tests/path-portability.txt
```

Requires pytest >=8.2. `TMPDIR`/`TEMP` must point outside Git checkouts. The GitHub
Actions workflow runs this same selection on Linux and Windows with no service
startup, secrets, deployments, or full regression suite. See `VALIDATION.md` for
actual results and limitations, including checks that did not pass locally.
