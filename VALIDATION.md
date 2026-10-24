# Path portability validation

Task: independent filesystem-path audit/fix, branch `work/portable-path-configuration`.
Base: `main` at `627b478e29f5f7254d279363790f46e92c114d8d` (verified through GitHub).
The Agent Channel Bridge branch/PR is not included. No live services, queues,
sprints, runtime-state or databases were modified; no merge/deployment occurred.

## Baseline findings

Four new reproductions run before production edits all failed:

1. Native POSIX directory configuration: `ValueError: managed path must be a canonical absolute DOS path`.
2. Native runtime root invariants: `RUNTIME_CONFIG_PATH_INVALID`.
3. `ManagedGitProvider` accepted a symlink ancestor instead of rejecting it.
4. Default sequential prompt template `D:\Prompt\{repository}` was relative on POSIX.

Evidence: `docs/path-portability/evidence/baseline-reproductions.xml`.
These are addressed by extending the existing configuration normalizer and
`sprint_types` path validators, with shared native boundary helpers and
`nginx_qa/path-defaults.properties`; details and compatibility rules are in
`docs/path-portability/README.md`.

An initial existing-test run used a temporary directory beneath the sandbox's
outer Git repository: **17 failed, 5 passed, 9 skipped, 4 deselected**. All 17
failed in fixture setup because the existing Git-parent isolation guard correctly
rejected that location. Attempts using absent `/tmp` and an uncreated alternate
TMPDIR fell back to the checkout and produced the same counts. We did not disable
the guard. Subsequent runs create the dedicated permitted temp parent in the
same invocation, outside all Git checkouts. Test data are temporary and isolated.

## Exact local commands and results

Working directory unless noted: `/workspace/scratch/6b32e2b88e04/nginx-paths`.
Python 3.12; existing isolated virtualenv reused only for dependencies.
For the commands below:

```sh
V=/workspace/scratch/6b32e2b88e04/nginx-qa/.venv/bin
T=/home/agent/.codex/skills/remote-skills
```

The alternate temp parent is a sandbox-permitted location, not application state.
Each test invocation creates it because non-workspace mount contents may not persist
between tool executions. On a normal host use an existing temp directory outside Git.

Baseline existing selection (first run; exit 1):

```sh
TMPDIR=/workspace/scratch/6b32e2b88e04/test-tmp "$V/python" -m pytest -q \
  tests/test_workspace_safety.py tests/test_staging_isolation.py \
  -k 'not listener and not interpreter and not snapshot_tool and not e2e_children' \
  --junitxml=docs/path-portability/evidence/baseline-existing.xml
```

Repeated with `TMPDIR=/tmp` and then `TMPDIR=$T` before creating `$T` in the
same invocation: same fixture failures. These failed environment attempts are
not counted as successful tests.

Initial four reproduction tests, before implementation (exit 1; 4 failed):

```sh
TMPDIR="$T" "$V/python" -m pytest -q tests/test_path_portability.py \
  --junitxml=docs/path-portability/evidence/baseline-reproductions.xml
```

Implementation iterations (only path tests):

* First corrected temp-root run: `mkdir -p "$T" && TMPDIR="$T" "$V/python" -m pytest -q tests/test_path_portability.py tests/test_workspace_safety.py tests/test_staging_isolation.py -k 'not listener and not interpreter and not snapshot_tool and not e2e_children' --junitxml=docs/path-portability/evidence/iteration-1.xml`: **26 passed, 9 skipped, 4 deselected, 20 subtests passed**, exit 0.
* Expanded tests: `mkdir -p "$T" && TMPDIR="$T" "$V/python" -m pytest -q tests/test_path_portability.py --junitxml=docs/path-portability/evidence/iteration-2.xml`: **51 passed**, exit 0.
* First explicit allowlist run: `mkdir -p "$T" && TMPDIR="$T" "$V/python" -m pytest -q @tests/path-portability.txt --junitxml=docs/path-portability/evidence/targeted-initial.xml`: **2 failed, 91 passed, 14 skipped, 30 subtests passed**, exit 1. One new test over-broadly mocked all `lstat` calls (including settings); it now rejects probes only of the protected tree. The second failure is described below.

Current explicit allowlist run (exit 1):

```sh
mkdir -p "$T" && TMPDIR="$T" "$V/python" -m pytest -q \
  @tests/path-portability.txt \
  --junitxml=docs/path-portability/evidence/targeted-final.xml \
  > docs/path-portability/evidence/targeted-final.txt 2>&1
```

**92 passed, 1 failed, 14 skipped, 30 subtests passed.** Repeated after minimizing
the implementation per user direction: identical counts (13.15s), exit 1.
Windows validators remain in their original module; managed configuration is
extended with a logical-directory/environment mapping rather than replaced. Skips are Windows-only
junction/PowerShell/ADS/drive-alias checks, not passes. The full allowlist remains
intact; the failing test is neither excluded nor marked xfail.

The remaining failure is
`test_persisted_prompt_templates_are_protected_with_launcher_env_unset`:
it receives `PROMPT_ARCHIVE_MIGRATION_UNAVAILABLE` rather than the expected overlap
error. It reproduces unchanged on the pre-existing baseline worktree at
`3ac994996e5ffd5fc2e79229622dd6a8f0b15267` (its Python source equals main; only spec
files differ), with the same exact test:

```sh
# cwd: /workspace/scratch/6b32e2b88e04/nginx-baseline
mkdir -p "$T" && TMPDIR="$T" "$V/python" -m pytest -q \
  tests/test_prompt_archive_migration.py::PromptArchiveMigrationTests::test_persisted_prompt_templates_are_protected_with_launcher_env_unset \
  --junitxml=/workspace/scratch/6b32e2b88e04/nginx-paths/docs/path-portability/evidence/baseline-migration.xml
```

Result: **1 failed**, exit 1. A direct isolated descriptor probe confirms:
`O_NOFOLLOW` exists, but `resolved_path_from_open_descriptor(fd)` returns `None`;
`os.readlink('/proc/self/fd/<fd>')` and `/dev/fd/<fd>` return ENOENT in this sandbox.
See `descriptor-capability.txt`. The existing fail-closed descriptor check remains
unchanged. Host validation with descriptor-path support is still required.

## Static checks

```sh
"$V/ruff" check nginx_qa/paths.py tests/test_path_portability.py
"$V/mypy" --strict --follow-imports=skip nginx_qa/paths.py
"$V/bandit" -q nginx_qa/paths.py
"$V/python" -m compileall -q nginx_qa/paths.py nginx_qa/sprint_types.py \
  nginx_qa/managed_import.py nginx_qa/git_provider.py nginx_qa/workspace_manager.py \
  nginx_qa/prompt_archive_migration.py staging_host_app.py main.py
git diff --check
```

All above pass (exit 0). Static type coverage is the new resolver, not the entire
untyped application. Targeted filesystem/security tests cover the consumers.

`ruff check --select F,E9` across changed existing Python files reports six
pre-existing findings: four in `main.py`, one in `sprint_types.py`, one in
`prompt_archive_migration.py`. The same check against `git show 627b478:<file>`
has identical code/message lists. No new findings; see `lint-comparison.json`.
The raw lint run exits 1 and is not represented as a fully clean application lint.

## Remaining acceptance

* Native Windows filesystem/junction/PowerShell/ADS tests cannot run on this Linux
  sandbox. `.github/workflows/path-portability.yml` defines the exact limited
  allowlist on Windows and Linux; actual remote results will be recorded separately.
* One local pre-existing migration path-security test remains blocked by missing
  kernel descriptor-path exposure. No fallback that weakens identity proof was added.
* Existing full regression, unrelated integrations and live services were not run.
  Deployment and operational acceptance are outside this change.

### Descriptor capability probe command

```sh
mkdir -p "$T" && TMPDIR="$T" "$V/python" - <<'PYPROBE'
import os, tempfile
from pathlib import Path
from nginx_qa.prompt_archive_storage import resolved_path_from_open_descriptor
with tempfile.TemporaryDirectory() as directory:
    path = Path(directory) / 'probe'
    path.write_bytes(b'probe')
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        print('O_NOFOLLOW available:', bool(os.O_NOFOLLOW))
        print('descriptor resolver:', resolved_path_from_open_descriptor(fd))
        for prefix in ['/proc/self/fd', '/dev/fd']:
            try:
                print(prefix, os.readlink(f'{prefix}/{fd}'))
            except OSError as exc:
                print(prefix, type(exc).__name__, exc.errno)
    finally:
        os.close(fd)
PYPROBE
```

The baseline/current lint comparison uses, for each changed existing Python file:

```sh
# FILE is one of the eight files listed in lint-comparison.json.
git show "627b478:$FILE" | "$V/ruff" check --select F,E9 \
  --output-format=json --stdin-filename "$FILE" -
"$V/ruff" check --select F,E9 --output-format=json "$FILE"
```

Comparing sorted `(code, message)` pairs (line numbers move) yields identical
lists for all eight files: six existing diagnostics and zero introduced ones.

## Hosted Linux/Windows path-only checks

Run 1: https://github.com/chartjs333/nginx-qa/actions/runs/40102290600
at commit `8dea9263eee55533d80bcf1e3ce279e35aba6e62`, exact command:

```sh
python -m pytest -q @tests/path-portability.txt --junitxml=path-results.xml
```

* Ubuntu: **93 passed, 14 skipped, 30 subtests passed**, exit 0 (7.48s).
  The descriptor-path migration test passes on a regular Linux host, confirming
  the local sandbox limitation. Windows-only skips remain explicitly skips.
* Windows initial run: **35 failed, 74 passed, 1 skipped, 11 subtests passed**,
  exit 1 (45.27s). Most fixtures received `C:/Users/RUNNER~1/...` from the
  runner TEMP configuration, correctly rejected by the existing 8.3 alias rule.
  CI now configures `TMPDIR`, `TEMP`, `TMP` to canonical `${{ runner.temp }}`.
  No validator exception was added for this runner.
* One Windows failure exposed an actual difference in `pathlib`: `lstat` of a
  child beneath a regular file can report a missing child, while POSIX reports
  ENOTDIR. The shared resolver now explicitly requires every existing ancestor
  (excluding the target, which may be a file) to be a directory. The existing
  missing-tail/non-directory test reproduces and covers this correction.

After this correction, the focused local command is:

```sh
mkdir -p "$T" && TMPDIR="$T" "$V/python" -m pytest -q \
  tests/test_path_portability.py::test_native_resolution_missing_tail_and_non_directory \
  tests/test_path_portability.py::test_reparse_ancestor_rejected_even_if_resolution_unchanged
```

Result: **2 passed**, exit 0 (0.19s). Resolver lint and strict type check pass.
