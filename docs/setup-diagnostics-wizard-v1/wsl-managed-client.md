# Owned WSL executor for the native managed client

The existing `nginx_qa.managed_codex_client` can select `sandbox_backend:
"wsl2-bwrap"`. It still uses the native Managed Sprint Engine for assignments,
graph transitions, independent review identities and accepted results. The Linux
helper is a bounded subprocess transport; it has no native API client, scheduler,
assignment authority or Git publication credentials.

This backend admits **only `docs-only-v1`**: one to 32 explicitly approved
`docs/*.md` paths in a small, regular-file repository. It is suitable for the
wizard's disposable self-test. It is **not** a general coding worker for nginx-qa
or arbitrary repositories. In particular, a repository containing a file larger
than 1 MiB, even only in history, is rejected. Existing Windows MXC readiness and
its honest Windows 10 BLOCKED result remain unchanged.

## Provision and select the backend

Use a separately imported, operator-owned Ubuntu 24.04 WSL2 distribution named
`NginxQaWorker-...`. The qualified installation is `NginxQaWorker-20261104`, under
`D:/nq-sdw-worker-runtime/distro`. Its Windows runtime directory is protected for
the operator account and SYSTEM. A root marker at
`D:/nq-sdw-worker-runtime/.nginx-qa-worker-owner.json` contains:

```json
{"kind":"nginx-qa-isolated-worker-runtime","distro":"NginxQaWorker-20261104"}
```

The installer verifies both that marker and the actual HKCU WSL registration's
BasePath. It requires an already imported guest; importing an OS and changing
Windows ACLs remain explicit operator provisioning. Run from the delivered
checkout, with the complete Linux Codex resources from the pinned installation:

```powershell
C:/nqt37/v/Scripts/python.exe -B scripts/install_wsl_worker.py --runtime-root D:/nq-sdw-worker-runtime --cli-directory C:/Users/madoev/.antigravity-ide/extensions/openai.chatgpt-26.1021.22027-win32-x64/bin/linux-x86_64 --bootstrap
```

`--bootstrap` installs guest packages, creates nonprivileged UID 1000 and writes
per-guest `wsl.conf` with automount and Windows interop disabled. It restarts only
that marked guest. Omit `--bootstrap` when updating an already provisioned idle
guest. The installer refuses live owned cgroups. Do not run it concurrently with
another installer or client startup. It never calls global `wsl --shutdown`,
changes the default distribution, starts Docker, changes global Codex settings,
creates credentials, starts a client service or calls a model.

The installer copies the supervisor to `/opt/nginx-qa-worker/supervisor.py` and
the four Linux CLI resources below `/opt/nginx-qa-worker/bin`. The installed
runtime does not depend on an assignment checkout. It generates
`D:/nq-sdw-worker-runtime/runner-config.json` with exact SHA-256 pins. Merge those
fields into the existing protected managed-client configuration, retaining its
native project/manifest/origin/branch/doc-path admission controls and token
references. `node_path` and `codex_script` are unused for this explicit backend.

```powershell
C:/nqt37/v/Scripts/python.exe -B -m nginx_qa.managed_codex_client --config D:/nq-sdw-worker-runtime/runner-config.json --check
```

Readiness needs only the runner configuration. It executes actual trusted author
and reviewer sandbox probes plus CLI login status, without a model request,
native engine request, HTTP listener, session resume or sprint creation. PASS
establishes current local readiness, not task completion or future availability.
A missing pin, expired login, exposed drive, failed canary or unretired group
blocks admission. There is no weaker-backend fallback.

## Boundary and resource contracts

The trusted boundary consists of the Windows controller account, the owned Linux
root supervisor, installed OS packages, pinned CLI resources and Codex's sandbox
implementation. SHA checks detect resource drift; they do not attest the entire
OS or protect against a malicious Windows administrator or Linux root operator.

Each invocation projects the exact native HEAD through a Git bundle. Before
export, Windows inspects all stored Git object sizes, including unreachable and
historical objects: at most 8192 objects, each at most 1 MiB, and at most 32 MiB
total uncompressed. The current tree is limited to 4096 regular files and 32 MiB;
the compressed bundle is separately limited to 32 MiB. Symlinks, submodules,
nonregular destinations, junctions and hardlinked destination files are rejected.
Linux clone/checkout helpers run in owned bounded groups and PID namespaces.

Each model invocation gets a fresh private filesystem/PID/IPC/user namespace,
UID/GID 1000 and no capabilities. The projected repository and Git metadata are
read-only. An author's exact precreated approved files have individual writable
bind mounts; their parent directories stay read-only. Reviewer files are all
read-only. New-file placeholders are not imported if the model never writes them.
Only validated document bytes are imported into Windows, after checking HEAD and
all destination contents again. Model-owned Git state is never imported.

The Codex tool namespace denies network, `/private` credentials, host drives and
sibling attempts. The trusted CLI parent keeps network access for its normal
authenticated model transport. Tools do not receive that authority. Readiness
performs actual socket operations against a proven reachable loopback listener
and an external address, actual forbidden reads/writes and role-specific writes;
it does not infer isolation from configuration flags. Per-invocation requirements
disable web search, hooks, MCP servers, plugins/apps, browser/computer tools,
multi-agent delegation, inherited rules, user configuration and remote control.

Limits per attempt are 2 GiB cgroup memory, 64 processes, 50% CPU quota, 30–1800
seconds role time (default 600), and a 16 MiB hard per-file write ceiling with
core dumps disabled. The latter permits Codex's private SQLite WAL; exported
documents still have the stricter 1 MiB limit. There are at most 32 writable
document files. Temporary namespace memory is charged to the cgroup. Request,
prompt, result and captured-output sizes are bounded separately. These are
per-attempt limits; retained attempt evidence consumes disk over time and requires
operator retention/capacity management. No broad recursive cleanup is automated.

## Login lifecycle

The implementation uses the ordinary Codex CLI login cache interface, consistent
with the official [headless authentication guidance](https://learn.chatgpt.com/docs/auth#fallback-authenticate-locally-and-copy-your-auth-cache).
It never uses a copied token in hand-written provider HTTP requests, and never
copies the complete Windows Codex home. Only a protected auth snapshot travels
over owned process stdin into a mode-0600 private temporary file. Tokens are not
logged or committed. The original Windows auth file is never written.

This version intentionally removes refresh authority from that snapshot. It
checks the cached access-token expiry before dispatch, requiring remaining
lifetime greater than the role deadline plus 120 seconds. A 401, revoked login,
unsupported auth JSON, keyring-only login or expired snapshot blocks execution;
it never silently switches authentication modes. Renew the normal Windows CLI
login through its supported UI/CLI flow and repeat readiness. A fresh snapshot
is taken per invocation. This qualified pinned CLI accepts the snapshot; it is
not a promise that another CLI version preserves the same auth-cache behavior.
Independent long-lived refresh and automatic propagation of refreshed tokens
between Windows and WSL are deliberately not implemented.

## Stop, crash and recovery

Only one Linux execution holds the guest-wide admission lock. All model and Git
preparation descendants enter owned cgroups before exec. Parent-death signals,
PID namespaces, deadlines and stdin heartbeats bound orphan lifetime, including
double-fork/setsid processes. Cleanup signals stable pidfds after rechecking group
membership. It does not kill a raw PID from an old receipt.

Durable launch intent precedes fork. Stop installs a permanent job-generation
cancellation fence before scanning cgroups, then waits for the job lock. It finds
groups by ownership even without a post-fork `active.json` receipt. STOPPED is
returned only after the owner cannot spawn and all matching groups are empty.
The interrupted job cannot be silently rerun with the same generation. An
unconfirmed cleanup retains the Windows admission slot in quarantine; repeated
readiness releases it only after actual stop confirmation. A restarted controller
also rejects any old live group, independently of its in-memory semaphore.

The existing native client still owns durable `OUTPUT_READY` and immutable
submission replay. Successful output can resume publication without running the
model again. A crash before that checkpoint needs native reconciliation/rework;
do not remove a cancellation fence to manufacture a retry. Stop and normal
cleanup remove the fixed private auth file. Hard supervisor loss can retain that
protected file until owned-job reconciliation; do not export raw guest attempts
as public evidence. Configuration/hash drift can block cleanup until the operator
restores the qualified runtime; no STOPPED claim is made in that state.

## Verification and scope of evidence

Sanitized evidence is in `evidence/wsl-worker-qualification/`. The recorded
source-file and installed runtime hashes identify the tested bytes. The pinned
Linux CLI is `0.164.0-alpha.11.1`; Ubuntu, Python, Git and bubblewrap are trusted
guest packages. An upgrade requires reinstallation, readiness and qualification.

```powershell
C:/nqt37/v/Scripts/python.exe -B -m unittest tests.test_managed_wsl_runner tests.test_setup_worker_corrective tests.test_managed_client_git_transport -v
C:/nqt37/v/Scripts/python.exe -B tests/wsl_runtime_qualification.py --config D:/nq-sdw-worker-runtime/runner-config.json --output C:/operator/wsl-lifecycle.json
C:/nqt37/v/Scripts/python.exe -B tests/wsl_cli_smoke.py --config D:/nq-sdw-worker-runtime/runner-config.json --fixture-parent C:/operator --output C:/operator/wsl-cli-smoke.json --run-model
```

The first command covers controller, native-client/Git-publication and historical
object-expansion regressions. The second uses real disposable guest processes:
deadline, controller EOF, stop before launch, missing receipt/restarted admission,
supervisor SIGKILL, process pressure and file-write pressure. It ends with actual
author/read-only reviewer boundary probes. The missing-receipt fixture deliberately
relinquishes ownership while retaining the WSL transport; it is not reported as a
hard process-crash test. The separate SIGKILL case tests actual supervisor loss.

The final command explicitly consumes a real model turn, writes a unique marker
read from the disposable repository into the approved document, checks exact
host-side file contents and verifies the Windows login remains unchanged. None of
these tests submits native results or approves a reviewer. Native end-to-end
self-test acceptance and two independent implementation reviewers are separate
engine-owned stages; they cannot be inferred from a model's `DONE` summary.
