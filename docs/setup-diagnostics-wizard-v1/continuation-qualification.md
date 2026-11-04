# SDWN-003: installed continuation qualification — 2026-11-04

The installed private wizard and its native client pass local qualification.
This report supersedes the earlier corrective **NO_GO** assessment for these
local criteria; earlier evidence remains historical and has not been rewritten.
Native qualification submission, two independent reviews of this result, and
the release node still follow this commit. This is not a claim that those future
reviews or real Telegram/mobile acceptance have happened.

The installed source is `a1841e420a517dfdb9ed2c5a4b44eaeb6b3286ca` (**Q**) in
`D:\nq-sdw-launcher`, branch
`codex/sdw-qualified-host-continuation-20261104`. Both host and client use this
stable checkout. Subsequent qualification changes are documentation, evidence,
and external operator harnesses; they do not change the installed product code.

## Actual results

- **183 distinct consolidated tests passed, zero failures/errors/skips**, against
  an isolated export of Q. This includes 18 modules and the three existing
  function-style Windows launcher checks. Earlier overlapping runs are not
  added to this count. Twelve separate recipient-bound authorization helper
  tests passed; their fixtures read no real credential.
- Real headless Edge pairing used the named operator `madoev` through the
  hardened helper. The helper establishes TCP, proves the recipient and stable
  launcher ancestry, then reads the admin secret and sends it on that same
  socket. It does not follow redirects or reconnect with a secret.
- The installed HTTP/UI returned and rendered an actual Ollama `qwen2.5:7b`
  response beginning **«Ответ модели»**. It cited unavailable checks honestly and
  proposed `diagnostics.refresh`; no operation was automatically created/applied.
  Plan cancellation generated no Apply request. A second plan produced a new
  exact operation receipt; the same operation survived page reload and switching
  projects 9000 → 9010 → 9000. The successful browser run had 19 HTTP responses
  with status 200 and no JavaScript errors. The screenshot was visually inspected.
- The first browser attempt completed model advice and Plan/Apply/reload, then
  encountered one diagnostic-capture HTTP 429 on switching to 9010. This failure
  is retained. A fresh authenticated 9010 read succeeded in 0.156 seconds; the
  next complete browser run passed. Its bounded Refresh retry path was **not
  exercised** (`diagnosticBusyRetries: []`), so this report does not claim otherwise.
- Project 9000 is registry-only in C37: `STORE_UNAVAILABLE`/UNKNOWN for its native
  store is legitimate. Project 9010 has actual completed native history and six
  PASS checks for slot/runtime/resources/delivery/reviews/recovery. No state was
  borrowed from control host C35 to fabricate readiness.
- The old owned host was gracefully stopped only after proving its actual TCP
  listener, exact launcher command, stable ancestry, held native process handles,
  creation times/images, and exclusive console members. Protected backups include
  a consistent SQLite backup, environment, policy, owner and operation/client state.
  The ownership transition changed only `approved_commit` and `branch`, retaining
  initialization ID `024a41a1-ecba-47f2-9df2-b1d6d3773425` and native history.
  Normal staging Check/Start passed. A second actual Q host stop/restart preserved
  the exact browser receipt and byte-identical native/client state; the completed
  Ollama request left no unresolved admission marker before or after restart.
- The idle client was moved from its assignment checkout to Q, restarted with
  held-handle identity proof, and probed through the real wizard. All imported
  repository modules (including the controller and WSL runner) have identical Git
  blobs to the actual R4 model run. The terminal journal remained byte-identical.
  A first startup verification rejected PowerShell's trailing command whitespace;
  only surrounding space/tab normalization was added, and the same live process
  then passed exact arguments, image, ancestry, birth and readiness checks.

## Autonomous native test provenance

The actual unattended R4 sprint is
`msv1-3895ce31dfa9807650532a9bcb8a7e498bd157baeb4c9d5a54b517e1af31f737`,
project 9010, result `73c00855b3a0ff3501047a18bfff8680e9672912`.
Author 7101 and independent reviewers 7191/7192 used three distinct real model
threads, wrote/read the document and read its Git diff. Both reviews approved.
Native state is completed; client state is TERMINAL; slot, branch leases, cgroups,
process/port ownership and per-job authentication snapshots were retired.

That model execution used client `be1443c8f03e2e0875d8225979d9aa3a1a0a4ccc`
and host `65662f69db52c27b6c0972ccec8031cfb68a02fc`. It is not relabeled as a Q
execution. Q revalidated the exact native result/reviews and idempotent Apply,
and actually restarted/probed its blob-identical installed client. No new model
run was requested for an unchanged controller. Older supervised R3 evidence is
separate and does not substitute for R4.

## Using the installed local version

Open **http://127.0.0.1:18038/setup**. The three commands in `C:\nqt37` are
`Запустить мастер.cmd`, `Вход в мастер.cmd`, and `Остановить мастер.cmd`.
For browser login, copy its ten-character pairing code into the login helper.
The helper always uses the stable named operator `madoev`.

All current helpers and ownership receipts are in the protected directory:

```text
C:\nqt35\client\sdw-continuation-20261104\qualification-final
```

After a machine restart, start the owned test dependency, client, then wizard:

```powershell
$h = 'C:\nqt35\client\sdw-continuation-20261104\qualification-final'
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "$h\start_git_fixture.ps1"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "$h\start_client.ps1"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "$h\start_wizard.ps1"
```

These helpers refuse an unproved occupied port. The Git fixture helper proves
the existing loopback 9418 process chain and provides the same explicit startup
arguments used by the accepted R4 fixture. Its existing-owner path was exercised;
the daemon was retained, not restarted during this qualification. It exports only
the accepted local test-fixture directory. It must remain available for that
profile's Git reads/publication. Do not replace its bare repository or immutable
manifest. Existing WSL distro `NginxQaWorker-20261104` and runner configuration
`D:\nq-sdw-worker-runtime\runner-config.json` are unchanged; no global WSL/Docker
configuration or shared Ollama instance was changed.

`stop_wizard.py --stop` gracefully signals only the proved wizard console and
refuses nonterminal native/client work. `verify_stable_client.py` is read-only by
default. Its explicit `--terminate-owned-idle-client` switch is a forced **idle**
client recovery/qualification tool, not a way to stop active work. It uses held
native handles and requires terminal native/client journals. For active work,
use the existing native engine's owned Stop operation first. Never kill a PID
copied from this document or a historical receipt.

Current host receipt: `host-launch-pid.txt` plus timestamped `host-launch-*.json`.
Current client receipt: `stable-client-launch.json`, `stable-client-owners.json`.
Readiness/equality: `stable-client-restart-evidence.json`,
`client-runtime-blob-equality.json`, `git-fixture-owner-proof.json`.
Protected rollback material: `protected-pre-upgrade-backup`; **do not publish it**.
The old remote host branch remains at 65662f6. Any rollback must first stop/prove
the current owner and validate the exact backed-up identity/environment/policy
and database compatibility; do not overwrite current native history with an old
database or blindly restore an ownership marker. Release must use current helper
pins, not the historical 656 helper.

## Limits and release handoff

The client profile is **docs-only-v1**, not general coding. Its configured test
project is 9010. Host 18038 remains private loopback with Telegram/public tunnel
disabled. The genuine inherited Cloudflare TLS/HTTP boundary test passed 24
checks on PR17 release `89f7f09b318a3d9d78ceb2d8c92b6e730e644626` on 2026-11-03;
its owned tunnel was then stopped. This qualification does not turn that historical
gateway result into a live public deployment of the wizard.

Actual bot/webhook credentials, an allowed Telegram actor, and physical
mobile/tablet acceptance are unavailable in the owned configuration. They remain
external deployment inputs, not missing user permission. No other bot's credentials
were borrowed and no messages were sent. Madoev and ports 18035/18036/18037 were
not changed. No main merge or PR merge was performed.

The machine-readable [qualification report](evidence/continuation-qualification/qualification.json)
and [evidence manifest](evidence/continuation-qualification/evidence-manifest.json)
include exact source IDs and hashes. After native submission and two independent
reviews, release should consolidate the accepted source into the existing delivery
branch/PR and issue its own current GO report. Do not edit historical receipts to
claim those subsequent actions already occurred.
