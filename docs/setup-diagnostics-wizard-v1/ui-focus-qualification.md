# Installed focus and recovery qualification

The accepted product `c9193d7d6cab67f0b191a86f5e0e268bce074f9e` is installed at `D:\nq-sdw-launcher` on the immutable branch `codex/sdw-qualified-ui-focus-recovery-20261104`. The previous `fdafbf58`, `a1841e4` and `65662f6` deployment refs remain unchanged. This report follows the retained 7002 `NEED_DECISION` findings; it does not rewrite their failure evidence or the intermediate `abd876` Apply-order defects.

The machine-readable results and exact artifact hashes are in `evidence/ui-focus-qualification/qualification.json` and `evidence-manifest.json`. This qualification does not submit its own native result or reviews. Native 7005 reviews and the subsequent release decision remain the coordinator's responsibility.

## Tested behavior

The actual installed private Edge browser uses a 390×844 viewport. Escape, keyboard Cancel and × return visible focus to the enabled Plan opener. Tab/Shift+Tab reach modal controls and cannot focus background controls; a native Tab transition into browser chrome is allowed and is not mistaken for background-page focus. Pending Apply preserves the dialog and stored request identity under Escape/Cancel/×. Completion restores valid visible focus without a duplicate request.

The six installed scenario groups cover natural 120-second plan expiry with an authoritative durable BLOCKED receipt and same-key replay, explicit fresh-plan recovery, actual cookie loss and named re-pair, Inspect/Escape/reload preserving the original request, receipt restoration and project switching. The 403 response case injects a response only after the real harmless `setup.open` effect and confirms authoritative inspection without another Apply. Busy/network/missing-worker/provider/fallback cases are explicitly routed fixtures; they are not live provider failures or model execution evidence.

The first run's real snapshot 429 caused the harness to explicitly Refresh. The old assertion then incorrectly required the prior stale-plan notice to remain. The preserved failure report shows that the notice existed before that newer work; clearing it is the intended completion fence. The corrected harness requires the durable receipt and cleared attempt throughout, and requires the old notice only if it did not perform a newer Refresh. No product code was changed during qualification.

Two additional isolated suites run the exact installed HTML/CSS/JS with all HTTP intercepted: ten focus groups and two reordered Apply completion cases. They include detached/hidden origin fallback, another open modal, newer session and newer Plan/Cancel. They supplement, and do not replace, installed backend evidence.

The existing 192-test regression baseline remains tied to `fdafbf58` (zero failures/errors/skips). The only product changes since that baseline are `setup.js` and the exact wizard deployment-branch allowlist in `run_staging.ps1`. Current affected UI/launcher tests are reported separately; overlapping counts are not added. The recipient-bound authorizer and startup wrapper have separate 12- and 3-test checks.

## Current operator entry points

Use the three commands in `C:\nqt37`: `Запустить мастер.cmd`, `Вход в мастер.cmd`, and `Остановить мастер.cmd`. Their versioned helpers are under:

```text
C:\nqt35\client\sdw-ui-recovery-20261104\focus-qualification
```

`start_all.ps1` checks/starts the owned loopback Git fixture, then the stable installed client, then the private wizard. It stops on failure and creates no job. The wizard is at `http://127.0.0.1:18038/setup`, the native client at `127.0.0.1:18039`. Run `authorize_wizard_browser.py` with the displayed pairing code; it proves the connected TCP recipient and stable launcher ancestry before reading the admin secret, sends it on the same socket, and uses the persistent operator `madoev`.

`stop_wizard.py --stop` stops only the proven owned wizard after held-handle, birth, image, argv, exclusive-console and native/client-quiescence checks. Without `--stop` it is read-only proof. `verify_stable_client.py --terminate-owned-idle-client` is the narrowly scoped idle-client stop; it refuses nonterminal sessions and proves held native identities. Restart it with `start_client.ps1`. Do not use historical helper receipts or raw PID termination. Current receipt filenames are `host-launch-pid.txt`, timestamped `host-launch-*.json`/`host-stop-*.json`, and `stable-client-launch.json`/`stable-client-owners.json` in the current helper directory.

The loopback-only Git fixture uses `C:\nqt37\qualification\git\setup-selftest.git`. Its original ownership receipt deliberately remains `C:\nqt35\client\sdw-continuation-20261104\r4-helper-launch.json`; it is still the original daemon. `start_git_fixture.ps1` verifies that receipt or its own subsequent launch receipt. An unknown listener is rejected. WSL distro `NginxQaWorker-20261104` and `D:\nq-sdw-worker-runtime\runner-config.json` remain unchanged.

The controlled restart's first harness waited on an anonymous output pipe inherited by a background process after the starter had already exited. The actual held-handle stop and new-owner launch proofs were retained. A corrected harness uses regular log files and resumed verification against those exact receipts; the same successful operation and native/client history were recovered without another restart. Only the two verified stale test-helper processes were terminated; running product services were not signalled. `restart-harness-pipe-cleanup.json` records this distinction.

New packaged operator helpers match their tested on-disk copies byte for byte, including `check_installed_native_history.py`. The earlier report-only helper wording drift remains historical evidence and is not silently rewritten.

Protected backups remain only in the operator journal's `protected-pre-upgrade-backup`. They include secrets and must not be published. The repository includes only sanitized manifests and receipts. Owner initialization, existing native state and terminal client journal are preserved; only the exact owner commit/branch changed. The environment bytes are unchanged.

## Execution provenance and limits

The actual unattended R4 run remains tied to client `be1443c8f03e2e0875d8225979d9aa3a1a0a4ccc` and host `65662f69db52c27b6c0972ccec8031cfb68a02fc`, with real result `73c00855b3a0ff3501047a18bfff8680e9672912`, one author and two independent model reviewers. Current installation verifies all 16 imported client module blobs unchanged, existing schema definitions unchanged, real worker connectivity, historical receipt replay and byte-identical native/client history. It is not relabelled as a new model run on c9193d. The supported worker profile remains `docs-only-v1`.

Real model advisor evidence retains its earlier exact source provenance; no model execution was needed for this UI-only follow-up. Project 9000 is registry-only in this host and legitimately reports `STORE_UNAVAILABLE`; project 9010 has the actual completed native history. Readiness is never fabricated from another host's database.

The live public gateway remains fail-closed with `503 GATEWAY_OFFLINE`. An isolated enabled production gateway denies all five wizard/private inspection paths with 404. The prior actual Cloudflare TLS/HTTP boundary run (24 checks, PR17 `89f7f09`, 2026-11-03) remains historical evidence; the tunnel is stopped. Designated Telegram bot/webhook credentials, an allowed real actor and physical Telegram/mobile acceptance remain external inputs. The headless narrow-width checks do not claim a physical phone test. No unrelated server, main branch, PR merge, WSL service or Ollama service was changed.
