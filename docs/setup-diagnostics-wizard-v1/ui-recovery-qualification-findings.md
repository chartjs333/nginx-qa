# SDWU-002 installed qualification findings — 2026-11-04

**Decision: NEEDS_REWORK. Do not release this result as a passing E03 gate.**
The installed product is `fdafbf58db85555706b8ea04e11c1d4450b0f120` in
`D:\nq-sdw-launcher`, branch `codex/sdw-qualified-ui-recovery-20261104`.
The qualification assignment changes only documentation, evidence and external
operator/test harnesses; no product repair was made here. Native submission and
independent review of these findings are the parent coordinator's next actions.

## Confirmed product defect: E03_FOCUS_RETURN

In actual Edge at 390 × 844, focus the **«Сформировать план»** button for
`setup.open`, press Enter, traverse the confirmation dialog using Tab/Shift+Tab,
and press Escape. The dialog closes without Apply, but focus remains on BODY
with `document.hasFocus() === true` and no visible focus. It does not return to
the invoking button. This is repeatable on the installed product.

The likely cause is the disabled opener during asynchronous planning and
`closePlan()` closing the dialog before `syncControls()` restores the controls.
See `nginx_qa/static/setup.js`, `syncControls`, `createPlan`, and `closePlan`.
An explicit safe return target may be needed; merely changing a test assertion
would not fix the keyboard flow. A repair must also preserve uncertain request
identity, actor/project fences and the original idempotency key.

The native dialog's separate Tab hop to browser chrome is **not** this defect:
that hop has `document.hasFocus() === false`. All focused controls inside the
page had visible focus; the background control remained inert while the modal
was open. The original overly strict chrome-focus assertion and its correction
are retained in the evidence rather than misrepresented as a product repair.

## Completed verification

- **192 consolidated regression tests passed, zero failures/errors/skips** on an
  isolated export of the exact installed commit. Separate changed-helper checks:
  12 authorization boundary tests and 3 fail-fast startup-order tests passed.
  These counts are not added to overlapping earlier qualification runs.
- The final actual Edge run contains six scenario groups and 63 HTTP responses.
  Five groups passed; the mobile/keyboard group retains the focus-return failure.
  Mobile dashboard and dialog have no horizontal overflow. Tab, Shift+Tab,
  Enter, modal exclusion and Escape cancellation worked. Screenshots were
  inspected directly; this is browser-width testing, not a physical phone test.
- A harmless real server plan naturally expired after approximately 120 seconds;
  no clock or server state was edited. Apply returned a bound
  `409 PLAN_STALE / NOT_STARTED` with a durable BLOCKED receipt. An explicit
  delayed same-key replay returned that same BLOCKED operation. A refreshed,
  explicitly created new plan then succeeded. No automatic Apply occurred.
- Clearing only the isolated browser context's cookies produced a real 401.
  The original attempt/key survived named re-pairing, inspection, Escape and page
  reload. Only an explicit retry sent the same original request; it succeeded
  once and cleared the bookmark. No unrelated operator permission was revoked.
- A routed 403 **after a real successful harmless backend operation** exercised
  uncertainty after effects. Actual passive inspection recovered the original
  receipt without a second Apply. The 403 was an injected response, not a claim
  of real permission revocation.
- Explicitly labelled routed 429, network failure, missing worker/provider
  snapshot and deterministic-advice fixtures exercised honest unavailable UI,
  explicit Refresh/reconnect and no automatic operations. They are not live
  provider/model outage evidence. The genuine prior Ollama execution retains its
  original source provenance; no new model call was needed in this qualification.
- The exact successful operation remained visible after reload and project
  switching 9000 → 9010 → 9000, with no automatic Apply.
- Live gateway 18037 returned fail-closed `503 GATEWAY_OFFLINE` for five private
  paths, including the new inspection endpoint. An isolated **enabled** production
  gateway TestClient returned 404 for all five. No gateway configuration or control
  credential was changed/sent. The genuine historical Cloudflare 24-check proof
  remains separate; the current offline gateway is not a new public TLS deployment.

Two harness issues are retained transparently: initial pairing encountered a
bounded diagnostic 429, and an explicit out-of-band same-key Apply replay
invalidated issued snapshots before the harness attempted a new plan. The final
run permits bounded explicit Refresh and refreshes after its own replay. Neither
issue was silently counted as a product fix. The only confirmed product blocker
in the completed run is `E03_FOCUS_RETURN`.

## Installation and provenance handoff

Only the owned host and idle client were stopped, with fresh held native handles,
creation/image/argument/ancestry proof and exclusive host console proof. A protected
consistent backup precedes the upgrade. Initialization ID
`024a41a1-ecba-47f2-9df2-b1d6d3773425`, all three native sprint rows and the terminal
client journal were preserved. The ownership tuple changed only source commit and
branch. Environment values are semantically unchanged: Ollama provider/model were
reasserted, not newly configured. Normal staging Check/Start passed.

Sixteen imported client/worker modules have identical Git blobs to the original
actual R4 execution; all existing schema definitions are unchanged, with only two
unrelated inspection definitions added. Real worker probe and the existing named
binding pass; no rebind or new model trio was needed. R4 remains native completed,
client TERMINAL, result `73c00855b3a0ff3501047a18bfff8680e9672912`, with original
client `be1443c8f03e2e0875d8225979d9aa3a1a0a4ccc` and original execution host
`65662f69db52c27b6c0972ccec8031cfb68a02fc`. This installation only revalidates that
history; it does not relabel old model execution as a new run.

The current private page is **http://127.0.0.1:18038/setup**. Its current protected
helper directory is:

```text
C:\nqt35\client\sdw-ui-recovery-20261104\qualification-final
```

`start_all.ps1` runs `start_git_fixture.ps1` → `start_client.ps1` →
`start_wizard.ps1`, fails fast, then proves readiness/ownership. The actual
idempotent owned-stack path passed. The original Git9418 daemon was retained;
its receipt fallback still references the actual previous continuation journal.
`authorize_wizard_browser.py` proves the connected recipient before reading an
admin key. `stop_wizard.py --stop` is the owned graceful host stop;
`verify_stable_client.py --terminate-owned-idle-client` only allows a terminal
idle client and uses held handles. No stale PID should be copied into a kill
command. Current receipts are `host-launch-pid.txt`, timestamped host launch/stop
records, and `stable-client-launch.json` / `stable-client-owners.json`.

The three `C:\nqt37` Russian shortcuts now target these current helpers. Their
prior bytes and secret-bearing backup remain private. Old remote installation
branches and historical accepted/rejected evidence are preserved. No main/PR
merge, Madoev/control-host change, Git daemon stop, WSL/Docker reconfiguration or
shared Ollama stop occurred. Bot/webhook credentials, a real Telegram actor and
physical Telegram phone/tablet acceptance remain external inputs.

The next native node should repair keyboard focus, obtain independent exact-result
reviews, and perform new installed qualification. This findings result itself is
**not** a final release approval. See
[qualification.json](evidence/ui-recovery-qualification/qualification.json) and
[browser evidence](evidence/ui-recovery-qualification/browser-recovery-evidence.json).
