# SDWU-004 focused keyboard repair

This coordinator result repairs `E03_FOCUS_RETURN` from the accepted
`NEED_DECISION` findings at `1a5064c6378dc4622b0f3c1d829654615f489690`.
Actual reviewers 7081 and 7082 approved the truthful findings and independently
reproduced the defect; those approvals did not approve E03 or release the product.
Their receipts and the future-only graph revision 2 receipt are preserved in
[`evidence/ui-focus-repair`](evidence/ui-focus-repair/). Historical failed
qualification artifacts and review timing remain unchanged.

## Repair

The UI records the actual Plan or recovery button before asynchronous work
disables it. On Escape, Cancel or the close control, it restores controls before
closing and explicitly returns focus to the enabled, visible, attached opener.
The opener must still belong to the current session/project. If it was removed,
hidden or replaced during refresh/reauthentication, the UI selects an available
recovery, Refresh or session-check control. It never restores background focus
while another dialog is open.

Apply results defer restoration until diagnostics or reauthentication settles;
a superseding session cannot receive the old callback's focus. When the focused
Apply button is disabled while its response is pending, focus moves to the
native dialog itself. This also fixes the related loss of focus observed in the
extended held-response fixture. Escape still cannot discard an in-flight
request. The original Apply identity survives uncertain responses, recovery
Escape/Cancel, reload and reauthentication; no automatic Apply was added.

Native dialog Tab/browser-chrome behavior is unchanged. No custom focus trap,
orchestration layer, backend authorization change or service action is included.
The exact additional wizard-only deployment identity is
`codex/sdw-qualified-ui-focus-recovery-20261104`; prior deployment branch pins,
defaults and exact origin/SHA/clean/owner checks remain intact.

## Evidence

`tests/setup_ui_focus_browser.cjs` runs the shipped HTML/CSS/JS unchanged in real
Edge at 390 × 844, intercepting **every HTTP request** on a fixture-only origin.
It never contacts the installed host. The frozen pre-repair export reproduces
BODY focus with no outline after Escape. The repaired source passes ten groups:

- Escape restores visible keyboard focus to the enabled Plan opener.
- Cancel and close restore the same opener without Apply.
- An advisor proposal returns to its own invoking button.
- Detached/hidden openers fall back to the current Refresh control.
- Another open modal retains focus when the original closes.
- Uncertain Apply, recovery Escape/Cancel and reload retain key and focus.
- Stale, successful, unauthenticated and held Apply outcomes restore usable
  focus; the held response retains visible focus on the native dialog meanwhile.

There were 79 intercepted requests, five explicitly confirmed fixture Apply
requests, zero automatic Apply requests and no page errors. The Escape
screenshot was inspected: the Plan button has a visible native focus outline.
The initial extended held-response probe failed its `inDialog` assertion before
the related pending-Apply focus fix; that observation is retained in the repair
journal rather than represented as an initial pass.

Ten affected Python tests passed: seven wizard tests, two deployment allow/reject
tests and the real temporary Git exact-remote-SHA isolation test. These preserve
the existing 9 request/receipt, 11 Apply-recovery and 16 reordered-session cases.
The prior 192-test installed qualification remains evidence for its original
`fdafbf58db85555706b8ea04e11c1d4450b0f120` source; overlapping checks are not added
to produce a larger distinct total. No model sprint was rerun.

Example isolated browser command (PowerShell, from this checkout):

```powershell
node tests/setup_ui_focus_browser.cjs . C:/nqt35/client/sdw-ui-recovery-20261104/focus-repair/after
```

The runner accepts `SDW_PLAYWRIGHT_MODULE` and `SDW_BROWSER_EXECUTABLE` for other
test installations. Its default paths point to the already available local
Playwright and Edge used for the recorded run.

## Native handoff

Recommended outcome: **RESUME_2**, once the parent has inspected this exact
published result. Native submission and two fresh independent reviews have not
been performed by this author. Qualification-recovery 7005 must then install
the exact approved product on the new immutable deployment branch and perform
the affected real installed browser checks. The current installed product is
still `fdafbf58...`; this fixture result is not an installation or release claim.

Use the parent's `focus-qualification-handoff.json` for current helper ownership
and safe deployment. In particular, reconcile the previously observed report-only
`check_installed_native_history.py` helper wording drift in the **new** package;
do not rewrite historical evidence. Provider, worker, model, Telegram and other
external acceptance limits are unchanged.
