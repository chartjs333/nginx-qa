# Automated qualification

## Final matrix rework

The initial packaging commit `59db178` changed the acceptance matrix after the
historical 890-test run. Both independent reviewers reproduced an existing test
that still required every case to be NOT_RUN. Security submitted an actual
REJECT; the engine issued QA rework cycle 1. That failure is retained in
`evidence/qa-initial-security-review.json`, its engine receipt, and the independent
`qa-initial-runtime-review.json`. The 890-test result was never relabelled as a
run against the changed matrix.

The contract now validates all sixteen IDs, the single-project rule, allowed
lifecycle values, actual unique passing test IDs and source commit in recorded
evidence, and separate required external proof before an external case can PASS.
The runner now tracks the matrix, prompt contract documents and root/example
fixtures in addition to source. No production behavior changed in this rework.

[Affected checks](evidence/qa-matrix-rework.json): 69 launcher contract, prompt
and sprint-type tests PASS, zero failures/errors/skips. The source commit and
hashes are in that report; [full post-run provenance](evidence/qa-matrix-rework-provenance.json)
was captured before adding the rework evidence. The unchanged broad run below
remains historical production regression coverage, rather than a second full run.
The final packaging commit also requires a post-packaging contract run, whose
receipt is retained outside that frozen commit under
`C:/nqt35/client/stage8-final-package-check.json` and checked by both reviewers.

## Broad isolated run

The isolated qualification ran 890 unique tests at source commit
`5899a69b44724a78b8829d64bfb5e51f37073744`: **PASS**, 0 failures,
0 errors, 0 skips in 3387.657 seconds.
This is the automated stage-8 gate; full external product acceptance is separate.

```powershell
& C:\nqt35\v\Scripts\python.exe -X utf8 -B scripts\qualify_telegram_launcher.py --evidence C:\nqt35\client\stage8-qualification.json
```

Exit code: 0. Run from the assigned QA checkout. The runner is an offline test
entry point, not an agent client or scheduler. Tests own disposable Git remotes,
temporary state, fake Telegram/executor/provider clients and local sockets.
Native Windows supervisor fixtures execute real disposable processes.

## Evidence and provenance

* [Per-test results, versions and source hashes](evidence/qa-qualification.json)
* [Full unchanged Git HEAD/status after the run](evidence/qa-post-run-provenance.json)
* [Installed qualification dependency versions](evidence/qualified-dependencies.txt)
* [Sixteen acceptance cases mapped to actual test IDs](acceptance-matrix.json)
* [Prior gateway runtime review](evidence/gateway-runtime-review.json)
* [Prior gateway security review](evidence/gateway-security-review.json) and
  [its accepted engine receipt](evidence/gateway-security-receipt.json)

The starting tree was clean. The runner's source inventory was identical before
and after; a separate full Git status/HEAD was captured before adding this report
and the evidence files. The later packaging commit changes documentation/evidence;
the machine-readable source commit remains the code that actually ran.
Reviewer approval of this QA result is a separate subsequent engine transaction.

Coverage includes launcher contract/prompt/intake/preparation/manual-auto controls,
auth/replay/CSRF, dual listeners, tunnel ownership, imports/continuity/recovery,
Git/workspace/branch/port leases, Windows processes, scope/legacy adapters,
inbound permissions and the PR16 cache/refresh/memory regressions. Exact test IDs
are authoritative; overlapping earlier focused runs are not added to this total.

## Additional integration evidence

The qualification fixture uses the actual result/review API to reject a result,
reject an obsolete review, issue a rework assignment, commit a correction and
obtain two fresh distinct reviews of that new commit before completion.

Two active test projects share the same control store. Stopping one preserves the
other's runtime aggregate, queue rows, workspace/branch/head and assignment; the
other then completes through its real result/review API. External clients are
fakes, so this is engine state/workspace isolation, not a claim of concurrent
native child-process isolation. Supervisor tests cover native resource behavior
separately. Three distinct concurrent envelopes for one project yield one winner
and two PROJECT_BUSY results.

The real Uvicorn fixture binds two ephemeral loopback sockets under one lifespan,
serves the public shell, denies private routes/OpenAPI on the public listener and
closes both sockets before the lifespan exits.

## Windows fixture corrections

The inherited one-process-limit test launched the Windows venv shim, which needs
another process before Python starts. It now launches `sys._base_executable` so
the asserted forbidden descendant is the actual fixture action. FAILED state,
lease-release and traceback assertions remain. Production enforcement is unchanged.

On this machine a legacy protected-root example contains a filesystem alias.
Its tests now first assert the production normalizer rejects that alias, then
canonicalize only a test copy to inspect the example's structural settings.
Production guards and the legacy template are unchanged: the raw aliased legacy
example still cannot be used here without an operator choosing canonical paths.
Historical stage-7 failure evidence is preserved, not relabelled as passing.

## Remaining external acceptance

No live Telegram, public Cloudflare, external AI or mobile session was exercised
by this runner. Browser fixture evidence uses real Edge with synthetic Telegram
identity, and is explicitly distinct from a real mobile bot session. External
cases remain BLOCKED/NOT_RUN in the matrix until their own release evidence exists.
No provider credentials, existing project aliases or live sprint state are used
by qualification. The release node must not report DONE from this suite alone.
