# Protected Telegram Mini App gateway

The launcher uses one public gateway for every permitted project. The existing
Managed Sprint Engine, import database, assignments, reviewers, queues and process
supervisor remain authoritative. The public app never mounts or proxies `main`.
It exposes only the launcher shell/assets, authentication, scoped status,
Start/Stop/Logout and the launcher-only Telegram webhook. Legacy/admin/file,
OpenAPI, health, SSE and WebSocket routes are unavailable publicly.

The v1 constraint is **one active sprint per canonical project**. A gateway or
tunnel is not a way to create another database/project alias to bypass that gate.

## Host and provenance

The opt-in `telegram-managed-sprint-launcher-v1-gateway` profile uses the final
`agent/tmsl-v1-release-documentation` branch at `D:/nq-tmsl-launcher`, external
state `C:/nqt36`, private loopback port 18036, narrow public loopback port 18037
and child pool 18700–18749. Other profiles retain their existing behavior.

Copy `orchestration/sprints/telegram-managed-sprint-launcher-v1/gateway-18036.env.example`
to the ignored `.env.staging` in that checkout. Set the exact approved release
commit, then run from that checkout:

```powershell
.\run_staging.ps1 -Profile telegram-managed-sprint-launcher-v1-gateway -Action Check -ExpectedCommit $releaseCommit
.\run_staging.ps1 -Profile telegram-managed-sprint-launcher-v1-gateway -Action Setup -ExpectedCommit $releaseCommit
.\run_staging.ps1 -Profile telegram-managed-sprint-launcher-v1-gateway -Action Start -ExpectedCommit $releaseCommit
```

`Setup` creates the isolated environment through the existing ownership guard.
`Start` validates the approved clean branch, Git remote, Python executable and
hash, marker initialization identity, distinct roots and ports. The Python host
repeats ownership/provenance checks before importing the legacy app. Both sockets
are reserved before the single engine lifespan starts. Both bind `127.0.0.1`;
forwarded headers never establish public authority.

Public listener failure leaves the engine/private listener running. Private
listener failure is fatal after complete cleanup. Common shutdown closes ingress,
drains requests and actual background/thread work, then exits the engine lifespan.
Cancellation during cleanup requests shutdown and waits for real quiescence.

## Credentials and project setup

Configure the existing project registry, launcher policy and scoped executor
using the complete examples in [installation and operation](README.md), with
contracts in `architecture.md`, `intake.md`, `launch-modes.md` and
`telegram-controls.md`. Creating a host does not create projects, agent clients or
grants. An unavailable existing executor blocks automatic activation recoverably.

Provide trusted environment values through your existing secret provider:

* `NGINX_QA_LAUNCHER_CONTROL_BOT_TOKEN`
* `NGINX_QA_LAUNCHER_CONTROL_WEBHOOK_SECRET`
* `NGINX_QA_LAUNCHER_CONTROL_EXECUTOR_TOKEN`, when the configured executor needs it.

Reference overrides must resolve inside `env:NGINX_QA_LAUNCHER_CONTROL_*`.
The public gateway requires protected bot/webhook values; it does not fall back
to raw legacy variables. Existing private Telegram integrations retain their
legacy fallback. Never place values in manifests, committed configuration, logs,
URLs or Telegram messages.

Child secret aliases, child destination names, Git credential resolution and
legacy subprocess inheritance reject/remove the control namespace (including
legacy Telegram credential variables). Prompt/report secret screening includes
its values. These are credential delegation controls, **not an operating-system
sandbox against arbitrary code running as the same Windows account**.

## One owned Cloudflare tunnel

For a stable operator-managed named tunnel, set
`NGINX_QA_LAUNCHER_PUBLIC_ORIGIN=https://your-owned-gateway.example`. Its ingress
must target only `http://127.0.0.1:18037`, never the private port or a child port.
Configure the bot webhook at `/launcher/telegram/webhook` with its protected
secret and `message,callback_query` updates. No public health or admin endpoint
is necessary.

For an isolated demonstration, reuse `cloudflared_quick_tunnel.ps1` with the new
explicit `-LauncherGateway` mode. It never runs legacy tunnel discovery, installs
software, stops an unrelated process or writes into the source checkout. Install
cloudflared separately and provide its executable path. In a separate shell,
import this owned checkout's `.env.staging` into that process (never `Invoke-Expression`):

```powershell
foreach ($line in Get-Content -LiteralPath .\.env.staging) {
    if (-not $line.Trim() -or $line.Trim().StartsWith('#')) { continue }
    $name, $value = $line.Split('=', 2)
    if ($name -notmatch '^NGINX_QA_[A-Z0-9_]+$') { throw 'Invalid environment setting' }
    [Environment]::SetEnvironmentVariable($name, $value, 'Process')
}
.\cloudflared_quick_tunnel.ps1 -LauncherGateway -Action Start -CloudflaredExecutable 'C:\path\cloudflared.exe'
```

Start runs the owned monitor in the foreground. A background operator launcher
must use `Start-Process -WindowStyle Hidden` and retain its logs/PID for inspection.
The monitor validates ownership/provenance independently. It launches cloudflared
suspended atomically inside the existing supervisor's Windows Job, persists its exact
birth/executable identity and command, then resumes it. The Job kills its children
if the monitor crashes. A native retained child handle is used for cleanup; no
process is discovered or killed by name/port/PID alone. The child receives only a
minimal OS environment, an owned config file and the public loopback upstream.

Add `-RegisterWebhook` only for the bot assigned to this gateway. Registration
uses protected references, preserves pending updates, and may replace only an
empty webhook, the current URL or this monitor's previous owned origin. A foreign
integration is refused. The receipt records `REGISTERED`, `FAILED` or
`NOT_REQUESTED`; a visible URL alone does not prove Telegram registration.
The last acknowledged origin and unacknowledged registration intents survive
failed restarts. Intent is persisted before contacting Telegram, so a lost reply
does not turn an owned webhook into a foreign one on retry. Successful registration
prunes this history; 64 unresolved attempts require operator inspection.

To stop only this tunnel, in a shell with the same owned environment:

```powershell
.\cloudflared_quick_tunnel.ps1 -LauncherGateway -Action Stop
```

Stop verifies the monitor's exact current identity and writes a scoped stop
request. The monitor drains its bounded webhook work and proves its Job empty.
It retains the final receipt. Unknown/reused PIDs, mismatched receipts or
uninspectable prior processes fail closed without killing them or erasing proof.
The engine and existing sprints continue running.

Origin receipt: `C:/nqt36/legacy/runtime_state/launcher-origin.json`. Its phase,
initialization ID, instance, public-port upstream and heartbeat must match the
host. A stale heartbeat (15 seconds), failed/stopped phase or malformed ownership
makes the gateway unavailable. Cloudflared handles connection reconnects while
running. After process failure, inspect the receipt and repeat Start once prior
death is proven. The replacement URL is read dynamically; `/sprint_status` issues
fresh buttons. Auth is bound to the exact origin, so old-origin challenges,
exchanges and sessions cannot be used on the replacement origin. Never combine
an explicit named origin with the Quick Tunnel mode.

Quick Tunnels are temporary test infrastructure, have a 200 concurrent-request
limit and do not support SSE. The Mini App polls scoped status. Use a named tunnel
for persistent service: [Cloudflare Quick Tunnel documentation](https://developers.cloudflare.com/tunnel/get-started/quick-tunnels/).

## Authentication and browser behavior

The three Telegram buttons contain opaque owner/instance-bound handles, never
authentication secrets. GET previews return static markup without cookies or
state. The Mini App verifies Telegram's raw signed `initData` on the server using
the bot-token HMAC contract, including `signature` in the signed field set.
`initDataUnsafe` and query-string identities are not trusted. Only supported
private bot contexts are accepted. Signed data has a five-minute freshness limit
and 30-second future skew; duplicate fields and malformed encodings fail.

A 120-second server challenge binds a random HttpOnly browser cookie to the
owner, project, instance, import attempt, handle and origin. Authenticating
consumes the challenge and canonical Telegram launch digest atomically. It creates
a one-use exchange token, hash-only in the existing import DB (60 seconds by
default, configurable 5–300). Exchange checks the same signed user/browser/scope
again and atomically creates a session. Lost authentication responses require a
fresh Telegram launch. Replay never mints another session.

Session cookies use `__Host-`, `Secure`, `HttpOnly`, `Path=/`, `SameSite=None`
and `Partitioned`. This permits Telegram Web's cross-site iframe while isolating
the cookie by top-level site. APIs still require the configured Host, exact Origin
on writes, valid session, current ACL and CSRF. Browser script cannot read cookies;
raw initData/exchange/CSRF values are kept in memory, never local storage or URLs.
Logout revokes the session and deletes its partitioned cookie. Expiry defaults to
30 minutes (configurable 1–60); quotas limit global and per-user sessions.

The gateway shell is frameable by Telegram Web. Only static shell/assets exempt
cross-site navigation checks; API routes do not. Response policy is no-store,
no-referrer with a restrictive CSP. Cookies use a distinct handle suffix, so tabs
for different instances do not overwrite each other. Reload reuses a valid
session before attempting a new Telegram exchange. Clients blocking required
cookies receive an explicit error; there is no URL bearer-token fallback.

Browser fixture evidence proves the behavior in isolated Edge with third-party
cookies blocked and synthetic signed Telegram data. Real Telegram mobile/tablet,
bot registration and public Cloudflare acceptance remain separate release gates.

## Recovery, validation and rollback

Auth/tunnel failure does not delete sprint history or stop engine assignments.
Preserve the import DB, owned roots, registry and marker during restart. Do not
reset the owner marker or run another host against the same roots. To disable
public access, stop the owned tunnel; existing work can finish through the private
API. A source rollback must use the existing exact-commit ownership process and
compatible state; never silently rewrite an approved marker.

Automated checks: `tests.test_launcher_gateway`, `tests.test_launcher_auth_races`,
`tests.test_launcher_control_secrets`, `tests.test_launcher_host`,
`tests.test_launcher_tunnel`, plus the existing launcher/import/supervisor suites.
Native fixtures use temporary state. Browser-only evidence is explicitly marked
as a fixture; it is not a substitute for the required external end-to-end demo.
