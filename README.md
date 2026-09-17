# nginx-qa

FastAPI service for coordinating QA and development queues between project roles.

## What is included

- `main.py` - FastAPI application and embedded browser UI.
- `requirements.txt` - Python dependencies.
- `run.bat` - Windows launcher for port `8025`.
- `run_8026.bat` - Windows launcher for port `8026`.
- `prompts/` - sanitized role prompt templates.
- `group_templates.json` - declarative agent specs, group templates, queue links, cross-group topologies, and customer reporting rules.
- [Project Manager 0001 agent contract](prompts/project_management_client_0001.md) - resolves or creates a project by Git address and returns its assigned project phone.
- [Declarative Groups and Development Cycles API agent contract](prompts/group_management_api.md) - creates idempotent project groups, routes tasks through declared connections, and exposes the cycle audit trail and lineage graph.
- helper scripts for posting and polling queue messages.

Runtime files such as queue history, agent state, email routes, screenshots,
evidence files, logs, and the local virtual environment are intentionally ignored
by Git.

## Run

On Windows:

```bat
run.bat
```

Or manually:

```bat
python -m venv .venv
.venv\Scripts\activate.bat
python -m pip install -r requirements.txt
python main.py
```

The default app URL is:

```text
http://localhost:8025
```

## Project agents JSON import

Agents and their initial task lists can be imported for a project by its
canonical four-digit project phone:

```text
POST /api/v1/projects/{project_phone}/agents/import
```

Use `agents.overwrite: true` to replace the project's existing non-group
agents before importing. Set `agents.include_managed: true` as well only when
group-managed agents should be removed and their active groups archived.
Every imported task is stored with its agent and immediately queued for that
agent's phone. See [`examples/project_agents_import.json`](examples/project_agents_import.json).

Each item may set `git_branch`, for example `agent/backend-developer`. When it
is omitted, the server creates a stable `agent/<agent-id>` branch name. The
branch is stored both as `agent.git_branch` and in `agent.parameters.git_branch`.

At import time the server appends a generated communication section to every
imported profile. It contains the agent's own phone and branch, all three
receive/send endpoints, a ready JSON message body, and the names, phones, IDs,
and branches of the other imported project agents. Re-importing replaces this
generated section instead of duplicating it. An agent can retrieve its current
complete card with:

```text
GET /api/v1/projects/{project_phone}/agents/{agent_phone}
```

### Dynamic identity and presence

At startup an agent can ask who it is and mark itself alive:

```text
POST /api/v1/projects/{project_phone}/agents/{agent_phone}/whoami
Content-Type: application/json

{"message":"Кто я?"}
```

The response contains the current agent card and profile, assigned Git branch,
all stored tasks, the project agent directory, and the complete matching work
history since the agent was created. It also includes counts by direction and
event type. The heartbeat stores `first_seen_at`, `last_seen_at`, `alive_until`,
and `heartbeat_count` while keeping the operational agent status unchanged.

Agents should repeat the heartbeat every five minutes. Presence remains
`alive` for 15 minutes after the most recent heartbeat and is then reported as
`offline` until the agent checks in again. The generated communication section
in every imported profile includes this endpoint and instruction.

All project agents and their pending phone-addressed tasks can be removed with:

```text
DELETE /api/v1/projects/{project_phone}/agents?include_managed=true
```

The previous `/actors` routes and `actors` JSON key remain accepted for
backward compatibility. The UI exposes both operations in the Agents tab.

### Telegram webhook

Configure a Telegram bot webhook to point to:

```text
POST /api/v1/telegram/agents
```

Send the same JSON either as message text or as a `.json` document. The JSON
must include `project_id` (or `project_phone`). Copy `.env.example` to `.env`
and configure the local file; `.env` is intentionally ignored by Git.

- `TELEGRAM_BOT_TOKEN` downloads documents and sends import confirmations.
- `TELEGRAM_WEBHOOK_SECRET` protects the webhook request header.
- `TELEGRAM_WEBHOOK_URL` is the public HTTPS URL ending in
  `/api/v1/telegram/agents`.
- `TELEGRAM_WEBHOOK_AUTO_REGISTER=1` makes `run.bat` call `setWebhook` before
  starting the application.
- `TELEGRAM_DROP_PENDING_UPDATES=1` discards old pending messages during
  webhook registration; leave it disabled unless that is intentional.
- `TELEGRAM_ALLOWED_CHAT_IDS` and `TELEGRAM_ALLOWED_USER_IDS` are optional
  comma-separated allowlists. Configure at least one for a bot that can mutate
  project data.

`run.bat` and `run_8026.bat` load `.env` without printing its values. Never put
a real bot token directly in either tracked launcher. If a token has appeared
in chat or Git history, revoke it with BotFather before saving its replacement
in `.env`.

Run one application process per runtime directory. `run_8026.bat` is an
alternative port launcher, not a second concurrent worker for the same local
queue state.

## Notes

The app creates local runtime JSON files as needed. Do not commit live
credentials, queue history, screenshots, or evidence artifacts.
