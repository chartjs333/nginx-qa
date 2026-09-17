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

## Project actors JSON import

Actors and their initial task lists can be imported for a project by its
canonical four-digit project phone:

```text
POST /api/v1/projects/{project_phone}/actors/import
```

Use `actors.overwrite: true` to replace the project's existing non-group
actors before importing. Set `actors.include_managed: true` as well only when
group-managed actors should be removed and their active groups archived.
Every imported task is stored with its actor and immediately queued for that
actor's phone. See [`examples/project_actors_import.json`](examples/project_actors_import.json).

All project actors and their pending phone-addressed tasks can be removed with:

```text
DELETE /api/v1/projects/{project_phone}/actors?include_managed=true
```

The UI exposes both operations in the Agents tab.

### Telegram webhook

Configure a Telegram bot webhook to point to:

```text
POST /api/v1/telegram/actors
```

Send the same JSON either as message text or as a `.json` document. The JSON
must include `project_id` (or `project_phone`). Set `TELEGRAM_BOT_TOKEN` so the
service can download documents and send an import summary. Optionally set
`TELEGRAM_WEBHOOK_SECRET` and pass the same value to Telegram's webhook
`secret_token`; when configured, requests without the matching
`X-Telegram-Bot-Api-Secret-Token` header are rejected.

Run one application process per runtime directory. `run_8026.bat` is an
alternative port launcher, not a second concurrent worker for the same local
queue state.

## Notes

The app creates local runtime JSON files as needed. Do not commit live
credentials, queue history, screenshots, or evidence artifacts.
