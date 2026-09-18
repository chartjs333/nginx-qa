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
- [Sequential sprint team JSON architect contract](prompts/architect_sequential_team_json.md) - defines the once-per-sprint team file and queue-driven identity transitions.
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
Every imported task is stored with its agent. In parallel mode all tasks are
immediately queued to their agents' phones; in sequential mode the entry node
is queued first and later nodes are reached through graph handoffs. See
[`examples/project_agents_import.json`](examples/project_agents_import.json).

Every successful import also creates a persistent project sprint record. When
the next JSON is imported, the previous record is archived together with its
latest execution graph, agents, pending queues, and up to 10,000 project
history events. The original import payload is retained as well. The first
import after upgrading preserves any already existing project state as a
legacy archive before creating the new current sprint. List and download these
records with:

```text
GET /api/v1/projects/{project_phone}/sprints
GET /api/v1/projects/{project_phone}/sprints/{sprint_id}/download
```

The Agents tab shows the same project-specific history and provides a
`Скачать JSON` button for every current or archived sprint. A JSON project
reference is validated against the project selected in the import URL, so a
file for another project is rejected instead of being imported into the
active project.

Downloaded archives also contain `code_history`. It follows the UI's
`Скопировать сообщения + патчи` semantics: messages stay in chronological
order, a Git patch is inserted before the first message with a changed commit,
and the same commit transition is never repeated. Unavailable patches are kept
in the timeline with their error reason instead of failing the sprint import.

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

For a strictly non-parallel project, set this inside the `agents` object:

```json
"assignment_mode": "sequential"
```

This is a queue-driven graph, not a permanent assignment of several executors.
The first item in `agents.items` is only the entry node. The single executor
starts discovery at the common URL:

```text
GET or POST /api/v1/agents/whoami
```

The response asks for the Git repository and supplies an absolute `reply_url`.
Send the answer to that URL:

```json
{"git_address":"https://github.com/owner/repository.git"}
```

The service takes the oldest project item from `worker-all`, `tester-all`, or
`consultant-all`. The item's `to_agent_id` or `to_phone` selects the current
agent. The response contains `agent`, `profile`, `git_branch`, `active_task`,
`team`, `communication`, and `graph_position`. The executor therefore becomes
the addressed agent only for this graph node. A successful response also sets
`execution_authorized: true` and `requires_additional_confirmation: false`:
show the requested identity summary and start immediately without pausing for
another approval.

The same response includes the complete `project_state`. Its
`activity_with_patches` array follows the UI's `Скопировать сообщения + патчи`
ordering: a `patch` entry is inserted immediately before the first `activity`
entry whose commit differs from the previous known commit, and the same commit
transition is emitted only once. `code_patches` contains the patch entries on
their own and `code_patch_summary` reports available and unavailable patch
counts. `history_with_patches.text` contains the same copy-ready text as the UI.
Patch lookup errors are returned as `status: "unavailable"` and do not prevent
the agent from receiving its task. The state endpoint accepts `history_limit`
when a larger history window is needed.

The repository reply is idempotent while that node remains active. Repeating
the request before a handoff returns the same identity and `active_task` with
`identity_reused: true`; it does not consume another queue item. Deployments
from older versions can recover the active task from project history and set
`active_task_recovered_from_history: true`.

After finishing the node, send the result or the next task to a team member
through an endpoint from `communication.send_endpoints`, then post the Git
address to the same `reply_url` again. The next queue item can select any role,
including a previous one, so cycles and conditional graph paths are supported.
JSON order does not control later transitions.

An architect can also declare the complete graph with top-level `execution`
and `nodes`; see
[`examples/project_sequential_graph_import.json`](examples/project_sequential_graph_import.json).
Every declared transition is checked by the two reviewers from
`execution.reviewers`. With `execution.initialize_reviewers: true`, the common
identity queue first returns both persistent reviewer cards and then the graph
entry node. Omit the flag (or set it to `false`) to start directly from the
entry node and activate reviewers only when a transition needs approval.

### Sequential graph with two transition reviewers

For a declared conditional graph use `execution` and `nodes` instead of
`agents.items`. See
[`examples/project_sequential_graph_import.json`](examples/project_sequential_graph_import.json).
`agents.overwrite: true` still controls replacement of the project's existing
agents.

`execution.reviewers` must contain exactly two different reviewers. Both are
created during import with normal agent cards, project Git context, the complete
team directory, and a profile containing the full graph. The live, restart-safe
project snapshot is also available to them at:

```text
GET /api/v1/projects/{project_phone}/state.json
```

When a graph-node agent submits an outcome, the requested transition does not
happen immediately. The common identity queue first returns reviewer 1 and then
reviewer 2. Both must independently send `APPROVE`. A single `REJECT` cancels
the proposed transition and returns the source node for rework with the review
feedback. This gate also applies to transitions into terminal nodes.

Submit a node result to the `whoami_endpoint` from the active queue item:

```json
{
  "assignment_id": "value-from-active-task-metadata",
  "status": "DONE",
  "result": "Implementation and verification evidence",
  "from_commit": "commit checked out when work started",
  "git_commit": "commit containing the completed work"
}
```

The allowed status values are the keys from that node's `transitions`, for
example `DONE`, `PASS`, or `FAIL`. When the two commit hashes are supplied, the
service inserts their Git diff into the stored transition context and into the
task sent to both reviewers. For a locally available repository the final HEAD
can be detected automatically, but supplying both hashes is recommended for a
remote repository. Each reviewer uses the same endpoint shape:

```json
{
  "assignment_id": "value-from-active-task-metadata",
  "status": "APPROVE",
  "feedback": "Transition checked"
}
```

The other decision is `REJECT`; it requires non-empty `feedback` so the source
agent receives an actionable rework instruction. A confirmed backward/`FAIL`
transition increments `rework_cycle_count`. When `max_rework_cycles` is
exceeded, the run ends with status `blocked` instead of looping forever.

All project agents and their pending phone-addressed tasks can be removed with:

```text
DELETE /api/v1/projects/{project_phone}/agents?include_managed=true
```

The previous `/actors` routes and `actors` JSON key remain accepted for
backward compatibility. The UI exposes both operations in the Agents tab.

### Telegram webhook

There are two fixed Telegram import URLs. The URL determines the execution
model; an `assignment_mode` value inside the JSON cannot switch it.

Sequential graph traversal with one executor that changes identity at each
node:

```text
POST /api/v1/telegram/agents/sequential
```

The first graph node is queued immediately. Use the common
`/api/v1/agents/whoami` flow above to receive it together with the resolved
identity and team JSON. The legacy shared queue URL
`/worker/all/{project_phone}?to_phone={project_phone}` remains available for a
low-level client, but it does not enrich arbitrary handoffs with the full agent
card.

Parallel execution with permanent roles and no identity switching:

```text
POST /api/v1/telegram/agents/parallel
```

All agents' tasks are queued immediately to their own phones. The legacy
`/api/v1/telegram/agents` and `/api/v1/telegram/actors` endpoints remain
parallel aliases for compatibility.

Send the same JSON either as message text or as a `.json` document. The JSON
should include `git_address`; the service finds the already registered project
and its internal project phone automatically. If several registered project
contexts use the same repository, also include the exact `git_context_key`
returned by Project Manager 0001. The legacy `project_id`/`project_phone`
fields remain accepted when no Git reference is supplied. Unknown repositories
are rejected and are not created by the Telegram import.

The programmer can therefore send a file shaped like
[`examples/project_agents_import.json`](examples/project_agents_import.json)
without knowing the project's internal phone. Copy `.env.example` to `.env`
and configure the local file; `.env` is intentionally ignored by Git.

- `TELEGRAM_BOT_TOKEN` downloads documents and sends import confirmations.
- `TELEGRAM_WEBHOOK_SECRET` protects the webhook request header.
- `TELEGRAM_WEBHOOK_URL` is one of the two public HTTPS URLs above. A Telegram
  bot can have only one active webhook, so choose one mode per bot; use a second
  bot when both modes must be active simultaneously.
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
