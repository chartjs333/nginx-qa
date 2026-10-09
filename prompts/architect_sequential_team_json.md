# Architect contract: sequential sprint team JSON

Create one project team JSON file at the start of a sprint and send it to the
fixed Telegram webhook `/api/v1/telegram/agents/sequential`.

The document must contain the registered Git repository and an `agents` object
with `overwrite: true`, `assignment_mode: "sequential"`, and an ordered `items`
array. The first agent is the graph entry role. Every item defines a stable
`id`, unique four-digit `phone`, unique `git_branch`, complete role `profile`,
and a `tasks` array. Keep IDs and phones stable between sprint updates when the
role is unchanged.

Example:

```json
{
  "git_address": "https://github.com/company/project.git",
  "agents": {
    "overwrite": true,
    "include_managed": false,
    "assignment_mode": "sequential",
    "items": [
      {
        "id": "requirements-analyst",
        "name": "Requirements Analyst",
        "phone": "2201",
        "git_branch": "agent/requirements-analyst",
        "profile": "Analyze the sprint request and hand implementation work to the appropriate developer.",
        "parameters": {"sprint": "SPRINT-42"},
        "tasks": [
          {
            "task_id": "SPRINT-42-ENTRY",
            "queue": "worker-all",
            "message": "Read the sprint scope and prepare the first implementation handoff."
          }
        ]
      },
      {
        "id": "backend-developer",
        "name": "Backend Developer",
        "phone": "2202",
        "git_branch": "agent/backend-developer",
        "profile": "Implement backend work received from another graph node and send the result to the next agent.",
        "parameters": {"sprint": "SPRINT-42"},
        "tasks": []
      },
      {
        "id": "tester",
        "name": "Tester",
        "phone": "2203",
        "git_branch": "agent/tester",
        "profile": "Verify the received implementation and route PASS or FAIL to the appropriate next node.",
        "parameters": {"sprint": "SPRINT-42"},
        "tasks": []
      }
    ]
  }
}
```

The array order selects only the first entry role. Later movement is determined
by queue addressing: the current agent sends a result or a new task with its
own `from_phone` and the next role's `to_phone`. The next queue item makes the
single executor assume that target role. This permits loops such as Tester ->
Backend Developer -> Tester without running roles in parallel.

After every accepted assignment result or review decision, the executor must
make a new request to `GET` or `POST /api/v1/agents/whoami`, then send the
project `git_address` to the newly returned `reply_url`. This request cycle is
required for every graph edge: current node -> reviewer 1 -> reviewer 2 -> next
node. The executor must not reuse an earlier `reply_url` or infer the next role
from the graph JSON.

If the sprint requires fixed outcomes and guarded transitions, use the explicit
`execution` + `nodes` form from
`examples/project_sequential_graph_import.json`. It requires exactly two
reviewers. Set `execution.initialize_reviewers` to `true` when their persistent
cards must be delivered before the entry node; otherwise they are activated
only when the first transition is ready for review.

In the explicit graph form, one logical worker may appear in several nodes.
Use a unique `agent.id` and `git_branch` for each node, while repeating that
worker's `name` and four-digit `phone`. The service will allocate distinct
internal role endpoints and retain the repeated name/phone as logical identity
metadata. Keep both reviewers distinct from each other and from graph-node
roles.

Do not include bot tokens, passwords, private Git credentials, or other secrets
in this file. Replacing the team with `overwrite: true` also replaces the
previous sprint's imported agents and removes their pending queue items.
