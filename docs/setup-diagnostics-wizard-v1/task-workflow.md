# From a request and workbook to a reviewed change

Open `/tasks` on the configured nginx-qa host. The task page uses the same local
operator pairing as `/setup`. It is intended for an already connected project;
an arbitrary repository URL does not grant an executor or credentials.

1. Confirm the browser session using the installation's **Вход в мастер.cmd**.
2. Select the project, paste the request and attach its original `.xlsx` if needed.
3. Click **Проверить и подготовить**. Check the repository, source commit, attachment
   and proposed effects. This creates a local immutable draft, not a pending
   sprint, an assignment or a running model.
4. Click **Запустить работу**. The existing Managed Sprint Engine reserves the
   project's active slot, creates its managed instance and assigns implementation.
   The original attachment and manifest are published on separate project branches.
5. Follow the assignments and two independent reviews. Reloading the page preserves
   the task; history belongs to the paired operator. A running status is not a test
   result. The page distinguishes native state, executor health, author evidence
   and review decisions.
6. Open the result comparison and read the recorded validation. Merge, production
   data changes, deployment and sending a reply are separate actions.

## Correct a result and start a new attempt

1. Open the previous task in history and click **Изменить и запустить заново**.
   This opens an editable copy; it does not run or reopen the old sprint. If the
   form already contains edits, explicitly confirm keeping that text, source and
   selected file. Refreshing status or opening history never overwrites the form.
2. Correct the request. Choose **Сохранить прежний Excel**, **Прикрепить новый
   Excel**, or **Без вложения**. Keeping uses the original immutable blob and
   verifies its hash; there is no need to upload it again. Detaching excludes the
   attachment from the new task; it does not delete files already in Git history
   or in the selected source tree.
3. Choose a remote branch (use **Обновить список веток**) or a full commit SHA.
   The editing copy initially suggests the previous implementation result, or
   its original base if there is no result. Only branches of this registered
   origin and commits reachable from those branches are accepted. Tags, local
   assignments, short SHAs, arbitrary revision expressions and other repos are
   excluded.
4. Click **Проверить и подготовить**. Inspect the full pinned SHA, revised text
   and attachment. A new task ID and linked attempt are saved; the previous task,
   native sprint, reviews and results remain unchanged. Editing after preview
   disables Start until a new plan is prepared.
5. Click **Запустить работу** for the new plan. If the selected branch moved,
   prepare another plan. A selected immutable SHA stays pinned even if its branch
   advances, but must still be reachable from this origin at the first Start.
   A project with an active sprint refuses a second Start; editing and preview
   remain available. **Предыдущая попытка** opens the earlier results.

**Продолжить после исправления** instead retries the same interrupted transport
with the same assignment and task. It does not accept revised requirements.
**Новое задание** explicitly clears the form and exits the linked editing mode.

The setup page diagnoses installation and connectivity. The task page actually
submits work; users do not have to author a sprint JSON or operate each graph node.

## Example: LITELRRK22

The supplied workbook is already edited. The instruction “added lines 120, 8;
edited fields E10, E24” describes that input, not four additional workbook edits.
The request must say to run the existing **FormsGenerator**, inspect its generated
main and regional HTML/Java forms, preserve unmanaged sections, and separately
update the recruitment tracker variant label. The NMC variant can differ from the
Genetic PD variant; a global text replacement is inappropriate.

Use the existing generation service in an isolated workspace/scratch copy. Do not
call a production Excel Manager endpoint: its publication path can write directly
to the application's project root. Generation tests and compilation do not prove
that a live deployment or database migration has occurred.

## Failure and recovery

If the Start response is lost, refresh the same task. Retrying Start reuses the
native operation and assignment; it does not create another sprint. If the model
transport later fails, the page displays the sanitized executor error instead of
silently treating native `Running` as proof of progress. After repairing the cause,
**Продолжить после исправления** repeats the transport handshake for that exact
operation. It cannot revive a stopped task, change its policy or bypass review.

**Остановить работу** cancels this owned native operation and quiesces its client.
Completed commits and evidence remain available. Missing requirements route to
the native read-only continuity node; an autonomous coordinator cannot invent an
operator decision or resume work with guessed instructions.

## Operator configuration and scope

The protected `task-projects.json` under the instance state root maps registered
project IDs to allowed operators, exact repository/default source, role phones, executor
and exact writable paths. The matching `project_templates` in the native client
are the authority ceiling. See [Java/Maven client](java-maven-client.md) for the
worker profile and private Git credential reference.

The first coding profile is Java 8/Maven with a provisioned offline dependency
cache. New projects or dependencies require operator setup. The worker cannot
install dependencies, contact application services, deploy, merge main or send
messages. Source secrets and Git history are hidden from the model; review source
diffs are produced by the trusted parent. Attachment contents never grant commands
or permissions. Only one active native sprint is admitted per project.

Source selection uses the existing template's repository authority, not a new
admission capability. The protected client still accepts only its exact URL,
workspace, branch prefix, role constraints and writable-path ceiling. Browser
requests cannot pass a manifest, credential reference, source publication intent
or writable paths. The trusted ingress validates fetched remote reachability,
pins the selected commit and generates the unchanged native manifest contract.
Without an attachment, Start publishes a create-only `agent/task-source/task-*`
topic pointing at that exact commit (the native importer requires a ref). With an
attachment it publishes the existing immutable input topic. Preview publishes
neither. Start rechecks the complete immutable plan digest and current policy
under its task lock before reserving the existing native project slot; lost ACK
retries retain the same reserved operation rather than switching sources.

Read-only `GET /api/v1/tasks/{task_id}/revision` returns editing defaults.
`GET /api/v1/tasks/projects/{project_id}/sources` fetches the registered origin
and lists up to 2,000 remote heads. Both require the normal operator session and
project authorization; revision also requires task ownership. Preview additionally
accepts `parent_task_id`, `source: {kind: "branch"|"commit", value: ...}` and
`attachment_action: "keep"|"replace"|"none"`. Branch values use `refs/heads/...`.
Only preview creates the linked local draft, and only explicit Start can publish,
admit or execute it.

Limits: 2 MiB input workbook, 3 MiB HTTP request, bounded ZIP/XML extraction,
16,000-character request, exact source-path allowlist, and two independent
reviewers. Test assertions in a model report remain reported evidence until
independently checked. A GitHub comparison is available from result commits;
creating a PR and publishing a test deployment are separate delivery operations.
