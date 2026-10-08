import json
import re
import shutil
import subprocess
import unittest

import main


class PendingSprintsUiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.html = main.render_index_v2()
        match = re.search(r"<script>(.*)</script>", cls.html, flags=re.DOTALL)
        if match is None:
            raise AssertionError("render_index_v2 has no inline script")
        cls.javascript = match.group(1)

    def test_tab_view_and_controls_have_unique_stable_markers(self) -> None:
        self.assertEqual(
            self.html.count('class="page-tab" data-view="pending-sprints"'),
            1,
        )
        self.assertEqual(
            self.html.count(
                'class="view pending-sprints-view" data-view="pending-sprints"'
            ),
            1,
        )
        for element_id in (
            "pendingSprintsProjectSelect",
            "pendingSprintsProjectSummary",
            "refreshPendingSprintsButton",
            "pendingSprintsStatus",
            "pendingSprints",
            "pendingSprintPreviewTitle",
            "pendingSprintProposalDetails",
            "pendingSprintPayloadLabel",
            "pendingSprintJsonPreview",
            "pendingSprintProposalActionFields",
            "pendingSprintOperatorComment",
            "pendingSprintValidateButton",
            "pendingSprintPlayButton",
            "pendingSprintCommentButton",
            "pendingSprintRegenerateButton",
            "pendingSprintRejectButton",
        ):
            self.assertEqual(self.html.count(f'id="{element_id}"'), 1)
        self.assertIn('data-help-topic="page:pending-sprints"', self.html)

    def test_ui_uses_project_scoped_pending_sprint_contract(self) -> None:
        for marker in (
            "function refreshPendingSprints()",
            "function loadPendingSprintPreview(sprintId",
            "function startPendingSprint(sprintId)",
            "/api/v1/projects/${encodeURIComponent(projectPhone)}/pending-sprints`",
            "/pending-sprints/${encodeURIComponent(selectedId)}`",
            "/pending-sprints/${encodeURIComponent(selectedId)}/start`",
            "function submitPendingSprintAction(action)",
            'suffix: "preview"',
            'suffix: "comments"',
            'suffix: "reject"',
            'suffix: "regenerate-request"',
            'data-action="preview-pending-sprint"',
            'data-action="start-pending-sprint"',
            "data.pending_sprints",
            "detail.import_payload",
            'pendingSprintsFetchOptions({method: "POST"})',
            'headers: {"Content-Type": "application/json; charset=utf-8"}',
            "window.confirm(",
        ):
            self.assertIn(marker, self.html)

    @unittest.skipUnless(shutil.which("node"), "Node.js is required for JS behavior check")
    def test_proposal_action_contract_matrix(self) -> None:
        request_source = re.search(
            r"function pendingSprintActionRequest\(.*?\n    \}\n\n"
            r"    async function submitPendingSprintAction",
            self.javascript,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(request_source)
        function_source = request_source.group(0).rsplit(
            "\n\n    async function submitPendingSprintAction",
            1,
        )[0]
        script = "\n".join(
            (
                "const pendingSprintActionAttempts = new Map();",
                function_source,
                "const proposal = {revision: 7};",
                "const preview = pendingSprintActionRequest('pending-1', proposal, 'preview');",
                "const comment = pendingSprintActionRequest('pending-1', proposal, 'comment', 'note');",
                "const reject = pendingSprintActionRequest('pending-1', proposal, 'reject', 'reason');",
                "const regenerate = pendingSprintActionRequest('pending-1', proposal, 'request_regeneration', 'instruction');",
                "const replay = pendingSprintActionRequest('pending-1', proposal, 'comment', 'note');",
                "const changed = pendingSprintActionRequest('pending-1', proposal, 'comment', 'changed');",
                "process.stdout.write(JSON.stringify({preview, comment, reject, regenerate, replay, changed}));",
            )
        )
        result = subprocess.run(
            [shutil.which("node"), "-e", script],
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        actions = json.loads(result.stdout)
        expected = {
            "preview": ("preview", "preview", None),
            "comment": ("comments", "comment", "note"),
            "reject": ("reject", "reject", "reason"),
            "regenerate": (
                "regenerate-request",
                "request_regeneration",
                "instruction",
            ),
        }
        for key, (suffix, action, comment) in expected.items():
            request = actions[key]
            self.assertEqual(request["suffix"], suffix)
            self.assertEqual(request["payload"]["schema_version"], 1)
            self.assertEqual(request["payload"]["action"], action)
            self.assertEqual(request["payload"]["expected_revision"], 7)
            self.assertTrue(request["payload"]["idempotency_key"])
            self.assertLessEqual(len(request["payload"]["idempotency_key"]), 200)
            if comment is None:
                self.assertNotIn("comment", request["payload"])
            else:
                self.assertEqual(request["payload"]["comment"], comment)
        self.assertEqual(
            actions["comment"]["payload"]["idempotency_key"],
            actions["replay"]["payload"]["idempotency_key"],
        )
        self.assertNotEqual(
            actions["comment"]["payload"]["idempotency_key"],
            actions["changed"]["payload"]["idempotency_key"],
        )
        submit_source = re.search(
            r"async function submitPendingSprintAction\(.*?\n    \}\n\n"
            r"    function pendingSprintStatusLabel",
            self.javascript,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(submit_source)
        for marker in (
            'method: "POST"',
            'headers: {"Content-Type": "application/json; charset=utf-8"}',
            "body: JSON.stringify(request.payload)",
            "pendingSprintsFetchOptions({",
        ):
            self.assertIn(marker, submit_source.group(0))

    @unittest.skipUnless(shutil.which("node"), "Node.js is required for JS behavior check")
    def test_proposal_presentation_and_action_state_matrix(self) -> None:
        presentation_source = re.search(
            r"function pendingSprintProposalPresentation\(.*?\n    \}\n\n"
            r"    function renderPendingSprintProposalDetails",
            self.javascript,
            flags=re.DOTALL,
        )
        start_source = re.search(
            r"function pendingSprintStartPresentation\((.*?)\n    \}",
            self.javascript,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(presentation_source)
        self.assertIsNotNone(start_source)
        pure_presentation = presentation_source.group(0).rsplit(
            "\n\n    function renderPendingSprintProposalDetails",
            1,
        )[0]
        script = "\n".join(
            (
                pure_presentation,
                start_source.group(0),
                "function proposal(status, activation, validation, startable) {",
                "  return {id: 'pending-1', status: activation === 'starting' ? 'activating' : 'pending', startable, last_activation_error: {http_status: 409, detail: {error: 'SPRINT_PREFLIGHT_FAILED', phase: 'VALIDATE', issues: [{code: 'REF_INVALID', path: 'ref', message: '<preflight>'}]}}, proposal: {proposal_status: status, activation_state: activation, revision: 4, source_metadata: {source_type: 'email', title: '<source>'}, summary: '<summary>', candidate: {kind: 'managed_git', request: {repository_id: 'main', ref: 'refs/heads/inbound', manifest_path: 'sprints/inbound.json', idempotency_key: 'activate:v1'}}, validation: {status: validation, checked_at: '2026-10-08T08:30:00Z', issues: [{code: 'ISSUE', path: 'candidate.ref', message: '<invalid>'}]}, comments: [{text: '<comment>', actor: {actor_type: 'operator', actor_id: 'local-operator'}}]}};",
                "}",
                "function enabled(view) { return ['canPreview','canComment','canReject','canRegenerate','canPlay'].filter((key) => view[key]); }",
                "const managed = pendingSprintProposalPresentation(proposal('ready','not_started','valid',true));",
                "const states = {",
                "  created: enabled(pendingSprintProposalPresentation(proposal('created','not_started','not_run',false))),",
                "  ready: enabled(managed),",
                "  starting: enabled(pendingSprintProposalPresentation(proposal('ready','starting','valid',false))),",
                "  recovering: enabled(pendingSprintProposalPresentation(proposal('ready','starting','valid',true))),",
                "  started: enabled(pendingSprintProposalPresentation(proposal('started','started','valid',false))),",
                "  rejected: enabled(pendingSprintProposalPresentation(proposal('rejected','not_started','valid',false))),",
                "  failedInvalid: enabled(pendingSprintProposalPresentation(proposal('failed','not_started','invalid',false))),",
                "  failedActivation: enabled(pendingSprintProposalPresentation(proposal('failed','failed','valid',false)))",
                "};",
                "const legacy = pendingSprintProposalPresentation({id:'legacy', source:'telegram', startable:true}, {import_payload:{actors:[]}});",
                "const starts = {ready: pendingSprintStartPresentation(proposal('ready','not_started','valid',true), false), blocked: pendingSprintStartPresentation(proposal('created','not_started','not_run',false), false), recovering: pendingSprintStartPresentation(proposal('ready','starting','valid',true), false), mutation: pendingSprintStartPresentation(proposal('ready','not_started','valid',true), false, true)};",
                "process.stdout.write(JSON.stringify({managed, states, legacy, starts}));",
            )
        )
        result = subprocess.run(
            [shutil.which("node"), "-e", script],
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        rendered = json.loads(result.stdout)
        self.assertEqual(rendered["managed"]["sourceType"], "email")
        self.assertEqual(rendered["managed"]["summary"], "<summary>")
        self.assertEqual(rendered["managed"]["proposalStatus"], "ready")
        self.assertEqual(rendered["managed"]["validationStatus"], "valid")
        self.assertEqual(
            rendered["managed"]["previewPayload"]["manifest_path"],
            "sprints/inbound.json",
        )
        self.assertEqual(
            rendered["managed"]["activationError"]["detail"]["error"],
            "SPRINT_PREFLIGHT_FAILED",
        )
        self.assertEqual(
            rendered["states"]["created"],
            ["canPreview", "canComment", "canReject", "canRegenerate"],
        )
        self.assertEqual(
            rendered["states"]["ready"],
            [
                "canPreview",
                "canComment",
                "canReject",
                "canRegenerate",
                "canPlay",
            ],
        )
        self.assertEqual(rendered["states"]["starting"], ["canComment"])
        self.assertEqual(
            rendered["states"]["recovering"],
            ["canComment", "canPlay"],
        )
        self.assertEqual(rendered["states"]["started"], ["canComment"])
        self.assertEqual(
            rendered["states"]["rejected"],
            ["canComment", "canRegenerate"],
        )
        self.assertEqual(
            rendered["states"]["failedInvalid"],
            ["canPreview", "canComment", "canReject", "canRegenerate"],
        )
        self.assertEqual(
            rendered["states"]["failedActivation"],
            ["canComment", "canReject", "canRegenerate"],
        )
        self.assertFalse(rendered["starts"]["ready"]["disabled"])
        self.assertTrue(rendered["starts"]["blocked"]["disabled"])
        self.assertEqual(rendered["starts"]["recovering"]["label"], "Повторить запуск")
        self.assertTrue(rendered["starts"]["mutation"]["disabled"])
        self.assertFalse(rendered["legacy"]["hasProposal"])
        self.assertTrue(rendered["legacy"]["canPlay"])
        self.assertEqual(rendered["legacy"]["previewPayload"], {"actors": []})
        render_source = re.search(
            r"function renderPendingSprintProposalDetails\(.*?\n    \}\n\n"
            r"    function clearPendingSprintPreview",
            self.javascript,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(render_source)
        for marker in (
            "escapeHtml(String(value))",
            "escapeHtml(message)",
            "escapeHtml(String(comment.text || \"\"))",
            "view.managedRequest.repository_id",
            "view.managedRequest.ref",
            "view.managedRequest.manifest_path",
            "Последний Play preflight / activation error",
            "activationDetail.error",
        ):
            self.assertIn(marker, render_source.group(0))

    def test_deep_link_uses_exact_parameters_and_is_applied_after_git_config(self) -> None:
        for marker in (
            'initialPageParams.get("view")',
            'initialPageParams.get("project_phone")',
            'initialPageParams.get("sprint_id")',
            'initialPageParams.get("pending_token")',
            'pendingSprintsDeepLink.view !== "pending-sprints"',
            'url.searchParams.set("view", "pending-sprints")',
            'url.searchParams.set("project_phone", projectPhone)',
            'url.searchParams.set("sprint_id", pendingSprintsSelectedId)',
            'url.searchParams.set("pending_token", pendingSprintsPublicToken)',
            'setActiveView("pending-sprints", {refreshView: false, syncUrl: false})',
        ):
            self.assertIn(marker, self.javascript)
        refresh_source = re.search(
            r"async function refresh\(\) \{(.*?)\n    \}",
            self.javascript,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(refresh_source)
        source = refresh_source.group(1)
        self.assertLess(
            source.index("await refreshGitConfig();"),
            source.index("applyPendingSprintsDeepLink();"),
        )
        self.assertLess(
            source.index("applyPendingSprintsDeepLink();"),
            source.index("await refreshAgents();"),
        )

    def test_refresh_rejects_stale_project_responses(self) -> None:
        refresh_source = re.search(
            r"async function refreshPendingSprints\(\) \{(.*?)\n    \}\n\n"
            r"    async function startPendingSprint",
            self.javascript,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(refresh_source)
        source = refresh_source.group(1)
        for marker in (
            "const requestVersion = ++pendingSprintsRequestVersion;",
            "requestVersion !== pendingSprintsRequestVersion",
            "projectPhone !== pendingSprintsActiveProjectPhone()",
        ):
            self.assertIn(marker, source)

        preview_source = re.search(
            r"async function loadPendingSprintPreview\(.*?\) \{(.*?)\n    \}\n\n"
            r"    async function refreshPendingSprints",
            self.javascript,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(preview_source)
        for marker in (
            "requestVersion !== pendingSprintPreviewRequestVersion",
            "projectPhone !== pendingSprintsActiveProjectPhone()",
            "selectedId !== pendingSprintsSelectedId",
        ):
            self.assertIn(marker, preview_source.group(1))

    def test_operator_draft_is_scoped_and_navigation_is_locked_during_actions(self) -> None:
        preview_source = re.search(
            r"async function loadPendingSprintPreview\(.*?\) \{(.*?)\n    \}\n\n"
            r"    async function refreshPendingSprints",
            self.javascript,
            flags=re.DOTALL,
        )
        render_source = re.search(
            r"function renderPendingSprints\(\) \{(.*?)\n    \}\n\n"
            r"    function syncPendingSprintsUrl",
            self.javascript,
            flags=re.DOTALL,
        )
        action_source = re.search(
            r"async function submitPendingSprintAction\(.*?\n    \}\n\n"
            r"    function pendingSprintStatusLabel",
            self.javascript,
            flags=re.DOTALL,
        )
        detail_source = re.search(
            r"function renderPendingSprintProposalDetails\(.*?\n    \}\n\n"
            r"    function clearPendingSprintPreview",
            self.javascript,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(preview_source)
        self.assertIsNotNone(render_source)
        self.assertIsNotNone(action_source)
        self.assertIsNotNone(detail_source)

        preview = preview_source.group(1)
        self.assertIn(
            "pendingSprintStartInFlightId || pendingSprintActionInFlightId",
            preview,
        )
        self.assertIn("selectedId !== pendingSprintsSelectedId", preview)
        self.assertLess(
            preview.index('pendingSprintOperatorCommentEl.value = "";'),
            preview.index("pendingSprintsSelectedId = selectedId;"),
        )

        rendered_cards = render_source.group(1)
        self.assertIn("const operationInFlight = Boolean(", rendered_cards)
        self.assertIn('operationInFlight ? " disabled" : ""', rendered_cards)
        self.assertIn("operationInFlight && !isStartingLocally", rendered_cards)
        self.assertIn(
            "pendingSprintOperatorCommentEl.disabled = !view.canComment || actionBusy;",
            detail_source.group(0),
        )

        action = action_source.group(0)
        self.assertIn("projectPhone === pendingSprintsActiveProjectPhone()", action)
        self.assertIn("selectedId === pendingSprintsSelectedId", action)

    def test_public_token_is_sent_and_scoped_to_its_deep_link_project(self) -> None:
        for marker in (
            "const pendingSprintsPublicToken =",
            'headers.set("X-Pending-Sprints-Token", pendingSprintsPublicToken)',
            'url.searchParams.delete("pending_token")',
            "pendingSprintsPublicToken && projectPhone !== tokenProjectPhone",
            "pendingSprintsFetchOptions()",
            'pendingSprintsFetchOptions({method: "POST"})',
        ):
            self.assertIn(marker, self.javascript)
        self.assertEqual(
            self.javascript.count("pendingSprintsFetchOptions()"),
            2,
            "list and detail GET must both include the token-aware options",
        )
        self.assertIn(
            "if (pendingSprintsPublicToken && projectPhone !== tokenProjectPhone)",
            self.javascript,
        )

    def test_pending_sprint_respects_backend_startable_flag(self) -> None:
        render_source = re.search(
            r"function renderPendingSprints\(\) \{(.*?)\n    \}\n\n"
            r"    function syncPendingSprintsUrl",
            self.javascript,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(render_source)
        for marker in (
            "pendingSprintStartPresentation(",
            "isStartingLocally",
            "operationInFlight",
            "startPresentation.disabled",
            "startPresentation.label",
        ):
            self.assertIn(marker, render_source.group(1))

        start_source = re.search(
            r"async function startPendingSprint\(.*?\) \{(.*?)\n    \}\n\n"
            r"    function applyPendingSprintsDeepLink",
            self.javascript,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(start_source)
        for marker in (
            "if (sprint.startable !== true)",
            "Предыдущая попытка запуска не завершилась",
            'isRetry ? "Повторяю запуск" : "Запускаю"',
        ):
            self.assertIn(marker, start_source.group(1))

    @unittest.skipUnless(shutil.which("node"), "Node.js is required for JS behavior check")
    def test_created_proposal_is_rendered_non_startable(self) -> None:
        status_source = re.search(
            r"function pendingSprintStatusLabel\(sprint\) \{(.*?)\n    \}",
            self.javascript,
            flags=re.DOTALL,
        )
        presentation_source = re.search(
            r"function pendingSprintStartPresentation\((.*?)\n    \}",
            self.javascript,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(status_source)
        self.assertIsNotNone(presentation_source)
        script = "\n".join(
            (
                status_source.group(0),
                presentation_source.group(0),
                "const sprint = {status: 'pending', startable: false, "
                "proposal: {proposal_status: 'created', activation_state: 'not_started'}};",
                "process.stdout.write(JSON.stringify({"
                "status: pendingSprintStatusLabel(sprint), "
                "start: pendingSprintStartPresentation(sprint, false)}));",
            )
        )
        result = subprocess.run(
            [shutil.which("node"), "-e", script],
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        rendered = json.loads(result.stdout)
        self.assertEqual(rendered["status"], "ожидает проверки")
        self.assertEqual(
            rendered["start"],
            {"disabled": True, "label": "Запуск недоступен"},
        )

    def test_only_play_uses_activation_endpoint(self) -> None:
        action_source = re.search(
            r"async function submitPendingSprintAction\(.*?\n    \}\n\n"
            r"    function pendingSprintStatusLabel",
            self.javascript,
            flags=re.DOTALL,
        )
        start_source = re.search(
            r"async function startPendingSprint\(.*?\) \{(.*?)\n    \}\n\n"
            r"    function applyPendingSprintsDeepLink",
            self.javascript,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(action_source)
        self.assertIsNotNone(start_source)
        nonactivation = action_source.group(0)
        activation = start_source.group(1)
        for forbidden in (
            "/start`",
            "startPendingSprint(",
            "refreshAgents()",
            "refreshQueues()",
            "refreshScheduledTasks()",
            "refreshHistory()",
            "refreshProjectSprints()",
        ):
            self.assertNotIn(forbidden, nonactivation)
        for required in (
            "/start`",
            'pendingSprintsFetchOptions({method: "POST"})',
            "refreshAgents()",
            "refreshQueues()",
            "refreshScheduledTasks()",
            "refreshHistory()",
            "refreshProjectSprints()",
        ):
            self.assertIn(required, activation)
        self.assertNotIn("body: JSON.stringify", activation)

    def test_successful_start_refreshes_all_affected_views(self) -> None:
        start_source = re.search(
            r"async function startPendingSprint\(.*?\) \{(.*?)\n    \}\n\n"
            r"    function applyPendingSprintsDeepLink",
            self.javascript,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(start_source)
        source = start_source.group(1)
        for marker in (
            "await refreshAgents();",
            "refreshPendingSprints()",
            "refreshQueues()",
            "refreshScheduledTasks()",
            "refreshHistory()",
            "refreshProjectSprints()",
        ):
            self.assertIn(marker, source)

    def test_proxy_html_timeout_is_reconciled_without_raw_json_error(self) -> None:
        for marker in (
            "async function pendingSprintsResponseJson(response, fallback)",
            'raw.trimStart().startsWith("<")',
            "HTML-страницу прокси вместо JSON",
        ):
            self.assertIn(marker, self.javascript)
        start_source = re.search(
            r"async function startPendingSprint\(.*?\) \{(.*?)\n    \}\n\n"
            r"    function applyPendingSprintsDeepLink",
            self.javascript,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(start_source)
        source = start_source.group(1)
        for marker in (
            "catch (responseError)",
            "await refreshPendingSprints();",
            "промежуточный ответ прокси был потерян",
            "повторно загружать файл не нужно",
        ):
            self.assertIn(marker, source)
        lost_response_branch = re.search(
            r"if \(!refreshed\) \{(.*?)\n\s*return;",
            source,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(lost_response_branch)
        for marker in (
            "await refreshAgents();",
            "refreshQueues()",
            "refreshScheduledTasks()",
            "refreshHistory()",
            "refreshProjectSprints()",
        ):
            self.assertIn(marker, lost_response_branch.group(1))

    @unittest.skipUnless(shutil.which("node"), "Node.js is required for JS syntax check")
    def test_rendered_javascript_passes_node_syntax_check(self) -> None:
        result = subprocess.run(
            [shutil.which("node"), "--check", "-"],
            input=self.javascript,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
