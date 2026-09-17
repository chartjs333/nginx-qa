import json
import re
import shutil
import subprocess
import unittest

import main


class HistoryUiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.html = main.render_index_v2()

    def history_helper_source(self) -> str:
        match = re.search(
            r"(function formatMessage\(message\).*?\n    \})\n\n"
            r"    function queuesForContext",
            self.html,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(match)
        return match.group(1)

    def test_history_renderer_uses_lifecycle_payload_and_revision_helpers(self) -> None:
        for marker in (
            "function formatHistoryRecordMessage(record)",
            "function historyRecordRevision(record)",
            "const message = formatHistoryRecordMessage(record);",
            "const revision = historyRecordRevision(record);",
            "const eventLabel = meta.cycle_event_type || record.event;",
            "${escapeHtml(revision.label)}: ${escapeHtml(revision.value)}",
        ):
            self.assertIn(marker, self.html)

    @unittest.skipUnless(shutil.which("node"), "Node.js is required for the UI behavior test")
    def test_null_lifecycle_messages_render_artifact_and_report_payloads(self) -> None:
        artifact_record = {
            "event": "cycle_lifecycle_event",
            "message": None,
            "metadata": {
                "cycle_event_type": "ARTIFACT_CREATED",
                "cycle_event_payload": {
                    "artifact": {
                        "status": "verified",
                        "changes": ["structured finalUrgency"],
                        "verification": ["npm run build PASS"],
                        "ref": "e1dbf99+dirty",
                        "path": r"D:\new-live\react",
                    }
                },
            },
        }
        report_record = {
            "event": "cycle_lifecycle_event",
            "message": None,
            "metadata": {
                "cycle_event_type": "GROUP_REPORT_SUBMITTED",
                "cycle_event_payload": {
                    "report": {
                        "RESULT": "PASS — implementation verified",
                        "VERIFICATION": ["53/53 PASS"],
                    }
                },
            },
        }
        regular_record = {
            "event": "queued_to_worker_all",
            "message": "STATUS: PASS\nSUMMARY: original text",
            "metadata": {"git_commit_short": "abc123"},
        }
        paths_only_record = {
            "event": "cycle_lifecycle_event",
            "message": None,
            "metadata": {
                "cycle_event_type": "ARTIFACT_CREATED",
                "cycle_event_payload": {
                    "artifact": {"paths": ["main.py", "tests/test_history_ui.py"]}
                },
            },
        }
        decision_record = {
            "event": "cycle_lifecycle_event",
            "message": None,
            "metadata": {
                "cycle_id": "cycle-accepted-1",
                "cycle_event_type": "CYCLE_COMPLETED",
                "cycle_event_payload": {
                    "decision": {"status": "accepted", "summary": "Ready"}
                },
            },
        }
        script = "\n".join(
            (
                self.history_helper_source(),
                f"const artifactRecord = {json.dumps(artifact_record)};",
                f"const reportRecord = {json.dumps(report_record)};",
                f"const regularRecord = {json.dumps(regular_record)};",
                f"const pathsOnlyRecord = {json.dumps(paths_only_record)};",
                f"const decisionRecord = {json.dumps(decision_record)};",
                "console.log(JSON.stringify({",
                "  artifactMessage: formatHistoryRecordMessage(artifactRecord),",
                "  artifactRevision: historyRecordRevision(artifactRecord),",
                "  reportMessage: formatHistoryRecordMessage(reportRecord),",
                "  regularMessage: formatHistoryRecordMessage(regularRecord),",
                "  regularRevision: historyRecordRevision(regularRecord),",
                "  pathsOnlyRevision: historyRecordRevision(pathsOnlyRecord),",
                "  decisionMessage: formatHistoryRecordMessage(decisionRecord),",
                "  decisionRevision: historyRecordRevision(decisionRecord),",
                "  emptyMessage: formatHistoryRecordMessage({event: 'other', message: null, metadata: {}})",
                "}));",
            )
        )
        result = subprocess.run(
            [shutil.which("node"), "-e", script],
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        actual = json.loads(result.stdout)

        artifact_message = actual["artifactMessage"]
        self.assertIn("STATUS: VERIFIED", artifact_message)
        self.assertIn("SUMMARY: D:\\new-live\\react", artifact_message)
        self.assertIn("EVENT: ARTIFACT_CREATED", artifact_message)
        self.assertIn("PAYLOAD:", artifact_message)
        self.assertIn('"artifact"', artifact_message)
        self.assertIn('"changes"', artifact_message)
        self.assertNotEqual(artifact_message, "null")
        self.assertEqual(
            actual["artifactRevision"],
            {"label": "Ref", "value": "e1dbf99+dirty"},
        )

        self.assertIn("STATUS: PASS", actual["reportMessage"])
        self.assertIn("PASS — implementation verified", actual["reportMessage"])
        self.assertEqual(actual["regularMessage"], regular_record["message"])
        self.assertEqual(
            actual["regularRevision"],
            {"label": "Commit", "value": "abc123"},
        )
        self.assertEqual(
            actual["pathsOnlyRevision"],
            {"label": "Artifact", "value": "main.py"},
        )
        self.assertIn("STATUS: ACCEPTED", actual["decisionMessage"])
        self.assertIn('"decision"', actual["decisionMessage"])
        self.assertEqual(
            actual["decisionRevision"],
            {"label": "Cycle", "value": "cycle-accepted-1"},
        )
        self.assertEqual(actual["emptyMessage"], "")


if __name__ == "__main__":
    unittest.main()
