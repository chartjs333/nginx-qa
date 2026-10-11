import copy
import hashlib
import json
import math
import os
import re
import tempfile
import unittest
from pathlib import Path
from typing import Any

import main


ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = ROOT / "docs" / "PROMPT_ARCHIVE_STORAGE_V1.md"


class StrictJsonSpanParser:
    """Strict JSON parser that also records exact character spans by path."""

    _NUMBER = re.compile(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?")

    def __init__(self, text: str) -> None:
        self.text = text
        self.index = 0
        self.spans: dict[tuple[Any, ...], tuple[int, int]] = {}

    def parse(self) -> Any:
        value = self._parse_value(())
        self._skip_whitespace()
        if self.index != len(self.text):
            raise ValueError(f"trailing JSON data at character {self.index}")
        return value

    def encoded_span(self, path: tuple[Any, ...]) -> bytes:
        start, end = self.spans[path]
        return self.text[start:end].encode("utf-8")

    def _skip_whitespace(self) -> None:
        while self.index < len(self.text) and self.text[self.index] in " \t\r\n":
            self.index += 1

    def _parse_value(self, path: tuple[Any, ...]) -> Any:
        self._skip_whitespace()
        start = self.index
        if self.index >= len(self.text):
            raise ValueError("unexpected end of JSON input")
        token = self.text[self.index]
        if token == "{":
            value = self._parse_object(path)
        elif token == "[":
            value = self._parse_array(path)
        elif token == '"':
            value = self._parse_string()
        elif self.text.startswith("true", self.index):
            self.index += 4
            value = True
        elif self.text.startswith("false", self.index):
            self.index += 5
            value = False
        elif self.text.startswith("null", self.index):
            self.index += 4
            value = None
        else:
            match = self._NUMBER.match(self.text, self.index)
            if match is None:
                raise ValueError(f"invalid or non-finite JSON token at {self.index}")
            lexeme = match.group(0)
            self.index = match.end()
            value = json.loads(lexeme)
            if isinstance(value, float) and not math.isfinite(value):
                raise ValueError("non-finite JSON number")
        self.spans[path] = (start, self.index)
        return value

    def _parse_object(self, path: tuple[Any, ...]) -> dict[str, Any]:
        self.index += 1
        result: dict[str, Any] = {}
        seen: set[str] = set()
        self._skip_whitespace()
        if self.index < len(self.text) and self.text[self.index] == "}":
            self.index += 1
            return result
        while True:
            self._skip_whitespace()
            if self.index >= len(self.text) or self.text[self.index] != '"':
                raise ValueError(f"JSON object key is not a string at {self.index}")
            key = self._parse_string()
            if key in seen:
                raise ValueError(f"duplicate JSON object member {key!r}")
            seen.add(key)
            self._skip_whitespace()
            if self.index >= len(self.text) or self.text[self.index] != ":":
                raise ValueError(f"missing JSON object colon at {self.index}")
            self.index += 1
            result[key] = self._parse_value((*path, key))
            self._skip_whitespace()
            if self.index >= len(self.text):
                raise ValueError("unterminated JSON object")
            if self.text[self.index] == "}":
                self.index += 1
                return result
            if self.text[self.index] != ",":
                raise ValueError(f"missing JSON object comma at {self.index}")
            self.index += 1

    def _parse_array(self, path: tuple[Any, ...]) -> list[Any]:
        self.index += 1
        result: list[Any] = []
        self._skip_whitespace()
        if self.index < len(self.text) and self.text[self.index] == "]":
            self.index += 1
            return result
        while True:
            result.append(self._parse_value((*path, len(result))))
            self._skip_whitespace()
            if self.index >= len(self.text):
                raise ValueError("unterminated JSON array")
            if self.text[self.index] == "]":
                self.index += 1
                return result
            if self.text[self.index] != ",":
                raise ValueError(f"missing JSON array comma at {self.index}")
            self.index += 1

    def _parse_string(self) -> str:
        start = self.index
        self.index += 1
        escaped = False
        while self.index < len(self.text):
            token = self.text[self.index]
            self.index += 1
            if escaped:
                escaped = False
            elif token == "\\":
                escaped = True
            elif token == '"':
                value = json.loads(self.text[start:self.index])
                if not isinstance(value, str):
                    raise ValueError("JSON object key did not decode as a string")
                return value
        raise ValueError("unterminated JSON string")


def representative_checkpoint() -> dict[str, Any]:
    return {
        "checkpoint_id": "checkpoint-example-1",
        "sequence": 1,
        "recorded_at": "2026-10-11T12:00:00Z",
        "execution_revision": 7,
        "snapshot_sha256": "a" * 64,
        "snapshot": {
            "project_id": "9000",
            "sprint_id": "sprint-example-1",
            "execution": {
                "status": "active",
                "revision": 7,
                "assignments": [
                    {
                        "assignment_id": "assignment-example-1",
                        "node_id": "storage-contract",
                        "status": "awaiting_review",
                    }
                ],
            },
            "scope_requests": [],
        },
    }


def representative_transition(*, settled: bool) -> dict[str, Any]:
    value = {
        "transition_id": "transition-example-1",
        "source_node_id": "storage-contract",
        "source_assignment_id": "assignment-example-1",
        "outcome": "DONE",
        "target_node_id": "writer-core",
        "result": "Contract and tests completed.",
        "feedback": "Evidence is attached.",
        "proposed_at": "2026-10-11T12:01:00Z",
        "reviews": (
            [
                {
                    "reviewer_index": 1,
                    "reviewer_agent_id": "reviewer-one",
                    "decision": "APPROVE",
                },
                {
                    "reviewer_index": 2,
                    "reviewer_agent_id": "reviewer-two",
                    "decision": "APPROVE",
                },
            ]
            if settled
            else []
        ),
        "status": "approved" if settled else "reviewing",
        "review_context": {
            "text": "history\n\n---\n\ndiff --git a/a b/a\n+change",
            "timeline": [{"type": "activity", "activity": {"id": "event-1"}}],
            "patches": [{"type": "patch", "patch": "+change"}],
        },
    }
    if settled:
        value.update(
            {
                "resolved_at": "2026-10-11T12:03:00Z",
                "applied_target_node_id": "writer-core",
            }
        )
    return value


def representative_response(*, settled: bool) -> dict[str, Any]:
    transition = representative_transition(settled=settled)
    assignment_record = {
        "assignment_id": "assignment-example-1",
        "node_id": "storage-contract",
        "status": "transitioned" if settled else "awaiting_review",
    }
    assignment = {
        "mode": "sequential",
        "strategy": "conditional_graph",
        "status": "active",
        "revision": 7,
        "assignments": [copy.deepcopy(assignment_record)],
        "pending_transition": None if settled else copy.deepcopy(transition),
        "last_transition": copy.deepcopy(transition) if settled else None,
        "execution_audit": {"checkpoints": [representative_checkpoint()]},
    }
    activity = {
        "id": "event-1",
        "timestamp": "2026-10-11T12:00:00Z",
        "message": "Принято 🚀",
    }
    timeline = [{"type": "activity", "activity": copy.deepcopy(activity)}]
    patches = [{"type": "patch", "patch": "+change"}]
    project_state = {
        "schema_version": 1,
        "project_id": "9000",
        "execution": copy.deepcopy(assignment),
        "recent_activity": [copy.deepcopy(activity)],
        "history_with_patches": {
            "text": "history\n\n---\n\n+change",
            "record_count": 1,
            "patch_count": 1,
            "patch_error_count": 0,
            "timeline": copy.deepcopy(timeline),
            "patches": copy.deepcopy(patches),
        },
        "activity_with_patches": copy.deepcopy(timeline),
        "code_patches": copy.deepcopy(patches),
        "code_patch_summary": {
            "activity_count": 1,
            "available_patch_count": 1,
        },
    }
    return {
        "answer": "Переход сохранён",
        "project_id": "9000",
        "project": {
            "project_name": "Prompt Archive Contract",
            "git_address": "https://github.com/example/prompt-archive.git",
            "git_context_key": "github.com/example/prompt-archive",
        },
        "agent": {"id": "reviewer-one", "name": "Reviewer One", "phone": "2978"},
        "active_task": {"id": "review-example-1", "message": "Review the result."},
        "work_history": [copy.deepcopy(activity)],
        "assignment": assignment,
        "pending_transition": None if settled else copy.deepcopy(transition),
        "last_transition": copy.deepcopy(transition) if settled else None,
        "transition_applied": copy.deepcopy(transition) if settled else None,
        "completed_assignments": [copy.deepcopy(assignment_record)],
        "project_state": project_state,
        "project_state_url": "http://testserver/api/v1/projects/9000/state.json",
    }


class PromptArchiveResponseShapeContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary_directory.name)

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    def materialize(self, *, settled: bool) -> tuple[dict[str, Any], bytes, StrictJsonSpanParser, dict[str, Any]]:
        compact = main.materialize_sequential_graph_response(
            response=representative_response(settled=settled),
            directory_template=str(self.root / "prompts" / "{repository}"),
            agent_latest_file_template=str(
                self.root / "agents" / "{repository}_{agent_phone}.prompt"
            ),
            response_kind="transition-response",
            request_agent_phone="2971",
        )
        response_path = Path(compact["response_file_path"])
        self.assertTrue(response_path.is_file())
        raw = response_path.read_bytes()
        self.assertTrue(raw.endswith(b"\n"), "archive must retain its final LF")
        parser = StrictJsonSpanParser(raw.decode("utf-8"))
        parsed = parser.parse()
        self.assertIsInstance(parsed, dict)
        rebuilt_text = json.dumps(parsed, ensure_ascii=False, indent=2) + "\n"
        # The existing legacy writer uses Path.write_text/newline=None, so its
        # exact on-disk evidence follows the platform text newline convention.
        rebuilt = rebuilt_text.replace("\n", os.linesep).encode("utf-8")
        self.assertEqual(rebuilt, raw)
        self.assertEqual(len(rebuilt), len(raw))
        self.assertEqual(hashlib.sha256(rebuilt).digest(), hashlib.sha256(raw).digest())
        return compact, raw, parser, parsed

    def assert_common_shape(self, parsed: dict[str, Any], parser: StrictJsonSpanParser) -> None:
        project_state = parsed["project_state"]
        self.assertIsInstance(project_state["recent_activity"], list)
        self.assertIsInstance(project_state["history_with_patches"]["text"], str)
        self.assertIsInstance(project_state["history_with_patches"]["timeline"], list)
        self.assertIsInstance(project_state["history_with_patches"]["patches"], list)
        self.assertEqual(
            project_state["history_with_patches"]["timeline"],
            project_state["activity_with_patches"],
        )
        self.assertEqual(
            project_state["history_with_patches"]["patches"],
            project_state["code_patches"],
        )
        self.assertIsInstance(parsed["work_history"], list)
        self.assertEqual(
            parsed["assignment"]["assignments"],
            project_state["execution"]["assignments"],
        )

        assignment_audit = parsed["assignment"]["execution_audit"]["checkpoints"]
        project_audit = project_state["execution"]["execution_audit"]["checkpoints"]
        self.assertEqual(assignment_audit, project_audit)
        for checkpoint in assignment_audit:
            self.assertNotIn("execution_audit", checkpoint["snapshot"]["execution"])
        self.assertNotEqual(
            parser.encoded_span(("assignment", "execution_audit", "checkpoints")),
            parser.encoded_span(
                ("project_state", "execution", "execution_audit", "checkpoints")
            ),
            "semantic audit aliases at different nesting depths have different exact bytes",
        )

        for nonexistent_top_level_slot in (
            "results",
            "feedback",
            "source_metadata",
            "execution_audit",
        ):
            self.assertNotIn(nonexistent_top_level_slot, parsed)

    def test_pending_response_freezes_real_alias_inventory_and_exact_bytes(self) -> None:
        _, _, parser, parsed = self.materialize(settled=False)
        self.assert_common_shape(parsed, parser)

        pending = parsed["pending_transition"]
        self.assertEqual(pending, parsed["assignment"]["pending_transition"])
        self.assertEqual(pending, parsed["project_state"]["execution"]["pending_transition"])
        self.assertIsInstance(pending["review_context"], dict)
        self.assertIsInstance(pending["review_context"]["text"], str)
        self.assertIsInstance(pending["review_context"]["timeline"], list)
        self.assertIsInstance(pending["review_context"]["patches"], list)
        self.assertIsInstance(pending["result"], str)
        self.assertIsInstance(pending["feedback"], str)
        self.assertIsNone(parsed["last_transition"])
        self.assertIsNone(parsed["transition_applied"])

    def test_settled_response_freezes_real_alias_inventory_and_exact_bytes(self) -> None:
        _, _, parser, parsed = self.materialize(settled=True)
        self.assert_common_shape(parsed, parser)

        self.assertIsNone(parsed["pending_transition"])
        self.assertIsNone(parsed["assignment"]["pending_transition"])
        self.assertIsNone(parsed["project_state"]["execution"]["pending_transition"])
        settled = parsed["last_transition"]
        self.assertEqual(settled, parsed["assignment"]["last_transition"])
        self.assertEqual(settled, parsed["project_state"]["execution"]["last_transition"])
        self.assertEqual(settled, parsed["transition_applied"])
        self.assertEqual(len(settled["reviews"]), 2)
        self.assertIsInstance(settled["review_context"]["text"], str)
        self.assertIsInstance(settled["result"], str)
        self.assertIsInstance(settled["feedback"], str)

    def test_native_writer_preconditions_are_frozen_in_the_contract(self) -> None:
        contract = CONTRACT_PATH.read_text(encoding="utf-8")
        for required_text in (
            "recursively requires every",
            "object key to be a string",
            "coercion is forbidden",
            "allow_nan=False",
            "duplicate-aware parser",
            "NaN`, `Infinity`, and `-Infinity",
        ):
            with self.subTest(required_text=required_text):
                self.assertIn(required_text, contract)


if __name__ == "__main__":
    unittest.main()
