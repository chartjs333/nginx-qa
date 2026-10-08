import copy
import json
import unittest
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import ValidationError
from referencing import Registry, Resource

from nginx_qa.managed_import import parse_strict_json_object
from nginx_qa.sprint_types import canonical_json_bytes


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_ROOT = ROOT / "schemas"
CONTRACT_PATH = ROOT / "docs" / "INBOUND_PROPOSAL_API_V1.md"
SPEC_PATH = ROOT / "docs" / "INBOUND_COORDINATOR_V1_SPEC.md"
CONTRACT_SCHEMAS = {
    "inbound-api-error-v1.schema.json",
    "inbound-pending-proposal-action-v1.schema.json",
    "inbound-pending-proposal-create-v1.schema.json",
    "inbound-pending-proposal-response-v1.schema.json",
    "inbound-pending-proposal-v1.schema.json",
    "inbound-producer-registry-v1.schema.json",
}
REFERENCE_SCHEMAS = {"start-sprint-from-git-v1.schema.json"}


def managed_create_fixture() -> dict:
    return {
        "schema_version": 1,
        "proposal_id": "hub-thread-2026-10-08-17",
        "idempotency_key": "create:hub-thread-2026-10-08-17:v1",
        "source": {
            "source_type": "email",
            "conversation_id": "conversation-17",
            "thread_id": "thread-17",
            "message_id": "message-42",
            "sender_label": "Release coordinator",
            "title": "Inbound coordinator sprint",
            "observed_at": "2026-10-08T08:30:00Z",
        },
        "summary": "Review and start the pinned managed sprint after validation.",
        "candidate": {
            "kind": "managed_git",
            "request": {
                "repository_id": "main",
                "ref": "refs/heads/inbound-sprint",
                "manifest_path": "orchestration/sprints/inbound/sequential-sprint.json",
                "idempotency_key": "activate:hub-thread-2026-10-08-17:v1",
            },
        },
    }


def proposal_fixture() -> dict:
    create = managed_create_fixture()
    return {
        "schema_version": 1,
        "proposal_id": create["proposal_id"],
        "pending_sprint_id": "pending-0123456789abcdef0123456789abcdef",
        "project_id": "9000",
        "revision": 0,
        "proposal_status": "created",
        "activation_state": "not_started",
        "source_metadata": copy.deepcopy(create["source"]),
        "summary": create["summary"],
        "candidate": copy.deepcopy(create["candidate"]),
        "validation": {"status": "not_run", "checked_at": None, "issues": []},
        "comments": [],
        "regenerate_requested": False,
        "submitted_by": {"producer_id": "inbound-hub"},
        "created_at": "2026-10-08T08:31:00Z",
        "updated_at": "2026-10-08T08:31:00Z",
        "started_sprint_id": None,
    }


class InboundCoordinatorContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        loaded_schemas = {
            name: json.loads((SCHEMA_ROOT / name).read_text(encoding="utf-8"))
            for name in CONTRACT_SCHEMAS | REFERENCE_SCHEMAS
        }
        cls.schemas = {name: loaded_schemas[name] for name in CONTRACT_SCHEMAS}
        cls.all_schemas = loaded_schemas
        registry = Registry()
        for schema in loaded_schemas.values():
            registry = registry.with_resource(
                schema["$id"], Resource.from_contents(schema)
            )
        cls.registry = registry

    def validator(self, name: str) -> Draft202012Validator:
        return Draft202012Validator(
            self.all_schemas[name],
            registry=self.registry,
            format_checker=FormatChecker(),
        )

    def assert_invalid(self, name: str, value: dict) -> None:
        with self.assertRaises(ValidationError):
            self.validator(name).validate(value)

    def test_schemas_meta_validate_and_positive_examples_validate(self) -> None:
        for name, schema in self.schemas.items():
            with self.subTest(schema=name):
                Draft202012Validator.check_schema(schema)

        create = managed_create_fixture()
        proposal = proposal_fixture()
        cases = {
            "inbound-pending-proposal-create-v1.schema.json": create,
            "inbound-pending-proposal-v1.schema.json": proposal,
            "inbound-pending-proposal-response-v1.schema.json": {
                "schema_version": 1,
                "correlation_id": "corr-create-17",
                "deduplicated": False,
                "proposal": proposal,
            },
            "inbound-pending-proposal-action-v1.schema.json": {
                "schema_version": 1,
                "action": "comment",
                "expected_revision": 0,
                "idempotency_key": "comment:proposal-17:v1",
                "comment": "Please verify the candidate manifest.",
            },
            "inbound-api-error-v1.schema.json": {
                "detail": {
                    "error": "PROPOSAL_IDEMPOTENCY_CONFLICT",
                    "message": "The key is already bound to another request.",
                    "correlation_id": "corr-conflict-17",
                    "retryable": False,
                }
            },
            "inbound-producer-registry-v1.schema.json": {
                "schema_version": 1,
                "producers": [
                    {
                        "producer_id": "inbound-hub",
                        "token_sha256": "a" * 64,
                        "project_ids": ["9000"],
                        "actions": ["create", "read", "preview"],
                    }
                ],
            },
        }
        self.assertEqual(set(cases), CONTRACT_SCHEMAS)
        for name, value in cases.items():
            with self.subTest(instance=name):
                self.validator(name).validate(value)

    def test_source_metadata_is_closed_and_credentials_are_not_members(self) -> None:
        for field in (
            "authorization",
            "cookie",
            "credential",
            "password",
            "token",
            "raw_headers",
        ):
            request = managed_create_fixture()
            request["source"][field] = "must-not-cross-the-boundary"
            with self.subTest(field=field):
                self.assert_invalid(
                    "inbound-pending-proposal-create-v1.schema.json", request
                )

        request = managed_create_fixture()
        request["candidate"]["request"]["credential"] = (
            "must-not-be-a-candidate-member"
        )
        self.assert_invalid("inbound-pending-proposal-create-v1.schema.json", request)

        for field_path in (("source", "title"), ("summary",)):
            request = managed_create_fixture()
            target = request
            for component in field_path[:-1]:
                target = target[component]
            target[field_path[-1]] = "unsafe\ncontrol"
            with self.subTest(field_path=field_path):
                self.assert_invalid(
                    "inbound-pending-proposal-create-v1.schema.json", request
                )

    def test_candidate_union_preserves_legacy_and_requires_managed_git(self) -> None:
        managed = managed_create_fixture()
        self.validator("start-sprint-from-git-v1.schema.json").validate(
            managed["candidate"]["request"]
        )

        request = managed_create_fixture()
        request["candidate"] = {
            "kind": "legacy_json",
            "payload": {"actors": {}, "assignment_mode": "parallel"},
        }
        self.validator("inbound-pending-proposal-create-v1.schema.json").validate(
            request
        )

        explicit_legacy = copy.deepcopy(request)
        explicit_legacy["candidate"]["payload"]["sprint_type"] = "legacy_v1"
        self.validator("inbound-pending-proposal-create-v1.schema.json").validate(
            explicit_legacy
        )

        managed_direct = copy.deepcopy(request)
        managed_direct["candidate"]["payload"]["sprint_type"] = (
            "managed_workspace_v1"
        )
        self.assert_invalid(
            "inbound-pending-proposal-create-v1.schema.json", managed_direct
        )

        malformed_path = managed_create_fixture()
        malformed_path["candidate"]["request"]["manifest_path"] = "../sprint.json"
        self.assert_invalid(
            "inbound-pending-proposal-create-v1.schema.json", malformed_path
        )

    def test_strict_json_and_canonical_fingerprint_inputs_are_deterministic(self) -> None:
        left = {"summary": "Привет", "source": {"b": 2, "a": 1}}
        right = {"source": {"a": 1, "b": 2}, "summary": "Привет"}
        self.assertEqual(canonical_json_bytes(left), canonical_json_bytes(right))
        self.assertNotIn(b"\\u", canonical_json_bytes(left))

        for raw in (
            b'{"proposal_id":"one","proposal_id":"two"}',
            b'{"value":NaN}',
            b'{"value":Infinity}',
            b'{"value":"\xff"}',
        ):
            with self.subTest(raw=raw):
                with self.assertRaises(ValueError):
                    parse_strict_json_object(raw)

    def test_resource_state_conditions_are_strict(self) -> None:
        ready = proposal_fixture()
        ready["proposal_status"] = "ready"
        self.assert_invalid("inbound-pending-proposal-v1.schema.json", ready)
        ready["validation"] = {
            "status": "valid",
            "checked_at": "2026-10-08T08:32:00Z",
            "issues": [],
        }
        self.validator("inbound-pending-proposal-v1.schema.json").validate(ready)

        missing_checked_at = copy.deepcopy(ready)
        missing_checked_at["validation"].pop("checked_at")
        self.assert_invalid(
            "inbound-pending-proposal-v1.schema.json", missing_checked_at
        )
        ready["activation_state"] = "starting"
        self.validator("inbound-pending-proposal-v1.schema.json").validate(ready)

        started = copy.deepcopy(ready)
        started["proposal_status"] = "started"
        started["activation_state"] = "started"
        started.pop("started_sprint_id")
        self.assert_invalid("inbound-pending-proposal-v1.schema.json", started)
        started["started_sprint_id"] = "msv1-" + ("a" * 64)
        self.validator("inbound-pending-proposal-v1.schema.json").validate(started)

        rejected = proposal_fixture()
        rejected["proposal_status"] = "rejected"
        rejected["activation_state"] = "starting"
        self.assert_invalid("inbound-pending-proposal-v1.schema.json", rejected)

        invalid = proposal_fixture()
        invalid["validation"] = {
            "status": "invalid",
            "checked_at": "2026-10-08T08:32:00Z",
            "issues": [
                {"code": "MANIFEST_INVALID", "message": "Manifest is invalid."}
            ],
        }
        self.assert_invalid("inbound-pending-proposal-v1.schema.json", invalid)
        invalid["proposal_status"] = "failed"
        self.validator("inbound-pending-proposal-v1.schema.json").validate(invalid)

        rejected_invalid = copy.deepcopy(invalid)
        rejected_invalid["proposal_status"] = "rejected"
        self.validator("inbound-pending-proposal-v1.schema.json").validate(
            rejected_invalid
        )

        activation_failed = copy.deepcopy(ready)
        activation_failed["proposal_status"] = "failed"
        activation_failed["activation_state"] = "failed"
        self.validator("inbound-pending-proposal-v1.schema.json").validate(
            activation_failed
        )

        impossible_start_id = proposal_fixture()
        impossible_start_id["started_sprint_id"] = "sprint-impossible"
        self.assert_invalid(
            "inbound-pending-proposal-v1.schema.json", impossible_start_id
        )

        impossible_activation = proposal_fixture()
        impossible_activation["activation_state"] = "started"
        self.assert_invalid(
            "inbound-pending-proposal-v1.schema.json", impossible_activation
        )

    def test_non_activation_actions_are_closed_and_revision_bound(self) -> None:
        action = {
            "schema_version": 1,
            "action": "reject",
            "expected_revision": 4,
            "idempotency_key": "reject:proposal-17:v1",
            "comment": "The source needs a corrected manifest.",
        }
        validator = self.validator("inbound-pending-proposal-action-v1.schema.json")
        validator.validate(action)

        preview = {
            "schema_version": 1,
            "action": "preview",
            "expected_revision": 4,
            "idempotency_key": "preview:proposal-17:v1",
        }
        validator.validate(preview)
        preview["comment"] = "Preview must not carry a comment."
        self.assert_invalid(
            "inbound-pending-proposal-action-v1.schema.json", preview
        )

        missing_revision = copy.deepcopy(action)
        missing_revision.pop("expected_revision")
        self.assert_invalid(
            "inbound-pending-proposal-action-v1.schema.json", missing_revision
        )
        start_action = copy.deepcopy(action)
        start_action["action"] = "start"
        self.assert_invalid(
            "inbound-pending-proposal-action-v1.schema.json", start_action
        )

        unsafe_comment = copy.deepcopy(action)
        unsafe_comment["comment"] = "unsafe\ncontrol"
        self.assert_invalid(
            "inbound-pending-proposal-action-v1.schema.json", unsafe_comment
        )

    def test_producer_registry_cannot_contain_plaintext_or_start_scope(self) -> None:
        registry = {
            "schema_version": 1,
            "producers": [
                {
                    "producer_id": "inbound-hub",
                    "token_sha256": "a" * 64,
                    "project_ids": ["9000"],
                    "actions": ["create", "read"],
                }
            ],
        }
        validator = self.validator("inbound-producer-registry-v1.schema.json")
        validator.validate(registry)

        plaintext = copy.deepcopy(registry)
        plaintext["producers"][0]["token"] = "must-not-be-stored"
        self.assert_invalid("inbound-producer-registry-v1.schema.json", plaintext)

        start_scope = copy.deepcopy(registry)
        start_scope["producers"][0]["actions"].append("start")
        self.assert_invalid("inbound-producer-registry-v1.schema.json", start_scope)

    def test_errors_are_normalized_and_do_not_accept_ad_hoc_payloads(self) -> None:
        error = {
            "detail": {
                "error": "PROPOSAL_REQUEST_INVALID",
                "correlation_id": "corr-invalid-17",
                "retryable": False,
            }
        }
        validator = self.validator("inbound-api-error-v1.schema.json")
        validator.validate(error)

        lowercase = copy.deepcopy(error)
        lowercase["detail"]["error"] = "proposal_request_invalid"
        self.assert_invalid("inbound-api-error-v1.schema.json", lowercase)
        unknown = copy.deepcopy(error)
        unknown["detail"]["error"] = "PROPOSAL_UNKNOWN_ERROR"
        self.assert_invalid("inbound-api-error-v1.schema.json", unknown)
        wrong_retryability = copy.deepcopy(error)
        wrong_retryability["detail"]["retryable"] = True
        self.assert_invalid(
            "inbound-api-error-v1.schema.json", wrong_retryability
        )
        unavailable = copy.deepcopy(error)
        unavailable["detail"]["error"] = "PROPOSAL_SERVICE_UNAVAILABLE"
        unavailable["detail"]["retryable"] = True
        self.validator("inbound-api-error-v1.schema.json").validate(unavailable)
        echoed_value = copy.deepcopy(error)
        echoed_value["detail"]["invalid_value"] = "raw-secret-like-value"
        self.assert_invalid("inbound-api-error-v1.schema.json", echoed_value)

        response = {
            "schema_version": 1,
            "correlation_id": "corr-response-17",
            "proposal": proposal_fixture(),
        }
        self.assert_invalid(
            "inbound-pending-proposal-response-v1.schema.json", response
        )

    def test_normative_document_freezes_routes_and_safety_boundaries(self) -> None:
        contract = CONTRACT_PATH.read_text(encoding="utf-8")
        spec = SPEC_PATH.read_text(encoding="utf-8")
        for marker in (
            "POST` | `/api/v1/projects/{project_id}/pending-sprints`",
            "/pending-sprints/{pending_sprint_id}/reject",
            "/pending-sprints/{pending_sprint_id}/comments",
            "/pending-sprints/{pending_sprint_id}/regenerate-request",
            "`POST .../start` remains the only activation boundary",
            "(canonical_project_id, authenticated_producer_id, idempotency_key)",
            "PROPOSAL_IDEMPOTENCY_CONFLICT",
            "X-Correlation-ID",
            "Producer credentials do not authorize `/start`",
            "Bearer ownership is mandatory",
            "retains its existing bodyless request",
            "A confirmed managed `FAILED` start record is different",
            "Coordinator `RETRY_IMPORT`",
            "fresh transport envelope",
            "proposal: null",
            "detail record remains readable after",
            "NQII-001 intentionally changes no backend route",
        ):
            with self.subTest(marker=marker):
                self.assertIn(marker, contract)
        self.assertIn("INBOUND_PROPOSAL_API_V1.md", spec)


if __name__ == "__main__":
    unittest.main()
