import builtins
import hashlib
import hmac
import os
import socket
import subprocess
import unittest
from unittest import mock

import nginx_qa.prompt_archive_source_metadata as source_metadata_module
from nginx_qa.prompt_archive_source_metadata import (
    PromptArchiveSourceMetadataError,
    freeze_source_metadata_screen,
    project_source_metadata,
    validate_source_metadata_projection,
)
from nginx_qa.prompt_archive_storage import canonical_json_bytes_v1


class PromptArchiveSourceMetadataTests(unittest.TestCase):
    @staticmethod
    def empty_screen():
        return freeze_source_metadata_screen()

    def assert_error(self, code, callback):
        with self.assertRaises(PromptArchiveSourceMetadataError) as raised:
            callback()
        self.assertEqual(raised.exception.code, code)
        self.assertEqual(
            str(raised.exception),
            "Prompt archive source metadata is invalid.",
        )

    def test_safe_projection_is_canonical_closed_and_receipted(self) -> None:
        raw = {
            "title": "release/docs",
            "source_type": "email",
            "conversation_id": "conversation-17",
            "thread_id": "thread-4",
            "message_id": "message-42",
            "sender_label": "Alice",
            "observed_at": "2026-10-20T12:34:56.123+02:00",
            "adapter": "email",
            "command": "start",
        }

        projected = project_source_metadata(raw, screen=self.empty_screen())
        document = projected.document

        self.assertEqual(document["source_type"], "email")
        for field in ("conversation_id", "thread_id", "message_id"):
            self.assertEqual(
                document[field],
                "sha256:" + hashlib.sha256(raw[field].encode("utf-8")).hexdigest(),
            )
        self.assertEqual(document["sender_label"], "Alice")
        self.assertEqual(document["title"], "release/docs")
        self.assertEqual(document["observed_at"], raw["observed_at"])
        self.assertEqual(document["redacted_fields"], [])
        self.assertEqual(document["redaction_count"], 0)
        self.assertEqual(document["discarded_field_count"], 2)
        self.assertNotIn("adapter", document)
        self.assertNotIn("command", document)
        self.assertEqual(
            projected.canonical_bytes,
            canonical_json_bytes_v1(document, maximum=16 * 1024),
        )
        self.assertEqual(
            projected.sha256,
            hashlib.sha256(projected.canonical_bytes).hexdigest(),
        )
        self.assertEqual(
            projected.receipt,
            {
                "schema_version": 1,
                "kind": "prompt_archive_source_metadata_projection_receipt",
                "projection_version": "prompt-archive-source-projection-v1",
                "redaction_version": "prompt-archive-source-redaction-v1",
                "projection_sha256": projected.sha256,
                "projection_byte_size": len(projected.canonical_bytes),
                "source_type": "email",
                "discarded_field_count": 2,
                "projected_fields": [
                    "conversation_id",
                    "message_id",
                    "observed_at",
                    "sender_label",
                    "thread_id",
                    "title",
                ],
            },
        )

        # Returned mappings are copies; callers cannot mutate the frozen
        # canonical bytes or receipt held by the result.
        document["source_type"] = "api"
        receipt = projected.receipt
        receipt["source_type"] = "api"
        self.assertEqual(projected.document["source_type"], "email")
        self.assertEqual(projected.receipt["source_type"], "email")

    def test_fixed_negative_vectors_are_wholly_redacted(self) -> None:
        vectors = (
            "https://alice:hunter2@example.test/x",
            "https://example.test/?api_key=x",
            "eyJabcdefgh.abcdefgh.abcdefgh",
            "-----BEGIN PRIVATE KEY-----",
            "xoxb-abcdefgh",
            r"C:\secret\x",
            r"\\server\share\x",
            "/var/lib/secret",
        )
        for index, vector in enumerate(vectors):
            with self.subTest(vector=vector):
                field = "sender_label" if index % 2 else "title"
                projected = project_source_metadata(
                    {"source_type": "email", field: vector},
                    screen=self.empty_screen(),
                )
                self.assertEqual(projected.document[field], "[REDACTED]")
                self.assertEqual(projected.document["redacted_fields"], [field])
                self.assertNotIn(vector.encode("utf-8"), projected.canonical_bytes)

    def test_frozen_nonmatches_and_ascii_casefolded_rules(self) -> None:
        for value in ("release/docs", "Alice", "tokenizer"):
            with self.subTest(value=value):
                projected = project_source_metadata(
                    {"source_type": "manual", "title": value},
                    screen=self.empty_screen(),
                )
                self.assertEqual(projected.document["title"], value)
                self.assertEqual(projected.document["redacted_fields"], [])

        for value in ("API_KEY=x", "BeArEr abcdefgh", "cOoKiE:value"):
            with self.subTest(value=value):
                projected = project_source_metadata(
                    {"title": value}, screen=self.empty_screen()
                )
                self.assertEqual(projected.document["title"], "[REDACTED]")

    def test_configured_secret_short_long_and_case_sensitive_semantics(self) -> None:
        screen = freeze_source_metadata_screen(
            configured_values=("", "x", "ab", "xyz", "CanaRy42")
        )
        for value in ("x", "ab", "xyz", "prefixCanaRy42suffix"):
            with self.subTest(rejected=value):
                projected = project_source_metadata({"title": value}, screen=screen)
                self.assertEqual(projected.document["title"], "[REDACTED]")
        for value in ("embedded-ab-value", "prefixcanary42suffix"):
            with self.subTest(preserved=value):
                projected = project_source_metadata({"title": value}, screen=screen)
                self.assertEqual(projected.document["title"], value)

        representation = repr(screen)
        for secret in ("x", "ab", "xyz", "CanaRy42"):
            self.assertNotIn(secret, representation)

    def test_registered_token_digest_uses_constant_time_comparison(self) -> None:
        registered_token = "A" * 43
        safe_token = "B" * 43
        screen = freeze_source_metadata_screen(
            registered_token_sha256=(
                hashlib.sha256(registered_token.encode("ascii")).hexdigest(),
            )
        )
        with mock.patch.object(
            source_metadata_module.hmac,
            "compare_digest",
            wraps=hmac.compare_digest,
        ) as compared:
            rejected = project_source_metadata(
                {"title": f"prefix {registered_token} suffix"}, screen=screen
            )
            preserved = project_source_metadata(
                {"title": f"prefix {safe_token} suffix"}, screen=screen
            )

        self.assertGreaterEqual(compared.call_count, 2)
        self.assertEqual(rejected.document["title"], "[REDACTED]")
        self.assertEqual(preserved.document["title"], f"prefix {safe_token} suffix")
        self.assertNotIn(registered_token.encode("ascii"), rejected.canonical_bytes)

    def test_registered_token_digest_cannot_be_bypassed_by_adjacent_token_chars(
        self,
    ) -> None:
        registered_token = "A" * 43
        safe_exact_token = "B" * 43
        screen = freeze_source_metadata_screen(
            registered_token_sha256=(
                hashlib.sha256(registered_token.encode("ascii")).hexdigest(),
            )
        )

        for value in (
            f"x{registered_token}",
            f"{registered_token}y",
            f"x{registered_token}y",
        ):
            with self.subTest(value=value):
                projected = project_source_metadata({"title": value}, screen=screen)
                self.assertEqual(projected.document["title"], "[REDACTED]")
                self.assertNotIn(value.encode("ascii"), projected.canonical_bytes)

        preserved = project_source_metadata(
            {"title": safe_exact_token}, screen=screen
        )
        self.assertEqual(preserved.document["title"], safe_exact_token)

        # The digest registry does not retain plaintext token lengths, so a
        # longer maximal token-character run is intentionally fail-closed.
        ambiguous_long_run = project_source_metadata(
            {"title": "B" * 44}, screen=screen
        )
        self.assertEqual(ambiguous_long_run.document["title"], "[REDACTED]")

    def test_configured_secret_equal_to_observed_at_is_omitted(self) -> None:
        observed_at = "2026-10-20T12:34:56Z"
        screen = freeze_source_metadata_screen(configured_values=(observed_at,))

        projected = project_source_metadata(
            {"source_type": "api", "observed_at": observed_at}, screen=screen
        )

        self.assertNotIn("observed_at", projected.document)
        self.assertNotIn(observed_at.encode("utf-8"), projected.canonical_bytes)
        self.assertNotIn("observed_at", projected.receipt["projected_fields"])

    def test_configured_secret_equal_to_source_type_projects_unknown(self) -> None:
        screen = freeze_source_metadata_screen(configured_values=("email",))

        projected = project_source_metadata(
            {"source_type": "email", "title": "release/docs"}, screen=screen
        )

        self.assertEqual(projected.document["source_type"], "unknown")
        self.assertEqual(projected.receipt["source_type"], "unknown")
        self.assertNotIn(b'"source_type":"email"', projected.canonical_bytes)

    def test_identifiers_are_screened_before_exact_utf8_hashing(self) -> None:
        composed = "caf\N{LATIN SMALL LETTER E WITH ACUTE}"
        decomposed = "cafe\N{COMBINING ACUTE ACCENT}"
        left = project_source_metadata(
            {"conversation_id": composed}, screen=self.empty_screen()
        )
        right = project_source_metadata(
            {"conversation_id": decomposed}, screen=self.empty_screen()
        )
        self.assertNotEqual(
            left.document["conversation_id"], right.document["conversation_id"]
        )

        unsafe = project_source_metadata(
            {"message_id": "https://example.test/?token=x"},
            screen=self.empty_screen(),
        )
        self.assertEqual(unsafe.document["message_id"], "[REDACTED]")
        invalid_utf8 = project_source_metadata(
            {"thread_id": "bad\ud800value"}, screen=self.empty_screen()
        )
        self.assertEqual(invalid_utf8.document["thread_id"], "[REDACTED]")

    def test_invalid_optional_values_are_redacted_but_bad_timestamp_is_omitted(self) -> None:
        projected = project_source_metadata(
            {
                "source_type": ["email"],
                "conversation_id": "",
                "sender_label": None,
                "title": "[REDACTED]",
                "observed_at": "2026-99-99T99:99:99Z",
            },
            screen=self.empty_screen(),
        )
        self.assertEqual(projected.document["source_type"], "unknown")
        self.assertEqual(projected.document["conversation_id"], "[REDACTED]")
        self.assertEqual(projected.document["sender_label"], "[REDACTED]")
        self.assertEqual(projected.document["title"], "[REDACTED]")
        self.assertEqual(
            projected.document["redacted_fields"],
            ["conversation_id", "sender_label", "title"],
        )
        self.assertNotIn("observed_at", projected.document)

    def test_discarded_field_bound_is_exact(self) -> None:
        at_limit = {f"extension_{index}": index for index in range(1024)}
        projected = project_source_metadata(at_limit, screen=self.empty_screen())
        self.assertEqual(projected.document["discarded_field_count"], 1024)

        over_limit = dict(at_limit)
        over_limit["one_more"] = True
        self.assert_error(
            "PROMPT_ARCHIVE_LIMIT_EXCEEDED",
            lambda: project_source_metadata(over_limit, screen=self.empty_screen()),
        )

    def test_projection_and_receipt_validation_is_biconditional(self) -> None:
        projected = project_source_metadata(
            {
                "source_type": "telegram",
                "message_id": "Bearer token-value",
                "sender_label": "Alice",
            },
            screen=self.empty_screen(),
        )
        rebuilt = validate_source_metadata_projection(
            projected.document, projected.receipt
        )
        self.assertEqual(rebuilt.canonical_bytes, projected.canonical_bytes)
        self.assertEqual(rebuilt.receipt, projected.receipt)

        wrong_receipt = projected.receipt
        wrong_receipt["discarded_field_count"] = 1
        self.assert_error(
            "PROMPT_ARCHIVE_INTEGRITY_MISMATCH",
            lambda: validate_source_metadata_projection(
                projected.document, wrong_receipt
            ),
        )

        wrong_redaction = projected.document
        wrong_redaction["redacted_fields"] = []
        wrong_redaction["redaction_count"] = 0
        self.assert_error(
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
            lambda: validate_source_metadata_projection(wrong_redaction),
        )

        extra = projected.document
        extra["adapter"] = "telegram"
        self.assert_error(
            "PROMPT_ARCHIVE_REFERENCE_INVALID",
            lambda: validate_source_metadata_projection(extra),
        )

    def test_snapshot_inputs_and_error_messages_never_echo_secrets(self) -> None:
        self.assert_error(
            "PROMPT_ARCHIVE_REQUEST_INVALID",
            lambda: freeze_source_metadata_screen(configured_values="secret"),
        )
        self.assert_error(
            "PROMPT_ARCHIVE_REQUEST_INVALID",
            lambda: freeze_source_metadata_screen(
                registered_token_sha256=("A" * 64,)
            ),
        )
        canary = "CanaRy42"
        screen = freeze_source_metadata_screen(configured_values=(canary,))
        projected = project_source_metadata(
            {"title": f"prefix-{canary}-suffix"}, screen=screen
        )
        serialized_receipt = canonical_json_bytes_v1(
            projected.receipt, maximum=16 * 1024
        )
        self.assertNotIn(canary.encode("utf-8"), projected.canonical_bytes)
        self.assertNotIn(canary.encode("utf-8"), serialized_receipt)
        self.assertNotIn(canary, repr(screen))
        self.assertNotIn(canary, repr(projected))

    def test_projector_has_no_io_environment_network_or_process_reads(self) -> None:
        screen = freeze_source_metadata_screen(
            configured_values=("canary-value",),
            registered_token_sha256=(hashlib.sha256(b"A" * 43).hexdigest(),),
        )
        with (
            mock.patch.object(
                builtins, "open", side_effect=AssertionError("filesystem read")
            ),
            mock.patch.object(
                os, "getenv", side_effect=AssertionError("environment read")
            ),
            mock.patch.object(
                socket, "socket", side_effect=AssertionError("network access")
            ),
            mock.patch.object(
                subprocess, "run", side_effect=AssertionError("process access")
            ),
        ):
            projected = project_source_metadata(
                {
                    "source_type": "api",
                    "conversation_id": "conversation-1",
                    "title": "release/docs",
                    "observed_at": "2026-10-20T12:34:56Z",
                },
                screen=screen,
            )
            validate_source_metadata_projection(
                projected.document, projected.receipt
            )
        self.assertEqual(projected.document["source_type"], "api")


if __name__ == "__main__":
    unittest.main()
