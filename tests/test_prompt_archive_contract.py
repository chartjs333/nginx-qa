import base64
import binascii
import copy
import hashlib
import json
import struct
import unittest
import zlib
from pathlib import Path
from urllib.parse import urldefrag, urljoin

from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import ValidationError
from referencing import Registry, Resource


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_ROOT = ROOT / "schemas"
CONTRACT_PATH = ROOT / "docs" / "PROMPT_ARCHIVE_STORAGE_V1.md"
SCHEMA_NAMES = {
    "prompt-archive-blob-v1.schema.json",
    "prompt-archive-ref-v1.schema.json",
    "prompt-archive-record-v1.schema.json",
    "prompt-archive-index-node-v1.schema.json",
    "prompt-archive-json-edge-v1.schema.json",
    "prompt-archive-json-value-v1.schema.json",
    "prompt-archive-envelope-v1.schema.json",
    "prompt-archive-manifest-v1.schema.json",
    "prompt-archive-pointer-v1.schema.json",
    "prompt-archive-locator-v1.schema.json",
    "prompt-archive-publication-plan-v1.schema.json",
    "prompt-archive-journal-v1.schema.json",
    "prompt-archive-latest-slot-v1.schema.json",
    "prompt-archive-pending-operation-registry-v1.schema.json",
    "prompt-archive-source-inventory-v1.schema.json",
    "prompt-archive-source-metadata-v1.schema.json",
    "prompt-archive-descriptor-stub-v1.schema.json",
    "prompt-archive-descriptor-v1.schema.json",
    "prompt-archive-error-v1.schema.json",
}
SHA_A = "a" * 64
SHA_B = "b" * 64
SHA_C = "c" * 64
ARCHIVE_ID = f"par1-{SHA_A}"
RESPONSE_ID = f"parresp1-{'1' * 64}"


def pas1_frame(value: str | None) -> bytes:
    if value is None:
        return b"\xff"
    raw = value.encode("utf-8")
    return b"\x00" + struct.pack(">Q", len(raw)) + raw


def pas1_id_digest(domain: str, fields: list[tuple[str, str | None]]) -> str:
    preimage = b"nginx-qa-pas1-id-v1\x00" + pas1_frame(domain)
    for name, value in fields:
        preimage += pas1_frame(name) + pas1_frame(value)
    return hashlib.sha256(preimage).hexdigest()


def canonical_control_bytes(value: object) -> bytes:
    """Reference encoder for schema-safe test fixtures (no duplicate keys/floats)."""
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def frozen_gzip_bytes(raw: bytes) -> bytes:
    compressor = zlib.compressobj(
        level=9,
        method=zlib.DEFLATED,
        wbits=-15,
        memLevel=8,
        strategy=zlib.Z_DEFAULT_STRATEGY,
    )
    body = compressor.compress(raw) + compressor.flush(zlib.Z_FINISH)
    return (
        bytes.fromhex("1f8b08000000000002ff")
        + body
        + struct.pack("<II", zlib.crc32(raw) & 0xFFFFFFFF, len(raw) & 0xFFFFFFFF)
    )


def bind_ref_to_bytes(ref: dict, raw: bytes) -> None:
    ref["digest"] = hashlib.sha256(raw).hexdigest()
    ref["uncompressed_size"] = len(raw)
    if len(raw) < 1024:
        stored = raw
        ref["content_encoding"] = "identity"
        ref.pop("encoding_parameters", None)
    else:
        stored = frozen_gzip_bytes(raw)
        ref["content_encoding"] = "gzip"
        ref["encoding_parameters"] = "gzip-rfc1952-zlib-1.2.13-level9-v1"
    ref["stored_size"] = len(stored)
    ref["stored_sha256"] = hashlib.sha256(stored).hexdigest()


def blob_ref(logical_kind: str = "payload_chunk", *, gzip: bool = False) -> dict:
    media_types = {
        "raw_chunk": "application/json",
        "payload_chunk": "application/json",
        "render_plan_chunk": (
            "application/vnd.nginx-qa.prompt-response-envelope-v1+json"
        ),
        "literal_chunk": (
            "application/vnd.nginx-qa.prompt-archive-json-fragment-v1"
        ),
        "json_value": (
            "application/vnd.nginx-qa.prompt-archive-json-value-v1+json"
        ),
        "json_edge": (
            "application/vnd.nginx-qa.prompt-archive-json-edge-v1+json"
        ),
        "event_record": "application/vnd.nginx-qa.prompt-archive-record-v1+json",
        "patch_record": "application/vnd.nginx-qa.prompt-archive-record-v1+json",
        "history_text_record": (
            "application/vnd.nginx-qa.prompt-archive-record-v1+json"
        ),
        "result_record": "application/vnd.nginx-qa.prompt-archive-record-v1+json",
        "feedback_record": "application/vnd.nginx-qa.prompt-archive-record-v1+json",
        "review_context_record": (
            "application/vnd.nginx-qa.prompt-archive-record-v1+json"
        ),
        "execution_audit_record": (
            "application/vnd.nginx-qa.prompt-archive-record-v1+json"
        ),
        "source_metadata_record": (
            "application/vnd.nginx-qa.prompt-archive-record-v1+json"
        ),
        "current_field_record": (
            "application/vnd.nginx-qa.prompt-archive-record-v1+json"
        ),
        "index_node": (
            "application/vnd.nginx-qa.prompt-archive-index-node-v1+json"
        ),
        "manifest": "application/vnd.nginx-qa.prompt-archive-manifest-v1+json",
        "publication_plan": (
            "application/vnd.nginx-qa.prompt-archive-publication-plan-v1+json"
        ),
        "journal": "application/vnd.nginx-qa.prompt-archive-journal-v1+json",
        "pointer": "application/vnd.nginx-qa.prompt-archive-pointer-v1+json",
        "archive_locator": (
            "application/vnd.nginx-qa.prompt-archive-locator-v1+json"
        ),
        "descriptor_stub": (
            "application/vnd.nginx-qa.prompt-archive-descriptor-stub-v1+json"
        ),
        "latest_slot": (
            "application/vnd.nginx-qa.prompt-archive-latest-slot-v1+json"
        ),
        "source_inventory": (
            "application/vnd.nginx-qa.prompt-archive-source-inventory-v1+json"
        ),
        "pending_operation_registry": (
            "application/vnd.nginx-qa.prompt-archive-pending-operation-registry-v1+json"
        ),
    }
    value = {
        "schema_version": 1,
        "kind": "prompt_archive_blob_ref",
        "hash_algorithm": "sha256",
        "digest": SHA_A,
        "logical_kind": logical_kind,
        "media_type": media_types[logical_kind],
        "content_encoding": "gzip" if gzip else "identity",
        "uncompressed_size": 128,
        "stored_size": 64 if gzip else 128,
        "stored_sha256": SHA_B if gzip else SHA_A,
    }
    if gzip:
        raw = bytes(1024)
        stored = frozen_gzip_bytes(raw)
        value["digest"] = hashlib.sha256(raw).hexdigest()
        value["uncompressed_size"] = len(raw)
        value["stored_size"] = len(stored)
        value["stored_sha256"] = hashlib.sha256(stored).hexdigest()
        value["encoding_parameters"] = "gzip-rfc1952-zlib-1.2.13-level9-v1"
    return value


def index_ref() -> dict:
    value = blob_ref("index_node")
    value["media_type"] = (
        "application/vnd.nginx-qa.prompt-archive-index-node-v1+json"
    )
    return value


def scope_fixture() -> dict:
    return {
        "project_id": "9000",
        "sprint_id": "sprint-example-1",
        "response_id": RESPONSE_ID,
        "response_kind": "transition-response",
        "occurrence_kind": "transition",
        "occurrence_id": "transition-example-1",
        "transition_id": "transition-example-1",
    }


def integrity_fixture(semantics: str = "raw-byte-equality") -> dict:
    return {
        "semantics": semantics,
        "hydrated_media_type": "application/json",
        "hydrated_size": 4096,
        "hydrated_sha256": SHA_C,
        "canonical": {
            "algorithm": "nginx-qa-canonical-json-v1",
            "digest": SHA_B,
        },
    }


def limits_fixture() -> dict:
    return {
        "profile": "prompt-archive-limits-v1",
        "max_blob_uncompressed_bytes": 8 * 1024 * 1024,
        "max_blob_stored_bytes": 8 * 1024 * 1024 + 64 * 1024,
        "max_manifest_bytes": 256 * 1024,
        "max_hydrated_bytes": 256 * 1024 * 1024,
        "max_referenced_stored_bytes": 384 * 1024 * 1024,
        "max_referenced_uncompressed_bytes": 384 * 1024 * 1024,
        "max_reference_depth": 512,
        "max_reference_count": 250_000,
        "max_index_fanout": 64,
        "max_control_json_depth": 64,
        "max_json_value_depth": 64,
        "max_json_value_nodes": 250_000,
        "max_json_edges": 250_000,
        "max_render_literal_bytes": 8 * 1024 * 1024,
        "max_compression_ratio": 2048,
    }


def manifest_fixture(*, native: bool = False) -> dict:
    roots = (
        {"current_fields": index_ref(), "activity_with_patches": index_ref()}
        if native
        else {"raw_bytes": index_ref()}
    )
    value = {
        "schema_version": 1,
        "kind": "prompt_archive_manifest",
        "media_type": "application/vnd.nginx-qa.prompt-archive-manifest-v1+json",
        "id_algorithm": "PAS1-ID-v1",
        "archive_id": ARCHIVE_ID,
        "archive_format": (
            "incremental_transition_v1" if native else "legacy_raw_chunks_v1"
        ),
        "scope": scope_fixture(),
        "source": {
            "source_kind": "runtime_shadow" if native else "offline_migration",
            "source_commit": "d" * 40,
            "assignment_id": "assignment-example-1",
            "predecessor_archive_id": None,
        },
        "authorization": {
            "auth_model_version": "prompt-archive-authorization-v1",
            "principal_kind": "managed_agent" if native else "offline_operator",
            "principal_id": (
                "psc-storage-contract-engineer" if native else "operator-example-1"
            ),
            "binding_id": "assignment-example-1" if native else None,
            "scope_source": (
                "managed_effective_scope" if native else "offline_operator_gate"
            ),
            "effective_scope_revision": 1 if native else None,
            "effective_scope_sha256": "e" * 64 if native else None,
            "decision_occurrence_id": "authorization-example-1",
        },
        "roots": roots,
        "integrity": integrity_fixture(
            "native-byte-equality" if native else "raw-byte-equality"
        ),
        "counts": {
            "records": 3 if native else 0,
            "raw_chunks": 0 if native else 2,
            "payload_chunks": 0,
            "literal_chunks": 3 if native else 0,
            "render_plan_chunks": 1 if native else 0,
            "json_value_nodes": 3 if native else 0,
            "json_edges": 2 if native else 0,
            "index_nodes": 2 if native else 1,
            "unique_referenced_objects": 12 if native else 3,
            "graph_edges": 11 if native else 2,
            "logical_bytes": 4096,
            "referenced_stored_bytes": 1024,
            "referenced_uncompressed_bytes": 1024,
        },
        "limits": limits_fixture(),
        "publication": {
            "mode": "shadow" if native else "migration",
            "legacy_authoritative": True,
            "writer_version": "prompt-archive-writer-v1",
            "verified_at": "2026-10-11T11:30:00Z",
        },
        "created_at": "2026-10-11T11:29:00Z",
    }

    if not native:
        value["scope"].update(
            {
                "response_kind": "migration-snapshot",
                "occurrence_kind": "migration",
                "occurrence_id": "migration-example-1",
                "transition_id": None,
            }
        )

    if native:
        envelope = envelope_fixture(roots)
        envelope_bytes = canonical_control_bytes(envelope)
        value["renderer_version"] = "pas1-concat-renderer-v1"
        value["json_value_model"] = "pas1-recursive-json-merkle-v1"
        value["integrity"]["hydrated_size"] = envelope["source_byte_size"]
        value["integrity"]["hydrated_sha256"] = envelope["source_sha256"]
        value["envelope"] = {
            "media_type": "application/vnd.nginx-qa.prompt-response-envelope-v1+json",
            "byte_size": len(envelope_bytes),
            "sha256": hashlib.sha256(envelope_bytes).hexdigest(),
            "chunks_root": index_ref(),
        }
        value["legacy_comparison"] = {
            "byte_size": envelope["source_byte_size"],
            "sha256": envelope["source_sha256"],
            "renderer_version": "pas1-concat-renderer-v1",
            "matched": True,
        }
    return value


def envelope_fixture(roots: dict | None = None) -> dict:
    roots = roots or {
        "current_fields": index_ref(),
        "activity_with_patches": index_ref(),
    }
    literal = b'{"current":'
    current_field = b'{"task":"x"}'
    activity = (
        b',"events":[' + b'{"event":1}' + b',' + b'{"event":2}' + b']}'
    )
    source = literal + current_field + activity
    literal_end = len(literal)
    current_field_end = literal_end + len(current_field)
    return {
        "schema_version": 1,
        "kind": "prompt_archive_response_envelope",
        "media_type": "application/vnd.nginx-qa.prompt-response-envelope-v1+json",
        "builder_version": "pas1-render-plan-builder-v1",
        "renderer_version": "pas1-concat-renderer-v1",
        "json_value_model": "pas1-recursive-json-merkle-v1",
        "response_id": RESPONSE_ID,
        "response_kind": "transition-response",
        "source_byte_size": len(source),
        "source_sha256": hashlib.sha256(source).hexdigest(),
        "steps": [
            {
                "step_kind": "literal",
                "source_start": 0,
                "source_end": literal_end,
                "chunks_root": index_ref(),
                "byte_size": len(literal),
                "sha256": hashlib.sha256(literal).hexdigest(),
            },
            {
                "step_kind": "current_field",
                "source_start": literal_end,
                "source_end": current_field_end,
                "value_ordinal": 0,
                "record_ref": blob_ref("current_field_record"),
            },
            {
                "step_kind": "sequence",
                "source_start": current_field_end,
                "source_end": len(source),
                "slot_id": "project_state.activity_with_patches",
                "sequence_kind": "events",
                "index_root": copy.deepcopy(roots["activity_with_patches"]),
                "prefix_b64": base64.b64encode(b',"events":[').decode("ascii"),
                "separator_b64": base64.b64encode(b",").decode("ascii"),
                "suffix_b64": base64.b64encode(b"]}").decode("ascii"),
            },
        ],
    }


def record_fixture() -> dict:
    return {
        "schema_version": 1,
        "kind": "prompt_archive_record",
        "id_algorithm": "PAS1-ID-v1",
        "record_id": f"parrec1-{SHA_B}",
        "record_type": "event",
        "ordinal": 0,
        "occurred_at": "2026-10-11T11:28:00Z",
        "scope": {
            "project_id": "9000",
            "sprint_id": "sprint-example-1",
        },
        "payload": {
            "representation": "recursive_json_merkle_v1",
            "value_root": blob_ref("json_value"),
            "media_type": "application/json",
            "byte_size": 128,
            "sha256": SHA_A,
        },
        "provenance": {
            "source_kind": "runtime_transition",
            "origin_response_id": RESPONSE_ID,
            "origin_transition_id": "transition-example-1",
            "source_id": "history-event-1",
            "source_commit": "d" * 40,
            "assignment_id": "assignment-example-1",
        },
    }


def json_value_fixture() -> dict:
    token = b"null"
    return {
        "schema_version": 1,
        "kind": "prompt_archive_json_value",
        "media_type": "application/vnd.nginx-qa.prompt-archive-json-value-v1+json",
        "builder_version": "pas1-recursive-json-merkle-v1",
        "node_kind": "scalar",
        "scalar_kind": "null",
        "token": {
            "chunks_root": index_ref(),
            "byte_size": len(token),
            "sha256": hashlib.sha256(token).hexdigest(),
        },
        "byte_size": len(token),
        "sha256": hashlib.sha256(token).hexdigest(),
    }


def json_edge_fixture() -> dict:
    separator = b""
    value = b"null"
    return {
        "schema_version": 1,
        "kind": "prompt_archive_json_edge",
        "media_type": "application/vnd.nginx-qa.prompt-archive-json-edge-v1+json",
        "builder_version": "pas1-recursive-json-merkle-v1",
        "edge_kind": "array_item",
        "separator_before": {
            "chunks_root": index_ref(),
            "byte_size": len(separator),
            "sha256": hashlib.sha256(separator).hexdigest(),
        },
        "value_ref": blob_ref("json_value"),
        "byte_size": len(value),
        "sha256": hashlib.sha256(value).hexdigest(),
    }


def index_fixture() -> dict:
    return {
        "schema_version": 1,
        "kind": "prompt_archive_index_node",
        "sequence_kind": "events",
        "level": 0,
        "item_count": 1,
        "logical_bytes": 128,
        "children": [
            {
                "ordinal_start": 0,
                "item_count": 1,
                "logical_bytes": 128,
                "ref": blob_ref("event_record"),
            }
        ],
    }


def pointer_fixture(
    manifest_ref: dict | None = None,
    *,
    published_at: str = "2026-10-11T11:31:00Z",
) -> dict:
    if manifest_ref is None:
        manifest_ref = blob_ref("manifest")
        manifest_ref["media_type"] = (
            "application/vnd.nginx-qa.prompt-archive-manifest-v1+json"
        )
    pointer_id = "parptr1-" + pas1_id_digest(
        "pointer-id",
        [
            ("project_id", "9000"),
            ("sprint_id", "sprint-example-1"),
            ("response_id", RESPONSE_ID),
            ("generation", "1"),
            ("archive_id", ARCHIVE_ID),
            ("manifest_sha256", manifest_ref["digest"]),
            ("previous_pointer_id", None),
            ("published_at", published_at),
        ],
    )
    return {
        "schema_version": 1,
        "kind": "prompt_archive_pointer",
        "id_algorithm": "PAS1-ID-v1",
        "pointer_id": pointer_id,
        "project_id": "9000",
        "sprint_id": "sprint-example-1",
        "response_id": RESPONSE_ID,
        "archive_id": ARCHIVE_ID,
        "generation": 1,
        "previous_pointer_id": None,
        "manifest_ref": manifest_ref,
        "published_at": published_at,
    }


def descriptor_fixture() -> dict:
    base = f"/api/v1/projects/9000/sprints/sprint-example-1/prompt-archives/{ARCHIVE_ID}"
    return {
        "schema_version": 1,
        "kind": "prompt_archive_descriptor",
        "media_type": "application/vnd.nginx-qa.prompt-archive-descriptor+json",
        "correlation_id": "prompt-archive-request-example-1",
        "route_context": "local_operator",
        "agent_phone": None,
        "archive_id": ARCHIVE_ID,
        "archive_format": "legacy_raw_chunks_v1",
        "scope": scope_fixture(),
        "integrity": integrity_fixture(),
        "created_at": "2026-10-11T11:29:00Z",
        "descriptor_url": base,
        "manifest_url": base + "/manifest",
        "content_url": base + "/content",
    }


def managed_descriptor_fixture() -> dict:
    value = descriptor_fixture()
    value["route_context"] = "managed_agent"
    value["agent_phone"] = "2971"
    base = (
        f"/api/v1/projects/9000/agents/2971/sprints/sprint-example-1/"
        f"prompt-archives/{ARCHIVE_ID}"
    )
    value["descriptor_url"] = base
    value["manifest_url"] = base + "/manifest"
    value["content_url"] = base + "/content"
    return value


def archive_locator_fixture(
    pointer: dict | None = None,
    manifest_ref: dict | None = None,
    *,
    published_at: str | None = None,
) -> dict:
    pointer = pointer or pointer_fixture(manifest_ref)
    manifest_ref = manifest_ref or copy.deepcopy(pointer["manifest_ref"])
    published_at = published_at or pointer["published_at"]
    pointer_bytes = canonical_control_bytes(pointer)
    pointer_ref = blob_ref("pointer")
    pointer_ref["media_type"] = (
        "application/vnd.nginx-qa.prompt-archive-pointer-v1+json"
    )
    bind_ref_to_bytes(pointer_ref, pointer_bytes)
    locator_id = "pararc1-" + pas1_id_digest(
        "archive-locator-id",
        [
            ("project_id", pointer["project_id"]),
            ("sprint_id", pointer["sprint_id"]),
            ("response_id", pointer["response_id"]),
            ("archive_id", pointer["archive_id"]),
            ("pointer_id", pointer["pointer_id"]),
            ("manifest_sha256", manifest_ref["digest"]),
            ("published_at", published_at),
        ],
    )
    return {
        "schema_version": 1,
        "kind": "prompt_archive_locator",
        "media_type": "application/vnd.nginx-qa.prompt-archive-locator-v1+json",
        "id_algorithm": "PAS1-ID-v1",
        "locator_id": locator_id,
        "scope_kind": "sprint",
        "project_id": "9000",
        "sprint_id": "sprint-example-1",
        "response_id": RESPONSE_ID,
        "archive_id": ARCHIVE_ID,
        "pointer_id": pointer["pointer_id"],
        "pointer_ref": pointer_ref,
        "manifest_ref": copy.deepcopy(manifest_ref),
        "published_at": published_at,
    }


def descriptor_stub_fixture(
    pointer: dict | None = None,
    manifest_ref: dict | None = None,
) -> dict:
    pointer = pointer or pointer_fixture(manifest_ref)
    manifest_ref = manifest_ref or copy.deepcopy(pointer["manifest_ref"])
    created_at = "2026-10-11T11:29:00Z"
    published_at = pointer["published_at"]
    stub_id = "parstub1-" + pas1_id_digest(
        "descriptor-stub-id",
        [
            ("project_id", pointer["project_id"]),
            ("sprint_id", pointer["sprint_id"]),
            ("response_id", pointer["response_id"]),
            ("archive_id", pointer["archive_id"]),
            ("pointer_id", pointer["pointer_id"]),
            ("manifest_sha256", manifest_ref["digest"]),
            ("created_at", created_at),
            ("published_at", published_at),
        ],
    )
    return {
        "schema_version": 1,
        "kind": "prompt_archive_descriptor_stub",
        "media_type": (
            "application/vnd.nginx-qa.prompt-archive-descriptor-stub-v1+json"
        ),
        "id_algorithm": "PAS1-ID-v1",
        "stub_id": stub_id,
        "archive_id": pointer["archive_id"],
        "archive_format": "legacy_raw_chunks_v1",
        "scope": scope_fixture(),
        "integrity": integrity_fixture(),
        "pointer_id": pointer["pointer_id"],
        "manifest_ref": copy.deepcopy(manifest_ref),
        "created_at": created_at,
        "published_at": published_at,
    }


def source_inventory_fixture() -> dict:
    created_at = "2026-10-11T11:20:00Z"
    inventory_id = "parinv1-" + pas1_id_digest(
        "source-inventory-id",
        [
            ("project_id", "9000"),
            ("source_root_handle_id", "isolated-root-1"),
            ("source_root_identity_sha256", SHA_A),
            ("inventory_sha256", SHA_B),
            ("entry_count", "2"),
            ("created_at", created_at),
        ],
    )
    return {
        "schema_version": 1,
        "kind": "prompt_archive_source_inventory",
        "media_type": (
            "application/vnd.nginx-qa.prompt-archive-source-inventory-v1+json"
        ),
        "id_algorithm": "PAS1-ID-v1",
        "inventory_id": inventory_id,
        "inventory_format": "prompt-archive-isolated-inventory-v1",
        "project_id": "9000",
        "source_root_handle_id": "isolated-root-1",
        "source_root_identity_sha256": SHA_A,
        "inventory_sha256": SHA_B,
        "entry_count": 2,
        "created_at": created_at,
    }


def source_metadata_fixture() -> dict:
    return {
        "schema_version": 1,
        "kind": "prompt_archive_source_metadata",
        "projection_version": "prompt-archive-source-projection-v1",
        "redaction_version": "prompt-archive-source-redaction-v1",
        "source_type": "email",
        "conversation_id": "sha256:" + "1" * 64,
        "thread_id": "[REDACTED]",
        "sender_label": "[REDACTED]",
        "title": "Release evidence",
        "observed_at": "2026-10-11T11:19:00Z",
        "redaction_count": 2,
        "redacted_fields": ["sender_label", "thread_id"],
        "discarded_field_count": 3,
    }


def revision_authority_fixture() -> dict:
    return {
        "protocol": "prompt-archive-durable-project-latest-revision-v1",
        "epoch": 1,
        "reset_policy": "restore_high_water_mark_or_fail_closed",
    }


def latest_bootstrap_fixture(kind: str = "enrolled") -> dict:
    value = {
        "protocol": "prompt-archive-project-global-bootstrap-enrollment-v1",
        "lock_scope": "project",
        "slot_id": "latest-response",
        "registry_shard_count": 256,
        "writer_lock_policy": "all-latest-writers-pre-and-post-enrollment-v1",
        "lock_location_policy": "external-latest-colocated-stable-lock-v1",
        "registration_order": "observe-mark-unlock-register-relock-reconcile-v1",
        "bootstrap_completion_barrier": (
            "durable-marker-before-unlock-content-slot-before-final-unlock-v1"
        ),
        "kind": kind,
    }
    if kind == "legacy_bytes":
        value.update({"sha256": SHA_C, "byte_size": 512})
    return value


def latest_intent_fixture(
    *,
    operation_id: str = "parop1-" + "4" * 32,
    occurrence_id: str = "transition-example-1",
    content_kind: str = "legacy_inline",
    content_sha256: str = SHA_A,
    content_size: int = 512,
    descriptor_ref: dict | None = None,
    response_id: str = RESPONSE_ID,
    bootstrap_kind: str = "enrolled",
) -> dict:
    descriptor_digest = None if descriptor_ref is None else descriptor_ref["digest"]
    authority = revision_authority_fixture()
    bootstrap = latest_bootstrap_fixture(bootstrap_kind)
    fields = [
        ("project_id", "9000"),
        ("slot_id", "latest-response"),
        ("revision_protocol", authority["protocol"]),
        ("revision_epoch", str(authority["epoch"])),
        ("revision_reset_policy", authority["reset_policy"]),
        ("source_revision", "42"),
        ("occurrence_id", occurrence_id),
        ("operation_id", operation_id),
        ("bootstrap_kind", bootstrap["kind"]),
        ("bootstrap_sha256", bootstrap.get("sha256")),
        (
            "bootstrap_byte_size",
            None
            if bootstrap.get("byte_size") is None
            else str(bootstrap["byte_size"]),
        ),
        ("content_kind", content_kind),
        ("content_sha256", content_sha256),
        ("content_size", str(content_size)),
        ("descriptor_digest", descriptor_digest),
        ("source_sprint_id", "sprint-example-1"),
        ("source_response_id", response_id),
        ("published_at", "2026-10-11T11:35:00Z"),
    ]
    return {
        "identity_version": "prompt-archive-latest-content-intent-v1",
        "intent_id": "parlatestintent1-" + pas1_id_digest(
            "latest-content-intent-id", fields
        ),
        "project_id": "9000",
        "slot_scope": "project",
        "slot_id": "latest-response",
        "publication_protocol": (
            "prompt-archive-project-global-ordered-latest-intent-v1"
        ),
        "ordering": {
            "revision_authority": authority,
            "source_revision": 42,
            "occurrence_id": occurrence_id,
            "operation_id": operation_id,
        },
        "bootstrap_precondition": bootstrap,
        "content_kind": content_kind,
        "content_sha256": content_sha256,
        "content_size": content_size,
        "descriptor_ref": copy.deepcopy(descriptor_ref),
        "source_sprint_id": "sprint-example-1",
        "source_response_id": response_id,
        "published_at": "2026-10-11T11:35:00Z",
    }


def latest_slot_fixture(intent: dict | None = None) -> dict:
    intent = intent or latest_intent_fixture()
    descriptor_digest = (
        None if intent["descriptor_ref"] is None else intent["descriptor_ref"]["digest"]
    )
    published_at = intent["published_at"]
    ordering = intent["ordering"]
    authority = ordering["revision_authority"]
    latest_slot_id = "parlatest1-" + pas1_id_digest(
        "latest-slot-id",
        [
            ("project_id", intent["project_id"]),
            ("slot_id", intent["slot_id"]),
            ("intent_id", intent["intent_id"]),
            ("revision_protocol", authority["protocol"]),
            ("revision_epoch", str(authority["epoch"])),
            ("revision_reset_policy", authority["reset_policy"]),
            ("source_revision", str(ordering["source_revision"])),
            ("occurrence_id", ordering["occurrence_id"]),
            ("operation_id", ordering["operation_id"]),
            ("generation", "1"),
            ("previous_slot_id", None),
            ("content_kind", intent["content_kind"]),
            ("content_sha256", intent["content_sha256"]),
            ("content_size", str(intent["content_size"])),
            ("descriptor_digest", descriptor_digest),
            ("source_sprint_id", intent["source_sprint_id"]),
            ("source_response_id", intent["source_response_id"]),
            ("published_at", published_at),
        ],
    )
    return {
        "schema_version": 1,
        "kind": "prompt_archive_latest_slot",
        "media_type": "application/vnd.nginx-qa.prompt-archive-latest-slot-v1+json",
        "id_algorithm": "PAS1-ID-v1",
        "latest_slot_id": latest_slot_id,
        "intent_id": intent["intent_id"],
        "project_id": intent["project_id"],
        "slot_scope": intent["slot_scope"],
        "slot_id": intent["slot_id"],
        "publication_protocol": intent["publication_protocol"],
        "ordering": copy.deepcopy(intent["ordering"]),
        "generation": 1,
        "previous_slot_id": None,
        "content_kind": intent["content_kind"],
        "content_sha256": intent["content_sha256"],
        "content_size": intent["content_size"],
        "descriptor_ref": copy.deepcopy(intent["descriptor_ref"]),
        "source_sprint_id": intent["source_sprint_id"],
        "source_response_id": intent["source_response_id"],
        "published_at": published_at,
    }


def pending_registry_plan_fixture(
    recovery_locator: dict,
    operation_id: str,
    *,
    published_at: str,
) -> dict:
    recovery_key = recovery_locator["recovery_key"]
    return {
        "protocol": "prompt-archive-pending-registry-cas-v1",
        "shard": {
            "project_id": recovery_locator["project_id"],
            "shard_algorithm": "sha256-recovery-key-prefix-2-v1",
            "shard_id": hashlib.sha256(recovery_key.encode("utf-8")).hexdigest()[:2],
            "max_entries": 1024,
        },
        "entry_precondition": {"kind": "absent"},
        "intended_mutation": {
            "mutation": "upsert_planned",
            "operation_id": operation_id,
            "recovery_key": recovery_key,
            "plan_ref_binding": "canonical_current_plan_ref",
            "state": "planned",
            "journal_ref": None,
            "created_at": published_at,
            "updated_at": published_at,
            "published_at": published_at,
        },
    }


def pending_registry_fixture(
    entry: dict,
    *,
    published_at: str = "2026-10-11T11:30:00Z",
) -> dict:
    entries = [copy.deepcopy(entry)]
    entries_sha256 = hashlib.sha256(canonical_control_bytes(entries)).hexdigest()
    shard_id = hashlib.sha256(entry["recovery_key"].encode("utf-8")).hexdigest()[:2]
    registry_id = "parpending1-" + pas1_id_digest(
        "pending-operation-registry-id",
        [
            ("project_id", "9000"),
            ("shard_id", shard_id),
            ("generation", "1"),
            ("previous_registry_id", None),
            ("entries_sha256", entries_sha256),
            ("published_at", published_at),
        ],
    )
    return {
        "schema_version": 1,
        "kind": "prompt_archive_pending_operation_registry",
        "media_type": (
            "application/vnd.nginx-qa.prompt-archive-pending-operation-registry-v1+json"
        ),
        "id_algorithm": "PAS1-ID-v1",
        "registry_id": registry_id,
        "project_id": "9000",
        "shard_algorithm": "sha256-recovery-key-prefix-2-v1",
        "shard_id": shard_id,
        "generation": 1,
        "previous_registry_id": None,
        "entries": entries,
        "published_at": published_at,
    }


def pending_registry_example_fixture() -> dict:
    plan = publication_plan_fixture()
    plan_ref = blob_ref("publication_plan")
    bind_ref_to_bytes(plan_ref, canonical_control_bytes(plan))
    entry = {
        "operation_id": plan["operation_id"],
        "recovery_key": plan["recovery_locator"]["recovery_key"],
        "plan_ref": plan_ref,
        "state": "planned",
        "journal_ref": None,
        "created_at": plan["frozen_timestamps"]["planned_at"],
        "updated_at": plan["frozen_timestamps"]["planned_at"],
    }
    return pending_registry_fixture(
        entry, published_at=plan["frozen_timestamps"]["planned_at"]
    )


def error_fixture(code: str = "PROMPT_ARCHIVE_NOT_FOUND") -> dict:
    return {
        "detail": {
            "schema_version": 1,
            "error": code,
            "message": "Prompt archive is unavailable for this request.",
            "correlation_id": "prompt-archive-request-example-1",
            "retryable": code in {
                "PROMPT_ARCHIVE_GENERATION_CONFLICT",
                "PROMPT_ARCHIVE_BUSY",
                "PROMPT_ARCHIVE_UNAVAILABLE",
            },
        }
    }


def blob_fixture() -> dict:
    return {
        "schema_version": 1,
        "kind": "prompt_archive_blob",
        "media_type": "application/vnd.nginx-qa.prompt-archive-blob-v1+json",
        "ref": blob_ref(),
    }


def publication_plan_fixture() -> dict:
    manifest = manifest_fixture()
    manifest_bytes = canonical_control_bytes(manifest)
    manifest_ref = blob_ref("manifest")
    manifest_ref["media_type"] = (
        "application/vnd.nginx-qa.prompt-archive-manifest-v1+json"
    )
    bind_ref_to_bytes(manifest_ref, manifest_bytes)
    frozen_timestamps = {
        "source_observed_at": "2026-10-11T11:28:00Z",
        "planned_at": "2026-10-11T11:29:00Z",
        "intent_at": "2026-10-11T11:30:00Z",
        "blobs_verified_at": "2026-10-11T11:31:00Z",
        "manifest_verified_at": "2026-10-11T11:32:00Z",
        "pointer_published_at": "2026-10-11T11:33:00Z",
        "locator_published_at": "2026-10-11T11:34:00Z",
        "compatibility_published_at": "2026-10-11T11:35:00Z",
        "completed_at": "2026-10-11T11:36:00Z",
        "aborted_at": "2026-10-11T11:37:00Z",
    }
    pointer = pointer_fixture(
        manifest_ref,
        published_at=frozen_timestamps["pointer_published_at"],
    )
    pointer_ref = blob_ref("pointer")
    pointer_ref["media_type"] = (
        "application/vnd.nginx-qa.prompt-archive-pointer-v1+json"
    )
    bind_ref_to_bytes(pointer_ref, canonical_control_bytes(pointer))
    locator = archive_locator_fixture(
        pointer,
        manifest_ref,
        published_at=frozen_timestamps["locator_published_at"],
    )
    locator_ref = blob_ref("archive_locator")
    locator_ref["media_type"] = (
        "application/vnd.nginx-qa.prompt-archive-locator-v1+json"
    )
    bind_ref_to_bytes(locator_ref, canonical_control_bytes(locator))
    operation_id = "parop1-" + "4" * 32
    recovery_locator = {
        "locator_version": "prompt-archive-publication-locator-v1",
        "recovery_key": (
            "parloc1-cd339ca08291d59153ceb7f317c975870af10d3ce11c5b16630e0187cbf6f12c"
        ),
        "project_id": "9000",
        "sprint_id": "sprint-example-1",
        "response_id": RESPONSE_ID,
        "occurrence_id": "migration-example-1",
        "operation_id": operation_id,
    }
    inventory = source_inventory_fixture()
    inventory_ref = blob_ref("source_inventory")
    bind_ref_to_bytes(inventory_ref, canonical_control_bytes(inventory))
    pending_registry = pending_registry_plan_fixture(
        recovery_locator,
        operation_id,
        published_at=frozen_timestamps["planned_at"],
    )
    return {
        "schema_version": 1,
        "kind": "prompt_archive_publication_plan",
        "media_type": (
            "application/vnd.nginx-qa.prompt-archive-publication-plan-v1+json"
        ),
        "id_algorithm": "PAS1-ID-v1",
        "operation_id": operation_id,
        "operation_kind": "offline_migration",
        "scope": {"project_id": "9000", "sprint_id": "sprint-example-1"},
        "occurrence": {
            "response_id": RESPONSE_ID,
            "response_kind": "migration-snapshot",
            "occurrence_kind": "migration",
            "occurrence_id": "migration-example-1",
            "transition_id": None,
        },
        "recovery_locator": recovery_locator,
        "pending_registry": pending_registry,
        "source_locator": {
            "locator_kind": "isolated_inventory",
            "locator_version": "prompt-archive-isolated-inventory-locator-v1",
            "inventory_id": inventory["inventory_id"],
            "inventory_format": inventory["inventory_format"],
            "inventory_sha256": inventory["inventory_sha256"],
            "entry_count": inventory["entry_count"],
            "inventory_ref": inventory_ref,
        },
        "intended_manifest": {
            "identity_version": "prompt-archive-intended-manifest-identity-v1",
            "archive_id": ARCHIVE_ID,
            "manifest_ref": manifest_ref,
            "document": manifest,
        },
        "intended_pointer": {
            "identity_version": "prompt-archive-intended-pointer-identity-v1",
            "pointer_id": pointer["pointer_id"],
            "pointer_ref": pointer_ref,
            "document": pointer,
        },
        "intended_archive_locator": {
            "identity_version": "prompt-archive-intended-locator-identity-v1",
            "locator_id": locator["locator_id"],
            "locator_ref": locator_ref,
            "document": locator,
        },
        "planned_reachable_set": {
            "digest_version": "prompt-archive-reachable-set-digest-v1",
            "hash_algorithm": "sha256",
            "digest": "5" * 64,
            "unique_ref_count": 4,
            "includes_manifest": True,
        },
        "pointer_precondition": {
            "expected_generation": 0,
            "previous_pointer_id": None,
        },
        "compatibility_publication": {"mode": "none"},
        "frozen_timestamps": frozen_timestamps,
    }


def legacy_latest_plan_fixture(write_kind: str = "transition") -> dict:
    if write_kind not in {"transition", "identity", "state", "rollback"}:
        raise ValueError(f"unsupported legacy write kind: {write_kind}")
    operation_id = "parop1-" + "6" * 32
    occurrence_id = f"{write_kind}-example-1"
    if write_kind == "transition":
        response_kind = "transition-response"
        occurrence_kind = "transition"
        transition_id = occurrence_id
    else:
        response_kind = "identity-response" if write_kind == "identity" else "state-response"
        occurrence_kind = "identity-state"
        transition_id = None
    frozen_timestamps = {
        "source_observed_at": "2026-10-11T11:28:00Z",
        "planned_at": "2026-10-11T11:29:00Z",
        "intent_at": "2026-10-11T11:30:00Z",
        "legacy_payload_verified_at": "2026-10-11T11:32:00Z",
        "compatibility_published_at": "2026-10-11T11:35:00Z",
        "completed_at": "2026-10-11T11:36:00Z",
        "aborted_at": "2026-10-11T11:37:00Z",
    }
    recovery_locator = {
        "locator_version": "prompt-archive-publication-locator-v1",
        "recovery_key": "parloc1-"
        + pas1_id_digest(
            "publication-locator",
            [
                ("project_id", "9000"),
                ("sprint_id", "sprint-example-1"),
                ("response_id", RESPONSE_ID),
                ("occurrence_id", occurrence_id),
                ("operation_id", operation_id),
            ],
        ),
        "project_id": "9000",
        "sprint_id": "sprint-example-1",
        "response_id": RESPONSE_ID,
        "occurrence_id": occurrence_id,
        "operation_id": operation_id,
    }
    latest_intent = latest_intent_fixture(
        operation_id=operation_id,
        occurrence_id=occurrence_id,
        content_kind="legacy_inline",
        content_sha256=SHA_C,
        content_size=512,
        response_id=RESPONSE_ID,
        bootstrap_kind="enrolled",
    )
    return {
        "schema_version": 1,
        "kind": "prompt_archive_publication_plan",
        "media_type": (
            "application/vnd.nginx-qa.prompt-archive-publication-plan-v1+json"
        ),
        "id_algorithm": "PAS1-ID-v1",
        "operation_id": operation_id,
        "operation_kind": "legacy_latest_publish",
        "scope": {"project_id": "9000", "sprint_id": "sprint-example-1"},
        "occurrence": {
            "response_id": RESPONSE_ID,
            "response_kind": response_kind,
            "occurrence_kind": occurrence_kind,
            "occurrence_id": occurrence_id,
            "transition_id": transition_id,
        },
        "recovery_locator": recovery_locator,
        "pending_registry": pending_registry_plan_fixture(
            recovery_locator,
            operation_id,
            published_at=frozen_timestamps["planned_at"],
        ),
        "compatibility_publication": {
            "mode": "legacy_latest_publish",
            "latest_intent": latest_intent,
        },
        "legacy_latest": {
            "identity_version": "prompt-archive-legacy-latest-payload-v1",
            "write_kind": write_kind,
            "content_media_type": "application/json",
            "content_sha256": SHA_C,
            "content_size": 512,
            "payload_root": index_ref(),
            "authoritative_drain_barrier": {
                "protocol": "reconcile-authoritative-before-legacy-latest-write-v1",
                "lock_scope": "project",
                "slot_id": "latest-response",
                "registry_shard_count": 256,
                "observed_incomplete_operation_ids": [],
                "observed_at": frozen_timestamps["planned_at"],
            },
        },
        "frozen_timestamps": frozen_timestamps,
    }


def legacy_latest_journal_fixture(
    write_kind: str = "transition",
    state: str = "complete",
) -> dict:
    states = [
        "intent",
        "legacy_payload_verified",
        "compatibility_published",
        "complete",
    ]
    plan = legacy_latest_plan_fixture(write_kind)
    plan_ref = blob_ref("publication_plan")
    bind_ref_to_bytes(plan_ref, canonical_control_bytes(plan))
    latest_slot = latest_slot_fixture(
        plan["compatibility_publication"]["latest_intent"]
    )
    latest_slot_ref = blob_ref("latest_slot")
    bind_ref_to_bytes(latest_slot_ref, canonical_control_bytes(latest_slot))
    timestamp_keys = {
        "intent": "intent_at",
        "legacy_payload_verified": "legacy_payload_verified_at",
        "compatibility_published": "compatibility_published_at",
        "complete": "completed_at",
    }
    expected_journal_id = "parjournal1-" + pas1_id_digest(
        "publication-journal",
        [
            ("project_id", plan["scope"]["project_id"]),
            ("sprint_id", plan["scope"]["sprint_id"]),
            ("response_id", plan["occurrence"]["response_id"]),
            ("operation_id", plan["operation_id"]),
        ],
    )
    base = {
        "schema_version": 1,
        "kind": "prompt_archive_journal",
        "media_type": "application/vnd.nginx-qa.prompt-archive-journal-v1+json",
        "id_algorithm": "PAS1-ID-v1",
        "journal_id": expected_journal_id,
        "operation_id": plan["operation_id"],
        "operation_kind": plan["operation_kind"],
        "scope": copy.deepcopy(plan["scope"]),
        "occurrence": {
            "response_id": plan["occurrence"]["response_id"],
            "occurrence_id": plan["occurrence"]["occurrence_id"],
        },
        "recovery_locator": copy.deepcopy(plan["recovery_locator"]),
        "plan_ref": plan_ref,
        "legacy_payload_root": copy.deepcopy(plan["legacy_latest"]["payload_root"]),
        "legacy_content_sha256": plan["legacy_latest"]["content_sha256"],
        "legacy_content_size": plan["legacy_latest"]["content_size"],
        "authoritative_drain_barrier": copy.deepcopy(
            plan["legacy_latest"]["authoritative_drain_barrier"]
        ),
        "compatibility_mode": "legacy_latest_publish",
        "compatibility_descriptor_ref": None,
        "latest_intent": copy.deepcopy(
            plan["compatibility_publication"]["latest_intent"]
        ),
        "frozen_timestamps": copy.deepcopy(plan["frozen_timestamps"]),
    }
    previous: dict | None = None
    documents: dict[str, dict] = {}
    events: list[dict] = []
    for generation, event_state in enumerate(states):
        occurred_at = plan["frozen_timestamps"][timestamp_keys[event_state]]
        events.append(
            {"state": event_state, "ordinal": generation, "occurred_at": occurred_at}
        )
        if previous is None:
            precondition_entry = {
                "operation_id": plan["operation_id"],
                "recovery_key": plan["recovery_locator"]["recovery_key"],
                "plan_ref": copy.deepcopy(plan_ref),
                "state": "planned",
                "journal_ref": None,
                "created_at": plan["frozen_timestamps"]["planned_at"],
                "updated_at": plan["frozen_timestamps"]["planned_at"],
            }
        else:
            previous_ref = blob_ref("journal")
            bind_ref_to_bytes(previous_ref, canonical_control_bytes(previous))
            precondition_entry = {
                "operation_id": plan["operation_id"],
                "recovery_key": plan["recovery_locator"]["recovery_key"],
                "plan_ref": copy.deepcopy(plan_ref),
                "state": "journaled",
                "journal_ref": previous_ref,
                "created_at": plan["frozen_timestamps"]["planned_at"],
                "updated_at": previous["updated_at"],
            }
        terminal = event_state == "complete"
        document = copy.deepcopy(base)
        document.update(
            {
                "pending_registry": {
                    "protocol": "prompt-archive-pending-registry-cas-v1",
                    "shard": copy.deepcopy(plan["pending_registry"]["shard"]),
                    "precondition_entry": precondition_entry,
                    "intended_mutation": (
                        {
                            "mutation": "remove_terminal_entry",
                            "terminal_state": "complete",
                            "published_at": occurred_at,
                        }
                        if terminal
                        else {
                            "mutation": "bind_current_journal",
                            "journal_ref_binding": "canonical_current_journal_ref",
                            "updated_at": occurred_at,
                            "published_at": occurred_at,
                        }
                    ),
                },
                "journal_generation": generation,
                "previous_journal_generation": (
                    None if previous is None else generation - 1
                ),
                "previous_journal_sha256": (
                    None
                    if previous is None
                    else hashlib.sha256(canonical_control_bytes(previous)).hexdigest()
                ),
                "result_latest_slot_id": (
                    latest_slot["latest_slot_id"] if generation >= 2 else None
                ),
                "result_latest_slot_ref": (
                    copy.deepcopy(latest_slot_ref) if generation >= 2 else None
                ),
                "latest_slot_outcome": "published" if generation >= 2 else "pending",
                "compatibility_result": "published" if generation >= 2 else "pending",
                "state": event_state,
                "events": copy.deepcopy(events),
                "updated_at": occurred_at,
            }
        )
        documents[event_state] = document
        previous = document
    return documents[state]


def journal_fixture(state: str = "complete") -> dict:
    states = [
        "intent",
        "blobs_verified",
        "manifest_verified",
        "pointer_published",
        "locator_published",
        "compatibility_published",
        "complete",
    ]
    plan = publication_plan_fixture()
    manifest_ref = copy.deepcopy(plan["intended_manifest"]["manifest_ref"])
    plan_ref = blob_ref("publication_plan")
    plan_ref["media_type"] = (
        "application/vnd.nginx-qa.prompt-archive-publication-plan-v1+json"
    )
    plan_bytes = canonical_control_bytes(plan)
    bind_ref_to_bytes(plan_ref, plan_bytes)
    pointer = plan["intended_pointer"]
    locator = plan["intended_archive_locator"]
    timestamp_keys = {
        "intent": "intent_at",
        "blobs_verified": "blobs_verified_at",
        "manifest_verified": "manifest_verified_at",
        "pointer_published": "pointer_published_at",
        "locator_published": "locator_published_at",
        "compatibility_published": "compatibility_published_at",
        "complete": "completed_at",
    }
    base = {
        "schema_version": 1,
        "kind": "prompt_archive_journal",
        "media_type": "application/vnd.nginx-qa.prompt-archive-journal-v1+json",
        "id_algorithm": "PAS1-ID-v1",
        "journal_id": (
            "parjournal1-c291f580c6c8db505d2538e3cca8f0f308be74ed9078ab6d0cdfa3c4293ec2a2"
        ),
        "operation_id": "parop1-" + "4" * 32,
        "operation_kind": "offline_migration",
        "scope": {"project_id": "9000", "sprint_id": "sprint-example-1"},
        "occurrence": {
            "response_id": RESPONSE_ID,
            "occurrence_id": "migration-example-1",
        },
        "recovery_locator": plan["recovery_locator"],
        "archive_id": ARCHIVE_ID,
        "plan_ref": plan_ref,
        "manifest_ref": manifest_ref,
        "pointer_ref": copy.deepcopy(pointer["pointer_ref"]),
        "archive_locator_ref": copy.deepcopy(locator["locator_ref"]),
        "compatibility_mode": "none",
        "compatibility_descriptor_ref": None,
        "latest_intent": None,
        "expected_pointer_generation": 0,
        "previous_pointer_id": None,
        "frozen_timestamps": copy.deepcopy(plan["frozen_timestamps"]),
    }
    previous: dict | None = None
    documents: dict[str, dict] = {}
    events: list[dict] = []
    for generation, event_state in enumerate(states):
        occurred_at = plan["frozen_timestamps"][timestamp_keys[event_state]]
        events.append(
            {
                "state": event_state,
                "ordinal": generation,
                "occurred_at": occurred_at,
            }
        )
        document = copy.deepcopy(base)
        if previous is None:
            precondition_entry = {
                "operation_id": plan["operation_id"],
                "recovery_key": plan["recovery_locator"]["recovery_key"],
                "plan_ref": copy.deepcopy(plan_ref),
                "state": "planned",
                "journal_ref": None,
                "created_at": plan["frozen_timestamps"]["planned_at"],
                "updated_at": plan["frozen_timestamps"]["planned_at"],
            }
        else:
            previous_ref = blob_ref("journal")
            bind_ref_to_bytes(previous_ref, canonical_control_bytes(previous))
            precondition_entry = {
                "operation_id": plan["operation_id"],
                "recovery_key": plan["recovery_locator"]["recovery_key"],
                "plan_ref": copy.deepcopy(plan_ref),
                "state": "journaled",
                "journal_ref": previous_ref,
                "created_at": plan["frozen_timestamps"]["planned_at"],
                "updated_at": previous["updated_at"],
            }
        pending_mutation = (
            {
                "mutation": "remove_terminal_entry",
                "terminal_state": "complete",
                "published_at": occurred_at,
            }
            if event_state == "complete"
            else {
                "mutation": "bind_current_journal",
                "journal_ref_binding": "canonical_current_journal_ref",
                "updated_at": occurred_at,
                "published_at": occurred_at,
            }
        )
        document.update(
            {
                "pending_registry": {
                    "protocol": "prompt-archive-pending-registry-cas-v1",
                    "shard": copy.deepcopy(plan["pending_registry"]["shard"]),
                    "precondition_entry": precondition_entry,
                    "intended_mutation": pending_mutation,
                },
                "journal_generation": generation,
                "previous_journal_generation": (
                    None if previous is None else generation - 1
                ),
                "previous_journal_sha256": (
                    None
                    if previous is None
                    else hashlib.sha256(canonical_control_bytes(previous)).hexdigest()
                ),
                "result_pointer_id": (
                    pointer["pointer_id"] if generation >= 3 else None
                ),
                "result_archive_locator_id": (
                    locator["locator_id"] if generation >= 4 else None
                ),
                "result_latest_slot_id": None,
                "result_latest_slot_ref": None,
                "latest_slot_outcome": (
                    "not_applicable" if generation >= 5 else "pending"
                ),
                "compatibility_result": (
                    "none" if generation >= 5 else "pending"
                ),
                "state": event_state,
                "events": copy.deepcopy(events),
                "updated_at": occurred_at,
            }
        )
        documents[event_state] = document
        previous = document
    return documents[state]


def manifest_semantic_issues(manifest: dict) -> tuple[str, ...]:
    issues: list[str] = []
    if manifest["publication"]["mode"] == "shadow":
        comparison = manifest.get("legacy_comparison") or {}
        integrity = manifest["integrity"]
        if comparison.get("byte_size") != integrity["hydrated_size"]:
            issues.append("legacy comparison size does not match hydrated size")
        if comparison.get("sha256") != integrity["hydrated_sha256"]:
            issues.append("legacy comparison digest does not match hydrated digest")
        if comparison.get("renderer_version") != manifest.get("renderer_version"):
            issues.append("legacy comparison renderer does not match manifest renderer")
    return tuple(issues)


def ref_representation_issues(ref: dict, raw: bytes) -> tuple[str, ...]:
    issues: list[str] = []
    expected = copy.deepcopy(ref)
    bind_ref_to_bytes(expected, raw)
    for key in (
        "digest",
        "content_encoding",
        "encoding_parameters",
        "uncompressed_size",
        "stored_size",
        "stored_sha256",
    ):
        if ref.get(key) != expected.get(key):
            issues.append(f"ref {key} differs from deterministic representation")
    return tuple(issues)


def source_inventory_semantic_issues(inventory: dict) -> tuple[str, ...]:
    expected = "parinv1-" + pas1_id_digest(
        "source-inventory-id",
        [
            ("project_id", inventory["project_id"]),
            ("source_root_handle_id", inventory["source_root_handle_id"]),
            ("source_root_identity_sha256", inventory["source_root_identity_sha256"]),
            ("inventory_sha256", inventory["inventory_sha256"]),
            ("entry_count", str(inventory["entry_count"])),
            ("created_at", inventory["created_at"]),
        ],
    )
    return (() if inventory["inventory_id"] == expected else (
        "source inventory id digest differs from inventory identity",
    ))


def source_metadata_semantic_issues(metadata: dict) -> tuple[str, ...]:
    issues: list[str] = []
    redacted_fields = metadata["redacted_fields"]
    if redacted_fields != sorted(redacted_fields):
        issues.append("source metadata redacted fields are not canonically sorted")
    if metadata["redaction_count"] != len(redacted_fields):
        issues.append("source metadata redaction count differs from redacted fields")
    for field in redacted_fields:
        if metadata.get(field) != "[REDACTED]":
            issues.append(f"source metadata {field} is not wholly redacted")
    return tuple(issues)


def latest_intent_semantic_issues(
    intent: dict,
    *,
    project_id: str | None = None,
    sprint_id: str | None = None,
    response_id: str | None = None,
    occurrence_id: str | None = None,
    operation_id: str | None = None,
    published_at: str | None = None,
    content_kind: str | None = None,
    content_sha256: str | None = None,
    content_size: int | None = None,
) -> tuple[str, ...]:
    issues: list[str] = []
    ordering = intent["ordering"]
    authority = ordering["revision_authority"]
    bootstrap = intent["bootstrap_precondition"]
    descriptor_digest = (
        None if intent["descriptor_ref"] is None else intent["descriptor_ref"]["digest"]
    )
    expected_id = "parlatestintent1-" + pas1_id_digest(
        "latest-content-intent-id",
        [
            ("project_id", intent["project_id"]),
            ("slot_id", intent["slot_id"]),
            ("revision_protocol", authority["protocol"]),
            ("revision_epoch", str(authority["epoch"])),
            ("revision_reset_policy", authority["reset_policy"]),
            ("source_revision", str(ordering["source_revision"])),
            ("occurrence_id", ordering["occurrence_id"]),
            ("operation_id", ordering["operation_id"]),
            ("bootstrap_kind", bootstrap["kind"]),
            ("bootstrap_sha256", bootstrap.get("sha256")),
            (
                "bootstrap_byte_size",
                None
                if bootstrap.get("byte_size") is None
                else str(bootstrap["byte_size"]),
            ),
            ("content_kind", intent["content_kind"]),
            ("content_sha256", intent["content_sha256"]),
            ("content_size", str(intent["content_size"])),
            ("descriptor_digest", descriptor_digest),
            ("source_sprint_id", intent["source_sprint_id"]),
            ("source_response_id", intent["source_response_id"]),
            ("published_at", intent["published_at"]),
        ],
    )
    if intent["intent_id"] != expected_id:
        issues.append("latest intent id digest differs from frozen content intent")
    expected_values = {
        "project_id": (intent["project_id"], project_id),
        "source_sprint_id": (intent["source_sprint_id"], sprint_id),
        "source_response_id": (intent["source_response_id"], response_id),
        "ordering occurrence_id": (ordering["occurrence_id"], occurrence_id),
        "ordering operation_id": (ordering["operation_id"], operation_id),
        "published_at": (intent["published_at"], published_at),
        "content_kind": (intent["content_kind"], content_kind),
        "content_sha256": (intent["content_sha256"], content_sha256),
        "content_size": (intent["content_size"], content_size),
    }
    for label, (actual, expected) in expected_values.items():
        if expected is not None and actual != expected:
            issues.append(f"latest intent {label} differs from enclosing plan")
    return tuple(issues)


def latest_slot_semantic_issues(slot: dict, intent: dict) -> tuple[str, ...]:
    issues: list[str] = []
    copied = (
        "intent_id",
        "project_id",
        "slot_scope",
        "slot_id",
        "publication_protocol",
        "ordering",
        "content_kind",
        "content_sha256",
        "content_size",
        "descriptor_ref",
        "source_sprint_id",
        "source_response_id",
        "published_at",
    )
    for field in copied:
        if slot[field] != intent[field]:
            issues.append(f"latest slot {field} differs from content intent")
    ordering = slot["ordering"]
    authority = ordering["revision_authority"]
    descriptor_digest = (
        None if slot["descriptor_ref"] is None else slot["descriptor_ref"]["digest"]
    )
    expected_id = "parlatest1-" + pas1_id_digest(
        "latest-slot-id",
        [
            ("project_id", slot["project_id"]),
            ("slot_id", slot["slot_id"]),
            ("intent_id", slot["intent_id"]),
            ("revision_protocol", authority["protocol"]),
            ("revision_epoch", str(authority["epoch"])),
            ("revision_reset_policy", authority["reset_policy"]),
            ("source_revision", str(ordering["source_revision"])),
            ("occurrence_id", ordering["occurrence_id"]),
            ("operation_id", ordering["operation_id"]),
            ("generation", str(slot["generation"])),
            ("previous_slot_id", slot["previous_slot_id"]),
            ("content_kind", slot["content_kind"]),
            ("content_sha256", slot["content_sha256"]),
            ("content_size", str(slot["content_size"])),
            ("descriptor_digest", descriptor_digest),
            ("source_sprint_id", slot["source_sprint_id"]),
            ("source_response_id", slot["source_response_id"]),
            ("published_at", slot["published_at"]),
        ],
    )
    if slot["latest_slot_id"] != expected_id:
        issues.append("latest slot id digest differs from slot content")
    return tuple(issues)


def pending_registry_semantic_issues(registry: dict) -> tuple[str, ...]:
    issues: list[str] = []
    entries = registry["entries"]
    if entries != sorted(entries, key=lambda item: item["recovery_key"]):
        issues.append("pending registry entries are not canonically ordered")
    recovery_keys = [entry["recovery_key"] for entry in entries]
    if len(recovery_keys) != len(set(recovery_keys)):
        issues.append("pending registry contains duplicate recovery keys")
    for entry in entries:
        expected_shard = hashlib.sha256(
            entry["recovery_key"].encode("utf-8")
        ).hexdigest()[:2]
        if registry["shard_id"] != expected_shard:
            issues.append("pending registry entry hashes to another shard")
        if entry["updated_at"] < entry["created_at"]:
            issues.append("pending registry entry updated before creation")
    entries_digest = hashlib.sha256(canonical_control_bytes(entries)).hexdigest()
    expected_id = "parpending1-" + pas1_id_digest(
        "pending-operation-registry-id",
        [
            ("project_id", registry["project_id"]),
            ("shard_id", registry["shard_id"]),
            ("generation", str(registry["generation"])),
            ("previous_registry_id", registry["previous_registry_id"]),
            ("entries_sha256", entries_digest),
            ("published_at", registry["published_at"]),
        ],
    )
    if registry["registry_id"] != expected_id:
        issues.append("pending registry id digest differs from registry content")
    return tuple(issues)


def plan_pending_registry_semantic_issues(plan: dict) -> tuple[str, ...]:
    issues: list[str] = []
    pending = plan["pending_registry"]
    shard = pending["shard"]
    mutation = pending["intended_mutation"]
    recovery_key = plan["recovery_locator"]["recovery_key"]
    expected_shard = hashlib.sha256(recovery_key.encode("utf-8")).hexdigest()[:2]
    if shard["project_id"] != plan["scope"]["project_id"]:
        issues.append("pending registry project differs from enclosing plan")
    if shard["shard_id"] != expected_shard:
        issues.append("pending registry shard differs from recovery key")
    if mutation["operation_id"] != plan["operation_id"]:
        issues.append("pending registry operation differs from enclosing plan")
    if mutation["recovery_key"] != recovery_key:
        issues.append("pending registry recovery key differs from enclosing plan")
    planned_at = plan["frozen_timestamps"]["planned_at"]
    for field in ("created_at", "updated_at", "published_at"):
        if mutation[field] != planned_at:
            issues.append(f"pending registry {field} differs from frozen plan")
    return tuple(issues)


def legacy_publication_plan_semantic_issues(plan: dict) -> tuple[str, ...]:
    issues = list(plan_pending_registry_semantic_issues(plan))
    scope = plan["scope"]
    occurrence = plan["occurrence"]
    locator = plan["recovery_locator"]
    for field, expected in {
        "project_id": scope["project_id"],
        "sprint_id": scope["sprint_id"],
        "response_id": occurrence["response_id"],
        "occurrence_id": occurrence["occurrence_id"],
        "operation_id": plan["operation_id"],
    }.items():
        if locator[field] != expected:
            issues.append(f"recovery locator {field} differs from plan")
    expected_recovery_key = "parloc1-" + pas1_id_digest(
        "publication-locator",
        [
            ("project_id", scope["project_id"]),
            ("sprint_id", scope["sprint_id"]),
            ("response_id", occurrence["response_id"]),
            ("occurrence_id", occurrence["occurrence_id"]),
            ("operation_id", plan["operation_id"]),
        ],
    )
    if locator["recovery_key"] != expected_recovery_key:
        issues.append("recovery locator digest differs from plan")
    latest = plan["compatibility_publication"]["latest_intent"]
    legacy = plan["legacy_latest"]
    issues.extend(
        latest_intent_semantic_issues(
            latest,
            project_id=scope["project_id"],
            sprint_id=scope["sprint_id"],
            response_id=occurrence["response_id"],
            occurrence_id=occurrence["occurrence_id"],
            operation_id=plan["operation_id"],
            published_at=plan["frozen_timestamps"]["compatibility_published_at"],
            content_kind="legacy_inline",
            content_sha256=legacy["content_sha256"],
            content_size=legacy["content_size"],
        )
    )
    if legacy["authoritative_drain_barrier"]["observed_at"] != plan[
        "frozen_timestamps"
    ]["planned_at"]:
        issues.append("legacy drain observation differs from frozen plan")
    return tuple(issues)


def publication_plan_semantic_issues(plan: dict) -> tuple[str, ...]:
    if plan["operation_kind"] == "legacy_latest_publish":
        return legacy_publication_plan_semantic_issues(plan)
    issues: list[str] = []
    issues.extend(plan_pending_registry_semantic_issues(plan))
    manifest = plan["intended_manifest"]["document"]
    manifest_bytes = canonical_control_bytes(manifest)
    manifest_ref = plan["intended_manifest"]["manifest_ref"]
    if plan["intended_manifest"]["archive_id"] != manifest["archive_id"]:
        issues.append("plan archive id differs from intended manifest")
    for issue in ref_representation_issues(manifest_ref, manifest_bytes):
        issues.append("manifest " + issue)
    scope = plan["scope"]
    occurrence = plan["occurrence"]
    manifest_scope = manifest["scope"]
    for key in ("project_id", "sprint_id"):
        if scope[key] != manifest_scope[key]:
            issues.append(f"plan {key} differs from intended manifest")
    for key in (
        "response_id",
        "response_kind",
        "occurrence_kind",
        "occurrence_id",
        "transition_id",
    ):
        if occurrence[key] != manifest_scope[key]:
            issues.append(f"plan {key} differs from intended manifest")
    locator = plan["recovery_locator"]
    expected_locator = {
        "project_id": scope["project_id"],
        "sprint_id": scope["sprint_id"],
        "response_id": occurrence["response_id"],
        "occurrence_id": occurrence["occurrence_id"],
        "operation_id": plan["operation_id"],
    }
    for key, expected in expected_locator.items():
        if locator[key] != expected:
            issues.append(f"recovery locator {key} differs from plan")
    expected_locator_digest = pas1_id_digest(
        "publication-locator",
        [
            ("project_id", scope["project_id"]),
            ("sprint_id", scope["sprint_id"]),
            ("response_id", occurrence["response_id"]),
            ("occurrence_id", occurrence["occurrence_id"]),
            ("operation_id", plan["operation_id"]),
        ],
    )
    if locator["recovery_key"] != "parloc1-" + expected_locator_digest:
        issues.append("recovery locator digest differs from plan")
    source = plan["source_locator"]
    if source["locator_kind"] == "runtime_occurrence":
        if source["runtime_occurrence_id"] != occurrence["occurrence_id"]:
            issues.append("runtime source occurrence differs from plan")
    else:
        inventory = source_inventory_fixture()
        inventory_ref = source["inventory_ref"]
        for field in (
            "inventory_id",
            "inventory_format",
            "inventory_sha256",
            "entry_count",
        ):
            if source[field] != inventory[field]:
                issues.append(f"source locator {field} differs from inventory")
        for issue in ref_representation_issues(
            inventory_ref, canonical_control_bytes(inventory)
        ):
            issues.append("source inventory " + issue)
    pointer = plan["intended_pointer"]
    pointer_document = pointer["document"]
    for issue in ref_representation_issues(
        pointer["pointer_ref"], canonical_control_bytes(pointer_document)
    ):
        issues.append("pointer " + issue)
    if pointer["pointer_id"] != pointer_document["pointer_id"]:
        issues.append("pointer id differs from intended pointer document")
    expected_pointer_id = "parptr1-" + pas1_id_digest(
        "pointer-id",
        [
            ("project_id", pointer_document["project_id"]),
            ("sprint_id", pointer_document["sprint_id"]),
            ("response_id", pointer_document["response_id"]),
            ("generation", str(pointer_document["generation"])),
            ("archive_id", pointer_document["archive_id"]),
            ("manifest_sha256", pointer_document["manifest_ref"]["digest"]),
            ("previous_pointer_id", pointer_document["previous_pointer_id"]),
            ("published_at", pointer_document["published_at"]),
        ],
    )
    if pointer_document["pointer_id"] != expected_pointer_id:
        issues.append("pointer id digest differs from intended pointer document")
    if pointer_document["manifest_ref"] != manifest_ref:
        issues.append("pointer manifest ref differs from intended manifest")
    for key, expected in {
        "project_id": scope["project_id"],
        "sprint_id": scope["sprint_id"],
        "response_id": occurrence["response_id"],
        "archive_id": manifest["archive_id"],
    }.items():
        if pointer_document[key] != expected:
            issues.append(f"pointer {key} differs from plan")
    precondition = plan["pointer_precondition"]
    if pointer_document["generation"] != precondition["expected_generation"] + 1:
        issues.append("pointer generation differs from precondition")
    if pointer_document["previous_pointer_id"] != precondition["previous_pointer_id"]:
        issues.append("pointer predecessor differs from precondition")
    archive_locator = plan["intended_archive_locator"]
    locator_document = archive_locator["document"]
    for issue in ref_representation_issues(
        archive_locator["locator_ref"], canonical_control_bytes(locator_document)
    ):
        issues.append("archive locator " + issue)
    if archive_locator["locator_id"] != locator_document["locator_id"]:
        issues.append("archive locator id differs from intended locator document")
    if locator_document["pointer_ref"] != pointer["pointer_ref"]:
        issues.append("archive locator pointer ref differs from plan")
    if locator_document["manifest_ref"] != manifest_ref:
        issues.append("archive locator manifest ref differs from plan")
    expected_locator_id = "pararc1-" + pas1_id_digest(
        "archive-locator-id",
        [
            ("project_id", locator_document["project_id"]),
            ("sprint_id", locator_document["sprint_id"]),
            ("response_id", locator_document["response_id"]),
            ("archive_id", locator_document["archive_id"]),
            ("pointer_id", locator_document["pointer_id"]),
            ("manifest_sha256", locator_document["manifest_ref"]["digest"]),
            ("published_at", locator_document["published_at"]),
        ],
    )
    if locator_document["locator_id"] != expected_locator_id:
        issues.append("archive locator id digest differs from intended document")
    for key, expected in {
        "project_id": scope["project_id"],
        "sprint_id": scope["sprint_id"],
        "response_id": occurrence["response_id"],
        "archive_id": manifest["archive_id"],
        "pointer_id": pointer["pointer_id"],
    }.items():
        if locator_document[key] != expected:
            issues.append(f"archive locator {key} differs from plan")
    timestamps = plan["frozen_timestamps"]
    if pointer_document["published_at"] != timestamps["pointer_published_at"]:
        issues.append("pointer timestamp differs from frozen plan")
    if locator_document["published_at"] != timestamps["locator_published_at"]:
        issues.append("archive locator timestamp differs from frozen plan")
    return tuple(issues)


def journal_pending_registry_semantic_issues(
    journal: dict,
    plan: dict,
    previous_generation: dict | None,
) -> tuple[str, ...]:
    issues: list[str] = []
    pending = journal["pending_registry"]
    if pending["shard"] != plan["pending_registry"]["shard"]:
        issues.append("journal pending registry shard differs from plan")
    entry = pending["precondition_entry"]
    if entry["operation_id"] != journal["operation_id"]:
        issues.append("journal pending entry operation differs from journal")
    if entry["recovery_key"] != journal["recovery_locator"]["recovery_key"]:
        issues.append("journal pending entry recovery key differs from journal")
    if entry["plan_ref"] != journal["plan_ref"]:
        issues.append("journal pending entry plan ref differs from journal")
    planned_at = plan["frozen_timestamps"]["planned_at"]
    if entry["created_at"] != planned_at:
        issues.append("journal pending entry creation differs from frozen plan")
    if previous_generation is None:
        if entry["state"] != "planned" or entry["journal_ref"] is not None:
            issues.append("journal intent does not consume planned registry entry")
        if entry["updated_at"] != planned_at:
            issues.append("journal planned entry update differs from frozen plan")
    else:
        if entry["state"] != "journaled" or entry["journal_ref"] is None:
            issues.append("journal successor does not consume journaled registry entry")
        else:
            for issue in ref_representation_issues(
                entry["journal_ref"], canonical_control_bytes(previous_generation)
            ):
                issues.append("journal pending predecessor " + issue)
        if entry["updated_at"] != previous_generation["updated_at"]:
            issues.append("journal pending predecessor timestamp differs")
    mutation = pending["intended_mutation"]
    if journal["state"] in {"complete", "aborted"}:
        if mutation.get("mutation") != "remove_terminal_entry":
            issues.append("terminal journal does not remove pending entry")
        if mutation.get("terminal_state") != journal["state"]:
            issues.append("pending removal terminal state differs from journal")
        if mutation.get("published_at") != journal["updated_at"]:
            issues.append("pending removal timestamp differs from journal")
    else:
        if mutation.get("mutation") != "bind_current_journal":
            issues.append("nonterminal journal does not bind current journal")
        for field in ("updated_at", "published_at"):
            if mutation.get(field) != journal["updated_at"]:
                issues.append(f"pending journal bind {field} differs from journal")
    return tuple(issues)


def legacy_journal_semantic_issues(
    journal: dict,
    plan: dict,
    previous_generation: dict | None = None,
) -> tuple[str, ...]:
    issues: list[str] = []
    for issue in ref_representation_issues(
        journal["plan_ref"], canonical_control_bytes(plan)
    ):
        issues.append("journal plan " + issue)
    for field in ("operation_id", "operation_kind", "scope", "recovery_locator"):
        if journal[field] != plan[field]:
            issues.append(f"journal {field} differs from plan")
    expected_occurrence = {
        "response_id": plan["occurrence"]["response_id"],
        "occurrence_id": plan["occurrence"]["occurrence_id"],
    }
    if journal["occurrence"] != expected_occurrence:
        issues.append("journal occurrence differs from plan")
    compatibility = plan["compatibility_publication"]
    if journal["compatibility_mode"] != compatibility["mode"]:
        issues.append("journal compatibility mode differs from plan")
    if journal["latest_intent"] != compatibility["latest_intent"]:
        issues.append("journal latest intent differs from plan")
    legacy = plan["legacy_latest"]
    for journal_field, plan_field in (
        ("legacy_payload_root", "payload_root"),
        ("legacy_content_sha256", "content_sha256"),
        ("legacy_content_size", "content_size"),
        ("authoritative_drain_barrier", "authoritative_drain_barrier"),
    ):
        if journal[journal_field] != legacy[plan_field]:
            issues.append(f"journal {journal_field} differs from plan")
    if journal["frozen_timestamps"] != plan["frozen_timestamps"]:
        issues.append("journal frozen timestamps differ from plan")
    issues.extend(
        journal_pending_registry_semantic_issues(journal, plan, previous_generation)
    )
    if previous_generation is not None:
        if journal["previous_journal_generation"] != previous_generation[
            "journal_generation"
        ]:
            issues.append("journal predecessor generation differs from retained record")
        expected_sha = hashlib.sha256(
            canonical_control_bytes(previous_generation)
        ).hexdigest()
        if journal["previous_journal_sha256"] != expected_sha:
            issues.append("journal predecessor digest differs from retained record")
    expected_id = "parjournal1-" + pas1_id_digest(
        "publication-journal",
        [
            ("project_id", journal["scope"]["project_id"]),
            ("sprint_id", journal["scope"]["sprint_id"]),
            ("response_id", journal["occurrence"]["response_id"]),
            ("operation_id", journal["operation_id"]),
        ],
    )
    if journal["journal_id"] != expected_id:
        issues.append("journal id digest differs from journal identity")
    return tuple(issues)


def journal_semantic_issues(
    journal: dict,
    plan: dict,
    previous_generation: dict | None = None,
) -> tuple[str, ...]:
    if journal["operation_kind"] == "legacy_latest_publish":
        return legacy_journal_semantic_issues(journal, plan, previous_generation)
    issues: list[str] = []
    plan_bytes = canonical_control_bytes(plan)
    plan_ref = journal["plan_ref"]
    for issue in ref_representation_issues(plan_ref, plan_bytes):
        issues.append("journal plan " + issue)
    if journal["operation_id"] != plan["operation_id"]:
        issues.append("journal operation id differs from plan")
    if journal["operation_kind"] != plan["operation_kind"]:
        issues.append("journal operation kind differs from plan")
    if journal["scope"] != plan["scope"]:
        issues.append("journal scope differs from plan")
    expected_occurrence = {
        "response_id": plan["occurrence"]["response_id"],
        "occurrence_id": plan["occurrence"]["occurrence_id"],
    }
    if journal["occurrence"] != expected_occurrence:
        issues.append("journal occurrence differs from plan")
    if journal["recovery_locator"] != plan["recovery_locator"]:
        issues.append("journal recovery locator differs from plan")
    intended = plan["intended_manifest"]
    if journal["archive_id"] != intended["archive_id"]:
        issues.append("journal archive id differs from plan")
    if journal["manifest_ref"] != intended["manifest_ref"]:
        issues.append("journal manifest ref differs from plan")
    pointer = plan["intended_pointer"]
    locator = plan["intended_archive_locator"]
    if journal["pointer_ref"] != pointer["pointer_ref"]:
        issues.append("journal pointer ref differs from plan")
    if journal["archive_locator_ref"] != locator["locator_ref"]:
        issues.append("journal archive locator ref differs from plan")
    precondition = plan["pointer_precondition"]
    if journal["expected_pointer_generation"] != precondition["expected_generation"]:
        issues.append("journal pointer generation precondition differs from plan")
    if journal["previous_pointer_id"] != precondition["previous_pointer_id"]:
        issues.append("journal pointer predecessor differs from plan")
    compatibility = plan["compatibility_publication"]
    if journal["compatibility_mode"] != compatibility["mode"]:
        issues.append("journal compatibility mode differs from plan")
    expected_descriptor_ref = (
        compatibility.get("descriptor", {}).get("descriptor_ref")
        if compatibility["mode"] != "none"
        else None
    )
    if journal["compatibility_descriptor_ref"] != expected_descriptor_ref:
        issues.append("journal compatibility descriptor differs from plan")
    if journal["frozen_timestamps"] != plan["frozen_timestamps"]:
        issues.append("journal frozen timestamps differ from plan")
    expected_latest_intent = compatibility.get("latest_intent")
    if journal["latest_intent"] != expected_latest_intent:
        issues.append("journal latest intent differs from plan")
    issues.extend(
        journal_pending_registry_semantic_issues(journal, plan, previous_generation)
    )
    if previous_generation is not None:
        if journal["previous_journal_generation"] != previous_generation[
            "journal_generation"
        ]:
            issues.append("journal predecessor generation differs from retained record")
        expected_previous_sha = hashlib.sha256(
            canonical_control_bytes(previous_generation)
        ).hexdigest()
        if journal["previous_journal_sha256"] != expected_previous_sha:
            issues.append("journal predecessor digest differs from retained record")
    expected_journal_digest = pas1_id_digest(
        "publication-journal",
        [
            ("project_id", journal["scope"]["project_id"]),
            ("sprint_id", journal["scope"]["sprint_id"]),
            ("response_id", journal["occurrence"]["response_id"]),
            ("operation_id", journal["operation_id"]),
        ],
    )
    if journal["journal_id"] != "parjournal1-" + expected_journal_digest:
        issues.append("journal id digest differs from journal identity")
    return tuple(issues)


def canonical_base64_issues(value: str) -> tuple[str, ...]:
    try:
        decoded = base64.b64decode(value, validate=True)
    except (ValueError, binascii.Error):
        return ("invalid base64",)
    issues: list[str] = []
    if base64.b64encode(decoded).decode("ascii") != value:
        issues.append("base64 is not canonical RFC 4648")
    if len(decoded) > 4096:
        issues.append("base64 framing exceeds 4096 decoded bytes")
    return tuple(issues)


SLOT_ROOTS = {
    "project_state.recent_activity": "recent_activity",
    "project_state.history_with_patches.text": "history_text",
    "project_state.history_with_patches.timeline": "history_timeline",
    "project_state.activity_with_patches": "activity_with_patches",
    "project_state.history_with_patches.patches": "history_patches",
    "project_state.code_patches": "code_patches",
    "work_history": "work_history",
    "assignment.execution_audit.checkpoints": "execution_audit",
}


def render_plan_semantic_issues(
    plan: dict,
    manifest: dict,
    reachable_digests: set[str],
) -> tuple[str, ...]:
    issues: list[str] = []
    scope = manifest["scope"]
    if plan["response_id"] != scope["response_id"]:
        issues.append("render plan response id differs from manifest")
    if plan["response_kind"] != scope["response_kind"]:
        issues.append("render plan response kind differs from manifest")
    if plan["renderer_version"] != manifest["renderer_version"]:
        issues.append("render plan renderer differs from manifest")
    if plan["json_value_model"] != manifest["json_value_model"]:
        issues.append("render plan JSON value model differs from manifest")
    if plan["source_byte_size"] != manifest["integrity"]["hydrated_size"]:
        issues.append("render plan source size differs from manifest")
    if plan["source_sha256"] != manifest["integrity"]["hydrated_sha256"]:
        issues.append("render plan source digest differs from manifest")
    plan_bytes = canonical_control_bytes(plan)
    envelope = manifest["envelope"]
    if envelope["byte_size"] != len(plan_bytes):
        issues.append("render plan byte size differs from manifest")
    if envelope["sha256"] != hashlib.sha256(plan_bytes).hexdigest():
        issues.append("render plan digest differs from manifest")
    required_digests = {envelope["chunks_root"]["digest"]}
    required_digests.update(ref["digest"] for ref in manifest["roots"].values())
    used_roots: set[str] = set()
    literal_total = 0
    seen_slots: set[str] = set()
    previous_source_end = 0
    previous_value_ordinal = -1
    for step in plan["steps"]:
        if step["source_start"] != previous_source_end:
            issues.append("render plan source spans are not contiguous")
        if step["source_end"] <= step["source_start"]:
            issues.append("render plan source span is empty or reversed")
        previous_source_end = step["source_end"]
        if step["step_kind"] == "literal":
            literal_total += step["byte_size"]
            if step["source_end"] - step["source_start"] != step["byte_size"]:
                issues.append("render literal source span differs from byte size")
            required_digests.add(step["chunks_root"]["digest"])
            continue
        if step["step_kind"] == "current_field":
            used_roots.add("current_fields")
            if step["value_ordinal"] <= previous_value_ordinal:
                issues.append("current field ordinals are not strictly increasing")
            previous_value_ordinal = step["value_ordinal"]
            required_digests.add(step["record_ref"]["digest"])
            continue
        slot = step["slot_id"]
        if slot in seen_slots:
            issues.append("render plan repeats a frozen slot")
        seen_slots.add(slot)
        root_name = SLOT_ROOTS[slot]
        used_roots.add(root_name)
        expected_root = manifest["roots"].get(root_name)
        if expected_root != step["index_root"]:
            issues.append(f"render slot {slot} root differs from manifest")
        required_digests.add(step["index_root"]["digest"])
        for field in ("prefix_b64", "separator_b64", "suffix_b64"):
            for issue in canonical_base64_issues(step[field]):
                issues.append(f"render slot {slot} {field}: {issue}")
    if previous_source_end != plan["source_byte_size"]:
        issues.append("render plan source spans do not cover the source")
    if literal_total > 8 * 1024 * 1024:
        issues.append("render plan literal total exceeds 8388608 bytes")
    if used_roots != set(manifest["roots"]):
        issues.append("render plan does not consume every manifest root exactly by slot")
    if not required_digests.issubset(reachable_digests):
        issues.append("render plan or manifest root is absent from reachable graph")
    return tuple(issues)


def reference_graph_semantic_issues(
    roots: list[tuple[dict, str]],
    objects: dict[str, dict],
    *,
    max_depth: int,
    max_unique_refs: int,
) -> tuple[tuple[str, ...], int]:
    issues: list[str] = []
    active: set[str] = set()
    complete: set[str] = set()
    unique: set[str] = set()

    def visit(ref: dict, expected_sequence: str, depth: int) -> None:
        digest = ref["digest"]
        unique.add(digest)
        if len(unique) > max_unique_refs:
            issues.append("unique reference count exceeds limit")
        if depth > max_depth:
            issues.append("reference depth exceeds limit")
            return
        if digest in active:
            issues.append("active-ancestor reference cycle")
            return
        if digest in complete:
            return
        node = objects.get(digest)
        if node is None:
            issues.append("referenced object missing")
            return
        if ref["logical_kind"] != "index_node":
            complete.add(digest)
            return
        if node["sequence_kind"] != expected_sequence:
            issues.append("index sequence kind differs from parent branch")
        active.add(digest)
        for child in node["children"]:
            child_ref = child["ref"]
            if node["level"] > 0:
                child_node = objects.get(child_ref["digest"])
                if child_ref["logical_kind"] != "index_node" or child_node is None:
                    issues.append("internal index child is not an index node")
                else:
                    if child_node["level"] != node["level"] - 1:
                        issues.append("child index level is not parent level minus one")
                    if child_node["sequence_kind"] != node["sequence_kind"]:
                        issues.append("child sequence kind differs from parent branch")
            elif child_ref["logical_kind"] == "index_node":
                issues.append("leaf index contains another index node")
            visit(child_ref, expected_sequence, depth + 1)
        active.remove(digest)
        complete.add(digest)

    for root, sequence_kind in roots:
        visit(root, sequence_kind, 1)
    return tuple(issues), len(unique)


def descriptor_semantic_issues(descriptor: dict) -> tuple[str, ...]:
    scope = descriptor["scope"]
    if descriptor["route_context"] == "local_operator":
        base = (
            f"/api/v1/projects/{scope['project_id']}/sprints/{scope['sprint_id']}/"
            f"prompt-archives/{descriptor['archive_id']}"
        )
    else:
        base = (
            f"/api/v1/projects/{scope['project_id']}/agents/"
            f"{descriptor['agent_phone']}/sprints/{scope['sprint_id']}/"
            f"prompt-archives/{descriptor['archive_id']}"
        )
    expected = {
        "descriptor_url": base,
        "manifest_url": base + "/manifest",
        "content_url": base + "/content",
    }
    return tuple(
        f"{name} differs from descriptor identity"
        for name, value in expected.items()
        if descriptor[name] != value
    )


class PromptArchiveContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.schemas = {
            name: json.loads((SCHEMA_ROOT / name).read_text(encoding="utf-8"))
            for name in SCHEMA_NAMES
        }
        registry = Registry()
        for schema in cls.schemas.values():
            registry = registry.with_resource(
                schema["$id"], Resource.from_contents(schema)
            )
        cls.registry = registry
        cls.schema_by_id = {
            schema["$id"]: schema for schema in cls.schemas.values()
        }
        cls.contract = CONTRACT_PATH.read_text(encoding="utf-8")

    def validator(self, name: str) -> Draft202012Validator:
        return Draft202012Validator(
            self.schemas[name],
            registry=self.registry,
            format_checker=FormatChecker(),
        )

    def assert_invalid(self, name: str, value: dict) -> None:
        with self.assertRaises(ValidationError):
            self.validator(name).validate(value)

    def test_pas1_id_and_control_encoding_golden_vectors(self) -> None:
        response_id = "parresp1-" + "1" * 64
        archive_id = "par1-" + "2" * 64
        vectors = {
            "project-storage-key": (
                [("project_id", "9000")],
                "d23f4ba66de9f6b13715a9dedafdbc65fe54c2fd121bbc788bf671745ab42531",
            ),
            "sprint-storage-key": (
                [("project_id", "9000"), ("sprint_id", "sprint-example-1")],
                "52c984a54d25d125de23576c14d0d99f038af5c35cba1b3f2d835f341b3f2dff",
            ),
            "sprint-storage-null": (
                [("project_id", "9000"), ("sprint_id", None)],
                "f1b65187a5e2e5fefed336defbf94faa63fdb4c572ed69d3933cf8534dbe4ef7",
            ),
            "response-id": (
                [
                    ("project_id", "9000"),
                    ("sprint_id", "sprint-example-1"),
                    ("response_kind", "transition-response"),
                    ("occurrence_kind", "transition"),
                    ("occurrence_id", "transition-example-1"),
                ],
                "7dec908e702d975e49a84abbb17140b876a0b70de06a78fd745751030abb25a8",
            ),
            "record-id": (
                [
                    ("project_id", "9000"),
                    ("sprint_id", "sprint-example-1"),
                    ("record_type", "event"),
                    ("origin_response_id", response_id),
                    ("origin_transition_id", "transition-example-1"),
                    ("ordinal", "0"),
                    ("payload_size", "128"),
                    ("payload_sha256", SHA_A),
                    ("payload_root_digest", SHA_B),
                ],
                "82e6999cb68ddd46f7aad412f35ed80852341bb6149bcf8be5d143ca92a37133",
            ),
            "archive-id": (
                [
                    ("archive_format", "incremental_transition_v1"),
                    ("project_id", "9000"),
                    ("sprint_id", "sprint-example-1"),
                    ("response_id", response_id),
                    ("envelope_sha256", "e" * 64),
                    ("raw_bytes_root_digest", None),
                    ("current_fields_root_digest", "2" * 64),
                    ("recent_activity_root_digest", "3" * 64),
                    ("history_text_root_digest", None),
                    ("history_timeline_root_digest", None),
                    ("activity_with_patches_root_digest", None),
                    ("history_patches_root_digest", None),
                    ("code_patches_root_digest", None),
                    ("work_history_root_digest", None),
                    ("review_context_root_digest", None),
                    ("results_root_digest", None),
                    ("feedback_root_digest", None),
                    ("execution_audit_root_digest", None),
                    ("source_metadata_root_digest", None),
                    ("hydrated_size", "4096"),
                    ("hydrated_sha256", SHA_C),
                ],
                "68adb2078c8a76895b65a9a3c0799bcd8148c3c9b657aeef724584c019b60b6f",
            ),
            "pointer-id": (
                [
                    ("project_id", "9000"),
                    ("sprint_id", "sprint-example-1"),
                    ("response_id", response_id),
                    ("generation", "1"),
                    ("archive_id", archive_id),
                    ("manifest_sha256", "d" * 64),
                    ("previous_pointer_id", None),
                    ("published_at", "2026-10-11T11:31:00Z"),
                ],
                "e5d9c58e7327d00a10397724f920c9e75fe2e4e2c8a0506db9761e1d2d435649",
            ),
            "archive-locator-id": (
                [
                    ("project_id", "9000"),
                    ("sprint_id", "sprint-example-1"),
                    ("response_id", response_id),
                    ("archive_id", archive_id),
                    (
                        "pointer_id",
                        "parptr1-e5d9c58e7327d00a10397724f920c9e75fe2e4e2c8a0506db9761e1d2d435649",
                    ),
                    ("manifest_sha256", "d" * 64),
                    ("published_at", "2026-10-11T11:31:00Z"),
                ],
                "2774305b7b2edcec5015264b8c0ff67f3630b66232fa41986618b3ab143c99f4",
            ),
            "descriptor-stub-id": (
                [
                    ("project_id", "9000"),
                    ("sprint_id", "sprint-example-1"),
                    ("response_id", response_id),
                    ("archive_id", archive_id),
                    (
                        "pointer_id",
                        "parptr1-e5d9c58e7327d00a10397724f920c9e75fe2e4e2c8a0506db9761e1d2d435649",
                    ),
                    ("manifest_sha256", "d" * 64),
                    ("created_at", "2026-10-11T11:29:00Z"),
                    ("published_at", "2026-10-11T11:31:00Z"),
                ],
                "337b0a743457de538ba51140e248f3efebcdc8cdd4890a47e4d967ee4d877714",
            ),
            "latest-content-intent-id": (
                [
                    ("project_id", "9000"),
                    ("slot_id", "latest-response"),
                    (
                        "revision_protocol",
                        "prompt-archive-durable-project-latest-revision-v1",
                    ),
                    ("revision_epoch", "1"),
                    (
                        "revision_reset_policy",
                        "restore_high_water_mark_or_fail_closed",
                    ),
                    ("source_revision", "42"),
                    ("occurrence_id", "transition-example-1"),
                    ("operation_id", "parop1-" + "0" * 32),
                    ("bootstrap_kind", "absent"),
                    ("bootstrap_sha256", None),
                    ("bootstrap_byte_size", None),
                    ("content_kind", "descriptor_stub"),
                    ("content_sha256", SHA_A),
                    ("content_size", "512"),
                    ("descriptor_digest", SHA_B),
                    ("source_sprint_id", "sprint-example-1"),
                    ("source_response_id", response_id),
                    ("published_at", "2026-10-11T11:31:00Z"),
                ],
                "a930dd73e2a2dccc3e8d0e3e37c5c04272b6e729fa847efd738b11a2c658e734",
            ),
            "latest-slot-id": (
                [
                    ("project_id", "9000"),
                    ("slot_id", "latest-response"),
                    (
                        "intent_id",
                        "parlatestintent1-a930dd73e2a2dccc3e8d0e3e37c5c04272b6e729fa847efd738b11a2c658e734",
                    ),
                    (
                        "revision_protocol",
                        "prompt-archive-durable-project-latest-revision-v1",
                    ),
                    ("revision_epoch", "1"),
                    (
                        "revision_reset_policy",
                        "restore_high_water_mark_or_fail_closed",
                    ),
                    ("source_revision", "42"),
                    ("occurrence_id", "transition-example-1"),
                    ("operation_id", "parop1-" + "0" * 32),
                    ("generation", "1"),
                    ("previous_slot_id", None),
                    ("content_kind", "descriptor_stub"),
                    ("content_sha256", SHA_A),
                    ("content_size", "512"),
                    ("descriptor_digest", SHA_B),
                    ("source_sprint_id", "sprint-example-1"),
                    ("source_response_id", response_id),
                    ("published_at", "2026-10-11T11:31:00Z"),
                ],
                "344fa50c4d3efd72cd2561c8b9ed845ca4a1d53125f6a9fe1b8e90d8a80ace09",
            ),
            "source-inventory-id": (
                [
                    ("project_id", "9000"),
                    ("source_root_handle_id", "isolated-root-1"),
                    ("source_root_identity_sha256", SHA_A),
                    ("inventory_sha256", SHA_B),
                    ("entry_count", "2"),
                    ("created_at", "2026-10-11T11:20:00Z"),
                ],
                "f869634a4debc8433f0b7b5c33bd8a6642cc0761d0b426a6bc4aae4a764bf69f",
            ),
            "pending-operation-registry-id": (
                [
                    ("project_id", "9000"),
                    ("shard_id", "ab"),
                    ("generation", "1"),
                    ("previous_registry_id", None),
                    ("entries_sha256", SHA_C),
                    ("published_at", "2026-10-11T11:27:00Z"),
                ],
                "6395e842a7492bb36f6e44a7ac5e0ab9653e09f6484d04a961d94e02c482277a",
            ),
            "publication-locator": (
                [
                    ("project_id", "9000"),
                    ("sprint_id", "sprint-example-1"),
                    ("response_id", response_id),
                    ("occurrence_id", "transition-example-1"),
                    ("operation_id", "parop1-" + "4" * 32),
                ],
                "c7aad3e0d806c2f97d5249fbdde565849d66d9696508ba5f6c67a41f93585481",
            ),
            "publication-journal": (
                [
                    ("project_id", "9000"),
                    ("sprint_id", "sprint-example-1"),
                    ("response_id", response_id),
                    ("operation_id", "parop1-" + "4" * 32),
                ],
                "c291f580c6c8db505d2538e3cca8f0f308be74ed9078ab6d0cdfa3c4293ec2a2",
            ),
        }
        for domain, (fields, expected) in vectors.items():
            with self.subTest(domain=domain):
                id_domain = (
                    "sprint-storage-key"
                    if domain == "sprint-storage-null"
                    else domain
                )
                self.assertEqual(pas1_id_digest(id_domain, fields), expected)

        canonical = (
            '{"a":[true,null,-2,"\\u000a"],"b":"Привет"}'.encode("utf-8")
        )
        self.assertEqual(
            hashlib.sha256(canonical).hexdigest(),
            "8f5516fc3f959f64d4077f58bbb4f9a2b97c14d072ae3373c46942b52b4aea10",
        )

    def test_schemas_meta_validate_refs_resolve_and_examples_validate(self) -> None:
        for name, schema in self.schemas.items():
            with self.subTest(schema=name):
                Draft202012Validator.check_schema(schema)
                for node in self._walk(schema):
                    if not isinstance(node, dict) or "$ref" not in node:
                        continue
                    resource_uri, _ = urldefrag(urljoin(schema["$id"], node["$ref"]))
                    self.assertIn(resource_uri, self.schema_by_id)

        examples = {
            "prompt-archive-ref-v1.schema.json": blob_ref(),
            "prompt-archive-blob-v1.schema.json": blob_fixture(),
            "prompt-archive-record-v1.schema.json": record_fixture(),
            "prompt-archive-index-node-v1.schema.json": index_fixture(),
            "prompt-archive-json-edge-v1.schema.json": json_edge_fixture(),
            "prompt-archive-json-value-v1.schema.json": json_value_fixture(),
            "prompt-archive-envelope-v1.schema.json": envelope_fixture(),
            "prompt-archive-manifest-v1.schema.json": manifest_fixture(),
            "prompt-archive-pointer-v1.schema.json": pointer_fixture(),
            "prompt-archive-locator-v1.schema.json": archive_locator_fixture(),
            "prompt-archive-publication-plan-v1.schema.json": (
                publication_plan_fixture()
            ),
            "prompt-archive-journal-v1.schema.json": journal_fixture(),
            "prompt-archive-latest-slot-v1.schema.json": latest_slot_fixture(),
            "prompt-archive-pending-operation-registry-v1.schema.json": (
                pending_registry_example_fixture()
            ),
            "prompt-archive-source-inventory-v1.schema.json": (
                source_inventory_fixture()
            ),
            "prompt-archive-source-metadata-v1.schema.json": (
                source_metadata_fixture()
            ),
            "prompt-archive-descriptor-stub-v1.schema.json": (
                descriptor_stub_fixture()
            ),
            "prompt-archive-descriptor-v1.schema.json": descriptor_fixture(),
            "prompt-archive-error-v1.schema.json": error_fixture(),
        }
        self.assertEqual(set(examples), SCHEMA_NAMES)
        for name, value in examples.items():
            with self.subTest(instance=name):
                self.validator(name).validate(value)
        self.validator("prompt-archive-manifest-v1.schema.json").validate(
            manifest_fixture(native=True)
        )

    @staticmethod
    def _walk(value):
        yield value
        if isinstance(value, dict):
            for child in value.values():
                yield from PromptArchiveContractTests._walk(child)
        elif isinstance(value, list):
            for child in value:
                yield from PromptArchiveContractTests._walk(child)

    def test_reference_is_closed_versioned_bounded_and_path_free(self) -> None:
        validator_name = "prompt-archive-ref-v1.schema.json"
        validator = self.validator(validator_name)
        validator.validate(blob_ref())
        validator.validate(blob_ref(gzip=True))

        for field, value in (
            ("schema_version", 2),
            ("hash_algorithm", "sha512"),
            ("digest", "A" * 64),
            ("digest", "a" * 63),
            ("content_encoding", "zstd"),
            ("uncompressed_size", 16 * 1024 * 1024 + 1),
        ):
            candidate = blob_ref()
            candidate[field] = value
            with self.subTest(field=field, value=value):
                self.assert_invalid(validator_name, candidate)

        for forbidden in ("path", "url", "token", "credential", "$cas_ref"):
            candidate = blob_ref()
            candidate[forbidden] = "must-not-cross-the-boundary"
            with self.subTest(forbidden=forbidden):
                self.assert_invalid(validator_name, candidate)

        gzip_without_parameters = blob_ref(gzip=True)
        gzip_without_parameters.pop("encoding_parameters")
        self.assert_invalid(validator_name, gzip_without_parameters)

        identity_with_parameters = blob_ref()
        identity_with_parameters["encoding_parameters"] = (
            "gzip-rfc1952-zlib-1.2.13-level9-v1"
        )
        self.assert_invalid(validator_name, identity_with_parameters)

        gzip_below_threshold = blob_ref(gzip=True)
        gzip_below_threshold["uncompressed_size"] = 1023
        self.assert_invalid(validator_name, gzip_below_threshold)

        identity_at_threshold = blob_ref()
        identity_at_threshold["uncompressed_size"] = 1024
        self.assert_invalid(validator_name, identity_at_threshold)

        crlf_media = blob_ref()
        crlf_media["media_type"] = "application/json\r\nX-Unsafe: yes"
        self.assert_invalid(validator_name, crlf_media)

    def test_frozen_gzip_codec_vectors_and_selection_boundary(self) -> None:
        vectors = (
            (
                b"",
                "1f8b08000000000002ff03000000000000000000",
                "9ceffb7310338057cfe71a4ae1e2c98d2c485d81cdef906532a801f457a38d64",
            ),
            (
                b"hello",
                "1f8b08000000000002ffcb48cdc9c9070086a6103605000000",
                "3223ab15154bd181c783e5b434b108c68f5798bf10be7eb681faba9b6668bc65",
            ),
            (
                bytes(1024),
                "1f8b08000000000002ff63601805a360148c5400002eafb5ef00040000",
                "a1b1b81d3d1afdb8fe119b002318c12c20934713b9754a40f702adb18a2540b9",
            ),
        )
        self.assertEqual(zlib.ZLIB_RUNTIME_VERSION, "1.2.13")
        for raw, expected_hex, expected_sha in vectors:
            with self.subTest(size=len(raw)):
                stored = frozen_gzip_bytes(raw)
                self.assertEqual(stored.hex(), expected_hex)
                self.assertEqual(hashlib.sha256(stored).hexdigest(), expected_sha)
        self.assertEqual("identity" if len(bytes(1023)) < 1024 else "gzip", "identity")
        self.assertEqual("identity" if len(bytes(1024)) < 1024 else "gzip", "gzip")
        zero_chunk = bytes(8 * 1024 * 1024)
        zero_chunk_stored = frozen_gzip_bytes(zero_chunk)
        ratio = len(zero_chunk) / len(zero_chunk_stored)
        self.assertGreater(ratio, 1000)
        self.assertLessEqual(ratio, limits_fixture()["max_compression_ratio"])

    def test_ref_shaped_user_payload_is_opaque_raw_data(self) -> None:
        raw = (
            b'{"$cas_ref":{"schema_version":999,"path":"..\\\\secret",'
            b'"sha256":"not-a-reference"}}'
        )
        pieces = [raw[:9], raw[9:31], raw[31:]]
        reconstructed = b"".join(pieces)
        self.assertEqual(reconstructed, raw)
        self.assertEqual(
            hashlib.sha256(reconstructed).digest(), hashlib.sha256(raw).digest()
        )
        self.assertIn(
            "a PAS1 schema explicitly",
            self.contract,
        )

    def test_legacy_raw_fixture_preserves_exact_bytes_not_only_json_value(self) -> None:
        raw = (
            b'\xef\xbb\xbf{\r\n  "n": 1.2300e+02,\r\n'
            b'  "duplicate": 1, "duplicate": 2,\r\n'
            b'  "unicode": "\\u041f\\u0440\\u0438\\u0432\\u0435\\u0442"\r\n}\r\n'
        )
        chunks = [raw[:17], raw[17:53], raw[53:91], raw[91:]]
        reconstructed = b"".join(chunks)
        self.assertEqual(len(reconstructed), len(raw))
        self.assertEqual(
            hashlib.sha256(reconstructed).hexdigest(),
            hashlib.sha256(raw).hexdigest(),
        )

        normalized = raw.replace(b"\r\n", b"\n")
        self.assertNotEqual(
            hashlib.sha256(normalized).digest(), hashlib.sha256(raw).digest()
        )
        self.assertIn("raw-byte equality", self.contract)
        self.assertIn("canonical equality", self.contract)

    def test_manifest_distinguishes_raw_and_native_integrity(self) -> None:
        schema_name = "prompt-archive-manifest-v1.schema.json"
        self.validator(schema_name).validate(manifest_fixture())
        self.validator(schema_name).validate(manifest_fixture(native=True))

        raw_with_native_semantics = manifest_fixture()
        raw_with_native_semantics["integrity"]["semantics"] = "native-byte-equality"
        self.assert_invalid(schema_name, raw_with_native_semantics)

        empty_native_history = manifest_fixture(native=True)
        empty_native_history["roots"] = {
            "current_fields": empty_native_history["roots"]["current_fields"]
        }
        empty_native_history["counts"]["records"] = 1
        self.validator(schema_name).validate(empty_native_history)

        runtime_without_sprint = manifest_fixture(native=True)
        runtime_without_sprint["scope"]["sprint_id"] = None
        self.assert_invalid(schema_name, runtime_without_sprint)

        migration_without_sprint = manifest_fixture()
        migration_without_sprint["scope"]["sprint_id"] = None
        self.validator(schema_name).validate(migration_without_sprint)

        active_content_type = manifest_fixture(native=True)
        active_content_type["integrity"]["hydrated_media_type"] = "text/html"
        self.assert_invalid(schema_name, active_content_type)

        native_without_envelope = manifest_fixture(native=True)
        native_without_envelope.pop("envelope")
        self.assert_invalid(schema_name, native_without_envelope)

        native_with_raw_root = manifest_fixture(native=True)
        native_with_raw_root["roots"]["raw_bytes"] = index_ref()
        self.assert_invalid(schema_name, native_with_raw_root)

        native_shadow_without_comparison = manifest_fixture(native=True)
        native_shadow_without_comparison.pop("legacy_comparison")
        self.assert_invalid(schema_name, native_shadow_without_comparison)

        raw_without_raw_root = manifest_fixture()
        raw_without_raw_root["roots"] = {"events": index_ref()}
        self.assert_invalid(schema_name, raw_without_raw_root)

        raw_with_mixed_roots = manifest_fixture()
        raw_with_mixed_roots["roots"]["events"] = index_ref()
        self.assert_invalid(schema_name, raw_with_mixed_roots)

        raw_with_envelope = manifest_fixture()
        raw_with_envelope["envelope"] = {
            "media_type": "application/vnd.nginx-qa.prompt-response-envelope-v1+json",
            "byte_size": 1,
            "sha256": SHA_A,
            "chunks_root": index_ref(),
        }
        self.assert_invalid(schema_name, raw_with_envelope)

        authoritative = manifest_fixture(native=True)
        authoritative["source"]["source_kind"] = "native_runtime"
        authoritative.pop("legacy_comparison")
        authoritative["publication"] = {
            "mode": "authoritative",
            "legacy_authoritative": False,
            "operator_gate": "prompt-archive-authoritative-v1",
            "writer_version": "prompt-archive-writer-v1",
            "verified_at": "2026-10-11T11:30:00Z",
        }
        self.validator(schema_name).validate(authoritative)

        canonical_is_optional = manifest_fixture()
        canonical_is_optional["integrity"].pop("canonical")
        self.validator(schema_name).validate(canonical_is_optional)

        for forbidden in ("path", "token", "credential"):
            candidate = manifest_fixture()
            candidate[forbidden] = "must-not-be-persisted"
            with self.subTest(forbidden=forbidden):
                self.assert_invalid(schema_name, candidate)

    def test_publication_authorization_and_shadow_equality_are_closed(self) -> None:
        schema_name = "prompt-archive-manifest-v1.schema.json"
        validator = self.validator(schema_name)

        migration = manifest_fixture()
        validator.validate(migration)
        shadow = manifest_fixture(native=True)
        validator.validate(shadow)
        self.assertEqual(manifest_semantic_issues(shadow), ())

        wrong_source = copy.deepcopy(shadow)
        wrong_source["source"]["source_kind"] = "native_runtime"
        self.assert_invalid(schema_name, wrong_source)

        wrong_migration_mode = copy.deepcopy(migration)
        wrong_migration_mode["publication"]["mode"] = "shadow"
        self.assert_invalid(schema_name, wrong_migration_mode)

        managed_without_binding = copy.deepcopy(shadow)
        managed_without_binding["authorization"]["binding_id"] = None
        self.assert_invalid(schema_name, managed_without_binding)

        managed_without_scope_proof = copy.deepcopy(shadow)
        managed_without_scope_proof["authorization"]["effective_scope_sha256"] = None
        self.assert_invalid(schema_name, managed_without_scope_proof)

        offline_with_binding = copy.deepcopy(migration)
        offline_with_binding["authorization"]["binding_id"] = "assignment-bypass"
        self.assert_invalid(schema_name, offline_with_binding)

        mismatched_comparison = copy.deepcopy(shadow)
        mismatched_comparison["legacy_comparison"]["sha256"] = SHA_A
        self.assertIn(
            "legacy comparison digest does not match hydrated digest",
            manifest_semantic_issues(mismatched_comparison),
        )

        wrong_renderer = copy.deepcopy(shadow)
        wrong_renderer["legacy_comparison"]["renderer_version"] = "other-v1"
        self.assertIn(
            "legacy comparison renderer does not match manifest renderer",
            manifest_semantic_issues(wrong_renderer),
        )

        crlf_media = copy.deepcopy(shadow)
        crlf_media["integrity"]["hydrated_media_type"] = (
            "application/json\r\nX-Unsafe: yes"
        )
        self.assert_invalid(schema_name, crlf_media)

    def test_render_plan_is_closed_and_reconstructs_in_exact_step_order(self) -> None:
        name = "prompt-archive-envelope-v1.schema.json"
        plan = envelope_fixture()
        literal = b'{"current":'
        current_field = b'{"task":"x"}'
        records = {
            "project_state.activity_with_patches": [
                b'{"event":1}',
                b'{"event":2}',
            ],
        }
        self.validator(name).validate(plan)

        def render(value: dict) -> bytes:
            output = bytearray()
            for step in value["steps"]:
                if step["step_kind"] == "literal":
                    chunk = literal
                    if len(chunk) != step["byte_size"]:
                        raise ValueError("literal size mismatch")
                    if hashlib.sha256(chunk).hexdigest() != step["sha256"]:
                        raise ValueError("literal digest mismatch")
                    output.extend(chunk)
                    continue
                if step["step_kind"] == "current_field":
                    output.extend(current_field)
                    continue
                output.extend(base64.b64decode(step["prefix_b64"], validate=True))
                separator = base64.b64decode(step["separator_b64"], validate=True)
                for ordinal, payload in enumerate(records[step["slot_id"]]):
                    if ordinal:
                        output.extend(separator)
                    output.extend(payload)
                output.extend(base64.b64decode(step["suffix_b64"], validate=True))
            rendered = bytes(output)
            if len(rendered) != value["source_byte_size"]:
                raise ValueError("source size mismatch")
            if hashlib.sha256(rendered).hexdigest() != value["source_sha256"]:
                raise ValueError("source digest mismatch")
            return rendered

        self.assertEqual(
            render(plan),
            b'{"current":{"task":"x"},"events":[{"event":1},{"event":2}]}',
        )

        self_referential = copy.deepcopy(plan)
        self_referential["archive_id"] = ARCHIVE_ID
        self.assert_invalid(name, self_referential)

        bad_base64 = copy.deepcopy(plan)
        bad_base64["steps"][2]["prefix_b64"] = "not base64"
        self.assert_invalid(name, bad_base64)

        noncanonical_base64 = copy.deepcopy(plan)
        noncanonical_base64["steps"][2]["prefix_b64"] = "Zh=="
        self.assert_invalid(name, noncanonical_base64)
        self.assertIn(
            "base64 is not canonical RFC 4648",
            canonical_base64_issues("Zh=="),
        )

        oversized_framing = base64.b64encode(bytes(4097)).decode("ascii")
        oversized_base64 = copy.deepcopy(plan)
        oversized_base64["steps"][2]["prefix_b64"] = oversized_framing
        self.assert_invalid(name, oversized_base64)
        self.assertIn(
            "base64 framing exceeds 4096 decoded bytes",
            canonical_base64_issues(oversized_framing),
        )

        bad_literal = copy.deepcopy(plan)
        bad_literal["steps"][0]["sha256"] = SHA_A
        with self.assertRaisesRegex(ValueError, "literal digest mismatch"):
            render(bad_literal)

        wrong_slot = copy.deepcopy(plan)
        wrong_slot["steps"][2]["slot_id"] = "project_state.code_patches"
        self.assert_invalid(name, wrong_slot)

        manifest = manifest_fixture(native=True)
        manifest["roots"]["current_fields"]["digest"] = "1" * 64
        manifest["roots"]["activity_with_patches"]["digest"] = "2" * 64
        manifest["envelope"]["chunks_root"]["digest"] = "3" * 64
        bound_plan = envelope_fixture(manifest["roots"])
        bound_plan["steps"][0]["chunks_root"]["digest"] = "4" * 64
        bound_plan["steps"][1]["record_ref"]["digest"] = "5" * 64
        bound_plan_bytes = canonical_control_bytes(bound_plan)
        manifest["envelope"]["byte_size"] = len(bound_plan_bytes)
        manifest["envelope"]["sha256"] = hashlib.sha256(bound_plan_bytes).hexdigest()
        reachable = {
            manifest["envelope"]["chunks_root"]["digest"],
            *(ref["digest"] for ref in manifest["roots"].values()),
            *(step["chunks_root"]["digest"] for step in bound_plan["steps"]
              if step["step_kind"] == "literal"),
            *(step["record_ref"]["digest"] for step in bound_plan["steps"]
              if step["step_kind"] == "current_field"),
        }
        self.assertEqual(
            render_plan_semantic_issues(bound_plan, manifest, reachable),
            (),
        )
        repeated_plan = envelope_fixture(manifest["roots"])
        repeated_plan["steps"][0]["chunks_root"]["digest"] = "4" * 64
        repeated_plan["steps"][1]["record_ref"]["digest"] = "5" * 64
        self.assertEqual(
            canonical_control_bytes(bound_plan),
            canonical_control_bytes(repeated_plan),
        )

        mismatched_response = copy.deepcopy(bound_plan)
        mismatched_response["response_id"] = "parresp1-" + "9" * 64
        self.assertIn(
            "render plan response id differs from manifest",
            render_plan_semantic_issues(mismatched_response, manifest, reachable),
        )

        unused_root_manifest = copy.deepcopy(manifest)
        unused_root_manifest["roots"]["feedback"] = index_ref()
        self.assertIn(
            "render plan does not consume every manifest root exactly by slot",
            render_plan_semantic_issues(
                bound_plan,
                unused_root_manifest,
                reachable | {unused_root_manifest["roots"]["feedback"]["digest"]},
            ),
        )

        unreachable = set(reachable)
        unreachable.remove(manifest["envelope"]["chunks_root"]["digest"])
        self.assertIn(
            "render plan or manifest root is absent from reachable graph",
            render_plan_semantic_issues(bound_plan, manifest, unreachable),
        )

        excessive_literals = copy.deepcopy(bound_plan)
        excessive_literals["steps"][0]["byte_size"] = 65536
        for _ in range(128):
            excessive_literals["steps"].insert(
                1,
                copy.deepcopy(excessive_literals["steps"][0]),
            )
        self.validator(name).validate(excessive_literals)
        self.assertIn(
            "render plan literal total exceeds 8388608 bytes",
            render_plan_semantic_issues(excessive_literals, manifest, reachable),
        )

    def test_chunk_and_fanout_layout_is_deterministic(self) -> None:
        chunk_size = 8 * 1024 * 1024

        def chunk_lengths(size: int) -> list[int]:
            if size == 0:
                return [0]
            return [
                min(chunk_size, size - offset)
                for offset in range(0, size, chunk_size)
            ]

        def packed_widths(item_count: int) -> list[list[int]]:
            levels: list[list[int]] = []
            current = item_count
            while current > 1:
                widths = [min(64, current - offset) for offset in range(0, current, 64)]
                levels.append(widths)
                current = len(widths)
            return levels

        self.assertEqual(chunk_lengths(0), [0])
        self.assertEqual(chunk_lengths(chunk_size), [chunk_size])
        self.assertEqual(chunk_lengths(chunk_size + 1), [chunk_size, 1])
        self.assertEqual(chunk_lengths(2 * chunk_size), [chunk_size, chunk_size])
        self.assertEqual(packed_widths(64), [[64]])
        self.assertEqual(packed_widths(65), [[64, 1], [2]])
        self.assertEqual(packed_widths(4097), [[64] * 64 + [1], [64, 1], [2]])

    def test_reference_graph_traversal_distinguishes_cycles_from_dag_reuse(self) -> None:
        def graph_ref(digit: str, kind: str = "index_node") -> dict:
            value = index_ref() if kind == "index_node" else blob_ref(kind)
            value["digest"] = digit * 64
            return value

        root_ref = graph_ref("1")
        leaf_ref = graph_ref("2")
        record_ref = graph_ref("3", "event_record")
        child = lambda ref: {
            "ordinal_start": 0,
            "item_count": 1,
            "logical_bytes": 1,
            "ref": ref,
        }
        objects = {
            root_ref["digest"]: {
                "sequence_kind": "events",
                "level": 1,
                "children": [child(leaf_ref), child(leaf_ref)],
            },
            leaf_ref["digest"]: {
                "sequence_kind": "events",
                "level": 0,
                "children": [child(record_ref)],
            },
            record_ref["digest"]: {"record_type": "event"},
        }
        issues, unique_count = reference_graph_semantic_issues(
            [(root_ref, "events")],
            objects,
            max_depth=3,
            max_unique_refs=3,
        )
        self.assertEqual(issues, ())
        self.assertEqual(unique_count, 3)

        too_shallow, _ = reference_graph_semantic_issues(
            [(root_ref, "events")], objects, max_depth=2, max_unique_refs=3
        )
        self.assertIn("reference depth exceeds limit", too_shallow)

        too_many, unique_count = reference_graph_semantic_issues(
            [(root_ref, "events")], objects, max_depth=3, max_unique_refs=2
        )
        self.assertIn("unique reference count exceeds limit", too_many)
        self.assertEqual(unique_count, 3)

        wrong_level = copy.deepcopy(objects)
        wrong_level[leaf_ref["digest"]]["level"] = 1
        wrong_level_issues, _ = reference_graph_semantic_issues(
            [(root_ref, "events")], wrong_level, max_depth=3, max_unique_refs=3
        )
        self.assertIn(
            "child index level is not parent level minus one", wrong_level_issues
        )

        wrong_sequence = copy.deepcopy(objects)
        wrong_sequence[leaf_ref["digest"]]["sequence_kind"] = "patches"
        wrong_sequence_issues, _ = reference_graph_semantic_issues(
            [(root_ref, "events")], wrong_sequence, max_depth=3, max_unique_refs=3
        )
        self.assertIn("child sequence kind differs from parent branch", wrong_sequence_issues)

        cyclic = copy.deepcopy(objects)
        cyclic[leaf_ref["digest"]]["children"] = [child(root_ref)]
        cycle_issues, _ = reference_graph_semantic_issues(
            [(root_ref, "events")], cyclic, max_depth=3, max_unique_refs=3
        )
        self.assertIn("active-ancestor reference cycle", cycle_issues)

    def test_latest_intent_is_predecessor_independent_ordered_and_fail_closed(self) -> None:
        plan_name = "prompt-archive-publication-plan-v1.schema.json"
        slot_name = "prompt-archive-latest-slot-v1.schema.json"
        plan = legacy_latest_plan_fixture()
        self.validator(plan_name).validate(plan)
        self.assertEqual(publication_plan_semantic_issues(plan), ())
        intent = plan["compatibility_publication"]["latest_intent"]

        self.assertNotIn("generation", intent)
        self.assertNotIn("previous_slot_id", intent)
        self.assertEqual(latest_intent_semantic_issues(intent), ())
        self.assertEqual(
            intent["ordering"]["revision_authority"],
            {
                "protocol": "prompt-archive-durable-project-latest-revision-v1",
                "epoch": 1,
                "reset_policy": "restore_high_water_mark_or_fail_closed",
            },
        )

        first = latest_slot_fixture(intent)
        self.validator(slot_name).validate(first)
        self.assertEqual(latest_slot_semantic_issues(first, intent), ())
        successor = copy.deepcopy(first)
        successor["generation"] = 2
        successor["previous_slot_id"] = first["latest_slot_id"]
        successor["latest_slot_id"] = "parlatest1-" + "9" * 64
        self.validator(slot_name).validate(successor)
        self.assertEqual(successor["intent_id"], first["intent_id"])
        self.assertEqual(successor["ordering"], first["ordering"])

        for bootstrap_kind in ("enrolled", "absent", "legacy_bytes"):
            candidate = legacy_latest_plan_fixture()
            bootstrap = latest_bootstrap_fixture(bootstrap_kind)
            candidate["compatibility_publication"]["latest_intent"][
                "bootstrap_precondition"
            ] = bootstrap
            with self.subTest(bootstrap=bootstrap_kind):
                self.validator(plan_name).validate(candidate)

        missing_legacy_observation = legacy_latest_plan_fixture()
        missing_legacy_observation["compatibility_publication"]["latest_intent"][
            "bootstrap_precondition"
        ] = latest_bootstrap_fixture("legacy_bytes")
        missing_legacy_observation["compatibility_publication"]["latest_intent"][
            "bootstrap_precondition"
        ].pop("byte_size")
        self.assert_invalid(plan_name, missing_legacy_observation)

        wrong_external_lock = legacy_latest_plan_fixture()
        wrong_external_lock["compatibility_publication"]["latest_intent"][
            "bootstrap_precondition"
        ]["lock_location_policy"] = "runtime-root-lock-v1"
        self.assert_invalid(plan_name, wrong_external_lock)

        for field, stale_value in (
            ("registration_order", "observe-register-reconcile-before-unlock-v1"),
            (
                "bootstrap_completion_barrier",
                "first-content-and-sidecar-reconciled-before-unlock-v1",
            ),
        ):
            stale_bootstrap_protocol = legacy_latest_plan_fixture()
            stale_bootstrap_protocol["compatibility_publication"]["latest_intent"][
                "bootstrap_precondition"
            ][field] = stale_value
            with self.subTest(stale_bootstrap_field=field):
                self.assert_invalid(plan_name, stale_bootstrap_protocol)

        reset_high_water = legacy_latest_plan_fixture()
        reset_high_water["compatibility_publication"]["latest_intent"]["ordering"][
            "revision_authority"
        ]["reset_policy"] = "restart_from_zero"
        self.assert_invalid(plan_name, reset_high_water)

        maximum_revision = legacy_latest_plan_fixture()
        maximum_revision["compatibility_publication"]["latest_intent"]["ordering"][
            "source_revision"
        ] = 9223372036854775807
        self.validator(plan_name).validate(maximum_revision)
        overflow_revision = copy.deepcopy(maximum_revision)
        overflow_revision["compatibility_publication"]["latest_intent"]["ordering"][
            "source_revision"
        ] += 1
        self.assert_invalid(plan_name, overflow_revision)

        nested_mismatches = (
            (
                ("ordering", "operation_id"),
                "parop1-" + "7" * 32,
                "latest intent ordering operation_id differs from enclosing plan",
            ),
            (
                ("ordering", "occurrence_id"),
                "another-occurrence",
                "latest intent ordering occurrence_id differs from enclosing plan",
            ),
            (
                ("project_id",),
                "another-project",
                "latest intent project_id differs from enclosing plan",
            ),
            (
                ("source_sprint_id",),
                "another-sprint",
                "latest intent source_sprint_id differs from enclosing plan",
            ),
            (
                ("source_response_id",),
                "parresp1-" + "9" * 64,
                "latest intent source_response_id differs from enclosing plan",
            ),
            (
                ("published_at",),
                "2026-10-11T11:35:01Z",
                "latest intent published_at differs from enclosing plan",
            ),
            (
                ("content_sha256",),
                SHA_A,
                "latest intent content_sha256 differs from enclosing plan",
            ),
        )
        for path, value, expected_issue in nested_mismatches:
            candidate = legacy_latest_plan_fixture()
            target = candidate["compatibility_publication"]["latest_intent"]
            for component in path[:-1]:
                target = target[component]
            target[path[-1]] = value
            self.validator(plan_name).validate(candidate)
            with self.subTest(intent_path=".".join(path)):
                self.assertIn(
                    expected_issue, publication_plan_semantic_issues(candidate)
                )

        copied_intent_changed = copy.deepcopy(first)
        copied_intent_changed["ordering"]["operation_id"] = "parop1-" + "8" * 32
        self.assertIn(
            "latest slot ordering differs from content intent",
            latest_slot_semantic_issues(copied_intent_changed, intent),
        )

    def test_pending_registry_is_bounded_recoverable_and_key_scoped(self) -> None:
        registry_name = "prompt-archive-pending-operation-registry-v1.schema.json"
        plan_name = "prompt-archive-publication-plan-v1.schema.json"
        journal_name = "prompt-archive-journal-v1.schema.json"
        registry = pending_registry_example_fixture()
        self.validator(registry_name).validate(registry)
        self.assertEqual(pending_registry_semantic_issues(registry), ())

        wrong_shard = copy.deepcopy(registry)
        wrong_shard["shard_id"] = "00" if registry["shard_id"] != "00" else "01"
        self.validator(registry_name).validate(wrong_shard)
        self.assertIn(
            "pending registry entry hashes to another shard",
            pending_registry_semantic_issues(wrong_shard),
        )

        stale_id = copy.deepcopy(registry)
        stale_id["entries"][0]["updated_at"] = "2026-10-11T11:30:01Z"
        self.assertIn(
            "pending registry id digest differs from registry content",
            pending_registry_semantic_issues(stale_id),
        )

        duplicate = copy.deepcopy(registry)
        duplicate["entries"].append(copy.deepcopy(duplicate["entries"][0]))
        self.assertIn(
            "pending registry contains duplicate recovery keys",
            pending_registry_semantic_issues(duplicate),
        )

        generation_one_with_predecessor = copy.deepcopy(registry)
        generation_one_with_predecessor["previous_registry_id"] = (
            "parpending1-" + "9" * 64
        )
        self.assert_invalid(registry_name, generation_one_with_predecessor)

        plan = legacy_latest_plan_fixture()
        self.validator(plan_name).validate(plan)
        wrong_plan_operation = copy.deepcopy(plan)
        wrong_plan_operation["pending_registry"]["intended_mutation"][
            "operation_id"
        ] = "parop1-" + "8" * 32
        self.validator(plan_name).validate(wrong_plan_operation)
        self.assertIn(
            "pending registry operation differs from enclosing plan",
            publication_plan_semantic_issues(wrong_plan_operation),
        )

        intent = legacy_latest_journal_fixture(state="intent")
        payload = legacy_latest_journal_fixture(state="legacy_payload_verified")
        published = legacy_latest_journal_fixture(state="compatibility_published")
        complete = legacy_latest_journal_fixture(state="complete")
        for journal in (intent, payload, published, complete):
            self.validator(journal_name).validate(journal)
        self.assertEqual(journal_semantic_issues(intent, plan), ())
        self.assertEqual(journal_semantic_issues(payload, plan, intent), ())
        self.assertEqual(journal_semantic_issues(published, plan, payload), ())
        self.assertEqual(journal_semantic_issues(complete, plan, published), ())
        self.assertEqual(
            intent["pending_registry"]["precondition_entry"]["state"], "planned"
        )
        self.assertEqual(
            payload["pending_registry"]["precondition_entry"]["state"],
            "journaled",
        )
        self.assertEqual(
            complete["pending_registry"]["intended_mutation"],
            {
                "mutation": "remove_terminal_entry",
                "terminal_state": "complete",
                "published_at": complete["updated_at"],
            },
        )

        wrong_predecessor_entry = copy.deepcopy(payload)
        wrong_predecessor_entry["pending_registry"]["precondition_entry"][
            "journal_ref"
        ]["digest"] = SHA_A
        self.assertIn(
            "journal pending predecessor ref digest differs from deterministic representation",
            journal_semantic_issues(wrong_predecessor_entry, plan, intent),
        )

    def test_legacy_latest_transition_identity_state_and_rollback_are_journaled(self) -> None:
        plan_name = "prompt-archive-publication-plan-v1.schema.json"
        journal_name = "prompt-archive-journal-v1.schema.json"
        for write_kind in ("transition", "identity", "state", "rollback"):
            plan = legacy_latest_plan_fixture(write_kind)
            with self.subTest(write_kind=write_kind, artifact="plan"):
                self.validator(plan_name).validate(plan)
                self.assertEqual(publication_plan_semantic_issues(plan), ())
            previous = None
            for state in (
                "intent",
                "legacy_payload_verified",
                "compatibility_published",
                "complete",
            ):
                journal = legacy_latest_journal_fixture(write_kind, state)
                with self.subTest(write_kind=write_kind, state=state):
                    self.validator(journal_name).validate(journal)
                    self.assertEqual(
                        journal_semantic_issues(journal, plan, previous), ()
                    )
                previous = journal

        bypass_drain = legacy_latest_plan_fixture()
        bypass_drain["legacy_latest"]["authoritative_drain_barrier"][
            "observed_incomplete_operation_ids"
        ] = ["parop1-" + "1" * 32]
        self.assert_invalid(plan_name, bypass_drain)

        leaked_archive_state = legacy_latest_journal_fixture()
        leaked_archive_state["archive_id"] = ARCHIVE_ID
        self.assert_invalid(journal_name, leaked_archive_state)

    def test_inventory_source_metadata_descriptor_stub_and_busy_error_are_closed(self) -> None:
        inventory_name = "prompt-archive-source-inventory-v1.schema.json"
        metadata_name = "prompt-archive-source-metadata-v1.schema.json"
        stub_name = "prompt-archive-descriptor-stub-v1.schema.json"
        error_name = "prompt-archive-error-v1.schema.json"

        inventory = source_inventory_fixture()
        self.validator(inventory_name).validate(inventory)
        self.assertEqual(source_inventory_semantic_issues(inventory), ())
        changed_inventory = copy.deepcopy(inventory)
        changed_inventory["entry_count"] += 1
        self.assertIn(
            "source inventory id digest differs from inventory identity",
            source_inventory_semantic_issues(changed_inventory),
        )
        inventory_path = copy.deepcopy(inventory)
        inventory_path["source_root_path"] = "C:/forbidden"
        self.assert_invalid(inventory_name, inventory_path)

        metadata = source_metadata_fixture()
        self.validator(metadata_name).validate(metadata)
        self.assertEqual(source_metadata_semantic_issues(metadata), ())
        wrong_count = copy.deepcopy(metadata)
        wrong_count["redaction_count"] = 1
        self.assertIn(
            "source metadata redaction count differs from redacted fields",
            source_metadata_semantic_issues(wrong_count),
        )
        unredacted = copy.deepcopy(metadata)
        unredacted["sender_label"] = "operator@example.invalid"
        self.assertIn(
            "source metadata sender_label is not wholly redacted",
            source_metadata_semantic_issues(unredacted),
        )
        adapter_control = copy.deepcopy(metadata)
        adapter_control["adapter_command"] = "start-from-git"
        self.assert_invalid(metadata_name, adapter_control)

        stub = descriptor_stub_fixture()
        self.validator(stub_name).validate(stub)
        leaked_route = copy.deepcopy(stub)
        leaked_route["content_url"] = "https://attacker.invalid/archive"
        self.assert_invalid(stub_name, leaked_route)

        busy = error_fixture("PROMPT_ARCHIVE_BUSY")
        self.assertTrue(busy["detail"]["retryable"])
        self.validator(error_name).validate(busy)
        busy["detail"]["retryable"] = False
        self.assert_invalid(error_name, busy)

    def test_index_and_pointer_are_bounded_and_generation_fenced(self) -> None:
        index_name = "prompt-archive-index-node-v1.schema.json"
        index = index_fixture()
        index["children"] = index["children"] * 65
        self.assert_invalid(index_name, index)

        index = index_fixture()
        index["level"] = 33
        self.assert_invalid(index_name, index)

        wrong_record_kind = index_fixture()
        wrong_record_kind["children"][0]["ref"]["logical_kind"] = "patch_record"
        self.assert_invalid(index_name, wrong_record_kind)

        current_record = record_fixture()
        current_record["record_type"] = "current_field"
        self.validator("prompt-archive-record-v1.schema.json").validate(current_record)
        current_index = index_fixture()
        current_index["sequence_kind"] = "current_fields"
        current_index["children"][0]["ref"] = blob_ref("current_field_record")
        current_index["children"][0]["ref"]["media_type"] = (
            "application/vnd.nginx-qa.prompt-archive-record-v1+json"
        )
        self.validator(index_name).validate(current_index)

        pointer_name = "prompt-archive-pointer-v1.schema.json"
        pointer = pointer_fixture()
        pointer["generation"] = 0
        self.assert_invalid(pointer_name, pointer)

        pointer = pointer_fixture()
        pointer["generation"] = 2
        self.assert_invalid(pointer_name, pointer)

        pointer = pointer_fixture()
        pointer["previous_pointer_id"] = "parptr1-" + "9" * 64
        self.assert_invalid(pointer_name, pointer)

        pointer = pointer_fixture()
        pointer["manifest_ref"]["logical_kind"] = "payload_chunk"
        self.assert_invalid(pointer_name, pointer)

        locator_name = "prompt-archive-locator-v1.schema.json"
        locator = archive_locator_fixture()
        self.validator(locator_name).validate(locator)
        expected_locator_id = "pararc1-" + pas1_id_digest(
            "archive-locator-id",
            [
                ("project_id", locator["project_id"]),
                ("sprint_id", locator["sprint_id"]),
                ("response_id", locator["response_id"]),
                ("archive_id", locator["archive_id"]),
                ("pointer_id", locator["pointer_id"]),
                ("manifest_sha256", locator["manifest_ref"]["digest"]),
                ("published_at", locator["published_at"]),
            ],
        )
        self.assertEqual(locator["locator_id"], expected_locator_id)
        path_in_locator = copy.deepcopy(locator)
        path_in_locator["path"] = "C:/forbidden"
        self.assert_invalid(locator_name, path_in_locator)

        for state in (
            "intent",
            "blobs_verified",
            "manifest_verified",
            "pointer_published",
            "locator_published",
            "compatibility_published",
            "complete",
        ):
            self.validator("prompt-archive-journal-v1.schema.json").validate(
                journal_fixture(state)
            )

        plan = publication_plan_fixture()
        previous = journal_fixture("compatibility_published")
        self.assertEqual(
            journal_semantic_issues(journal_fixture(), plan, previous), ()
        )
        mismatched_journal = journal_fixture()
        mismatched_journal["occurrence"]["occurrence_id"] = "another-occurrence"
        self.assertIn(
            "journal occurrence differs from plan",
            journal_semantic_issues(mismatched_journal, plan),
        )

        skipped_stage = journal_fixture("complete")
        skipped_stage["events"].pop(1)
        self.assert_invalid("prompt-archive-journal-v1.schema.json", skipped_stage)

        invalid_lineage = journal_fixture("intent")
        invalid_lineage["expected_pointer_generation"] = 1
        self.assert_invalid("prompt-archive-journal-v1.schema.json", invalid_lineage)

        for from_state in ("intent", "blobs_verified", "manifest_verified"):
            predecessor = journal_fixture(from_state)
            abort = copy.deepcopy(predecessor)
            next_generation = predecessor["journal_generation"] + 1
            abort.update(
                {
                    "state": "aborted",
                    "abort_from_state": from_state,
                    "abort_reason_code": "source_changed",
                    "journal_generation": next_generation,
                    "previous_journal_generation": predecessor[
                        "journal_generation"
                    ],
                    "previous_journal_sha256": hashlib.sha256(
                        canonical_control_bytes(predecessor)
                    ).hexdigest(),
                    "updated_at": abort["frozen_timestamps"]["aborted_at"],
                }
            )
            abort["events"].append(
                {
                    "state": "aborted",
                    "ordinal": next_generation,
                    "occurred_at": abort["frozen_timestamps"]["aborted_at"],
                    "from_state": from_state,
                }
            )
            predecessor_ref = blob_ref("journal")
            bind_ref_to_bytes(
                predecessor_ref, canonical_control_bytes(predecessor)
            )
            abort["pending_registry"]["precondition_entry"].update(
                {
                    "state": "journaled",
                    "journal_ref": predecessor_ref,
                    "updated_at": predecessor["updated_at"],
                }
            )
            abort["pending_registry"]["intended_mutation"] = {
                "mutation": "remove_terminal_entry",
                "terminal_state": "aborted",
                "published_at": abort["frozen_timestamps"]["aborted_at"],
            }
            with self.subTest(abort_from_state=from_state):
                self.validator("prompt-archive-journal-v1.schema.json").validate(
                    abort
                )

        illegal_post_pointer_abort = journal_fixture("pointer_published")
        illegal_post_pointer_abort.update(
            {
                "state": "aborted",
                "abort_from_state": "manifest_verified",
                "abort_reason_code": "operator_cancelled",
            }
        )
        illegal_post_pointer_abort["events"][-1].update(
            {"state": "aborted", "from_state": "manifest_verified"}
        )
        self.assert_invalid(
            "prompt-archive-journal-v1.schema.json", illegal_post_pointer_abort
        )

        wrong_precondition = journal_fixture()
        wrong_precondition["expected_pointer_generation"] = 1
        self.assertIn(
            "journal pointer generation precondition differs from plan",
            journal_semantic_issues(wrong_precondition, plan),
        )

        tampered_predecessor = journal_fixture()
        self.assertIn(
            "journal predecessor digest differs from retained record",
            journal_semantic_issues(
                tampered_predecessor,
                plan,
                journal_fixture("locator_published"),
            ),
        )

    def test_durable_publication_plan_is_replayable_and_path_free(self) -> None:
        name = "prompt-archive-publication-plan-v1.schema.json"
        plan = publication_plan_fixture()
        self.validator(name).validate(plan)
        self.assertEqual(publication_plan_semantic_issues(plan), ())

        wrong_locator = copy.deepcopy(plan)
        wrong_locator["source_locator"] = {
            "locator_kind": "runtime_occurrence",
            "locator_version": "prompt-archive-runtime-occurrence-locator-v1",
            "runtime_instance_id": "instance-example-1",
            "runtime_occurrence_id": "transition-example-1",
            "state_revision": 1,
        }
        self.assert_invalid(name, wrong_locator)
        self.assertIn(
            "runtime source occurrence differs from plan",
            publication_plan_semantic_issues(wrong_locator),
        )

        missing_document = copy.deepcopy(plan)
        missing_document["intended_manifest"].pop("document")
        self.assert_invalid(name, missing_document)

        path_injection = copy.deepcopy(plan)
        path_injection["source_locator"]["path"] = "C:/forbidden"
        self.assert_invalid(name, path_injection)

        mismatched_scope = copy.deepcopy(plan)
        mismatched_scope["scope"]["project_id"] = "another-project"
        self.assertIn(
            "plan project_id differs from intended manifest",
            publication_plan_semantic_issues(mismatched_scope),
        )

        tampered_document = copy.deepcopy(plan)
        tampered_document["intended_manifest"]["document"]["created_at"] = (
            "2026-10-11T11:29:01Z"
        )
        self.assertIn(
            "manifest ref digest differs from deterministic representation",
            publication_plan_semantic_issues(tampered_document),
        )

        tampered_pointer = copy.deepcopy(plan)
        tampered_pointer["intended_pointer"]["document"]["generation"] = 2
        self.assertIn(
            "pointer generation differs from precondition",
            publication_plan_semantic_issues(tampered_pointer),
        )

        tampered_locator = copy.deepcopy(plan)
        tampered_locator["intended_archive_locator"]["document"][
            "pointer_id"
        ] = "parptr1-" + "f" * 64
        self.assertIn(
            "archive locator ref digest differs from deterministic representation",
            publication_plan_semantic_issues(tampered_locator),
        )

        refs = [
            ("manifest", "a" * 64, "b" * 64, 100, 80),
            ("payload_chunk", "c" * 64, "d" * 64, 5, 5),
            ("manifest", "a" * 64, "b" * 64, 100, 80),
        ]
        lines = {
            ("\0".join(str(field) for field in ref) + "\n").encode("ascii")
            for ref in refs
        }
        reachable_preimage = (
            b"prompt-archive-reachable-set-digest-v1\n" + b"".join(sorted(lines))
        )
        self.assertEqual(len(lines), 2)
        self.assertEqual(
            hashlib.sha256(reachable_preimage).hexdigest(),
            "5854e85a1e9da65f758bf957f402c7379de848c08c9420b95148e3536e8c8ffe",
        )

    def test_scope_provenance_and_api_urls_are_closed(self) -> None:
        record_name = "prompt-archive-record-v1.schema.json"
        record = record_fixture()
        record["provenance"]["authorization"] = "secret"
        self.assert_invalid(record_name, record)

        reusable = record_fixture()
        reusable["provenance"]["origin_response_id"] = "parresp1-" + "9" * 64
        reusable["payload"]["byte_size"] = 20 * 1024 * 1024
        self.validator(record_name).validate(reusable)

        consuming_identity_in_scope = record_fixture()
        consuming_identity_in_scope["scope"]["response_id"] = RESPONSE_ID
        self.assert_invalid(record_name, consuming_identity_in_scope)
        self.assertIn("Reuse across project or sprint scope is\nforbidden", self.contract)

        descriptor_name = "prompt-archive-descriptor-v1.schema.json"
        local_descriptor = descriptor_fixture()
        self.validator(descriptor_name).validate(local_descriptor)
        self.assertEqual(descriptor_semantic_issues(local_descriptor), ())

        managed_descriptor = managed_descriptor_fixture()
        self.validator(descriptor_name).validate(managed_descriptor)
        self.assertEqual(descriptor_semantic_issues(managed_descriptor), ())

        wrong_managed_phone = managed_descriptor_fixture()
        wrong_managed_phone["descriptor_url"] = wrong_managed_phone[
            "descriptor_url"
        ].replace("/agents/2971/", "/agents/2972/")
        self.validator(descriptor_name).validate(wrong_managed_phone)
        self.assertIn(
            "descriptor_url differs from descriptor identity",
            descriptor_semantic_issues(wrong_managed_phone),
        )

        mismatched_context = descriptor_fixture()
        mismatched_context["route_context"] = "managed_agent"
        self.assert_invalid(descriptor_name, mismatched_context)

        descriptor = descriptor_fixture()
        descriptor["content_url"] = "https://attacker.invalid/blob"
        self.assert_invalid(descriptor_name, descriptor)

        descriptor = descriptor_fixture()
        descriptor["blob_url"] = "/api/v1/blobs/" + SHA_A
        self.assert_invalid(descriptor_name, descriptor)

        descriptor_without_kind = descriptor_fixture()
        descriptor_without_kind.pop("kind")
        self.assert_invalid(descriptor_name, descriptor_without_kind)

        descriptor_with_crlf = descriptor_fixture()
        descriptor_with_crlf["media_type"] += "\r\nX-Unsafe: yes"
        self.assert_invalid(descriptor_name, descriptor_with_crlf)

        blob = blob_fixture()
        blob["path"] = "C:/forbidden"
        self.assert_invalid("prompt-archive-blob-v1.schema.json", blob)

        over_ratio = manifest_fixture()
        over_ratio["limits"]["max_compression_ratio"] = 4097
        self.assert_invalid("prompt-archive-manifest-v1.schema.json", over_ratio)

        source_metadata = record_fixture()
        source_metadata["record_type"] = "source_metadata"
        source_metadata["payload"]["media_type"] = (
            "application/vnd.nginx-qa.prompt-archive-source-metadata-v1+json"
        )
        source_metadata["provenance"]["source_id"] = "inbound-email-adapter"
        self.validator(record_name).validate(source_metadata)
        adapter_like_payload = canonical_control_bytes(
            {"adapter": "email", "execute": True, "command": "start-from-git"}
        )
        self.assertEqual(
            json.loads(adapter_like_payload),
            {"adapter": "email", "execute": True, "command": "start-from-git"},
        )
        self.assertIn(
            "Native `source_metadata` is the canonical bytes of",
            self.contract,
        )

    def test_stable_error_envelope_has_no_unbounded_context(self) -> None:
        name = "prompt-archive-error-v1.schema.json"
        codes = self.schemas[name]["properties"]["detail"]["properties"][
            "error"
        ]["enum"]
        for code in codes:
            with self.subTest(code=code):
                self.validator(name).validate(error_fixture(code))

                wrong_retry = error_fixture(code)
                wrong_retry["detail"]["retryable"] = not wrong_retry["detail"][
                    "retryable"
                ]
                self.assert_invalid(name, wrong_retry)

                wrong_message = error_fixture(code)
                wrong_message["detail"]["message"] = "A different safe message."
                self.assert_invalid(name, wrong_message)

        leaked = error_fixture()
        leaked["detail"]["path"] = "C:/sensitive/archive"
        self.assert_invalid(name, leaked)

        control_character = error_fixture()
        control_character["detail"]["message"] = "unsafe\npath"
        self.assert_invalid(name, control_character)

    def test_contract_freezes_hot_path_api_rollout_and_safety_boundaries(self) -> None:
        required_fragments = (
            "NGINX_QA_PROMPT_ARCHIVE_MODE=legacy|shadow|authoritative",
            "Unset mode means `legacy`",
            "shadow mode necessarily exercises",
            "must not call `project_state_json()`",
            "GET /api/v1/projects/{project_id}/sprints/{sprint_id}/prompt-archives/{archive_id}",
            "GET /api/v1/projects/{project_id}/agents/{agent_phone}/sprints/{sprint_id}/prompt-archives/{archive_id}",
            "X-Nginx-QA-Scope-Token",
            "Content-Disposition: attachment",
            "X-Content-Type-Options: nosniff",
            "PROMPT_ARCHIVE_GENERATION_CONFLICT",
            "gzip-rfc1952-zlib-1.2.13-level9-v1",
            "pas1-concat-renderer-v1",
            "prompt-archive-publication-plan-v1",
            "operations/v1/<response_id>/<operation_id>/plan.json",
            "pointers/v1/<response_id>.json",
            "archives/v1/<archive_id>.json",
            "cross-process per-operation lock",
            "A registered `planned` entry with no journal",
            "fixed generation slot",
            "journal fork",
            "between journal CAS and registry rebinding",
            "descriptor stubs",
            "Project scoping prevents cross-project deduplication",
            "There is no V1 `migrate-live`, `delete`,",
            "prompt-archive-authoritative-v1",
            "hashing is itself a read",
            "Nothing in this contract authorizes reading `D:\\\\Prompt`",
        )
        for fragment in required_fragments:
            with self.subTest(fragment=fragment):
                self.assertIn(fragment, self.contract)

        self.assertNotIn("canonical equality proves raw", self.contract.casefold())


if __name__ == "__main__":
    unittest.main()
