"""Strict exact-byte JSON span parser for Prompt Archive V1.

The parser deliberately operates on UTF-8 bytes rather than on a decoded
``dict``/``list`` tree.  It preserves the exact lexical representation of
every value, separator, object key, and container boundary so a caller can
build the PAS1 recursive Merkle representation without losing whitespace,
member order, escape spelling, or number spelling.

This module does not interpret reference-shaped user data and performs no
filesystem or runtime work.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
import re
from typing import Final, Literal


_JSON_WHITESPACE: Final = frozenset(b" \t\r\n")
_NUMBER_RE: Final = re.compile(
    rb"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?"
)
_HEX_DIGITS: Final = frozenset(b"0123456789abcdefABCDEF")
_SIMPLE_ESCAPES: Final = {
    ord('"'): '"',
    ord("\\"): "\\",
    ord("/"): "/",
    ord("b"): "\b",
    ord("f"): "\f",
    ord("n"): "\n",
    ord("r"): "\r",
    ord("t"): "\t",
}

DEFAULT_MAX_DEPTH: Final = 64
HARD_MAX_DEPTH: Final = 128
DEFAULT_MAX_NODES: Final = 250_000
HARD_MAX_NODES: Final = 1_000_000
DEFAULT_MAX_EDGES: Final = 250_000
HARD_MAX_EDGES: Final = 1_000_000

ScalarKind = Literal["null", "boolean", "number", "string"]
NodeKind = Literal["scalar", "array", "object"]
EdgeKind = Literal["array_item", "object_member"]


class JsonSpanParseError(ValueError):
    """A payload-safe parse failure with an exact byte offset."""

    def __init__(self, reason: str, byte_offset: int) -> None:
        super().__init__(f"{reason} at byte offset {byte_offset}")
        self.reason = reason
        self.byte_offset = byte_offset


@dataclass(frozen=True, slots=True)
class ByteSpan:
    """A half-open byte range in the original JSON document."""

    start: int
    end: int

    def __post_init__(self) -> None:
        if (
            type(self.start) is not int
            or type(self.end) is not int
            or self.start < 0
            or self.end < self.start
        ):
            raise ValueError("invalid byte span")

    @property
    def byte_size(self) -> int:
        return self.end - self.start

    def bytes_from(self, source: bytes) -> bytes:
        if type(source) is not bytes:
            raise TypeError("source must be exact bytes")
        if self.end > len(source):
            raise ValueError("byte span exceeds source")
        return source[self.start : self.end]


@dataclass(frozen=True, slots=True)
class JsonNode:
    """One exact JSON value and its context-free lexical partition."""

    node_kind: NodeKind
    start: int
    end: int
    scalar_kind: ScalarKind | None = None
    token: ByteSpan | None = None
    prefix: ByteSpan | None = None
    suffix: ByteSpan | None = None
    edges: tuple[JsonEdge, ...] = ()

    def __post_init__(self) -> None:
        span = ByteSpan(self.start, self.end)
        if span.byte_size == 0:
            raise ValueError("JSON node span must not be empty")
        if self.node_kind == "scalar":
            if (
                self.scalar_kind not in {"null", "boolean", "number", "string"}
                or self.token != span
                or self.prefix is not None
                or self.suffix is not None
                or self.edges
            ):
                raise ValueError("invalid scalar JSON node")
            return
        if self.node_kind not in {"array", "object"}:
            raise ValueError("invalid JSON node kind")
        if (
            self.scalar_kind is not None
            or self.token is not None
            or self.prefix is None
            or self.suffix is None
            or self.prefix.start != self.start
            or self.suffix.end != self.end
        ):
            raise ValueError("invalid container JSON node")
        expected_edge_kind = (
            "array_item" if self.node_kind == "array" else "object_member"
        )
        if any(edge.edge_kind != expected_edge_kind for edge in self.edges):
            raise ValueError("container contains the wrong edge kind")
        if not self.edges:
            if self.prefix.end != self.suffix.start:
                raise ValueError("empty container spans are not contiguous")
            return
        if self.prefix.end != self.edges[0].start:
            raise ValueError("container prefix and first edge are not contiguous")
        for previous, current in zip(self.edges, self.edges[1:]):
            if previous.end != current.start:
                raise ValueError("container edges are not contiguous")
        if self.edges[-1].end != self.suffix.start:
            raise ValueError("last edge and container suffix are not contiguous")

    @property
    def span(self) -> ByteSpan:
        return ByteSpan(self.start, self.end)

    @property
    def entry_count(self) -> int:
        return len(self.edges)

    def bytes_from(self, source: bytes) -> bytes:
        return self.span.bytes_from(source)


@dataclass(frozen=True, slots=True)
class JsonEdge:
    """One exact array item or object member, including its separator."""

    edge_kind: EdgeKind
    start: int
    end: int
    separator_before: ByteSpan
    value: JsonNode
    key_token: ByteSpan | None = None
    key_value_separator: ByteSpan | None = None
    member_name: str | None = None

    def __post_init__(self) -> None:
        span = ByteSpan(self.start, self.end)
        if span.byte_size == 0 or self.separator_before.start != self.start:
            raise ValueError("invalid JSON edge span")
        if self.value.end != self.end:
            raise ValueError("JSON edge does not end with its value")
        if self.edge_kind == "array_item":
            if (
                self.separator_before.end != self.value.start
                or self.key_token is not None
                or self.key_value_separator is not None
                or self.member_name is not None
            ):
                raise ValueError("invalid array edge")
            return
        if self.edge_kind != "object_member":
            raise ValueError("invalid JSON edge kind")
        if (
            self.key_token is None
            or self.key_value_separator is None
            or type(self.member_name) is not str
            or self.separator_before.end != self.key_token.start
            or self.key_token.end != self.key_value_separator.start
            or self.key_value_separator.end != self.value.start
        ):
            raise ValueError("invalid object edge")

    @property
    def span(self) -> ByteSpan:
        return ByteSpan(self.start, self.end)

    def bytes_from(self, source: bytes) -> bytes:
        return self.span.bytes_from(source)


class _Parser:
    def __init__(
        self,
        source: bytes,
        *,
        max_depth: int,
        max_nodes: int,
        max_edges: int,
    ) -> None:
        self.source = source
        self.length = len(source)
        self.position = 0
        self.max_depth = max_depth
        self.max_nodes = max_nodes
        self.max_edges = max_edges
        self.node_count = 0
        self.edge_count = 0

    def error(self, reason: str, offset: int | None = None) -> JsonSpanParseError:
        return JsonSpanParseError(
            reason,
            self.position if offset is None else offset,
        )

    def skip_whitespace(self) -> None:
        while (
            self.position < self.length
            and self.source[self.position] in _JSON_WHITESPACE
        ):
            self.position += 1

    def parse(self) -> JsonNode:
        self.skip_whitespace()
        if self.position == self.length:
            raise self.error("JSON document is empty")
        root = self.parse_value(depth=1)
        self.skip_whitespace()
        if self.position != self.length:
            raise self.error("trailing data after JSON value")
        if root.node_kind == "scalar":
            return replace(
                root,
                start=0,
                end=self.length,
                token=ByteSpan(0, self.length),
            )
        assert root.prefix is not None and root.suffix is not None
        return replace(
            root,
            start=0,
            end=self.length,
            prefix=ByteSpan(0, root.prefix.end),
            suffix=ByteSpan(root.suffix.start, self.length),
        )

    def count_node(self, depth: int) -> None:
        if depth > self.max_depth:
            raise self.error("maximum JSON depth exceeded")
        self.node_count += 1
        if self.node_count > self.max_nodes:
            raise self.error("maximum JSON node count exceeded")

    def count_edge(self) -> None:
        self.edge_count += 1
        if self.edge_count > self.max_edges:
            raise self.error("maximum JSON edge count exceeded")

    def parse_value(self, *, depth: int) -> JsonNode:
        self.count_node(depth)
        if self.position >= self.length:
            raise self.error("unexpected end of JSON input")
        token = self.source[self.position]
        if token == ord("{"):
            return self.parse_object(depth=depth)
        if token == ord("["):
            return self.parse_array(depth=depth)
        if token == ord('"'):
            start = self.position
            self.parse_string_token()
            return JsonNode(
                "scalar",
                start,
                self.position,
                scalar_kind="string",
                token=ByteSpan(start, self.position),
            )
        for literal, scalar_kind in (
            (b"null", "null"),
            (b"true", "boolean"),
            (b"false", "boolean"),
        ):
            if self.source.startswith(literal, self.position):
                start = self.position
                self.position += len(literal)
                return JsonNode(
                    "scalar",
                    start,
                    self.position,
                    scalar_kind=scalar_kind,
                    token=ByteSpan(start, self.position),
                )
        if token == ord("-") or ord("0") <= token <= ord("9"):
            match = _NUMBER_RE.match(self.source, self.position)
            if match is None:
                raise self.error("invalid JSON number")
            start = self.position
            self.position = match.end()
            return JsonNode(
                "scalar",
                start,
                self.position,
                scalar_kind="number",
                token=ByteSpan(start, self.position),
            )
        raise self.error("invalid JSON value")

    def parse_array(self, *, depth: int) -> JsonNode:
        start = self.position
        self.position += 1
        self.skip_whitespace()
        prefix_end = self.position
        prefix = ByteSpan(start, prefix_end)
        if self.position >= self.length:
            raise self.error("unterminated JSON array")
        if self.source[self.position] == ord("]"):
            suffix_start = self.position
            self.position += 1
            return JsonNode(
                "array",
                start,
                self.position,
                prefix=prefix,
                suffix=ByteSpan(suffix_start, self.position),
            )

        edges: list[JsonEdge] = []
        separator = ByteSpan(prefix_end, prefix_end)
        while True:
            self.count_edge()
            value = self.parse_value(depth=depth + 1)
            edges.append(
                JsonEdge(
                    "array_item",
                    separator.start,
                    value.end,
                    separator,
                    value,
                )
            )
            self.skip_whitespace()
            if self.position >= self.length:
                raise self.error("unterminated JSON array")
            delimiter = self.source[self.position]
            if delimiter == ord("]"):
                suffix_start = value.end
                self.position += 1
                return JsonNode(
                    "array",
                    start,
                    self.position,
                    prefix=prefix,
                    suffix=ByteSpan(suffix_start, self.position),
                    edges=tuple(edges),
                )
            if delimiter != ord(","):
                raise self.error("expected ',' or ']' in JSON array")
            separator_start = value.end
            self.position += 1
            self.skip_whitespace()
            if self.position >= self.length:
                raise self.error("unterminated JSON array")
            if self.source[self.position] == ord("]"):
                raise self.error("trailing comma in JSON array")
            separator = ByteSpan(separator_start, self.position)

    def parse_object(self, *, depth: int) -> JsonNode:
        start = self.position
        self.position += 1
        self.skip_whitespace()
        prefix_end = self.position
        prefix = ByteSpan(start, prefix_end)
        if self.position >= self.length:
            raise self.error("unterminated JSON object")
        if self.source[self.position] == ord("}"):
            suffix_start = self.position
            self.position += 1
            return JsonNode(
                "object",
                start,
                self.position,
                prefix=prefix,
                suffix=ByteSpan(suffix_start, self.position),
            )

        seen_names: set[str] = set()
        edges: list[JsonEdge] = []
        separator = ByteSpan(prefix_end, prefix_end)
        while True:
            self.count_edge()
            if self.source[self.position] != ord('"'):
                raise self.error("JSON object member name must be a string")
            key_start = self.position
            member_name = self.parse_string_token()
            key_end = self.position
            if member_name in seen_names:
                raise self.error("duplicate JSON object member", key_start)
            seen_names.add(member_name)

            key_value_start = key_end
            self.skip_whitespace()
            if self.position >= self.length or self.source[self.position] != ord(":"):
                raise self.error("missing ':' after JSON object member name")
            self.position += 1
            self.skip_whitespace()
            if self.position >= self.length:
                raise self.error("missing JSON object member value")
            key_value_separator = ByteSpan(key_value_start, self.position)
            value = self.parse_value(depth=depth + 1)
            edges.append(
                JsonEdge(
                    "object_member",
                    separator.start,
                    value.end,
                    separator,
                    value,
                    key_token=ByteSpan(key_start, key_end),
                    key_value_separator=key_value_separator,
                    member_name=member_name,
                )
            )

            self.skip_whitespace()
            if self.position >= self.length:
                raise self.error("unterminated JSON object")
            delimiter = self.source[self.position]
            if delimiter == ord("}"):
                suffix_start = value.end
                self.position += 1
                return JsonNode(
                    "object",
                    start,
                    self.position,
                    prefix=prefix,
                    suffix=ByteSpan(suffix_start, self.position),
                    edges=tuple(edges),
                )
            if delimiter != ord(","):
                raise self.error("expected ',' or '}' in JSON object")
            separator_start = value.end
            self.position += 1
            self.skip_whitespace()
            if self.position >= self.length:
                raise self.error("unterminated JSON object")
            if self.source[self.position] == ord("}"):
                raise self.error("trailing comma in JSON object")
            separator = ByteSpan(separator_start, self.position)

    def parse_string_token(self) -> str:
        if self.position >= self.length or self.source[self.position] != ord('"'):
            raise self.error("expected JSON string")
        self.position += 1
        segment_start = self.position
        decoded: list[str] = []
        while self.position < self.length:
            token = self.source[self.position]
            if token == ord('"'):
                if segment_start < self.position:
                    decoded.append(
                        self.source[segment_start : self.position].decode("utf-8")
                    )
                self.position += 1
                return "".join(decoded)
            if token < 0x20:
                raise self.error("unescaped control character in JSON string")
            if token != ord("\\"):
                self.position += 1
                continue

            if segment_start < self.position:
                decoded.append(
                    self.source[segment_start : self.position].decode("utf-8")
                )
            escape_start = self.position
            if escape_start + 1 >= self.length:
                raise self.error("unterminated JSON string escape", escape_start)
            escape = self.source[escape_start + 1]
            if escape in _SIMPLE_ESCAPES:
                decoded.append(_SIMPLE_ESCAPES[escape])
                self.position += 2
                segment_start = self.position
                continue
            if escape != ord("u"):
                raise self.error("invalid JSON string escape", escape_start)

            code_unit = self.parse_unicode_escape(escape_start)
            self.position = escape_start + 6
            if 0xD800 <= code_unit <= 0xDBFF:
                low_start = self.position
                if (
                    low_start + 6 > self.length
                    or self.source[low_start : low_start + 2] != b"\\u"
                ):
                    raise self.error(
                        "high surrogate is not followed by a low surrogate",
                        escape_start,
                    )
                low_unit = self.parse_unicode_escape(low_start)
                if not 0xDC00 <= low_unit <= 0xDFFF:
                    raise self.error(
                        "high surrogate is not followed by a low surrogate",
                        escape_start,
                    )
                decoded.append(
                    chr(
                        0x10000
                        + ((code_unit - 0xD800) << 10)
                        + (low_unit - 0xDC00)
                    )
                )
                self.position = low_start + 6
            elif 0xDC00 <= code_unit <= 0xDFFF:
                raise self.error("unpaired low surrogate in JSON string", escape_start)
            else:
                decoded.append(chr(code_unit))
            segment_start = self.position
        raise self.error("unterminated JSON string")

    def parse_unicode_escape(self, escape_start: int) -> int:
        end = escape_start + 6
        if end > self.length:
            raise self.error("incomplete Unicode escape", escape_start)
        digits = self.source[escape_start + 2 : end]
        if len(digits) != 4 or any(digit not in _HEX_DIGITS for digit in digits):
            raise self.error("invalid Unicode escape", escape_start)
        return int(digits, 16)


def _validated_limit(
    value: int,
    *,
    name: str,
    minimum: int,
    maximum: int,
) -> int:
    if type(value) is not int or value < minimum or value > maximum:
        raise ValueError(f"{name} must be an integer in {minimum}..{maximum}")
    return value


def parse_json_bytes(
    source: bytes,
    *,
    max_depth: int = DEFAULT_MAX_DEPTH,
    max_nodes: int = DEFAULT_MAX_NODES,
    max_edges: int = DEFAULT_MAX_EDGES,
) -> JsonNode:
    """Parse one strict UTF-8 JSON document into immutable exact byte spans.

    Leading and trailing JSON whitespace belongs to the root node.  For a root
    scalar it is included in ``token``.  For a root container it extends the
    container ``prefix`` and ``suffix``.  Child values never absorb surrounding
    separator whitespace.
    """

    if type(source) is not bytes:
        raise TypeError("source must be exact bytes")
    max_depth = _validated_limit(
        max_depth,
        name="max_depth",
        minimum=1,
        maximum=HARD_MAX_DEPTH,
    )
    max_nodes = _validated_limit(
        max_nodes,
        name="max_nodes",
        minimum=1,
        maximum=HARD_MAX_NODES,
    )
    max_edges = _validated_limit(
        max_edges,
        name="max_edges",
        minimum=0,
        maximum=HARD_MAX_EDGES,
    )
    try:
        source.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise JsonSpanParseError("invalid UTF-8 JSON input", exc.start) from None
    return _Parser(
        source,
        max_depth=max_depth,
        max_nodes=max_nodes,
        max_edges=max_edges,
    ).parse()


__all__ = [
    "ByteSpan",
    "DEFAULT_MAX_DEPTH",
    "DEFAULT_MAX_EDGES",
    "DEFAULT_MAX_NODES",
    "HARD_MAX_DEPTH",
    "HARD_MAX_EDGES",
    "HARD_MAX_NODES",
    "JsonEdge",
    "JsonNode",
    "JsonSpanParseError",
    "parse_json_bytes",
]
