import dataclasses
import unittest

from nginx_qa.prompt_archive_json_spans import (
    ByteSpan,
    JsonSpanParseError,
    parse_json_bytes,
)


def render_node(source: bytes, node) -> bytes:
    if node.node_kind == "scalar":
        return node.token.bytes_from(source)
    output = bytearray(node.prefix.bytes_from(source))
    for edge in node.edges:
        output.extend(edge.separator_before.bytes_from(source))
        if edge.edge_kind == "object_member":
            output.extend(edge.key_token.bytes_from(source))
            output.extend(edge.key_value_separator.bytes_from(source))
        output.extend(render_node(source, edge.value))
    output.extend(node.suffix.bytes_from(source))
    return bytes(output)


class PromptArchiveJsonSpanTests(unittest.TestCase):
    def assert_parse_error(
        self,
        source: bytes,
        *,
        reason: str | None = None,
        **limits,
    ) -> JsonSpanParseError:
        with self.assertRaises(JsonSpanParseError) as raised:
            parse_json_bytes(source, **limits)
        if reason is not None:
            self.assertEqual(raised.exception.reason, reason)
        self.assertIs(type(raised.exception.byte_offset), int)
        payload_prefix = source[:16].decode("utf-8", "ignore")
        if payload_prefix:
            self.assertNotIn(payload_prefix, str(raised.exception))
        return raised.exception

    def test_multibyte_object_offsets_and_all_lexical_spans_are_exact(self) -> None:
        source = (
            ' \r\n{\n  "ключ" \t: [ "é", {"a" : 1} ],\n'
            '  "emoji": "😀"\n}\t '
        ).encode("utf-8")
        root = parse_json_bytes(source)

        self.assertEqual(root.node_kind, "object")
        self.assertEqual((root.start, root.end), (0, len(source)))
        self.assertEqual(root.prefix.bytes_from(source), ' \r\n{\n  '.encode())
        self.assertEqual(root.suffix.bytes_from(source), b"\n}\t ")
        self.assertEqual([edge.member_name for edge in root.edges], ["ключ", "emoji"])
        self.assertEqual(root.edges[0].key_token.bytes_from(source), '"ключ"'.encode())
        self.assertEqual(root.edges[0].key_value_separator.bytes_from(source), b" \t: ")
        self.assertEqual(root.edges[1].separator_before.bytes_from(source), b",\n  ")

        values = root.edges[0].value
        self.assertEqual(values.node_kind, "array")
        self.assertEqual(values.prefix.bytes_from(source), b"[ ")
        self.assertEqual(values.edges[1].separator_before.bytes_from(source), b", ")
        self.assertEqual(values.suffix.bytes_from(source), b" ]")
        self.assertEqual(
            values.edges[0].value.token.bytes_from(source),
            '"é"'.encode("utf-8"),
        )
        self.assertEqual(render_node(source, root), source)

    def test_root_scalar_token_owns_outer_whitespace(self) -> None:
        for source, scalar_kind in (
            (b" \t null\r\n", "null"),
            (b"\n true ", "boolean"),
            (b" -0.00e+10\t", "number"),
            (b' \t "value"\r\n', "string"),
        ):
            with self.subTest(source=source):
                root = parse_json_bytes(source)
                self.assertEqual(root.scalar_kind, scalar_kind)
                self.assertEqual(root.token, ByteSpan(0, len(source)))
                self.assertEqual(render_node(source, root), source)

    def test_empty_containers_partition_outer_and_inner_whitespace(self) -> None:
        for source, kind, prefix, suffix in (
            (b" \n { \t } \r", "object", b" \n { \t ", b"} \r"),
            (b"\t[\r\n]\n", "array", b"\t[\r\n", b"]\n"),
        ):
            with self.subTest(source=source):
                root = parse_json_bytes(source)
                self.assertEqual(root.node_kind, kind)
                self.assertEqual(root.entry_count, 0)
                self.assertEqual(root.prefix.bytes_from(source), prefix)
                self.assertEqual(root.suffix.bytes_from(source), suffix)
                self.assertEqual(render_node(source, root), source)

    def test_array_and_object_separators_have_half_open_byte_boundaries(self) -> None:
        array_source = b"[\n 1 , \t2\r\n]"
        array = parse_json_bytes(array_source)
        self.assertEqual(array.prefix.bytes_from(array_source), b"[\n ")
        self.assertEqual(array.edges[0].separator_before.byte_size, 0)
        self.assertEqual(
            array.edges[1].separator_before.bytes_from(array_source), b" , \t"
        )
        self.assertEqual(array.suffix.bytes_from(array_source), b"\r\n]")
        self.assertEqual(array.edges[0].end, array.edges[1].start)

        object_source = b'{ "a" \t:\n 1 ,\r\n "b":2 }'
        obj = parse_json_bytes(object_source)
        self.assertEqual(obj.prefix.bytes_from(object_source), b"{ ")
        self.assertEqual(obj.edges[0].key_value_separator.bytes_from(object_source), b" \t:\n ")
        self.assertEqual(obj.edges[1].separator_before.bytes_from(object_source), b" ,\r\n ")
        self.assertEqual(obj.suffix.bytes_from(object_source), b" }")
        self.assertEqual(render_node(object_source, obj), object_source)

    def test_duplicate_names_are_compared_after_escape_decoding(self) -> None:
        cases = (
            b'{"a":1,"\\u0061":2}',
            '{"😀":1,"\\ud83d\\ude00":2}'.encode("utf-8"),
            b'{"\\/":1,"/":2}',
        )
        for source in cases:
            with self.subTest(source=source):
                self.assert_parse_error(source, reason="duplicate JSON object member")

    def test_member_names_are_not_unicode_normalized(self) -> None:
        source = '{"é":1,"e\\u0301":2}'.encode("utf-8")
        root = parse_json_bytes(source)
        self.assertEqual([edge.member_name for edge in root.edges], ["é", "é"])
        self.assertEqual(render_node(source, root), source)

    def test_valid_surrogate_pair_is_decoded_for_member_identity(self) -> None:
        source = b'{"\\ud83d\\ude00":true}'
        root = parse_json_bytes(source)
        self.assertEqual(root.edges[0].member_name, "😀")
        self.assertEqual(root.edges[0].key_token.bytes_from(source), b'"\\ud83d\\ude00"')

    def test_invalid_utf8_is_rejected_at_the_exact_byte_offset(self) -> None:
        error = self.assert_parse_error(
            b'{"a":"\xff"}',
            reason="invalid UTF-8 JSON input",
        )
        self.assertEqual(error.byte_offset, 6)

    def test_invalid_escapes_and_surrogates_are_rejected(self) -> None:
        cases = (
            (b'"\\x"', "invalid JSON string escape"),
            (b'"\\u12"', "incomplete Unicode escape"),
            (b'"\\u12xz"', "invalid Unicode escape"),
            (b'"\\ud800"', "high surrogate is not followed by a low surrogate"),
            (b'"\\ud800\\u0041"', "high surrogate is not followed by a low surrogate"),
            (b'"\\udc00"', "unpaired low surrogate in JSON string"),
        )
        for source, reason in cases:
            with self.subTest(source=source):
                self.assert_parse_error(source, reason=reason)

    def test_raw_controls_and_unterminated_strings_are_rejected(self) -> None:
        self.assert_parse_error(
            b'"line\nfeed"',
            reason="unescaped control character in JSON string",
        )
        self.assert_parse_error(b'"unterminated', reason="unterminated JSON string")
        self.assert_parse_error(
            b'"escape\\',
            reason="unterminated JSON string escape",
        )

    def test_nonstandard_and_malformed_numbers_are_rejected(self) -> None:
        invalid = (
            b"NaN",
            b"Infinity",
            b"-Infinity",
            b"+1",
            b"01",
            b"-01",
            b"1.",
            b"1e",
            b"1e+",
            b"[0,01]",
        )
        for source in invalid:
            with self.subTest(source=source):
                self.assert_parse_error(source)

    def test_trailing_data_commas_and_missing_delimiters_are_rejected(self) -> None:
        invalid = (
            b"null false",
            b"{}{}",
            b"[1,]",
            b'{"a":1,}',
            b"[1 2]",
            b'{"a" 1}',
            b'{a:1}',
        )
        for source in invalid:
            with self.subTest(source=source):
                self.assert_parse_error(source)

    def test_empty_or_whitespace_only_documents_are_rejected(self) -> None:
        for source in (b"", b" \t\r\n"):
            self.assert_parse_error(source, reason="JSON document is empty")

    def test_depth_node_and_edge_limits_fail_before_excess_is_accepted(self) -> None:
        source = b'[[{"a":0}]]'
        parsed = parse_json_bytes(source, max_depth=4, max_nodes=4, max_edges=3)
        self.assertEqual(render_node(source, parsed), source)

        self.assert_parse_error(
            source,
            max_depth=3,
            max_nodes=4,
            max_edges=3,
            reason="maximum JSON depth exceeded",
        )
        self.assert_parse_error(
            source,
            max_depth=4,
            max_nodes=3,
            max_edges=3,
            reason="maximum JSON node count exceeded",
        )
        self.assert_parse_error(
            source,
            max_depth=4,
            max_nodes=4,
            max_edges=2,
            reason="maximum JSON edge count exceeded",
        )

    def test_limits_are_exact_bounded_integers(self) -> None:
        for kwargs in (
            {"max_depth": True},
            {"max_depth": 0},
            {"max_depth": 129},
            {"max_nodes": 0},
            {"max_nodes": 1_000_001},
            {"max_edges": -1},
            {"max_edges": 1_000_001},
        ):
            with self.subTest(kwargs=kwargs):
                with self.assertRaises(ValueError):
                    parse_json_bytes(b"null", **kwargs)

    def test_source_requires_exact_immutable_bytes(self) -> None:
        for source in (bytearray(b"null"), memoryview(b"null"), "null"):
            with self.subTest(type=type(source).__name__):
                with self.assertRaises(TypeError):
                    parse_json_bytes(source)  # type: ignore[arg-type]

    def test_nodes_edges_and_spans_are_immutable(self) -> None:
        source = b'{"a":[1]}'
        root = parse_json_bytes(source)
        with self.assertRaises(dataclasses.FrozenInstanceError):
            root.start = 1  # type: ignore[misc]
        with self.assertRaises(dataclasses.FrozenInstanceError):
            root.edges[0].member_name = "b"  # type: ignore[misc]
        with self.assertRaises(dataclasses.FrozenInstanceError):
            root.prefix.start = 1  # type: ignore[misc]

    def test_byte_span_rejects_invalid_ranges_and_wrong_sources(self) -> None:
        for start, end in ((-1, 0), (2, 1), (True, 1)):
            with self.subTest(start=start, end=end):
                with self.assertRaises(ValueError):
                    ByteSpan(start, end)
        with self.assertRaises(TypeError):
            ByteSpan(0, 1).bytes_from(bytearray(b"x"))  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            ByteSpan(0, 2).bytes_from(b"x")


if __name__ == "__main__":
    unittest.main()
