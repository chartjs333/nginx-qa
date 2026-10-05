"""Offline migrator safety; all paths are private temporary fixtures."""
from contextlib import redirect_stdout, redirect_stderr
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest

from nginx_qa.scope_control_migrate import main, migrate_document
from nginx_qa.scope_control_prestart import CompatibilityError


class ScopeMigrationTests(unittest.TestCase):
    def test_copy_out_requires_hash_and_never_overwrites(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source.json"
            output = root / "migrated.json"
            data = b'{"projects":{"example":{"project_phone":"1000"}}}'
            source.write_bytes(data)
            checksum = hashlib.sha256(data).hexdigest()
            arguments = ["--state-file", str(source), "--output", str(output),
                "--expected-sha256", checksum, "--project", "1000", "--writers-stopped"]
            with redirect_stdout(io.StringIO()):
                self.assertEqual(main(arguments), 0)
            self.assertEqual(source.read_bytes(), data)
            self.assertEqual(json.loads(output.read_bytes()), json.loads(data))
            with self.assertRaises(FileExistsError), redirect_stdout(io.StringIO()):
                main(arguments)
            self.assertEqual(source.read_bytes(), data)
            self.assertEqual(json.loads(output.read_bytes()), json.loads(data))

    def test_in_place_wrong_hash_or_missing_freeze_assertion_fail_before_output(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source, output = root / "source.json", root / "output.json"
            data = b'{"projects":{}}'
            source.write_bytes(data)
            checksum = hashlib.sha256(data).hexdigest()
            cases = [
                ["--state-file", str(source), "--output", str(source), "--expected-sha256", checksum, "--writers-stopped"],
                ["--state-file", str(source), "--output", str(output), "--expected-sha256", "0" * 64, "--writers-stopped"],
                ["--state-file", str(source), "--output", str(output), "--expected-sha256", checksum],
            ]
            for arguments in cases:
                arguments.extend(["--project", "1000"])
                with self.subTest(arguments=arguments), self.assertRaises(SystemExit), redirect_stderr(io.StringIO()):
                    main(arguments)
                self.assertFalse(output.exists())
                self.assertEqual(source.read_bytes(), data)

    def test_unsupported_or_corrupt_scope_is_not_migrated(self):
        for control in ({"schema_version": 3}, {"schema_version": 1, "minimum_runtime_capability": "legacy_scope_control_v1", "amendments": []}):
            document = {"projects": {"example": {"agent_assignment": {"scope_control": control}}}}
            before = json.dumps(document, sort_keys=True)
            with self.assertRaises(CompatibilityError):
                migrate_document(document, migrated_at="test", source_sha256="0" * 64)
            self.assertEqual(json.dumps(document, sort_keys=True), before)


if __name__ == "__main__":
    unittest.main()
