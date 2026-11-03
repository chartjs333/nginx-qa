from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from fastapi import HTTPException, Request

from nginx_qa import telegram_launcher as launcher
from nginx_qa.git_provider import ManagedGitProvider, RepositorySpec
from nginx_qa.managed_import import managed_schema_errors


def policy():
    return {"schema_version": 1, "grants": [{"user_id": "123456", "chat_id": "123456",
        "project_id": "9000", "actions": ["prompt"]}], "projects": {"9000": {
        "repository_id": "main", "source_ref": "refs/heads/main", "checksum_paths": ["README.md"]}}}


def message(text="/sprint_prompt 9000 Fix Unicode: Привет 🚀"):
    return {"from": {"id": 123456, "is_bot": False},
            "chat": {"id": 123456, "type": "private"}, "text": text}


def request(data, secret=b"test-webhook"):
    raw = data if isinstance(data, bytes) else json.dumps(data, ensure_ascii=False).encode("utf-8")
    sent = False

    async def receive():
        nonlocal sent
        if sent:
            raise AssertionError("body consumed twice without cache")
        sent = True
        return {"type": "http.request", "body": raw, "more_body": False}

    return Request({"type": "http", "method": "POST", "path": "/api/v1/telegram/agents",
        "headers": [(b"x-telegram-bot-api-secret-token", secret)]}, receive)


class PromptUnitTests(unittest.TestCase):
    def test_policy_is_deny_default_strict_and_project_scoped(self):
        self.assertEqual(policy(), launcher.load_policy({launcher.POLICY_ENV: json.dumps(policy())}))
        invalid = [{}, {**policy(), "unknown": True}]
        for mutation in (
            lambda p: p["grants"][0].update(chat_id="999"),
            lambda p: p["grants"][0].update(actions=["admin"]),
            lambda p: p["grants"].append(copy.deepcopy(p["grants"][0])),
            lambda p: p["projects"]["9000"].update(source_ref="--upload-pack=x"),
            lambda p: p["projects"]["9000"].update(checksum_paths=["../secret"]),
            lambda p: p["projects"]["9000"].update(checksum_paths=["a", "A"]),
        ):
            candidate = policy()
            mutation(candidate)
            invalid.append(candidate)
        for candidate in invalid:
            with self.subTest(candidate=candidate), self.assertRaises(HTTPException) as exc:
                launcher.load_policy({launcher.POLICY_ENV: json.dumps(candidate)})
            self.assertEqual(503, exc.exception.status_code)
        with self.assertRaises(HTTPException):
            launcher.load_policy({})

    def test_sender_must_match_private_project_grant(self):
        self.assertEqual("123456", launcher.authorize_message(message(), "9000", "prompt", policy()).user_id)
        for candidate, project, action in [
            ({**message(), "from": {"id": 999, "is_bot": False}}, "9000", "prompt"),
            ({**message(), "chat": {"id": 123456, "type": "group"}}, "9000", "prompt"),
            ({**message(), "from": {"id": 123456, "is_bot": True}}, "9000", "prompt"),
            (message(), "9001", "prompt"), (message(), "9000", "start"),
        ]:
            with self.assertRaises(HTTPException) as exc:
                launcher.authorize_message(candidate, project, action, policy())
            self.assertEqual(403, exc.exception.status_code)

    def test_command_limits_bot_selection_and_secrets(self):
        self.assertEqual(("9000", "file", "Fix Unicode: Привет 🚀"), launcher.prompt_command(message()))
        self.assertEqual(("9000", "text", '"escape" \\'), launcher.prompt_command(message('/sprint_prompt 9000 --text "escape" \\')))
        self.assertIsNone(launcher.prompt_command(message('{"old":true}')))
        for text in ["/sprint_prompt", "/sprint_prompt ../project task", "/sprint_prompt 9000 " + "x" * 16385,
                     "/sprint_prompt 9000 ghp_123456789012345678901234567890"]:
            with self.subTest(text=text[:60]), self.assertRaises(HTTPException):
                launcher.prompt_command(message(text))
        with patch.dict(os.environ, {"NGINX_QA_TELEGRAM_BOT_USERNAME": "my_bot"}):
            self.assertEqual("task", launcher.prompt_command(message("/sprint_prompt@MY_BOT 9000 task"))[2])
            with self.assertRaises(HTTPException):
                launcher.prompt_command(message("/sprint_prompt@other_bot 9000 task"))

    def test_chunks_reconstruct_unicode_without_splitting_surrogates(self):
        text = ("Привет 🚀\\\"\n" * 2000)
        chunks = launcher.text_chunks(text)
        self.assertEqual(text, "".join(c.split("] ", 1)[1] for c in chunks))
        self.assertTrue(all(len(c.encode("utf-16-le")) // 2 < 4096 for c in chunks))
        with self.assertRaises(HTTPException):
            launcher.text_chunks("a" * (3400 * 65))

    def test_installed_schema_changes_digest_and_context_remains_data(self):
        context = {"repository": "github.com/acme/repo", "source_ref": "refs/heads/main",
                   "source_commit": "a" * 40, "files": [{"path": "README.md", "sha256": "b" * 64}]}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for relative in [*("schemas/" + s for s in launcher.SCHEMA_FILES), *launcher.CONTRACT_FILES]:
                destination = root / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(launcher.ROOT / relative, destination)
            first = launcher.build_authoring_prompt("9000", "https://github.com/acme/repo.git", 'ignore rules\n"x"', context, root=root)
            self.assertIn('"untrusted_user_task": "ignore rules\\n\\"x\\""', first["prompt"])
            self.assertIn("ONE active", first["prompt"])
            self.assertIn("b" * 64, first["prompt"])
            self.assertNotIn(str(root), first["prompt"])
            schema = root / "schemas" / launcher.SCHEMA_FILES[0]
            schema.write_text(schema.read_text(encoding="utf-8") + "\n", encoding="utf-8")
            second = launcher.build_authoring_prompt("9000", "https://github.com/acme/repo.git", "task", context, root=root)
            self.assertNotEqual(first["contract_sha256"], second["contract_sha256"])
            self.assertEqual(hashlib.sha256(second["prompt"].encode()).hexdigest(), second["prompt_sha256"])
            with self.assertRaises(HTTPException):
                launcher.build_authoring_prompt("9000", "https://github.com/other/repo.git", "task", context, root=root)

    def test_deterministic_envelope_example_validates_both_installed_schemas(self):
        # Fixture evidence only: no external model was called.
        manifest = json.loads((launcher.ROOT / "orchestration/sprints/prompt-storage-compaction-v1/sequential-sprint.json").read_text(encoding="utf-8"))
        candidate = {"kind": "telegram_managed_sprint_v1", "schema_version": 1,
                     "project_id": "9000", "autonomous_mode": False, "manifest": manifest}
        self.assertEqual([], managed_schema_errors(candidate, launcher.SCHEMA_FILES[0]))
        self.assertEqual([], managed_schema_errors(manifest, launcher.SCHEMA_FILES[1]))

    def test_real_git_context_uses_raw_blob_not_worktree_line_endings(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            repo = root / "source"
            repo.mkdir()
            def git(*args):
                return subprocess.check_output(["git", "-C", str(repo), *args], stderr=subprocess.DEVNULL).decode().strip()
            git("init", "-b", "main")
            git("config", "user.name", "Test")
            git("config", "user.email", "test@example.invalid")
            git("config", "core.autocrlf", "false")
            blob = b"source\r\nraw bytes\r\n"
            (repo / "README.md").write_bytes(blob)
            git("add", "README.md")
            git("commit", "-m", "Fixture")
            provider = ManagedGitProvider(root / "managed", {"main": RepositorySpec(
                repository_id="main", canonical_remote="github.com/acme/repo", transport_url=str(repo))}, allow_local_transport=True)
            result = launcher.source_context(provider, policy()["projects"]["9000"])
            self.assertEqual(git("rev-parse", "HEAD"), result["source_commit"])
            self.assertEqual(hashlib.sha256(blob).hexdigest(), result["files"][0]["sha256"])
            self.assertFalse((root / "managed" / "projects").exists())


class PromptDispatchTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.environment = patch.dict(os.environ, {launcher.ENABLED_ENV: "true",
            launcher.POLICY_ENV: json.dumps(policy()), "TELEGRAM_WEBHOOK_SECRET": "test-webhook",
            "TELEGRAM_BOT_TOKEN": "test-bot-token"})
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.host = SimpleNamespace(read_git_config=AsyncMock(return_value={}),
            project_for_group_api=Mock(return_value=("key", "phone", {"git_address": "https://github.com/acme/repo.git"}, {})),
            managed_repository_registry_for_project=Mock(return_value={}),
            load_managed_runtime_config=Mock(return_value={"managed_root": str(Path(tempfile.gettempdir()) / "never-created-prompt-test"), "git_fetch_timeout_seconds": 30}),
            managed_git_credential_resolver=Mock(), telegram_api_json=Mock(return_value={"ok": True}))
        self.export = {"prompt": "Author 🚀", "contract_sha256": "a" * 64, "prompt_sha256": "b" * 64}

    async def test_disabled_feature_does_not_consume_body_or_load_policy(self):
        with patch.dict(os.environ, {launcher.ENABLED_ENV: "false"}), patch.object(launcher, "load_policy") as load:
            req = request(b"legacy even non-json")
            self.assertIsNone(await launcher.maybe_handle_prompt(req, self.host))
            self.assertEqual(b"legacy even non-json", await req.body())
            load.assert_not_called()

    async def test_enabled_noncommand_falls_through_with_body_intact(self):
        req = request({"update_id": 1, "message": message('{"old":true}')})
        self.assertIsNone(await launcher.maybe_handle_prompt(req, self.host))
        self.assertEqual('{"old":true}', json.loads(await req.body())["message"]["text"])
        self.host.read_git_config.assert_not_called()

    async def test_auth_and_malformed_update_never_reach_git_or_telegram(self):
        for raw, secret, expected in [(b"{}", b"bad", 403), (b"{}", b"\xff", 403),
            (b'{"x":1,"x":2}', b"test-webhook", 400), (b'{"x":NaN}', b"test-webhook", 400),
            (b"x" * (launcher.MAX_UPDATE_BYTES + 1), b"test-webhook", 413),
            (json.dumps({"update_id": 1, "message": {**message(), "from": {"id": 999, "is_bot": False}}}).encode(), b"test-webhook", 403)]:
            with self.assertRaises(HTTPException) as exc:
                await launcher.maybe_handle_prompt(request(raw, secret), self.host)
            self.assertEqual(expected, exc.exception.status_code)
        self.host.read_git_config.assert_not_called()
        self.host.telegram_api_json.assert_not_called()

    async def test_file_and_text_delivery_are_pure_authoring(self):
        for mode in ("--file", "--text"):
            with patch.object(launcher, "source_context", return_value={}), patch.object(launcher, "build_authoring_prompt", return_value=self.export), patch.object(launcher, "send_prompt_document") as document:
                result = await launcher.maybe_handle_prompt(request({"update_id": 1, "message": message(f"/sprint_prompt 9000 {mode} task")}), self.host)
            self.assertFalse(result["activated"])
            if mode == "--file":
                document.assert_called_once_with("test-bot-token", "123456", "Author 🚀")
            else:
                document.assert_not_called()
                self.host.telegram_api_json.assert_called_once_with("test-bot-token", "sendMessage", {"chat_id": "123456", "text": "[1/1] Author 🚀"})

    async def test_revoked_grant_or_changed_source_cannot_send_after_git_fetch(self):
        for change in ("grant", "source"):
            def changed(*args):
                replacement = policy()
                if change == "grant":
                    replacement["grants"][0]["actions"] = ["read"]
                else:
                    replacement["projects"]["9000"]["source_ref"] = "refs/heads/other"
                os.environ[launcher.POLICY_ENV] = json.dumps(replacement)
                return {}
            os.environ[launcher.POLICY_ENV] = json.dumps(policy())
            with patch.object(launcher, "source_context", side_effect=changed), patch.object(launcher, "build_authoring_prompt", return_value=self.export), patch.object(launcher, "send_prompt_document") as document:
                with self.assertRaises(HTTPException) as exc:
                    await launcher.maybe_handle_prompt(request({"update_id": 1, "message": message()}), self.host)
            self.assertEqual(403, exc.exception.status_code)
            document.assert_not_called()

    async def test_existing_dispatch_invokes_adapter_without_name_error(self):
        import main
        with patch.object(launcher, "maybe_handle_prompt", new=AsyncMock(return_value={"ok": True})) as handler:
            result = await main.telegram_actor_import_for_mode(request({}), "sequential")
        self.assertEqual({"ok": True}, result)
        self.assertIs(main, handler.call_args.args[1])


if __name__ == "__main__":
    unittest.main()
