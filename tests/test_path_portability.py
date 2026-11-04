"""Native path regressions; all filesystem objects live in temporary directories."""

import json
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from nginx_qa.git_provider import ManagedGitProvider
from nginx_qa.managed_import import normalize_managed_runtime_config
from nginx_qa.paths import (
    configured_prompt_root,
    default_prompt_root,
    normalize_configured_path,
    protected_root_values,
    resolve_native_path,
)
from nginx_qa.sprint_types import (
    absolute_path_key,
    join_path,
    managed_activation_invariant_issues,
    managed_runtime_config_invariant_issues,
    path_is_strictly_within,
    path_is_within,
    paths_overlap,
    windows_absolute_path_key,
)
from tests.test_sprint_type_contract import (
    active_runtime_fixture,
    process_fixture,
    runtime_config_fixture,
)


def native_config(base):
    config = runtime_config_fixture()
    for key, value in config.items():
        if isinstance(value, str) and value.startswith("D:/nginx-qa-staging/"):
            config[key] = (base / value.removeprefix("D:/nginx-qa-staging/")).as_posix()
    config["protected_roots"] = [(base / "protected").as_posix()]
    return config


def environment_for(config):
    result = {}
    for key, value in config.items():
        if key in {
            "process_runtime_root",
            "log_root",
            "pid_root",
            "lease_root",
            "child_port_start",
            "child_port_end",
        }:
            continue
        result["NGINX_QA_" + key.upper()] = (
            json.dumps(value) if isinstance(value, (list, bool)) else str(value)
        )
    result["NGINX_QA_CHILD_PORT_RANGE"] = (
        f"{config['child_port_start']}-{config['child_port_end']}"
    )
    return result


def test_native_directory_configuration(tmp_path):
    config = native_config(tmp_path)
    assert normalize_managed_runtime_config(environment_for(config)) == config


def test_native_runtime_path_invariants(tmp_path):
    assert managed_runtime_config_invariant_issues(native_config(tmp_path)) == ()


def test_git_provider_rejects_symlink_ancestor(tmp_path):
    target = tmp_path / "target"
    target.mkdir()
    alias = tmp_path / "alias"
    try:
        alias.symlink_to(target, target_is_directory=True)
    except OSError:
        pytest.skip("symlink creation unavailable")
    with pytest.raises(ValueError):
        ManagedGitProvider(alias / "managed", {})
    assert not (target / "managed").exists()


@pytest.mark.skipif(os.name == "nt", reason="POSIX default on Linux")
def test_default_prompt_template_is_native():
    import main

    assert Path(main.DEFAULT_SEQUENTIAL_PROMPT_DIRECTORY_TEMPLATE).is_absolute()


@pytest.mark.parametrize(
    "value",
    [
        "relative",
        "C:relative",
        r"\rooted",
        r"\\server\share",
        r"\\?\C:\safe",
        r"\\.\C:\safe",
        r"C:\safe\CON",
        r"C:\safe\COM¹.txt",
        r"C:\PROGRA~1",
        r"C:\safe\data:stream",
        "/safe/../outside",
        "/safe/./child",
        "/safe//child",
        "//server/share",
        "/safe/child\x00",
        "/safe\\child",
        "C:/safe/../outside",
        "/safe/\nchild",
    ],
)
def test_invalid_path_spellings(value):
    assert absolute_path_key(value) is None
    with pytest.raises(ValueError):
        normalize_configured_path(value)


@pytest.mark.parametrize(
    "root,child,inside",
    [
        ("C:/safe", r"c:\SAFE\child", True),
        ("C:/safe", "C:/safe-escape", False),
        ("C:/safe", "D:/safe/child", False),
        ("C:/", "C:/child", True),
        ("/safe", "/safe/child", True),
        ("/safe", "/SAFE/child", False),
        ("/safe", "/safe-escape/child", False),
        ("/", "/safe", True),
        ("/safe", "C:/safe/child", False),
    ],
)
def test_containment_uses_components_and_path_flavor(root, child, inside):
    assert path_is_within(child, root) is inside
    assert path_is_strictly_within(child, root) is inside
    assert paths_overlap(child, root) is inside
    assert not path_is_strictly_within(root, root)


def test_legacy_windows_snapshot_and_join_are_host_independent():
    config = runtime_config_fixture()
    assert normalize_managed_runtime_config(environment_for(config)) == config
    assert managed_runtime_config_invariant_issues(config) == ()
    assert join_path("C:/runtime", "logs", "agent.log") == "C:/runtime/logs/agent.log"
    assert join_path("/runtime", "logs", "agent.log") == "/runtime/logs/agent.log"
    assert windows_absolute_path_key("/runtime") is None


def test_foreign_path_cannot_access_filesystem():
    foreign = "/var/managed" if os.name == "nt" else "D:/managed"
    with patch.object(
        Path, "lstat", side_effect=AssertionError("foreign filesystem probed")
    ):
        with pytest.raises(ValueError):
            resolve_native_path(foreign)
        with pytest.raises(ValueError):
            ManagedGitProvider(foreign, {})


@pytest.mark.parametrize(
    "mutation,issue",
    [
        ("prefix", None),
        ("protected", "RUNTIME_CONFIG_PROTECTED_ROOT_CONFLICT"),
        ("overlap", "RUNTIME_CONFIG_PATH_CONFLICT"),
        ("derived", "RUNTIME_CONFIG_DERIVED_ROOT_MISMATCH"),
        ("mixed", "RUNTIME_CONFIG_PATH_INVALID"),
    ],
)
def test_native_config_isolation(tmp_path, mutation, issue):
    config = native_config(tmp_path)
    if mutation == "prefix":
        config["protected_roots"] = [config["managed_root"] + "-sibling"]
    elif mutation == "protected":
        config["protected_roots"] = [config["managed_root"]]
    elif mutation == "overlap":
        config["prompt_root"] = config["managed_root"]
    elif mutation == "derived":
        config["log_root"] = str(tmp_path / "elsewhere")
    else:
        config["managed_root"] = "/managed" if os.name == "nt" else "C:/managed"
    issues = managed_runtime_config_invariant_issues(config)
    assert issue in issues if issue else issues == ()


@pytest.mark.parametrize(
    "raw", ["[]", "[1]", '{"root":"/safe"}', "null", "[broken", '"/safe"']
)
def test_invalid_protected_roots_fail_closed(raw):
    with pytest.raises(ValueError):
        # Exercise both parser and absolute-root validation.
        for value in protected_root_values(raw):
            normalize_configured_path(value)


def test_legacy_protected_root_lists():
    assert protected_root_values("D:/live;E:/other") == ["D:/live", "E:/other"]
    assert protected_root_values("D:/live") == ["D:/live"]
    if os.name != "nt":
        assert protected_root_values("/live:/other") == ["/live", "/other"]


def test_native_resolution_missing_tail_and_non_directory(tmp_path):
    candidate = tmp_path / "missing" / "child"
    assert resolve_native_path(candidate) == candidate
    assert not candidate.exists()
    with pytest.raises(FileNotFoundError):
        resolve_native_path(candidate, must_exist=True)
    regular = tmp_path / "file"
    regular.write_text("preserve")
    with pytest.raises(OSError):
        resolve_native_path(regular / "child")
    assert regular.read_text() == "preserve"


@pytest.mark.parametrize("broken", [False, True])
def test_configuration_rejects_symlink_ancestor_and_dangling_link(tmp_path, broken):
    target = tmp_path / "target"
    if not broken:
        target.mkdir()
    link = tmp_path / "link"
    try:
        link.symlink_to(target, target_is_directory=True)
    except OSError:
        pytest.skip("symlink creation unavailable")
    config = native_config(tmp_path)
    config["managed_root"] = str(link / "missing")
    with pytest.raises(ValueError):
        normalize_managed_runtime_config(environment_for(config))
    assert not (target / "missing").exists()


def test_reparse_ancestor_rejected_even_if_resolution_unchanged(tmp_path):
    actual = Path.lstat

    def attributes(path, *args, **kwargs):
        if path == tmp_path:
            return SimpleNamespace(st_mode=0o40700, st_file_attributes=0x400)
        return actual(path, *args, **kwargs)

    with (
        patch.object(Path, "lstat", attributes),
        pytest.raises(ValueError, match="reparse"),
    ):
        resolve_native_path(tmp_path / "missing")


def test_prompt_defaults_and_environment(tmp_path):
    assert default_prompt_root(platform="nt") == "D:/Prompt"
    assert default_prompt_root(platform="posix") == "~/.local/state/nginx-qa/prompts"
    assert (
        configured_prompt_root(
            {"NGINX_QA_SEQUENTIAL_PROMPT_ROOT": str(tmp_path / "prompt")}
        )
        == tmp_path / "prompt"
    )
    with pytest.raises(ValueError):
        configured_prompt_root({"NGINX_QA_SEQUENTIAL_PROMPT_ROOT": ""})
    with pytest.raises(ValueError):
        configured_prompt_root({"NGINX_QA_SEQUENTIAL_PROMPT_ROOT": "relative"})


def test_native_workspace_and_process_snapshot_paths(tmp_path):
    state = active_runtime_fixture(include_process_definition=True)
    state["processes"] = [process_fixture()]

    # Only translate path fields, leaving IDs, hashes and graph structure intact.
    def translate(value):
        if isinstance(value, dict):
            return {key: translate(item) for key, item in value.items()}
        if isinstance(value, list):
            return [translate(item) for item in value]
        if isinstance(value, str) and value.startswith("D:/"):
            return str(tmp_path / value[3:])
        if value == "C:/Python/python.exe":
            return str(tmp_path / "bin" / "python")
        return value

    state = translate(state)
    # These assertions isolate path-related contracts from process lifecycle setup.
    path_codes = {
        "RUNTIME_CONFIG_PATH_INVALID",
        "RUNTIME_CONFIG_DERIVED_ROOT_MISMATCH",
        "REPOSITORY_PROVENANCE_INVALID",
        "ASSIGNMENT_WORKSPACE_BINDING_INVALID",
        "ACTIVE_WORKSPACE_INCONSISTENT",
        "PROCESS_PATH_CONFIGURATION_MISMATCH",
        "PROCESS_LAUNCH_CONFIGURATION_MISMATCH",
    }
    assert not path_codes.intersection(managed_activation_invariant_issues(state))
    state["workspaces"][0]["actual_git_toplevel"] += "-escape"
    assert (
        "ASSIGNMENT_WORKSPACE_BINDING_INVALID"
        in managed_activation_invariant_issues(state)
    )


def test_prompt_resolution_rejects_redirect_before_writing(tmp_path):
    import main

    target = tmp_path / "target"
    target.mkdir()
    link = tmp_path / "link"
    try:
        link.symlink_to(target, target_is_directory=True)
    except OSError:
        pytest.skip("symlink creation unavailable")
    arguments = {
        "project_name": "safe",
        "git_address": "https://github.com/acme/safe.git",
        "git_context_key": "acme/safe",
    }
    with pytest.raises(main.HTTPException) as rejected:
        main.resolve_sequential_prompt_directory(
            str(link / "{repository}"), **arguments
        )
    assert rejected.value.status_code == 400
    with pytest.raises(main.HTTPException):
        main.resolve_sequential_agent_latest_file(
            str(link / "{repository}_{agent_phone}-latest.prompt"),
            agent_phone="1001",
            **arguments,
        )
    assert list(target.iterdir()) == []


def test_migration_protects_configured_prompt_default_without_probing(
    tmp_path, monkeypatch
):
    from nginx_qa.prompt_archive_migration import _configured_protected_roots

    root = tmp_path / "prompts"
    monkeypatch.setenv("NGINX_QA_SEQUENTIAL_PROMPT_ROOT", str(root))
    actual = Path.lstat

    def reject_protected(path, *args, **kwargs):
        if path == root or root in path.parents:
            raise AssertionError("protected root probed")
        return actual(path, *args, **kwargs)

    with patch.object(Path, "lstat", reject_protected):
        roots, _ = _configured_protected_roots(())
    assert root in roots


@pytest.mark.skipif(os.name != "nt", reason="real NTFS junction requires Windows")
def test_native_resolver_rejects_real_windows_junction(tmp_path):
    import subprocess

    target = tmp_path / "target"
    target.mkdir()
    junction = tmp_path / "junction"
    result = subprocess.run(
        ["cmd.exe", "/c", "mklink", "/J", str(junction), str(target)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    try:
        with pytest.raises(ValueError, match="reparse"):
            resolve_native_path(junction / "missing")
        with pytest.raises(ValueError):
            ManagedGitProvider(junction / "managed", {})
        assert list(target.iterdir()) == []
    finally:
        junction.rmdir()
