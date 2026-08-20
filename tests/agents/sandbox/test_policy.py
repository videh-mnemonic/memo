from __future__ import annotations

import os
import shlex
import subprocess
from dataclasses import replace
from pathlib import Path

import pytest

from memo.agents.sandbox.command import SandboxUnavailable, build_command, self_test
from memo.agents.sandbox.config import Grant, SandboxConfig, write_root_config
from memo.agents.sandbox.policy import resolve_policy
from memo.recording.paths import StoragePaths


def _config(*grants: Grant) -> SandboxConfig:
    return SandboxConfig(
        network=True,
        gpu=False,
        home_read_only_if_present=(),
        home_read_write_if_present=(),
        system_read_only=("/usr",),
        system_read_only_if_present=("/bin", "/lib", "/lib64"),
        environment_exclude=("*_TOKEN", "MEMO_*", "SSH_AUTH_SOCK"),
        grants=grants,
    )


def test_policy_filters_environment_and_keeps_root_boundary(tmp_path: Path, monkeypatch) -> None:
    home = tmp_path / "home"
    root = home / "project"
    root.mkdir(parents=True)
    shim = tmp_path / "shims"
    shim.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("PATH", f"{shim}{os.pathsep}/usr/bin")
    monkeypatch.setenv("MEMO_SHIM_DIR", str(shim))
    monkeypatch.setenv("SERVICE_TOKEN", "secret")
    monkeypatch.setenv("SAFE_VALUE", "kept")
    write_root_config(root, _config())

    policy = resolve_policy(root, root)

    assert "SERVICE_TOKEN" not in policy.environment
    assert policy.environment["SAFE_VALUE"] == "kept"
    assert policy.environment["PATH"] == "/usr/bin"
    assert policy.environment["HOME"] == str(home)
    assert any(item.destination == root and item.mode == "read-write" for item in policy.mounts)
    assert policy.mounts[-1].destination == root / ".memo-sandbox"
    assert "secret" not in str(policy.summary())

    destinations = {item.destination for item in policy.mounts}
    for system_path in (Path("/bin"), Path("/lib"), Path("/lib64")):
        if system_path.exists():
            assert system_path in destinations

    sibling = home / "sibling"
    sibling.mkdir()
    with pytest.raises(ValueError, match="outside recording root"):
        resolve_policy(root, sibling)


def test_policy_rejects_recording_root_that_exposes_memo_storage(
    tmp_path: Path, monkeypatch
) -> None:
    root = tmp_path / "project"
    memo_home = root / ".memo-home"
    root.mkdir()
    monkeypatch.setenv("MEMO_HOME", str(memo_home))
    write_root_config(root, _config())

    with pytest.raises(ValueError, match="overlaps Memo storage"):
        resolve_policy(root, root)


def test_linked_worktree_mounts_only_shared_git_metadata(tmp_path: Path, monkeypatch) -> None:
    home = tmp_path / "home"
    root = home / "worktree"
    common = home / "repository" / ".git"
    gitdir = common / "worktrees" / "worktree"
    root.mkdir(parents=True)
    gitdir.mkdir(parents=True)
    (root / ".git").write_text(f"gitdir: {gitdir}\n")
    (gitdir / "commondir").write_text("../..\n")
    monkeypatch.setenv("HOME", str(home))
    write_root_config(root, _config())

    policy = resolve_policy(root, root)
    purposes = {item.purpose: item.destination for item in policy.mounts}
    assert purposes["linked-worktree-gitdir"] == gitdir
    assert purposes["shared-git-metadata"] == common
    assert home / "repository" not in {item.destination for item in policy.mounts}


def test_command_is_fail_closed_and_uses_private_runtime(tmp_path: Path, monkeypatch) -> None:
    home = tmp_path / "home"
    root = home / "project"
    root.mkdir(parents=True)
    monkeypatch.setenv("HOME", str(home))
    write_root_config(root, _config())
    policy = resolve_policy(root, root)

    command = build_command(policy, ["/usr/bin/true"], executable="/usr/bin/bwrap")
    assert "--die-with-parent" in command
    assert "--unshare-pid" in command
    assert command[-2:] == ["--", "/usr/bin/true"]
    assert ("--tmpfs", str(home)) in set(zip(command, command[1:], strict=False))
    with pytest.raises(ValueError, match="unsupported"):
        build_command(
            policy,
            ["/usr/bin/true"],
            sandbox_args=["--bind", "/", "/"],
            executable="/usr/bin/bwrap",
        )


def test_self_test_mounts_dynamic_loader_paths(tmp_path: Path, monkeypatch) -> None:
    commands: list[list[str]] = []

    class Result:
        returncode = 0
        stdout = "bubblewrap 1.0\n"
        stderr = ""

    def run(command: list[str], **_kwargs: object) -> Result:
        commands.append(command)
        return Result()

    monkeypatch.setattr("memo.agents.sandbox.command.shutil.which", lambda _name: "/usr/bin/bwrap")
    monkeypatch.setattr("memo.agents.sandbox.command.subprocess.run", run)

    self_test(StoragePaths(tmp_path / "memo-home"), force=True)

    self_test_command = commands[-1]
    for library_directory in (Path("/lib"), Path("/lib64")):
        if library_directory.exists():
            mount = ("--ro-bind", str(library_directory), str(library_directory))
            arguments = zip(self_test_command, self_test_command[1:], self_test_command[2:])
            assert mount in set(arguments)


def test_home_npm_provider_mounts_only_its_package(tmp_path: Path, monkeypatch) -> None:
    home = tmp_path / "home"
    root = home / "project"
    package = home / ".local" / "lib" / "node_modules" / "@vendor" / "agent"
    executable = package / "bin" / "agent.js"
    executable.parent.mkdir(parents=True)
    executable.write_text("#!/usr/bin/env node\n")
    root.mkdir()
    monkeypatch.setenv("HOME", str(home))
    write_root_config(root, _config())

    policy = resolve_policy(root, root, provider="codex", executable=executable)
    installation = [item for item in policy.mounts if item.purpose == "provider-installation"]
    assert [(item.source, item.destination) for item in installation] == [(package, package)]
    assert home / ".local" not in {item.destination for item in policy.mounts}


def test_symlinked_cache_keeps_its_conventional_destination(tmp_path: Path, monkeypatch) -> None:
    home = tmp_path / "home"
    root = home / "project"
    cache_target = tmp_path / "cache-target"
    root.mkdir(parents=True)
    cache_target.mkdir()
    (home / ".cache").symlink_to(cache_target)
    monkeypatch.setenv("HOME", str(home))
    config = _config()
    write_root_config(root, replace(config, home_read_write_if_present=(".cache",)))

    policy = resolve_policy(root, root)
    cache = next(item for item in policy.mounts if item.purpose == "shared-cache")
    assert cache.source == cache_target
    assert cache.destination == home / ".cache"


def test_missing_optional_cache_is_not_created(tmp_path: Path, monkeypatch) -> None:
    home = tmp_path / "home"
    root = home / "project"
    root.mkdir(parents=True)
    monkeypatch.setenv("HOME", str(home))
    missing = home / ".missing-cache"
    write_root_config(
        root,
        replace(_config(), home_read_write_if_present=(".missing-cache",)),
    )

    policy = resolve_policy(root, root)
    assert missing in policy.missing_optional
    assert not missing.exists()


def test_optional_home_state_is_mounted_read_only(tmp_path: Path, monkeypatch) -> None:
    home = tmp_path / "home"
    root = home / "project"
    gh_config = home / ".config" / "gh"
    root.mkdir(parents=True)
    gh_config.mkdir(parents=True)
    monkeypatch.setenv("HOME", str(home))
    write_root_config(
        root,
        replace(_config(), home_read_only_if_present=(".config/gh", ".ssh")),
    )

    policy = resolve_policy(root, root)

    mount = next(item for item in policy.mounts if item.destination == gh_config)
    assert mount.mode == "read"
    assert mount.purpose == "shared-home-state"
    assert home / ".ssh" in policy.missing_optional


def test_live_bubblewrap_boundary_when_host_supports_it(tmp_path: Path, monkeypatch) -> None:
    paths = StoragePaths(tmp_path / "memo-home")
    try:
        self_test(paths, force=True)
    except SandboxUnavailable as error:
        pytest.skip(str(error))
    home = tmp_path / "home"
    root = home / "project"
    sibling = home / "sibling"
    cache = home / ".cache"
    readonly = tmp_path / "readonly.txt"
    root.mkdir(parents=True)
    sibling.mkdir()
    cache.mkdir()
    readonly.write_text("kept")
    secret = sibling / "secret"
    secret.write_text("hidden")
    (root / "escape").symlink_to(secret)
    monkeypatch.setenv("HOME", str(home))
    write_root_config(
        root,
        replace(
            _config(Grant(str(readonly), str(home / "readonly.txt"), "read")),
            home_read_write_if_present=(".cache",),
        ),
    )
    policy = resolve_policy(root, root)
    output = root / "created"
    cache_output = cache / "created"
    host_pid = os.getpid()
    shell = " && ".join(
        [
            f"test ! -e {shlex.quote(str(sibling))}",
            f"test ! -e {shlex.quote(str(root / 'escape'))}",
            f"test ! -e /proc/{host_pid}",
            f"! echo changed > {shlex.quote(str(home / 'readonly.txt'))}",
            f": > {shlex.quote(str(output))}",
            f": > {shlex.quote(str(cache_output))}",
        ]
    )
    command = build_command(
        policy,
        ["/bin/sh", "-c", shell],
    )
    assert subprocess.run(command, env=policy.environment).returncode == 0
    assert output.is_file()
    assert cache_output.is_file()
    assert output.stat().st_uid == os.getuid()
    assert readonly.read_text() == "kept"

    if policy.gpu:
        gpu = build_command(policy, ["/usr/bin/nvidia-smi", "-L"])
        assert subprocess.run(gpu, env=policy.environment).returncode == 0
