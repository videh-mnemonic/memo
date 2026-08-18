"""Manage and inspect Memo agent sandboxing."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import uuid
from dataclasses import replace
from pathlib import Path

from ...agents.sandbox.command import SandboxUnavailable, build_command, self_test
from ...agents.sandbox.config import (
    POLICY_NAME,
    Grant,
    expand_path,
    load_root_config,
    policy_path,
    reset_root_config,
    write_root_config,
)
from ...agents.sandbox.policy import resolve_policy
from ...daemon.client import ensure_daemon
from ...daemon.protocol import request
from ...recording.paths import StoragePaths
from ...recording.snapshots import utcnow

NAME = "sandbox"


def configure(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser(NAME, help="manage agent sandboxing")
    commands = parser.add_subparsers(dest="sandbox_command", required=True)

    show = commands.add_parser("show", help="show configured and effective policy")
    show.set_defaults(handler=_show)

    allow = commands.add_parser("allow", help="add a root-persistent mount")
    mode = allow.add_mutually_exclusive_group(required=True)
    mode.add_argument("--read", metavar="SOURCE")
    mode.add_argument("--read-write", metavar="SOURCE")
    allow.add_argument("--at", metavar="DESTINATION")
    allow.set_defaults(handler=_allow)

    disallow = commands.add_parser("disallow", help="remove a mount by sandbox destination")
    disallow.add_argument("destination")
    disallow.set_defaults(handler=_disallow)

    reset = commands.add_parser("reset", help="replace root policy with current defaults")
    reset.set_defaults(handler=_reset)

    shell = commands.add_parser("shell", help="enter the recorded sandbox environment")
    shell.set_defaults(handler=_shell)

    setup = commands.add_parser("setup", help="check Bubblewrap installation and compatibility")
    setup.add_argument("--force", action="store_true", help="rerun the compatibility self-test")
    setup.set_defaults(handler=_setup)


def _root(*, active_required: bool = False) -> Path:
    value = os.environ.get("MEMO_RECORDING_ROOT")
    if value:
        return Path(value).expanduser().resolve(strict=True)
    if active_required:
        raise RuntimeError("an active Memo recording is required")
    cwd = Path.cwd().resolve(strict=True)
    for candidate in (cwd, *cwd.parents):
        if (candidate / POLICY_NAME).is_file():
            return candidate
    return cwd


def _show(_args: argparse.Namespace) -> int:
    root = _root()
    config = load_root_config(root)
    policy = resolve_policy(root, Path.cwd() if Path.cwd().is_relative_to(root) else root)
    print(f"root: {root}")
    print(f"policy: {policy_path(root)}")
    print(f"network: {'shared' if config.network else 'isolated'}")
    print(f"gpu: {'enabled when available' if config.gpu else 'disabled'}")
    print("effective mounts:")
    for mount in policy.mounts:
        print(f"  {mount.mode}: {mount.source} -> {mount.destination} ({mount.purpose})")
    for path in policy.missing_optional:
        print(f"  configured: {path} (absent; ephemeral if created)")
    return 0


def _allow(args: argparse.Namespace) -> int:
    root = _root()
    config = load_root_config(root)
    source_value = args.read if args.read is not None else args.read_write
    mode = "read" if args.read is not None else "read-write"
    source = expand_path(source_value).absolute()
    destination = expand_path(args.at).absolute() if args.at else source
    grant = Grant(str(source), str(destination), mode)
    grants = tuple(item for item in config.grants if item.destination != grant.destination)
    write_root_config(root, replace(config, grants=(*grants, grant)))
    print(f"allowed {mode}: {source} -> {destination}")
    return 0


def _disallow(args: argparse.Namespace) -> int:
    root = _root()
    config = load_root_config(root)
    destination = expand_path(args.destination).absolute()
    grants = tuple(
        item for item in config.grants if expand_path(item.destination).absolute() != destination
    )
    home_entries = tuple(
        item
        for item in config.home_read_write_if_present
        if expand_path(item if item.startswith("/") else f"~/{item}").absolute() != destination
    )
    home_read_entries = tuple(
        item
        for item in config.home_read_only_if_present
        if expand_path(item if item.startswith("/") else f"~/{item}").absolute() != destination
    )
    if (
        grants == config.grants
        and home_entries == config.home_read_write_if_present
        and home_read_entries == config.home_read_only_if_present
    ):
        raise ValueError(f"sandbox destination is not configured: {destination}")
    write_root_config(
        root,
        replace(
            config,
            grants=grants,
            home_read_only_if_present=home_read_entries,
            home_read_write_if_present=home_entries,
        ),
    )
    print(f"disallowed: {destination}")
    return 0


def _reset(_args: argparse.Namespace) -> int:
    root = _root()
    reset_root_config(root)
    print(f"reset: {policy_path(root)}")
    return 0


def _setup(args: argparse.Namespace) -> int:
    try:
        identity = self_test(force=args.force)
    except SandboxUnavailable as error:
        print(f"memo: {error}", file=sys.stderr)
        return 1
    print(
        "sandbox ready: "
        f"{identity['bubblewrap']}; kernel {identity['kernel']}; Memo {identity['memo']}"
    )
    return 0


def _shell(_args: argparse.Namespace) -> int:
    root = _root(active_required=True)
    session_id = os.environ.get("MEMO_SESSION_ID")
    terminal_id = os.environ.get("MEMO_TERMINAL_ID")
    if not session_id or not terminal_id:
        raise RuntimeError("memo sandbox shell requires an active recorded terminal")
    paths = StoragePaths.discover()
    self_test(paths)
    shell = Path(os.environ.get("SHELL") or "/bin/sh").resolve(strict=True)
    policy = resolve_policy(root, Path.cwd(), executable=shell)
    command = build_command(policy, [str(shell)])
    launch_id = uuid.uuid4().hex
    ensure_daemon(paths)
    request(
        str(paths.socket),
        "sandbox_shell_launch",
        {
            "launch_id": launch_id,
            "session_id": session_id,
            "terminal_id": terminal_id,
            "cwd": str(Path.cwd()),
            "command": [str(shell)],
            "started_utc": utcnow(),
            "policy_summary": policy.summary(),
            "policy_digest": policy.digest(),
        },
    )
    process: subprocess.Popen[bytes] | None = None
    exit_code = 127
    try:
        process = subprocess.Popen(command, env=policy.environment)
        exit_code = process.wait()
    except KeyboardInterrupt:
        assert process is not None
        exit_code = process.wait()
    except OSError as error:
        print(f"memo: failed to launch sandbox shell: {error}", file=sys.stderr)
    finally:
        try:
            request(
                str(paths.socket),
                "sandbox_shell_complete",
                {
                    "launch_id": launch_id,
                    "ended_utc": utcnow(),
                    "exit_code": exit_code if process is None else process.returncode,
                },
                timeout=60,
            )
        except Exception as error:
            print(f"memo: sandbox shell completion capture failed: {error}", file=sys.stderr)
    return 128 - exit_code if exit_code < 0 else exit_code
