"""Intercept supported agent invocations and link their lifecycles to Memo sessions."""

from __future__ import annotations

import os
import shlex
import shutil
import stat
import subprocess
import sys
import uuid
from dataclasses import replace
from pathlib import Path

from ..daemon.client import ensure_daemon
from ..daemon.protocol import request
from ..recording.filesystem import atomic_write
from ..recording.paths import StoragePaths
from ..recording.snapshots import utcnow
from .harnesses import get_harness, registered_harnesses
from .sandbox.command import SandboxUnavailable, build_command, self_test
from .sandbox.guidance import effective_provider_args, guidance_digest
from .sandbox.policy import resolve_policy


def ensure_shims(paths: StoragePaths | None = None) -> Path:
    paths = paths or StoragePaths.discover()
    paths.ensure_storage()
    directory = paths.runtime / "shims"
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    directory.chmod(0o700)
    names: set[str] = set()
    for harness in registered_harnesses():
        name = harness.executable
        if not name or Path(name).name != name or name in names:
            raise ValueError(f"unsafe or duplicate harness executable: {name}")
        names.add(name)
        body = (
            "#!/bin/sh\n"
            f'exec {shlex.quote(sys.executable)} -m memo.agents.shim {shlex.quote(harness.name)} "$@"\n'
        ).encode()
        destination = directory / name
        atomic_write(destination, body)
        destination.chmod(stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
    return directory


def _real_executable(name: str, shim_directory: Path) -> str | None:
    entries = []
    for value in os.environ.get("PATH", "").split(os.pathsep):
        try:
            if Path(value).resolve() == shim_directory.resolve():
                continue
        except OSError:
            pass
        entries.append(value)
    return shutil.which(name, path=os.pathsep.join(entries))


def _memo_arguments(args: list[str]) -> tuple[list[str], bool, list[str]]:
    provider: list[str] = []
    no_sandbox = False
    for index, value in enumerate(args):
        if value == "--":
            provider.extend(args[index:])
            break
        if value == "--no-sandbox":
            if no_sandbox:
                raise ValueError("--no-sandbox may be specified only once")
            no_sandbox = True
            continue
        if value == "--sandbox-args":
            sandbox = args[index + 1 :]
            if no_sandbox or "--no-sandbox" in sandbox:
                raise ValueError("--no-sandbox cannot be combined with --sandbox-args")
            if "--sandbox-args" in sandbox:
                raise ValueError("--sandbox-args may be specified only once")
            return provider, no_sandbox, sandbox
        provider.append(value)
    else:
        return provider, no_sandbox, []
    return provider, no_sandbox, []


def run(harness_name: str, args: list[str]) -> int:
    harness = get_harness(harness_name)
    shim_directory = Path(os.environ.get("MEMO_SHIM_DIR", ""))
    executable = _real_executable(harness.executable, shim_directory)
    if executable is None:
        print(f"memo: executable not found: {harness.executable}", file=sys.stderr)
        return 127

    try:
        provider_args, no_sandbox, sandbox_args = _memo_arguments(args)
    except ValueError as error:
        print(f"memo: {error}", file=sys.stderr)
        return 2

    session_id = os.environ.get("MEMO_SESSION_ID")
    terminal_id = os.environ.get("MEMO_TERMINAL_ID")
    paths = StoragePaths.discover()
    sandboxed = bool(session_id and terminal_id and not no_sandbox)
    policy = None
    effective_args = list(provider_args)
    process_environment = os.environ.copy()
    if sandboxed:
        root_value = os.environ.get("MEMO_RECORDING_ROOT")
        if not root_value:
            print("memo: active recording root is unavailable; use --no-sandbox", file=sys.stderr)
            return 1
        try:
            self_test(paths)
            effective_args = effective_provider_args(harness.name, provider_args)
            sandbox_executable = Path(executable).resolve(strict=True)
            policy = resolve_policy(
                Path(root_value),
                Path.cwd(),
                provider=harness.name,
                executable=sandbox_executable,
            )
            if "--unshare-net" in sandbox_args:
                policy = replace(policy, network=False)
            process_command = build_command(
                policy,
                [str(sandbox_executable), *effective_args],
                sandbox_args=sandbox_args,
            )
            if sandbox_args:
                print(f"memo: custom sandbox command: {shlex.join(process_command)}", file=sys.stderr)
            process_environment = policy.environment
        except (OSError, ValueError, SandboxUnavailable) as error:
            print(f"memo: sandbox unavailable: {error}", file=sys.stderr)
            return 1
    else:
        if sandbox_args:
            print("memo: --sandbox-args requires an active sandboxed Memo session", file=sys.stderr)
            return 2
        process_command = [executable, *effective_args]

    launch_id = uuid.uuid4().hex
    notified = False
    if session_id and terminal_id:
        try:
            ensure_daemon(paths)
            request(
                str(paths.socket),
                "agent_launch",
                {
                    "launch_id": launch_id,
                    "session_id": session_id,
                    "terminal_id": terminal_id,
                    "harness": harness.name,
                    "cwd": str(Path.cwd()),
                    "command": [harness.executable, *provider_args],
                    "effective_command": [harness.executable, *effective_args],
                    "sandbox_mode": (
                        "custom"
                        if sandboxed and sandbox_args
                        else "sandbox"
                        if sandboxed
                        else "no-sandbox"
                    ),
                    "sandbox_args": sandbox_args,
                    "policy_summary": None if policy is None else policy.summary(),
                    "policy_digest": None if policy is None else policy.digest(),
                    "guidance_digest": guidance_digest() if sandboxed else None,
                    "started_utc": utcnow(),
                },
            )
            notified = True
        except Exception as error:
            print(f"memo: agent capture unavailable: {error}", file=sys.stderr)

    process: subprocess.Popen[bytes] | None = None
    exit_code = 127
    try:
        process = subprocess.Popen(process_command, env=process_environment)
        exit_code = process.wait()
    except KeyboardInterrupt:
        assert process is not None
        exit_code = process.wait()
    except OSError as error:
        print(f"memo: provider launch failed: {error}", file=sys.stderr)
    finally:
        if notified:
            try:
                request(
                    str(paths.socket),
                    "agent_complete",
                    {
                        "launch_id": launch_id,
                        "ended_utc": utcnow(),
                        "exit_code": exit_code if process is None else process.returncode,
                    },
                    timeout=60.0,
                )
            except Exception as error:
                print(f"memo: agent completion capture failed: {error}", file=sys.stderr)
    return 128 - exit_code if exit_code < 0 else exit_code


def main() -> int:
    if len(sys.argv) < 2:
        print("memo shim: harness name is required", file=sys.stderr)
        return 2
    return run(sys.argv[1], sys.argv[2:])


if __name__ == "__main__":
    raise SystemExit(main())
