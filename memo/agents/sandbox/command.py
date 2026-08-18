"""Build Bubblewrap commands and verify backend availability."""

from __future__ import annotations

import json
import platform
import shutil
import subprocess
from pathlib import Path

from ... import __version__
from ...recording.filesystem import atomic_write
from ...recording.paths import StoragePaths
from .policy import EffectivePolicy, Mount


class SandboxUnavailable(RuntimeError):
    pass


def install_hint() -> str:
    distro = ""
    try:
        for line in Path("/etc/os-release").read_text().splitlines():
            if line.startswith("ID="):
                distro = line.split("=", 1)[1].strip().strip('"').lower()
    except OSError:
        pass
    if distro in {"ubuntu", "debian", "linuxmint", "pop"}:
        return "install it with: sudo apt-get install bubblewrap"
    if distro in {"fedora", "rhel", "centos"}:
        return "install it with: sudo dnf install bubblewrap"
    if distro in {"arch", "manjaro"}:
        return "install it with: sudo pacman -S bubblewrap"
    return "install the Bubblewrap package using your system package manager"


def _identity(executable: str) -> dict[str, str]:
    try:
        version = subprocess.run(
            [executable, "--version"], capture_output=True, check=True, text=True, timeout=3
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        version = "unknown"
    return {
        "memo": __version__,
        "bubblewrap": version,
        "kernel": platform.release(),
    }


def self_test(paths: StoragePaths | None = None, *, force: bool = False) -> dict[str, str]:
    executable = shutil.which("bwrap")
    if executable is None:
        raise SandboxUnavailable(
            f"Bubblewrap is required for Memo sandboxing; {install_hint()}. "
            "Use --no-sandbox only as an explicit fallback."
        )
    identity = _identity(executable)
    paths = paths or StoragePaths.discover()
    paths.ensure_storage()
    cache = paths.runtime / "sandbox-self-test.json"
    if not force:
        try:
            if json.loads(cache.read_text()) == identity:
                return identity
        except (OSError, json.JSONDecodeError):
            pass
    command = [
        executable,
        "--die-with-parent",
        "--unshare-user",
        "--unshare-pid",
        "--unshare-ipc",
        "--unshare-uts",
        "--cap-drop",
        "ALL",
        "--ro-bind",
        "/usr",
        "/usr",
    ]
    # Dynamically linked executables under /usr still load their interpreter
    # and libraries through /lib or /lib64.  Those paths are separate mounts
    # (or usr-merge symlinks) and are not created by the /usr bind alone.
    for library_directory in (Path("/lib"), Path("/lib64")):
        if library_directory.exists():
            command.extend(
                ["--ro-bind", str(library_directory), str(library_directory)]
            )
    command.extend(
        [
            "--proc",
            "/proc",
            "--dev",
            "/dev",
            "--tmpfs",
            "/tmp",
            "--",
            "/usr/bin/true",
        ]
    )
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=10)
    except (OSError, subprocess.SubprocessError) as error:
        raise SandboxUnavailable(f"Bubblewrap self-test could not run: {error}") from error
    if result.returncode:
        detail = (result.stderr or result.stdout).strip()
        raise SandboxUnavailable(
            "Bubblewrap self-test failed: "
            f"{detail or f'exit {result.returncode}'}. "
            "Enable unprivileged user namespaces or use --no-sandbox explicitly."
        )
    atomic_write(cache, (json.dumps(identity, sort_keys=True) + "\n").encode())
    return identity


def _mount_arguments(mount: Mount) -> list[str]:
    option = {
        "read": "--ro-bind",
        "read-write": "--bind",
        "device": "--dev-bind",
    }[mount.mode]
    return [option, str(mount.source), str(mount.destination)]


def _destination_dirs(mounts: tuple[Mount, ...], home: Path) -> list[Path]:
    values = {Path("/run/memo"), home.parent, home}
    for mount in mounts:
        destination = mount.destination
        if destination == Path("/proc") or destination.is_relative_to(Path("/proc")):
            continue
        parent = destination if mount.source.is_dir() else destination.parent
        while parent != Path("/"):
            values.add(parent)
            parent = parent.parent
    return sorted(values, key=lambda value: (len(value.parts), str(value)))


def _sandbox_arguments(values: list[str]) -> list[str]:
    allowed = {"--unshare-net"}
    invalid = [value for value in values if value not in allowed]
    if invalid:
        raise ValueError(f"unsupported --sandbox-args value: {invalid[0]}")
    return values


def build_command(
    policy: EffectivePolicy,
    target: list[str],
    *,
    sandbox_args: list[str] | None = None,
    executable: str | None = None,
) -> list[str]:
    if not target:
        raise ValueError("sandbox target command is required")
    bwrap = executable or shutil.which("bwrap")
    if not bwrap:
        raise SandboxUnavailable(f"Bubblewrap is required; {install_hint()}")
    command = [
        bwrap,
        "--die-with-parent",
        "--unshare-user",
        "--unshare-pid",
        "--unshare-ipc",
        "--unshare-uts",
        "--cap-drop",
        "ALL",
        "--proc",
        "/proc",
        "--dev",
        "/dev",
        "--tmpfs",
        "/tmp",
        "--tmpfs",
        "/dev/shm",
    ]
    custom_arguments = _sandbox_arguments(list(sandbox_args or []))
    if not policy.network:
        command.append("--unshare-net")
    command.extend(
        value for value in custom_arguments if not (value == "--unshare-net" and not policy.network)
    )
    for name in policy.excluded_environment:
        command.extend(["--unsetenv", name])
    for name in ("HOME", "PATH", "PWD"):
        command.extend(["--setenv", name, policy.environment[name]])
    for directory in _destination_dirs(policy.mounts, policy.home):
        command.extend(["--dir", str(directory)])
    command.extend(["--tmpfs", str(policy.home)])
    # Recreate nested mount destinations after the synthetic home hides their placeholders.
    for directory in _destination_dirs(policy.mounts, policy.home):
        if directory != policy.home and directory.is_relative_to(policy.home):
            command.extend(["--dir", str(directory)])
    for mount in policy.mounts:
        command.extend(_mount_arguments(mount))
    command.extend(["--chdir", str(policy.cwd), "--", *target])
    return command
