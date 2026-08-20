"""Resolve machine-specific sandbox mounts and environment from root policy."""

from __future__ import annotations

import fnmatch
import hashlib
import json
import os
import shutil
import subprocess
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path

from ...recording.paths import StoragePaths
from .config import POLICY_NAME, SandboxConfig, expand_path, load_root_config


@dataclass(frozen=True)
class Mount:
    source: Path
    destination: Path
    mode: str
    purpose: str


@dataclass(frozen=True)
class EffectivePolicy:
    root: Path
    cwd: Path
    home: Path
    network: bool
    gpu: bool
    mounts: tuple[Mount, ...]
    environment: dict[str, str]
    excluded_environment: tuple[str, ...]
    missing_optional: tuple[Path, ...]

    def summary(self) -> dict[str, object]:
        return {
            "root": str(self.root),
            "cwd": str(self.cwd),
            "network": self.network,
            "gpu": self.gpu,
            "mounts": [
                {
                    "source": str(item.source),
                    "destination": str(item.destination),
                    "mode": item.mode,
                    "purpose": item.purpose,
                }
                for item in self.mounts
            ],
            "environment_inherited": sorted(self.environment),
            "environment_excluded": list(self.excluded_environment),
            "missing_optional": [str(path) for path in self.missing_optional],
        }

    def digest(self) -> str:
        body = json.dumps(self.summary(), separators=(",", ":"), sort_keys=True).encode()
        return hashlib.sha256(body).hexdigest()


def _inside(path: Path, parent: Path) -> bool:
    return path == parent or path.is_relative_to(parent)


def _git_mounts(root: Path) -> list[Mount]:
    marker = root / ".git"
    if not marker.is_file():
        return []
    try:
        line = marker.read_text(errors="replace").splitlines()[0]
    except (OSError, IndexError):
        return []
    if not line.startswith("gitdir:"):
        return []
    gitdir = Path(line.removeprefix("gitdir:").strip())
    if not gitdir.is_absolute():
        gitdir = marker.parent / gitdir
    gitdir = gitdir.resolve(strict=True)
    result = [Mount(gitdir, gitdir, "read-write", "linked-worktree-gitdir")]
    commondir_file = gitdir / "commondir"
    if commondir_file.is_file():
        common = Path(commondir_file.read_text().strip())
        if not common.is_absolute():
            common = gitdir / common
        common = common.resolve(strict=True)
        if common != gitdir:
            result.append(Mount(common, common, "read-write", "shared-git-metadata"))
    return result


def _provider_state(provider: str | None, home: Path) -> list[Mount]:
    values: tuple[Path, ...]
    if provider == "codex":
        values = (Path(os.environ.get("CODEX_HOME", str(home / ".codex"))).expanduser(),)
    elif provider == "claude":
        config = Path(os.environ.get("CLAUDE_CONFIG_DIR", str(home / ".claude"))).expanduser()
        values = (config, home / ".claude.json")
    else:
        values = ()
    return [
        Mount(path.resolve(strict=True), path.absolute(), "read-write", f"{provider}-state")
        for path in values
        if path.exists()
    ]


def _nvm_root(path: Path) -> Path | None:
    parts = path.parts
    for index in range(len(parts) - 4):
        if parts[index : index + 3] == (".nvm", "versions", "node"):
            return Path(*parts[: index + 4])
    return None


def _node_package_root(path: Path) -> Path | None:
    parts = path.parts
    try:
        index = parts.index("node_modules")
    except ValueError:
        return None
    package_index = index + 1
    if package_index >= len(parts):
        return None
    end = package_index + (2 if parts[package_index].startswith("@") else 1)
    if end > len(parts):
        return None
    return Path(*parts[:end])


def _installation_mounts(executable: Path | None, home: Path, root: Path) -> list[Mount]:
    if executable is None:
        return []
    original = executable.absolute()
    resolved = original.resolve(strict=True)
    if _inside(original, root) or not _inside(original, home):
        return []
    nvm = _nvm_root(resolved)
    if nvm is not None:
        return [Mount(nvm, nvm, "read", "provider-installation")]
    package = _node_package_root(resolved)
    if package is not None:
        return [Mount(package, package, "read", "provider-installation")]
    return [Mount(resolved, resolved, "read", "provider-executable")]


def _gpu_mounts() -> list[Mount]:
    devices: list[Path] = sorted(
        path for path in Path("/dev").glob("nvidia*") if path.is_char_device()
    )
    caps = Path("/dev/nvidia-caps")
    if caps.is_dir():
        devices.extend(sorted(path for path in caps.iterdir() if path.is_char_device()))
    if not devices:
        return []
    dri = Path("/dev/dri")
    if dri.is_dir():
        devices.extend(sorted(path for path in dri.glob("renderD*") if path.is_char_device()))
    result = [Mount(path, path, "device", "gpu-device") for path in devices]
    for path in (Path("/proc/driver/nvidia"), Path("/sys")):
        if path.exists():
            result.append(Mount(path, path, "read", "gpu-host-information"))
    ldconfig = shutil.which("ldconfig")
    if ldconfig:
        try:
            output = subprocess.run(
                [ldconfig, "-p"], capture_output=True, check=True, text=True, timeout=3
            ).stdout
        except (OSError, subprocess.SubprocessError):
            output = ""
        for line in output.splitlines():
            if not any(name in line.lower() for name in ("libcuda", "libnvidia", "libnvml")):
                continue
            _, separator, target = line.rpartition("=>")
            path = Path(target.strip()) if separator else None
            if path and path.exists() and not _inside(path, Path("/usr")):
                result.append(Mount(path, path, "read", "gpu-driver-library"))
    return result


def _filtered_environment(
    config: SandboxConfig, cwd: Path
) -> tuple[dict[str, str], tuple[str, ...]]:
    excluded: list[str] = []
    result: dict[str, str] = {}
    patterns = tuple(pattern.upper() for pattern in config.environment_exclude)
    for name, value in os.environ.items():
        if any(fnmatch.fnmatchcase(name.upper(), pattern) for pattern in patterns):
            excluded.append(name)
        else:
            result[name] = value
    shim = os.environ.get("MEMO_SHIM_DIR")
    entries = result.get("PATH", "").split(os.pathsep)
    if shim:
        shim_path = Path(shim).resolve(strict=False)
        entries = [item for item in entries if Path(item or ".").resolve(strict=False) != shim_path]
    result["PATH"] = os.pathsep.join(entries)
    result["HOME"] = str(Path.home())
    result["PWD"] = str(cwd)
    return result, tuple(sorted(excluded))


def resolve_policy(
    root: Path,
    cwd: Path,
    *,
    provider: str | None = None,
    executable: Path | None = None,
    initialize: bool = True,
) -> EffectivePolicy:
    root = root.expanduser().resolve(strict=True)
    cwd = cwd.expanduser().resolve(strict=True)
    if not cwd.is_dir() or not _inside(cwd, root):
        raise ValueError(
            f"sandbox cwd {cwd} is outside recording root {root}; "
            "return to the recording root or use --no-sandbox"
        )
    memo_home = StoragePaths.discover().home.resolve(strict=False)
    if _inside(memo_home, root) or _inside(root, memo_home):
        raise ValueError(
            f"recording root {root} overlaps Memo storage {memo_home}; "
            "Memo archives and runtime state cannot be exposed to a sandbox"
        )
    config = load_root_config(root, initialize=initialize)
    home = Path.home().resolve(strict=True)
    mounts: list[Mount] = []
    missing: list[Path] = []
    for value in config.system_read_only:
        destination = expand_path(value, home).absolute()
        mounts.append(Mount(destination.resolve(strict=True), destination, "read", "system"))
    for value in config.system_read_only_if_present:
        destination = expand_path(value, home).absolute()
        if destination.exists():
            mounts.append(Mount(destination.resolve(strict=True), destination, "read", "system"))
        else:
            missing.append(destination)
    mounts.append(Mount(root, root, "read-write", "recording-root"))
    mounts.extend(_git_mounts(root))
    for values, mode, purpose in (
        (config.home_read_only_if_present, "read", "shared-home-state"),
        (config.home_read_write_if_present, "read-write", "shared-cache"),
    ):
        for value in values:
            destination = expand_path(value if value.startswith("/") else f"~/{value}", home)
            if destination.exists():
                mounts.append(
                    Mount(
                        destination.resolve(strict=True),
                        destination.absolute(),
                        mode,
                        purpose,
                    )
                )
            else:
                missing.append(destination)
    mounts.extend(_provider_state(provider, home))
    mounts.extend(_installation_mounts(executable, home, root))
    if provider is not None:
        guidance = Path(str(files(__package__).joinpath("agent-guidance.md")))
        mounts.append(
            Mount(guidance, Path("/run/memo/agent-guidance.md"), "read", "agent-guidance")
        )
    if config.gpu:
        mounts.extend(_gpu_mounts())
    for grant in config.grants:
        source = expand_path(grant.source, home).absolute()
        destination = expand_path(grant.destination, home).absolute()
        mounts.append(Mount(source, destination, grant.mode, "user-grant"))
    policy = root / POLICY_NAME
    if policy.exists():
        mounts.append(Mount(policy, policy, "read", "sandbox-policy"))
    # Later entries intentionally override configured defaults at the same destination.
    unique: dict[Path, Mount] = {}
    for mount in mounts:
        unique[mount.destination] = mount
    environment, excluded = _filtered_environment(config, cwd)
    return EffectivePolicy(
        root=root,
        cwd=cwd,
        home=home,
        network=config.network,
        gpu=bool(config.gpu and any(item.mode == "device" for item in unique.values())),
        mounts=tuple(unique.values()),
        environment=environment,
        excluded_environment=excluded,
        missing_optional=tuple(missing),
    )
