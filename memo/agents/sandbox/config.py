"""Load, initialize, and update root-persistent sandbox policy."""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path
from typing import Any

from ...recording.filesystem import atomic_write

POLICY_NAME = ".memo-sandbox"
_ALLOWED_TOP_LEVEL = {"network", "gpu", "home", "system", "environment", "grants"}


@dataclass(frozen=True)
class Grant:
    source: str
    destination: str
    mode: str = "read"

    def __post_init__(self) -> None:
        if self.mode not in {"read", "read-write"}:
            raise ValueError(f"invalid sandbox grant mode: {self.mode}")


@dataclass(frozen=True)
class SandboxConfig:
    network: bool
    gpu: bool
    home_read_write_if_present: tuple[str, ...]
    system_read_only: tuple[str, ...]
    system_read_only_if_present: tuple[str, ...]
    environment_exclude: tuple[str, ...]
    grants: tuple[Grant, ...] = ()


def defaults_bytes() -> bytes:
    return files(__package__).joinpath("defaults.toml").read_bytes()


def policy_path(root: Path) -> Path:
    return root.resolve(strict=True) / POLICY_NAME


def ensure_root_config(root: Path) -> Path:
    path = policy_path(root)
    if not path.exists():
        atomic_write(path, defaults_bytes())
    return path


def _strings(value: Any, label: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not all(isinstance(item, str) and item for item in value):
        raise ValueError(f"{label} must be a list of nonempty strings")
    return tuple(value)


def parse_config(data: bytes, source: str = "sandbox policy") -> SandboxConfig:
    try:
        value = tomllib.loads(data.decode())
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
        raise ValueError(f"invalid {source}: {error}") from error
    unknown = set(value) - _ALLOWED_TOP_LEVEL
    if unknown:
        raise ValueError(f"unknown {source} field: {sorted(unknown)[0]}")
    if not isinstance(value.get("network"), bool) or not isinstance(value.get("gpu"), bool):
        raise ValueError(f"{source} requires boolean network and gpu values")
    home = value.get("home", {})
    system = value.get("system", {})
    environment = value.get("environment", {})
    if not all(isinstance(item, dict) for item in (home, system, environment)):
        raise ValueError(f"invalid table in {source}")
    if set(home) - {"read_write_if_present"}:
        raise ValueError(f"unknown {source} home field")
    if set(system) - {"read_only", "read_only_if_present"}:
        raise ValueError(f"unknown {source} system field")
    if set(environment) - {"inherit", "exclude"}:
        raise ValueError(f"unknown {source} environment field")
    if environment.get("inherit") != "all":
        raise ValueError(f"{source} environment.inherit must be 'all'")
    grants_value = value.get("grants", [])
    if not isinstance(grants_value, list):
        raise ValueError(f"{source} grants must be an array of tables")
    grants: list[Grant] = []
    for item in grants_value:
        if not isinstance(item, dict) or set(item) != {"source", "destination", "mode"}:
            raise ValueError(f"invalid grant in {source}")
        if not isinstance(item["source"], str) or not isinstance(item["destination"], str):
            raise ValueError(f"invalid grant path in {source}")
        grants.append(Grant(item["source"], item["destination"], item["mode"]))
    return SandboxConfig(
        network=value["network"],
        gpu=value["gpu"],
        home_read_write_if_present=_strings(
            home.get("read_write_if_present", []), "home.read_write_if_present"
        ),
        system_read_only=_strings(system.get("read_only", []), "system.read_only"),
        system_read_only_if_present=_strings(
            system.get("read_only_if_present", []), "system.read_only_if_present"
        ),
        environment_exclude=_strings(environment.get("exclude", []), "environment.exclude"),
        grants=tuple(grants),
    )


def load_root_config(root: Path, *, initialize: bool = True) -> SandboxConfig:
    path = ensure_root_config(root) if initialize else policy_path(root)
    return parse_config(path.read_bytes(), str(path))


def _quote(value: str) -> str:
    import json

    return json.dumps(value)


def render_config(config: SandboxConfig) -> bytes:
    lines = [
        f"network = {str(config.network).lower()}",
        f"gpu = {str(config.gpu).lower()}",
        "",
        "[home]",
        "read_write_if_present = ["
        + ", ".join(_quote(item) for item in config.home_read_write_if_present)
        + "]",
        "",
        "[system]",
        "read_only = [" + ", ".join(_quote(item) for item in config.system_read_only) + "]",
        "read_only_if_present = [",
        *(f"    {_quote(item)}," for item in config.system_read_only_if_present),
        "]",
        "",
        "[environment]",
        'inherit = "all"',
        "exclude = [",
        *(f"    {_quote(item)}," for item in config.environment_exclude),
        "]",
    ]
    for grant in config.grants:
        lines.extend(
            [
                "",
                "[[grants]]",
                f"source = {_quote(grant.source)}",
                f"destination = {_quote(grant.destination)}",
                f"mode = {_quote(grant.mode)}",
            ]
        )
    return ("\n".join(lines) + "\n").encode()


def write_root_config(root: Path, config: SandboxConfig) -> None:
    atomic_write(policy_path(root), render_config(config))


def reset_root_config(root: Path) -> None:
    atomic_write(policy_path(root), defaults_bytes())


def expand_path(value: str, home: Path | None = None) -> Path:
    if value.startswith("~/"):
        return (home or Path.home()) / value[2:]
    return Path(os.path.expandvars(value)).expanduser()
