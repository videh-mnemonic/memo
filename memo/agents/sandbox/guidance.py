"""Construct provider-specific arguments for sandboxed launches."""

from __future__ import annotations

import json
import os
import tomllib
from importlib.resources import files
from pathlib import Path

GUIDANCE_DESTINATION = "/run/memo/agent-guidance.md"


def guidance_bytes() -> bytes:
    return files(__package__).joinpath("agent-guidance.md").read_bytes()


def guidance_digest() -> str:
    import hashlib

    return hashlib.sha256(guidance_bytes()).hexdigest()


def _has_option(args: list[str], names: set[str]) -> bool:
    return any(value.split("=", 1)[0] in names for value in args)


def _codex_instructions(args: list[str]) -> tuple[list[str], str | None]:
    existing: str | None = None
    config_home = Path(os.environ.get("CODEX_HOME", "~/.codex")).expanduser()
    try:
        value = tomllib.loads((config_home / "config.toml").read_text())
        configured = value.get("developer_instructions")
        if isinstance(configured, str):
            existing = configured
    except (OSError, tomllib.TOMLDecodeError):
        pass
    result: list[str] = []
    index = 0
    while index < len(args):
        item = args[index]
        config_value: str | None = None
        consumed = 1
        if item in {"-c", "--config"} and index + 1 < len(args):
            config_value = args[index + 1]
            consumed = 2
        elif item.startswith("--config="):
            config_value = item.split("=", 1)[1]
        if config_value and config_value.startswith("developer_instructions="):
            raw = config_value.split("=", 1)[1]
            try:
                parsed = tomllib.loads(f"value = {raw}")["value"]
            except tomllib.TOMLDecodeError:
                parsed = raw
            if isinstance(parsed, str):
                existing = parsed
            index += consumed
            continue
        result.extend(args[index : index + consumed])
        index += consumed
    return result, existing


def _codex_has_security_override(args: list[str]) -> bool:
    keys = {"approval_policy", "sandbox_mode", "default_permissions"}
    for index, item in enumerate(args):
        value: str | None = None
        if item in {"-c", "--config"} and index + 1 < len(args):
            value = args[index + 1]
        elif item.startswith("--config="):
            value = item.split("=", 1)[1]
        if value:
            key = value.split("=", 1)[0]
            if key in keys or key.startswith("sandbox_workspace_write."):
                return True
    return False


def effective_provider_args(provider: str, args: list[str]) -> list[str]:
    guidance = guidance_bytes().decode().strip()
    if provider == "claude":
        result = list(args)
        permission_options = {
            "--dangerously-skip-permissions",
            "--allow-dangerously-skip-permissions",
            "--permission-mode",
            "--permission-prompt-tool",
            "--allowedTools",
            "--allowed-tools",
            "--disallowedTools",
            "--disallowed-tools",
            "--tools",
            "--add-dir",
        }
        if not _has_option(result, permission_options):
            result.insert(0, "--dangerously-skip-permissions")
        return ["--append-system-prompt-file", GUIDANCE_DESTINATION, *result]
    if provider == "codex":
        result, existing = _codex_instructions(args)
        permission_options = {
            "--dangerously-bypass-approvals-and-sandbox",
            "--sandbox",
            "-s",
            "--ask-for-approval",
            "-a",
            "--approve-for-me",
            "--add-dir",
        }
        prefix: list[str] = []
        if not _has_option(result, permission_options) and not _codex_has_security_override(result):
            prefix.append("--dangerously-bypass-approvals-and-sandbox")
        combined = guidance if not existing else f"{existing.rstrip()}\n\n{guidance}"
        prefix.extend(["-c", f"developer_instructions={json.dumps(combined)}"])
        return [*prefix, *result]
    raise ValueError(f"unsupported sandbox provider: {provider}")
