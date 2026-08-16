"""Registered provider-specific agent trace harnesses."""

from __future__ import annotations

from .base import AgentHarness
from .claude import ClaudeHarness
from .codex import CodexHarness

_HARNESSES: dict[str, AgentHarness] = {
    harness.name: harness for harness in (ClaudeHarness(), CodexHarness())
}


def get_harness(name: str) -> AgentHarness:
    try:
        return _HARNESSES[name]
    except KeyError as error:
        raise ValueError(f"unsupported agent harness: {name}") from error


def registered_harnesses() -> tuple[AgentHarness, ...]:
    return tuple(_HARNESSES.values())
