from __future__ import annotations

from .claude import ClaudeHarness
from .codex import CodexHarness
from .harness import AgentHarness


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
