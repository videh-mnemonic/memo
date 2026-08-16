"""Define and validate metadata stored for captured coding-agent runs."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from ..recording.filesystem import atomic_write


@dataclass(kw_only=True)
class AgentRunMetadata:
    run_id: str
    harness: str
    model: Any
    reasoning: Any
    command: list[str] | None
    cwd: str
    started_utc: str | None
    ended_utc: str | None
    exit_code: int | None
    agent_session_id: str
    trace_file: str | None = None
    trace_complete_size: int | None = None
    trace_digest: str | None = None
    imported_agent_only: bool = False

    def validate(self) -> None:
        for label, value in (
            ("run ID", self.run_id),
            ("harness", self.harness),
            ("agent session ID", self.agent_session_id),
            ("working directory", self.cwd),
        ):
            if not isinstance(value, str) or not value:
                raise ValueError(f"agent run {label} is required")
        if Path(self.run_id).name != self.run_id:
            raise ValueError(f"unsafe agent run ID: {self.run_id}")
        if not isinstance(self.trace_file, str) or Path(self.trace_file).name != self.trace_file:
            raise ValueError(f"agent run references unsafe trace: {self.run_id}")
        if (
            not isinstance(self.trace_complete_size, int)
            or isinstance(self.trace_complete_size, bool)
            or self.trace_complete_size < 0
        ):
            raise ValueError(f"invalid agent trace size: {self.run_id}")
        if (
            not isinstance(self.trace_digest, str)
            or len(self.trace_digest) != 64
            or any(character not in "0123456789abcdef" for character in self.trace_digest)
        ):
            raise ValueError(f"invalid agent trace digest: {self.run_id}")
        if self.command is not None and (
            not isinstance(self.command, list)
            or any(not isinstance(value, str) for value in self.command)
        ):
            raise ValueError(f"invalid agent command: {self.run_id}")
        if self.exit_code is not None and (
            not isinstance(self.exit_code, int) or isinstance(self.exit_code, bool)
        ):
            raise ValueError(f"invalid agent exit code: {self.run_id}")
        if not isinstance(self.imported_agent_only, bool):
            raise ValueError(f"invalid imported-agent marker: {self.run_id}")

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        if not self.imported_agent_only:
            value.pop("imported_agent_only")
        return value

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> AgentRunMetadata:
        result = cls(**value)
        result.validate()
        return result

    @classmethod
    def load(cls, path: Path) -> AgentRunMetadata:
        value = json.loads(path.read_text())
        if not isinstance(value, dict):
            raise ValueError("agent run metadata must be an object")
        return cls.from_dict(value)

    def write(self, path: Path) -> None:
        self.validate()
        data = (json.dumps(self.to_dict(), indent=2, sort_keys=True) + "\n").encode()
        atomic_write(path, data)
