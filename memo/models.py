from __future__ import annotations

import json
import getpass
import socket
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


DIRECTORY_FORMAT_VERSION = 2
STEP_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class SessionOrigin:
    memo_version_id: str
    username: str
    hostname: str

    @classmethod
    def current(cls) -> "SessionOrigin":
        from . import __version__
        return cls(__version__, getpass.getuser(), socket.gethostname())

    def validate(self) -> None:
        if any(not isinstance(value, str) or not value for value in (
            self.memo_version_id, self.username, self.hostname
        )):
            raise ValueError("session origin fields must be nonempty")

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


@dataclass
class DirectorySession:
    session_id: str
    root: str
    created_utc: str
    updated_utc: str
    origin: SessionOrigin
    state: str = "active"
    format: str = "memo-directory-session"
    format_version: int = DIRECTORY_FORMAT_VERSION
    last_pushed_step: int | None = None
    last_pushed_digest: str | None = None
    remote_object: str | None = None

    def validate(self) -> None:
        if self.format != "memo-directory-session" or self.format_version != DIRECTORY_FORMAT_VERSION:
            raise ValueError("unsupported directory session format")
        if self.state not in {"active", "ending", "complete"}:
            raise ValueError(f"invalid directory session state: {self.state}")
        if not Path(self.root).is_absolute():
            raise ValueError("directory session root must be absolute")
        self.origin.validate()
        if self.last_pushed_step is not None and self.last_pushed_step < 0:
            raise ValueError("last pushed step must be nonnegative")
        remote_values = (self.last_pushed_step, self.last_pushed_digest, self.remote_object)
        if any(value is not None for value in remote_values) and not all(
            value is not None for value in remote_values
        ):
            raise ValueError("remote transport state must be recorded atomically")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "DirectorySession":
        value = dict(value)
        value["origin"] = SessionOrigin(**value["origin"])
        result = cls(**value)
        result.validate()
        return result

    @classmethod
    def load(cls, path: Path) -> "DirectorySession":
        return cls.from_dict(json.loads(path.read_text()))


@dataclass(frozen=True)
class SnapshotEntry:
    path: str
    kind: str
    mode: int
    size: int | None = None
    detail: str | None = None
    retained: bool = False

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "SnapshotEntry":
        return cls(**value)


@dataclass
class StepManifest:
    session_id: str
    step: int
    created_utc: str
    snapshot: str
    entries: list[SnapshotEntry] = field(default_factory=list)
    stream_high_water: dict[str, int] = field(default_factory=dict)
    schema_version: int = STEP_SCHEMA_VERSION
    agent_runs: list[str] = field(default_factory=list)

    def validate(self) -> None:
        if self.schema_version != STEP_SCHEMA_VERSION:
            raise ValueError("unsupported step schema version")
        if not isinstance(self.step, int) or self.step < 0:
            raise ValueError("step must be nonnegative")
        expected = f"snapshots/{self.step}"
        if self.snapshot != expected:
            raise ValueError(f"step snapshot must be {expected}")
        for entry in self.entries:
            path = Path(entry.path)
            if path.is_absolute() or ".." in path.parts:
                raise ValueError(f"unsafe snapshot entry: {entry.path}")
            if entry.kind not in {
                "file", "directory", "ignored-policy", "oversized", "special",
                "missing", "unstable",
            }:
                raise ValueError(f"unsupported snapshot entry kind: {entry.kind}")
        for terminal_id, sequence in self.stream_high_water.items():
            if not terminal_id or Path(terminal_id).name != terminal_id:
                raise ValueError(f"unsafe terminal stream id: {terminal_id}")
            if not isinstance(sequence, int) or sequence < 0:
                raise ValueError(f"invalid stream high-water mark: {terminal_id}={sequence}")
        for run_id in self.agent_runs:
            if not run_id or Path(run_id).name != run_id:
                raise ValueError(f"unsafe agent run id: {run_id}")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "StepManifest":
        value = dict(value)
        value["entries"] = [SnapshotEntry.from_dict(item) for item in value.get("entries", [])]
        result = cls(**value)
        result.validate()
        return result

    @classmethod
    def load(cls, path: Path) -> "StepManifest":
        return cls.from_dict(json.loads(path.read_text()))
