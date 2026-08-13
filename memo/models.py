from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any


DIRECTORY_FORMAT_VERSION = 1
CHECKPOINT_SCHEMA_VERSION = 1
AGENT_SESSION_FORMAT_VERSION = 1


@dataclass
class Leg:
    leg_id: str
    tool_args: list[str]
    start_utc: str
    end_utc: str | None = None
    exit_code: int | None = None
    trace_file: str | None = None
    complete: bool = False

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "Leg":
        return cls(**value)

    def validate(self, expected_number: int) -> None:
        expected_id = f"{expected_number:03d}"
        if self.leg_id != expected_id:
            raise ValueError(f"agent session legs must be ordered from 001: expected {expected_id}")
        if not isinstance(self.tool_args, list) or not all(isinstance(arg, str) for arg in self.tool_args):
            raise ValueError(f"invalid tool arguments for leg {self.leg_id}")
        if not self.start_utc:
            raise ValueError(f"start time is required for leg {self.leg_id}")
        if self.trace_file is not None and self.trace_file != f"leg-{self.leg_id}.jsonl":
            raise ValueError(f"invalid trace reference for leg {self.leg_id}")
        if self.complete and (self.end_utc is None or self.exit_code is None):
            raise ValueError(f"completed leg {self.leg_id} requires end time and exit code")
        if not self.complete and (self.end_utc is not None or self.exit_code is not None):
            raise ValueError(f"active leg {self.leg_id} cannot have completion data")


@dataclass
class SessionMeta:
    session_id: str
    provider: str
    repo_kind: str
    repo_root: str
    repo_name: str
    remote: str
    canonical_remote: str
    archive_namespace: str
    initial_head: str
    final_head: str
    first_seen_utc: str
    last_activity_utc: str
    legs: list[Leg] = field(default_factory=list)
    resumes: str | None = None
    coverage: str = "full"
    shipped: bool = False
    shipped_at: str | None = None
    archive_sha256: str | None = None
    branch: str = ""
    format: str = "memo-agent-session"
    format_version: int = AGENT_SESSION_FORMAT_VERSION

    def validate(self) -> None:
        if self.format != "memo-agent-session" or self.format_version != AGENT_SESSION_FORMAT_VERSION:
            raise ValueError("unsupported agent session format")
        if (not self.session_id or self.session_id in {".", ".."}
                or Path(self.session_id).name != self.session_id):
            raise ValueError("agent session id must be a safe, non-empty path component")
        if (not self.provider or self.provider in {".", ".."}
                or Path(self.provider).name != self.provider):
            raise ValueError("agent session provider is required")
        if self.repo_kind not in {"real", "synthetic"}:
            raise ValueError(f"invalid repo_kind: {self.repo_kind}")
        ns = self.archive_namespace
        if not ns or ns in {".", ".."} or "/" in ns or "\\" in ns or any(ord(c) < 32 for c in ns):
            raise ValueError("archive_namespace must be a safe, non-empty path component")
        if not self.remote and self.canonical_remote:
            raise ValueError("canonical_remote requires remote")
        if self.coverage not in {"full", "partial_outside_repo"}:
            raise ValueError(f"invalid coverage: {self.coverage}")
        if self.resumes is not None and (not self.resumes or self.resumes in {".", ".."}
                                         or Path(self.resumes).name != self.resumes):
            raise ValueError("resume relationship must contain a safe session id")
        if self.resumes == self.session_id:
            raise ValueError("an agent session cannot resume itself")
        for number, leg in enumerate(self.legs, 1):
            leg.validate(number)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "SessionMeta":
        value = dict(value)
        value["legs"] = [Leg.from_dict(v) for v in value.get("legs", [])]
        return cls(**value)

    @classmethod
    def load(cls, path: Path) -> "SessionMeta":
        result = cls.from_dict(json.loads(path.read_text()))
        result.validate()
        return result

    def save(self, path: Path) -> None:
        self.validate()
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.tmp")
        temporary.write_text(json.dumps(self.to_dict(), indent=2, sort_keys=True) + "\n")
        temporary.replace(path)


@dataclass
class DirectorySession:
    session_id: str
    root: str
    archive_namespace: str
    created_utc: str
    updated_utc: str
    state: str = "active"
    format: str = "memo-directory-session"
    format_version: int = DIRECTORY_FORMAT_VERSION
    last_pushed_generation: int | None = None
    last_pushed_digest: str | None = None
    remote_object: str | None = None

    def validate(self) -> None:
        if self.format != "memo-directory-session" or self.format_version != DIRECTORY_FORMAT_VERSION:
            raise ValueError("unsupported directory session format")
        if self.state not in {"active", "ending", "complete"}:
            raise ValueError(f"invalid directory session state: {self.state}")
        if not Path(self.root).is_absolute():
            raise ValueError("directory session root must be absolute")
        ns = self.archive_namespace
        if not ns or ns in {".", ".."} or "/" in ns or "\\" in ns:
            raise ValueError("archive_namespace must be a safe path component")
        if self.last_pushed_generation is not None and self.last_pushed_generation < 1:
            raise ValueError("last pushed generation must be positive")
        remote_values = (self.last_pushed_generation, self.last_pushed_digest, self.remote_object)
        if any(value is not None for value in remote_values) and not all(
            value is not None for value in remote_values
        ):
            raise ValueError("remote transport state must be recorded atomically")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "DirectorySession":
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
class CheckpointManifest:
    checkpoint_id: str
    session_id: str
    generation: int
    created_utc: str
    snapshot: str
    entries: list[SnapshotEntry] = field(default_factory=list)
    stream_high_water: dict[str, int] = field(default_factory=dict)
    schema_version: int = CHECKPOINT_SCHEMA_VERSION

    def validate(self) -> None:
        if self.schema_version != CHECKPOINT_SCHEMA_VERSION:
            raise ValueError("unsupported checkpoint schema version")
        if self.generation < 1:
            raise ValueError("checkpoint generation must be positive")
        expected = f"snapshots/{self.checkpoint_id}"
        if self.snapshot != expected:
            raise ValueError(f"checkpoint snapshot must be {expected}")
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

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "CheckpointManifest":
        value = dict(value)
        value["entries"] = [SnapshotEntry.from_dict(item) for item in value.get("entries", [])]
        result = cls(**value)
        result.validate()
        return result

    @classmethod
    def load(cls, path: Path) -> "CheckpointManifest":
        return cls.from_dict(json.loads(path.read_text()))
