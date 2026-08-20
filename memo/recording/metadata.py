"""Define serialized metadata for recording identity, sessions, snapshots, and steps."""

from __future__ import annotations

import getpass
import hashlib
import json
import socket
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any

DIRECTORY_FORMAT_VERSION = 2
STEP_SCHEMA_VERSION = 3

#: Serialisation of a shared snapshot entry list.
ENTRIES_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class SessionOrigin:
    memo_version_id: str
    username: str
    hostname: str

    @classmethod
    def current(cls) -> SessionOrigin:
        from .. import __version__

        return cls(__version__, getpass.getuser(), socket.gethostname())

    def validate(self) -> None:
        if any(
            not isinstance(value, str) or not value
            for value in (self.memo_version_id, self.username, self.hostname)
        ):
            raise ValueError("session origin fields must be nonempty")


@dataclass
class DirectorySession:
    session_id: str
    root: str
    created_utc: str
    updated_utc: str
    origin: SessionOrigin
    state: str = "active"
    capture_scope: str = "partial"
    format: str = "memo-directory-session"
    format_version: int = DIRECTORY_FORMAT_VERSION
    last_pushed_step: int | None = None
    last_pushed_digest: str | None = None
    remote_object: str | None = None

    def validate(self) -> None:
        if (
            self.format != "memo-directory-session"
            or self.format_version != DIRECTORY_FORMAT_VERSION
        ):
            raise ValueError("unsupported directory session format")
        if self.state not in {"active", "ending", "complete"}:
            raise ValueError(f"invalid directory session state: {self.state}")
        if self.capture_scope not in {"partial", "full", "agent-only"}:
            raise ValueError(f"invalid capture scope: {self.capture_scope}")
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
    def from_dict(cls, value: dict[str, Any]) -> DirectorySession:
        value = dict(value)
        value["origin"] = SessionOrigin(**value["origin"])
        result = cls(**value)
        result.validate()
        return result

    @classmethod
    def load(cls, path: Path) -> DirectorySession:
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
    def from_dict(cls, value: dict[str, Any]) -> SnapshotEntry:
        return cls(**value)


def _is_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value.lower())
    )


def entries_directory(session_path: Path) -> Path:
    """Where a session keeps the snapshot entry lists its steps share."""
    return session_path / "entries"


def encode_entries(entries: Sequence[SnapshotEntry]) -> bytes:
    """Serialise an entry list canonically, so equal lists hash identically."""
    return json.dumps(
        {
            "schema_version": ENTRIES_SCHEMA_VERSION,
            "entries": [asdict(entry) for entry in entries],
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def digest_entries(entries: Sequence[SnapshotEntry]) -> str:
    return hashlib.sha256(encode_entries(entries)).hexdigest()


@lru_cache(maxsize=64)
def read_entries(path: str) -> tuple[SnapshotEntry, ...]:
    """Read a shared entry list.

    The file is named for the hash of its contents, so a given path can never
    describe two different lists and the parse is cached indefinitely.
    """
    value = json.loads(Path(path).read_text())
    if value.get("schema_version") != ENTRIES_SCHEMA_VERSION:
        raise ValueError("unsupported snapshot entry list version")
    return tuple(SnapshotEntry.from_dict(item) for item in value["entries"])


@dataclass
class StepManifest:
    session_id: str
    step: int
    created_utc: str
    snapshot: str
    entries: list[SnapshotEntry] = field(default_factory=list)
    stream_high_water: dict[str, int] = field(default_factory=dict)
    schema_version: int = 1
    agent_runs: list[str] = field(default_factory=list)
    snapshot_commit: str | None = None
    entries_digest: str | None = None

    def validate(self) -> None:
        if self.schema_version not in {1, 2, STEP_SCHEMA_VERSION}:
            raise ValueError("unsupported step schema version")
        if self.entries_digest is not None and not _is_sha256(self.entries_digest):
            raise ValueError("invalid snapshot entry digest")
        if not isinstance(self.step, int) or self.step < 0:
            raise ValueError("step must be nonnegative")
        expected = f"snapshots/{self.step}"
        if self.snapshot != expected:
            raise ValueError(f"step snapshot must be {expected}")
        if self.schema_version >= 2 and not self.snapshot_commit:
            raise ValueError("Git-backed step is missing its snapshot commit")
        if self.schema_version >= STEP_SCHEMA_VERSION and not self.entries_digest:
            raise ValueError("step is missing its snapshot entry digest")
        if self.snapshot_commit and (
            len(self.snapshot_commit) not in {40, 64}
            or any(value not in "0123456789abcdef" for value in self.snapshot_commit.lower())
        ):
            raise ValueError("invalid snapshot commit")
        for entry in self.entries:
            path = Path(entry.path)
            if path.is_absolute() or ".." in path.parts:
                raise ValueError(f"unsafe snapshot entry: {entry.path}")
            if entry.kind not in {
                "file",
                "directory",
                "ignored-policy",
                "oversized",
                "special",
                "missing",
                "unstable",
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

    def to_stored_dict(self) -> dict[str, Any]:
        """Serialise for the archive, leaving a shared entry list out of line."""
        value = asdict(self)
        if self.entries_digest:
            value["entries"] = []
        return value

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> StepManifest:
        value = dict(value)
        value["entries"] = [SnapshotEntry.from_dict(item) for item in value.get("entries", [])]
        result = cls(**value)
        result.validate()
        return result

    @classmethod
    def load(cls, path: Path) -> StepManifest:
        """Read a step, resolving a shared entry list from beside its session."""
        manifest = cls.from_dict(json.loads(path.read_text()))
        if manifest.entries_digest and not manifest.entries:
            entries_path = entries_directory(path.parent.parent) / f"{manifest.entries_digest}.json"
            manifest.entries = list(read_entries(str(entries_path)))
        return manifest
