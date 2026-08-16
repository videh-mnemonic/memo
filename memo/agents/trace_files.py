"""Discover, checkpoint, and safely snapshot native agent trace files."""

from __future__ import annotations

import json
import os
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True)
class TraceState:
    device: int
    inode: int
    mtime_ns: int
    size: int
    complete_size: int

    @classmethod
    def from_stat(cls, value: os.stat_result, complete_size: int | None = None) -> "TraceState":
        return cls(
            value.st_dev, value.st_ino, value.st_mtime_ns, value.st_size,
            value.st_size if complete_size is None else complete_size,
        )


@dataclass
class TraceCheckpoint:
    files: dict[str, TraceState]

    def to_json(self) -> str:
        return json.dumps(
            {path: asdict(state) for path, state in self.files.items()},
            sort_keys=True,
            separators=(",", ":"),
        )

    @classmethod
    def from_json(cls, value: str) -> "TraceCheckpoint":
        raw = json.loads(value)
        return cls({path: TraceState(**state) for path, state in raw.items()})


def files(roots: Sequence[Path]) -> list[Path]:
    result: list[Path] = []
    for root in roots:
        if root.exists():
            result.extend(path for path in root.rglob("*.jsonl") if path.is_file())
    return sorted(set(result))


def capture(roots: Sequence[Path]) -> TraceCheckpoint:
    values: dict[str, TraceState] = {}
    for path in files(roots):
        try:
            values[str(path.resolve())] = TraceState.from_stat(path.stat())
        except (FileNotFoundError, PermissionError, OSError):
            continue
    return TraceCheckpoint(values)


def changed(roots: Sequence[Path], checkpoint: TraceCheckpoint) -> list[Path]:
    result = []
    for path in files(roots):
        try:
            resolved = str(path.resolve())
            state = TraceState.from_stat(path.stat())
        except (FileNotFoundError, PermissionError, OSError):
            continue
        previous = checkpoint.files.get(resolved)
        if previous != state or (
            previous is not None and previous.complete_size < previous.size
        ):
            result.append(path)
    return result


def snapshot_complete(source: Path, destination: Path) -> tuple[TraceState, int]:
    """Copy a fixed-size prefix ending at the final complete JSONL record."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    last_newline = 0
    copied = 0
    with source.open("rb") as reader, destination.open("wb") as writer:
        state = TraceState.from_stat(os.fstat(reader.fileno()))
        remaining = state.size
        while remaining:
            chunk = reader.read(min(1024 * 1024, remaining))
            if not chunk:
                break
            writer.write(chunk)
            index = chunk.rfind(b"\n")
            if index >= 0:
                last_newline = copied + index + 1
            copied += len(chunk)
            remaining -= len(chunk)
        writer.truncate(last_newline)
        writer.flush()
        os.fsync(writer.fileno())
    return TraceState(
        state.device, state.inode, state.mtime_ns, state.size, last_newline
    ), last_newline
