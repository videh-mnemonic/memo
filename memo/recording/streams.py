from __future__ import annotations

import base64
import gzip
import json
import os
import struct
import threading
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Iterable

from ..config import StoragePaths
from .store import atomic_write

if TYPE_CHECKING:
    from ..daemon.registry import Registry


@dataclass(frozen=True)
class StreamEvent:
    terminal_id: str
    sequence: int
    direction: str
    data: str
    receipt_ns: int

    def validate(self) -> None:
        if self.sequence < 1:
            raise ValueError("event sequence must be positive")
        if self.direction not in {"input", "output"}:
            raise ValueError(f"invalid event direction: {self.direction}")
        try:
            base64.b64decode(self.data, validate=True)
        except ValueError as error:
            raise ValueError("event data must be base64") from error

    def bytes(self) -> bytes:
        return base64.b64decode(self.data)

    def to_dict(self) -> dict[str, object]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, object]) -> "StreamEvent":
        event = cls(**value)  # type: ignore[arg-type]
        event.validate()
        return event


class StreamStore:
    def __init__(self, paths: StoragePaths, registry: Registry):
        self.paths = paths
        self.registry = registry
        self._locks: dict[str, threading.Lock] = {}
        self._locks_guard = threading.Lock()

    def _lock(self, terminal_id: str) -> threading.Lock:
        with self._locks_guard:
            return self._locks.setdefault(terminal_id, threading.Lock())

    def _spool(self, session_id: str, terminal_id: str) -> Path:
        assert self.paths.spool is not None
        return self.paths.spool / session_id / "spool" / f"{terminal_id}.frames"

    @staticmethod
    def _encode_event(event: StreamEvent) -> bytes:
        body = json.dumps(event.to_dict(), sort_keys=True, separators=(",", ":")).encode()
        return struct.pack("!I", len(body)) + body

    def append(self, session_id: str, terminal_id: str,
               values: list[dict[str, object]], receipt_ns: int) -> int:
        attachment = self.registry.attachment(terminal_id)
        if attachment is None or attachment.session_id != session_id:
            raise KeyError(f"unknown terminal attachment: {terminal_id}")
        if not values:
            return attachment.accepted_sequence
        if len(values) > 1024:
            raise ValueError("event batch exceeds backpressure limit")
        with self._lock(terminal_id):
            attachment = self.registry.attachment(terminal_id)
            if attachment is None or attachment.session_id != session_id:
                raise RuntimeError(f"recording ended: {session_id}")
            active = self.registry.lookup_session(session_id)
            if active is None or active.state != "active" or attachment.detached_utc is not None:
                raise RuntimeError(f"recording ended: {session_id}")
            expected = attachment.accepted_sequence
            events = []
            for value in values:
                event = StreamEvent(
                    terminal_id=terminal_id,
                    sequence=int(value["sequence"]),
                    direction=str(value["direction"]),
                    data=str(value["data"]),
                    receipt_ns=receipt_ns,
                )
                event.validate()
                expected += 1
                if event.sequence != expected:
                    raise ValueError(
                        f"event sequence {event.sequence} does not follow acknowledged sequence {expected - 1}"
                    )
                events.append(event)
            spool = self._spool(session_id, terminal_id)
            spool.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            with spool.open("ab") as handle:
                for event in events:
                    handle.write(self._encode_event(event))
                handle.flush()
                os.fsync(handle.fileno())
            self.registry.accept_sequence(terminal_id, attachment.accepted_sequence, expected)
            return expected

    def drain_and_detach(self, session_id: str, detached_utc: str) -> list[str]:
        detached = []
        for attachment in self.registry.attached(session_id):
            self.detach(attachment.terminal_id, detached_utc)
            detached.append(attachment.terminal_id)
        return detached

    def detach(self, terminal_id: str, detached_utc: str) -> None:
        with self._lock(terminal_id):
            self.registry.detach(terminal_id, detached_utc)

    def events(self, session_id: str, terminal_id: str) -> list[StreamEvent]:
        spool = self._spool(session_id, terminal_id)
        if not spool.is_file():
            return []
        result = []
        with spool.open("rb") as handle:
            while header := handle.read(4):
                if len(header) != 4:
                    raise ValueError("incomplete stream spool header")
                size = struct.unpack("!I", header)[0]
                body = handle.read(size)
                if len(body) != size:
                    raise ValueError("incomplete stream spool frame")
                result.append(StreamEvent.from_dict(json.loads(body)))
        return result

    def recover_spool(self, session_id: str, terminal_id: str) -> int:
        spool = self._spool(session_id, terminal_id)
        if not spool.is_file():
            self.registry.recover_sequence(terminal_id, 0)
            return 0
        valid_end = 0
        events: list[StreamEvent] = []
        with spool.open("r+b") as handle:
            while True:
                frame_start = handle.tell()
                header = handle.read(4)
                if not header:
                    valid_end = frame_start
                    break
                if len(header) != 4:
                    break
                size = struct.unpack("!I", header)[0]
                if size > 16 * 1024 * 1024:
                    break
                body = handle.read(size)
                if len(body) != size:
                    break
                try:
                    event = StreamEvent.from_dict(json.loads(body))
                except (ValueError, TypeError, KeyError, json.JSONDecodeError):
                    break
                expected = len(events) + 1
                if event.terminal_id != terminal_id or event.sequence != expected:
                    break
                events.append(event)
                valid_end = handle.tell()
            handle.truncate(valid_end)
            handle.flush()
            os.fsync(handle.fileno())
        accepted = events[-1].sequence if events else 0
        self.registry.recover_sequence(terminal_id, accepted)
        return accepted

    def recover_all(self) -> dict[str, int]:
        recovered = {}
        for active in self.registry.list_active():
            for attachment in self.registry.list_attachments(active.session_id):
                recovered[attachment.terminal_id] = self.recover_spool(
                    active.session_id, attachment.terminal_id
                )
        return recovered

    def seal_session(self, session_id: str) -> dict[str, int]:
        assert self.paths.archive is not None
        session_path = self.paths.archive / session_id
        high_water: dict[str, int] = {}
        for attachment in self.registry.list_attachments(session_id):
            with self._lock(attachment.terminal_id):
                events = self.events(session_id, attachment.terminal_id)
                if not events:
                    high_water[attachment.terminal_id] = 0
                    continue
                terminal_path = session_path / "streams" / "terminals" / attachment.terminal_id
                chunks = terminal_path / "chunks"
                chunks.mkdir(parents=True, exist_ok=True)
                metadata_path = terminal_path / "stream.json"
                metadata = json.loads(metadata_path.read_text()) if metadata_path.is_file() else {
                    "schema_version": 1,
                    "terminal_id": attachment.terminal_id,
                    "highest_sequence": 0,
                    "chunks": [],
                }
                previous = int(metadata["highest_sequence"])
                if previous > attachment.accepted_sequence:
                    raise ValueError(f"stream metadata exceeds durable spool: {attachment.terminal_id}")
                new_events = [event for event in events if event.sequence > previous]
                end = attachment.accepted_sequence
                if new_events:
                    chunk_id = f"{new_events[0].sequence:08d}-{end:08d}"
                    chunk = chunks / f"{chunk_id}.jsonl.gz"
                    content = b"".join(
                        json.dumps(event.to_dict(), sort_keys=True, separators=(",", ":")).encode() + b"\n"
                        for event in new_events
                    )
                    atomic_write(chunk, gzip.compress(content, mtime=0))
                    metadata["chunks"].append(f"chunks/{chunk.name}")
                metadata["highest_sequence"] = end
                atomic_write(
                    metadata_path,
                    (json.dumps(metadata, sort_keys=True, indent=2) + "\n").encode(),
                )
                high_water[attachment.terminal_id] = end
        return high_water


def read_chunk(path: Path) -> list[StreamEvent]:
    return [StreamEvent.from_dict(json.loads(line)) for line in gzip.decompress(path.read_bytes()).splitlines()]


def merged_timeline(chunks: Iterable[Path]) -> list[StreamEvent]:
    events = [event for chunk in chunks for event in read_chunk(chunk)]
    return sorted(events, key=lambda event: (event.receipt_ns, event.terminal_id, event.sequence))
