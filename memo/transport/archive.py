"""Create, hash, verify, safely extract, and install recording archives."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tarfile
import tempfile
import uuid
from collections.abc import Iterable
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO

import zstandard

from ..agents.run_metadata import AgentRunMetadata
from ..recording.metadata import DirectorySession, StepManifest
from ..recording.store import SessionStore

STREAM_READ_SIZE = 64 * 1024


class HashingWriter:
    """Forward writes while calculating their SHA-256 digest."""

    def __init__(self, target: BinaryIO, digest: Any | None = None) -> None:
        self.target = target
        self.digest = digest or hashlib.sha256()

    def writable(self) -> bool:
        return True

    def write(self, data: bytes) -> int:
        self.digest.update(data)
        written = self.target.write(data)
        if written != len(data):
            raise OSError(f"short write: expected {len(data)} bytes, wrote {written}")
        return written

    def flush(self) -> None:
        flush = getattr(self.target, "flush", None)
        if flush is not None:
            flush()

    def hexdigest(self) -> str:
        return self.digest.hexdigest()


class HashingReader:
    """Bound reads while calculating their SHA-256 digest."""

    def __init__(
        self, source: BinaryIO, digest: Any | None = None, read_size: int = STREAM_READ_SIZE
    ) -> None:
        self.source = source
        self.digest = digest or hashlib.sha256()
        self.read_size = read_size

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        requested = self.read_size if size < 0 else min(size, self.read_size)
        data = self.source.read(requested)
        self.digest.update(data)
        return data

    def readinto(self, buffer: bytearray | memoryview) -> int:
        data = self.read(min(len(buffer), self.read_size))
        count = len(data)
        buffer[:count] = data
        return count

    def hexdigest(self) -> str:
        return self.digest.hexdigest()


def write_deterministic_tar_zst(root: Path, paths: Iterable[Path], target: BinaryIO) -> None:
    """Write a reproducible, streaming tar.zst archive."""
    compressor = zstandard.ZstdCompressor(
        level=3,
        threads=1,
        write_content_size=False,
        write_checksum=False,
        write_dict_id=False,
    )
    with compressor.stream_writer(target, closefd=False) as compressed:
        with tarfile.open(fileobj=compressed, mode="w|", format=tarfile.PAX_FORMAT) as archive:
            for path in sorted(paths, key=lambda item: item.relative_to(root).as_posix()):
                relative = path.relative_to(root)
                if relative.as_posix() == "session.lock" or path.is_socket():
                    continue
                info = archive.gettarinfo(str(path), arcname=relative.as_posix())
                info.uid = info.gid = 0
                info.uname = info.gname = ""
                info.mtime = 0
                if info.isfile():
                    with path.open("rb") as handle:
                        archive.addfile(info, handle)
                else:
                    archive.addfile(info)


def safe_extract_tar_zst_stream(source: BinaryIO, target: Path) -> str:
    """Safely extract a tar.zst stream and return its SHA-256 digest."""
    root = target.resolve()
    hashing = HashingReader(source)
    decompressor = zstandard.ZstdDecompressor()
    shapes: dict[tuple[str, ...], str] = {}
    with decompressor.stream_reader(hashing, closefd=False) as decompressed:
        with tarfile.open(fileobj=decompressed, mode="r|") as archive:
            for member in archive:
                name = PurePosixPath(member.name)
                parts = name.parts
                if (
                    name.is_absolute()
                    or not parts
                    or member.name.endswith("/.")
                    or any(part in ("", ".", "..") for part in parts)
                ):
                    raise ValueError(f"unsafe archive path: {member.name}")
                if member.issym() or member.islnk() or member.isdev():
                    raise ValueError(f"unsupported archive entry: {member.name}")
                if not (member.isfile() or member.isdir()):
                    raise ValueError(f"unsupported archive entry: {member.name}")
                key = tuple(parts)
                if key in shapes:
                    raise ValueError(f"duplicate archive entry: {member.name}")
                for index in range(1, len(key)):
                    if shapes.get(key[:index]) == "file":
                        raise ValueError(f"archive path conflict: {member.name}")
                if member.isfile() and any(
                    existing[: len(key)] == key for existing in shapes if len(existing) > len(key)
                ):
                    raise ValueError(f"archive path conflict: {member.name}")
                try:
                    destination = root.joinpath(*parts)
                    destination.resolve().relative_to(root)
                except ValueError as error:
                    raise ValueError(f"archive path escapes destination: {member.name}") from error
                shapes[key] = "file" if member.isfile() else "directory"
                archive.extract(member, target, filter="data")
    while hashing.read(STREAM_READ_SIZE):
        pass
    return hashing.hexdigest()


def atomic_install_directory(prepared: Path, destination: Path, force: bool = False) -> None:
    """Install a prepared directory without leaving a partial destination."""
    if destination.exists() and not force:
        raise FileExistsError(f"local session already exists: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    backup = destination.with_name(f".{destination.name}.backup-{uuid.uuid4().hex}")
    replaced = False
    try:
        if destination.exists():
            os.replace(destination, backup)
            replaced = True
        os.replace(prepared, destination)
    except BaseException:
        if replaced and backup.exists() and not destination.exists():
            os.replace(backup, destination)
        raise
    finally:
        if backup.exists():
            shutil.rmtree(backup)


def _history_paths(session_path: Path, manifests: list[StepManifest]) -> list[Path]:
    paths = [
        session_path / "session.json",
        session_path / "HEAD",
        session_path / "steps",
        session_path / "snapshots",
    ]
    for manifest in manifests:
        paths.append(session_path / "steps" / f"{manifest.step}.json")
        paths.extend((session_path / manifest.snapshot).rglob("*"))
        paths.append(session_path / manifest.snapshot)
    terminal_root = session_path / "streams" / "terminals"
    high_water_by_terminal: dict[str, int] = {}
    for manifest in manifests:
        for terminal_id, high_water in manifest.stream_high_water.items():
            high_water_by_terminal[terminal_id] = max(
                high_water, high_water_by_terminal.get(terminal_id, 0)
            )
    if high_water_by_terminal:
        paths.extend([session_path / "streams", terminal_root])
    for terminal_id, high_water in high_water_by_terminal.items():
        if high_water == 0:
            continue
        metadata = terminal_root / terminal_id / "stream.json"
        paths.extend([metadata, metadata.parent, metadata.parent / "chunks"])
        values = json.loads(metadata.read_text())
        paths.extend(metadata.parent / item for item in values.get("chunks", []))
    agent_runs = sorted({run_id for manifest in manifests for run_id in manifest.agent_runs})
    if agent_runs:
        paths.extend(
            [
                session_path / "agents",
                session_path / "agents" / "runs",
                session_path / "agents" / "traces",
            ]
        )
    for run_id in agent_runs:
        metadata = session_path / "agents" / "runs" / f"{run_id}.json"
        paths.append(metadata)
        run = AgentRunMetadata.load(metadata)
        paths.append(session_path / "agents" / "traces" / run.trace_file)
    return sorted(
        {path for path in paths if path.exists()},
        key=lambda item: item.relative_to(session_path).as_posix(),
    )


@dataclass
class PreparedGeneration:
    """A temporary archive ready to upload as a remote generation."""

    session_id: str
    step: int
    digest: str
    path: Path

    def cleanup(self) -> None:
        self.path.unlink(missing_ok=True)


def prepare_generation(store: SessionStore, session: DirectorySession) -> PreparedGeneration:
    """Prepare a deterministic generation archive on disk."""
    manifests = store.steps(session.session_id)
    if not manifests:
        raise ValueError(f"session has no published step: {session.session_id}")
    manifest = manifests[-1]
    root = store.session_path(session.session_id)
    upload_dir = store.paths.runtime / "uploads"
    upload_dir.mkdir(parents=True, exist_ok=True)
    descriptor, name = tempfile.mkstemp(
        prefix=f".{session.session_id}-{manifest.step:08d}-",
        suffix=".tar.zst",
        dir=upload_dir,
    )
    path = Path(name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            hashing = HashingWriter(handle)
            write_deterministic_tar_zst(root, _history_paths(root, manifests), hashing)
            handle.flush()
            os.fsync(handle.fileno())
        return PreparedGeneration(session.session_id, manifest.step, hashing.hexdigest(), path)
    except BaseException:
        with suppress(OSError):
            os.close(descriptor)
        path.unlink(missing_ok=True)
        raise
