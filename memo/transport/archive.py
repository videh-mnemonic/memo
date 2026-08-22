"""Create, hash, verify, safely extract, and install recording archives."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tarfile
import tempfile
import uuid
from collections.abc import Callable, Iterable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO

import zstandard

from ..agents.run_metadata import AgentRunMetadata
from ..recording.git_snapshots import GitSnapshotStore
from ..recording.metadata import DirectorySession, StepManifest
from ..recording.store import SessionStore

STREAM_READ_SIZE = 64 * 1024
DEFAULT_LARGE_ARCHIVE_BYTES = 1 * 1024 * 1024 * 1024
ArchiveFileHandler = Callable[[BinaryIO], None]
ArchiveFileHandlerFactory = Callable[[PurePosixPath, tarfile.TarInfo], ArchiveFileHandler | None]


class LargeGenerationError(ValueError):
    """Raised when an archive exceeds the configured upload safety limit."""


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
        self,
        source: BinaryIO,
        digest: Any | None = None,
        read_size: int = STREAM_READ_SIZE,
        progress: Callable[[int, int, str], None] | None = None,
        progress_total: int | None = None,
        progress_message: str = "reading archive",
    ) -> None:
        self.source = source
        self.digest = digest or hashlib.sha256()
        self.read_size = read_size
        self.progress = progress
        self.progress_total = progress_total
        self.progress_message = progress_message
        self.bytes_read = 0

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        requested = self.read_size if size < 0 else min(size, self.read_size)
        data = self.source.read(requested)
        self.digest.update(data)
        self.bytes_read += len(data)
        if self.progress is not None and self.progress_total is not None:
            self.progress(self.bytes_read, self.progress_total, self.progress_message)
        return data

    def readinto(self, buffer: bytearray | memoryview) -> int:
        data = self.read(min(len(buffer), self.read_size))
        count = len(data)
        buffer[:count] = data
        return count

    def hexdigest(self) -> str:
        return self.digest.hexdigest()


def write_deterministic_tar_zst(
    root: Path,
    paths: Iterable[Path],
    target: BinaryIO,
    *,
    extra_files: Mapping[str, Path] | None = None,
) -> None:
    """Write a reproducible, streaming tar.zst archive."""
    compressor = zstandard.ZstdCompressor(
        level=3,
        threads=1,
        write_content_size=False,
        write_checksum=False,
        write_dict_id=False,
    )
    entries = [
        (path.relative_to(root).as_posix(), path)
        for path in paths
        if path.relative_to(root).as_posix() != "session.lock" and not path.is_socket()
    ]
    entries.extend((name, path) for name, path in (extra_files or {}).items())
    names = [name for name, _ in entries]
    if len(names) != len(set(names)):
        raise ValueError("archive contains duplicate paths")
    with compressor.stream_writer(target, closefd=False) as compressed:
        with tarfile.open(fileobj=compressed, mode="w|", format=tarfile.PAX_FORMAT) as archive:
            for name, path in sorted(entries):
                info = archive.gettarinfo(str(path), arcname=name)
                info.uid = info.gid = 0
                info.uname = info.gname = ""
                info.mtime = 0
                if info.isfile():
                    with path.open("rb") as handle:
                        archive.addfile(info, handle)
                else:
                    archive.addfile(info)


def safe_extract_tar_zst_stream(
    source: BinaryIO,
    target: Path,
    *,
    progress: Callable[[int, int, str], None] | None = None,
    progress_total: int | None = None,
    progress_message: str = "downloading archive",
    file_handler: ArchiveFileHandlerFactory | None = None,
) -> str:
    """Safely extract a tar.zst stream and return its SHA-256 digest.

    ``file_handler`` may consume selected regular files instead of writing
    them. Every member still passes the same path, type, duplicate, and shape
    checks before the handler is selected.
    """
    root = target.resolve()
    hashing = HashingReader(
        source,
        progress=progress,
        progress_total=progress_total,
        progress_message=progress_message,
    )
    decompressor = zstandard.ZstdDecompressor()
    shapes: dict[tuple[str, ...], str] = {}
    paths_with_descendants: set[tuple[str, ...]] = set()
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
                if member.isfile() and key in paths_with_descendants:
                    raise ValueError(f"archive path conflict: {member.name}")
                try:
                    destination = root.joinpath(*parts)
                    destination.resolve().relative_to(root)
                except ValueError as error:
                    raise ValueError(f"archive path escapes destination: {member.name}") from error
                shapes[key] = "file" if member.isfile() else "directory"
                paths_with_descendants.update(key[:index] for index in range(1, len(key)))
                handler = file_handler(name, member) if file_handler and member.isfile() else None
                if handler is not None:
                    extracted = archive.extractfile(member)
                    if extracted is None:
                        raise ValueError(f"archive file cannot be read: {member.name}")
                    handler(extracted)
                    while extracted.read(STREAM_READ_SIZE):
                        pass
                    continue
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
    entries = session_path / "entries"
    best_effort_report = session_path / "legacy-best-effort-migration.json"
    if best_effort_report.is_file():
        paths.append(best_effort_report)
    if entries.is_dir():
        paths.append(entries)
        paths.extend(entries.rglob("*"))
    for manifest in manifests:
        paths.append(session_path / "steps" / f"{manifest.step}.json")
        if not manifest.snapshot_commit:
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
    launch_root = session_path / "agents" / "launches"
    if launch_root.is_dir():
        paths.extend([session_path / "agents", launch_root, *launch_root.glob("*.json")])
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
    size_bytes: int = 0

    def cleanup(self) -> None:
        self.path.unlink(missing_ok=True)


def prepare_generation(
    store: SessionStore,
    session: DirectorySession,
    progress: Callable[[int, int, str], None] | None = None,
) -> PreparedGeneration:
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
        if progress is not None:
            progress(0, 1, "creating archive")
        with tempfile.TemporaryDirectory(prefix="bundle-", dir=upload_dir) as bundle_dir:
            extra_files: dict[str, Path] = {}
            if manifest.snapshot_commit:
                bundle = Path(bundle_dir) / "snapshots.bundle"
                GitSnapshotStore(root / "snapshots.git").create_bundle(
                    manifest.snapshot_commit, bundle
                )
                extra_files["snapshots.bundle"] = bundle
            with os.fdopen(descriptor, "wb") as handle:
                hashing = HashingWriter(handle)
                write_deterministic_tar_zst(
                    root,
                    _history_paths(root, manifests),
                    hashing,
                    extra_files=extra_files,
                )
                handle.flush()
                os.fsync(handle.fileno())
        prepared = PreparedGeneration(
            session.session_id, manifest.step, hashing.hexdigest(), path, path.stat().st_size
        )
        if progress is not None:
            progress(1, 1, f"archive ready ({prepared.size_bytes / (1024**2):.1f} MiB)")
        return prepared
    except BaseException:
        with suppress(OSError):
            os.close(descriptor)
        path.unlink(missing_ok=True)
        raise


def large_archive_limit() -> int:
    value = os.environ.get("MEMO_LARGE_ARCHIVE_BYTES")
    if value is None:
        return DEFAULT_LARGE_ARCHIVE_BYTES
    try:
        limit = int(value)
    except ValueError as error:
        raise ValueError("MEMO_LARGE_ARCHIVE_BYTES must be a nonnegative integer") from error
    if limit < 0:
        raise ValueError("MEMO_LARGE_ARCHIVE_BYTES must be a nonnegative integer")
    return limit


def enforce_archive_limit(prepared: PreparedGeneration, allow_large: bool = False) -> None:
    limit = large_archive_limit()
    if not allow_large and prepared.size_bytes > limit:
        raise LargeGenerationError(
            f"archive for {prepared.session_id} step {prepared.step} is "
            f"{prepared.size_bytes / (1024**3):.1f} GiB, exceeding the configured "
            f"limit of {limit / (1024**3):.1f} GiB; "
            "inspect the recording or retry with --allow-large"
        )
