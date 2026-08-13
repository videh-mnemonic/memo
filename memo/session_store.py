from __future__ import annotations

import json
import os
import tempfile
import shutil
from pathlib import Path
from typing import TYPE_CHECKING

from .config import Paths
from .models import CheckpointManifest, DirectorySession

if TYPE_CHECKING:
    from .streams import StreamEvent


def _json_bytes(value: dict[str, object]) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")


def atomic_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(name, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except BaseException:
        Path(name).unlink(missing_ok=True)
        raise


class SessionStore:
    def __init__(self, paths: Paths):
        self.paths = paths
        self.paths.ensure_storage()

    def session_path(self, namespace: str, session_id: str) -> Path:
        assert self.paths.directory_archive is not None
        return self.paths.directory_archive / namespace / session_id

    def list_sessions(self) -> list[tuple[Path, DirectorySession]]:
        return list_directory_sessions(self.paths)

    def create(self, session: DirectorySession) -> Path:
        session.validate()
        path = self.session_path(session.archive_namespace, session.session_id)
        path.mkdir(parents=True, exist_ok=False)
        (path / "checkpoints").mkdir()
        (path / "snapshots").mkdir()
        (path / "streams" / "terminals").mkdir(parents=True)
        atomic_write(path / "session.json", _json_bytes(session.to_dict()))
        return path

    def load_session(self, namespace: str, session_id: str) -> DirectorySession:
        return DirectorySession.load(self.session_path(namespace, session_id) / "session.json")

    def update_session(self, session: DirectorySession) -> None:
        session.validate()
        atomic_write(
            self.session_path(session.archive_namespace, session.session_id) / "session.json",
            _json_bytes(session.to_dict()),
        )

    def head(self, namespace: str, session_id: str) -> CheckpointManifest | None:
        path = self.session_path(namespace, session_id)
        head = path / "HEAD"
        if not head.is_file():
            return None
        checkpoint_id = head.read_text().strip()
        if not checkpoint_id or Path(checkpoint_id).name != checkpoint_id:
            raise ValueError("invalid HEAD checkpoint id")
        manifest = CheckpointManifest.load(path / "checkpoints" / f"{checkpoint_id}.json")
        if manifest.session_id != session_id:
            raise ValueError("HEAD references checkpoint for another session")
        if not (path / manifest.snapshot).is_dir():
            raise ValueError(f"HEAD references missing snapshot: {manifest.snapshot}")
        snapshot = path / manifest.snapshot
        for entry in manifest.entries:
            if entry.kind == "file" or entry.retained:
                artifact = snapshot / entry.path
                if not artifact.is_file():
                    raise ValueError(f"HEAD references missing snapshot file: {entry.path}")
        for terminal_id, sequence in manifest.stream_high_water.items():
            metadata_path = path / "streams" / "terminals" / terminal_id / "stream.json"
            if sequence and not metadata_path.is_file():
                raise ValueError(f"HEAD references missing terminal stream: {terminal_id}")
            if sequence:
                metadata = json.loads(metadata_path.read_text())
                if metadata.get("highest_sequence", -1) < sequence:
                    raise ValueError(f"terminal stream does not reach checkpoint: {terminal_id}")
                for chunk in metadata.get("chunks", []):
                    chunk_path = metadata_path.parent / chunk
                    if not chunk_path.is_file():
                        raise ValueError(f"terminal stream references missing chunk: {terminal_id}/{chunk}")
        return manifest

    def checkpoint(self, namespace: str, session_id: str,
                   selector: str | int | None = None) -> CheckpointManifest:
        path = self.session_path(namespace, session_id)
        if selector is None or selector == "final" or selector == "HEAD":
            manifest = self.head(namespace, session_id)
            if manifest is None:
                raise ValueError(f"session has no published checkpoint: {session_id}")
            return manifest
        matches: list[Path]
        if isinstance(selector, int) or str(selector).isdigit():
            generation = int(selector)
            matches = sorted((path / "checkpoints").glob("*.json"))
            manifests = [CheckpointManifest.load(item) for item in matches]
            found = [item for item in manifests if item.generation == generation]
            if len(found) != 1:
                raise ValueError(f"checkpoint generation not found: {generation}")
            manifest = found[0]
        else:
            checkpoint_id = str(selector).removeprefix("checkpoint:")
            if Path(checkpoint_id).name != checkpoint_id:
                raise ValueError(f"invalid checkpoint selector: {selector}")
            manifest = CheckpointManifest.load(path / "checkpoints" / f"{checkpoint_id}.json")
        self._validate_manifest(path, session_id, manifest)
        return manifest

    @staticmethod
    def _validate_manifest(path: Path, session_id: str,
                           manifest: CheckpointManifest) -> None:
        if manifest.session_id != session_id:
            raise ValueError("checkpoint belongs to another session")
        snapshot = path / manifest.snapshot
        if not snapshot.is_dir():
            raise ValueError(f"checkpoint references missing snapshot: {manifest.snapshot}")
        for entry in manifest.entries:
            if entry.kind == "file" or entry.retained:
                if not (snapshot / entry.path).is_file():
                    raise ValueError(f"checkpoint references missing snapshot file: {entry.path}")

    def restore(self, namespace: str, session_id: str, destination: Path,
                selector: str | int | None = None, force: bool = False) -> Path:
        manifest = self.checkpoint(namespace, session_id, selector)
        source = self.session_path(namespace, session_id) / manifest.snapshot
        if destination.exists():
            occupied = not destination.is_dir() or any(destination.iterdir())
            if occupied and not force:
                raise FileExistsError(f"destination is not empty: {destination}")
            if occupied:
                if destination.is_dir():
                    shutil.rmtree(destination)
                else:
                    destination.unlink()
        destination.mkdir(parents=True, exist_ok=True)
        shutil.copytree(source, destination, dirs_exist_ok=True, copy_function=shutil.copy2)
        return destination

    def stream_events(self, namespace: str, session_id: str,
                      terminal_id: str | None = None,
                      selector: str | int | None = None) -> list[StreamEvent]:
        from .streams import merged_timeline
        manifest = self.checkpoint(namespace, session_id, selector)
        terminals = manifest.stream_high_water
        if terminal_id is not None and terminal_id not in terminals:
            raise KeyError(f"terminal stream not found: {terminal_id}")
        selected = [terminal_id] if terminal_id is not None else sorted(terminals)
        chunks = []
        root = self.session_path(namespace, session_id) / "streams" / "terminals"
        for stream_id in selected:
            metadata_path = root / stream_id / "stream.json"
            if terminals[stream_id] == 0:
                continue
            if not metadata_path.is_file():
                raise ValueError(f"checkpoint references missing terminal stream: {stream_id}")
            metadata = json.loads(metadata_path.read_text())
            for relative in metadata.get("chunks", []):
                chunk = metadata_path.parent / relative
                if not chunk.is_file():
                    raise ValueError(f"terminal stream references missing chunk: {stream_id}/{relative}")
                chunks.append(chunk)
        events = merged_timeline(chunks)
        return [event for event in events
                if event.sequence <= terminals.get(event.terminal_id, -1)]

    def check_integrity(self, namespace: str, session_id: str) -> CheckpointManifest | None:
        path = self.session_path(namespace, session_id)
        session = self.load_session(namespace, session_id)
        if session.session_id != session_id or session.archive_namespace != namespace:
            raise ValueError("session metadata does not match archive location")
        return self.head(namespace, session_id)

    def next_generation(self, namespace: str, session_id: str) -> int:
        current = self.head(namespace, session_id)
        return 1 if current is None else current.generation + 1

    def publish(self, session: DirectorySession, manifest: CheckpointManifest,
                prepared_snapshot: Path) -> CheckpointManifest:
        manifest.validate()
        if manifest.session_id != session.session_id:
            raise ValueError("checkpoint belongs to a different session")
        path = self.session_path(session.archive_namespace, session.session_id)
        expected_generation = self.next_generation(session.archive_namespace, session.session_id)
        if manifest.generation != expected_generation:
            raise ValueError(
                f"checkpoint generation {manifest.generation} is not next generation {expected_generation}"
            )
        snapshot = path / manifest.snapshot
        manifest_path = path / "checkpoints" / f"{manifest.checkpoint_id}.json"
        if snapshot.exists() or manifest_path.exists():
            raise FileExistsError(f"checkpoint already exists: {manifest.checkpoint_id}")
        prepared_snapshot.replace(snapshot)
        self._fsync_tree(snapshot)
        atomic_write(manifest_path, _json_bytes(manifest.to_dict()))
        atomic_write(path / "HEAD", f"{manifest.checkpoint_id}\n".encode())
        return manifest

    @staticmethod
    def _fsync_tree(root: Path) -> None:
        for path in sorted(root.rglob("*"), reverse=True):
            if path.is_file():
                with path.open("rb") as handle:
                    os.fsync(handle.fileno())
            elif path.is_dir():
                descriptor = os.open(path, os.O_RDONLY)
                try:
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
        descriptor = os.open(root, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def list_directory_sessions(paths: Paths) -> list[tuple[Path, DirectorySession]]:
    assert paths.directory_archive is not None
    result = []
    if not paths.directory_archive.exists():
        return result
    for session_file in sorted(paths.directory_archive.glob("*/*/session.json")):
        try:
            result.append((session_file.parent, DirectorySession.load(session_file)))
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            continue
    return result
