from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

from .config import Paths
from .models import CheckpointManifest, DirectorySession


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
        if not (path / manifest.snapshot).is_dir():
            raise ValueError(f"HEAD references missing snapshot: {manifest.snapshot}")
        for terminal_id, sequence in manifest.stream_high_water.items():
            metadata_path = path / "streams" / "terminals" / terminal_id / "stream.json"
            if sequence and not metadata_path.is_file():
                raise ValueError(f"HEAD references missing terminal stream: {terminal_id}")
            if sequence:
                metadata = json.loads(metadata_path.read_text())
                if metadata.get("highest_sequence", -1) < sequence:
                    raise ValueError(f"terminal stream does not reach checkpoint: {terminal_id}")
        return manifest

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
