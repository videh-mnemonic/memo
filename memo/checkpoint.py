from __future__ import annotations

import os
import shutil
import stat
import tempfile
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from .models import CheckpointManifest, DirectorySession, SnapshotEntry
from .session_store import SessionStore


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def scan_tree(root: Path, destination: Path) -> list[SnapshotEntry]:
    entries: list[SnapshotEntry] = []
    destination.mkdir(parents=True, exist_ok=True)
    for current, directories, files in os.walk(root, topdown=True, followlinks=False):
        current_path = Path(current)
        relative_dir = current_path.relative_to(root)
        directories[:] = sorted(
            name for name in directories if not (current_path / name).is_symlink()
        )
        files.sort()
        if relative_dir != Path("."):
            source_stat = current_path.stat(follow_symlinks=False)
            (destination / relative_dir).mkdir()
            os.chmod(destination / relative_dir, stat.S_IMODE(source_stat.st_mode))
            entries.append(SnapshotEntry(relative_dir.as_posix(), "directory",
                                         stat.S_IMODE(source_stat.st_mode)))
        for name in files:
            source = current_path / name
            source_stat = source.stat(follow_symlinks=False)
            if not stat.S_ISREG(source_stat.st_mode):
                continue
            relative = source.relative_to(root)
            target = destination / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, target, follow_symlinks=False)
            os.chmod(target, stat.S_IMODE(source_stat.st_mode))
            entries.append(SnapshotEntry(relative.as_posix(), "file",
                                         stat.S_IMODE(source_stat.st_mode), source_stat.st_size))
    return entries


@dataclass
class _State:
    running: bool = False
    requested: bool = False


class CheckpointPublisher:
    def __init__(self, store: SessionStore, seal_streams=None):
        self.store = store
        self.seal_streams = seal_streams or (lambda _session: {})
        self._condition = threading.Condition()
        self._states: dict[str, _State] = {}

    def publish(self, session: DirectorySession) -> CheckpointManifest:
        with self._condition:
            state = self._states.setdefault(session.session_id, _State())
            if state.running:
                state.requested = True
                while state.running:
                    self._condition.wait()
                current = self.store.head(session.archive_namespace, session.session_id)
                if current is None:
                    raise RuntimeError("coalesced checkpoint did not publish")
                return current
            state.running = True
        try:
            result = self._publish_once(session)
            while True:
                with self._condition:
                    if not state.requested:
                        return result
                    state.requested = False
                result = self._publish_once(session)
        finally:
            with self._condition:
                state.running = False
                self._condition.notify_all()

    def _publish_once(self, session: DirectorySession) -> CheckpointManifest:
        stream_high_water = self.seal_streams(session)
        generation = self.store.next_generation(session.archive_namespace, session.session_id)
        checkpoint_id = f"{generation:08d}-{uuid.uuid4().hex[:12]}"
        session_path = self.store.session_path(session.archive_namespace, session.session_id)
        temporary = Path(tempfile.mkdtemp(prefix=f".{checkpoint_id}.", dir=session_path / "snapshots"))
        try:
            entries = scan_tree(Path(session.root), temporary)
            manifest = CheckpointManifest(
                checkpoint_id=checkpoint_id,
                session_id=session.session_id,
                generation=generation,
                created_utc=utcnow(),
                snapshot=f"snapshots/{checkpoint_id}",
                entries=entries,
                stream_high_water=stream_high_water,
            )
            return self.store.publish(session, manifest, temporary)
        except BaseException:
            shutil.rmtree(temporary, ignore_errors=True)
            raise
