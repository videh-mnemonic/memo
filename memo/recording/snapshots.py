"""Capture stable filesystem snapshots and publish numbered recording steps."""

from __future__ import annotations

import os
import shutil
import stat
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from .paths import StoragePaths
from .ignore import IgnorePolicy
from .models import DirectorySession, SnapshotEntry, StepManifest
from .store import SessionStore


MAX_FILE_SIZE_BYTES = 100 * 1024 * 1024


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _identity(value: os.stat_result) -> tuple[int, int, int, int, int]:
    return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_mode


def _retain(previous: Path | None, relative: Path, target: Path) -> bool:
    if previous is None or not (previous / relative).is_file():
        return False
    source = previous / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, target)
    os.chmod(target, stat.S_IMODE(source.stat().st_mode))
    return True


def _stable_copy(source: Path, target: Path, before: os.stat_result) -> bool:
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        with source.open("rb") as reader, target.open("wb") as writer:
            opened = os.fstat(reader.fileno())
            if _identity(opened) != _identity(before):
                return False
            shutil.copyfileobj(reader, writer)
            after = os.fstat(reader.fileno())
        current = source.stat(follow_symlinks=False)
    except (FileNotFoundError, PermissionError, OSError):
        target.unlink(missing_ok=True)
        return False
    if _identity(before) != _identity(after) or _identity(after) != _identity(current):
        target.unlink(missing_ok=True)
        return False
    os.chmod(target, stat.S_IMODE(current.st_mode))
    return True


def scan_tree(root: Path, destination: Path, *, previous: Path | None = None,
              paths: StoragePaths | None = None, max_file_size: int | None = None) -> list[SnapshotEntry]:
    entries: list[SnapshotEntry] = []
    seen: set[str] = set()
    policy = IgnorePolicy(root, paths)
    size_limit = MAX_FILE_SIZE_BYTES if max_file_size is None else max_file_size
    destination.mkdir(parents=True, exist_ok=True)
    for current, directories, files in os.walk(root, topdown=True, followlinks=False):
        current_path = Path(current)
        relative_dir = current_path.relative_to(root)
        kept = []
        for name in sorted(directories):
            source = current_path / name
            relative = source.relative_to(root)
            try:
                source_stat = source.stat(follow_symlinks=False)
            except FileNotFoundError:
                entries.append(SnapshotEntry(relative.as_posix(), "missing", 0))
                continue
            decision = policy.decision(source, is_dir=True)
            if decision.ignored:
                entries.append(SnapshotEntry(relative.as_posix(), "ignored-policy",
                                             stat.S_IMODE(source_stat.st_mode), detail=decision.source))
                seen.add(relative.as_posix())
            elif not stat.S_ISDIR(source_stat.st_mode):
                entries.append(SnapshotEntry(relative.as_posix(), "special",
                                             stat.S_IMODE(source_stat.st_mode)))
                seen.add(relative.as_posix())
            else:
                kept.append(name)
        directories[:] = kept
        files.sort()
        if relative_dir != Path("."):
            try:
                source_stat = current_path.stat(follow_symlinks=False)
            except FileNotFoundError:
                entries.append(SnapshotEntry(relative_dir.as_posix(), "missing", 0))
                continue
            (destination / relative_dir).mkdir()
            os.chmod(destination / relative_dir, stat.S_IMODE(source_stat.st_mode))
            entries.append(SnapshotEntry(relative_dir.as_posix(), "directory",
                                         stat.S_IMODE(source_stat.st_mode)))
            seen.add(relative_dir.as_posix())
        for name in files:
            source = current_path / name
            relative = source.relative_to(root)
            relative_name = relative.as_posix()
            try:
                source_stat = source.stat(follow_symlinks=False)
            except FileNotFoundError:
                entries.append(SnapshotEntry(relative_name, "missing", 0))
                seen.add(relative_name)
                continue
            decision = policy.decision(source)
            if decision.ignored:
                entries.append(SnapshotEntry(relative_name, "ignored-policy",
                                             stat.S_IMODE(source_stat.st_mode), source_stat.st_size,
                                             decision.source))
            elif not stat.S_ISREG(source_stat.st_mode):
                entries.append(SnapshotEntry(relative_name, "special",
                                             stat.S_IMODE(source_stat.st_mode), detail="non-regular"))
            else:
                target = destination / relative
                if source_stat.st_size > size_limit:
                    retained = _retain(previous, relative, target)
                    entries.append(SnapshotEntry(relative_name, "oversized",
                                                 stat.S_IMODE(source_stat.st_mode), source_stat.st_size,
                                                 f"limit={size_limit}", retained))
                elif _stable_copy(source, target, source_stat):
                    entries.append(SnapshotEntry(relative_name, "file",
                                                 stat.S_IMODE(source_stat.st_mode), source_stat.st_size))
                else:
                    retained = _retain(previous, relative, target)
                    entries.append(SnapshotEntry(relative_name, "unstable",
                                                 stat.S_IMODE(source_stat.st_mode), source_stat.st_size,
                                                 "changed-during-read", retained))
            seen.add(relative_name)
    if previous is not None:
        for old in sorted(previous.rglob("*")):
            relative = old.relative_to(previous)
            if old.is_file() and relative.as_posix() not in seen:
                entries.append(SnapshotEntry(relative.as_posix(), "missing",
                                             stat.S_IMODE(old.stat().st_mode), old.stat().st_size))
    return entries


class StepPublisher:
    def __init__(self, store: SessionStore, seal_streams=None):
        self.store = store
        self.seal_streams = seal_streams or (lambda _session: {})

    def publish(self, session: DirectorySession) -> StepManifest:
        return self._publish_once(session)

    def _publish_once(self, session: DirectorySession) -> StepManifest:
        high_water = self.seal_streams(session)
        previous_manifest = self.store.head(session.session_id)
        previous = None if previous_manifest is None else (
            self.store.session_path(session.session_id) / previous_manifest.snapshot
        )
        step = self.store.next_step(session.session_id)
        session_path = self.store.session_path(session.session_id)
        agent_runs = sorted(path.stem for path in (session_path / "agents" / "runs").glob("*.json"))
        temporary = Path(tempfile.mkdtemp(prefix=f".{step}.", dir=session_path / "snapshots"))
        try:
            entries = scan_tree(Path(session.root), temporary, previous=previous, paths=self.store.paths)
            manifest = StepManifest(session.session_id, step, utcnow(), f"snapshots/{step}",
                                    entries, high_water, agent_runs=agent_runs)
            return self.store.publish(session, manifest, temporary)
        except BaseException:
            shutil.rmtree(temporary, ignore_errors=True)
            raise
