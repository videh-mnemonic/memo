from __future__ import annotations

import fcntl
import json
import tarfile
from contextlib import AbstractContextManager
from dataclasses import dataclass
from pathlib import Path
from typing import IO

from .config import Paths
from .models import SessionMeta


class SessionNotFoundError(FileNotFoundError):
    pass


class AmbiguousSessionError(RuntimeError):
    pass


class SessionLockedError(RuntimeError):
    pass


@dataclass(frozen=True)
class SessionLocation:
    kind: str
    path: Path
    namespace: str | None = None


class SessionLock(AbstractContextManager["SessionLock"]):
    def __init__(self, path: Path):
        self.path = path
        self.handle: IO[str] | None = None

    def __enter__(self) -> "SessionLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("a+")
        try:
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.handle.close()
            self.handle = None
            raise SessionLockedError(f"session is active: {self.path.parent.name}")
        return self

    def __exit__(self, *args: object) -> None:
        if self.handle:
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
            self.handle.close()
            self.handle = None


def lock_is_held(path: Path) -> bool:
    try:
        with SessionLock(path):
            return False
    except SessionLockedError:
        return True


def find_session(session_id: str, paths: Paths | None = None) -> SessionLocation:
    paths = paths or Paths.discover()
    scratch = paths.scratch / session_id
    if scratch.is_dir():
        return SessionLocation("scratch", scratch)
    matches = sorted(paths.archive.glob(f"*/{session_id}.tar.gz")) if paths.archive.exists() else []
    if not matches:
        raise SessionNotFoundError(f"session not found: {session_id}")
    if len(matches) > 1:
        namespaces = ", ".join(p.parent.name for p in matches)
        raise AmbiguousSessionError(f"session {session_id} exists in multiple namespaces: {namespaces}")
    return SessionLocation("archive", matches[0], matches[0].parent.name)


def list_scratch(paths: Paths | None = None) -> list[tuple[Path, SessionMeta]]:
    paths = paths or Paths.discover()
    if not paths.scratch.exists():
        return []
    result = []
    for directory in sorted(paths.scratch.iterdir()):
        try:
            result.append((directory, SessionMeta.load(directory / "meta.json")))
        except (OSError, ValueError, TypeError):
            continue
    return result


def list_saved(paths: Paths | None = None) -> list[tuple[Path, SessionMeta]]:
    paths = paths or Paths.discover()
    if not paths.archive.exists():
        return []
    result = []
    for archive_path in sorted(paths.archive.glob("*/*.tar.gz")):
        try:
            with tarfile.open(archive_path, "r:gz") as archive:
                member = archive.getmember("meta.json")
                handle = archive.extractfile(member)
                if handle is None:
                    continue
                meta = SessionMeta.from_dict(json.loads(handle.read()))
                meta.validate()
                result.append((archive_path, meta))
        except (OSError, ValueError, TypeError, KeyError, tarfile.TarError):
            continue
    return result
