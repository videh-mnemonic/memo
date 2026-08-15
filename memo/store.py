from __future__ import annotations

from pathlib import Path

from .config import Paths
from .models import DirectorySession
from .registry import Registry
from .session_store import AmbiguousSessionError, SessionNotFoundError, SessionStore


def find_session(session_id: str, paths: Paths | None = None) -> tuple[Path, DirectorySession]:
    return SessionStore(paths or Paths.discover()).find(session_id)


def find_active(path: Path, paths: Paths | None = None) -> DirectorySession | None:
    paths = paths or Paths.discover()
    assert paths.registry is not None
    if not paths.registry.exists():
        return None
    with Registry(paths.registry) as registry:
        active = registry.lookup(path)
    if active is None:
        return None
    return DirectorySession.load(
        paths.archive / active.archive_namespace / active.session_id / "session.json"
    )


__all__ = ["AmbiguousSessionError", "SessionNotFoundError", "find_active", "find_session"]
