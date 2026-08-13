from __future__ import annotations

from .config import Paths
from .session_store import SessionStore


def inspect_session(session_id: str, paths: Paths | None = None) -> str:
    paths = paths or Paths.discover()
    store = SessionStore(paths)
    location, session = store.find(session_id)
    head = store.step(session.archive_namespace, session_id, -1)
    streams = sorted(head.stream_high_water)
    lines = [
        f"Session: {session.session_id}",
        "Format: directory",
        f"State: {session.state}",
        f"Source: {location}",
        f"Root: {session.root}",
        f"Namespace: {session.archive_namespace}",
        f"Created: {session.created_utc}",
        f"Updated: {session.updated_utc}",
        f"Step: {head.step}",
        f"Step time: {head.created_utc}",
        f"Snapshot entries: {len(head.entries)}",
        f"Terminal streams: {len(streams)}",
    ]
    lines.extend(f"  {terminal_id}: sequence={head.stream_high_water[terminal_id]}"
                 for terminal_id in streams)
    return "\n".join(lines) + "\n"
