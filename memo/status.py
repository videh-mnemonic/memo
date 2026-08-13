from __future__ import annotations

from datetime import datetime, timezone

from .config import Paths
from .registry import Registry
from .session_store import SessionStore
from .store import list_directory_saved, list_saved, list_scratch, lock_is_held


def _idle(value: str) -> str:
    stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    seconds = max(0, int((datetime.now(timezone.utc) - stamp).total_seconds()))
    if seconds >= 86400:
        return f"{seconds // 86400}d"
    if seconds >= 3600:
        return f"{seconds // 3600}h"
    if seconds >= 60:
        return f"{seconds // 60}m"
    return f"{seconds}s"


def render_status(paths: Paths | None = None) -> str:
    paths = paths or Paths.discover()
    rows = [("FORMAT", "STATE", "SESSION", "ROOT/REPO", "NAMESPACE", "GEN/LEGS", "ATTACH", "UPDATED")]
    active = {}
    if paths.registry is not None and paths.registry.exists():
        with Registry(paths.registry) as registry:
            for item in registry.list_active():
                active[item.session_id] = (
                    item.state,
                    len([attachment for attachment in registry.list_attachments(item.session_id)
                         if attachment.detached_utc is None]),
                )
    session_store = SessionStore(paths)
    for _, session in list_directory_saved(paths):
        head = session_store.head(session.archive_namespace, session.session_id)
        state, attachments = active.get(session.session_id, (session.state, 0))
        rows.append(("directory", state, session.session_id, session.root,
                     session.archive_namespace, str(head.generation if head else 0),
                     str(attachments), head.created_utc if head else session.updated_utc))
    for directory, meta in list_scratch(paths):
        state = "active" if lock_is_held(directory / "session.lock") else "scratch"
        rows.append(("legacy", state, meta.session_id, meta.repo_name, meta.archive_namespace,
                     str(len(meta.legs)), "-", meta.last_activity_utc))
    for _, meta in list_saved(paths):
        rows.append(("legacy", "saved", meta.session_id, meta.repo_name, meta.archive_namespace,
                     str(len(meta.legs)), "-", meta.shipped_at or meta.last_activity_utc))
    if len(rows) == 1:
        return "No sessions.\n"
    widths = [max(len(row[i]) for row in rows) for i in range(len(rows[0]))]
    return "\n".join("  ".join(value.ljust(widths[i]) for i, value in enumerate(row)) for row in rows) + "\n"
