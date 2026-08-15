from __future__ import annotations

from .config import Paths
from .registry import Registry
from .session_store import SessionStore


def render_status(paths: Paths | None = None) -> str:
    paths = paths or Paths.discover()
    rows = [("STATE", "SESSION", "ROOT", "NAMESPACE", "STEP", "ATTACH", "UPDATED")]
    active = {}
    if paths.registry is not None and paths.registry.exists():
        with Registry(paths.registry) as registry:
            for item in registry.list_active():
                active[item.session_id] = (
                    item.state,
                    len([value for value in registry.list_attachments(item.session_id)
                         if value.detached_utc is None]),
                )
    store = SessionStore(paths)
    for _, session in store.list_sessions():
        head = store.head(session.archive_namespace, session.session_id)
        state, attachments = active.get(session.session_id, (session.state, 0))
        rows.append((state, session.session_id, session.root, session.archive_namespace,
                     str(head.step) if head else "-", str(attachments),
                     head.created_utc if head else session.updated_utc))
    if len(rows) == 1:
        return "No sessions.\n"
    widths = [max(len(row[index]) for row in rows) for index in range(len(rows[0]))]
    return "\n".join("  ".join(value.ljust(widths[index]) for index, value in enumerate(row))
                     for row in rows) + "\n"
