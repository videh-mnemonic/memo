from __future__ import annotations

from datetime import datetime, timezone

from .config import Paths
from .store import list_scratch, lock_is_held


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
    rows = [("SESSION", "REPO", "NAMESPACE", "TOOL", "LEGS", "IDLE", "LOCKED", "COVERAGE")]
    for directory, meta in list_scratch(paths):
        namespace = meta.archive_namespace if len(meta.archive_namespace) <= 28 else meta.archive_namespace[:25] + "..."
        rows.append((meta.session_id, meta.repo_name, namespace, meta.tool, str(len(meta.legs)),
                     _idle(meta.last_activity_utc), "yes" if lock_is_held(directory / "session.lock") else "no",
                     meta.coverage))
    if len(rows) == 1:
        return "No scratch sessions.\n"
    widths = [max(len(row[i]) for row in rows) for i in range(len(rows[0]))]
    return "\n".join("  ".join(value.ljust(widths[i]) for i, value in enumerate(row)) for row in rows) + "\n"

