"""Render human-readable summaries of local and archived Memo sessions."""

from __future__ import annotations

import argparse
import stat
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ...daemon.registry import Registry
from ...recording.metadata import DirectorySession
from ...recording.paths import StoragePaths
from ...recording.store import SessionStore
from .common import require_local_session

NAME = "status"


def _positive_int(value: str) -> int:
    number = int(value)
    if number < 1:
        raise argparse.ArgumentTypeError("must be positive")
    return number


def configure(subparsers: Any) -> None:
    command = subparsers.add_parser(NAME, help="list recordings")
    command.add_argument("session_id", nargs="?", help="show one recording")
    command.add_argument(
        "--include-archive", action="store_true", help="include remote-only archived recordings"
    )
    command.add_argument(
        "--limit", type=_positive_int, help="maximum number of recordings to display"
    )
    command.set_defaults(handler=run)


def run(args: Any) -> int:
    if args.session_id is not None:
        if args.include_archive or args.limit is not None:
            raise ValueError("single-session status cannot use --include-archive or --limit")
        require_local_session(args.session_id)
    print(
        render_status(
            include_archive=args.include_archive,
            limit=args.limit,
            session_id=args.session_id,
        ),
        end="",
    )
    return 0


def _parse_utc(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _age(value: str, now: datetime, *, ago: bool = False) -> str:
    timestamp = _parse_utc(value)
    if timestamp is None:
        return "—"
    seconds = max(0, int((now - timestamp).total_seconds()))
    if ago and seconds < 5:
        return "just now"
    if seconds < 60:
        result = f"{seconds}s"
    elif seconds < 60 * 60:
        result = f"{seconds // 60}m"
    elif seconds < 24 * 60 * 60:
        result = f"{seconds // (60 * 60)}h"
    elif seconds < 7 * 24 * 60 * 60:
        result = f"{seconds // (24 * 60 * 60)}d"
    elif seconds < 30 * 24 * 60 * 60:
        result = f"{seconds // (7 * 24 * 60 * 60)}w"
    elif seconds < 365 * 24 * 60 * 60:
        result = f"{seconds // (30 * 24 * 60 * 60)}mo"
    else:
        result = f"{seconds // (365 * 24 * 60 * 60)}y"
    return f"{result} ago" if ago else result


def _session_size(root: Path) -> int:
    total = 0
    for path in root.rglob("*"):
        try:
            metadata = path.lstat()
        except OSError:
            continue
        if stat.S_ISREG(metadata.st_mode):
            total += metadata.st_size
    return total


def _format_size(size: int) -> str:
    units = ("B", "KiB", "MiB", "GiB", "TiB", "PiB")
    value = float(max(0, size))
    unit = units[0]
    for unit in units:
        if value < 1024 or unit == units[-1]:
            break
        value /= 1024
    if unit == "B":
        return f"{int(value)} B"
    rounded = round(value, 1)
    return f"{int(rounded)} {unit}" if rounded.is_integer() else f"{rounded:.1f} {unit}"


def _local_row(
    store: SessionStore,
    session_path: Path,
    session: DirectorySession,
    active: dict[str, tuple[str, int]],
    now: datetime,
) -> tuple[str, ...]:
    head = store.head(session.session_id)
    state, attachments = active.get(session.session_id, (session.state, 0))
    steps = 0 if head is None else head.step + 1
    archived = (
        "—"
        if session.last_pushed_step is None
        else f"{min(session.last_pushed_step + 1, steps)}/{steps}"
    )
    return (
        session.session_id,
        session.root,
        state,
        session.capture_scope,
        _age(session.created_utc, now),
        "—" if head is None else _age(head.created_utc, now, ago=True),
        str(attachments),
        str(steps),
        _format_size(_session_size(session_path)),
        archived,
    )


def render_status(
    paths: StoragePaths | None = None,
    *,
    now: datetime | None = None,
    include_archive: bool = False,
    limit: int | None = None,
    session_id: str | None = None,
) -> str:
    if limit is not None and limit < 1:
        raise ValueError("limit must be positive")
    if session_id is not None and (include_archive or limit is not None):
        raise ValueError("single-session status cannot use --include-archive or --limit")
    paths = paths or StoragePaths.discover()
    now = now or datetime.now(UTC)
    if now.tzinfo is None:
        now = now.replace(tzinfo=UTC)
    now = now.astimezone(UTC)
    rows = [
        (
            "SESSION",
            "ROOT",
            "STATE",
            "SCOPE",
            "AGE",
            "LAST",
            "TERMINALS",
            "STEPS",
            "SIZE",
            "ARCHIVED",
        )
    ]
    active = {}
    if paths.registry.exists():
        with Registry(paths.registry) as registry:
            for item in registry.list_active():
                active[item.session_id] = (
                    item.state,
                    len(
                        [
                            value
                            for value in registry.list_attachments(item.session_id)
                            if value.detached_utc is None
                        ]
                    ),
                )
    store = SessionStore(paths)
    if session_id is not None:
        local_sessions = [store.find(session_id)]
        local_ids = {session_id}
    else:
        all_local_sessions = store.list_sessions()
        local_ids = {session.session_id for _, session in all_local_sessions}
        local_sessions = all_local_sessions[:limit]
    rows.extend(
        _local_row(store, session_path, session, active, now)
        for session_path, session in local_sessions
    )
    remaining = None if limit is None else limit - len(local_sessions)
    if include_archive and (remaining is None or remaining > 0):
        from ...transport import list_archived_session_ids

        archived_ids = [
            session_id for session_id in list_archived_session_ids() if session_id not in local_ids
        ]
        if remaining is not None:
            archived_ids = archived_ids[:remaining]
        rows.extend(
            (session_id, "—", "archived", "—", "—", "—", "—", "—", "—", "yes")
            for session_id in archived_ids
        )
    if len(rows) == 1:
        return "No sessions.\n"
    widths = [max(len(row[index]) for row in rows) for index in range(len(rows[0]))]
    return (
        "\n".join(
            "  ".join(
                value if index == len(row) - 1 else value.ljust(widths[index])
                for index, value in enumerate(row)
            )
            for row in rows
        )
        + "\n"
    )
