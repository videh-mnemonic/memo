"""Render human-readable summaries of local and archived Memo sessions."""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ...agents.run_metadata import AgentRunMetadata
from ...daemon.registry import Attachment, Registry
from ...daemon.server import TERMINAL_STALE_SECONDS
from ...recording.metadata import DirectorySession
from ...recording.paths import StoragePaths
from ...recording.store import SessionStore
from ...transport import ArchivedSession, list_archived_sessions
from .common import format_size as _format_size
from .common import session_size as _session_size

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
        "--archive", action="store_true", help="list only recordings indexed in S3"
    )
    command.add_argument(
        "--limit", type=_positive_int, help="maximum number of recordings to display"
    )
    command.add_argument("--active", action="store_true", help="list only active recordings")
    command.add_argument(
        "--json", action="store_true", dest="json_output", help="emit machine-readable JSON"
    )
    command.set_defaults(handler=run)


def run(args: Any) -> int:
    if args.session_id is not None:
        if args.archive or args.limit is not None or args.active:
            raise ValueError("single-session status cannot use --archive, --limit, or --active")
    print(
        render_status(
            archive_only=args.archive,
            limit=args.limit,
            session_id=args.session_id,
            active_only=args.active,
            json_output=args.json_output,
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


def _local_record(
    store: SessionStore,
    session_path: Path,
    session: DirectorySession,
    active: dict[str, tuple[str, int]],
) -> dict[str, object]:
    head = store.head(session.session_id)
    state, attachments = active.get(session.session_id, (session.state, 0))
    steps = 0 if head is None else head.step + 1
    archived_steps = (
        None if session.last_pushed_step is None else min(session.last_pushed_step + 1, steps)
    )
    return {
        "session_id": session.session_id,
        "root": session.root,
        "state": state,
        "capture_scope": session.capture_scope,
        "created_utc": session.created_utc,
        "updated_utc": session.updated_utc,
        "last_published_utc": None if head is None else head.created_utc,
        "active_terminals": attachments,
        "steps": steps,
        "size_bytes": _session_size(session_path),
        "archived_steps": archived_steps,
    }


def _local_row(record: dict[str, object], now: datetime) -> tuple[str, ...]:
    steps = int(record["steps"])
    archived_steps = record["archived_steps"]
    return (
        str(record["session_id"]),
        str(record["root"]),
        str(record["state"]),
        str(record["capture_scope"]),
        _age(str(record["created_utc"]), now),
        (
            "—"
            if record["last_published_utc"] is None
            else _age(str(record["last_published_utc"]), now, ago=True)
        ),
        str(record["active_terminals"]),
        str(steps),
        _format_size(int(record["size_bytes"])),
        "—" if archived_steps is None else f"{archived_steps}/{steps}",
    )


def _archive_record(session: ArchivedSession) -> dict[str, object]:
    return {
        "session_id": session.session_id,
        "memo_version_id": session.memo_version_id,
        "username": session.username,
        "hostname": session.hostname,
        "complete": session.complete,
        "state": "complete" if session.complete else "active",
        "step": session.step,
        "steps": session.step + 1,
        "size_bytes": session.size_bytes,
        "object": session.object_key,
    }


def _archive_row(record: dict[str, object]) -> tuple[str, ...]:
    return (
        str(record["session_id"]),
        str(record["username"]),
        str(record["hostname"]),
        str(record["memo_version_id"]),
        str(record["state"]),
        str(record["steps"]),
        _format_size(int(record["size_bytes"])),
    )


def _render_table(rows: list[tuple[str, ...]]) -> str:
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


def _json(value: object) -> str:
    return json.dumps(value, indent=2, sort_keys=True) + "\n"


def _terminal_is_active(attachment: Attachment, cutoff_seen_ns: int) -> bool:
    if attachment.detached_utc is not None:
        return False
    return attachment.last_seen_ns == 0 or attachment.last_seen_ns >= cutoff_seen_ns


def _archived_progress(session: DirectorySession, steps: int) -> str:
    if session.last_pushed_step is None:
        return "—"
    return f"{min(session.last_pushed_step + 1, steps)}/{steps}"


def _render_session_detail(
    store: SessionStore,
    session_path: Path,
    session: DirectorySession,
    active: dict[str, tuple[str, int]],
    now: datetime,
    paths: StoragePaths,
) -> str:
    head = store.head(session.session_id)
    steps = 0 if head is None else head.step + 1
    state, active_terminals = active.get(session.session_id, (session.state, 0))
    lines = [
        f"Session: {session.session_id}",
        f"Root: {session.root}",
        f"State: {state}",
        f"Scope: {session.capture_scope}",
        f"Created: {session.created_utc} ({_age(session.created_utc, now)} old)",
        f"Updated: {session.updated_utc}",
        f"Last published: {'—' if head is None else f'step {head.step} ({_age(head.created_utc, now, ago=True)})'}",
        f"Steps: {steps}",
        f"Size: {_format_size(_session_size(session_path))}",
        f"Archived: {_archived_progress(session, steps)}",
        f"Active terminals: {active_terminals}",
    ]
    if state == "active":
        lines.append("Lifecycle: active; exiting a shell detaches it, and memo end completes it.")
    elif state == "complete":
        lines.append("Lifecycle: complete; no further steps will publish unless this is replaced.")
    else:
        lines.append(f"Lifecycle: {state}")

    if paths.registry.exists():
        with Registry(paths.registry) as registry:
            attachments = registry.list_attachments(session.session_id)
            launches = registry.launches(session.session_id)
            sandbox_shells = registry.sandbox_shell_launches(session.session_id)
            windows = registry.windows(session.session_id)
    else:
        attachments = []
        launches = []
        sandbox_shells = []
        windows = []
    lines.append("")
    lines.append("Terminals:")
    if attachments:
        for item in attachments:
            cutoff = time.time_ns() - int(TERMINAL_STALE_SECONDS * 1_000_000_000)
            status = (
                "detached"
                if item.detached_utc
                else "stale"
                if not _terminal_is_active(item, cutoff)
                else "attached"
            )
            lines.append(
                f"  {item.terminal_id}: {status}, accepted={item.accepted_sequence}, "
                f"attached={item.attached_utc}, detached={item.detached_utc or '—'}"
            )
    else:
        lines.append("  (none)")

    lines.append("")
    lines.append("Agent capture:")
    if launches:
        for launch in launches:
            status = "running" if launch.ended_utc is None else f"exit={launch.exit_code}"
            mode = launch.sandbox_mode or "legacy"
            lines.append(
                f"  launch {launch.launch_id}: {launch.harness}, {mode}, {status}, cwd={launch.cwd}"
            )
    elif windows:
        for window in windows:
            lines.append(f"  watching {window.harness}: cwd={window.cwd}")
    else:
        lines.append("  (none active)")

    lines.append("")
    lines.append("Sandbox shells:")
    if sandbox_shells:
        for launch in sandbox_shells:
            status = "running" if launch.ended_utc is None else f"exit={launch.exit_code}"
            lines.append(
                f"  {launch.launch_id}: {status}, cwd={launch.cwd}, "
                f"policy={launch.policy_digest[:12]}"
            )
    else:
        lines.append("  (none)")

    lines.append("")
    lines.append("Recorded agent runs:")
    run_paths = sorted((session_path / "agents" / "runs").glob("*.json"))
    if run_paths:
        for path in run_paths:
            metadata = AgentRunMetadata.load(path)
            lines.append(
                f"  {metadata.run_id}: {metadata.harness}, native={metadata.agent_session_id}, "
                f"trace={metadata.trace_file}, ended={metadata.ended_utc or '—'}"
            )
    else:
        lines.append("  (none)")
    return "\n".join(lines) + "\n"


def _session_detail_record(
    store: SessionStore,
    session_path: Path,
    session: DirectorySession,
    active: dict[str, tuple[str, int]],
    paths: StoragePaths,
) -> dict[str, object]:
    result = _local_record(store, session_path, session, active)
    if paths.registry.exists():
        with Registry(paths.registry) as registry:
            attachments = registry.list_attachments(session.session_id)
            launches = registry.launches(session.session_id)
            sandbox_shells = registry.sandbox_shell_launches(session.session_id)
            windows = registry.windows(session.session_id)
    else:
        attachments = []
        launches = []
        sandbox_shells = []
        windows = []
    cutoff = time.time_ns() - int(TERMINAL_STALE_SECONDS * 1_000_000_000)
    terminals = []
    for attachment in attachments:
        value = asdict(attachment)
        value["status"] = (
            "detached"
            if attachment.detached_utc
            else "stale"
            if not _terminal_is_active(attachment, cutoff)
            else "attached"
        )
        terminals.append(value)
    result.update(
        {
            "terminals": terminals,
            "agent_launches": [asdict(launch) for launch in launches],
            "capture_windows": [asdict(window) for window in windows],
            "sandbox_shells": [asdict(launch) for launch in sandbox_shells],
            "recorded_agent_runs": [
                AgentRunMetadata.load(path).to_dict()
                for path in sorted((session_path / "agents" / "runs").glob("*.json"))
            ],
        }
    )
    return result


def render_status(
    paths: StoragePaths | None = None,
    *,
    now: datetime | None = None,
    archive_only: bool = False,
    limit: int | None = None,
    session_id: str | None = None,
    active_only: bool = False,
    json_output: bool = False,
) -> str:
    if limit is not None and limit < 1:
        raise ValueError("limit must be positive")
    if session_id is not None and (archive_only or limit is not None or active_only):
        raise ValueError("single-session status cannot use --archive, --limit, or --active")
    if active_only and archive_only:
        raise ValueError("--active cannot be combined with --archive")
    if archive_only:
        records = [_archive_record(session) for session in list_archived_sessions(limit=limit)]
        if json_output:
            return _json(records)
        if not records:
            return "No archived sessions.\n"
        rows = [("SESSION", "USER", "HOST", "VERSION", "STATE", "STEPS", "SIZE")]
        rows.extend(_archive_row(record) for record in records)
        return _render_table(rows)

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
    cutoff_seen_ns = time.time_ns() - int(TERMINAL_STALE_SECONDS * 1_000_000_000)
    if paths.registry.exists():
        with Registry(paths.registry) as registry:
            for item in registry.list_active():
                active[item.session_id] = (
                    item.state,
                    len(
                        [
                            value
                            for value in registry.list_attachments(item.session_id)
                            if _terminal_is_active(value, cutoff_seen_ns)
                        ]
                    ),
                )
    store = SessionStore(paths)
    if session_id is not None:
        session_path, session = store.find(session_id)
        if json_output:
            return _json(_session_detail_record(store, session_path, session, active, paths))
        return _render_session_detail(store, session_path, session, active, now, paths)
    all_local_sessions = store.list_sessions()
    if active_only:
        active_ids = set(active)
        all_local_sessions = [
            item for item in all_local_sessions if item[1].session_id in active_ids
        ]
    local_sessions = all_local_sessions[:limit]
    records = [
        _local_record(store, session_path, session, active)
        for session_path, session in local_sessions
    ]
    if json_output:
        return _json(records)
    rows.extend(_local_row(record, now) for record in records)
    if len(rows) == 1:
        return "No sessions.\n"
    return _render_table(rows)
