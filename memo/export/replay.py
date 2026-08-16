"""Restore recorded snapshots and optionally render captured prompts."""

from __future__ import annotations

import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from ..recording.paths import StoragePaths
from ..recording.models import StepManifest
from ..recording.store import SessionStore
from ..recording.streams import StreamEvent


def parse_step(value: str | int) -> int:
    try:
        step = int(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"invalid step selector: {value}") from error
    if step < -1:
        raise ValueError(f"invalid step selector: {value}")
    return step


def _timestamp(receipt_ns: int) -> str:
    return datetime.fromtimestamp(receipt_ns / 1_000_000_000, timezone.utc).isoformat().replace(
        "+00:00", "Z"
    )


def _fence(text: str) -> str:
    runs = [len(match.group(0)) for match in re.finditer(r"`+", text)]
    return "`" * max(3, (max(runs) + 1) if runs else 3)


def render_prompts(events: list[StreamEvent], manifest: StepManifest) -> str:
    grouped: dict[str, list[StreamEvent]] = defaultdict(list)
    for event in events:
        if event.direction == "input":
            grouped[event.terminal_id].append(event)
    lines = [
        "# Recorded Terminal Inputs",
        "",
        ("These are recorded terminal input events bounded by "
         f"session step {manifest.step}; they are not inferred semantic prompts."),
        "",
    ]
    for terminal_id in sorted(grouped):
        lines.extend([f"## Terminal `{terminal_id}`", ""])
        for event in grouped[terminal_id]:
            text = event.bytes().decode("utf-8", errors="replace")
            fence = _fence(text)
            lines.extend([
                (f"Sequence {event.sequence} | Timestamp {_timestamp(event.receipt_ns)} | "
                 f"Receipt ns {event.receipt_ns}"),
                "",
                f"{fence}text",
                text,
                fence,
                "",
            ])
    return "\n".join(lines)


def replay_session(session_id: str, at: str | int, destination: Path,
                   include_prompts: bool = False, force: bool = False,
                   paths: StoragePaths | None = None) -> Path:
    store = SessionStore(paths or StoragePaths.discover())
    _, session = store.find(session_id)
    if session.capture_scope == "agent-only":
        raise ValueError("filesystem replay is unavailable for an agent-only session")
    manifest = store.step(session_id, parse_step(at))
    store.restore_manifest(session_id, manifest, destination, force)
    if include_prompts:
        events = store.stream_events_for_manifest(session_id, manifest)
        (destination / ".prompts.md").write_text(render_prompts(events, manifest))
    return destination
