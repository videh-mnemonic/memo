from __future__ import annotations

import json
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from .config import Paths
from .agents.harnesses import get_harness
from .agents.harnesses.harness import source_records, trace_events
from .models import StepManifest
from .session_store import SessionStore
from .streams import StreamEvent


def _session(store: SessionStore, session_id: str):
    _, session = store.find(session_id)
    return session


def terminal_ids(session_id: str, paths: Paths | None = None) -> list[str]:
    store = SessionStore(paths or Paths.discover())
    return sorted(store.step(session_id, -1).stream_high_water)


def parse_step(value: str | int) -> int:
    try:
        step = int(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"invalid step selector: {value}") from error
    if step < -1:
        raise ValueError(f"invalid step selector: {value}")
    return step


def _decoded_event(event: StreamEvent) -> dict[str, object]:
    return {
        "terminal_id": event.terminal_id,
        "sequence": event.sequence,
        "direction": event.direction,
        "data": event.bytes().decode("utf-8", errors="replace"),
        "receipt_ns": event.receipt_ns,
    }


def trace_json(session_id: str, terminal_ids: list[str] | None = None,
               paths: Paths | None = None, raw: bool = False) -> str:
    store = SessionStore(paths or Paths.discover())
    session = _session(store, session_id)
    manifest = store.step(session_id, -1)
    if manifest.agent_runs and terminal_ids is None:
        session_path = store.session_path(session_id)
        result = []
        for run_id in manifest.agent_runs:
            metadata = json.loads(
                (session_path / "agents" / "runs" / f"{run_id}.json").read_text()
            )
            trace_file = metadata.get("trace_file")
            if not trace_file:
                continue
            trace_path = session_path / "agents" / "traces" / trace_file
            if raw:
                result.extend(record.value for record in source_records(trace_path)
                              if record.error is None)
            else:
                result.extend(trace_events(get_harness(metadata["harness"]), trace_path, run_id))
        return json.dumps(result, indent=2, sort_keys=True) + "\n"
    events = store.stream_events_for_manifest(session_id, manifest, terminal_ids)
    return json.dumps([_decoded_event(event) for event in events], indent=2, sort_keys=True) + "\n"


def write_traces(session_id: str, destination: Path, terminal_ids: list[str] | None = None,
                 paths: Paths | None = None, raw: bool = False) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(trace_json(session_id, terminal_ids, paths, raw))
    return destination


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
                   paths: Paths | None = None) -> Path:
    store = SessionStore(paths or Paths.discover())
    session = _session(store, session_id)
    if session.capture_scope == "agent-only":
        raise ValueError("filesystem replay is unavailable for an agent-only session")
    manifest = store.step(session_id, parse_step(at))
    store.restore_manifest(session_id, manifest, destination, force)
    if include_prompts:
        events = store.stream_events_for_manifest(session_id, manifest)
        (destination / ".prompts.md").write_text(render_prompts(events, manifest))
    return destination
