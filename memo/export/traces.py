"""Export terminal and agent traces from recorded sessions."""

from __future__ import annotations

import json
from pathlib import Path

from ..agents.harnesses import get_harness
from ..agents.harnesses.base import source_records, trace_events
from ..agents.run_metadata import AgentRunMetadata
from ..recording.paths import StoragePaths
from ..recording.store import SessionStore
from ..recording.streams import StreamEvent


def terminal_ids(session_id: str, paths: StoragePaths | None = None) -> list[str]:
    store = SessionStore(paths or StoragePaths.discover())
    return sorted(store.step(session_id, -1).stream_high_water)


def _decoded_event(event: StreamEvent) -> dict[str, object]:
    return {
        "terminal_id": event.terminal_id,
        "sequence": event.sequence,
        "direction": event.direction,
        "data": event.bytes().decode("utf-8", errors="replace"),
        "receipt_ns": event.receipt_ns,
    }


def trace_json(
    session_id: str,
    terminal_ids: list[str] | None = None,
    paths: StoragePaths | None = None,
    raw: bool = False,
) -> str:
    store = SessionStore(paths or StoragePaths.discover())
    store.find(session_id)
    manifest = store.step(session_id, -1)
    if manifest.agent_runs and terminal_ids is None:
        session_path = store.session_path(session_id)
        result = []
        for run_id in manifest.agent_runs:
            metadata = AgentRunMetadata.load(session_path / "agents" / "runs" / f"{run_id}.json")
            trace_path = session_path / "agents" / "traces" / metadata.trace_file
            if raw:
                result.extend(
                    record.value for record in source_records(trace_path) if record.error is None
                )
            else:
                result.extend(trace_events(get_harness(metadata.harness), trace_path, run_id))
        return json.dumps(result, indent=2, sort_keys=True) + "\n"
    events = store.stream_events_for_manifest(session_id, manifest, terminal_ids)
    return json.dumps([_decoded_event(event) for event in events], indent=2, sort_keys=True) + "\n"


def write_traces(
    session_id: str,
    destination: Path,
    terminal_ids: list[str] | None = None,
    paths: StoragePaths | None = None,
    raw: bool = False,
) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(trace_json(session_id, terminal_ids, paths, raw))
    return destination
