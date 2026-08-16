from __future__ import annotations

import json
import hashlib
import uuid
from pathlib import Path
from typing import Any

from .harnesses import get_harness
from .harnesses.harness import SourceRecord, source_records
from .registry import CaptureWindow, Registry
from .session_store import SessionStore, atomic_write
from .tracewatch import TraceCheckpoint, changed, snapshot_complete


def _json_bytes(value: dict[str, object]) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()


def _model_context(records: list[SourceRecord]) -> tuple[object | None, object | None]:
    model = None
    reasoning = None
    for record in records:
        if not isinstance(record.value, dict):
            continue
        values = [record.value]
        values.extend(
            value for key in ("payload", "message", "meta", "session")
            if isinstance((value := record.value.get(key)), dict)
        )
        for value in values:
            model = value.get("model", model)
            reasoning = value.get("effort", value.get("reasoning", reasoning))
    return model, reasoning


class TraceCollector:
    def __init__(self, store: SessionStore, registry: Registry):
        self.store = store
        self.registry = registry

    def collect(self, session_id: str) -> list[str]:
        archived: list[str] = []
        for window in self.registry.windows(session_id):
            collected, pending = self._collect_window(window)
            archived.extend(collected)
            active = [launch for launch in self.registry.launches(
                session_id, window.harness, window.cwd
            ) if launch.ended_utc is None]
            if not active and not pending:
                self.registry.remove_window(window)
        return archived

    def _collect_window(self, window: CaptureWindow) -> tuple[list[str], bool]:
        harness = get_harness(window.harness)
        checkpoint = TraceCheckpoint.from_json(window.checkpoint)
        archived: list[str] = []
        for source in changed(harness.trace_roots(), checkpoint):
            resolved = str(source.resolve())
            session_path = self.store.session_path(window.session_id)
            temporary = session_path / "agents" / "traces" / f".collect-{uuid.uuid4().hex}.jsonl"
            try:
                try:
                    state, boundary = snapshot_complete(source, temporary)
                except OSError:
                    continue
                if boundary == 0:
                    checkpoint.files[resolved] = state
                    continue
                records = list(source_records(temporary))
                trace_cwd = harness.identify_cwd(records, source)
                if trace_cwd is None or str(trace_cwd) != window.cwd:
                    checkpoint.files[resolved] = type(state)(
                        state.device, state.inode, state.mtime_ns, state.size, state.size
                    )
                    continue
                native_id = harness.identify_session(records, source)
                run_id, metadata = self._run(window, native_id)
                model, reasoning = _model_context(records)
                if model is not None:
                    metadata["model"] = model
                if reasoning is not None:
                    metadata["reasoning"] = reasoning
                trace_name = f"{run_id}.jsonl"
                destination = session_path / "agents" / "traces" / trace_name
                temporary.replace(destination)
                metadata["trace_file"] = trace_name
                metadata["trace_complete_size"] = boundary
                hashing = hashlib.sha256()
                with destination.open("rb") as handle:
                    for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                        hashing.update(chunk)
                metadata["trace_digest"] = hashing.hexdigest()
                atomic_write(
                    session_path / "agents" / "runs" / f"{run_id}.json",
                    _json_bytes(metadata),
                )
                checkpoint.files[resolved] = state
                archived.append(run_id)
            finally:
                temporary.unlink(missing_ok=True)
        self.registry.update_window(window, checkpoint.to_json())
        pending = any(state.complete_size < state.size for state in checkpoint.files.values())
        return archived, pending

    def _run(self, window: CaptureWindow, native_id: str) -> tuple[str, dict[str, Any]]:
        session_path = self.store.session_path(window.session_id)
        for metadata_path in (session_path / "agents" / "runs").glob("*.json"):
            metadata = json.loads(metadata_path.read_text())
            if (metadata.get("harness"), metadata.get("agent_session_id")) == (
                window.harness, native_id
            ):
                run_id = str(metadata["run_id"])
                break
        else:
            run_id = uuid.uuid4().hex
            metadata = {
                "run_id": run_id,
                "harness": window.harness,
                "model": None,
                "reasoning": None,
                "command": None,
                "cwd": window.cwd,
                "started_utc": None,
                "ended_utc": None,
                "exit_code": None,
                "agent_session_id": native_id,
                "trace_file": None,
            }
        harness = get_harness(window.harness)
        candidates = self.registry.launches(window.session_id, window.harness, window.cwd)
        exact = [launch for launch in candidates
                 if harness.parse_resume(launch.command[1:]) == native_id]
        if exact:
            launches = exact
        elif len(candidates) == 1:
            launches = candidates
        else:
            launches = []
        if launches:
            starts = [launch.started_utc for launch in launches]
            if metadata.get("started_utc"):
                starts.append(str(metadata["started_utc"]))
            metadata["started_utc"] = min(starts)
            latest = max(launches, key=lambda launch: (launch.started_utc, launch.launch_id))
            metadata["command"] = latest.command
            metadata["cwd"] = latest.cwd
            active = any(launch.ended_utc is None for launch in launches)
            completed = [launch for launch in launches if launch.ended_utc is not None]
            metadata["ended_utc"] = None if active or not completed else max(
                str(launch.ended_utc) for launch in completed
            )
            metadata["exit_code"] = None if active or not completed else max(
                completed, key=lambda launch: str(launch.ended_utc)
            ).exit_code
        return run_id, metadata
