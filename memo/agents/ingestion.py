"""Ingest provider-native traces into active Memo recording windows."""

from __future__ import annotations

import hashlib
import uuid

from ..daemon.registry import CaptureWindow, Registry
from ..recording.store import SessionStore
from .harnesses import get_harness
from .harnesses.base import model_context, source_records
from .run_metadata import AgentRunMetadata
from .trace_files import TraceCheckpoint, changed, snapshot_complete


class TraceIngester:
    def __init__(self, store: SessionStore, registry: Registry):
        self.store = store
        self.registry = registry

    def ingest(self, session_id: str) -> list[str]:
        archived: list[str] = []
        for window in self.registry.windows(session_id):
            collected, pending = self._collect_window(window)
            archived.extend(collected)
            active = [
                launch
                for launch in self.registry.launches(session_id, window.harness, window.cwd)
                if launch.ended_utc is None
            ]
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
                model, reasoning = model_context(records)
                if model is not None:
                    metadata.model = model
                if reasoning is not None:
                    metadata.reasoning = reasoning
                trace_name = f"{run_id}.jsonl"
                destination = session_path / "agents" / "traces" / trace_name
                temporary.replace(destination)
                metadata.trace_file = trace_name
                metadata.trace_complete_size = boundary
                with destination.open("rb") as handle:
                    metadata.trace_digest = hashlib.file_digest(handle, "sha256").hexdigest()
                metadata.write(session_path / "agents" / "runs" / f"{run_id}.json")
                checkpoint.files[resolved] = state
                archived.append(run_id)
            finally:
                temporary.unlink(missing_ok=True)
        self.registry.update_window(window, checkpoint.to_json())
        pending = any(state.complete_size < state.size for state in checkpoint.files.values())
        return archived, pending

    def _run(self, window: CaptureWindow, native_id: str) -> tuple[str, AgentRunMetadata]:
        session_path = self.store.session_path(window.session_id)
        for metadata_path in (session_path / "agents" / "runs").glob("*.json"):
            metadata = AgentRunMetadata.load(metadata_path)
            if (metadata.harness, metadata.agent_session_id) == (window.harness, native_id):
                run_id = metadata.run_id
                break
        else:
            run_id = uuid.uuid4().hex
            metadata = AgentRunMetadata(
                run_id=run_id,
                harness=window.harness,
                model=None,
                reasoning=None,
                command=None,
                cwd=window.cwd,
                started_utc=None,
                ended_utc=None,
                exit_code=None,
                agent_session_id=native_id,
            )
        harness = get_harness(window.harness)
        candidates = self.registry.launches(window.session_id, window.harness, window.cwd)
        exact = [
            launch for launch in candidates if harness.parse_resume(launch.command[1:]) == native_id
        ]
        if exact:
            launches = exact
        elif len(candidates) == 1:
            launches = candidates
        else:
            launches = []
        if launches:
            starts = [launch.started_utc for launch in launches]
            if metadata.started_utc:
                starts.append(metadata.started_utc)
            metadata.started_utc = min(starts)
            latest = max(launches, key=lambda launch: (launch.started_utc, launch.launch_id))
            metadata.command = latest.command
            metadata.cwd = latest.cwd
            active = any(launch.ended_utc is None for launch in launches)
            completed = [launch for launch in launches if launch.ended_utc is not None]
            metadata.ended_utc = (
                None
                if active or not completed
                else max(str(launch.ended_utc) for launch in completed)
            )
            metadata.exit_code = (
                None
                if active or not completed
                else max(completed, key=lambda launch: str(launch.ended_utc)).exit_code
            )
        return run_id, metadata
