"""Discover native agent sessions and create or refresh standalone Memo recordings."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from ..recording.metadata import DirectorySession, SessionOrigin, StepManifest
from ..recording.paths import StoragePaths
from ..recording.snapshots import utcnow
from ..recording.store import SessionStore, validate_session_id
from ..transport.config import S3Config
from .harnesses import registered_harnesses
from .harnesses.base import (
    AgentHarness,
    SourceRecord,
    model_context,
    record_containers,
    source_records,
)
from .run_metadata import AgentRunMetadata
from .trace_files import files, snapshot_complete

ProgressCallback = Callable[[int, int, str], None]


@dataclass
class ImportSummary:
    imported: list[str] = field(default_factory=list)
    refreshed: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    failed: list[tuple[str, str]] = field(default_factory=list)


@dataclass(frozen=True)
class Candidate:
    harness: AgentHarness
    source: Path
    native_id: str
    cwd: Path
    records: tuple[SourceRecord, ...]


@dataclass(frozen=True)
class KnownRun:
    session_id: str
    capture_scope: str
    harness: str
    native_id: str
    complete_size: int
    digest: str | None
    local: bool
    state: str = "active"
    continued_from_session_id: str | None = None
    continued_from_trace_size: int | None = None
    continued_from_trace_digest: str | None = None
    archived: bool = True


def _digest(path: Path, limit: int | None = None) -> str:
    if limit is None:
        with path.open("rb") as handle:
            return hashlib.file_digest(handle, "sha256").hexdigest()
    hashing = hashlib.sha256()
    remaining = limit
    with path.open("rb") as handle:
        while remaining > 0:
            size = min(1024 * 1024, remaining)
            chunk = handle.read(size)
            if not chunk:
                break
            hashing.update(chunk)
            remaining -= len(chunk)
    return hashing.hexdigest()


def _is_prefix(source: Path, size: int, digest: str | None) -> bool:
    return (
        size >= 0
        and source.stat().st_size >= size
        and (digest is None or _digest(source, size) == digest)
    )


def _timestamps(records: tuple[SourceRecord, ...], source: Path) -> tuple[str, str]:
    values: list[datetime] = []
    for value in record_containers(records):
        raw = value.get("timestamp") or value.get("created_at")
        if not isinstance(raw, str):
            continue
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError:
            continue
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        values.append(parsed.astimezone(UTC))
    if not values:
        fallback = datetime.fromtimestamp(source.stat().st_mtime, UTC)
        values = [fallback]

    def render(value: datetime) -> str:
        return value.isoformat().replace("+00:00", "Z")

    return render(min(values)), render(max(values))


def _discover(
    summary: ImportSummary, progress: ProgressCallback | None = None
) -> dict[tuple[str, str], list[Candidate]]:
    result: dict[tuple[str, str], list[Candidate]] = {}
    discovered_paths: dict[Path, list[AgentHarness]] = {}
    if progress is not None:
        progress(0, 1, "scanning native trace files")
    for harness in registered_harnesses():
        for source in files(harness.trace_roots()):
            discovered_paths.setdefault(source.resolve(), []).append(harness)
    total = max(len(discovered_paths), 1)
    for index, (source, harnesses) in enumerate(sorted(discovered_paths.items()), start=1):
        if progress is not None:
            progress(index - 1, total, f"reading {source.name}")
        try:
            records = tuple(source_records(source))
            if len(harnesses) > 1:
                codex_native = any(
                    isinstance(record.value, dict)
                    and record.value.get("type") == "session_meta"
                    and isinstance(record.value.get("payload"), dict)
                    for record in records
                )
                provider = "codex" if codex_native else "claude"
                harnesses = [harness for harness in harnesses if harness.name == provider]
                if len(harnesses) != 1:
                    raise ValueError("native log provider is ambiguous")
            harness = harnesses[0]
            native_id = validate_session_id(harness.identify_session(records, source))
            cwd = harness.identify_cwd(records, source)
            if cwd is None:
                raise ValueError("working directory not found")
            candidate = Candidate(harness, source, native_id, cwd, records)
            result.setdefault((harness.name, native_id), []).append(candidate)
        except (OSError, ValueError, IndexError) as error:
            summary.failed.append((str(source), str(error)))
        if progress is not None:
            progress(index, total, f"inspected {source.name}")
    if progress is not None and not discovered_paths:
        progress(1, 1, "no native trace files found")
    return result


def _local_runs(store: SessionStore) -> tuple[list[KnownRun], set[str]]:
    runs: list[KnownRun] = []
    session_ids: set[str] = set()
    for session_path, session in store.list_sessions():
        session_ids.add(session.session_id)
        head = store.head(session.session_id)
        archived = (
            head is not None
            and session.last_pushed_step == head.step
            and session.last_pushed_digest is not None
            and session.remote_object is not None
        )
        for metadata_path in (session_path / "agents" / "runs").glob("*.json"):
            try:
                metadata = AgentRunMetadata.load(metadata_path)
                runs.append(
                    KnownRun(
                        session.session_id,
                        session.capture_scope,
                        metadata.harness,
                        metadata.agent_session_id,
                        metadata.trace_complete_size,
                        metadata.trace_digest,
                        True,
                        session.state,
                        metadata.continued_from_session_id,
                        metadata.continued_from_trace_size,
                        metadata.continued_from_trace_digest,
                        archived,
                    )
                )
            except (KeyError, TypeError, ValueError, OSError, json.JSONDecodeError):
                continue
    return runs, session_ids


def _choose_candidate(
    candidates: list[Candidate], temporary: Path
) -> tuple[Candidate, Path, int, str]:
    snapshots: list[tuple[Candidate, Path, int, str]] = []
    for index, candidate in enumerate(candidates):
        target = temporary / f"candidate-{index}.jsonl"
        _, boundary = snapshot_complete(candidate.source, target)
        if boundary == 0:
            raise ValueError("native log has no complete JSONL records")
        records = tuple(source_records(target))
        native_id = validate_session_id(
            candidate.harness.identify_session(records, candidate.source)
        )
        cwd = candidate.harness.identify_cwd(records, candidate.source)
        if native_id != candidate.native_id or cwd is None or cwd != candidate.cwd:
            raise ValueError("native log identity changed while it was being inspected")
        fixed = Candidate(candidate.harness, candidate.source, native_id, cwd, records)
        snapshots.append((fixed, target, boundary, _digest(target)))
    snapshots.sort(key=lambda item: item[2], reverse=True)
    chosen = snapshots[0]
    for _, path, size, digest in snapshots[1:]:
        if size > chosen[2] or not _is_prefix(chosen[1], size, digest):
            raise ValueError("divergent native logs claim the same session ID")
    return chosen


def _metadata(
    candidate: Candidate,
    run_id: str,
    trace_name: str,
    boundary: int,
    digest: str,
    continuation: KnownRun | None = None,
) -> AgentRunMetadata:
    started, ended = _timestamps(candidate.records, candidate.source)
    model, reasoning = model_context(candidate.records)
    return AgentRunMetadata(
        run_id=run_id,
        harness=candidate.harness.name,
        model=model,
        reasoning=reasoning,
        command=None,
        cwd=str(candidate.cwd),
        started_utc=started,
        ended_utc=ended,
        exit_code=None,
        agent_session_id=candidate.native_id,
        trace_file=trace_name,
        trace_complete_size=boundary,
        trace_digest=digest,
        imported_agent_only=True,
        continued_from_session_id=(continuation.session_id if continuation else None),
        continued_from_trace_size=(continuation.complete_size if continuation else None),
        continued_from_trace_digest=(continuation.digest if continuation else None),
    )


def _continuation_session_id(candidate: Candidate, boundary: int, digest: str, source: KnownRun) -> str:
    value = (
        "memo-agent-continuation:"
        f"{candidate.harness.name}:{candidate.native_id}:{boundary}:{digest}:"
        f"{source.session_id}:{source.complete_size}:{source.digest}"
    )
    return uuid.uuid5(uuid.NAMESPACE_URL, value).hex


def _publish_empty_step(store: SessionStore, session: DirectorySession) -> None:
    step = store.next_step(session.session_id)
    session_path = store.session_path(session.session_id)
    runs = sorted(path.stem for path in (session_path / "agents" / "runs").glob("*.json"))
    with tempfile.TemporaryDirectory(
        prefix=f".{step}.", dir=session_path / "snapshots"
    ) as temporary:
        manifest = StepManifest(
            session.session_id,
            step,
            utcnow(),
            f"snapshots/{step}",
            agent_runs=runs,
        )
        store.publish(session, manifest, Path(temporary))


def _create(
    store: SessionStore,
    candidate: Candidate,
    snapshot: Path,
    boundary: int,
    digest: str,
    session_id: str | None = None,
    continuation: KnownRun | None = None,
) -> str:
    session_id = session_id or candidate.native_id
    destination = store.session_path(session_id)
    if destination.exists():
        raise ValueError("native session ID collides with an existing Memo session")
    started, ended = _timestamps(candidate.records, candidate.source)
    session = DirectorySession(
        session_id,
        str(candidate.cwd),
        started,
        ended,
        SessionOrigin.current(),
        state="active",
        capture_scope="agent-only",
    )
    with tempfile.TemporaryDirectory(prefix=".import-", dir=store.paths.archive) as staging_name:
        staging_archive = Path(staging_name)
        staging_paths = StoragePaths(
            store.paths.home,
            archive=staging_archive,
            runtime=store.paths.runtime,
            socket=store.paths.socket,
            registry=store.paths.registry,
            spool=store.paths.spool,
        )
        staging_store = SessionStore(staging_paths)
        session_path = staging_store.create(session)
        run_id = uuid.uuid4().hex
        trace_name = f"{run_id}.jsonl"
        shutil.copyfile(snapshot, session_path / "agents" / "traces" / trace_name)
        metadata = _metadata(candidate, run_id, trace_name, boundary, digest, continuation)
        metadata.write(session_path / "agents" / "runs" / f"{run_id}.json")
        _publish_empty_step(staging_store, session)
        os.replace(session_path, destination)
        return session.session_id


def _refresh(
    store: SessionStore,
    session_id: str,
    candidate: Candidate,
    snapshot: Path,
    boundary: int,
    digest: str,
) -> str:
    session_path, session = store.find(session_id)
    if session.capture_scope != "agent-only":
        raise ValueError("only agent-only sessions can be refreshed")
    metadata_path = next(
        path
        for path in (session_path / "agents" / "runs").glob("*.json")
        if (value := AgentRunMetadata.load(path)).harness == candidate.harness.name
        and value.agent_session_id == candidate.native_id
    )
    metadata = AgentRunMetadata.load(metadata_path)
    trace = session_path / "agents" / "traces" / metadata.trace_file
    temporary = trace.with_name(f".{trace.name}.{uuid.uuid4().hex}")
    try:
        shutil.copyfile(snapshot, temporary)
        os.replace(temporary, trace)
    finally:
        temporary.unlink(missing_ok=True)
    refreshed = _metadata(
        candidate,
        metadata.run_id,
        metadata.trace_file,
        boundary,
        digest,
    )
    refreshed.continued_from_session_id = metadata.continued_from_session_id
    refreshed.continued_from_trace_size = metadata.continued_from_trace_size
    refreshed.continued_from_trace_digest = metadata.continued_from_trace_digest
    refreshed.write(metadata_path)
    session.updated_utc = _timestamps(candidate.records, candidate.source)[1]
    store.update_session(session)
    _publish_empty_step(store, session)
    return session_id


def import_native_sessions(
    paths: StoragePaths | None = None,
    *,
    config: S3Config | None = None,
    client: Any | None = None,
    dry_run: bool = False,
    progress: ProgressCallback | None = None,
) -> ImportSummary:
    paths = paths or StoragePaths.discover()
    store = SessionStore(paths)
    summary = ImportSummary()
    discovered = _discover(summary, progress)
    if progress is not None:
        progress(0, 1, "reading local Memo archive")
    known, session_ids = _local_runs(store)
    if progress is not None:
        progress(1, 1, "read local Memo archive")
    config = config if config is not None else S3Config.discover(required=True)
    assert config is not None
    from ..transport import inspect_archived_agent_runs

    if progress is not None:
        progress(0, 1, "inspecting remote archive")
    remote_runs, remote_ids = inspect_archived_agent_runs(
        SessionOrigin.current(),
        config,
        client=client,
    )
    if progress is not None:
        progress(1, 1, "inspected remote archive")
    known.extend(KnownRun(**value, local=False) for value in remote_runs)
    session_ids.update(remote_ids)

    with tempfile.TemporaryDirectory(prefix="memo-import-", dir=paths.runtime) as work_name:
        work = Path(work_name)
        items = sorted(discovered.items())
        total = max(len(items), 1)
        for index, (key, candidates) in enumerate(items, start=1):
            label = f"{key[0]}:{key[1]}"
            if progress is not None:
                progress(index - 1, total, f"processing {label}")
            try:
                candidate, snapshot, boundary, digest = _choose_candidate(candidates, work)
                matches = [run for run in known if (run.harness, run.native_id) == key]
                covering = [
                    run for run in matches if run.complete_size == boundary and run.digest == digest
                ]
                prefixes = [
                    run
                    for run in matches
                    if run.complete_size < boundary
                    and _is_prefix(snapshot, run.complete_size, run.digest)
                ]
                divergent = [run for run in matches if run not in prefixes and run not in covering]
                if divergent:
                    raise ValueError("native log diverges from an archived trace")
                if any(run.archived for run in covering):
                    summary.skipped.append(label)
                    continue
                completed = [
                    run
                    for run in prefixes
                    if run.state == "complete" and run.archived
                ]
                continued = [
                    run
                    for run in covering
                    for complete in completed
                    if run.continued_from_session_id == complete.session_id
                    and run.continued_from_trace_size == complete.complete_size
                    and run.continued_from_trace_digest == complete.digest
                ]
                if covering and (not completed or continued):
                    summary.skipped.append(label)
                    continue
                imported = [
                    run
                    for run in prefixes
                    if run.capture_scope == "agent-only" and run.state != "complete"
                ]
                if imported:
                    current = max(imported, key=lambda run: run.complete_size)
                    if dry_run:
                        summary.refreshed.append(current.session_id)
                        continue
                    if not current.local:
                        from ..transport import pull_session

                        pull_session(current.session_id, paths, config, client=client)
                    summary.refreshed.append(
                        _refresh(store, current.session_id, candidate, snapshot, boundary, digest)
                    )
                elif completed:
                    continuation = max(completed, key=lambda run: run.complete_size)
                    session_id = _continuation_session_id(candidate, boundary, digest, continuation)
                    if session_id in session_ids:
                        raise ValueError("native continuation ID collides with an existing Memo session")
                    if dry_run:
                        summary.imported.append(session_id)
                    else:
                        from ..transport import pull_session

                        summary.imported.append(
                            _create(
                                store,
                                candidate,
                                snapshot,
                                boundary,
                                digest,
                                session_id,
                                continuation,
                            )
                        )
                        session_ids.add(session_id)
                        if not continuation.local:
                            pull_session(
                                continuation.session_id,
                                paths,
                                config,
                                force=True,
                                client=client,
                            )
                else:
                    if candidate.native_id in session_ids:
                        raise ValueError("native session ID collides with an existing Memo session")
                    if dry_run:
                        summary.imported.append(candidate.native_id)
                    else:
                        summary.imported.append(
                            _create(store, candidate, snapshot, boundary, digest)
                        )
                        session_ids.add(candidate.native_id)
            except (OSError, ValueError, StopIteration, json.JSONDecodeError) as error:
                summary.failed.append((label, str(error)))
            if progress is not None:
                progress(index, total, f"processed {label}")
        if progress is not None and not items:
            progress(1, 1, "no native sessions to import")
    return summary
