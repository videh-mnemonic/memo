from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .collector import _model_context
from .config import Paths, TransportConfig
from .harnesses import registered_harnesses
from .harnesses.harness import AgentHarness, SourceRecord, source_records
from .models import DirectorySession, SessionOrigin, StepManifest
from .session_store import SessionStore, atomic_write, validate_session_id
from .step import utcnow
from .tracewatch import files, snapshot_complete


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


def _digest(path: Path, limit: int | None = None) -> str:
    hashing = hashlib.sha256()
    remaining = limit
    with path.open("rb") as handle:
        while remaining is None or remaining > 0:
            size = 1024 * 1024 if remaining is None else min(1024 * 1024, remaining)
            chunk = handle.read(size)
            if not chunk:
                break
            hashing.update(chunk)
            if remaining is not None:
                remaining -= len(chunk)
    return hashing.hexdigest()


def _is_prefix(source: Path, size: int, digest: str | None) -> bool:
    return size >= 0 and source.stat().st_size >= size and (
        digest is None or _digest(source, size) == digest
    )


def _timestamps(records: tuple[SourceRecord, ...], source: Path) -> tuple[str, str]:
    values: list[datetime] = []
    for record in records:
        if not isinstance(record.value, dict):
            continue
        containers = [record.value]
        containers.extend(
            value for key in ("payload", "message", "meta", "session")
            if isinstance((value := record.value.get(key)), dict)
        )
        for value in containers:
            raw = value.get("timestamp") or value.get("created_at")
            if not isinstance(raw, str):
                continue
            try:
                parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            except ValueError:
                continue
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            values.append(parsed.astimezone(timezone.utc))
            break
    if not values:
        fallback = datetime.fromtimestamp(source.stat().st_mtime, timezone.utc)
        values = [fallback]
    render = lambda value: value.isoformat().replace("+00:00", "Z")
    return render(min(values)), render(max(values))


def _discover(summary: ImportSummary) -> dict[tuple[str, str], list[Candidate]]:
    result: dict[tuple[str, str], list[Candidate]] = {}
    discovered_paths: dict[Path, list[AgentHarness]] = {}
    for harness in registered_harnesses():
        for source in files(harness.trace_roots()):
            discovered_paths.setdefault(source.resolve(), []).append(harness)
    for source, harnesses in sorted(discovered_paths.items()):
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
    return result


def _local_runs(store: SessionStore) -> tuple[list[KnownRun], set[str]]:
    runs: list[KnownRun] = []
    session_ids: set[str] = set()
    for session_path, session in store.list_sessions():
        session_ids.add(session.session_id)
        for metadata_path in (session_path / "agents" / "runs").glob("*.json"):
            try:
                metadata = json.loads(metadata_path.read_text())
                harness = str(metadata["harness"])
                native_id = str(metadata["agent_session_id"])
                trace_file = metadata.get("trace_file")
                trace = session_path / "agents" / "traces" / str(trace_file)
                complete_size = int(metadata.get("trace_complete_size", trace.stat().st_size))
                digest = metadata.get("trace_digest")
                if digest is None and trace.is_file():
                    digest = _digest(trace, complete_size)
                runs.append(KnownRun(
                    session.session_id, session.capture_scope, harness, native_id,
                    complete_size, str(digest) if digest else None, True,
                ))
            except (KeyError, TypeError, ValueError, OSError, json.JSONDecodeError):
                continue
    return runs, session_ids


def _choose_candidate(candidates: list[Candidate], temporary: Path) -> tuple[Candidate, Path, int, str]:
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


def _metadata(candidate: Candidate, run_id: str, trace_name: str,
              boundary: int, digest: str) -> dict[str, Any]:
    started, ended = _timestamps(candidate.records, candidate.source)
    model, reasoning = _model_context(list(candidate.records))
    return {
        "run_id": run_id,
        "harness": candidate.harness.name,
        "model": model,
        "reasoning": reasoning,
        "command": None,
        "cwd": str(candidate.cwd),
        "started_utc": started,
        "ended_utc": ended,
        "exit_code": None,
        "agent_session_id": candidate.native_id,
        "trace_file": trace_name,
        "trace_complete_size": boundary,
        "trace_digest": digest,
        "imported_agent_only": True,
    }


def _publish_empty_step(store: SessionStore, session: DirectorySession) -> None:
    step = store.next_step(session.session_id)
    session_path = store.session_path(session.session_id)
    runs = sorted(path.stem for path in (session_path / "agents" / "runs").glob("*.json"))
    prepared = Path(tempfile.mkdtemp(prefix=f".{step}.", dir=session_path / "snapshots"))
    manifest = StepManifest(
        session.session_id, step, utcnow(), f"snapshots/{step}", agent_runs=runs,
    )
    store.publish(session, manifest, prepared)


def _create(store: SessionStore, candidate: Candidate, snapshot: Path,
            boundary: int, digest: str) -> str:
    destination = store.session_path(candidate.native_id)
    if destination.exists():
        raise ValueError("native session ID collides with an existing Memo session")
    started, ended = _timestamps(candidate.records, candidate.source)
    session = DirectorySession(
        candidate.native_id, str(candidate.cwd), started, ended, SessionOrigin.current(),
        state="active", capture_scope="agent-only",
    )
    assert store.paths.archive is not None
    staging_archive = Path(tempfile.mkdtemp(prefix=".import-", dir=store.paths.archive))
    staging_paths = Paths(
        store.paths.home, archive=staging_archive, runtime=store.paths.runtime,
        socket=store.paths.socket, registry=store.paths.registry, spool=store.paths.spool,
    )
    staging_store = SessionStore(staging_paths)
    session_path = staging_store.create(session)
    try:
        run_id = uuid.uuid4().hex
        trace_name = f"{run_id}.jsonl"
        shutil.copyfile(snapshot, session_path / "agents" / "traces" / trace_name)
        metadata = _metadata(candidate, run_id, trace_name, boundary, digest)
        atomic_write(
            session_path / "agents" / "runs" / f"{run_id}.json",
            (json.dumps(metadata, indent=2, sort_keys=True) + "\n").encode(),
        )
        _publish_empty_step(staging_store, session)
        os.replace(session_path, destination)
        return session.session_id
    except BaseException:
        raise
    finally:
        shutil.rmtree(staging_archive, ignore_errors=True)


def _refresh(store: SessionStore, session_id: str, candidate: Candidate,
             snapshot: Path, boundary: int, digest: str) -> str:
    session_path, session = store.find(session_id)
    if session.capture_scope != "agent-only":
        raise ValueError("only agent-only sessions can be refreshed")
    metadata_path = next(
        path for path in (session_path / "agents" / "runs").glob("*.json")
        if (value := json.loads(path.read_text())).get("harness") == candidate.harness.name
        and value.get("agent_session_id") == candidate.native_id
    )
    metadata = json.loads(metadata_path.read_text())
    trace = session_path / "agents" / "traces" / metadata["trace_file"]
    temporary = trace.with_name(f".{trace.name}.{uuid.uuid4().hex}")
    try:
        shutil.copyfile(snapshot, temporary)
        os.replace(temporary, trace)
    finally:
        temporary.unlink(missing_ok=True)
    metadata.update(_metadata(
        candidate, str(metadata["run_id"]), str(metadata["trace_file"]), boundary, digest,
    ))
    atomic_write(metadata_path, (json.dumps(metadata, indent=2, sort_keys=True) + "\n").encode())
    session.updated_utc = _timestamps(candidate.records, candidate.source)[1]
    store.update_session(session)
    _publish_empty_step(store, session)
    return session_id


def import_native_sessions(paths: Paths | None = None, *,
                           config: TransportConfig | None = None,
                           client: Any | None = None) -> ImportSummary:
    paths = paths or Paths.discover()
    store = SessionStore(paths)
    summary = ImportSummary()
    discovered = _discover(summary)
    known, session_ids = _local_runs(store)
    config = config if config is not None else TransportConfig.discover()
    if config is not None:
        from .transport import inspect_archived_agent_runs

        remote_runs, remote_ids = inspect_archived_agent_runs(
            SessionOrigin.current(), config, client=client,
        )
        known.extend(KnownRun(**value, local=False) for value in remote_runs)
        session_ids.update(remote_ids)

    assert paths.runtime is not None
    work = Path(tempfile.mkdtemp(prefix="memo-import-", dir=paths.runtime))
    try:
        for key, candidates in sorted(discovered.items()):
            label = f"{key[0]}:{key[1]}"
            try:
                candidate, snapshot, boundary, digest = _choose_candidate(candidates, work)
                matches = [run for run in known if (run.harness, run.native_id) == key]
                covering = [run for run in matches
                            if run.complete_size == boundary and run.digest == digest]
                if covering:
                    summary.skipped.append(label)
                    continue
                prefixes = [run for run in matches
                            if run.complete_size < boundary
                            and _is_prefix(snapshot, run.complete_size, run.digest)]
                divergent = [run for run in matches if run not in prefixes]
                if divergent:
                    raise ValueError("native log diverges from an archived trace")
                imported = [run for run in prefixes if run.capture_scope == "agent-only"]
                if imported:
                    current = max(imported, key=lambda run: run.complete_size)
                    if not current.local:
                        from .transport import pull_session

                        pull_session(current.session_id, paths, config, client=client)
                    summary.refreshed.append(
                        _refresh(store, current.session_id, candidate, snapshot, boundary, digest)
                    )
                else:
                    if candidate.native_id in session_ids:
                        raise ValueError("native session ID collides with an existing Memo session")
                    summary.imported.append(_create(store, candidate, snapshot, boundary, digest))
                    session_ids.add(candidate.native_id)
            except (OSError, ValueError, StopIteration, json.JSONDecodeError) as error:
                summary.failed.append((label, str(error)))
    finally:
        shutil.rmtree(work, ignore_errors=True)
    return summary
