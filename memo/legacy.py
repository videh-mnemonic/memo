"""Migrate recordings written by the pre-daemon Memo prototype."""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import tarfile
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .agents.run_metadata import AgentRunMetadata
from .recording.metadata import DirectorySession, SessionOrigin, StepManifest
from .recording.paths import StoragePaths
from .recording.snapshots import scan_tree, utcnow
from .recording.store import SessionStore, validate_session_id


@dataclass
class LegacyMigrationSummary:
    migrated: list[str] = field(default_factory=list)
    skipped: list[tuple[str, str]] = field(default_factory=list)
    failed: list[tuple[str, str]] = field(default_factory=list)


@dataclass(frozen=True)
class LegacySource:
    label: str
    path: Path
    archive: bool = False


def legacy_sources(paths: StoragePaths | None = None) -> list[LegacySource]:
    paths = paths or StoragePaths.discover()
    result: list[LegacySource] = []
    scratch = paths.home / "scratch"
    if scratch.is_dir():
        for directory in sorted(scratch.iterdir()):
            if (directory / "meta.json").is_file():
                result.append(LegacySource(f"scratch:{directory.name}", directory))
    if paths.archive.is_dir():
        for archive in sorted(paths.archive.glob("*/*.tar.gz")):
            result.append(LegacySource(f"archive:{archive.parent.name}/{archive.name}", archive, True))
    return result


def _run(args: list[str], cwd: Path) -> None:
    result = subprocess.run(args, cwd=cwd, text=True, capture_output=True)
    if result.returncode:
        raise RuntimeError(f"command failed ({' '.join(args)}): {result.stderr.strip()}")


def _safe_extract_tar(path: Path, target: Path) -> None:
    root = target.resolve()
    with tarfile.open(path, "r:*") as archive:
        members = []
        for member in archive.getmembers():
            name = Path(member.name)
            if name.is_absolute() or ".." in name.parts:
                raise RuntimeError(f"unsafe archive path: {member.name}")
            if member.issym() or member.islnk() or member.isdev():
                raise RuntimeError(f"unsafe archive entry type: {member.name}")
            destination = (root / name).resolve()
            try:
                destination.relative_to(root)
            except ValueError as error:
                raise RuntimeError(f"archive path escapes destination: {member.name}") from error
            members.append(member)
        archive.extractall(target, members=members, filter="data")


def _extract_source(source: LegacySource, destination: Path) -> Path:
    if source.archive:
        _safe_extract_tar(source.path, destination)
        return destination
    shutil.copytree(source.path, destination, dirs_exist_ok=True)
    return destination


def _apply_mailbox(path: Path, destination: Path) -> None:
    if path.is_file() and path.stat().st_size:
        _run(["git", "am", "--committer-date-is-author-date", str(path)], destination)


def _apply_diff(path: Path, destination: Path) -> None:
    if path.is_file() and path.stat().st_size:
        _run(["git", "apply", "--binary", str(path)], destination)


def _restore_final(legacy: Path, destination: Path) -> None:
    bundle = legacy / "git" / "initial.bundle"
    if not bundle.is_file():
        raise ValueError("legacy recording has no initial.bundle")
    _run(["git", "clone", str(bundle), str(destination)], legacy)
    cumulative = legacy / "git" / "session-commits.patch"
    if cumulative.is_file():
        _apply_mailbox(cumulative, destination)
    else:
        for patch in sorted((legacy / "legs").glob("*/commits.patch")):
            _apply_mailbox(patch, destination)
    _apply_diff(legacy / "git" / "final-uncommitted.patch", destination)
    untracked = legacy / "git" / "final-untracked.tar.gz"
    if untracked.is_file():
        _safe_extract_tar(untracked, destination)
    shutil.rmtree(destination / ".git", ignore_errors=True)


def _digest(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def _legacy_legs(meta: dict[str, Any]) -> list[dict[str, Any]]:
    legs = meta.get("legs", [])
    return [leg for leg in legs if isinstance(leg, dict)]


def _copy_agent_traces(
    legacy: Path, session_path: Path, meta: dict[str, Any]
) -> list[str]:
    run_ids = []
    tool = str(meta.get("tool") or "unknown")
    if tool not in {"claude", "codex"}:
        return run_ids
    session_id = str(meta["session_id"])
    cwd = str(meta.get("repo_root") or "/")
    for leg in _legacy_legs(meta):
        trace_file = leg.get("trace_file")
        if not isinstance(trace_file, str) or Path(trace_file).name != trace_file:
            continue
        source = legacy / "traces" / trace_file
        if not source.is_file():
            continue
        leg_id = str(leg.get("leg_id") or len(run_ids) + 1)
        run_id = f"legacy-{leg_id}"
        trace_name = f"{run_id}.jsonl"
        destination = session_path / "agents" / "traces" / trace_name
        shutil.copyfile(source, destination)
        metadata = AgentRunMetadata(
            run_id=run_id,
            harness=tool,
            model=None,
            reasoning=None,
            command=[tool, *leg.get("tool_args", [])]
            if isinstance(leg.get("tool_args"), list)
            and all(isinstance(value, str) for value in leg["tool_args"])
            else None,
            cwd=cwd,
            started_utc=leg.get("start_utc") if isinstance(leg.get("start_utc"), str) else None,
            ended_utc=leg.get("end_utc") if isinstance(leg.get("end_utc"), str) else None,
            exit_code=leg.get("exit_code") if isinstance(leg.get("exit_code"), int) else None,
            agent_session_id=session_id,
            trace_file=trace_name,
            trace_complete_size=destination.stat().st_size,
            trace_digest=_digest(destination),
        )
        metadata.write(session_path / "agents" / "runs" / f"{run_id}.json")
        run_ids.append(run_id)
    return run_ids


def _migrate_one(source: LegacySource, paths: StoragePaths, store: SessionStore) -> str:
    with tempfile.TemporaryDirectory(prefix="memo-legacy-", dir=paths.runtime) as work_name:
        work = Path(work_name)
        legacy = _extract_source(source, work / "legacy")
        meta_path = legacy / "meta.json"
        if not meta_path.is_file():
            raise ValueError("legacy metadata not found")
        meta = json.loads(meta_path.read_text())
        if not isinstance(meta, dict):
            raise ValueError("legacy metadata must be an object")
        session_id = validate_session_id(str(meta.get("session_id") or ""))
        if store.session_path(session_id).exists():
            raise FileExistsError("new-format session already exists")
        root = Path(str(meta.get("repo_root") or ""))
        if not root.is_absolute():
            raise ValueError("legacy recording root is not absolute")
        restored = work / "restored"
        _restore_final(legacy, restored)
        created = str(meta.get("first_seen_utc") or utcnow())
        updated = str(meta.get("last_activity_utc") or created)
        session = DirectorySession(
            session_id,
            str(root),
            created,
            updated,
            SessionOrigin.current(),
            state="complete",
            capture_scope="full" if meta.get("coverage") == "full" else "partial",
        )
        session_path = store.create(session)
        try:
            run_ids = _copy_agent_traces(legacy, session_path, meta)
            snapshot = work / "snapshot"
            entries = scan_tree(restored, snapshot, paths=paths)
            manifest = StepManifest(
                session_id,
                0,
                updated,
                "snapshots/0",
                entries,
                agent_runs=sorted(run_ids),
            )
            store.publish(session, manifest, snapshot)
        except BaseException:
            shutil.rmtree(session_path, ignore_errors=True)
            raise
        return session_id


def _preview_one(source: LegacySource, paths: StoragePaths, store: SessionStore) -> str:
    with tempfile.TemporaryDirectory(prefix="memo-legacy-", dir=paths.runtime) as work_name:
        legacy = _extract_source(source, Path(work_name) / "legacy")
        meta_path = legacy / "meta.json"
        if not meta_path.is_file():
            raise ValueError("legacy metadata not found")
        meta = json.loads(meta_path.read_text())
        if not isinstance(meta, dict):
            raise ValueError("legacy metadata must be an object")
        session_id = validate_session_id(str(meta.get("session_id") or ""))
        if store.session_path(session_id).exists():
            raise FileExistsError("new-format session already exists")
        root = Path(str(meta.get("repo_root") or ""))
        if not root.is_absolute():
            raise ValueError("legacy recording root is not absolute")
        if not (legacy / "git" / "initial.bundle").is_file():
            raise ValueError("legacy recording has no initial.bundle")
        return session_id


def migrate_legacy(
    paths: StoragePaths | None = None, *, dry_run: bool = False
) -> LegacyMigrationSummary:
    paths = paths or StoragePaths.discover()
    store = SessionStore(paths)
    summary = LegacyMigrationSummary()
    for source in legacy_sources(paths):
        try:
            if dry_run:
                summary.migrated.append(_preview_one(source, paths, store))
            else:
                summary.migrated.append(_migrate_one(source, paths, store))
        except FileExistsError as error:
            summary.skipped.append((source.label, str(error)))
        except Exception as error:
            summary.failed.append((source.label, str(error)))
    return summary
