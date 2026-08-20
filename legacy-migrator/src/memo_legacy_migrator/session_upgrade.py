"""Detect and upgrade every historical directory-session representation."""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from memo.recording.filesystem import atomic_write
from memo.recording.git_snapshots import GitSnapshotStore
from memo.recording.metadata import (
    STEP_SCHEMA_VERSION,
    DirectorySession,
    SessionOrigin,
    SnapshotEntry,
    StepManifest,
    digest_entries,
    encode_entries,
    entries_directory,
    snapshot_exceptions,
)
from memo.recording.paths import StoragePaths
from memo.recording.store import SessionStore, validate_session_id
from memo.transport import remote_sessions


@dataclass(frozen=True)
class SourceStep:
    created_utc: str
    source_snapshot: Path | None
    source_commit: str | None
    entries: list[SnapshotEntry]
    stream_high_water: dict[str, int]
    agent_runs: list[str]


@dataclass(frozen=True)
class UpgradeResult:
    source_format: str
    session: DirectorySession
    tree_ids: list[str]
    entries: list[list[SnapshotEntry]]


class AlreadyLatest(ValueError):
    """Raised after a latest-format session has passed full validation."""


def _json_bytes(value: dict[str, object]) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()


def _read_object(path: Path, label: str) -> dict[str, Any]:
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be a JSON object")
    return value


def _entries(value: object) -> list[SnapshotEntry]:
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise ValueError("snapshot entries must be a list of objects")
    return [SnapshotEntry.from_dict(item) for item in value]


def _strings(value: object, label: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ValueError(f"{label} must be a list of strings")
    return list(value)


def _high_water(value: object) -> dict[str, int]:
    if value is None:
        return {}
    if not isinstance(value, dict) or not all(
        isinstance(key, str) and isinstance(sequence, int) and not isinstance(sequence, bool)
        for key, sequence in value.items()
    ):
        raise ValueError("stream high-water marks must be integer-valued")
    return dict(value)


def _session(
    raw: dict[str, Any], session_id: str, fallback_origin: SessionOrigin
) -> DirectorySession:
    if raw.get("format") != "memo-directory-session":
        raise ValueError("unsupported session format")
    if raw.get("session_id") != session_id:
        raise ValueError("session metadata does not match its remote identity")
    origin_value = raw.get("origin")
    if isinstance(origin_value, dict):
        origin = SessionOrigin(**origin_value)
    else:
        origin = fallback_origin
    session = DirectorySession(
        session_id=session_id,
        root=str(raw.get("root") or ""),
        created_utc=str(raw.get("created_utc") or ""),
        updated_utc=str(raw.get("updated_utc") or raw.get("created_utc") or ""),
        origin=origin,
        state=str(raw.get("state") or "complete"),
        capture_scope=str(raw.get("capture_scope") or "partial"),
    )
    session.validate()
    if session.state != "complete":
        raise ValueError("remote session is not complete")
    return session


def _checkpoint_steps(root: Path, session_id: str) -> list[SourceStep]:
    head = (root / "HEAD").read_text().strip()
    if not head or Path(head).name != head:
        raise ValueError("invalid checkpoint HEAD")
    manifests: list[tuple[int, dict[str, Any]]] = []
    for path in (root / "checkpoints").glob("*.json"):
        value = _read_object(path, "checkpoint manifest")
        if value.get("schema_version") != 1:
            raise ValueError("unsupported checkpoint schema version")
        generation = value.get("generation")
        if not isinstance(generation, int) or isinstance(generation, bool) or generation < 1:
            raise ValueError("invalid checkpoint generation")
        if value.get("checkpoint_id") != path.stem or value.get("session_id") != session_id:
            raise ValueError("checkpoint identity does not match its filename")
        if value.get("snapshot") != f"snapshots/{path.stem}":
            raise ValueError("checkpoint snapshot does not match its identity")
        manifests.append((generation, value))
    if not manifests or not any(value.get("checkpoint_id") == head for _, value in manifests):
        raise ValueError("checkpoint HEAD is missing")
    head_generation = next(
        generation for generation, value in manifests if value.get("checkpoint_id") == head
    )
    manifests = [item for item in manifests if item[0] <= head_generation]
    manifests.sort(key=lambda item: item[0])
    if len({generation for generation, _ in manifests}) != len(manifests):
        raise ValueError("duplicate checkpoint generation")
    result = []
    for _, value in manifests:
        snapshot = root / str(value.get("snapshot") or "")
        if not snapshot.is_dir():
            raise ValueError("checkpoint references a missing snapshot")
        result.append(
            SourceStep(
                str(value.get("created_utc") or ""),
                snapshot,
                None,
                _entries(value.get("entries")),
                _high_water(value.get("stream_high_water")),
                [],
            )
        )
    return result


def _numeric_steps(root: Path, session_id: str) -> tuple[list[SourceStep], list[int]]:
    head_value = (root / "HEAD").read_text().strip()
    if not head_value.isdigit():
        raise ValueError("invalid numeric HEAD step")
    head = int(head_value)
    numbered = sorted(
        (path for path in (root / "steps").glob("*.json") if path.stem.isdigit()),
        key=lambda path: int(path.stem),
    )
    numbered = [path for path in numbered if int(path.stem) <= head]
    if not numbered or int(numbered[-1].stem) != head:
        raise ValueError("step HEAD is missing")
    numbers = [int(path.stem) for path in numbered]
    if numbers != list(range(head + 1)) and len(numbers) != 1:
        raise ValueError("step history is neither complete nor a single-generation archive")
    result = []
    schemas = []
    for path in numbered:
        manifest = StepManifest.load(path)
        if manifest.session_id != session_id or manifest.step != int(path.stem):
            raise ValueError("step identity does not match its filename")
        result.append(
            SourceStep(
                manifest.created_utc,
                None if manifest.snapshot_commit else root / manifest.snapshot,
                manifest.snapshot_commit,
                manifest.entries,
                dict(manifest.stream_high_water),
                list(manifest.agent_runs),
            )
        )
        schemas.append(manifest.schema_version)
    return result, schemas


def _validate_entry_files(step: SourceStep, tree: Path) -> None:
    for entry in step.entries:
        candidate = Path(entry.path)
        if candidate.is_absolute() or ".." in candidate.parts:
            raise ValueError(f"unsafe snapshot entry: {entry.path}")
        if (entry.kind == "file" or entry.retained) and not (tree / candidate).is_file():
            raise ValueError(f"snapshot entry is missing: {entry.path}")


def _write_entries(root: Path, entries: list[SnapshotEntry]) -> str:
    digest = digest_entries(entries)
    target = entries_directory(root) / f"{digest}.json"
    if not target.is_file():
        atomic_write(target, encode_entries(entries))
    return digest


def _rebuild_history(
    root: Path, session_id: str, source_steps: list[SourceStep], *, add_boundary: bool
) -> tuple[list[str], list[list[SnapshotEntry]]]:
    old_repository = GitSnapshotStore(root / "snapshots.git")
    new_path = root / "snapshots.latest.git"
    if new_path.exists():
        raise FileExistsError(new_path)
    repository = GitSnapshotStore(new_path)
    trees: list[str] = []
    compact_entries: list[list[SnapshotEntry]] = []
    converted: list[StepManifest] = []
    parent: str | None = None
    try:
        for step_number, source in enumerate(source_steps):
            temporary: Path | None = None
            if source.source_commit:
                temporary = Path(tempfile.mkdtemp(prefix="upgrade-tree-", dir=root.parent))
                old_repository.restore(source.source_commit, temporary)
                tree = temporary
            else:
                tree = source.source_snapshot
            if tree is None or not tree.is_dir():
                raise ValueError("step references a missing filesystem snapshot")
            try:
                _validate_entry_files(source, tree)
                tree_id = repository.write_tree(tree)
            finally:
                if temporary is not None:
                    shutil.rmtree(temporary, ignore_errors=True)
            commit = repository.commit_tree(
                tree_id, parent, f"Memo filesystem snapshot {step_number}"
            )
            exceptions = snapshot_exceptions(source.entries)
            manifest = StepManifest(
                session_id,
                step_number,
                source.created_utc,
                f"snapshots/{step_number}",
                exceptions,
                source.stream_high_water,
                schema_version=STEP_SCHEMA_VERSION,
                agent_runs=source.agent_runs,
                snapshot_commit=commit,
                entries_digest=digest_entries(exceptions),
            )
            converted.append(manifest)
            trees.append(tree_id)
            compact_entries.append(exceptions)
            parent = commit
        if add_boundary:
            head = converted[-1]
            converted.append(
                replace(head, step=head.step + 1, snapshot=f"snapshots/{head.step + 1}")
            )
            trees.append(trees[-1])
            compact_entries.append(compact_entries[-1])

        shutil.rmtree(root / "steps", ignore_errors=True)
        shutil.rmtree(root / "entries", ignore_errors=True)
        (root / "steps").mkdir()
        for manifest, entries in zip(converted, compact_entries, strict=True):
            manifest.entries_digest = _write_entries(root, entries)
            atomic_write(
                root / "steps" / f"{manifest.step}.json",
                _json_bytes(manifest.to_stored_dict()),
            )
        atomic_write(root / "HEAD", f"{converted[-1].step}\n".encode())
        if (root / "snapshots.git").exists():
            shutil.rmtree(root / "snapshots.git")
        os.replace(new_path, root / "snapshots.git")
        return trees, compact_entries
    except BaseException:
        shutil.rmtree(new_path, ignore_errors=True)
        raise


def _store(root: Path) -> SessionStore:
    return SessionStore(
        StoragePaths(
            root.parent,
            archive=root.parent,
            runtime=root.parent / "runtime-upgrade",
            spool=root.parent / "spool-upgrade",
        )
    )


def upgrade_session(
    root: Path,
    session_id: str,
    fallback_origin: SessionOrigin,
    *,
    transport_is_current: bool,
    archive_had_bundle: bool,
) -> UpgradeResult:
    """Upgrade an extracted session and return its verified semantic state."""
    session_id = validate_session_id(session_id)
    raw_session = _read_object(root / "session.json", "session metadata")
    format_version = raw_session.get("format_version")
    if format_version not in {1, 2}:
        raise ValueError(f"unsupported directory session version: {format_version}")

    if archive_had_bundle:
        remote_sessions._restore_snapshot_bundle(root, session_id)

    if format_version == 1:
        source_steps = _checkpoint_steps(root, session_id)
        source_format = "directory-v1-checkpoints"
    else:
        source_steps, schemas = _numeric_steps(root, session_id)
        schema_label = "-".join(str(value) for value in sorted(set(schemas)))
        representation = (
            "bundle"
            if archive_had_bundle
            else ("repository" if (root / "snapshots.git").is_dir() else "directories")
        )
        source_format = f"directory-v2-steps-{schema_label}-{representation}"
        complete_session_fields = set(DirectorySession.__dataclass_fields__).issubset(raw_session)
        if (
            transport_is_current
            and archive_had_bundle
            and complete_session_fields
            and schemas
            and all(schema == STEP_SCHEMA_VERSION for schema in schemas)
        ):
            session = DirectorySession.from_dict(raw_session)
            manifests = _store(root).steps(session_id)
            if not manifests:
                raise ValueError("session has no published steps")
            raise AlreadyLatest("already uses the latest session and archive formats")

    session = _session(raw_session, session_id, fallback_origin)
    atomic_write(root / "session.json", _json_bytes(session.to_dict()))
    trees, entries = _rebuild_history(
        root,
        session_id,
        source_steps,
        add_boundary=transport_is_current,
    )
    manifests = _store(root).steps(session_id)
    if [manifest.entries for manifest in manifests] != entries:
        raise ValueError("upgraded entry metadata did not validate")
    return UpgradeResult(source_format, session, trees, entries)
