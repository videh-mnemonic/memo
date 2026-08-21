"""Detect and upgrade every historical directory-session representation."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from collections.abc import Callable
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
    read_entries,
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
    snapshots_by_tree: dict[str, dict[str, tuple[int, str]]]


@dataclass(frozen=True)
class NumericHistory:
    steps: list[SourceStep]
    schemas: list[int]
    head: int


ProgressCallback = Callable[[int, int, str], None]
GitRecoveryCallback = Callable[[Path, list[SourceStep], set[str], ProgressCallback | None], bool]


def _progress_range(
    progress: ProgressCallback | None, start: int, end: int
) -> ProgressCallback | None:
    if progress is None:
        return None

    def report(completed: int, total: int, message: str) -> None:
        fraction = max(0.0, min(completed / max(total, 1), 1.0))
        progress(round(start + ((end - start) * fraction)), 100, message)

    return report


def _report(progress: ProgressCallback | None, completed: int, total: int, message: str) -> None:
    if progress is not None:
        progress(completed, total, message)


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
    raw: dict[str, Any],
    session_id: str,
    fallback_origin: SessionOrigin,
    *,
    remote_complete: bool,
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
        if not remote_complete:
            raise ValueError("remote session is not complete")
        session.state = "complete"
        session.validate()
    return session


def _checkpoint_steps(root: Path, session_id: str) -> tuple[list[SourceStep], int]:
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
    return result, head_generation


def _numeric_steps(
    root: Path,
    session_id: str,
    progress: ProgressCallback | None = None,
) -> tuple[list[SourceStep], list[int], int]:
    head_value = (root / "HEAD").read_text().strip()
    if not head_value.isdigit():
        raise ValueError("invalid numeric HEAD step")
    head = int(head_value)
    numbered = sorted(
        (path for path in (root / "steps").glob("*.json") if path.stem.isdigit()),
        key=lambda path: int(path.stem),
    )
    collector = NumericHistoryCollector(root, session_id)
    for path in numbered:
        collector.add(int(path.stem), path.read_bytes())
    history = collector.finish(head, progress=progress)
    return history.steps, history.schemas, history.head


class NumericHistoryCollector:
    """Parse numeric step manifests without requiring them to remain on disk."""

    def __init__(self, root: Path, session_id: str) -> None:
        self.root = root
        self.session_id = session_id
        self._steps: dict[int, SourceStep] = {}
        self._schemas: dict[int, int] = {}
        self._pending: dict[int, StepManifest] = {}
        self._previous_raw_entries: object = None
        self._previous_entries: list[SnapshotEntry] | None = None
        self._shared_entries: dict[str, list[SnapshotEntry]] = {}
        self._inline_entries: dict[tuple[SnapshotEntry, ...], list[SnapshotEntry]] = {}

    @property
    def empty(self) -> bool:
        return not self._steps and not self._pending

    def add(self, number: int, data: bytes) -> int:
        if number in self._steps or number in self._pending:
            raise ValueError(f"duplicate numeric step: {number}")
        value = json.loads(data)
        if not isinstance(value, dict):
            raise ValueError("step manifest must be a JSON object")
        raw_entries = value.get("entries", [])
        if (
            raw_entries
            and raw_entries == self._previous_raw_entries
            and self._previous_entries is not None
        ):
            value["entries"] = []
            manifest = StepManifest.from_dict(value)
            manifest.entries = self._previous_entries
        else:
            manifest = StepManifest.from_dict(value)
            if manifest.entries_digest and not manifest.entries:
                entries_path = entries_directory(self.root) / f"{manifest.entries_digest}.json"
                if not entries_path.is_file():
                    if manifest.session_id != self.session_id or manifest.step != number:
                        raise ValueError("step identity does not match its filename")
                    self._pending[number] = manifest
                    self._schemas[number] = manifest.schema_version
                    return manifest.schema_version
                cached = self._shared_entries.get(manifest.entries_digest)
                if cached is None:
                    cached = list(read_entries(str(entries_path)))
                    if digest_entries(cached) != manifest.entries_digest:
                        raise ValueError("shared entry list digest does not match its filename")
                    self._shared_entries[manifest.entries_digest] = cached
                manifest.entries = cached
                manifest.validate()
            elif manifest.entries:
                key = tuple(manifest.entries)
                manifest.entries = self._inline_entries.setdefault(key, manifest.entries)
            self._previous_raw_entries = raw_entries
            self._previous_entries = manifest.entries
        if manifest.session_id != self.session_id or manifest.step != number:
            raise ValueError("step identity does not match its filename")
        self._steps[number] = SourceStep(
            manifest.created_utc,
            None if manifest.snapshot_commit else self.root / manifest.snapshot,
            manifest.snapshot_commit,
            manifest.entries,
            dict(manifest.stream_high_water),
            list(manifest.agent_runs),
        )
        self._schemas[number] = manifest.schema_version
        return manifest.schema_version

    def finish(self, head: int, progress: ProgressCallback | None = None) -> NumericHistory:
        for number, manifest in self._pending.items():
            if manifest.session_id != self.session_id or manifest.step != number:
                raise ValueError("step identity does not match its filename")
            digest = manifest.entries_digest
            if digest is None:
                raise ValueError("pending step has no shared entry digest")
            entries_path = entries_directory(self.root) / f"{digest}.json"
            if not entries_path.is_file():
                raise ValueError("step references a missing shared entry list")
            cached = self._shared_entries.get(digest)
            if cached is None:
                cached = list(read_entries(str(entries_path)))
                if digest_entries(cached) != digest:
                    raise ValueError("shared entry list digest does not match its filename")
                self._shared_entries[digest] = cached
            manifest.entries = cached
            manifest.validate()
            self._steps[number] = SourceStep(
                manifest.created_utc,
                None if manifest.snapshot_commit else self.root / manifest.snapshot,
                manifest.snapshot_commit,
                manifest.entries,
                dict(manifest.stream_high_water),
                list(manifest.agent_runs),
            )
        self._pending.clear()
        all_numbers = sorted(self._steps)
        if any(number > head for number in all_numbers):
            raise ValueError("step history contains data beyond HEAD")
        numbers = [number for number in all_numbers if number <= head]
        if not numbers or numbers[-1] != head:
            raise ValueError("step HEAD is missing")
        if numbers != list(range(head + 1)) and len(numbers) != 1:
            raise ValueError("step history is neither complete nor a single-generation archive")
        result = []
        schemas = []
        total = len(numbers)
        for index, number in enumerate(numbers, start=1):
            result.append(self._steps[number])
            schemas.append(self._schemas[number])
            if index == total or index % 256 == 0:
                _report(progress, index, total, f"parsed manifest {index}/{total}")
        return NumericHistory(result, schemas, head)


def _validate_current_history(root: Path, source_steps: list[SourceStep]) -> None:
    repository = GitSnapshotStore(root / "snapshots.git")
    commits = [step.source_commit or "" for step in source_steps]
    if any(not commit for commit in commits):
        raise ValueError("current history contains a step without a snapshot commit")
    if repository.contains_many(commits) != set(commits):
        raise ValueError("current history references a missing snapshot commit")
    final_commit = commits[-1]
    if not set(commits).issubset(repository.reachable_from(final_commit)):
        raise ValueError("current history contains commits outside its published history")
    repository.check_connectivity(final_commit)
    high_water: dict[str, int] = {}
    run_ids: dict[str, None] = {}
    for source in source_steps:
        for terminal_id, sequence in source.stream_high_water.items():
            high_water[terminal_id] = max(sequence, high_water.get(terminal_id, 0))
        for run_id in source.agent_runs:
            run_ids.setdefault(run_id, None)
    SessionStore._validate_streams(root, high_water, chunks=True)
    SessionStore._validate_agent_runs(root, run_ids)
    repository.pin(final_commit)


def _restore_streamed_bundle(root: Path, session_id: str, history: NumericHistory) -> None:
    bundle = root / "snapshots.bundle"
    if not bundle.is_file():
        raise ValueError("snapshot bundle is missing")
    if (root / "snapshots.git").exists():
        raise ValueError("archive contains both snapshot bundle and repository")
    commit = history.steps[-1].source_commit
    if not commit:
        raise ValueError("snapshot bundle archive has invalid HEAD manifest")
    GitSnapshotStore(root / "snapshots.git").import_bundle(bundle, commit)
    bundle.unlink()


def _validate_entry_files(step: SourceStep, tree: Path) -> None:
    for entry in step.entries:
        candidate = Path(entry.path)
        if candidate.is_absolute() or ".." in candidate.parts:
            raise ValueError(f"unsafe snapshot entry: {entry.path}")
        if (entry.kind == "file" or entry.retained) and not (tree / candidate).is_file():
            raise ValueError(f"snapshot entry is missing: {entry.path}")


def _validate_entry_fingerprint(
    step: SourceStep,
    fingerprint: dict[str, tuple[int, str]],
) -> None:
    for entry in step.entries:
        candidate = Path(entry.path)
        if candidate.is_absolute() or ".." in candidate.parts:
            raise ValueError(f"unsafe snapshot entry: {entry.path}")
        if (entry.kind == "file" or entry.retained) and candidate.as_posix() not in fingerprint:
            raise ValueError(f"snapshot entry is missing: {entry.path}")


def snapshot_fingerprint(tree: Path) -> dict[str, tuple[int, str]]:
    """Fingerprint every source file independently of Git's object creation."""
    result = {}
    for path in sorted(tree.rglob("*")):
        if not path.is_file():
            continue
        with path.open("rb") as handle:
            digest = hashlib.file_digest(handle, "sha256").hexdigest()
        result[path.relative_to(tree).as_posix()] = (path.stat().st_mode & 0o111, digest)
    return result


def _write_entries(root: Path, entries: list[SnapshotEntry]) -> str:
    digest = digest_entries(entries)
    target = entries_directory(root) / f"{digest}.json"
    if not target.is_file():
        atomic_write(target, encode_entries(entries))
    return digest


def _manifest(
    session_id: str,
    step_number: int,
    source: SourceStep,
    commit: str,
    entries: list[SnapshotEntry],
    entries_digest: str | None = None,
) -> StepManifest:
    return StepManifest(
        session_id,
        step_number,
        source.created_utc,
        f"snapshots/{step_number}",
        entries,
        source.stream_high_water,
        schema_version=STEP_SCHEMA_VERSION,
        agent_runs=source.agent_runs,
        snapshot_commit=commit,
        entries_digest=entries_digest or digest_entries(entries),
    )


def _install_history(
    root: Path,
    converted: list[StepManifest],
    compact_entries: list[list[SnapshotEntry]],
    progress: ProgressCallback | None = None,
) -> None:
    shutil.rmtree(root / "steps", ignore_errors=True)
    shutil.rmtree(root / "entries", ignore_errors=True)
    (root / "steps").mkdir()
    total = len(converted)
    entry_digests: dict[int, str] = {}
    for index, (manifest, entries) in enumerate(
        zip(converted, compact_entries, strict=True), start=1
    ):
        cache_key = id(entries)
        digest = entry_digests.get(cache_key)
        if digest is None:
            digest = _write_entries(root, entries)
            entry_digests[cache_key] = digest
        manifest.entries_digest = digest
        # This entire extracted session is disposable scratch state. Per-file
        # fsyncs make large histories pathologically slow and add no safety: an
        # interrupted conversion discards the scratch directory, while a
        # completed archive is extracted and independently verified below.
        (root / "steps" / f"{manifest.step}.json").write_bytes(
            _json_bytes(manifest.to_stored_dict())
        )
        if index == total or index % 256 == 0:
            _report(progress, index, total, f"wrote compact manifest {index}/{total}")
    atomic_write(root / "HEAD", f"{converted[-1].step}\n".encode())


def _append_boundary(
    converted: list[StepManifest],
    trees: list[str],
    compact_entries: list[list[SnapshotEntry]],
) -> None:
    head = converted[-1]
    converted.append(replace(head, step=head.step + 1, snapshot=f"snapshots/{head.step + 1}"))
    trees.append(trees[-1])
    compact_entries.append(compact_entries[-1])


def _preserve_git_history(
    root: Path,
    session_id: str,
    source_steps: list[SourceStep],
    *,
    add_boundary: bool,
    progress: ProgressCallback | None = None,
) -> tuple[
    list[str],
    list[list[SnapshotEntry]],
    dict[str, dict[str, tuple[int, str]]],
]:
    """Upgrade manifests while retaining an already content-addressed Git history."""
    repository = GitSnapshotStore(root / "snapshots.git")
    commits = [source.source_commit or "" for source in source_steps]
    if not commits or any(not commit for commit in commits):
        raise ValueError("Git-backed history contains a step without a snapshot commit")
    _report(progress, 0, 100, "validating existing Git history")
    present = repository.contains_many(commits)
    missing = [commit for commit in dict.fromkeys(commits) if commit not in present]
    if missing:
        raise ValueError(f"source Git history is missing {len(missing)} snapshot commit(s)")
    trees_by_commit = repository.tree_ids(commits)
    if len(trees_by_commit) != len(set(commits)):
        raise ValueError("source Git history contains a commit without a valid tree")
    final_commit = commits[-1]
    reachable = repository.reachable_from(final_commit)
    if not set(commits).issubset(reachable):
        raise ValueError("source Git history contains commits absent from its final bundle")
    repository.check_connectivity(final_commit)

    trees = [trees_by_commit[commit] for commit in commits]
    representatives: dict[str, tuple[str, SourceStep]] = {}
    for tree_id, commit, source in zip(trees, commits, source_steps, strict=True):
        representatives.setdefault(tree_id, (commit, source))
    snapshots_by_tree: dict[str, dict[str, tuple[int, str]]] = {}
    total_trees = len(representatives)
    tree_progress = _progress_range(progress, 5, 70)
    for index, (tree_id, (commit, source)) in enumerate(representatives.items(), start=1):
        with tempfile.TemporaryDirectory(prefix="upgrade-tree-", dir=root.parent) as name:
            tree = Path(name)
            repository.restore(commit, tree)
            fingerprint = snapshot_fingerprint(tree)
        _validate_entry_fingerprint(source, fingerprint)
        snapshots_by_tree[tree_id] = fingerprint
        _report(
            tree_progress, index, total_trees, f"fingerprinted unique tree {index}/{total_trees}"
        )

    converted: list[StepManifest] = []
    compact_entries: list[list[SnapshotEntry]] = []
    exceptions_by_entries: dict[int, list[SnapshotEntry]] = {}
    exception_digests: dict[int, str] = {}
    for step_number, (source, commit, tree_id) in enumerate(
        zip(source_steps, commits, trees, strict=True)
    ):
        _validate_entry_fingerprint(source, snapshots_by_tree[tree_id])
        cache_key = id(source.entries)
        exceptions = exceptions_by_entries.get(cache_key)
        if exceptions is None:
            exceptions = snapshot_exceptions(source.entries)
            exceptions_by_entries[cache_key] = exceptions
            exception_digests[cache_key] = digest_entries(exceptions)
        converted.append(
            _manifest(
                session_id,
                step_number,
                source,
                commit,
                exceptions,
                exception_digests[cache_key],
            )
        )
        compact_entries.append(exceptions)
    if add_boundary:
        _append_boundary(converted, trees, compact_entries)
    _install_history(
        root,
        converted,
        compact_entries,
        progress=_progress_range(progress, 70, 100),
    )
    return trees, compact_entries, snapshots_by_tree


def _rebuild_history(
    root: Path,
    session_id: str,
    source_steps: list[SourceStep],
    *,
    add_boundary: bool,
    progress: ProgressCallback | None = None,
) -> tuple[
    list[str],
    list[list[SnapshotEntry]],
    dict[str, dict[str, tuple[int, str]]],
]:
    old_repository = GitSnapshotStore(root / "snapshots.git")
    new_path = root / "snapshots.latest.git"
    if new_path.exists():
        raise FileExistsError(new_path)
    repository = GitSnapshotStore(new_path)
    trees: list[str] = []
    compact_entries: list[list[SnapshotEntry]] = []
    snapshots_by_tree: dict[str, dict[str, tuple[int, str]]] = {}
    converted: list[StepManifest] = []
    parent: str | None = None
    previous_tree: str | None = None
    tree_cache: dict[tuple[tuple[str, tuple[int, str]], ...], str] = {}
    exceptions_by_entries: dict[int, tuple[list[SnapshotEntry], str]] = {}
    try:
        total_steps = len(source_steps)
        conversion_progress = _progress_range(progress, 0, 75)
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
                fingerprint = snapshot_fingerprint(tree)
                fingerprint_key = tuple(sorted(fingerprint.items()))
                tree_id = tree_cache.get(fingerprint_key)
                if tree_id is None:
                    tree_id = repository.write_tree(tree)
                    tree_cache[fingerprint_key] = tree_id
                    snapshots_by_tree[tree_id] = fingerprint
            finally:
                if temporary is not None:
                    shutil.rmtree(temporary, ignore_errors=True)
            if parent is not None and tree_id == previous_tree:
                commit = parent
            else:
                commit = repository.commit_tree(
                    tree_id, parent, f"Memo filesystem snapshot {step_number}"
                )
            cache_key = id(source.entries)
            cached_exceptions = exceptions_by_entries.get(cache_key)
            if cached_exceptions is None:
                exceptions = snapshot_exceptions(source.entries)
                cached_exceptions = (exceptions, digest_entries(exceptions))
                exceptions_by_entries[cache_key] = cached_exceptions
            exceptions, exceptions_digest = cached_exceptions
            converted.append(
                _manifest(
                    session_id,
                    step_number,
                    source,
                    commit,
                    exceptions,
                    exceptions_digest,
                )
            )
            trees.append(tree_id)
            compact_entries.append(exceptions)
            parent = commit
            previous_tree = tree_id
            if (step_number + 1) == total_steps or (step_number + 1) % 64 == 0:
                _report(
                    conversion_progress,
                    step_number + 1,
                    total_steps,
                    f"converted snapshot {step_number + 1}/{total_steps}",
                )
        if add_boundary:
            _append_boundary(converted, trees, compact_entries)
        _install_history(
            root,
            converted,
            compact_entries,
            progress=_progress_range(progress, 75, 100),
        )
        if (root / "snapshots.git").exists():
            shutil.rmtree(root / "snapshots.git")
        os.replace(new_path, root / "snapshots.git")
        return trees, compact_entries, snapshots_by_tree
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
    expected_step: int | None = None,
    remote_complete: bool = False,
    progress: ProgressCallback | None = None,
    numeric_history: NumericHistory | None = None,
    recover_git_history: GitRecoveryCallback | None = None,
) -> UpgradeResult:
    """Upgrade an extracted session and return its verified semantic state."""
    session_id = validate_session_id(session_id)
    raw_session = _read_object(root / "session.json", "session metadata")
    format_version = raw_session.get("format_version")
    if format_version not in {1, 2}:
        raise ValueError(f"unsupported directory session version: {format_version}")

    if archive_had_bundle:
        if numeric_history is None:
            remote_sessions._restore_snapshot_bundle(root, session_id)
        else:
            _restore_streamed_bundle(root, session_id, numeric_history)

    if format_version == 1:
        if numeric_history is not None:
            raise ValueError("checkpoint session unexpectedly contains numeric history")
        source_steps, source_head = _checkpoint_steps(root, session_id)
        source_format = "directory-v1-checkpoints"
    else:
        if numeric_history is None:
            source_steps, schemas, source_head = _numeric_steps(
                root,
                session_id,
                progress=_progress_range(progress, 0, 20),
            )
        else:
            source_steps = numeric_history.steps
            schemas = numeric_history.schemas
            source_head = numeric_history.head
            _report(progress, 20, 100, f"validated {len(source_steps)} streamed manifests")

    if expected_step is not None and source_head != expected_step:
        raise ValueError(
            f"archive HEAD {source_head} does not match selected remote step {expected_step}"
        )

    recovered_git_history = False
    history_requires_rebuild = False
    source_commits = [step.source_commit for step in source_steps if step.source_commit]
    if source_commits:
        repository = GitSnapshotStore(root / "snapshots.git")
        present = repository.contains_many(source_commits)
        missing = set(source_commits) - present
        if missing and recover_git_history is not None:
            recovered_git_history = recover_git_history(
                root,
                source_steps,
                missing,
                _progress_range(progress, 20, 35),
            )
            present = repository.contains_many(source_commits)
            missing = set(source_commits) - present
        if missing:
            raise ValueError(f"source Git history is missing {len(missing)} snapshot commit(s)")
        final_commit = source_steps[-1].source_commit
        outside_published_history = final_commit is None or not set(source_commits).issubset(
            repository.reachable_from(final_commit)
        )
        # Only a commit recovered from a checksum-verified older generation has
        # independent provenance that permits linearizing a formerly forked
        # history. An unrelated object merely found in the selected repository
        # remains an error in _preserve_git_history below.
        history_requires_rebuild = recovered_git_history and outside_published_history

    if format_version == 2:
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
            and raw_session.get("state") == "complete"
            and not recovered_git_history
            and not history_requires_rebuild
        ):
            session = DirectorySession.from_dict(raw_session)
            if session.session_id != session_id:
                raise ValueError("session metadata does not match its remote identity")
            _validate_current_history(root, source_steps)
            raise AlreadyLatest("already uses the latest session and archive formats")

    session = _session(
        raw_session,
        session_id,
        fallback_origin,
        remote_complete=remote_complete,
    )
    atomic_write(root / "session.json", _json_bytes(session.to_dict()))
    history_progress = _progress_range(progress, 20, 100)
    if (
        source_steps
        and all(source.source_commit for source in source_steps)
        and not history_requires_rebuild
    ):
        trees, entries, snapshots_by_tree = _preserve_git_history(
            root,
            session_id,
            source_steps,
            add_boundary=transport_is_current,
            progress=history_progress,
        )
    else:
        trees, entries, snapshots_by_tree = _rebuild_history(
            root,
            session_id,
            source_steps,
            add_boundary=transport_is_current,
            progress=history_progress,
        )
    manifests = _store(root).steps(session_id)
    if [manifest.entries for manifest in manifests] != entries:
        raise ValueError("upgraded entry metadata did not validate")
    return UpgradeResult(source_format, session, trees, entries, snapshots_by_tree)
