"""Upgrade historical S3 session and transport formats without risking source data."""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from threading import Event, Lock
from typing import Any

from memo.recording.filesystem import atomic_write
from memo.recording.git_snapshots import GitSnapshotStore
from memo.recording.metadata import (
    ENTRIES_SCHEMA_VERSION,
    DirectorySession,
    SessionOrigin,
    SnapshotEntry,
    StepManifest,
    digest_entries,
)
from memo.recording.paths import StoragePaths
from memo.recording.store import SessionStore, validate_session_id
from memo.transport import remote_sessions
from memo.transport.archive import (
    PreparedGeneration,
    prepare_generation,
    safe_extract_tar_zst_stream,
)
from memo.transport.config import S3Config
from memo.transport.s3 import STREAM_READ_SIZE, S3Store

from .migrate import _safe_extract_tar
from .session_upgrade import (
    AlreadyLatest,
    BestEffortSubstitution,
    NumericHistory,
    NumericHistoryCollector,
    SourceStep,
    UpgradeResult,
    snapshot_fingerprint,
    upgrade_session,
)

SIDECAR_GENERATION = re.compile(r"^(\d{8,})\.tar\.zst$")
SIDECAR_CHECKSUM = re.compile(r"^(\d{8,})\.sha256$")
PROGRESS_BYTES = 8 * 1024 * 1024
STEP_MANIFEST_SIZE_LIMIT = 64 * 1024 * 1024
BEST_EFFORT_REPORT = "legacy-best-effort-migration.json"
ProgressCallback = Callable[[int, int, str], None]


@dataclass
class S3RecompressionSummary:
    sources: int = 0
    migrated: list[str] = field(default_factory=list)
    skipped: list[tuple[str, str]] = field(default_factory=list)
    failed: list[tuple[str, str]] = field(default_factory=list)
    original_bytes: int = 0
    replacement_bytes: int = 0
    formats: dict[str, str] = field(default_factory=dict)
    best_effort: dict[str, int] = field(default_factory=dict)
    retained_original_bytes: int = 0


@dataclass(frozen=True)
class RemoteCandidate:
    session_id: str
    layout: str
    locator: str


@dataclass(frozen=True)
class RecoveryGeneration:
    step: int
    object_key: str
    digest: str
    archive_kind: str = "tar.zst"


@dataclass(frozen=True)
class RemoteSource:
    candidate: RemoteCandidate
    origin: SessionOrigin
    step: int
    object_key: str
    digest: str
    archive_kind: str
    base: str
    completion_key: str | None
    completion_data: bytes | None
    cleanup_keys: tuple[str, ...]
    recovery_generations: tuple[RecoveryGeneration, ...] = ()

    @property
    def session_id(self) -> str:
        return self.candidate.session_id


@dataclass
class Replacement:
    source: RemoteSource
    session: DirectorySession
    prepared: PreparedGeneration
    original_size: int
    source_format: str
    best_effort_substitutions: tuple[BestEffortSubstitution, ...] = ()


@dataclass(frozen=True)
class CandidateOutcome:
    session_id: str
    status: str
    detail: str = ""
    source_format: str | None = None
    original_bytes: int = 0
    replacement_bytes: int = 0
    best_effort_substitutions: int = 0


class MigrationCancelled(BaseException):
    """Stop a worker promptly while allowing its scratch contexts to unwind."""


class NotEligible(ValueError):
    pass


def default_scratch_directory() -> Path:
    """Return a persistent parent for disposable migration run directories."""
    cache = os.environ.get("XDG_CACHE_HOME")
    root = Path(cache).expanduser() if cache else Path.home() / ".cache"
    return root / "memo" / "legacy-migrator"


def _scratch_directory(requested: Path | None) -> Path:
    root = (requested or default_scratch_directory()).expanduser()
    root.mkdir(parents=True, exist_ok=True)
    if not root.is_dir():
        raise NotADirectoryError(f"migration scratch path is not a directory: {root}")
    return root.resolve()


def _progress_range(
    progress: ProgressCallback | None,
    start: int,
    end: int,
) -> ProgressCallback | None:
    if progress is None:
        return None

    def report(completed: int, total: int, message: str) -> None:
        denominator = max(total, 1)
        fraction = max(0.0, min(completed / denominator, 1.0))
        progress(round(start + ((end - start) * fraction)), 100, message)

    return report


def _report(progress: ProgressCallback | None, completed: int, message: str) -> None:
    if progress is not None:
        progress(completed, 100, message)


def _canonical_json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def _key(config: S3Config, *parts: object) -> str:
    suffix = "/".join(str(part).strip("/") for part in parts)
    return f"{config.prefix}/{suffix}" if config.prefix else suffix


def _read_json(remote: S3Store, key: str) -> dict[str, Any]:
    value = json.loads(remote.read_bytes(key))
    if not isinstance(value, dict):
        raise ValueError(f"remote metadata is not an object: {key}")
    return value


def _valid_digest(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _origin(value: object, fallback: str) -> SessionOrigin:
    if isinstance(value, dict):
        try:
            fields = tuple(value[key] for key in ("memo_version_id", "username", "hostname"))
            if not all(isinstance(field, str) for field in fields):
                raise TypeError("origin fields must be strings")
            result = SessionOrigin(*fields)
            result.validate()
            return result
        except (KeyError, TypeError, ValueError):
            pass
    component = fallback.strip("/").split("/")[-1] or "unknown"
    return SessionOrigin("pre-origin", "legacy", component)


def discover_remote_candidates(remote: S3Store, config: S3Config) -> list[RemoteCandidate]:
    """Find sessions in every S3 hierarchy Memo has used."""
    prefix = f"{config.prefix}/" if config.prefix else ""
    index_prefix = f"{prefix}index/sessions/"
    current: set[str] = set()
    sidecar: dict[str, str] = {}
    latest: dict[str, list[str]] = {}
    for key in remote.list(prefix):
        if key.startswith(index_prefix):
            relative = key[len(index_prefix) :]
            parts = relative.split("/")
            if len(parts) == 2 and remote_sessions.INDEX_NAME.fullmatch(parts[1]):
                with suppress(ValueError):
                    current.add(validate_session_id(parts[0]))
            elif len(parts) == 1 and parts[0].endswith(".json"):
                with suppress(ValueError):
                    sidecar[validate_session_id(Path(parts[0]).stem)] = key
        if key.endswith("/latest.json"):
            try:
                session_id = validate_session_id(key.rsplit("/", 2)[-2])
            except ValueError:
                continue
            latest.setdefault(session_id, []).append(key)

    candidates: list[RemoteCandidate] = []
    all_ids = sorted(current | set(sidecar) | set(latest))
    for session_id in all_ids:
        layouts = (
            int(session_id in current) + int(session_id in sidecar) + int(session_id in latest)
        )
        if layouts > 1:
            # A flat index containing a `latest` pointer belongs to the mutable
            # layout and is not a second copy of the session.
            flat = sidecar.get(session_id)
            if flat and session_id in latest:
                try:
                    value = _read_json(remote, flat)
                except (OSError, ValueError, TypeError, json.JSONDecodeError):
                    value = {}
                if value.get("latest") in latest[session_id]:
                    sidecar.pop(session_id)
                    layouts -= 1
        if layouts != 1:
            candidates.append(RemoteCandidate(session_id, "conflict", ""))
        elif session_id in current:
            candidates.append(RemoteCandidate(session_id, "content-addressed", session_id))
        elif session_id in sidecar:
            candidates.append(RemoteCandidate(session_id, "sidecar", sidecar[session_id]))
        elif len(latest[session_id]) == 1:
            candidates.append(RemoteCandidate(session_id, "mutable-pointer", latest[session_id][0]))
        else:
            candidates.append(RemoteCandidate(session_id, "conflict", ""))
    return candidates


def _current_source(remote: S3Store, config: S3Config, candidate: RemoteCandidate) -> RemoteSource:
    session_id = candidate.session_id
    index = remote_sessions._load_index(remote, config, session_id)
    origin = SessionOrigin(index["memo_version_id"], index["username"], index["hostname"])
    base = remote_sessions._session_base(config, origin, session_id)
    step, object_key, digest, complete = remote_sessions._select_generation(
        remote, config, base, session_id
    )
    if not complete:
        raise NotEligible("remote session is not complete")
    generations = remote_sessions._list_generations(remote, f"{base}/generations/")
    if not generations or max(generations) != step:
        raise NotEligible("selected archive is not the latest remote generation")
    completion_key = remote_sessions._completion_key(base, step, digest)
    completion_data = remote.read_bytes(completion_key)
    return RemoteSource(
        candidate,
        origin,
        step,
        object_key,
        digest,
        "tar.zst",
        base,
        completion_key,
        completion_data,
        (completion_key, object_key),
        tuple(
            RecoveryGeneration(generation_step, key, generation_digest)
            for generation_step, (key, generation_digest) in sorted(
                generations.items(), reverse=True
            )
            if generation_step < step
        ),
    )


def _sidecar_pairs(remote: S3Store, prefix: str) -> dict[int, tuple[str, str]]:
    packages: dict[int, str] = {}
    checksums: dict[int, str] = {}
    for key in remote.list(prefix):
        relative = key[len(prefix) :]
        package = SIDECAR_GENERATION.fullmatch(relative)
        checksum = SIDECAR_CHECKSUM.fullmatch(relative)
        if package:
            packages[int(package.group(1))] = key
        elif checksum:
            checksums[int(checksum.group(1))] = key
    return {
        step: (package, checksums[step]) for step, package in packages.items() if step in checksums
    }


def _sidecar_source(remote: S3Store, config: S3Config, candidate: RemoteCandidate) -> RemoteSource:
    index = _read_json(remote, candidate.locator)
    if index.get("schema_version") != 1 or index.get("session_id") != candidate.session_id:
        raise ValueError("legacy sidecar index is invalid")
    origin = _origin(index, "sidecar")
    base = remote_sessions._session_base(config, origin, candidate.session_id)
    pairs = _sidecar_pairs(remote, f"{base}/generations/")
    completion_key = f"{base}/completion.json"
    if not remote.exists(completion_key):
        raise NotEligible("remote session is not complete")
    completion_data = remote.read_bytes(completion_key)
    completion = json.loads(completion_data)
    if (
        not isinstance(completion, dict)
        or completion.get("schema_version") != 1
        or completion.get("session_id") != candidate.session_id
    ):
        raise ValueError("legacy completion record is invalid")
    step = completion.get("final_step")
    digest = completion.get("sha256")
    if not isinstance(step, int) or isinstance(step, bool) or not _valid_digest(digest):
        raise ValueError("legacy completion record is invalid")
    pair = pairs.get(step)
    if pair is None or max(pairs) != step or completion.get("generation") != pair[0]:
        raise ValueError("legacy completion does not select the latest complete generation")
    checksum_data = remote.read_bytes(pair[1]).decode().split()
    if not checksum_data or checksum_data[0] != digest:
        raise ValueError("legacy checksum and completion record disagree")
    recovery_generations = []
    for earlier_step, (earlier_package, earlier_checksum) in sorted(pairs.items(), reverse=True):
        if earlier_step >= step:
            continue
        try:
            fields = remote.read_bytes(earlier_checksum).decode().split()
        except (OSError, UnicodeDecodeError):
            continue
        if not fields or not _valid_digest(fields[0]):
            continue
        recovery_generations.append(RecoveryGeneration(earlier_step, earlier_package, fields[0]))
    return RemoteSource(
        candidate,
        origin,
        step,
        pair[0],
        str(digest),
        "tar.zst",
        base,
        completion_key,
        completion_data,
        (completion_key, candidate.locator, pair[1], pair[0]),
        tuple(recovery_generations),
    )


def _mutable_source(remote: S3Store, config: S3Config, candidate: RemoteCandidate) -> RemoteSource:
    pointer = _read_json(remote, candidate.locator)
    if pointer.get("schema_version") not in {1, 2, 3}:
        raise ValueError("unsupported mutable pointer schema")
    if pointer.get("session_id") != candidate.session_id:
        raise ValueError("mutable pointer session identity mismatch")
    object_key = pointer.get("object")
    checksum_key = pointer.get("checksum")
    digest = pointer.get("digest")
    base = candidate.locator.removesuffix("/latest.json")
    if (
        not isinstance(object_key, str)
        or not object_key.startswith(f"{base}/")
        or not isinstance(checksum_key, str)
        or checksum_key != f"{object_key}.sha256"
        or not _valid_digest(digest)
    ):
        raise ValueError("mutable pointer object metadata is invalid")
    checksum = remote.read_bytes(checksum_key).decode().split()
    if not checksum or checksum[0] != digest:
        raise ValueError("mutable pointer and checksum disagree")
    step_value = pointer.get("step", pointer.get("generation", 1))
    if not isinstance(step_value, int) or isinstance(step_value, bool) or step_value < 0:
        raise ValueError("mutable pointer has an invalid generation")
    origin = _origin(pointer.get("origin"), str(pointer.get("namespace") or base))
    schema = int(pointer["schema_version"])
    if schema in {1, 2}:
        namespace = pointer.get("namespace")
        expected_base = _key(config, namespace, candidate.session_id)
        allowed_prefixes = (
            f"{base}/generations/",
            f"{base}/steps/",
        )
        if not isinstance(namespace, str) or not namespace or base != expected_base:
            raise ValueError("mutable pointer namespace does not match its object hierarchy")
    else:
        expected_base = remote_sessions._session_base(config, origin, candidate.session_id)
        allowed_prefixes = (f"{base}/steps/",)
        if base != expected_base:
            raise ValueError("mutable pointer origin does not match its object hierarchy")
    if not object_key.startswith(allowed_prefixes):
        raise ValueError("mutable pointer object is outside its generation hierarchy")
    if object_key.endswith(".tar.gz"):
        archive_kind = "tar.gz"
    elif object_key.endswith(".tar.zst"):
        archive_kind = "tar.zst"
    else:
        raise ValueError("mutable pointer has an unsupported archive type")
    flat_index = _key(config, "index", "sessions", f"{candidate.session_id}.json")
    cleanup = [candidate.locator, checksum_key]
    if remote.exists(flat_index):
        index = _read_json(remote, flat_index)
        if index.get("latest") == candidate.locator:
            cleanup.append(flat_index)
    cleanup.append(object_key)
    return RemoteSource(
        candidate,
        origin,
        int(step_value),
        object_key,
        str(digest),
        archive_kind,
        base,
        None,
        None,
        tuple(cleanup),
    )


def source_for_candidate(
    remote: S3Store, config: S3Config, candidate: RemoteCandidate
) -> RemoteSource:
    if candidate.layout == "content-addressed":
        return _current_source(remote, config, candidate)
    if candidate.layout == "sidecar":
        return _sidecar_source(remote, config, candidate)
    if candidate.layout == "mutable-pointer":
        return _mutable_source(remote, config, candidate)
    raise ValueError("multiple remote layouts advertise this session")


def _file_digest(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def _stream_digest(
    remote: S3Store,
    key: str,
    progress: ProgressCallback | None = None,
) -> tuple[str, int]:
    body = remote.open(key)
    digest = hashlib.sha256()
    size = 0
    expected_size = remote.size(key)
    if progress is not None:
        progress(0, expected_size or 1, "verifying uploaded bytes")
    reported = 0
    try:
        while chunk := body.read(STREAM_READ_SIZE):
            digest.update(chunk)
            size += len(chunk)
            if progress is not None and size - reported >= PROGRESS_BYTES:
                progress(size, expected_size or max(size, 1), "verifying uploaded bytes")
                reported = size
    finally:
        remote.close(body)
    if progress is not None:
        progress(size, expected_size or max(size, 1), "verified uploaded bytes")
    return digest.hexdigest(), size


def _preserved_files(root: Path) -> dict[str, tuple[int, str]]:
    result: dict[str, tuple[int, str]] = {}
    transformed_roots = {
        "checkpoints",
        "entries",
        "snapshots",
        "snapshots.bundle",
        "snapshots.git",
        "steps",
    }
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root)
        if (
            relative.as_posix() in {"HEAD", "session.json"}
            or relative.parts[0] in transformed_roots
        ):
            continue
        if path.is_file():
            result[relative.as_posix()] = (path.stat().st_mode & 0o777, _file_digest(path))
    return result


def _fingerprint_difference(
    expected: dict[str, tuple[int, str]],
    actual: dict[str, tuple[int, str]],
) -> str:
    def limited(values: list[str]) -> list[str]:
        return values[:20] + ([f"... and {len(values) - 20} more"] if len(values) > 20 else [])

    shared = expected.keys() & actual.keys()
    missing = sorted(expected.keys() - actual.keys())
    added = sorted(actual.keys() - expected.keys())
    content_changed = sorted(path for path in shared if expected[path][1] != actual[path][1])
    executable_changed = sorted(path for path in shared if expected[path][0] != actual[path][0])
    return (
        f"missing={limited(missing)}, added={limited(added)}, "
        f"content_changed={limited(content_changed)}, "
        f"executable_changed={limited(executable_changed)}"
    )


def _download(
    remote: S3Store,
    source: RemoteSource,
    destination: Path,
    progress: ProgressCallback | None = None,
) -> tuple[int, bool, NumericHistory | None]:
    expected_size = remote.size(source.object_key)
    if expected_size is None:
        raise ValueError("remote object size is unavailable")
    archive_path = destination.parent / f".{source.session_id}.original.{source.archive_kind}"
    body = remote.open(source.object_key)
    digest = hashlib.sha256()
    downloaded_size = 0
    reported = 0
    download_progress = _progress_range(progress, 0, 60)
    if download_progress is not None:
        download_progress(0, expected_size, "downloading source archive")
    try:
        with archive_path.open("wb") as archive:
            while chunk := body.read(STREAM_READ_SIZE):
                archive.write(chunk)
                digest.update(chunk)
                downloaded_size += len(chunk)
                if download_progress is not None and downloaded_size - reported >= PROGRESS_BYTES:
                    download_progress(downloaded_size, expected_size, "downloading source archive")
                    reported = downloaded_size
            archive.flush()
            os.fsync(archive.fileno())
    finally:
        remote.close(body)
    if digest.hexdigest() != source.digest:
        raise ValueError(
            f"download checksum mismatch: expected {source.digest}, got {digest.hexdigest()}"
        )
    if downloaded_size != expected_size:
        raise ValueError(f"download size mismatch: expected {expected_size}, got {downloaded_size}")
    if download_progress is not None:
        download_progress(downloaded_size, expected_size, "downloaded source archive")
    if source.archive_kind == "tar.zst":
        collector = NumericHistoryCollector(destination, source.session_id)

        def file_handler(name, member):
            if name.parts[0] != "steps":
                return None
            if len(name.parts) != 2 or name.suffix != ".json" or not name.stem.isdigit():
                raise ValueError(f"source contains an unexpected step file: {name.as_posix()}")
            if member.size > STEP_MANIFEST_SIZE_LIMIT:
                raise ValueError(f"step manifest is too large: {name.as_posix()}")
            number = int(name.stem)

            def capture(handle) -> None:
                data = handle.read(STEP_MANIFEST_SIZE_LIMIT + 1)
                if len(data) != member.size:
                    raise ValueError(f"step manifest is truncated: {name.as_posix()}")
                collector.add(number, data)

            return capture

        with archive_path.open("rb") as archive:
            extracted_digest = safe_extract_tar_zst_stream(
                archive,
                destination,
                progress=_progress_range(progress, 60, 100),
                progress_total=expected_size,
                progress_message="extracting source archive",
                file_handler=file_handler,
            )
        if extracted_digest != source.digest:
            raise ValueError("scratch archive changed between download and extraction")
    else:
        collector = None
        _report(progress, 60, "extracting source archive")
        _safe_extract_tar(archive_path, destination)
        if _file_digest(archive_path) != source.digest:
            raise ValueError("scratch archive changed between download and extraction")
        _report(progress, 100, "extracted source archive")
    had_bundle = (destination / "snapshots.bundle").is_file()
    history = None
    if collector is not None and not collector.empty:
        head_value = (destination / "HEAD").read_text().strip()
        if not head_value.isdigit():
            raise ValueError("invalid numeric HEAD step")
        history = collector.finish(int(head_value))
    return downloaded_size, had_bundle, history


def _recover_git_history(
    remote: S3Store,
    source: RemoteSource,
    work: Path,
    root: Path,
    _source_steps: list[SourceStep],
    missing: set[str],
    progress: ProgressCallback | None = None,
) -> bool:
    """Recover referenced objects from checksum-verified earlier generations.

    A generation is accepted as an object source only when it belongs to the
    same indexed session and its archive bytes match its content-addressed
    digest. Only the exact commit IDs named by the selected generation are
    imported; older manifests and disconnected objects may provide those
    bytes, but the selected manifests remain authoritative.
    """
    remaining = set(missing)
    recovered_any = False
    repository = GitSnapshotStore(root / "snapshots.git")
    generations = source.recovery_generations
    for index, generation in enumerate(generations, start=1):
        if not remaining:
            break
        if progress is not None:
            progress(index - 1, max(len(generations), 1), "checking older Git generation")
        with tempfile.TemporaryDirectory(
            prefix=f"memo-git-recovery-{generation.step}-", dir=work
        ) as recovery_name:
            recovery_root = Path(recovery_name) / source.session_id
            recovery_root.mkdir()
            recovery_source = RemoteSource(
                source.candidate,
                source.origin,
                generation.step,
                generation.object_key,
                generation.digest,
                generation.archive_kind,
                source.base,
                None,
                None,
                (),
            )
            recovery_download_progress = None
            if progress is not None:

                def recovery_download_progress(
                    completed: int,
                    total: int,
                    message: str,
                ) -> None:
                    fraction = max(0.0, min(completed / max(total, 1), 1.0))
                    progress(
                        round(((index - 1) + fraction) * 1000),
                        max(len(generations), 1) * 1000,
                        message,
                    )

            _, had_bundle, history = _download(
                remote,
                recovery_source,
                recovery_root,
                progress=recovery_download_progress,
            )
            metadata = json.loads((recovery_root / "session.json").read_bytes())
            if (
                not isinstance(metadata, dict)
                or metadata.get("format") != "memo-directory-session"
                or metadata.get("session_id") != source.session_id
            ):
                raise ValueError("older generation has mismatched session metadata")
            if history is None or not history.steps:
                continue
            published_commit = history.steps[-1].source_commit
            object_source = (
                recovery_root / "snapshots.bundle"
                if had_bundle
                else recovery_root / "snapshots.git"
            )
            if not object_source.exists():
                continue
            if object_source.is_dir():
                source_repository = GitSnapshotStore(object_source)
                actual_source_tip = source_repository.published_commit()
                # Some historical archives contain an empty snapshots.git
                # directory. It is not evidence for any missing object and
                # must not prevent searching still-older generations.
                if actual_source_tip is None:
                    continue
                if published_commit and actual_source_tip != published_commit:
                    continue
                if not source_repository.contains_many(remaining):
                    continue
            actual_tip = repository.import_objects(
                object_source,
                f"{generation.step}-{generation.digest[:16]}",
                remaining,
            )
            if published_commit and actual_tip != published_commit:
                raise ValueError("older Git generation tip does not match its manifest")
            recovered = repository.contains_many(remaining)
            remaining -= recovered
            recovered_any = recovered_any or bool(recovered)
        if progress is not None:
            progress(
                index, max(len(generations), 1), f"recovered Git objects; {len(remaining)} missing"
            )
    return recovered_any


def _store(root: Path) -> SessionStore:
    return SessionStore(
        StoragePaths(
            root.parent,
            archive=root.parent,
            runtime=root.parent / "runtime-verify",
            spool=root.parent / "spool-verify",
        )
    )


class ReplacementArchiveCollector:
    """Validate replacement metadata directly from a safe archive stream."""

    def __init__(self) -> None:
        self.session: DirectorySession | None = None
        self.head: int | None = None
        self.manifests: dict[int, StepManifest] = {}
        self.entries: dict[str, list[SnapshotEntry]] = {}

    @staticmethod
    def _read(handle, size: int, label: str) -> bytes:
        if size > STEP_MANIFEST_SIZE_LIMIT:
            raise ValueError(f"replacement {label} is too large")
        data = handle.read(STEP_MANIFEST_SIZE_LIMIT + 1)
        if len(data) != size:
            raise ValueError(f"replacement {label} is truncated")
        return data

    def file_handler(self, name, member):
        path = name.as_posix()
        if path == "session.json":

            def session(handle) -> None:
                value = json.loads(self._read(handle, member.size, "session metadata"))
                if not isinstance(value, dict):
                    raise ValueError("replacement session metadata must be an object")
                self.session = DirectorySession.from_dict(value)

            return session
        if path == "HEAD":

            def head(handle) -> None:
                value = self._read(handle, member.size, "HEAD").decode().strip()
                if not value.isdigit():
                    raise ValueError("replacement has an invalid HEAD")
                self.head = int(value)

            return head
        if len(name.parts) == 2 and name.parts[0] == "entries" and name.suffix == ".json":
            digest = name.stem
            if not _valid_digest(digest):
                raise ValueError("replacement has an invalid shared entry filename")

            def entries(handle) -> None:
                value = json.loads(self._read(handle, member.size, "shared entry list"))
                if (
                    not isinstance(value, dict)
                    or value.get("schema_version") != ENTRIES_SCHEMA_VERSION
                ):
                    raise ValueError("replacement has an invalid shared entry list")
                raw_entries = value.get("entries")
                if not isinstance(raw_entries, list) or not all(
                    isinstance(item, dict) for item in raw_entries
                ):
                    raise ValueError("replacement has an invalid shared entry list")
                parsed = [SnapshotEntry.from_dict(item) for item in raw_entries]
                if digest_entries(parsed) != digest:
                    raise ValueError("replacement shared entry digest does not match its filename")
                self.entries[digest] = parsed

            return entries
        if name.parts[0] == "entries":
            raise ValueError(f"replacement contains an unexpected entry file: {path}")
        if len(name.parts) == 2 and name.parts[0] == "steps" and name.suffix == ".json":
            if not name.stem.isdigit():
                raise ValueError(f"replacement contains an unexpected step file: {path}")
            number = int(name.stem)

            def manifest(handle) -> None:
                if number in self.manifests:
                    raise ValueError(f"replacement has duplicate numeric step: {number}")
                value = json.loads(self._read(handle, member.size, "step manifest"))
                if not isinstance(value, dict):
                    raise ValueError("replacement step manifest must be an object")
                parsed = StepManifest.from_dict(value)
                if parsed.step != number:
                    raise ValueError("replacement step identity does not match its filename")
                self.manifests[number] = parsed

            return manifest
        if name.parts[0] == "steps":
            raise ValueError(f"replacement contains an unexpected step file: {path}")
        if name.parts[0] in {"checkpoints", "snapshots", "snapshots.git"}:
            raise ValueError(f"replacement contains unexpected snapshot data: {path}")
        return None

    def finish(self) -> tuple[DirectorySession, list[StepManifest]]:
        if self.session is None or self.head is None:
            raise ValueError("replacement is missing session metadata or HEAD")
        numbers = sorted(self.manifests)
        if numbers != list(range(self.head + 1)):
            raise ValueError("replacement step history is not contiguous through HEAD")
        manifests = [self.manifests[number] for number in numbers]
        for manifest in manifests:
            if manifest.session_id != self.session.session_id:
                raise ValueError("replacement step belongs to another session")
            if manifest.entries:
                raise ValueError("replacement step contains inline shared entries")
            digest = manifest.entries_digest
            if not digest or digest not in self.entries:
                raise ValueError("replacement step references a missing shared entry list")
            manifest.entries = self.entries[digest]
            manifest.validate()
        if set(self.entries) != {manifest.entries_digest for manifest in manifests}:
            raise ValueError("replacement contains an unreferenced shared entry list")
        return self.session, manifests


def _verify_replacement(
    root: Path,
    result: UpgradeResult,
    expected_preserved: dict[str, tuple[int, str]],
    session: DirectorySession,
    manifests: list[StepManifest],
    progress: ProgressCallback | None = None,
) -> None:
    _report(progress, 0, "checking replacement metadata")
    if session != result.session:
        raise ValueError("replacement session metadata does not match the upgrade")
    commits = [manifest.snapshot_commit or "" for manifest in manifests]
    if any(not commit for commit in commits):
        raise ValueError("replacement contains a step without a snapshot commit")
    bundle = root / "snapshots.bundle"
    repository = GitSnapshotStore(root / "snapshots.git")
    repository.import_bundle(bundle, commits[-1])
    bundle.unlink()
    trees_by_commit = repository.tree_ids(commits)
    if len(trees_by_commit) != len(set(commits)):
        raise ValueError("replacement contains a missing or invalid snapshot commit")
    final_commit = commits[-1]
    if not set(commits).issubset(repository.reachable_from(final_commit)):
        raise ValueError("replacement contains commits outside its published history")
    repository.check_connectivity(final_commit)
    trees = [trees_by_commit[commit] for commit in commits]
    if trees != result.tree_ids:
        raise ValueError("replacement filesystem content does not match the source")
    if [manifest.entries for manifest in manifests] != result.entries:
        raise ValueError("replacement snapshot metadata does not match the source")
    high_water: dict[str, int] = {}
    run_ids: dict[str, None] = {}
    for manifest in manifests:
        for terminal_id, sequence in manifest.stream_high_water.items():
            high_water[terminal_id] = max(sequence, high_water.get(terminal_id, 0))
        for run_id in manifest.agent_runs:
            run_ids.setdefault(run_id, None)
    SessionStore._validate_streams(root, high_water, chunks=True)
    SessionStore._validate_agent_runs(root, run_ids)
    if set(trees) != set(result.snapshots_by_tree):
        raise ValueError("replacement unique filesystem states do not match the source")
    representatives: dict[str, StepManifest] = {}
    for tree_id, manifest in zip(trees, manifests, strict=True):
        representatives.setdefault(tree_id, manifest)
    total_snapshots = max(len(representatives), 1)
    for index, (tree_id, manifest) in enumerate(representatives.items(), start=1):
        with tempfile.TemporaryDirectory(prefix="verify-snapshot-", dir=root.parent) as name:
            restored = Path(name) / "tree"
            repository.restore(manifest.snapshot_commit or "", restored)
            expected_fingerprint = result.snapshots_by_tree[tree_id]
            actual_fingerprint = snapshot_fingerprint(restored)
            if actual_fingerprint != expected_fingerprint:
                raise ValueError(
                    "replacement filesystem bytes do not match the source "
                    f"({_fingerprint_difference(expected_fingerprint, actual_fingerprint)})"
                )
        if progress is not None:
            progress(index, total_snapshots, f"verified unique tree {index}/{total_snapshots}")
    actual_preserved = _preserved_files(root)
    if actual_preserved != expected_preserved:
        missing = sorted(expected_preserved.keys() - actual_preserved.keys())
        added = sorted(actual_preserved.keys() - expected_preserved.keys())
        changed = sorted(
            key
            for key in expected_preserved.keys() & actual_preserved.keys()
            if expected_preserved[key] != actual_preserved[key]
        )
        raise ValueError(
            "replacement changed non-snapshot session data "
            f"(missing={missing}, added={added}, changed={changed})"
        )


def _prepare_replacement(
    remote: S3Store,
    source: RemoteSource,
    work: Path,
    progress: ProgressCallback | None = None,
    best_effort: bool = False,
) -> Replacement:
    extracted = work / source.session_id
    extracted.mkdir()
    original_size, had_bundle, numeric_history = _download(
        remote,
        source,
        extracted,
        progress=_progress_range(progress, 0, 45),
    )
    _report(progress, 48, "fingerprinting preserved session data")
    expected_preserved = _preserved_files(extracted)
    _report(progress, 50, "upgrading session in scratch")
    recover_git_history = None
    if source.recovery_generations:

        def recover_git_history(
            root: Path,
            steps: list[SourceStep],
            missing: set[str],
            recovery_progress: ProgressCallback | None,
        ) -> bool:
            return _recover_git_history(
                remote,
                source,
                work,
                root,
                steps,
                missing,
                recovery_progress,
            )

    result = upgrade_session(
        extracted,
        source.session_id,
        source.origin,
        transport_is_current=source.candidate.layout == "content-addressed",
        archive_had_bundle=had_bundle,
        expected_step=source.step,
        remote_complete=source.completion_key is not None,
        progress=_progress_range(progress, 50, 75),
        numeric_history=numeric_history,
        recover_git_history=recover_git_history,
        best_effort=best_effort,
    )
    if result.best_effort_substitutions:
        report_path = extracted / BEST_EFFORT_REPORT
        if report_path.exists():
            raise ValueError(f"source session already contains reserved file: {BEST_EFFORT_REPORT}")
        report = {
            "schema_version": 1,
            "mode": "best-effort",
            "session_id": source.session_id,
            "source_archive": {
                "object_key": source.object_key,
                "sha256": source.digest,
                "size_bytes": original_size,
                "retained_in_s3": True,
            },
            "substitution_policy": "nearest verified state; prefer preceding on ties",
            "substitutions": [
                {
                    "step": substitution.step,
                    "missing_commit": substitution.missing_commit,
                    "substitute_step": substitution.substitute_step,
                    "substitute_commit": substitution.substitute_commit,
                    "direction": substitution.direction,
                }
                for substitution in result.best_effort_substitutions
            ],
        }
        atomic_write(report_path, _canonical_json(report) + b"\n")
        report_path.chmod(0o644)
        expected_preserved[BEST_EFFORT_REPORT] = (
            report_path.stat().st_mode & 0o777,
            _file_digest(report_path),
        )
    prepared = prepare_generation(
        _store(extracted),
        result.session,
        progress=_progress_range(progress, 75, 82),
    )
    try:
        verified = work / "verified" / source.session_id
        verified.mkdir(parents=True)
        replacement_archive = ReplacementArchiveCollector()
        with prepared.path.open("rb") as archive:
            digest = safe_extract_tar_zst_stream(
                archive,
                verified,
                progress=_progress_range(progress, 82, 88),
                progress_total=prepared.size_bytes,
                progress_message="scanning prepared replacement",
                file_handler=replacement_archive.file_handler,
            )
        if digest != prepared.digest:
            raise ValueError("locally prepared replacement checksum mismatch")
        replacement_session, replacement_manifests = replacement_archive.finish()
        _verify_replacement(
            verified,
            result,
            expected_preserved,
            replacement_session,
            replacement_manifests,
            progress=_progress_range(progress, 88, 100),
        )
        _report(progress, 100, "replacement verified locally")
        return Replacement(
            source,
            result.session,
            prepared,
            original_size,
            result.source_format,
            result.best_effort_substitutions,
        )
    except BaseException:
        prepared.cleanup()
        raise


def _completion_data(replacement: Replacement, generation: str) -> bytes:
    return _canonical_json(
        {
            "schema_version": 1,
            "session_id": replacement.source.session_id,
            "final_step": replacement.prepared.step,
            "generation": generation,
            "sha256": replacement.prepared.digest,
        }
    )


def _same_source(remote: S3Store, config: S3Config, source: RemoteSource) -> bool:
    try:
        return source_for_candidate(remote, config, source.candidate) == source
    except Exception:
        return False


def _install_replacement(
    remote: S3Store,
    config: S3Config,
    replacement: Replacement,
    progress: ProgressCallback | None = None,
) -> None:
    source = replacement.source
    base = remote_sessions._session_base(config, replacement.session.origin, source.session_id)
    generation = remote_sessions._generation_key(
        base, replacement.prepared.step, replacement.prepared.digest
    )
    completion_key = remote_sessions._completion_key(
        base, replacement.prepared.step, replacement.prepared.digest
    )
    index_key, index_data = remote_sessions._index_record(config, replacement.session)
    staging = f"{base}/migration-staging/{replacement.prepared.path.name}"
    if remote.exists(staging) or remote.exists(generation) or remote.exists(completion_key):
        raise FileExistsError(
            "replacement generation, completion, or staging object already exists"
        )

    _report(progress, 0, "uploading staging replacement")
    remote.upload_file(
        staging,
        replacement.prepared.path,
        progress=_progress_range(progress, 0, 20),
    )
    try:
        staged_digest, staged_size = _stream_digest(
            remote,
            staging,
            progress=_progress_range(progress, 20, 40),
        )
        if (staged_digest, staged_size) != (
            replacement.prepared.digest,
            replacement.prepared.size_bytes,
        ):
            raise ValueError("staged replacement is not byte-identical to the local candidate")
        if not _same_source(remote, config, source):
            raise ValueError("remote session changed while its replacement was prepared")

        _report(progress, 45, "uploading final replacement")
        remote.upload_file(
            generation,
            replacement.prepared.path,
            progress=_progress_range(progress, 45, 65),
        )
        final_digest, final_size = _stream_digest(
            remote,
            generation,
            progress=_progress_range(progress, 65, 85),
        )
        if (final_digest, final_size) != (
            replacement.prepared.digest,
            replacement.prepared.size_bytes,
        ):
            remote.remove(generation)
            raise ValueError("uploaded replacement is not byte-identical to the local candidate")

        old_completion_removed = False
        index_written = False
        try:
            _report(progress, 88, "publishing verified replacement")
            if source.candidate.layout == "content-addressed":
                assert source.completion_key is not None
                remote.remove(source.completion_key)
                old_completion_removed = True
            remote.put_bytes(index_key, index_data)
            index_written = source.candidate.layout != "content-addressed"
            remote.put_bytes(completion_key, _completion_data(replacement, generation))

            selected_index = remote_sessions._load_index(remote, config, source.session_id)
            selected_base = remote_sessions._session_base(config, selected_index, source.session_id)
            selected = remote_sessions._select_generation(
                remote, config, selected_base, source.session_id
            )
            if selected[0:3] != (
                replacement.prepared.step,
                generation,
                replacement.prepared.digest,
            ):
                raise ValueError("replacement was uploaded but is not the selected generation")
        except BaseException:
            with suppress(Exception):
                remote.remove(completion_key)
            if old_completion_removed and source.completion_key and source.completion_data:
                remote.put_bytes(source.completion_key, source.completion_data)
            if index_written:
                with suppress(Exception):
                    remote.remove(index_key)
            with suppress(Exception):
                remote.remove(generation)
            raise

        retain_source = bool(replacement.best_effort_substitutions)
        for key in source.cleanup_keys:
            if key != source.object_key and not (
                retain_source and key == f"{source.object_key}.sha256"
            ):
                remote.remove(key)
        # An equivalent migration removes the source archive last. A
        # best-effort replacement is intentionally not equivalent, so its
        # original bytes (and sidecar checksum, when present) remain available
        # for forensic recovery.
        if not retain_source:
            remote.remove(source.object_key)
        _report(progress, 100, "replacement installed")
    finally:
        with suppress(Exception):
            remote.remove(staging)


def recompress_s3(
    config: S3Config | None = None,
    client: object | None = None,
    *,
    dry_run: bool = False,
    scratch_dir: Path | None = None,
    progress: ProgressCallback | None = None,
    item_progress: ProgressCallback | None = None,
    workers: int = 4,
    session_ids: list[str] | None = None,
    best_effort: bool = False,
) -> S3RecompressionSummary:
    """Detect and upgrade every indexed historical S3 session format."""
    if not 1 <= workers <= 8:
        raise ValueError("workers must be between 1 and 8")
    config = config or S3Config.discover(required=True)
    assert config is not None
    remote = client if isinstance(client, S3Store) else S3Store(config, client)
    if progress is not None:
        progress(0, 1, "discovering indexed S3 sessions")
    candidates = discover_remote_candidates(remote, config)
    if session_ids is not None:
        requested = {validate_session_id(session_id) for session_id in session_ids}
        available = {candidate.session_id for candidate in candidates}
        missing_sessions = sorted(requested - available)
        if missing_sessions:
            raise ValueError(f"indexed S3 session not found: {', '.join(missing_sessions)}")
        candidates = [candidate for candidate in candidates if candidate.session_id in requested]
    summary = S3RecompressionSummary(sources=len(candidates))
    if not candidates:
        if progress is not None:
            progress(1, 1, "no indexed S3 sessions found")
        return summary
    scratch = _scratch_directory(scratch_dir)
    progress_total = len(candidates) * 1000
    progress_by_position = [0] * len(candidates)
    progress_lock = Lock()
    cancelled = Event()

    def process(position: int, candidate: RemoteCandidate) -> CandidateOutcome:
        prepared: PreparedGeneration | None = None
        last_report: tuple[int, str] | None = None

        def session_progress(completed: int, total: int, message: str) -> None:
            nonlocal last_report
            if cancelled.is_set():
                raise MigrationCancelled
            fraction = max(0.0, min(completed / max(total, 1), 1.0))
            progress_units = round(fraction * 1000)
            report = (progress_units, message)
            if report == last_report:
                return
            last_report = report
            with progress_lock:
                progress_by_position[position - 1] = progress_units
                overall = sum(progress_by_position)
                remaining = sum(value < 1000 for value in progress_by_position)
                identity = (
                    f"({position}/{len(candidates)}, {remaining} remaining) {candidate.session_id}"
                )
                if item_progress is not None:
                    item_progress(completed, total, f"{candidate.session_id} {message}")
                if progress is not None:
                    progress(
                        overall,
                        progress_total,
                        identity if item_progress is not None else f"{identity} {message}",
                    )

        session_progress(0, 1, "checking eligibility")
        try:
            source = source_for_candidate(remote, config, candidate)
            with tempfile.TemporaryDirectory(
                prefix=f"memo-s3-upgrade-{candidate.session_id}-",
                dir=scratch,
            ) as work_name:
                if progress is None and item_progress is None:
                    replacement = _prepare_replacement(
                        remote,
                        source,
                        Path(work_name),
                        best_effort=best_effort,
                    )
                else:
                    prepare_progress = (
                        session_progress if dry_run else _progress_range(session_progress, 0, 70)
                    )
                    replacement = _prepare_replacement(
                        remote,
                        source,
                        Path(work_name),
                        progress=prepare_progress,
                        best_effort=best_effort,
                    )
                prepared = replacement.prepared
                if not dry_run:
                    if progress is None and item_progress is None:
                        _install_replacement(remote, config, replacement)
                    else:
                        _install_replacement(
                            remote,
                            config,
                            replacement,
                            progress=_progress_range(session_progress, 70, 100),
                        )
                return CandidateOutcome(
                    candidate.session_id,
                    "migrated",
                    source_format=replacement.source_format,
                    original_bytes=replacement.original_size,
                    replacement_bytes=replacement.prepared.size_bytes,
                    best_effort_substitutions=len(replacement.best_effort_substitutions),
                )
        except (AlreadyLatest, NotEligible) as error:
            return CandidateOutcome(candidate.session_id, "skipped", str(error))
        except Exception as error:
            return CandidateOutcome(candidate.session_id, "failed", str(error))
        finally:
            if prepared is not None:
                prepared.cleanup()
            if not cancelled.is_set():
                session_progress(1, 1, "finished")

    executor = ThreadPoolExecutor(max_workers=min(workers, len(candidates)))
    futures: list[Future[CandidateOutcome]] = [
        executor.submit(process, position, candidate)
        for position, candidate in enumerate(candidates, start=1)
    ]
    try:
        outcomes = [future.result() for future in futures]
    except BaseException:
        cancelled.set()
        for future in futures:
            future.cancel()
        executor.shutdown(wait=True, cancel_futures=True)
        raise
    else:
        executor.shutdown(wait=True)

    for outcome in outcomes:
        if outcome.status == "migrated":
            summary.migrated.append(outcome.session_id)
            assert outcome.source_format is not None
            summary.formats[outcome.session_id] = outcome.source_format
            summary.original_bytes += outcome.original_bytes
            summary.replacement_bytes += outcome.replacement_bytes
            if outcome.best_effort_substitutions:
                summary.best_effort[outcome.session_id] = outcome.best_effort_substitutions
                summary.retained_original_bytes += outcome.original_bytes
        elif outcome.status == "skipped":
            summary.skipped.append((outcome.session_id, outcome.detail))
        else:
            summary.failed.append((outcome.session_id, outcome.detail))
    return summary


# The old public name remains import-compatible while the CLI moves to upgrade language.
upgrade_s3 = recompress_s3

# Tests and downstream callers used the original private type names.
_RemoteSource = RemoteSource
_Replacement = Replacement
