"""Upgrade historical S3 session and transport formats without risking source data."""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from memo.recording.git_snapshots import GitSnapshotStore
from memo.recording.metadata import DirectorySession, SessionOrigin
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
    UpgradeResult,
    snapshot_fingerprint,
    upgrade_session,
)

SIDECAR_GENERATION = re.compile(r"^(\d{8,})\.tar\.zst$")
SIDECAR_CHECKSUM = re.compile(r"^(\d{8,})\.sha256$")
PROGRESS_BYTES = 8 * 1024 * 1024
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


@dataclass(frozen=True)
class RemoteCandidate:
    session_id: str
    layout: str
    locator: str


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
    transformed_roots = {"checkpoints", "entries", "snapshots", "snapshots.git", "steps"}
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


def _download(
    remote: S3Store,
    source: RemoteSource,
    destination: Path,
    progress: ProgressCallback | None = None,
) -> tuple[int, bool]:
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
        with archive_path.open("rb") as archive:
            extracted_digest = safe_extract_tar_zst_stream(
                archive,
                destination,
                progress=_progress_range(progress, 60, 100),
                progress_total=expected_size,
                progress_message="extracting source archive",
            )
        if extracted_digest != source.digest:
            raise ValueError("scratch archive changed between download and extraction")
    else:
        _report(progress, 60, "extracting source archive")
        _safe_extract_tar(archive_path, destination)
        if _file_digest(archive_path) != source.digest:
            raise ValueError("scratch archive changed between download and extraction")
        _report(progress, 100, "extracted source archive")
    had_bundle = (destination / "snapshots.bundle").is_file()
    return downloaded_size, had_bundle


def _store(root: Path) -> SessionStore:
    return SessionStore(
        StoragePaths(
            root.parent,
            archive=root.parent,
            runtime=root.parent / "runtime-verify",
            spool=root.parent / "spool-verify",
        )
    )


def _verify_replacement(
    root: Path,
    result: UpgradeResult,
    expected_preserved: dict[str, tuple[int, str]],
    progress: ProgressCallback | None = None,
) -> None:
    _report(progress, 0, "checking replacement metadata")
    remote_sessions._restore_snapshot_bundle(root, result.session.session_id)
    session = DirectorySession.load(root / "session.json")
    if session != result.session:
        raise ValueError("replacement session metadata does not match the upgrade")
    manifests = _store(root).steps(session.session_id)
    repository = GitSnapshotStore(root / "snapshots.git")
    trees = [repository.tree_id(manifest.snapshot_commit or "") for manifest in manifests]
    if trees != result.tree_ids:
        raise ValueError("replacement filesystem content does not match the source")
    if [manifest.entries for manifest in manifests] != result.entries:
        raise ValueError("replacement snapshot metadata does not match the source")
    total_snapshots = max(len(manifests), 1)
    for index, (manifest, expected_snapshot) in enumerate(
        zip(manifests, result.snapshots, strict=True), start=1
    ):
        with tempfile.TemporaryDirectory(prefix="verify-snapshot-", dir=root.parent) as name:
            restored = Path(name) / "tree"
            _store(root).restore_manifest(session.session_id, manifest, restored)
            if snapshot_fingerprint(restored) != expected_snapshot:
                raise ValueError("replacement filesystem bytes do not match the source")
        if progress is not None:
            progress(index, total_snapshots, f"verified snapshot {index}/{total_snapshots}")
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
) -> Replacement:
    extracted = work / source.session_id
    extracted.mkdir()
    original_size, had_bundle = _download(
        remote,
        source,
        extracted,
        progress=_progress_range(progress, 0, 45),
    )
    _report(progress, 48, "fingerprinting source data")
    expected_preserved = _preserved_files(extracted)
    _report(progress, 52, "upgrading session in scratch")
    result = upgrade_session(
        extracted,
        source.session_id,
        source.origin,
        transport_is_current=source.candidate.layout == "content-addressed",
        archive_had_bundle=had_bundle,
        expected_step=source.step,
        remote_complete=source.completion_key is not None,
    )
    prepared = prepare_generation(
        _store(extracted),
        result.session,
        progress=_progress_range(progress, 60, 70),
    )
    try:
        verified = work / "verified" / source.session_id
        verified.mkdir(parents=True)
        with prepared.path.open("rb") as archive:
            digest = safe_extract_tar_zst_stream(
                archive,
                verified,
                progress=_progress_range(progress, 70, 80),
                progress_total=prepared.size_bytes,
                progress_message="extracting prepared replacement",
            )
        if digest != prepared.digest:
            raise ValueError("locally prepared replacement checksum mismatch")
        _verify_replacement(
            verified,
            result,
            expected_preserved,
            progress=_progress_range(progress, 80, 100),
        )
        _report(progress, 100, "replacement verified locally")
        return Replacement(source, result.session, prepared, original_size, result.source_format)
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

        for key in source.cleanup_keys:
            if key != source.object_key:
                remote.remove(key)
        # The source archive is always the final destructive operation.
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
) -> S3RecompressionSummary:
    """Detect and upgrade every indexed historical S3 session format."""
    config = config or S3Config.discover(required=True)
    assert config is not None
    remote = client if isinstance(client, S3Store) else S3Store(config, client)
    if progress is not None:
        progress(0, 1, "discovering indexed S3 sessions")
    candidates = discover_remote_candidates(remote, config)
    summary = S3RecompressionSummary(sources=len(candidates))
    if not candidates:
        if progress is not None:
            progress(1, 1, "no indexed S3 sessions found")
        return summary
    scratch = _scratch_directory(scratch_dir)
    progress_total = len(candidates) * 1000
    for position, candidate in enumerate(candidates, start=1):
        prepared: PreparedGeneration | None = None

        def session_progress(completed: int, total: int, message: str) -> None:
            if progress is None:
                return
            fraction = max(0.0, min(completed / max(total, 1), 1.0))
            overall = ((position - 1) * 1000) + round(fraction * 1000)
            progress(
                overall,
                progress_total,
                f"({position}/{len(candidates)}) {candidate.session_id} {message}",
            )

        session_progress(0, 1, "checking eligibility")
        try:
            source = source_for_candidate(remote, config, candidate)
            with tempfile.TemporaryDirectory(
                prefix=f"memo-s3-upgrade-{candidate.session_id}-",
                dir=scratch,
            ) as work_name:
                if progress is None:
                    replacement = _prepare_replacement(remote, source, Path(work_name))
                else:
                    prepare_progress = (
                        session_progress if dry_run else _progress_range(session_progress, 0, 70)
                    )
                    replacement = _prepare_replacement(
                        remote,
                        source,
                        Path(work_name),
                        progress=prepare_progress,
                    )
                prepared = replacement.prepared
                summary.formats[candidate.session_id] = replacement.source_format
                summary.original_bytes += replacement.original_size
                summary.replacement_bytes += replacement.prepared.size_bytes
                if not dry_run:
                    if progress is None:
                        _install_replacement(remote, config, replacement)
                    else:
                        _install_replacement(
                            remote,
                            config,
                            replacement,
                            progress=_progress_range(session_progress, 70, 100),
                        )
                summary.migrated.append(candidate.session_id)
        except (AlreadyLatest, NotEligible) as error:
            summary.skipped.append((candidate.session_id, str(error)))
        except Exception as error:
            summary.failed.append((candidate.session_id, str(error)))
        finally:
            if prepared is not None:
                prepared.cleanup()
            session_progress(1, 1, "finished")
    return summary


# The old public name remains import-compatible while the CLI moves to upgrade language.
upgrade_s3 = recompress_s3

# Tests and downstream callers used the original private type names.
_RemoteSource = RemoteSource
_Replacement = Replacement
