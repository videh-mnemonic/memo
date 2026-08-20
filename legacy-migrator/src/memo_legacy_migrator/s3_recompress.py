"""Recompress pre-Git remote session generations without risking source data."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from contextlib import suppress
from dataclasses import dataclass, field, replace
from pathlib import Path

from memo.recording.filesystem import atomic_write
from memo.recording.git_snapshots import GitSnapshotStore
from memo.recording.metadata import STEP_SCHEMA_VERSION, DirectorySession, StepManifest
from memo.recording.paths import StoragePaths
from memo.recording.store import SessionStore
from memo.transport import remote_sessions
from memo.transport.archive import (
    PreparedGeneration,
    prepare_generation,
    safe_extract_tar_zst_stream,
)
from memo.transport.config import S3Config
from memo.transport.s3 import STREAM_READ_SIZE, S3Store


@dataclass
class S3RecompressionSummary:
    sources: int = 0
    migrated: list[str] = field(default_factory=list)
    skipped: list[tuple[str, str]] = field(default_factory=list)
    failed: list[tuple[str, str]] = field(default_factory=list)
    original_bytes: int = 0
    replacement_bytes: int = 0


@dataclass(frozen=True)
class _RemoteSource:
    session_id: str
    step: int
    object_key: str
    digest: str
    completion_key: str | None
    completion_data: bytes | None


@dataclass
class _Replacement:
    source: _RemoteSource
    session: DirectorySession
    prepared: PreparedGeneration
    original_size: int


class _NotEligible(ValueError):
    pass


class _AlreadyCompressed(_NotEligible):
    pass


class _NotSmaller(_NotEligible):
    pass


def _json_bytes(value: dict[str, object]) -> bytes:
    return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()


def _file_digest(path: Path) -> str:
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def _stream_digest(remote: S3Store, key: str) -> tuple[str, int]:
    body = remote.open(key)
    digest = hashlib.sha256()
    size = 0
    try:
        while chunk := body.read(STREAM_READ_SIZE):
            digest.update(chunk)
            size += len(chunk)
    finally:
        remote.close(body)
    return digest.hexdigest(), size


def _preserved_files(root: Path) -> dict[str, tuple[str, int, str | None]]:
    """Fingerprint archive data that the snapshot conversion must not touch."""
    result: dict[str, tuple[str, int, str | None]] = {}
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root)
        transformed = (
            relative.as_posix() == "HEAD"
            or relative.parts[0] in {"snapshots", "snapshots.git"}
            or (
                len(relative.parts) == 2
                and relative.parts[0] == "steps"
                and relative.suffix == ".json"
                and relative.stem.isdigit()
            )
        )
        if transformed:
            continue
        if path.is_file():
            result[relative.as_posix()] = (
                "file",
                path.stat().st_mode & 0o777,
                _file_digest(path),
            )
        elif path.is_dir():
            result[relative.as_posix()] = (
                "directory",
                path.stat().st_mode & 0o777,
                None,
            )
    return result


def _source_for_session(remote: S3Store, config: S3Config, session_id: str) -> _RemoteSource:
    origin = remote_sessions._load_index(remote, config, session_id)
    base = remote_sessions._session_base(config, origin, session_id)
    step, object_key, digest, complete = remote_sessions._select_generation(
        remote, config, base, session_id
    )
    if not complete:
        raise _NotEligible("remote session is not complete")
    generations = remote_sessions._list_generations(remote, f"{base}/generations/")
    if not generations or max(generations) != step:
        raise _NotEligible("selected archive is not the latest remote generation")
    completion_key = remote_sessions._completion_key(base, step, digest)
    completion_data = remote.read_bytes(completion_key)
    return _RemoteSource(
        session_id, step, object_key, digest, completion_key, completion_data
    )


def _download(remote: S3Store, source: _RemoteSource, destination: Path) -> int:
    expected_size = remote.size(source.object_key)
    body = remote.open(source.object_key)
    archive_path = destination.parent / f".{source.session_id}.original.tar.zst"
    digest = hashlib.sha256()
    downloaded_size = 0
    try:
        with archive_path.open("wb") as archive:
            while chunk := body.read(STREAM_READ_SIZE):
                archive.write(chunk)
                digest.update(chunk)
                downloaded_size += len(chunk)
            archive.flush()
            os.fsync(archive.fileno())
    finally:
        remote.close(body)
    actual_digest = digest.hexdigest()
    if actual_digest != source.digest:
        raise ValueError(
            f"download checksum mismatch: expected {source.digest}, got {actual_digest}"
        )
    if expected_size is None or downloaded_size != expected_size:
        raise ValueError(
            f"download size mismatch: expected {expected_size}, got {downloaded_size}"
        )
    with archive_path.open("rb") as archive:
        extracted_digest = safe_extract_tar_zst_stream(archive, destination)
    if extracted_digest != actual_digest:
        raise ValueError("scratch archive changed between download and extraction")
    archive_path.unlink()
    return downloaded_size


def _convert_snapshots(session_root: Path, session_id: str) -> list[str]:
    paths = StoragePaths(
        session_root.parent,
        archive=session_root.parent,
        runtime=session_root.parent / "runtime",
        spool=session_root.parent / "spool",
    )
    store = SessionStore(paths)
    manifests = store.steps(session_id)
    if not manifests:
        raise ValueError("downloaded session has no published steps")
    if all(manifest.snapshot_commit for manifest in manifests):
        raise _AlreadyCompressed("filesystem snapshots already use Git storage")
    if any(manifest.snapshot_commit for manifest in manifests):
        raise ValueError("session mixes directory and Git-backed snapshots")

    repository = GitSnapshotStore(session_root / "snapshots.git")
    expected_trees: list[str] = []
    parent: str | None = None
    converted: list[StepManifest] = []
    for manifest in manifests:
        snapshot = session_root / manifest.snapshot
        tree_id = repository.write_tree(snapshot)
        commit = repository.commit_tree(
            tree_id, parent, f"Memo filesystem snapshot {manifest.step}"
        )
        expected_trees.append(tree_id)
        converted_manifest = replace(
            manifest,
            schema_version=STEP_SCHEMA_VERSION,
            snapshot_commit=commit,
        )
        atomic_write(
            session_root / "steps" / f"{manifest.step}.json",
            _json_bytes(converted_manifest.to_dict()),
        )
        converted.append(converted_manifest)
        parent = commit

    # A higher generation avoids a same-step fork while the verified replacement
    # and the original coexist in object storage.
    head = converted[-1]
    boundary = replace(
        head,
        step=head.step + 1,
        snapshot=f"snapshots/{head.step + 1}",
    )
    atomic_write(
        session_root / "steps" / f"{boundary.step}.json",
        _json_bytes(boundary.to_dict()),
    )
    atomic_write(session_root / "HEAD", f"{boundary.step}\n".encode())
    expected_trees.append(expected_trees[-1])
    return expected_trees


def _verify_replacement(
    replacement_root: Path,
    session_id: str,
    expected_trees: list[str],
    expected_preserved: dict[str, tuple[str, int, str | None]],
) -> None:
    paths = StoragePaths(
        replacement_root.parent,
        archive=replacement_root.parent,
        runtime=replacement_root.parent / "runtime-verify",
        spool=replacement_root.parent / "spool-verify",
    )
    store = SessionStore(paths)
    manifests = store.steps(session_id)
    if len(manifests) != len(expected_trees):
        raise ValueError("replacement step history does not match the source")
    repository = GitSnapshotStore(replacement_root / "snapshots.git")
    actual_trees = [
        repository.tree_id(manifest.snapshot_commit or "") for manifest in manifests
    ]
    if actual_trees != expected_trees:
        raise ValueError("replacement filesystem content does not match the source")
    if _preserved_files(replacement_root) != expected_preserved:
        raise ValueError("replacement changed non-snapshot session data")


def _prepare_replacement(
    remote: S3Store, source: _RemoteSource, work: Path
) -> _Replacement:
    extracted = work / source.session_id
    extracted.mkdir()
    original_size = _download(remote, source, extracted)
    expected_preserved = _preserved_files(extracted)
    expected_trees = _convert_snapshots(extracted, source.session_id)
    session = DirectorySession.load(extracted / "session.json")
    paths = StoragePaths(
        work,
        archive=work,
        runtime=work / "runtime",
        spool=work / "spool",
    )
    prepared = prepare_generation(SessionStore(paths), session)
    try:
        if prepared.step != source.step + 1:
            raise ValueError("replacement generation did not advance exactly one step")
        verified = work / "verified" / source.session_id
        verified.mkdir(parents=True)
        with prepared.path.open("rb") as archive:
            digest = safe_extract_tar_zst_stream(archive, verified)
        if digest != prepared.digest:
            raise ValueError("locally prepared replacement checksum mismatch")
        _verify_replacement(verified, source.session_id, expected_trees, expected_preserved)
        if prepared.size_bytes >= original_size:
            raise _NotSmaller(
                f"verified replacement is not smaller ({original_size} -> "
                f"{prepared.size_bytes} bytes)"
            )
        return _Replacement(source, session, prepared, original_size)
    except BaseException:
        prepared.cleanup()
        raise


def _completion_data(replacement: _Replacement, generation: str) -> bytes:
    return remote_sessions._canonical_json(
        {
            "schema_version": 1,
            "session_id": replacement.source.session_id,
            "final_step": replacement.prepared.step,
            "generation": generation,
            "sha256": replacement.prepared.digest,
        }
    )


def _install_replacement(
    remote: S3Store, config: S3Config, replacement: _Replacement
) -> None:
    source = replacement.source
    base = source.object_key.rsplit("/generations/", 1)[0]
    generation = remote_sessions._generation_key(
        base, replacement.prepared.step, replacement.prepared.digest
    )
    staging = f"{base}/migration-staging/{replacement.prepared.path.name}"
    if remote.exists(staging) or remote.exists(generation):
        raise FileExistsError("replacement or staging object already exists")

    # The source object and its completion record remain untouched throughout
    # candidate upload and byte-for-byte remote verification.
    remote.upload_file(staging, replacement.prepared.path)
    try:
        staged_digest, staged_size = _stream_digest(remote, staging)
        if (
            staged_digest != replacement.prepared.digest
            or staged_size != replacement.prepared.size_bytes
        ):
            raise ValueError("staged replacement is not byte-identical to the local candidate")

        if _source_for_session(remote, config, source.session_id) != source:
            raise ValueError("remote session changed while its replacement was prepared")

        remote.upload_file(generation, replacement.prepared.path)
        final_digest, final_size = _stream_digest(remote, generation)
        if (
            final_digest != replacement.prepared.digest
            or final_size != replacement.prepared.size_bytes
        ):
            remote.remove(generation)
            raise ValueError("uploaded replacement is not byte-identical to the local candidate")
        try:
            generations = remote_sessions._list_generations(
                remote, f"{base}/generations/"
            )
        except BaseException:
            remote.remove(generation)
            raise
        if (
            generations.get(replacement.prepared.step)
            != (generation, replacement.prepared.digest)
            or max(generations) != replacement.prepared.step
        ):
            remote.remove(generation)
            raise ValueError("remote session advanced while the replacement was uploaded")

        assert source.completion_key is not None
        assert source.completion_data is not None
        new_completion_key = remote_sessions._completion_key(
            base, replacement.prepared.step, replacement.prepared.digest
        )
        remote.remove(source.completion_key)
        try:
            remote.put_bytes(new_completion_key, _completion_data(replacement, generation))
        except BaseException:
            try:
                remote.put_bytes(source.completion_key, source.completion_data)
            finally:
                with suppress(Exception):
                    remote.remove(generation)
            raise

        selected = remote_sessions._select_generation(
            remote, config, base, source.session_id
        )
        if selected[0:3] != (
            replacement.prepared.step,
            generation,
            replacement.prepared.digest,
        ):
            raise ValueError("replacement was uploaded but is not the selected generation")

        # This is the first operation that removes original session data.
        remote.remove(source.object_key)
    finally:
        with suppress(Exception):
            remote.remove(staging)


def recompress_s3(
    config: S3Config | None = None,
    client: object | None = None,
    *,
    dry_run: bool = False,
) -> S3RecompressionSummary:
    """Find, verify, Git-compress, and safely replace legacy S3 generations."""
    config = config or S3Config.discover(required=True)
    assert config is not None
    remote = client if isinstance(client, S3Store) else S3Store(config, client)
    summary = S3RecompressionSummary()
    session_ids = remote_sessions.list_archived_session_ids(config, remote)
    summary.sources = len(session_ids)
    for session_id in session_ids:
        prepared: PreparedGeneration | None = None
        try:
            source = _source_for_session(remote, config, session_id)
            with tempfile.TemporaryDirectory(prefix="memo-s3-recompress-") as work_name:
                replacement = _prepare_replacement(remote, source, Path(work_name))
                prepared = replacement.prepared
                summary.original_bytes += replacement.original_size
                summary.replacement_bytes += replacement.prepared.size_bytes
                if not dry_run:
                    _install_replacement(remote, config, replacement)
                summary.migrated.append(session_id)
        except _NotEligible as error:
            summary.skipped.append((session_id, str(error)))
        except Exception as error:
            summary.failed.append((session_id, str(error)))
        finally:
            if prepared is not None:
                prepared.cleanup()
    return summary
