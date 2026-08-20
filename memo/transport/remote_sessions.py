"""Publish, discover, inspect, and restore Memo sessions in object storage."""

from __future__ import annotations

import hashlib
import json
import re
import tarfile
import tempfile
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import quote

import zstandard

from ..agents.run_metadata import AgentRunMetadata
from ..recording.filesystem import atomic_write
from ..recording.metadata import DirectorySession, SessionOrigin
from ..recording.paths import StoragePaths
from ..recording.store import SessionNotFoundError, SessionStore, validate_session_id
from .archive import (
    PreparedGeneration,
    atomic_install_directory,
    enforce_archive_limit,
    prepare_generation,
    safe_extract_tar_zst_stream,
)
from .config import S3Config
from .s3 import METADATA_SIZE_LIMIT, S3Store

GENERATION_NAME = re.compile(r"^(\d{8,})-([0-9a-f]{64})\.tar\.zst$")
COMPLETION_NAME = re.compile(r"^(\d{8,})-([0-9a-f]{64})\.json$")
INDEX_NAME = re.compile(r"^([0-9a-f]{64})\.json$")
MAX_PULL_WORKERS = 4
ProgressCallback = Callable[[int, int, str], None]


@dataclass
class PushSummary:
    pushed: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    failed: list[tuple[str, str]] = field(default_factory=list)


@dataclass
class PullSummary:
    pulled: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    failed: list[tuple[str, str]] = field(default_factory=list)


def _store(config: S3Config, client: Any | None) -> S3Store:
    return client if isinstance(client, S3Store) else S3Store(config, client)


def _key(config: S3Config, *parts: object) -> str:
    suffix = "/".join(str(part).strip("/") for part in parts)
    return f"{config.prefix}/{suffix}" if config.prefix else suffix


def _component(value: str) -> str:
    return quote(value, safe="")


def _canonical_json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def _valid_digest(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _session_base(config: S3Config, origin: SessionOrigin | dict[str, str], session_id: str) -> str:
    username = origin.username if isinstance(origin, SessionOrigin) else origin["username"]
    hostname = origin.hostname if isinstance(origin, SessionOrigin) else origin["hostname"]
    return _key(
        config,
        _component(username),
        _component(hostname),
        "sessions",
        session_id,
    )


def _generation_key(base: str, step: int, digest: str) -> str:
    return f"{base}/generations/{step:08d}-{digest}.tar.zst"


def _completion_key(base: str, step: int, digest: str) -> str:
    return f"{base}/completions/{step:08d}-{digest}.json"


def _index_value(session: DirectorySession) -> dict[str, object]:
    return {
        "schema_version": 1,
        "session_id": session.session_id,
        "memo_version_id": session.origin.memo_version_id,
        "username": session.origin.username,
        "hostname": session.origin.hostname,
    }


def _index_record(config: S3Config, session: DirectorySession) -> tuple[str, bytes]:
    data = _canonical_json(_index_value(session))
    digest = hashlib.sha256(data).hexdigest()
    return _key(config, "index", "sessions", session.session_id, f"{digest}.json"), data


def _validate_index(index: object, session_id: str) -> dict[str, str]:
    if (
        not isinstance(index, dict)
        or index.get("schema_version") != 1
        or index.get("session_id") != session_id
    ):
        raise ValueError("remote session index is invalid")
    if any(
        not isinstance(index.get(key), str) or not index.get(key)
        for key in ("memo_version_id", "username", "hostname")
    ):
        raise ValueError("remote session index has invalid origin")
    return index  # type: ignore[return-value]


def _load_index(store: S3Store, config: S3Config, session_id: str) -> dict[str, str]:
    prefix = _key(config, "index", "sessions", session_id) + "/"
    records: list[dict[str, str]] = []
    for key in store.list(prefix):
        relative = key[len(prefix) :]
        match = INDEX_NAME.fullmatch(relative)
        if match is None:
            continue
        data = store.read_bytes(key)
        if hashlib.sha256(data).hexdigest() != match.group(1):
            raise ValueError("remote session index checksum is invalid")
        records.append(_validate_index(json.loads(data), session_id))
    if not records:
        raise FileNotFoundError(f"remote session not found: {session_id}")
    if len(records) != 1:
        raise ValueError(f"remote session index conflict: {session_id}")
    return records[0]


def _list_generations(store: S3Store, prefix: str) -> dict[int, tuple[str, str]]:
    generations: dict[int, tuple[str, str]] = {}
    for key in store.list(prefix):
        match = GENERATION_NAME.fullmatch(key[len(prefix) :])
        if match is None:
            continue
        step, digest = int(match.group(1)), match.group(2)
        previous = generations.get(step)
        if previous is not None and previous[1] != digest:
            raise ValueError(f"remote generation fork at step {step}")
        generations[step] = (key, digest)
    return generations


def _list_completions(store: S3Store, prefix: str) -> list[tuple[int, str, str]]:
    completions: list[tuple[int, str, str]] = []
    for key in store.list(prefix):
        match = COMPLETION_NAME.fullmatch(key[len(prefix) :])
        if match is not None:
            completions.append((int(match.group(1)), match.group(2), key))
    if len(completions) > 1:
        raise ValueError("remote session has conflicting completion records")
    return completions


def _select_generation(
    store: S3Store, config: S3Config, base: str, session_id: str
) -> tuple[int, str, str, bool]:
    generation_prefix = f"{base}/generations/"
    generations = _list_generations(store, generation_prefix)
    completions = _list_completions(store, f"{base}/completions/")
    if not completions:
        if not generations:
            raise FileNotFoundError(f"remote session has no complete generation: {session_id}")
        step = max(generations)
        key, digest = generations[step]
        return step, key, digest, False
    step, digest, completion_key = completions[0]
    generation = generations.get(step)
    expected_key = _generation_key(base, step, digest)
    if generation != (expected_key, digest):
        raise ValueError("remote completion record references a missing generation")
    completion = json.loads(store.read_bytes(completion_key))
    if (
        not isinstance(completion, dict)
        or completion.get("schema_version") != 1
        or completion.get("session_id") != session_id
        or completion.get("final_step") != step
        or completion.get("generation") != expected_key
        or completion.get("sha256") != digest
    ):
        raise ValueError("remote completion record is invalid")
    return step, expected_key, digest, True


def publish_generation(
    store: SessionStore,
    session: DirectorySession,
    prepared: PreparedGeneration,
    config: S3Config,
    client: Any | None = None,
    *,
    update_local: bool = True,
    allow_large: bool = False,
    progress: ProgressCallback | None = None,
) -> dict[str, object]:
    """Publish an append-only generation and its discovery records."""
    remote = _store(config, client)
    enforce_archive_limit(prepared, allow_large)
    base = _session_base(config, session.origin, session.session_id)
    generation_prefix = f"{base}/generations/"
    generation = _generation_key(base, prepared.step, prepared.digest)
    existing = _list_generations(remote, generation_prefix).get(prepared.step)
    if existing is not None and existing != (generation, prepared.digest):
        raise ValueError(f"remote generation fork at step {prepared.step}")
    if not remote.exists(generation):
        if progress is not None:
            progress(0, prepared.size_bytes, "uploading archive")
        remote.upload_file(generation, prepared.path, progress=progress)
        if progress is not None:
            progress(prepared.size_bytes, prepared.size_bytes, "upload complete")
    remote_size = remote.size(generation)
    if remote_size != prepared.size_bytes:
        raise ValueError(
            f"remote generation size mismatch: expected {prepared.size_bytes}, "
            f"received {remote_size}"
        )
    return publish_generation_metadata(
        store,
        session,
        prepared.step,
        prepared.digest,
        generation,
        config,
        remote,
        update_local=update_local,
    )


def publish_generation_metadata(
    store: SessionStore,
    session: DirectorySession,
    step: int,
    digest: str,
    generation: str,
    config: S3Config,
    client: Any | None = None,
    *,
    update_local: bool = True,
) -> dict[str, object]:
    """Publish mutable discovery/completion records for an existing generation."""
    remote = _store(config, client)
    index_key, index_data = _index_record(config, session)
    index_prefix = _key(config, "index", "sessions", session.session_id) + "/"
    conflicting_indexes = [key for key in remote.list(index_prefix) if key != index_key]
    if conflicting_indexes:
        raise ValueError(f"remote session index conflict: {session.session_id}")
    remote.put_bytes(index_key, index_data)

    if session.state == "complete":
        base = _session_base(config, session.origin, session.session_id)
        completion_key = _completion_key(base, step, digest)
        existing_completions = _list_completions(remote, f"{base}/completions/")
        if existing_completions and existing_completions[0][2] != completion_key:
            raise ValueError("remote session has conflicting completion records")
        completion = _canonical_json(
            {
                "schema_version": 1,
                "session_id": session.session_id,
                "final_step": step,
                "generation": generation,
                "sha256": digest,
            }
        )
        remote.put_bytes(completion_key, completion)

    if update_local:
        session.last_pushed_step = step
        session.last_pushed_digest = digest
        session.remote_object = generation
        store.amend_session(
            session.session_id,
            last_pushed_step=step,
            last_pushed_digest=digest,
            remote_object=generation,
        )
    return {
        "session_id": session.session_id,
        "step": step,
        "digest": digest,
        "object": generation,
        "status": "pushed",
    }


def push_session(
    store: SessionStore,
    session: DirectorySession,
    config: S3Config,
    client: Any | None = None,
    *,
    allow_large: bool = False,
    progress: ProgressCallback | None = None,
) -> dict[str, object]:
    """Package and publish a session unless its current step was already pushed."""
    manifest = store.head(session.session_id)
    if manifest is None:
        raise ValueError(f"session has no published step: {session.session_id}")
    if session.last_pushed_step == manifest.step:
        if (
            session.state == "complete"
            and session.last_pushed_digest is not None
            and session.remote_object is not None
        ):
            remote = _store(config, client)
            base = _session_base(config, session.origin, session.session_id)
            completion_key = _completion_key(base, manifest.step, session.last_pushed_digest)
            existing = _list_completions(remote, f"{base}/completions/")
            if not existing:
                return publish_generation_metadata(
                    store,
                    session,
                    manifest.step,
                    session.last_pushed_digest,
                    session.remote_object,
                    config,
                    remote,
                )
            if existing[0][2] != completion_key:
                raise ValueError("remote session has conflicting completion records")
        return {
            "session_id": session.session_id,
            "step": manifest.step,
            "digest": session.last_pushed_digest,
            "status": "skipped",
        }
    prepared = prepare_generation(store, session, progress=progress)
    try:
        return publish_generation(
            store,
            session,
            prepared,
            config,
            client,
            allow_large=allow_large,
            progress=progress,
        )
    finally:
        prepared.cleanup()


def list_archived_session_ids(
    config: S3Config | None = None, client: Any | None = None
) -> list[str]:
    """List session IDs advertised by the remote archive index."""
    config = config or S3Config.discover(required=True)
    assert config is not None
    remote = _store(config, client)
    prefix = _key(config, "index", "sessions") + "/"
    session_ids: set[str] = set()
    for key in remote.list(prefix):
        remainder = key[len(prefix) :]
        parts = remainder.split("/")
        if len(parts) != 2 or INDEX_NAME.fullmatch(parts[1]) is None:
            continue
        try:
            session_ids.add(validate_session_id(parts[0]))
        except ValueError:
            continue
    return sorted(session_ids)


def _same_origin_remote_session_ids(
    origin: SessionOrigin, config: S3Config, store: S3Store
) -> list[str]:
    prefix = (
        _key(
            config,
            _component(origin.username),
            _component(origin.hostname),
            "sessions",
        )
        + "/"
    )
    session_ids: set[str] = set()
    for key in store.list(prefix):
        session_id = key[len(prefix) :].split("/", 1)[0]
        try:
            session_ids.add(validate_session_id(session_id))
        except ValueError:
            continue
    return sorted(session_ids)


def _stream_agent_run_metadata(store: S3Store, generation: str) -> list[AgentRunMetadata]:
    """Read agent-run metadata from the beginning of an archive stream."""
    body = store.open(generation)
    reader = zstandard.ZstdDecompressor().stream_reader(body, closefd=False)
    result: list[AgentRunMetadata] = []
    try:
        with tarfile.open(fileobj=reader, mode="r|") as archive:
            for member in archive:
                name = member.name.lstrip("./")
                if member.isfile() and name.startswith("agents/runs/") and name.endswith(".json"):
                    extracted = archive.extractfile(member)
                    if extracted is None or member.size > METADATA_SIZE_LIMIT:
                        raise ValueError("remote agent metadata is invalid")
                    value = json.loads(extracted.read(METADATA_SIZE_LIMIT + 1))
                    if not isinstance(value, dict):
                        raise ValueError("remote agent metadata is invalid")
                    result.append(AgentRunMetadata.from_dict(value))
                elif (
                    name.startswith("agents/traces/")
                    or name.startswith("snapshots/")
                    or name == "snapshots.git"
                    or name.startswith("snapshots.git/")
                ):
                    break
    finally:
        try:
            reader.close()
        finally:
            store.close(body)
    return result


def inspect_archived_agent_runs(
    origin: SessionOrigin, config: S3Config | None = None, client: Any | None = None
) -> tuple[list[dict[str, object]], set[str]]:
    """Inspect same-origin agent metadata without downloading filesystem snapshots."""
    config = config or S3Config.discover(required=True)
    assert config is not None
    remote = _store(config, client)
    session_ids = set(_same_origin_remote_session_ids(origin, config, remote))
    runs: list[dict[str, object]] = []
    for session_id in sorted(session_ids):
        base = _session_base(config, origin, session_id)
        try:
            complete = bool(_list_completions(remote, f"{base}/completions/"))
            _, generation, _, _ = _select_generation(remote, config, base, session_id)
        except FileNotFoundError:
            continue
        for metadata in _stream_agent_run_metadata(remote, generation):
            runs.append(
                {
                    "session_id": session_id,
                    "capture_scope": ("agent-only" if metadata.imported_agent_only else "partial"),
                    "harness": metadata.harness,
                    "native_id": metadata.agent_session_id,
                    "complete_size": metadata.trace_complete_size,
                    "digest": metadata.trace_digest,
                    "state": "complete" if complete else "active",
                    "continued_from_session_id": metadata.continued_from_session_id,
                    "continued_from_trace_size": metadata.continued_from_trace_size,
                    "continued_from_trace_digest": metadata.continued_from_trace_digest,
                }
            )
    return runs, session_ids


def ensure_local_session(
    session_id: str,
    paths: StoragePaths | None = None,
    config: S3Config | None = None,
    client: Any | None = None,
) -> Path:
    """Return a local session, pulling it from the archive when absent."""
    session_id = validate_session_id(session_id)
    paths = paths or StoragePaths.discover()
    store = SessionStore(paths)
    try:
        location, _ = store.find(session_id)
        return location
    except SessionNotFoundError:
        return pull_session(session_id, paths, config, client=client)


def pull_session(
    session_id: str,
    paths: StoragePaths | None = None,
    config: S3Config | None = None,
    force: bool = False,
    client: Any | None = None,
    progress: ProgressCallback | None = None,
) -> Path:
    """Download, verify, and atomically install a remote session."""
    session_id = validate_session_id(session_id)
    if progress is not None:
        progress(0, 100, f"resolving remote session {session_id}")
    paths = paths or StoragePaths.discover()
    config = config or S3Config.discover(required=True)
    assert config is not None
    remote = _store(config, client)
    origin = _load_index(remote, config, session_id)
    base = _session_base(config, origin, session_id)
    step, object_key, digest, complete = _select_generation(remote, config, base, session_id)
    if progress is not None:
        progress(10, 100, f"checking local archive for {session_id}")
    store = SessionStore(paths)
    destination = store.session_path(session_id)
    if destination.exists() and not force:
        local = store.head(session_id)
        if local and local.step >= step:
            raise FileExistsError(f"local step {local.step} is not older than remote step {step}")
        raise FileExistsError(f"local session exists: {session_id}; use --force to replace it")
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=f".{session_id}.pull-", dir=destination.parent
    ) as temporary_name:
        temporary = Path(temporary_name)
        object_size = remote.size(object_key) if progress is not None else None

        def download_progress(completed: int, total: int, message: str) -> None:
            if progress is None:
                return
            del message
            overall = 15 + int((min(completed, total) / max(total, 1)) * 70)
            progress(overall, 100, f"downloading {session_id}")

        body = remote.open(object_key)
        try:
            actual_digest = safe_extract_tar_zst_stream(
                body,
                temporary,
                progress=download_progress if object_size is not None else None,
                progress_total=object_size,
                progress_message=f"downloading {session_id}",
            )
        finally:
            remote.close(body)
        if progress is not None:
            progress(88, 100, f"validating {session_id}")
        if actual_digest != digest:
            raise ValueError(f"checksum mismatch: expected {digest}, got {actual_digest}")
        pulled = DirectorySession.load(temporary / "session.json")
        manifests = SessionStore._validate_history(temporary, session_id)
        if not manifests:
            raise ValueError("downloaded session has no published steps")
        manifest = manifests[-1]
        if (
            pulled.session_id != session_id
            or pulled.origin.memo_version_id != origin["memo_version_id"]
            or pulled.origin.username != origin["username"]
            or pulled.origin.hostname != origin["hostname"]
            or manifest.session_id != session_id
            or manifest.step != step
        ):
            raise ValueError("downloaded session does not match remote generation")
        pulled.last_pushed_step = manifest.step
        pulled.last_pushed_digest = digest
        pulled.remote_object = object_key
        if complete:
            pulled.state = "complete"
        atomic_write(
            temporary / "session.json",
            (json.dumps(pulled.to_dict(), indent=2, sort_keys=True) + "\n").encode(),
        )
        if progress is not None:
            progress(96, 100, f"installing {session_id}")
        atomic_install_directory(temporary, destination, force=force)
    if progress is not None:
        progress(100, 100, f"pulled {session_id}")
    return destination


def verify_archived_session(
    session_id: str,
    paths: StoragePaths | None = None,
    config: S3Config | None = None,
    client: Any | None = None,
    progress: ProgressCallback | None = None,
) -> dict[str, object]:
    """Read an archived generation back and confirm it is a restorable recording.

    A successful push records that bytes were stored, not that those bytes
    contain everything the steps reference. Reading the generation back and
    validating it the way a pull would is the only way to learn that before
    someone needs it.
    """
    session_id = validate_session_id(session_id)
    config = config or S3Config.discover(required=True)
    assert config is not None
    remote = _store(config, client)
    origin = _load_index(remote, config, session_id)
    base = _session_base(config, origin, session_id)
    step, object_key, digest, complete = _select_generation(remote, config, base, session_id)
    paths = paths or StoragePaths.discover()
    paths.ensure_storage()
    object_size = remote.size(object_key)
    with tempfile.TemporaryDirectory(
        prefix=f".{session_id}.verify-", dir=paths.runtime
    ) as temporary_name:
        temporary = Path(temporary_name)

        def download_progress(completed: int, total: int, message: str) -> None:
            if progress is None:
                return
            del message
            progress(
                int((min(completed, total) / max(total, 1)) * 90), 100, f"reading {session_id}"
            )

        body = remote.open(object_key)
        try:
            actual_digest = safe_extract_tar_zst_stream(
                body,
                temporary,
                progress=download_progress if progress is not None else None,
                progress_total=object_size,
                progress_message=f"reading {session_id}",
            )
        finally:
            remote.close(body)
        if actual_digest != digest:
            raise ValueError(f"checksum mismatch: expected {digest}, got {actual_digest}")
        if progress is not None:
            progress(92, 100, f"validating {session_id}")
        manifests = SessionStore._validate_history(temporary, session_id)
        if not manifests:
            raise ValueError("archived generation has no published steps")
        manifest = manifests[-1]
        if manifest.step != step:
            raise ValueError("archived generation does not reach the advertised step")
    if progress is not None:
        progress(100, 100, f"verified {session_id}")
    return {
        "session_id": session_id,
        "step": step,
        "steps": len(manifests),
        "object": object_key,
        "bytes": object_size,
        "complete": complete,
    }


def pull_all_sessions(
    paths: StoragePaths | None = None,
    config: S3Config | None = None,
    force: bool = False,
    client: Any | None = None,
    progress: ProgressCallback | None = None,
) -> PullSummary:
    """Pull every indexed remote session, continuing after individual failures."""
    paths = paths or StoragePaths.discover()
    config = config or S3Config.discover(required=True)
    assert config is not None
    remote = _store(config, client)
    store = SessionStore(paths)
    summary = PullSummary()
    pending: list[str] = []
    if progress is not None:
        progress(0, 1, "listing remote sessions")
    session_ids = list_archived_session_ids(config, remote)
    total = max(len(session_ids), 1)
    completed = 0
    for session_id in session_ids:
        if store.session_path(session_id).exists() and not force:
            summary.skipped.append(session_id)
            completed += 1
            if progress is not None:
                progress(completed, total, f"skipped local session {session_id}")
            continue
        pending.append(session_id)

    def pull_one(session_id: str) -> str | None:
        try:
            pull_session(session_id, paths, config, force=force, client=remote)
        except Exception as error:
            return str(error)
        return None

    with ThreadPoolExecutor(max_workers=MAX_PULL_WORKERS) as executor:
        for session_id, error in zip(pending, executor.map(pull_one, pending), strict=True):
            completed += 1
            if error is None:
                summary.pulled.append(session_id)
                message = f"pulled {session_id}"
            else:
                summary.failed.append((session_id, error))
                message = f"failed {session_id}"
            if progress is not None:
                progress(completed, total, message)
    if progress is not None and not session_ids:
        progress(1, 1, "no remote sessions found")
    return summary
