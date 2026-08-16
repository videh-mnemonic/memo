"""Publish, discover, inspect, and restore Memo sessions in object storage."""

from __future__ import annotations

import hashlib
import json
import re
import shutil
import tarfile
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import quote

import zstandard

from ..recording.models import DirectorySession, SessionOrigin
from ..recording.paths import StoragePaths
from ..recording.store import (SessionNotFoundError, SessionStore, atomic_write,
                               validate_session_id)
from .archive import (PreparedGeneration, atomic_install_directory,
                      prepare_generation, safe_extract_tar_zst_stream)
from .config import S3Config
from .s3 import METADATA_SIZE_LIMIT, STREAM_READ_SIZE, S3Store


GENERATION_NAME = re.compile(r"^(\d{8,})-([0-9a-f]{64})\.tar\.zst$")
COMPLETION_NAME = re.compile(r"^(\d{8,})-([0-9a-f]{64})\.json$")
INDEX_NAME = re.compile(r"^([0-9a-f]{64})\.json$")


@dataclass
class PushSummary:
    pushed: list[str] = field(default_factory=list)
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
    return (isinstance(value, str) and len(value) == 64
            and all(character in "0123456789abcdef" for character in value))


def _session_base(config: S3Config, origin: SessionOrigin | dict[str, str],
                  session_id: str) -> str:
    username = origin.username if isinstance(origin, SessionOrigin) else origin["username"]
    hostname = origin.hostname if isinstance(origin, SessionOrigin) else origin["hostname"]
    return _key(
        config, _component(username), _component(hostname), "sessions", session_id,
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
    if (not isinstance(index, dict) or index.get("schema_version") != 1
            or index.get("session_id") != session_id):
        raise ValueError("remote session index is invalid")
    if any(not isinstance(index.get(key), str) or not index.get(key)
           for key in ("memo_version_id", "username", "hostname")):
        raise ValueError("remote session index has invalid origin")
    return index  # type: ignore[return-value]


def _load_index(store: S3Store, config: S3Config, session_id: str) -> dict[str, str]:
    prefix = _key(config, "index", "sessions", session_id) + "/"
    records: list[dict[str, str]] = []
    for key in store.list(prefix):
        relative = key[len(prefix):]
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
        match = GENERATION_NAME.fullmatch(key[len(prefix):])
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
        match = COMPLETION_NAME.fullmatch(key[len(prefix):])
        if match is not None:
            completions.append((int(match.group(1)), match.group(2), key))
    if len(completions) > 1:
        raise ValueError("remote session has conflicting completion records")
    return completions


def _select_generation(store: S3Store, config: S3Config, base: str,
                       session_id: str) -> tuple[int, str, str]:
    generation_prefix = f"{base}/generations/"
    generations = _list_generations(store, generation_prefix)
    completions = _list_completions(store, f"{base}/completions/")
    if not completions:
        if not generations:
            raise FileNotFoundError(f"remote session has no complete generation: {session_id}")
        step = max(generations)
        key, digest = generations[step]
        return step, key, digest
    step, digest, completion_key = completions[0]
    generation = generations.get(step)
    expected_key = _generation_key(base, step, digest)
    if generation != (expected_key, digest):
        raise ValueError("remote completion record references a missing generation")
    completion = json.loads(store.read_bytes(completion_key))
    if (not isinstance(completion, dict) or completion.get("schema_version") != 1
            or completion.get("session_id") != session_id
            or completion.get("final_step") != step
            or completion.get("generation") != expected_key
            or completion.get("sha256") != digest):
        raise ValueError("remote completion record is invalid")
    return step, expected_key, digest


def publish_generation(store: SessionStore, session: DirectorySession,
                       prepared: PreparedGeneration, config: S3Config,
                       client: Any | None = None, *, update_local: bool = True) -> dict[str, object]:
    """Publish an append-only generation and its discovery records."""
    remote = _store(config, client)
    base = _session_base(config, session.origin, session.session_id)
    generation_prefix = f"{base}/generations/"
    generation = _generation_key(base, prepared.step, prepared.digest)
    existing = _list_generations(remote, generation_prefix).get(prepared.step)
    if existing is not None and existing != (generation, prepared.digest):
        raise ValueError(f"remote generation fork at step {prepared.step}")
    if not remote.exists(generation):
        remote.upload_file(generation, prepared.path)

    index_key, index_data = _index_record(config, session)
    index_prefix = _key(config, "index", "sessions", session.session_id) + "/"
    conflicting_indexes = [key for key in remote.list(index_prefix) if key != index_key]
    if conflicting_indexes:
        raise ValueError(f"remote session index conflict: {session.session_id}")
    remote.put_bytes(index_key, index_data)

    if session.state == "complete":
        completion_key = _completion_key(base, prepared.step, prepared.digest)
        existing_completions = _list_completions(remote, f"{base}/completions/")
        if existing_completions and existing_completions[0][2] != completion_key:
            raise ValueError("remote session has conflicting completion records")
        completion = _canonical_json({
            "schema_version": 1,
            "session_id": session.session_id,
            "final_step": prepared.step,
            "generation": generation,
            "sha256": prepared.digest,
        })
        remote.put_bytes(completion_key, completion)

    if update_local:
        session.last_pushed_step = prepared.step
        session.last_pushed_digest = prepared.digest
        session.remote_object = generation
        store.update_session(session)
    return {
        "session_id": session.session_id,
        "step": prepared.step,
        "digest": prepared.digest,
        "object": generation,
        "status": "pushed",
    }


def push_session(store: SessionStore, session: DirectorySession, config: S3Config,
                 client: Any | None = None) -> dict[str, object]:
    """Package and publish a session unless its current step was already pushed."""
    manifest = store.head(session.session_id)
    if manifest is None:
        raise ValueError(f"session has no published step: {session.session_id}")
    if session.last_pushed_step == manifest.step:
        return {
            "session_id": session.session_id,
            "step": manifest.step,
            "digest": session.last_pushed_digest,
            "status": "skipped",
        }
    prepared = prepare_generation(store, session)
    try:
        return publish_generation(store, session, prepared, config, client)
    finally:
        prepared.cleanup()


def list_archived_session_ids(config: S3Config | None = None,
                              client: Any | None = None) -> list[str]:
    """List session IDs advertised by the remote archive index."""
    config = config or S3Config.discover(required=True)
    assert config is not None
    remote = _store(config, client)
    prefix = _key(config, "index", "sessions") + "/"
    session_ids: set[str] = set()
    for key in remote.list(prefix):
        remainder = key[len(prefix):]
        parts = remainder.split("/")
        if len(parts) != 2 or INDEX_NAME.fullmatch(parts[1]) is None:
            continue
        try:
            session_ids.add(validate_session_id(parts[0]))
        except ValueError:
            continue
    return sorted(session_ids)


def _same_origin_remote_session_ids(origin: SessionOrigin, config: S3Config,
                                    store: S3Store) -> list[str]:
    prefix = _key(
        config, _component(origin.username), _component(origin.hostname), "sessions",
    ) + "/"
    session_ids: set[str] = set()
    for key in store.list(prefix):
        session_id = key[len(prefix):].split("/", 1)[0]
        try:
            session_ids.add(validate_session_id(session_id))
        except ValueError:
            continue
    return sorted(session_ids)


def _stream_agent_run_metadata(store: S3Store, generation: str) -> list[dict[str, Any]]:
    """Read run metadata, and legacy trace digests when needed, from an archive prefix."""
    body = store.open(generation)
    reader = zstandard.ZstdDecompressor().stream_reader(body, closefd=False)
    result: list[dict[str, Any]] = []
    legacy: dict[str, dict[str, Any]] = {}
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
                    result.append(value)
                    trace_file = value.get("trace_file")
                    if (not isinstance(value.get("trace_complete_size"), int)
                            or not _valid_digest(value.get("trace_digest"))):
                        if isinstance(trace_file, str) and Path(trace_file).name == trace_file:
                            legacy[trace_file] = value
                elif name.startswith("agents/traces/"):
                    if not legacy:
                        break
                    trace_name = Path(name).name
                    value = legacy.get(trace_name)
                    if value is None or not member.isfile():
                        continue
                    extracted = archive.extractfile(member)
                    if extracted is None:
                        raise ValueError("remote agent trace is invalid")
                    hashing = hashlib.sha256()
                    size = 0
                    while True:
                        chunk = extracted.read(STREAM_READ_SIZE)
                        if not chunk:
                            break
                        hashing.update(chunk)
                        size += len(chunk)
                    value["trace_complete_size"] = size
                    value["trace_digest"] = hashing.hexdigest()
                    legacy.pop(trace_name, None)
                elif name.startswith("snapshots/") or name.startswith("steps/"):
                    break
    finally:
        try:
            reader.close()
        finally:
            store.close(body)
    return result


def inspect_archived_agent_runs(origin: SessionOrigin, config: S3Config | None = None,
                                client: Any | None = None
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
            _, generation, _ = _select_generation(remote, config, base, session_id)
        except FileNotFoundError:
            continue
        for metadata in _stream_agent_run_metadata(remote, generation):
            harness = metadata.get("harness")
            native_id = metadata.get("agent_session_id")
            size = metadata.get("trace_complete_size")
            digest = metadata.get("trace_digest")
            if (not isinstance(harness, str) or not isinstance(native_id, str)
                    or not isinstance(size, int) or size < 0
                    or not _valid_digest(digest)):
                continue
            runs.append({
                "session_id": session_id,
                "capture_scope": (
                    "agent-only" if metadata.get("imported_agent_only") is True else "partial"
                ),
                "harness": harness,
                "native_id": native_id,
                "complete_size": size,
                "digest": str(digest),
            })
    return runs, session_ids


def ensure_local_session(session_id: str, paths: StoragePaths | None = None,
                         config: S3Config | None = None,
                         client: Any | None = None) -> Path:
    """Return a local session, pulling it from the archive when absent."""
    session_id = validate_session_id(session_id)
    paths = paths or StoragePaths.discover()
    store = SessionStore(paths)
    try:
        location, _ = store.find(session_id)
        return location
    except SessionNotFoundError:
        return pull_session(session_id, paths, config, client=client)


def pull_session(session_id: str, paths: StoragePaths | None = None,
                 config: S3Config | None = None, force: bool = False,
                 client: Any | None = None) -> Path:
    """Download, verify, and atomically install a remote session."""
    session_id = validate_session_id(session_id)
    paths = paths or StoragePaths.discover()
    config = config or S3Config.discover(required=True)
    assert config is not None
    remote = _store(config, client)
    origin = _load_index(remote, config, session_id)
    base = _session_base(config, origin, session_id)
    step, object_key, digest = _select_generation(remote, config, base, session_id)
    store = SessionStore(paths)
    destination = store.session_path(session_id)
    if destination.exists() and not force:
        local = store.head(session_id)
        if local and local.step >= step:
            raise FileExistsError(
                f"local step {local.step} is not older than remote step {step}"
            )
        raise FileExistsError(f"local session exists: {session_id}; use --force to replace it")
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{session_id}.pull-", dir=destination.parent))
    try:
        body = remote.open(object_key)
        try:
            actual_digest = safe_extract_tar_zst_stream(body, temporary)
        finally:
            remote.close(body)
        if actual_digest != digest:
            raise ValueError(f"checksum mismatch: expected {digest}, got {actual_digest}")
        pulled = DirectorySession.load(temporary / "session.json")
        manifests = SessionStore._validate_history(temporary, session_id)
        if not manifests:
            raise ValueError("downloaded session has no published steps")
        manifest = manifests[-1]
        if (pulled.session_id != session_id
                or pulled.origin.memo_version_id != origin["memo_version_id"]
                or pulled.origin.username != origin["username"]
                or pulled.origin.hostname != origin["hostname"]
                or manifest.session_id != session_id
                or manifest.step != step):
            raise ValueError("downloaded session does not match remote generation")
        pulled.last_pushed_step = manifest.step
        pulled.last_pushed_digest = digest
        pulled.remote_object = object_key
        atomic_write(
            temporary / "session.json",
            (json.dumps(pulled.to_dict(), indent=2, sort_keys=True) + "\n").encode(),
        )
        atomic_install_directory(temporary, destination, force=force)
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return destination
