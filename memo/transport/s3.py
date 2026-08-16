"""Package, publish, inspect, and restore Memo recordings through S3 storage."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import tarfile
import tempfile
import uuid
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO, Iterable
from urllib.parse import quote

import zstandard

from ..recording.paths import StoragePaths
from ..recording.models import DirectorySession, SessionOrigin, StepManifest
from ..recording.store import (SessionNotFoundError, SessionStore, atomic_write,
                               validate_session_id)
from .archive import deterministic_archive, digest_bytes
from .config import S3Config


MULTIPART_PART_SIZE = 8 * 1024 * 1024
METADATA_SIZE_LIMIT = 1024 * 1024
STREAM_READ_SIZE = 64 * 1024


def _s3_client(config: S3Config) -> Any:
    import boto3

    session = boto3.Session(profile_name=config.profile, region_name=config.region)
    return session.client("s3", endpoint_url=config.endpoint_url)


class HashingWriter:
    def __init__(self, target: BinaryIO, digest: Any | None = None) -> None:
        self.target = target
        self.digest = digest or hashlib.sha256()

    def writable(self) -> bool:
        return True

    def write(self, data: bytes) -> int:
        self.digest.update(data)
        written = self.target.write(data)
        if written != len(data):
            raise OSError(f"short write: expected {len(data)} bytes, wrote {written}")
        return written

    def flush(self) -> None:
        flush = getattr(self.target, "flush", None)
        if flush is not None:
            flush()

    def hexdigest(self) -> str:
        return self.digest.hexdigest()


class HashingReader:
    def __init__(self, source: BinaryIO, digest: Any | None = None,
                 read_size: int = STREAM_READ_SIZE) -> None:
        self.source = source
        self.digest = digest or hashlib.sha256()
        self.read_size = read_size

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        requested = self.read_size if size < 0 else min(size, self.read_size)
        data = self.source.read(requested)
        self.digest.update(data)
        return data

    def readinto(self, buffer: bytearray | memoryview) -> int:
        data = self.read(min(len(buffer), self.read_size))
        count = len(data)
        buffer[:count] = data
        return count

    def hexdigest(self) -> str:
        return self.digest.hexdigest()


class MultipartUploadWriter:
    def __init__(self, client: Any, bucket: str, key: str, upload_id: str,
                 part_size: int = MULTIPART_PART_SIZE) -> None:
        if part_size <= 0:
            raise ValueError("multipart part size must be positive")
        self.client = client
        self.bucket = bucket
        self.key = key
        self.upload_id = upload_id
        self.part_size = part_size
        self.buffer = bytearray()
        self.parts: list[dict[str, object]] = []
        self.finished = False

    def writable(self) -> bool:
        return True

    def write(self, data: bytes) -> int:
        if self.finished:
            raise ValueError("multipart upload writer is finished")
        view = memoryview(data)
        consumed = 0
        while consumed < len(view):
            count = min(self.part_size - len(self.buffer), len(view) - consumed)
            self.buffer.extend(view[consumed:consumed + count])
            consumed += count
            if len(self.buffer) == self.part_size:
                self._upload_buffer()
        return len(data)

    def flush(self) -> None:
        return None

    def _upload_buffer(self) -> None:
        part_number = len(self.parts) + 1
        data = self.buffer
        self.buffer = bytearray()
        response = self.client.upload_part(
            Bucket=self.bucket,
            Key=self.key,
            UploadId=self.upload_id,
            PartNumber=part_number,
            Body=data,
        )
        self.parts.append({"PartNumber": part_number, "ETag": response["ETag"]})

    def finish(self) -> list[dict[str, object]]:
        if not self.finished:
            if self.buffer or not self.parts:
                self._upload_buffer()
            self.finished = True
        return list(self.parts)


def write_deterministic_tar_zst(root: Path, paths: Iterable[Path], target: BinaryIO) -> None:
    compressor = zstandard.ZstdCompressor(
        level=3,
        threads=1,
        write_content_size=False,
        write_checksum=False,
        write_dict_id=False,
    )
    with compressor.stream_writer(target, closefd=False) as compressed:
        with tarfile.open(fileobj=compressed, mode="w|", format=tarfile.PAX_FORMAT) as archive:
            for path in sorted(paths, key=lambda item: item.relative_to(root).as_posix()):
                relative = path.relative_to(root)
                if relative.as_posix() == "session.lock" or path.is_socket():
                    continue
                info = archive.gettarinfo(str(path), arcname=relative.as_posix())
                info.uid = info.gid = 0
                info.uname = info.gname = ""
                info.mtime = 0
                if info.isfile():
                    with path.open("rb") as handle:
                        archive.addfile(info, handle)
                else:
                    archive.addfile(info)


def safe_extract_tar_zst_stream(source: BinaryIO, target: Path) -> str:
    root = target.resolve()
    hashing = HashingReader(source)
    decompressor = zstandard.ZstdDecompressor()
    shapes: dict[tuple[str, ...], str] = {}
    with decompressor.stream_reader(hashing, closefd=False) as decompressed:
        with tarfile.open(fileobj=decompressed, mode="r|") as archive:
            for member in archive:
                name = PurePosixPath(member.name)
                parts = name.parts
                if (name.is_absolute() or not parts or member.name.endswith("/.")
                        or any(part in ("", ".", "..") for part in parts)):
                    raise ValueError(f"unsafe archive path: {member.name}")
                if member.issym() or member.islnk() or member.isdev():
                    raise ValueError(f"unsupported archive entry: {member.name}")
                if not (member.isfile() or member.isdir()):
                    raise ValueError(f"unsupported archive entry: {member.name}")
                key = tuple(parts)
                if key in shapes:
                    raise ValueError(f"duplicate archive entry: {member.name}")
                for index in range(1, len(key)):
                    if shapes.get(key[:index]) == "file":
                        raise ValueError(f"archive path conflict: {member.name}")
                if member.isfile() and any(
                    existing[:len(key)] == key for existing in shapes if len(existing) > len(key)
                ):
                    raise ValueError(f"archive path conflict: {member.name}")
                try:
                    destination = root.joinpath(*parts)
                    destination.resolve().relative_to(root)
                except ValueError as error:
                    raise ValueError(
                        f"archive path escapes destination: {member.name}"
                    ) from error
                shapes[key] = "file" if member.isfile() else "directory"
                archive.extract(member, target, filter="data")
    while hashing.read(STREAM_READ_SIZE):
        pass
    return hashing.hexdigest()


def atomic_install_directory(prepared: Path, destination: Path, force: bool = False) -> None:
    if destination.exists() and not force:
        raise FileExistsError(f"local session already exists: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    backup = destination.with_name(f".{destination.name}.backup-{uuid.uuid4().hex}")
    replaced = False
    try:
        if destination.exists():
            os.replace(destination, backup)
            replaced = True
        os.replace(prepared, destination)
    except BaseException:
        if replaced and backup.exists() and not destination.exists():
            os.replace(backup, destination)
        raise
    finally:
        if backup.exists():
            shutil.rmtree(backup)


def _history_paths(session_path: Path, manifests: list[StepManifest]) -> list[Path]:
    paths = [session_path / "session.json", session_path / "HEAD",
             session_path / "steps", session_path / "snapshots"]
    for manifest in manifests:
        paths.append(session_path / "steps" / f"{manifest.step}.json")
        paths.extend((session_path / manifest.snapshot).rglob("*"))
        paths.append(session_path / manifest.snapshot)
    terminal_root = session_path / "streams" / "terminals"
    high_water_by_terminal: dict[str, int] = {}
    for manifest in manifests:
        for terminal_id, high_water in manifest.stream_high_water.items():
            high_water_by_terminal[terminal_id] = max(
                high_water, high_water_by_terminal.get(terminal_id, 0)
            )
    if high_water_by_terminal:
        paths.extend([session_path / "streams", terminal_root])
    for terminal_id, high_water in high_water_by_terminal.items():
        if high_water == 0:
            continue
        metadata = terminal_root / terminal_id / "stream.json"
        paths.extend([metadata, metadata.parent, metadata.parent / "chunks"])
        values = json.loads(metadata.read_text())
        paths.extend(metadata.parent / item for item in values.get("chunks", []))
    agent_runs = sorted({run_id for manifest in manifests for run_id in manifest.agent_runs})
    if agent_runs:
        paths.extend([session_path / "agents", session_path / "agents" / "runs",
                      session_path / "agents" / "traces"])
    for run_id in agent_runs:
        metadata = session_path / "agents" / "runs" / f"{run_id}.json"
        paths.append(metadata)
        values = json.loads(metadata.read_text())
        trace_file = values.get("trace_file")
        if trace_file:
            paths.append(session_path / "agents" / "traces" / trace_file)
    return sorted(
        {path for path in paths if path.exists()},
        key=lambda item: item.relative_to(session_path).as_posix(),
    )


def package_history(store: SessionStore, session: DirectorySession) -> tuple[bytes, str, StepManifest]:
    manifests = store.steps(session.session_id)
    if not manifests:
        raise ValueError(f"session has no published step: {session.session_id}")
    manifest = manifests[-1]
    root = store.session_path(session.session_id)
    data = deterministic_archive(root, _history_paths(root, manifests))
    return data, digest_bytes(data), manifest


@dataclass
class PreparedGeneration:
    session_id: str
    step: int
    digest: str
    path: Path

    def cleanup(self) -> None:
        self.path.unlink(missing_ok=True)


def prepare_generation(store: SessionStore, session: DirectorySession) -> PreparedGeneration:
    manifests = store.steps(session.session_id)
    if not manifests:
        raise ValueError(f"session has no published step: {session.session_id}")
    manifest = manifests[-1]
    root = store.session_path(session.session_id)
    assert store.paths.runtime is not None
    upload_dir = store.paths.runtime / "uploads"
    upload_dir.mkdir(parents=True, exist_ok=True)
    descriptor, name = tempfile.mkstemp(
        prefix=f".{session.session_id}-{manifest.step:08d}-", suffix=".tar.zst",
        dir=upload_dir,
    )
    path = Path(name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            hashing = HashingWriter(handle)
            write_deterministic_tar_zst(root, _history_paths(root, manifests), hashing)
            handle.flush()
            os.fsync(handle.fileno())
        return PreparedGeneration(session.session_id, manifest.step, hashing.hexdigest(), path)
    except BaseException:
        try:
            os.close(descriptor)
        except OSError:
            pass
        path.unlink(missing_ok=True)
        raise


def _error_code(error: BaseException) -> str | None:
    response = getattr(error, "response", None)
    if isinstance(response, dict):
        details = response.get("Error")
        if isinstance(details, dict):
            code = details.get("Code")
            return None if code is None else str(code)
    return None


def _is_precondition_failed(error: BaseException) -> bool:
    return _error_code(error) in {"PreconditionFailed", "412"}


def _is_not_found(error: BaseException) -> bool:
    return isinstance(error, KeyError) or _error_code(error) in {
        "NoSuchKey", "NotFound", "404",
    }


def _get_optional(client: Any, config: S3Config, key: str) -> bytes | None:
    try:
        return _bounded_body(client.get_object(Bucket=config.bucket, Key=key))
    except Exception as error:
        if _is_not_found(error):
            return None
        raise


def _put_immutable(client: Any, config: S3Config, key: str, value: bytes) -> None:
    try:
        client.put_object(
            Bucket=config.bucket, Key=key, Body=value, IfNoneMatch="*"
        )
    except Exception as error:
        if not _is_precondition_failed(error):
            raise
        existing = _get_optional(client, config, key)
        if existing != value:
            raise ValueError(f"remote integrity conflict for {key}") from error


def _remote_digest(client: Any, config: S3Config, key: str) -> str:
    response = client.get_object(Bucket=config.bucket, Key=key)
    body = response["Body"]
    hashing = hashlib.sha256()
    try:
        while True:
            chunk = body.read(STREAM_READ_SIZE)
            if not chunk:
                break
            hashing.update(chunk)
    finally:
        close = getattr(body, "close", None)
        if close is not None:
            close()
    return hashing.hexdigest()


def _upload_generation(client: Any, config: S3Config,
                       prepared: PreparedGeneration, key: str) -> None:
    response = client.create_multipart_upload(Bucket=config.bucket, Key=key)
    upload_id = response["UploadId"]
    try:
        multipart = MultipartUploadWriter(client, config.bucket, key, upload_id)
        with prepared.path.open("rb") as source:
            while True:
                chunk = source.read(STREAM_READ_SIZE)
                if not chunk:
                    break
                multipart.write(chunk)
        parts = multipart.finish()
        client.complete_multipart_upload(
            Bucket=config.bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
            IfNoneMatch="*",
        )
    except BaseException as error:
        try:
            client.abort_multipart_upload(
                Bucket=config.bucket, Key=key, UploadId=upload_id
            )
        except BaseException:
            pass
        if _is_precondition_failed(error):
            if _remote_digest(client, config, key) != prepared.digest:
                raise ValueError(f"remote integrity conflict for {key}") from error
            return
        raise


@dataclass
class PushSummary:
    pushed: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    failed: list[tuple[str, str]] = field(default_factory=list)


def _key(config: S3Config, *parts: object) -> str:
    suffix = "/".join(str(part).strip("/") for part in parts)
    return f"{config.prefix}/{suffix}" if config.prefix else suffix


def _component(value: str) -> str:
    return quote(value, safe="")


def publish_generation(store: SessionStore, session: DirectorySession,
                       prepared: PreparedGeneration, config: S3Config,
                       client: Any | None = None, *, update_local: bool = True) -> dict[str, object]:
    client = client or _s3_client(config)
    base = _key(config, _component(session.origin.username),
                _component(session.origin.hostname), "sessions", session.session_id)
    generation = f"{base}/generations/{prepared.step:08d}.tar.zst"
    checksum = f"{base}/generations/{prepared.step:08d}.sha256"
    _upload_generation(client, config, prepared, generation)
    sidecar = f"{prepared.digest}  {Path(generation).name}\n".encode()
    _put_immutable(client, config, checksum, sidecar)
    index_key = _key(config, "index", "sessions", f"{session.session_id}.json")
    index = json.dumps({
        "schema_version": 1,
        "session_id": session.session_id,
        "memo_version_id": session.origin.memo_version_id,
        "username": session.origin.username,
        "hostname": session.origin.hostname,
    }, sort_keys=True).encode()
    _put_immutable(client, config, index_key, index)
    if session.state == "complete":
        completion_key = f"{base}/completion.json"
        completion = json.dumps({
            "schema_version": 1,
            "session_id": session.session_id,
            "final_step": prepared.step,
            "generation": generation,
            "sha256": prepared.digest,
        }, sort_keys=True).encode()
        _put_immutable(client, config, completion_key, completion)
    if update_local:
        session.last_pushed_step = prepared.step
        session.last_pushed_digest = prepared.digest
        session.remote_object = generation
        store.update_session(session)
    return {"session_id": session.session_id, "step": prepared.step,
            "digest": prepared.digest, "object": generation, "status": "pushed"}


def push_session(store: SessionStore, session: DirectorySession, config: S3Config,
                 client: Any | None = None) -> dict[str, object]:
    manifest = store.head(session.session_id)
    if manifest is None:
        raise ValueError(f"session has no published step: {session.session_id}")
    if session.last_pushed_step == manifest.step:
        return {"session_id": session.session_id, "step": manifest.step,
                "digest": session.last_pushed_digest, "status": "skipped"}
    prepared = prepare_generation(store, session)
    try:
        return publish_generation(store, session, prepared, config, client)
    finally:
        prepared.cleanup()


def _bounded_body(response: dict[str, Any], limit: int = METADATA_SIZE_LIMIT) -> bytes:
    body = response["Body"]
    if not hasattr(body, "read"):
        data = bytes(body)
        if len(data) > limit:
            raise ValueError(f"remote metadata exceeds {limit} bytes")
        return data
    chunks = bytearray()
    try:
        while len(chunks) <= limit:
            chunk = body.read(min(STREAM_READ_SIZE, limit + 1 - len(chunks)))
            if not chunk:
                break
            chunks.extend(chunk)
        if len(chunks) > limit:
            raise ValueError(f"remote metadata exceeds {limit} bytes")
        return bytes(chunks)
    finally:
        close = getattr(body, "close", None)
        if close is not None:
            close()


def _validate_index(index: object, session_id: str) -> dict[str, str]:
    if (not isinstance(index, dict) or index.get("schema_version") != 1
            or index.get("session_id") != session_id):
        raise ValueError("remote session index is invalid")
    if any(not isinstance(index.get(key), str) or not index.get(key)
           for key in ("memo_version_id", "username", "hostname")):
        raise ValueError("remote session index has invalid origin")
    return index  # type: ignore[return-value]


def _list_generation_pairs(client: Any, config: S3Config,
                           prefix: str) -> dict[int, tuple[str, str]]:
    package_pattern = re.compile(rf"^{re.escape(prefix)}(\d{{8,}})\.tar\.zst$")
    checksum_pattern = re.compile(rf"^{re.escape(prefix)}(\d{{8,}})\.sha256$")
    packages: dict[int, str] = {}
    checksums: dict[int, str] = {}
    token: str | None = None
    while True:
        arguments: dict[str, object] = {"Bucket": config.bucket, "Prefix": prefix}
        if token is not None:
            arguments["ContinuationToken"] = token
        response = client.list_objects_v2(**arguments)
        for item in response.get("Contents", []):
            if not isinstance(item, dict) or not isinstance(item.get("Key"), str):
                continue
            key = item["Key"]
            package_match = package_pattern.fullmatch(key)
            checksum_match = checksum_pattern.fullmatch(key)
            if package_match:
                packages[int(package_match.group(1))] = key
            elif checksum_match:
                checksums[int(checksum_match.group(1))] = key
        if not response.get("IsTruncated"):
            break
        next_token = response.get("NextContinuationToken")
        if not isinstance(next_token, str) or not next_token:
            raise ValueError("remote generation listing is truncated without a token")
        token = next_token
    return {
        step: (package, checksums[step])
        for step, package in packages.items() if step in checksums
    }


def _valid_digest(value: object) -> bool:
    return (isinstance(value, str) and len(value) == 64
            and all(character in "0123456789abcdef" for character in value))


def list_archived_session_ids(config: S3Config | None = None,
                              client: Any | None = None) -> list[str]:
    """List session IDs advertised by the remote archive index."""
    config = config or S3Config.discover(required=True)
    assert config is not None
    client = client or _s3_client(config)
    prefix = _key(config, "index", "sessions") + "/"
    suffix = ".json"
    session_ids: set[str] = set()
    token: str | None = None
    while True:
        arguments: dict[str, object] = {"Bucket": config.bucket, "Prefix": prefix}
        if token is not None:
            arguments["ContinuationToken"] = token
        response = client.list_objects_v2(**arguments)
        for item in response.get("Contents", []):
            if not isinstance(item, dict) or not isinstance(item.get("Key"), str):
                continue
            key = item["Key"]
            if not key.startswith(prefix) or not key.endswith(suffix):
                continue
            session_id = key[len(prefix):-len(suffix)]
            try:
                session_ids.add(validate_session_id(session_id))
            except ValueError:
                continue
        if not response.get("IsTruncated"):
            break
        next_token = response.get("NextContinuationToken")
        if not isinstance(next_token, str) or not next_token:
            raise ValueError("remote session listing is truncated without a token")
        token = next_token
    return sorted(session_ids)


def _same_origin_remote_session_ids(origin: SessionOrigin, config: S3Config,
                                    client: Any) -> list[str]:
    prefix = _key(
        config, _component(origin.username), _component(origin.hostname), "sessions",
    ) + "/"
    session_ids: set[str] = set()
    token: str | None = None
    while True:
        arguments: dict[str, object] = {"Bucket": config.bucket, "Prefix": prefix}
        if token is not None:
            arguments["ContinuationToken"] = token
        response = client.list_objects_v2(**arguments)
        for item in response.get("Contents", []):
            if not isinstance(item, dict) or not isinstance(item.get("Key"), str):
                continue
            remainder = item["Key"][len(prefix):]
            session_id = remainder.split("/", 1)[0]
            try:
                session_ids.add(validate_session_id(session_id))
            except ValueError:
                continue
        if not response.get("IsTruncated"):
            break
        next_token = response.get("NextContinuationToken")
        if not isinstance(next_token, str) or not next_token:
            raise ValueError("remote session listing is truncated without a token")
        token = next_token
    return sorted(session_ids)


def _stream_agent_run_metadata(client: Any, config: S3Config,
                               generation: str) -> list[dict[str, Any]]:
    """Read run metadata, and legacy trace digests when needed, from an archive prefix."""
    response = client.get_object(Bucket=config.bucket, Key=generation)
    body = response["Body"]
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
            close = getattr(body, "close", None)
            if close is not None:
                close()
    return result


def inspect_archived_agent_runs(origin: SessionOrigin, config: S3Config | None = None,
                                client: Any | None = None
                                ) -> tuple[list[dict[str, object]], set[str]]:
    """Inspect same-origin agent metadata without downloading filesystem snapshots."""
    config = config or S3Config.discover(required=True)
    assert config is not None
    client = client or _s3_client(config)
    session_ids = set(_same_origin_remote_session_ids(origin, config, client))
    runs: list[dict[str, object]] = []
    for session_id in sorted(session_ids):
        base = _key(
            config, _component(origin.username), _component(origin.hostname),
            "sessions", session_id,
        )
        pairs = _list_generation_pairs(client, config, f"{base}/generations/")
        if not pairs:
            continue
        completion_data = _get_optional(client, config, f"{base}/completion.json")
        if completion_data is None:
            step = max(pairs)
        else:
            completion = json.loads(completion_data)
            if (not isinstance(completion, dict)
                    or completion.get("schema_version") != 1
                    or completion.get("session_id") != session_id
                    or not isinstance(completion.get("final_step"), int)
                    or isinstance(completion.get("final_step"), bool)
                    or int(completion["final_step"]) not in pairs):
                raise ValueError("remote completion marker is invalid")
            step = int(completion["final_step"])
        generation, _ = pairs[step]
        for metadata in _stream_agent_run_metadata(client, config, generation):
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
    session_id = validate_session_id(session_id)
    paths = paths or StoragePaths.discover()
    config = config or S3Config.discover(required=True)
    assert config is not None
    client = client or _s3_client(config)
    index_key = _key(config, "index", "sessions", f"{session_id}.json")
    try:
        index_data = _bounded_body(client.get_object(Bucket=config.bucket, Key=index_key))
    except Exception as error:
        if _is_not_found(error):
            raise FileNotFoundError(f"remote session not found: {session_id}") from error
        raise
    index = json.loads(index_data)
    origin = _validate_index(index, session_id)
    base = _key(
        config, _component(origin["username"]), _component(origin["hostname"]),
        "sessions", session_id,
    )
    generation_prefix = f"{base}/generations/"
    pairs = _list_generation_pairs(client, config, generation_prefix)
    completion_key = f"{base}/completion.json"
    completion_data = _get_optional(client, config, completion_key)
    expected_digest: str | None = None
    if completion_data is not None:
        completion = json.loads(completion_data)
        if (not isinstance(completion, dict) or completion.get("schema_version") != 1
                or completion.get("session_id") != session_id
                or not isinstance(completion.get("final_step"), int)
                or isinstance(completion.get("final_step"), bool)
                or completion.get("final_step") < 0
                or not _valid_digest(completion.get("sha256"))):
            raise ValueError("remote completion marker is invalid")
        step = int(completion["final_step"])
        pair = pairs.get(step)
        expected_generation = f"{generation_prefix}{step:08d}.tar.zst"
        if pair is None or completion.get("generation") != expected_generation:
            raise ValueError("remote completion marker references an incomplete generation")
        expected_digest = str(completion["sha256"])
    else:
        if not pairs:
            raise FileNotFoundError(f"remote session has no complete generation: {session_id}")
        step = max(pairs)
        pair = pairs[step]
    object_key, checksum_key = pair
    checksum = _bounded_body(
        client.get_object(Bucket=config.bucket, Key=checksum_key)
    ).decode()
    sidecar_digest = checksum.split()[0] if checksum.split() else ""
    if not _valid_digest(sidecar_digest):
        raise ValueError("remote generation checksum is invalid")
    if expected_digest is not None and sidecar_digest != expected_digest:
        raise ValueError("remote completion marker and checksum disagree")
    digest = sidecar_digest
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
        response = client.get_object(Bucket=config.bucket, Key=object_key)
        body = response["Body"]
        try:
            actual_digest = safe_extract_tar_zst_stream(body, temporary)
        except BaseException:
            close = getattr(body, "close", None)
            if close is not None:
                try:
                    close()
                except BaseException:
                    pass
            raise
        close = getattr(body, "close", None)
        if close is not None:
            close()
        if actual_digest != digest:
            raise ValueError(
                f"checksum mismatch: expected {digest}, got {actual_digest}"
            )
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
        atomic_write(temporary / "session.json",
                     (json.dumps(pulled.to_dict(), indent=2, sort_keys=True) + "\n").encode())
        atomic_install_directory(temporary, destination, force=force)
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return destination
