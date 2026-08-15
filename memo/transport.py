from __future__ import annotations

import gzip
import hashlib
import io
import json
import os
import shutil
import tarfile
import tempfile
import uuid
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO, Iterable

import zstandard

from .config import Paths, TransportConfig
from .models import DirectorySession, StepManifest
from .session_store import SessionStore, atomic_write


MULTIPART_PART_SIZE = 8 * 1024 * 1024
METADATA_SIZE_LIMIT = 1024 * 1024
STREAM_READ_SIZE = 64 * 1024


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


def deterministic_archive(root: Path, paths: Iterable[Path] | None = None) -> bytes:
    selected = list(paths) if paths is not None else list(root.rglob("*"))
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w", format=tarfile.PAX_FORMAT) as archive:
        for path in sorted(selected, key=lambda item: item.relative_to(root).as_posix()):
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
    result = io.BytesIO()
    with gzip.GzipFile(filename="", mode="wb", fileobj=result, mtime=0) as zipped:
        zipped.write(raw.getvalue())
    return result.getvalue()


def digest_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def verify_digest(data: bytes, expected: str) -> None:
    actual = digest_bytes(data)
    if actual != expected:
        raise ValueError(f"checksum mismatch: expected {expected}, got {actual}")


def safe_extract_bytes(data: bytes, target: Path) -> None:
    root = target.resolve()
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as archive:
        members = archive.getmembers()
        for member in members:
            name = Path(member.name)
            if name.is_absolute() or ".." in name.parts:
                raise ValueError(f"unsafe archive path: {member.name}")
            if member.issym() or member.islnk() or member.isdev():
                raise ValueError(f"unsupported archive entry: {member.name}")
            try:
                (root / name).resolve().relative_to(root)
            except ValueError as error:
                raise ValueError(f"archive path escapes destination: {member.name}") from error
        archive.extractall(target, members=members, filter="data")


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
    manifests = store.steps(session.archive_namespace, session.session_id)
    if not manifests:
        raise ValueError(f"session has no published step: {session.session_id}")
    manifest = manifests[-1]
    root = store.session_path(session.archive_namespace, session.session_id)
    data = deterministic_archive(root, _history_paths(root, manifests))
    return data, digest_bytes(data), manifest


def _multipart_package_history(store: SessionStore, session: DirectorySession,
                               config: TransportConfig, client: Any,
                               temporary: str) -> tuple[str, StepManifest]:
    manifests = store.steps(session.archive_namespace, session.session_id)
    if not manifests:
        raise ValueError(f"session has no published step: {session.session_id}")
    manifest = manifests[-1]
    root = store.session_path(session.archive_namespace, session.session_id)
    response = client.create_multipart_upload(Bucket=config.bucket, Key=temporary)
    upload_id = response["UploadId"]
    try:
        multipart = MultipartUploadWriter(client, config.bucket, temporary, upload_id)
        hashing = HashingWriter(multipart)
        write_deterministic_tar_zst(root, _history_paths(root, manifests), hashing)
        parts = multipart.finish()
        client.complete_multipart_upload(
            Bucket=config.bucket,
            Key=temporary,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
    except BaseException:
        try:
            client.abort_multipart_upload(
                Bucket=config.bucket, Key=temporary, UploadId=upload_id
            )
        except BaseException:
            pass
        raise
    return hashing.hexdigest(), manifest


@dataclass
class PushSummary:
    pushed: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    failed: list[tuple[str, str]] = field(default_factory=list)


def _key(config: TransportConfig, *parts: object) -> str:
    suffix = "/".join(str(part).strip("/") for part in parts)
    return f"{config.prefix}/{suffix}" if config.prefix else suffix


def push_session(store: SessionStore, session: DirectorySession, config: TransportConfig,
                 client: Any | None = None) -> dict[str, object]:
    manifest = store.head(session.archive_namespace, session.session_id)
    if manifest is None:
        raise ValueError(f"session has no published step: {session.session_id}")
    if session.last_pushed_step == manifest.step:
        return {"session_id": session.session_id, "step": manifest.step,
                "digest": session.last_pushed_digest, "status": "skipped"}
    client = client or config.client()
    base = _key(config, session.archive_namespace, session.session_id)
    temporary = f"{base}/tmp/{uuid.uuid4().hex}.tar.zst"
    digest, manifest = _multipart_package_history(store, session, config, client, temporary)
    version = f"{base}/steps/{manifest.step}-{digest}.tar.zst"
    checksum = f"{version}.sha256"
    try:
        client.copy_object(Bucket=config.bucket, Key=version,
                           CopySource={"Bucket": config.bucket, "Key": temporary})
        client.put_object(Bucket=config.bucket, Key=checksum,
                          Body=f"{digest}  {Path(version).name}\n".encode())
    except BaseException:
        try:
            client.delete_object(Bucket=config.bucket, Key=temporary)
        except BaseException:
            pass
        raise
    client.delete_object(Bucket=config.bucket, Key=temporary)
    pointer = json.dumps({"schema_version": 2, "session_id": session.session_id,
                          "namespace": session.archive_namespace,
                          "step": manifest.step, "digest": digest,
                          "object": version, "checksum": checksum},
                         sort_keys=True).encode()
    final_key = f"{base}/latest.json"
    client.put_object(Bucket=config.bucket, Key=final_key, Body=pointer)
    session.last_pushed_step = manifest.step
    session.last_pushed_digest = digest
    session.remote_object = final_key
    store.update_session(session)
    return {"session_id": session.session_id, "step": manifest.step,
            "digest": digest, "object": final_key, "status": "pushed"}


def push_sessions(paths: Paths | None = None, config: TransportConfig | None = None,
                  session_id: str | None = None, client: Any | None = None) -> PushSummary:
    paths = paths or Paths.discover()
    config = config or TransportConfig.discover(required=True)
    assert config is not None
    store = SessionStore(paths)
    summary = PushSummary()
    sessions = [session for _, session in store.list_sessions()
                if session_id is None or session.session_id == session_id]
    if session_id and not sessions:
        summary.failed.append((session_id, "directory session not found"))
    for session in sessions:
        try:
            result = push_session(store, session, config, client)
            target = summary.skipped if result["status"] == "skipped" else summary.pushed
            target.append(session.session_id)
        except Exception as error:
            summary.failed.append((session.session_id, str(error)))
    return summary


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


def _validate_pointer(pointer: object, session_id: str, pointer_key: str) -> dict[str, Any]:
    if not isinstance(pointer, dict):
        raise ValueError("remote pointer must be a JSON object")
    if pointer.get("schema_version") != 2:
        raise ValueError("unsupported remote pointer schema")
    if pointer.get("session_id") != session_id:
        raise ValueError("remote pointer session identity mismatch")
    namespace = pointer.get("namespace")
    step = pointer.get("step")
    digest = pointer.get("digest")
    object_key = pointer.get("object")
    checksum_key = pointer.get("checksum")
    if not isinstance(namespace, str) or not namespace:
        raise ValueError("remote pointer has invalid namespace")
    if not isinstance(step, int) or isinstance(step, bool) or step < 0:
        raise ValueError("remote pointer has invalid step")
    if (not isinstance(digest, str) or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)):
        raise ValueError("remote pointer has invalid digest")
    if not isinstance(object_key, str) or not object_key.endswith(".tar.zst"):
        raise ValueError("remote pointer object must be a .tar.zst package")
    if not isinstance(checksum_key, str) or checksum_key != f"{object_key}.sha256":
        raise ValueError("remote pointer has invalid checksum object")
    base = pointer_key.removesuffix("/latest.json")
    expected_suffix = f"{namespace}/{session_id}"
    if (base.strip("/") != expected_suffix
            and not base.strip("/").endswith(f"/{expected_suffix}")):
        raise ValueError("remote pointer object identity mismatch")
    if not object_key.startswith(f"{base}/steps/"):
        raise ValueError("remote pointer object identity mismatch")
    return pointer


def pull_session(session_id: str, paths: Paths | None = None,
                 config: TransportConfig | None = None, force: bool = False,
                 client: Any | None = None) -> Path:
    paths = paths or Paths.discover()
    config = config or TransportConfig.discover(required=True)
    assert config is not None
    client = client or config.client()
    prefix = _key(config, "")
    listing = client.list_objects_v2(Bucket=config.bucket, Prefix=prefix)
    keys = [item["Key"] for item in listing.get("Contents", [])
            if item["Key"].endswith(f"/{session_id}/latest.json")]
    if len(keys) != 1:
        raise FileNotFoundError(f"remote session lookup returned {len(keys)} matches: {session_id}")
    pointer = _validate_pointer(
        json.loads(_bounded_body(client.get_object(Bucket=config.bucket, Key=keys[0]))),
        session_id,
        keys[0],
    )
    checksum = _bounded_body(
        client.get_object(Bucket=config.bucket, Key=pointer["checksum"])
    ).decode()
    sidecar_digest = checksum.split()[0] if checksum.split() else ""
    if sidecar_digest != pointer["digest"]:
        raise ValueError("remote pointer and checksum disagree")
    store = SessionStore(paths)
    destination = store.session_path(pointer["namespace"], session_id)
    if destination.exists() and not force:
        local = store.head(pointer["namespace"], session_id)
        if local and local.step >= int(pointer["step"]):
            raise FileExistsError(
                f"local step {local.step} is not older than remote step {pointer['step']}"
            )
        raise FileExistsError(f"local session exists: {session_id}; use --force to replace it")
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{session_id}.pull-", dir=destination.parent))
    try:
        response = client.get_object(Bucket=config.bucket, Key=pointer["object"])
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
        if actual_digest != pointer["digest"]:
            raise ValueError(
                f"checksum mismatch: expected {pointer['digest']}, got {actual_digest}"
            )
        pulled = DirectorySession.load(temporary / "session.json")
        manifests = SessionStore._validate_history(
            temporary, str(pointer["namespace"]), session_id
        )
        if not manifests:
            raise ValueError("downloaded session has no published steps")
        manifest = manifests[-1]
        if (pulled.session_id != session_id
                or pulled.archive_namespace != pointer["namespace"]
                or manifest.session_id != session_id
                or manifest.step != int(pointer["step"])):
            raise ValueError("downloaded session does not match remote pointer")
        pulled.last_pushed_step = manifest.step
        pulled.last_pushed_digest = pointer["digest"]
        pulled.remote_object = keys[0]
        atomic_write(temporary / "session.json",
                     (json.dumps(pulled.to_dict(), indent=2, sort_keys=True) + "\n").encode())
        atomic_install_directory(temporary, destination, force=force)
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return destination
