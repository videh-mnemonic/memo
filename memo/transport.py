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
from pathlib import Path
from typing import Any, Iterable

from .config import Paths, TransportConfig
from .models import CheckpointManifest, DirectorySession
from .session_store import SessionStore, atomic_write


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


def _generation_paths(session_path: Path, manifest: CheckpointManifest) -> list[Path]:
    paths = [session_path / "session.json", session_path / "HEAD",
             session_path / "checkpoints" / f"{manifest.checkpoint_id}.json"]
    paths.extend((session_path / manifest.snapshot).rglob("*"))
    paths.append(session_path / manifest.snapshot)
    terminal_root = session_path / "streams" / "terminals"
    for terminal_id, high_water in manifest.stream_high_water.items():
        if high_water == 0:
            continue
        metadata = terminal_root / terminal_id / "stream.json"
        paths.extend([metadata, metadata.parent, metadata.parent / "chunks"])
        values = json.loads(metadata.read_text())
        paths.extend(metadata.parent / item for item in values.get("chunks", []))
    return [path for path in paths if path.exists()]


def package_generation(store: SessionStore, session: DirectorySession) -> tuple[bytes, str, CheckpointManifest]:
    manifest = store.head(session.archive_namespace, session.session_id)
    if manifest is None:
        raise ValueError(f"session has no published checkpoint: {session.session_id}")
    root = store.session_path(session.archive_namespace, session.session_id)
    data = deterministic_archive(root, _generation_paths(root, manifest))
    return data, digest_bytes(data), manifest


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
        raise ValueError(f"session has no published checkpoint: {session.session_id}")
    if session.last_pushed_generation == manifest.generation:
        return {"session_id": session.session_id, "generation": manifest.generation,
                "digest": session.last_pushed_digest, "status": "skipped"}
    data, digest, manifest = package_generation(store, session)
    client = client or config.client()
    base = _key(config, session.archive_namespace, session.session_id)
    version = f"{base}/generations/{manifest.generation}-{digest}.tar.gz"
    checksum = f"{version}.sha256"
    temporary = f"{base}/tmp/{uuid.uuid4().hex}.tar.gz"
    client.put_object(Bucket=config.bucket, Key=temporary, Body=data)
    try:
        client.copy_object(Bucket=config.bucket, Key=version,
                           CopySource={"Bucket": config.bucket, "Key": temporary})
        client.put_object(Bucket=config.bucket, Key=checksum,
                          Body=f"{digest}  {Path(version).name}\n".encode())
        pointer = json.dumps({"schema_version": 1, "session_id": session.session_id,
                              "namespace": session.archive_namespace,
                              "generation": manifest.generation, "digest": digest,
                              "object": version, "checksum": checksum},
                             sort_keys=True).encode()
        final_key = f"{base}/latest.json"
        client.put_object(Bucket=config.bucket, Key=final_key, Body=pointer)
    finally:
        client.delete_object(Bucket=config.bucket, Key=temporary)
    session.last_pushed_generation = manifest.generation
    session.last_pushed_digest = digest
    session.remote_object = final_key
    store.update_session(session)
    return {"session_id": session.session_id, "generation": manifest.generation,
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


def _body(response: dict[str, Any]) -> bytes:
    body = response["Body"]
    return body.read() if hasattr(body, "read") else bytes(body)


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
    pointer = json.loads(_body(client.get_object(Bucket=config.bucket, Key=keys[0])))
    data = _body(client.get_object(Bucket=config.bucket, Key=pointer["object"]))
    checksum = _body(client.get_object(Bucket=config.bucket, Key=pointer["checksum"])).decode()
    sidecar_digest = checksum.split()[0] if checksum.split() else ""
    if sidecar_digest != pointer["digest"]:
        raise ValueError("remote pointer and checksum disagree")
    verify_digest(data, pointer["digest"])
    store = SessionStore(paths)
    destination = store.session_path(pointer["namespace"], session_id)
    if destination.exists() and not force:
        local = store.head(pointer["namespace"], session_id)
        if local and local.generation >= int(pointer["generation"]):
            raise FileExistsError(
                f"local generation {local.generation} is not older than remote generation {pointer['generation']}"
            )
        raise FileExistsError(f"local session exists: {session_id}; use --force to replace it")
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{session_id}.pull-", dir=destination.parent))
    try:
        safe_extract_bytes(data, temporary)
        pulled = DirectorySession.load(temporary / "session.json")
        manifest = CheckpointManifest.load(
            temporary / "checkpoints" / f"{(temporary / 'HEAD').read_text().strip()}.json"
        )
        if pulled.session_id != session_id or manifest.generation != int(pointer["generation"]):
            raise ValueError("downloaded session does not match remote pointer")
        pulled.last_pushed_generation = manifest.generation
        pulled.last_pushed_digest = pointer["digest"]
        pulled.remote_object = keys[0]
        atomic_write(temporary / "session.json",
                     (json.dumps(pulled.to_dict(), indent=2, sort_keys=True) + "\n").encode())
        atomic_install_directory(temporary, destination, force=force)
        store.check_integrity(pointer["namespace"], session_id)
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return destination
