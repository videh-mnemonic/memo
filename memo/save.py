from __future__ import annotations

import gzip
import hashlib
import io
import json
import os
import shutil
import subprocess
import tarfile
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from .config import Paths
from .models import SessionMeta
from .store import SessionLock, SessionLockedError, list_scratch
from .wrapper import utcnow


@dataclass
class SaveSummary:
    shipped: list[str]
    locked: list[str]
    not_idle: list[str]
    failed: list[tuple[str, str]]


def _idle_seconds(meta: SessionMeta) -> float:
    stamp = datetime.fromisoformat(meta.last_activity_utc.replace("Z", "+00:00"))
    return (datetime.now(timezone.utc) - stamp).total_seconds()


def _cumulative_patch(session_dir: Path) -> None:
    chunks = [p.read_bytes() for p in sorted((session_dir / "legs").glob("*/commits.patch")) if p.stat().st_size]
    target = session_dir / "git" / "session-commits.patch"
    if chunks:
        target.write_bytes(b"\n".join(chunk.rstrip(b"\n") for chunk in chunks) + b"\n")
    elif target.exists():
        target.unlink()


def _coverage(session_dir: Path, meta: SessionMeta) -> str | None:
    binary = shutil.which("reproducible-trajectories")
    if not binary:
        return "coverage check skipped: reproducible-trajectories not installed"
    traces = sorted((session_dir / "traces").glob("*.jsonl"))
    if not traces:
        return None
    try:
        result = subprocess.run(
            [binary, "modified-files", *map(str, traces)], text=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60,
        )
        if result.returncode:
            return "coverage check failed; retaining existing coverage"
        root = Path(meta.repo_root).resolve()
        for line in result.stdout.splitlines():
            candidate = line.strip()
            if not candidate:
                continue
            try:
                Path(candidate).expanduser().resolve().relative_to(root)
            except ValueError:
                meta.coverage = "partial_outside_repo"
                break
    except (OSError, subprocess.SubprocessError):
        return "coverage check failed; retaining existing coverage"
    return None


def _archive_bytes(session_dir: Path) -> bytes:
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w", format=tarfile.PAX_FORMAT) as archive:
        for path in sorted(session_dir.rglob("*"), key=lambda p: p.relative_to(session_dir).as_posix()):
            relative = path.relative_to(session_dir)
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


def _atomic_write(path: Path, data: bytes) -> None:
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(name, path)
    except BaseException:
        Path(name).unlink(missing_ok=True)
        raise


def ship(session_dir: Path, meta: SessionMeta, paths: Paths) -> tuple[Path, str | None]:
    meta.validate()
    archive_dir = paths.archive / meta.archive_namespace
    archive_dir.mkdir(parents=True, exist_ok=True)
    destination = archive_dir / f"{meta.session_id}.tar.gz"
    sidecar = destination.with_suffix(destination.suffix + ".sha256")
    if destination.exists() or sidecar.exists():
        raise FileExistsError(f"archive already exists: {destination}")
    _cumulative_patch(session_dir)
    note = _coverage(session_dir, meta)
    meta.shipped = True
    meta.shipped_at = utcnow()
    meta.archive_sha256 = None  # The sidecar is authoritative; hashes cannot contain themselves.
    meta.save(session_dir / "meta.json")
    data = _archive_bytes(session_dir)
    digest = hashlib.sha256(data).hexdigest()
    temporary = archive_dir / f".{meta.session_id}.{os.getpid()}.tar.gz.tmp"
    try:
        _atomic_write(temporary, data)
        os.replace(temporary, destination)
        _atomic_write(sidecar, f"{digest}  {destination.name}\n".encode())
    finally:
        temporary.unlink(missing_ok=True)
    shutil.rmtree(session_dir)
    return destination, note


def save_sessions(*, all_sessions: bool = False, session_id: str | None = None,
                  older_than_hours: float = 48.0, paths: Paths | None = None) -> SaveSummary:
    paths = paths or Paths.discover()
    paths.ensure_storage()
    summary = SaveSummary([], [], [], [])
    sessions = list_scratch(paths)
    if session_id:
        sessions = [(d, m) for d, m in sessions if m.session_id == session_id]
        if not sessions:
            summary.failed.append((session_id, "scratch session not found"))
            return summary
    for directory, meta in sessions:
        if not all_sessions and not session_id and _idle_seconds(meta) < older_than_hours * 3600:
            summary.not_idle.append(meta.session_id)
            continue
        try:
            with SessionLock(directory / "session.lock"):
                ship(directory, meta, paths)
            summary.shipped.append(meta.session_id)
        except SessionLockedError:
            summary.locked.append(meta.session_id)
        except Exception as error:
            summary.failed.append((meta.session_id, str(error)))
    return summary

