"""Create, verify, and safely extract deterministic in-memory archives."""

from __future__ import annotations

import gzip
import hashlib
import io
import tarfile
from pathlib import Path
from typing import Iterable


def deterministic_archive(root: Path, paths: Iterable[Path] | None = None) -> bytes:
    """Build a deterministic in-memory archive."""
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
