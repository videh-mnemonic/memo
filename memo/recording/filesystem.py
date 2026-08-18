"""Provide durable filesystem primitives shared by recording subsystems."""

from __future__ import annotations

import os
import tempfile
from pathlib import Path


def atomic_write(path: Path, data: bytes) -> None:
    """Replace a file durably without exposing partially written content."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(name, path)
        descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    except BaseException:
        Path(name).unlink(missing_ok=True)
        raise
