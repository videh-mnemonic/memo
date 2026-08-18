"""Provide small helpers shared by multiple CLI commands."""

import stat
from pathlib import Path

from ...transport import ensure_local_session


def require_local_session(session_id: str) -> None:
    ensure_local_session(session_id)


def session_size(root: Path) -> int:
    total = 0
    for path in root.rglob("*"):
        try:
            metadata = path.lstat()
        except OSError:
            continue
        if stat.S_ISREG(metadata.st_mode):
            total += metadata.st_size
    return total


def format_size(size: int) -> str:
    units = ("B", "KiB", "MiB", "GiB", "TiB", "PiB")
    value = float(max(0, size))
    unit = units[0]
    for unit in units:
        if value < 1024 or unit == units[-1]:
            break
        value /= 1024
    if unit == "B":
        return f"{int(value)} B"
    rounded = round(value, 1)
    return f"{int(rounded)} {unit}" if rounded.is_integer() else f"{rounded:.1f} {unit}"
