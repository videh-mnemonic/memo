from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

NAMESPACE_MAX_LENGTH = 120


@dataclass(frozen=True)
class Paths:
    home: Path
    scratch: Path
    archive: Path
    unpack: Path
    runtime: Path | None = None
    socket: Path | None = None
    registry: Path | None = None
    spool: Path | None = None
    directory_archive: Path | None = None

    def __post_init__(self) -> None:
        runtime = self.runtime or self.home / "runtime"
        object.__setattr__(self, "runtime", runtime)
        object.__setattr__(self, "socket", self.socket or runtime / "memo.sock")
        object.__setattr__(self, "registry", self.registry or runtime / "registry.sqlite")
        object.__setattr__(self, "spool", self.spool or runtime / "sessions")
        object.__setattr__(self, "directory_archive", self.directory_archive or self.archive)

    @classmethod
    def discover(cls) -> "Paths":
        home = Path(os.environ.get("MEMO_HOME", "~/memo")).expanduser().resolve()
        temp = Path(os.environ.get("TMPDIR", tempfile.gettempdir())).resolve()
        return cls(home, home / "scratch", home / "archive", temp / "memo" / "unpack")

    def ensure_storage(self) -> None:
        self.scratch.mkdir(parents=True, exist_ok=True)
        self.archive.mkdir(parents=True, exist_ok=True)
        assert self.runtime is not None
        assert self.spool is not None
        self.runtime.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.spool.mkdir(parents=True, exist_ok=True, mode=0o700)


def checkpoint_interval() -> float:
    value = os.environ.get("MEMO_CHECKPOINT_INTERVAL", "15")
    try:
        return max(1.0, float(value))
    except ValueError:
        return 15.0


def _positive_float(name: str, default: float, minimum: float = 0.0) -> float:
    try:
        return max(minimum, float(os.environ.get(name, str(default))))
    except ValueError:
        return default


def _positive_int(name: str, default: int, minimum: int = 1) -> int:
    try:
        return max(minimum, int(os.environ.get(name, str(default))))
    except ValueError:
        return default


def maximum_file_size() -> int:
    return _positive_int("MEMO_MAX_FILE_SIZE", 100 * 1024 * 1024)


def spool_flush_interval() -> float:
    return _positive_float("MEMO_SPOOL_FLUSH_INTERVAL", 0.25)


def watcher_enabled() -> bool:
    return os.environ.get("MEMO_WATCHER", "1").lower() not in {"0", "false", "no", "off"}


def watcher_debounce() -> float:
    return _positive_float("MEMO_WATCHER_DEBOUNCE", 0.25)


def recovery_enabled() -> bool:
    return os.environ.get("MEMO_RECOVERY", "1").lower() not in {"0", "false", "no", "off"}
