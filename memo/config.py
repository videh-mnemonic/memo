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
