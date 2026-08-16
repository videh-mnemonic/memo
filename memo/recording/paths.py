"""Resolve and create filesystem locations for Memo archives and runtime state."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class StoragePaths:
    """Resolved locations for Memo's local archive and runtime state."""

    home: Path
    archive: Path | None = None
    runtime: Path | None = None
    socket: Path | None = None
    registry: Path | None = None
    spool: Path | None = None

    def __post_init__(self) -> None:
        archive = self.archive or self.home / "archive"
        runtime = self.runtime or self.home / "runtime"
        object.__setattr__(self, "archive", archive)
        object.__setattr__(self, "runtime", runtime)
        object.__setattr__(self, "socket", self.socket or runtime / "memo.sock")
        object.__setattr__(self, "registry", self.registry or runtime / "registry.sqlite")
        object.__setattr__(self, "spool", self.spool or runtime / "sessions")

    @classmethod
    def discover(cls) -> "StoragePaths":
        home = Path(os.environ.get("MEMO_HOME", "~/memo")).expanduser().resolve()
        return cls(home)

    def ensure_storage(self) -> None:
        assert self.archive is not None
        self.archive.mkdir(parents=True, exist_ok=True)
        assert self.runtime is not None
        assert self.spool is not None
        self.runtime.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.spool.mkdir(parents=True, exist_ok=True, mode=0o700)
