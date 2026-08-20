"""Resolve and create filesystem locations for Memo archives and runtime state."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, init=False)
class StoragePaths:
    """Resolved locations for Memo's local archive and runtime state."""

    home: Path
    archive: Path
    runtime: Path
    socket: Path
    registry: Path
    spool: Path
    log: Path

    def __init__(
        self,
        home: Path,
        archive: Path | None = None,
        runtime: Path | None = None,
        socket: Path | None = None,
        registry: Path | None = None,
        spool: Path | None = None,
        log: Path | None = None,
    ) -> None:
        resolved_archive = archive or home / "archive"
        resolved_runtime = runtime or home / "runtime"
        object.__setattr__(self, "home", home)
        object.__setattr__(self, "archive", resolved_archive)
        object.__setattr__(self, "runtime", resolved_runtime)
        object.__setattr__(self, "socket", socket or resolved_runtime / "memo.sock")
        object.__setattr__(self, "registry", registry or resolved_runtime / "registry.sqlite")
        object.__setattr__(self, "spool", spool or resolved_runtime / "sessions")
        object.__setattr__(self, "log", log or resolved_runtime / "daemon.log")

    @classmethod
    def discover(cls) -> StoragePaths:
        home = Path(os.environ.get("MEMO_HOME", "~/memo")).expanduser().resolve()
        return cls(home)

    def ensure_storage(self) -> None:
        self.archive.mkdir(parents=True, exist_ok=True)
        self.runtime.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.spool.mkdir(parents=True, exist_ok=True, mode=0o700)
