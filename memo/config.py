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

    @classmethod
    def discover(cls) -> "Paths":
        home = Path(os.environ.get("MEMO_HOME", "~/memo")).expanduser().resolve()
        temp = Path(os.environ.get("TMPDIR", tempfile.gettempdir())).resolve()
        return cls(home, home / "scratch", home / "archive", temp / "memo" / "unpack")

    def ensure_storage(self) -> None:
        self.scratch.mkdir(parents=True, exist_ok=True)
        self.archive.mkdir(parents=True, exist_ok=True)

