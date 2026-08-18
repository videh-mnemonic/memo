"""Interactive progress rendering for slow CLI operations."""

from __future__ import annotations

import shutil
import sys
from typing import TextIO


class ProgressBar:
    def __init__(
        self,
        *,
        stream: TextIO | None = None,
        enabled: bool | None = None,
        width: int = 24,
    ) -> None:
        self.stream = stream or sys.stderr
        self.enabled = self.stream.isatty() if enabled is None else enabled
        self.width = width
        self._last_length = 0

    def update(self, completed: int, total: int, message: str) -> None:
        if not self.enabled:
            return
        total = max(total, 1)
        completed = max(0, min(completed, total))
        percent = int((completed / total) * 100)
        filled = int((percent / 100) * self.width)
        bar = "#" * filled + "-" * (self.width - filled)
        line = f"[{bar}] {percent:3d}% {message}"
        columns = shutil.get_terminal_size((100, 20)).columns
        if len(line) >= columns:
            line = line[: max(columns - 1, 0)]
        padding = " " * max(self._last_length - len(line), 0)
        print(f"\r{line}{padding}", end="", file=self.stream, flush=True)
        self._last_length = len(line)

    def finish(self) -> None:
        if self.enabled and self._last_length:
            print(file=self.stream, flush=True)
            self._last_length = 0

    def __enter__(self) -> ProgressBar:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.finish()
