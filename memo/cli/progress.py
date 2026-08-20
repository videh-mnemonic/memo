"""Interactive progress rendering for slow CLI operations."""

from __future__ import annotations

import shutil
import sys
import time
from collections.abc import Callable
from math import ceil
from typing import TextIO


def _duration(seconds: float) -> str:
    value = max(0, ceil(seconds))
    if value < 60:
        return f"{value}s"
    minutes, remaining = divmod(value, 60)
    if minutes < 60:
        return f"{minutes}m {remaining:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes:02d}m"


class ProgressBar:
    def __init__(
        self,
        *,
        stream: TextIO | None = None,
        enabled: bool | None = None,
        width: int = 24,
        show_eta: bool = False,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.stream = stream or sys.stderr
        self.enabled = self.stream.isatty() if enabled is None else enabled
        self.width = width
        self.show_eta = show_eta
        self.clock = clock
        self._last_length = 0
        self._started: float | None = None

    def update(self, completed: int, total: int, message: str) -> None:
        if not self.enabled:
            return
        now = self.clock()
        if self._started is None:
            self._started = now
        total = max(total, 1)
        completed = max(0, min(completed, total))
        percent = int((completed / total) * 100)
        filled = int((percent / 100) * self.width)
        bar = "#" * filled + "-" * (self.width - filled)
        eta = ""
        if self.show_eta:
            if completed >= total:
                eta = " ETA 0s"
            elif completed <= 0:
                eta = " ETA --"
            else:
                elapsed = now - self._started
                eta = f" ETA {_duration(elapsed * (total - completed) / completed)}"
        line = f"[{bar}] {percent:3d}%{eta} {message}"
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
