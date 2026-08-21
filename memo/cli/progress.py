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

    def render_line(self, completed: int, total: int, message: str) -> str:
        """Format one update while retaining the ETA start time."""
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
        return f"[{bar}] {percent:3d}%{eta} {message}"

    def reset_eta(self) -> None:
        """Start ETA sampling again for a new unit of work."""
        self._started = None

    def update(self, completed: int, total: int, message: str) -> None:
        if not self.enabled:
            return
        line = self.render_line(completed, total, message)
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


class ProgressPair:
    """Render overall and current-item progress on two stable terminal lines."""

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
        self._overall = ProgressBar(
            stream=self.stream,
            enabled=False,
            width=width,
            show_eta=show_eta,
            clock=clock,
        )
        self._current = ProgressBar(
            stream=self.stream,
            enabled=False,
            width=width,
            show_eta=show_eta,
            clock=clock,
        )
        self._values = [(0, 1, "starting"), (0, 1, "waiting for first item")]
        self._rendered = False
        self._last_current = 0

    def _render(self) -> None:
        if not self.enabled:
            return
        columns = shutil.get_terminal_size((100, 20)).columns
        lines = []
        for label, bar, values in (
            ("Overall", self._overall, self._values[0]),
            ("Current", self._current, self._values[1]),
        ):
            line = f"{label} {bar.render_line(*values)}"
            lines.append(line[: max(columns - 1, 0)])
        if self._rendered:
            print("\r\x1b[1A", end="", file=self.stream)
        print(f"\r\x1b[2K{lines[0]}", file=self.stream)
        print(f"\r\x1b[2K{lines[1]}", end="", file=self.stream, flush=True)
        self._rendered = True

    def update_overall(self, completed: int, total: int, message: str) -> None:
        self._values[0] = (completed, total, message)
        self._render()

    def update_current(self, completed: int, total: int, message: str) -> None:
        normalized = max(0, min(completed, max(total, 1)))
        if normalized < self._last_current:
            self._current.reset_eta()
        self._last_current = normalized
        self._values[1] = (completed, total, message)
        self._render()

    def finish(self) -> None:
        if self.enabled and self._rendered:
            print(file=self.stream, flush=True)
            self._rendered = False

    def __enter__(self) -> ProgressPair:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.finish()
