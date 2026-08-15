from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from collections.abc import Sequence


@dataclass(frozen=True)
class TraceMarker:
    files: dict[Path, tuple[int, int]]


def _files(roots: Sequence[Path]) -> list[Path]:
    result: list[Path] = []
    for root in roots:
        if root.exists():
            result.extend(p for p in root.rglob("*.jsonl") if p.is_file())
    return result


def mark(roots: Sequence[Path]) -> TraceMarker:
    values = {}
    for path in _files(roots):
        stat = path.stat()
        values[path] = (stat.st_mtime_ns, stat.st_size)
    return TraceMarker(values)


def locate(roots: Sequence[Path], marker: TraceMarker) -> Path | None:
    candidates = []
    for path in _files(roots):
        stat = path.stat()
        previous = marker.files.get(path)
        if previous is None or (stat.st_mtime_ns, stat.st_size) != previous:
            candidates.append((stat.st_mtime_ns, path))
    return max(candidates, default=(0, None))[1]
