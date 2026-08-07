from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class TraceMarker:
    files: dict[Path, tuple[int, int]]


def trace_roots(tool: str) -> list[Path]:
    override = os.environ.get("MEMO_TRACE_DIR")
    if override:
        return [Path(override).expanduser()]
    home = Path.home()
    return [home / (".claude/projects" if tool == "claude" else ".codex/sessions")]


def _files(tool: str) -> list[Path]:
    result: list[Path] = []
    for root in trace_roots(tool):
        if root.exists():
            result.extend(p for p in root.rglob("*.jsonl") if p.is_file())
    return result


def mark(tool: str) -> TraceMarker:
    values = {}
    for path in _files(tool):
        stat = path.stat()
        values[path] = (stat.st_mtime_ns, stat.st_size)
    return TraceMarker(values)


def locate(tool: str, marker: TraceMarker) -> Path | None:
    candidates = []
    for path in _files(tool):
        stat = path.stat()
        previous = marker.files.get(path)
        if previous is None or (stat.st_mtime_ns, stat.st_size) != previous:
            candidates.append((stat.st_mtime_ns, path))
    return max(candidates, default=(0, None))[1]


def session_id(trace: Path) -> str:
    try:
        with trace.open(errors="replace") as handle:
            for line in handle:
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                for key in ("session_id", "sessionId", "conversation_id", "conversationId", "id"):
                    value = record.get(key)
                    if isinstance(value, str) and value:
                        return value
                for container_key in ("payload", "meta", "session"):
                    container = record.get(container_key)
                    if isinstance(container, dict):
                        for key in ("session_id", "sessionId", "conversation_id", "id"):
                            value = container.get(key)
                            if isinstance(value, str) and value:
                                return value
    except OSError:
        pass
    # Codex rollout filenames end in their UUID; Claude commonly uses the ID as the stem.
    import re
    match = re.search(r"([0-9a-fA-F]{8}-[0-9a-fA-F-]{27,})$", trace.stem)
    return match.group(1) if match else trace.stem
