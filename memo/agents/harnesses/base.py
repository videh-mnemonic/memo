from __future__ import annotations

"""Provider harness contracts and normalized trace event helpers."""

import json
import os
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence


TRACE_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class SourceRecord:
    seq: int
    value: Any = None
    line: str = ""
    error: str | None = None


@dataclass(frozen=True)
class ParseContext:
    trace: str
    seq: int


@dataclass(frozen=True)
class TraceEvent:
    schema_version: int
    provider: str
    position: dict[str, Any]
    event: dict[str, Any]
    relationships: dict[str, Any] | None
    usage: Any
    native: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class AgentHarness(ABC):
    name: str
    executable: str

    def trace_roots(self) -> tuple[Path, ...]:
        override = os.environ.get("MEMO_TRACE_DIR")
        if override:
            return (Path(override).expanduser(),)
        return self.default_trace_roots()

    @abstractmethod
    def default_trace_roots(self) -> tuple[Path, ...]:
        raise NotImplementedError

    @abstractmethod
    def parse_resume(self, args: Sequence[str]) -> str | None:
        raise NotImplementedError

    @abstractmethod
    def identify_session(self, records: Iterable[SourceRecord], path: Path) -> str:
        raise NotImplementedError

    def identify_cwd(self, records: Iterable[SourceRecord], path: Path) -> Path | None:
        keys = ("cwd", "project_path", "projectPath", "working_directory", "workspace")
        containers = ("payload", "message", "meta", "session")
        for source in records:
            if not isinstance(source.value, dict):
                continue
            values = [source.value]
            values.extend(
                value for key in containers
                if isinstance((value := source.value.get(key)), dict)
            )
            for value in values:
                for key in keys:
                    cwd = value.get(key)
                    if isinstance(cwd, str) and cwd:
                        return Path(cwd).expanduser().resolve(strict=False)
        return None

    @abstractmethod
    def parse_record(self, record: SourceRecord, context: ParseContext) -> TraceEvent:
        raise NotImplementedError

    def event(
        self,
        record: SourceRecord,
        context: ParseContext,
        event_type: str,
        *,
        content: Any,
        timestamp: Any = None,
        event_id: Any = None,
        parent_id: Any = None,
        relationships: dict[str, Any] | None = None,
        usage: Any = None,
        native_type: Any = None,
        native_version: Any = None,
    ) -> TraceEvent:
        semantic = {
            "type": event_type,
            "timestamp": timestamp,
            "id": event_id,
            "parent_id": parent_id,
            "content": content,
        }
        return TraceEvent(
            schema_version=TRACE_SCHEMA_VERSION,
            provider=self.name,
            position={"trace": context.trace, "seq": context.seq},
            event=semantic,
            relationships=relationships,
            usage=usage,
            native={"type": native_type, "version": native_version, "record": record.value},
        )

    def unknown(self, record: SourceRecord, context: ParseContext) -> TraceEvent:
        value = record.value
        native_type = value.get("type") if isinstance(value, dict) else None
        native_version = _native_version(value)
        return self.event(
            record, context, "unknown", content=None,
            native_type=native_type, native_version=native_version,
        )

    def parse_error(self, record: SourceRecord, context: ParseContext) -> TraceEvent:
        return self.event(
            record, context, "parse_error",
            content={"line": record.line, "error": record.error},
        )


def source_records(path: Path) -> Iterable[SourceRecord]:
    with path.open(encoding="utf-8", errors="replace") as handle:
        for seq, line in enumerate(handle):
            try:
                value = json.loads(line)
            except json.JSONDecodeError as error:
                yield SourceRecord(seq=seq, line=line.rstrip("\n"), error=str(error))
                continue
            yield SourceRecord(seq=seq, value=value, line=line.rstrip("\n"))


def model_context(records: Iterable[SourceRecord]) -> tuple[object | None, object | None]:
    model = None
    reasoning = None
    for record in records:
        if not isinstance(record.value, dict):
            continue
        values = [record.value]
        values.extend(
            value for key in ("payload", "message", "meta", "session")
            if isinstance((value := record.value.get(key)), dict)
        )
        for value in values:
            model = value.get("model", model)
            reasoning = value.get("effort", value.get("reasoning", reasoning))
    return model, reasoning


def _native_version(value: Any) -> Any:
    if not isinstance(value, dict):
        return None
    for key in ("version", "schema_version", "schemaVersion"):
        if key in value:
            return value[key]
    payload = value.get("payload")
    if isinstance(payload, dict):
        for key in ("version", "schema_version", "schemaVersion"):
            if key in payload:
                return payload[key]
    return None


def trace_events(harness: AgentHarness, path: Path, trace: str) -> list[dict[str, Any]]:
    result = []
    for record in source_records(path):
        context = ParseContext(trace=trace, seq=record.seq)
        event = harness.parse_error(record, context) if record.error else harness.parse_record(record, context)
        result.append(event.to_dict())
    return result
