from __future__ import annotations

from pathlib import Path
from typing import Iterable, Sequence

from .claude import _filename_id, _first_string, _flag_resume
from .harness import AgentHarness, ParseContext, SourceRecord, TraceEvent


class CodexHarness(AgentHarness):
    name = "codex"
    executable = "codex"

    def default_trace_roots(self) -> tuple[Path, ...]:
        return (Path.home() / ".codex" / "sessions",)

    def parse_resume(self, args: Sequence[str]) -> str | None:
        flagged = _flag_resume(args)
        if flagged:
            return flagged
        if args[:1] == ["resume"] and len(args) > 1:
            return args[1]
        return None

    def identify_session(self, records: Iterable[SourceRecord], path: Path) -> str:
        keys = ("session_id", "sessionId", "conversation_id", "conversationId", "id")
        for source in records:
            if not isinstance(source.value, dict):
                continue
            value = _first_string(source.value, keys)
            if value:
                return value
            for container_key in ("payload", "meta", "session"):
                container = source.value.get(container_key)
                if isinstance(container, dict):
                    value = _first_string(container, keys)
                    if value:
                        return value
        return _filename_id(path)

    def parse_record(self, record: SourceRecord, context: ParseContext) -> TraceEvent:
        outer = record.value
        if not isinstance(outer, dict):
            return self.unknown(record, context)
        payload = outer.get("payload")
        event = payload if isinstance(payload, dict) else outer
        kind = str(event.get("type", "")).lower()
        timestamp = outer.get("timestamp") or event.get("timestamp")
        common = {
            "timestamp": timestamp,
            "event_id": event.get("id"),
            "parent_id": event.get("parent_id"),
            "usage": event.get("usage"),
            "native_type": outer.get("type"),
            "native_version": outer.get("version"),
        }
        if kind in {"user", "user_message"}:
            return self.event(record, context, "user_input", content=event.get("message", event.get("content")), **common)
        if kind in {"assistant", "assistant_message", "message"}:
            return self.event(record, context, "agent_message", content=event.get("message", event.get("content")), **common)
        if kind in {"function_call", "custom_tool_call"}:
            call_id = event.get("call_id")
            content = {"tool_name": event.get("name"), "arguments": event.get("arguments")}
            relationships = {"call_id": call_id} if call_id is not None else None
            return self.event(record, context, "tool_call", content=content, relationships=relationships, **common)
        if kind in {"function_call_output", "custom_tool_call_output"}:
            call_id = event.get("call_id")
            relationships = {"call_id": call_id} if call_id is not None else None
            return self.event(record, context, "tool_result", content=event.get("output"), relationships=relationships, **common)
        return self.unknown(record, context)
