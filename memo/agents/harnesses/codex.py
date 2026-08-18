"""Codex native trace integration."""

from __future__ import annotations

import os
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any

from .base import (
    AgentHarness,
    ParseContext,
    SourceRecord,
    TraceEvent,
    flag_resume,
    identify_session,
)


class CodexHarness(AgentHarness):
    name = "codex"
    executable = "codex"

    def default_trace_roots(self) -> tuple[Path, ...]:
        home = Path(os.environ.get("CODEX_HOME", "~/.codex")).expanduser()
        return (home / "sessions",)

    def parse_resume(self, args: Sequence[str]) -> str | None:
        flagged = flag_resume(args)
        if flagged:
            return flagged
        if args[:1] == ["resume"] and len(args) > 1:
            return args[1]
        return None

    def identify_session(self, records: Iterable[SourceRecord], path: Path) -> str:
        return identify_session(records, path, ("payload", "meta", "session"))

    def parse_record(self, record: SourceRecord, context: ParseContext) -> TraceEvent:
        outer = record.value
        if not isinstance(outer, dict):
            return self.unknown(record, context)
        payload = outer.get("payload")
        event = payload if isinstance(payload, dict) else outer
        kind = str(event.get("type", "")).lower()
        timestamp = outer.get("timestamp") or event.get("timestamp")
        event_id = event.get("id") or outer.get("id")
        parent_id = event.get("parent_id") or outer.get("parent_id")
        usage = event.get("usage", outer.get("usage"))
        common = {
            "timestamp": timestamp,
            "event_id": event_id,
            "parent_id": parent_id,
            "usage": usage,
            "native_type": outer.get("type"),
            "native_version": self._native_version(outer, event),
        }
        if str(outer.get("type", "")).lower() == "session_meta":
            return self.event(record, context, "metadata", content=event, **common)
        if kind in {"user", "user_message"}:
            return self.event(
                record,
                context,
                "user_input",
                content=event.get("message", event.get("content")),
                **common,
            )
        if kind in {"assistant", "assistant_message", "message"}:
            role = str(event.get("role", "")).lower()
            event_type = "user_input" if role == "user" else "agent_message"
            return self.event(
                record,
                context,
                event_type,
                content=event.get("message", event.get("content")),
                **common,
            )
        if kind in {"function_call", "custom_tool_call"}:
            call_id = event.get("call_id")
            content = {"tool_name": event.get("name"), "arguments": event.get("arguments")}
            relationships = {"call_id": call_id} if call_id is not None else None
            return self.event(
                record, context, "tool_call", content=content, relationships=relationships, **common
            )
        if kind in {"function_call_output", "custom_tool_call_output"}:
            call_id = event.get("call_id")
            relationships = {"call_id": call_id} if call_id is not None else None
            return self.event(
                record,
                context,
                "tool_result",
                content=event.get("output"),
                relationships=relationships,
                **common,
            )
        if kind in {"token_count", "usage"}:
            usage = self._usage(event)
            common["usage"] = usage
            return self.event(record, context, "usage", content=None, **common)
        return self.unknown(record, context)

    @staticmethod
    def _native_version(outer: dict[str, Any], event: dict[str, Any]) -> Any:
        for value in (outer, event):
            for key in ("version", "schema_version", "schemaVersion"):
                if key in value:
                    return value[key]
        return None

    @staticmethod
    def _usage(event: dict[str, Any]) -> Any:
        if "usage" in event:
            return event["usage"]
        info = event.get("info")
        if isinstance(info, dict):
            return info.get("total_token_usage", info.get("last_token_usage", info))
        return {
            key: event[key]
            for key in ("input_tokens", "output_tokens", "total_tokens")
            if key in event
        } or None
