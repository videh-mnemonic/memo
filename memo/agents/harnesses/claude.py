from __future__ import annotations

"""Claude native trace integration."""

import re
from pathlib import Path
from typing import Any, Iterable, Sequence

from .harness import AgentHarness, ParseContext, SourceRecord, TraceEvent


def _flag_resume(args: Sequence[str]) -> str | None:
    for flag in ("--resume", "-r"):
        if flag in args:
            index = args.index(flag)
            if index + 1 < len(args):
                return args[index + 1]
    return None


def _filename_id(path: Path) -> str:
    match = re.search(r"([0-9a-fA-F]{8}-[0-9a-fA-F-]{27,})$", path.stem)
    return match.group(1) if match else path.stem


def _first_string(record: dict[str, Any], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = record.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _version(value: dict[str, Any]) -> Any:
    return value.get("version", value.get("schema_version", value.get("schemaVersion")))


def _content_blocks(message: dict[str, Any]) -> list[dict[str, Any]]:
    content = message.get("content")
    if not isinstance(content, list):
        return []
    return [item for item in content if isinstance(item, dict)]


class ClaudeHarness(AgentHarness):
    name = "claude"
    executable = "claude"

    def default_trace_roots(self) -> tuple[Path, ...]:
        return (Path.home() / ".claude" / "projects",)

    def parse_resume(self, args: Sequence[str]) -> str | None:
        return _flag_resume(args)

    def identify_session(self, records: Iterable[SourceRecord], path: Path) -> str:
        keys = ("session_id", "sessionId", "conversation_id", "conversationId", "id")
        for source in records:
            if not isinstance(source.value, dict):
                continue
            value = _first_string(source.value, keys)
            if value:
                return value
            for container_key in ("message", "meta", "session"):
                container = source.value.get(container_key)
                if isinstance(container, dict):
                    value = _first_string(container, keys)
                    if value:
                        return value
        return _filename_id(path)

    def parse_record(self, record: SourceRecord, context: ParseContext) -> TraceEvent:
        value = record.value
        if not isinstance(value, dict):
            return self.unknown(record, context)
        message = value.get("message") if isinstance(value.get("message"), dict) else value
        kind = str(value.get("type", "")).lower()
        role = str(message.get("role", value.get("role", ""))).lower()
        timestamp = value.get("timestamp") or value.get("created_at") or message.get("timestamp")
        event_id = value.get("uuid") or value.get("id") or message.get("id")
        parent_id = value.get("parentUuid") or value.get("parent_id") or message.get("parent_id")
        usage = message.get("usage", value.get("usage"))
        common = {
            "timestamp": timestamp,
            "event_id": event_id,
            "parent_id": parent_id,
            "usage": usage,
            "native_type": value.get("type"),
            "native_version": _version(value),
        }
        blocks = _content_blocks(message)
        tool_use = next((item for item in blocks if item.get("type") == "tool_use"), None)
        tool_result = next((item for item in blocks if item.get("type") == "tool_result"), None)
        if kind == "tool_use":
            tool_use = value
        elif kind == "tool_result":
            tool_result = value
        if tool_use is not None:
            call_id = tool_use.get("id") or tool_use.get("call_id")
            relationships = {"call_id": call_id} if call_id is not None else None
            content = {
                "tool_name": tool_use.get("name") or tool_use.get("tool_name"),
                "arguments": tool_use.get("input", tool_use.get("arguments")),
            }
            return self.event(
                record, context, "tool_call", content=content,
                relationships=relationships, **common,
            )
        if tool_result is not None:
            call_id = tool_result.get("tool_use_id") or tool_result.get("call_id")
            relationships = {"call_id": call_id} if call_id is not None else None
            return self.event(
                record, context, "tool_result",
                content=tool_result.get("content", tool_result.get("output")),
                relationships=relationships, **common,
            )
        if role == "user" or kind in {"user", "human"}:
            event_type = "user_input"
        elif role == "assistant" or kind in {"assistant", "agent"}:
            event_type = "agent_message"
        else:
            return self.unknown(record, context)
        content = message.get("content", message.get("text", message))
        return self.event(record, context, event_type, content=content, **common)
