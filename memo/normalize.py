from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

KINDS = {"user_input", "agent_message", "tool_call", "tool_result"}


def _content(record: dict[str, Any]) -> Any:
    for key in ("content", "message", "text", "input", "output", "payload"):
        if key in record:
            return record[key]
    return record


def _type(record: dict[str, Any]) -> str | None:
    kind = str(record.get("type", "")).lower()
    role = str(record.get("role", "")).lower()
    message = record.get("message")
    if isinstance(message, dict):
        role = str(message.get("role", role)).lower()
    if kind in KINDS:
        return kind
    if role == "user" or kind in {"user", "human", "prompt", "user_message"}:
        return "user_input"
    if role == "assistant" or kind in {"assistant", "agent", "assistant_message", "message"}:
        return "agent_message"
    if kind in {"function_call_output", "custom_tool_call_output"} or (
        "tool" in kind and any(word in kind for word in ("result", "output", "response"))
    ):
        return "tool_result"
    if "tool" in kind or kind in {"function_call", "function"}:
        return "tool_call"
    return None


def records(trace: Path) -> Iterable[dict[str, Any]]:
    with trace.open(errors="replace") as handle:
        for line in handle:
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                yield value


def normalize_trace(trace: Path, leg: str, raw: bool = False) -> list[dict[str, Any]]:
    result = []
    for seq, record in enumerate(records(trace)):
        if raw:
            result.append({**record, "leg": leg})
            continue
        payload = record.get("payload")
        event = payload if isinstance(payload, dict) else record
        # Codex wraps user messages in event_msg and assistant/tool items in response_item.
        kind = _type(event)
        if kind is None:
            continue
        message = event.get("message")
        body = _content(message) if isinstance(message, dict) else _content(event)
        tool_name = event.get("tool_name") or event.get("name")
        result.append({
            "leg": leg, "seq": seq,
            "timestamp": record.get("timestamp") or record.get("created_at") or record.get("time"),
            "type": kind, "tool_name": tool_name, "content": body,
        })
    return result


def all_traces(unpacked: Path, raw: bool = False, through_leg: int | None = None) -> list[dict[str, Any]]:
    result = []
    for trace in sorted((unpacked / "traces").glob("leg-*.jsonl")):
        leg = trace.stem.removeprefix("leg-")
        if through_leg is not None and int(leg) > through_leg:
            continue
        result.extend(normalize_trace(trace, leg, raw))
    return result
