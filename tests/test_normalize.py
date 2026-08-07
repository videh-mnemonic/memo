from __future__ import annotations

import json
from pathlib import Path

from memo.normalize import normalize_trace
from memo.tracewatch import session_id


def test_codex_nested_records(tmp_path: Path) -> None:
    trace = tmp_path / "rollout-2026-01-01-12345678-1234-1234-1234-123456789abc.jsonl"
    values = [
        {"type": "session_meta", "payload": {"id": "12345678-1234-1234-1234-123456789abc"}},
        {"timestamp": "now", "type": "event_msg", "payload": {"type": "user_message", "message": "hello"}},
        {"type": "response_item", "payload": {"type": "function_call", "name": "shell", "arguments": "{}"}},
        {"type": "response_item", "payload": {"type": "function_call_output", "output": "ok"}},
    ]
    trace.write_text("".join(json.dumps(v) + "\n" for v in values))
    assert session_id(trace) == "12345678-1234-1234-1234-123456789abc"
    normalized = normalize_trace(trace, "001")
    assert [item["type"] for item in normalized] == ["user_input", "tool_call", "tool_result"]
    assert normalized[0]["content"] == "hello"
