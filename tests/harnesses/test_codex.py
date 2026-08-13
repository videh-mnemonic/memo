from __future__ import annotations

from pathlib import Path

from memo.harnesses import get_harness
from memo.harnesses.harness import source_records, trace_events


FIXTURE = Path(__file__).parents[1] / "fixtures" / "harnesses" / "codex" / "recognized.jsonl"


def test_codex_nested_records() -> None:
    harness = get_harness("codex")
    assert harness.identify_session(source_records(FIXTURE), FIXTURE) == "12345678-1234-1234-1234-123456789abc"
    events = trace_events(harness, FIXTURE, "001")
    assert [item["event"]["type"] for item in events] == [
        "unknown", "user_input", "tool_call", "tool_result",
    ]
    assert events[1]["event"]["content"] == "hello"
    assert events[2]["event"]["content"] == {
        "tool_name": "shell", "arguments": '{"cmd":"pytest"}',
    }
    assert events[2]["relationships"] == {"call_id": "call-1"}
    assert events[3]["event"]["content"] == "ok"
    assert events[3]["native"]["record"]["payload"]["call_id"] == "call-1"
