from __future__ import annotations

from pathlib import Path

from memo.agents.harnesses import get_harness
from memo.agents.harnesses.base import source_records, trace_events


FIXTURE = Path(__file__).parents[1] / "fixtures" / "harnesses" / "claude" / "recognized.jsonl"
MIXED_FIXTURE = FIXTURE.with_name("mixed-records.jsonl")


def test_claude_recognized_messages_and_identity() -> None:
    harness = get_harness("claude")
    assert harness.identify_session(source_records(FIXTURE), FIXTURE) == "claude-session"
    events = trace_events(harness, FIXTURE, "001")
    assert [item["event"]["type"] for item in events] == ["user_input", "agent_message"]
    assert events[0]["schema_version"] == 1
    assert events[0]["provider"] == "claude"
    assert events[0]["position"] == {"trace": "001", "seq": 0}
    assert events[0]["event"] == {
        "type": "user_input", "timestamp": "2026-01-01T00:00:00Z",
        "id": "user-1", "parent_id": None, "content": "fix it",
    }
    assert events[1]["event"]["parent_id"] == "user-1"
    assert events[0]["native"]["record"]["session_id"] == "claude-session"


def test_claude_unknown_preserves_native_record(tmp_path: Path) -> None:
    path = tmp_path / "unknown.jsonl"
    path.write_text('{"type":"future_claude","detail":{"value":1}}\n')
    event = trace_events(get_harness("claude"), path, "001")[0]
    assert event["event"]["type"] == "unknown"
    assert event["native"]["record"] == {"type": "future_claude", "detail": {"value": 1}}


def test_claude_mixed_records_are_complete_and_granular() -> None:
    events = trace_events(get_harness("claude"), MIXED_FIXTURE, "002")

    assert [item["event"]["type"] for item in events] == [
        "tool_call", "tool_result", "unknown", "unknown", "parse_error",
    ]
    assert events[0]["event"] == {
        "type": "tool_call", "timestamp": "2026-01-01T00:00:02Z",
        "id": "assistant-2", "parent_id": "user-1",
        "content": {"tool_name": "Bash", "arguments": {"command": "pytest"}},
    }
    assert events[0]["relationships"] == {"call_id": "tool-1"}
    assert events[0]["usage"] == {"input_tokens": 10, "output_tokens": 4}
    assert events[0]["native"]["version"] == "claude-v1"
    assert events[1]["relationships"] == {"call_id": "tool-1"}
    assert events[1]["event"]["content"] == "2 passed"
    assert events[2]["native"]["record"]["detail"] == {"retained": True}
    assert events[2]["native"]["version"] == 7
    assert events[3]["native"]["record"] == ["non-object", 1]
    assert events[4]["event"]["content"]["line"] == "not json"
