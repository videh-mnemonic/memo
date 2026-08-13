from __future__ import annotations

from pathlib import Path

from memo.harnesses import get_harness
from memo.harnesses.harness import source_records, trace_events


FIXTURE = Path(__file__).parents[1] / "fixtures" / "harnesses" / "claude" / "recognized.jsonl"


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
