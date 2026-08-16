from __future__ import annotations

from pathlib import Path

from memo.agents.harnesses import get_harness
from memo.agents.harnesses.harness import source_records, trace_events


FIXTURE = Path(__file__).parents[1] / "fixtures" / "harnesses" / "codex" / "recognized.jsonl"
MIXED_FIXTURE = FIXTURE.with_name("mixed-records.jsonl")


def test_codex_nested_records() -> None:
    harness = get_harness("codex")
    assert harness.identify_session(source_records(FIXTURE), FIXTURE) == "12345678-1234-1234-1234-123456789abc"
    events = trace_events(harness, FIXTURE, "001")
    assert [item["event"]["type"] for item in events] == [
        "metadata", "user_input", "tool_call", "tool_result",
    ]
    assert events[1]["event"]["content"] == "hello"
    assert events[2]["event"]["content"] == {
        "tool_name": "shell", "arguments": '{"cmd":"pytest"}',
    }
    assert events[2]["relationships"] == {"call_id": "call-1"}
    assert events[3]["event"]["content"] == "ok"
    assert events[3]["native"]["record"]["payload"]["call_id"] == "call-1"


def test_codex_mixed_records_are_complete_and_granular() -> None:
    events = trace_events(get_harness("codex"), MIXED_FIXTURE, "003")

    assert [item["event"]["type"] for item in events] == [
        "metadata", "user_input", "tool_call", "tool_result", "usage",
        "unknown", "unknown", "parse_error",
    ]
    assert events[0]["native"]["version"] == "codex-v1"
    assert events[0]["event"]["content"]["model"] == "gpt"
    assert events[2]["event"]["id"] == "item-1"
    assert events[2]["event"]["parent_id"] == "message-1"
    assert events[2]["relationships"] == {"call_id": "call-1"}
    assert events[3]["event"]["content"] == {"exit_code": 0, "text": "2 passed"}
    assert events[4]["usage"] == {"input_tokens": 20, "output_tokens": 5}
    assert events[5]["native"]["record"]["payload"] == {"detail": {"retained": True}}
    assert events[5]["native"]["version"] == 9
    assert events[6]["native"]["record"] is None
