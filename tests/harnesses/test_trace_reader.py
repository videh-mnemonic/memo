from __future__ import annotations

from pathlib import Path

from memo.harnesses import get_harness
from memo.harnesses.harness import all_traces, source_records, trace_events


def test_source_records_represent_every_physical_line(tmp_path: Path) -> None:
    path = tmp_path / "mixed.jsonl"
    path.write_bytes(b'{"ok":"\xff"}\n\n[1,2]\n42\nnull\n{broken\n')

    records = list(source_records(path))

    assert [record.seq for record in records] == list(range(6))
    assert records[0].value == {"ok": "\ufffd"}
    assert records[1].error is not None
    assert records[2].value == [1, 2]
    assert records[3].value == 42
    assert records[4].value is None
    assert records[5].line == "{broken"


def test_trace_events_include_unknown_and_parse_errors(tmp_path: Path) -> None:
    path = tmp_path / "mixed.jsonl"
    path.write_text('[1]\nnull\n\n{broken\n')

    events = trace_events(get_harness("claude"), path, "007")

    assert [item["event"]["type"] for item in events] == [
        "unknown", "unknown", "parse_error", "parse_error",
    ]
    assert [item["position"] for item in events] == [
        {"trace": "007", "seq": index} for index in range(4)
    ]
    assert events[0]["native"]["record"] == [1]
    assert events[1]["native"]["record"] is None
    assert events[2]["event"]["content"]["line"] == ""


def test_all_traces_orders_legs_and_preserves_raw_values(tmp_path: Path) -> None:
    traces = tmp_path / "traces"
    traces.mkdir()
    (traces / "leg-010.jsonl").write_text('{"type":"user","content":"ten"}\n')
    (traces / "leg-002.jsonl").write_text('{"type":"user","content":"two"}\ninvalid\n')
    (traces / "leg-001.jsonl").write_text('null\n')
    harness = get_harness("claude")

    events = all_traces(harness, tmp_path)
    raw = all_traces(harness, tmp_path, raw=True)
    bounded = all_traces(harness, tmp_path, through_leg=2)

    assert [(item["position"]["trace"], item["position"]["seq"]) for item in events] == [
        ("001", 0), ("002", 0), ("002", 1), ("010", 0),
    ]
    assert raw == [None, {"type": "user", "content": "two"}, {"type": "user", "content": "ten"}]
    assert [item["position"]["trace"] for item in bounded] == ["001", "002", "002"]
