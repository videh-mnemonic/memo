from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest

from memo.agents.run_metadata import AgentRunMetadata


def _metadata() -> AgentRunMetadata:
    return AgentRunMetadata(
        run_id="run",
        harness="codex",
        model="gpt",
        reasoning="high",
        command=["codex", "resume", "native"],
        cwd="/work",
        started_utc="2026-08-16T00:00:00Z",
        ended_utc="2026-08-16T00:01:00Z",
        exit_code=0,
        agent_session_id="native",
        trace_file="run.jsonl",
        trace_complete_size=12,
        trace_digest="a" * 64,
    )


def test_agent_run_metadata_round_trips_without_false_import_marker(tmp_path: Path) -> None:
    path = tmp_path / "run.json"
    expected = _metadata()

    expected.write(path)

    assert AgentRunMetadata.load(path) == expected
    assert "imported_agent_only" not in json.loads(path.read_text())


@pytest.mark.parametrize(
    "metadata",
    [
        replace(_metadata(), trace_file="../trace.jsonl"),
        replace(_metadata(), trace_complete_size=-1),
        replace(_metadata(), trace_digest="invalid"),
        replace(_metadata(), command=["codex", 1]),
    ],
)
def test_agent_run_metadata_rejects_invalid_storage_values(
    metadata: AgentRunMetadata,
) -> None:
    with pytest.raises(ValueError):
        metadata.validate()
