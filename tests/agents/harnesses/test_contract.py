from __future__ import annotations

from pathlib import Path

from memo.agents.harnesses import get_harness, registered_harnesses
from memo.agents.harnesses.base import ParseContext, SourceRecord, TraceEvent


def test_registry_contract() -> None:
    harnesses = registered_harnesses()
    assert {harness.name for harness in harnesses} == {"claude", "codex"}
    for harness in harnesses:
        assert get_harness(harness.name) is harness
        assert harness.executable
        assert harness.default_trace_roots()
        result = harness.parse_record(
            SourceRecord(0, {"type": "unsupported"}), ParseContext("001", 0)
        )
        assert isinstance(result, TraceEvent)
        assert result.provider == harness.name
        assert result.event["type"] == "unknown"


def test_resume_contract() -> None:
    claude = get_harness("claude")
    codex = get_harness("codex")
    assert claude.parse_resume(["--resume", "session"]) == "session"
    assert claude.parse_resume(["-r", "session"]) == "session"
    assert codex.parse_resume(["resume", "session"]) == "session"
    assert claude.identify_session([], Path("claude-session.jsonl")) == "claude-session"
    assert codex.identify_session([], Path("codex-session.jsonl")) == "codex-session"


def test_custom_provider_state_controls_trace_roots(tmp_path: Path, monkeypatch) -> None:
    codex_home = tmp_path / "codex"
    claude_home = tmp_path / "claude"
    monkeypatch.setenv("CODEX_HOME", str(codex_home))
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(claude_home))

    assert get_harness("codex").default_trace_roots() == (codex_home / "sessions",)
    assert get_harness("claude").default_trace_roots() == (claude_home / "projects",)
