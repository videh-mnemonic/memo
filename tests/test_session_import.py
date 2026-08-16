from __future__ import annotations

import json
from pathlib import Path

from memo.recording.paths import StoragePaths
from memo.agents.harnesses.claude import ClaudeHarness
from memo.agents.harnesses.codex import CodexHarness
from memo.agents.session_import import import_native_sessions
from memo.export import trace_json
from memo.recording.store import SessionStore


def _record(session_id: str, cwd: Path, content: str) -> str:
    return json.dumps({
        "timestamp": "2026-08-15T12:00:00Z",
        "type": "session_meta",
        "payload": {"id": session_id, "cwd": str(cwd), "model": "gpt-test"},
    }) + "\n" + json.dumps({
        "timestamp": "2026-08-15T12:01:00Z",
        "type": "response_item",
        "payload": {"type": "message", "role": "assistant", "content": content},
    }) + "\n"


def _roots(monkeypatch, tmp_path: Path) -> tuple[Path, Path]:
    claude = tmp_path / "claude"
    codex = tmp_path / "codex"
    claude.mkdir()
    codex.mkdir()
    monkeypatch.setattr(ClaudeHarness, "trace_roots", lambda self: (claude,))
    monkeypatch.setattr(CodexHarness, "trace_roots", lambda self: (codex,))
    return claude, codex


def test_import_creates_and_refreshes_agent_only_session(tmp_path: Path, monkeypatch) -> None:
    _, codex = _roots(monkeypatch, tmp_path)
    root = tmp_path / "project"
    root.mkdir()
    source = codex / "session.jsonl"
    source.write_text(_record("native-session", root, "first"))
    paths = StoragePaths(tmp_path / "memo-home")

    first = import_native_sessions(paths)

    assert first.imported == ["native-session"]
    store = SessionStore(paths)
    session = store.load_session("native-session")
    assert (session.state, session.capture_scope) == ("active", "agent-only")
    assert store.head("native-session").step == 0
    assert json.loads(trace_json("native-session", paths=paths))[1]["event"]["content"] == "first"

    source.write_text(source.read_text() + json.dumps({
        "timestamp": "2026-08-15T12:02:00Z", "type": "response_item",
        "payload": {"type": "message", "role": "assistant", "content": "second"},
    }) + "\n")
    second = import_native_sessions(paths)

    assert second.refreshed == ["native-session"]
    assert store.head("native-session").step == 1
    assert len(json.loads(trace_json("native-session", paths=paths))) == 3


def test_import_is_idempotent_and_preserves_divergent_archive(
    tmp_path: Path, monkeypatch,
) -> None:
    _, codex = _roots(monkeypatch, tmp_path)
    root = tmp_path / "project"
    root.mkdir()
    source = codex / "session.jsonl"
    source.write_text(_record("native-session", root, "first"))
    paths = StoragePaths(tmp_path / "memo-home")
    import_native_sessions(paths)

    unchanged = import_native_sessions(paths)
    assert unchanged.skipped == ["codex:native-session"]

    source.write_text(_record("native-session", root, "rewritten"))
    conflict = import_native_sessions(paths)
    assert conflict.failed
    assert "diverges" in conflict.failed[0][1]
    assert SessionStore(paths).head("native-session").step == 0


def test_shared_trace_override_identifies_provider_once(tmp_path: Path, monkeypatch) -> None:
    traces = tmp_path / "traces"
    traces.mkdir()
    root = tmp_path / "project"
    root.mkdir()
    (traces / "session.jsonl").write_text(_record("native-session", root, "first"))
    monkeypatch.setenv("MEMO_TRACE_DIR", str(traces))

    summary = import_native_sessions(StoragePaths(tmp_path / "memo-home"))

    assert summary.imported == ["native-session"]
    assert summary.failed == []
