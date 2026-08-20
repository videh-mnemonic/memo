from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from memo.agents.harnesses.claude import ClaudeHarness
from memo.agents.harnesses.codex import CodexHarness
from memo.agents.run_metadata import AgentRunMetadata
from memo.agents.session_import import import_native_sessions
from memo.export import trace_json
from memo.recording.paths import StoragePaths
from memo.recording.store import SessionStore


@pytest.fixture(autouse=True)
def _configured_fake_s3(monkeypatch) -> None:
    monkeypatch.setenv("MEMO_S3_BUCKET", "test-bucket")
    monkeypatch.setattr(
        "memo.transport.inspect_archived_agent_runs",
        lambda *args, **kwargs: ([], set()),
    )


def _record(session_id: str, cwd: Path, content: str) -> str:
    return (
        json.dumps(
            {
                "timestamp": "2026-08-15T12:00:00Z",
                "type": "session_meta",
                "payload": {"id": session_id, "cwd": str(cwd), "model": "gpt-test"},
            }
        )
        + "\n"
        + json.dumps(
            {
                "timestamp": "2026-08-15T12:01:00Z",
                "type": "response_item",
                "payload": {"type": "message", "role": "assistant", "content": content},
            }
        )
        + "\n"
    )


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

    source.write_text(
        source.read_text()
        + json.dumps(
            {
                "timestamp": "2026-08-15T12:02:00Z",
                "type": "response_item",
                "payload": {"type": "message", "role": "assistant", "content": "second"},
            }
        )
        + "\n"
    )
    second = import_native_sessions(paths)

    assert second.refreshed == ["native-session"]
    assert store.head("native-session").step == 1
    assert len(json.loads(trace_json("native-session", paths=paths))) == 3


def test_import_splits_completed_agent_only_session_continuation(
    tmp_path: Path, monkeypatch
) -> None:
    _, codex = _roots(monkeypatch, tmp_path)
    root = tmp_path / "project"
    root.mkdir()
    source = codex / "session.jsonl"
    source.write_text(_record("native-session", root, "first"))
    paths = StoragePaths(tmp_path / "memo-home")
    import_native_sessions(paths)

    store = SessionStore(paths)
    session = store.load_session("native-session")
    session.state = "complete"
    session.last_pushed_step = store.head("native-session").step
    session.last_pushed_digest = "a" * 64
    session.remote_object = "memo/sessions/native-session/generations/00000000.tar.zst"
    store.update_session(session)
    source.write_text(
        source.read_text()
        + json.dumps(
            {
                "timestamp": "2026-08-15T12:02:00Z",
                "type": "response_item",
                "payload": {"type": "message", "role": "assistant", "content": "second"},
            }
        )
        + "\n"
    )

    summary = import_native_sessions(paths)

    assert len(summary.imported) == 1
    assert summary.imported[0] != "native-session"
    assert summary.refreshed == []
    assert store.head("native-session").step == 0
    assert len(json.loads(trace_json("native-session", paths=paths))) == 2
    continuation = store.load_session(summary.imported[0])
    assert (continuation.state, continuation.capture_scope) == ("active", "agent-only")
    assert store.head(summary.imported[0]).step == 0
    assert len(json.loads(trace_json(summary.imported[0], paths=paths))) == 3
    continuation_path = store.session_path(summary.imported[0])
    run_metadata = AgentRunMetadata.load(
        next((continuation_path / "agents" / "runs").glob("*.json"))
    )
    assert run_metadata.continued_from_session_id == "native-session"
    assert run_metadata.continued_from_trace_size == next(
        AgentRunMetadata.load(path).trace_complete_size
        for path in (store.session_path("native-session") / "agents" / "runs").glob("*.json")
    )
    assert run_metadata.continued_from_trace_digest == next(
        AgentRunMetadata.load(path).trace_digest
        for path in (store.session_path("native-session") / "agents" / "runs").glob("*.json")
    )
    repeated = import_native_sessions(paths)
    assert repeated.imported == []
    assert repeated.skipped == ["codex:native-session"]
    source.write_text(
        source.read_text()
        + json.dumps(
            {
                "timestamp": "2026-08-15T12:03:00Z",
                "type": "response_item",
                "payload": {"type": "message", "role": "assistant", "content": "third"},
            }
        )
        + "\n"
    )
    refreshed = import_native_sessions(paths)
    assert refreshed.refreshed == [summary.imported[0]]
    updated_metadata = AgentRunMetadata.load(
        next((continuation_path / "agents" / "runs").glob("*.json"))
    )
    assert updated_metadata.continued_from_session_id == run_metadata.continued_from_session_id
    assert updated_metadata.continued_from_trace_size == run_metadata.continued_from_trace_size
    assert updated_metadata.continued_from_trace_digest == run_metadata.continued_from_trace_digest


def test_import_splits_remote_completed_agent_only_session_continuation(
    tmp_path: Path, monkeypatch
) -> None:
    _, codex = _roots(monkeypatch, tmp_path)
    root = tmp_path / "project"
    root.mkdir()
    source = codex / "session.jsonl"
    original = _record("native-session", root, "first")
    source.write_text(
        original
        + json.dumps(
            {
                "timestamp": "2026-08-15T12:02:00Z",
                "type": "response_item",
                "payload": {"type": "message", "role": "assistant", "content": "second"},
            }
        )
        + "\n"
    )
    monkeypatch.setattr(
        "memo.transport.inspect_archived_agent_runs",
        lambda *args, **kwargs: (
            [
                {
                    "session_id": "remote-memo-session",
                    "capture_scope": "agent-only",
                    "harness": "codex",
                    "native_id": "native-session",
                    "complete_size": len(original.encode()),
                    "digest": hashlib.sha256(original.encode()).hexdigest(),
                    "state": "complete",
                }
            ],
            {"remote-memo-session"},
        ),
    )
    pulls = []
    monkeypatch.setattr(
        "memo.transport.pull_session",
        lambda session_id, paths, config, force=False, client=None: pulls.append(
            (session_id, force)
        ),
    )
    paths = StoragePaths(tmp_path / "memo-home")

    summary = import_native_sessions(paths)

    assert len(summary.imported) == 1
    assert summary.imported[0] not in {"native-session", "remote-memo-session"}
    assert summary.refreshed == []
    assert len(json.loads(trace_json(summary.imported[0], paths=paths))) == 3
    run_metadata = AgentRunMetadata.load(
        next(
            (SessionStore(paths).session_path(summary.imported[0]) / "agents" / "runs").glob(
                "*.json"
            )
        )
    )
    assert run_metadata.continued_from_session_id == "remote-memo-session"
    assert run_metadata.continued_from_trace_size == len(original.encode())
    assert run_metadata.continued_from_trace_digest == hashlib.sha256(original.encode()).hexdigest()
    assert pulls == [("remote-memo-session", True)]


def test_import_skips_when_completed_full_session_covers_trace(tmp_path: Path, monkeypatch) -> None:
    _, codex = _roots(monkeypatch, tmp_path)
    root = tmp_path / "project"
    root.mkdir()
    source = codex / "session.jsonl"
    content = _record("native-session", root, "first")
    source.write_text(content)
    monkeypatch.setattr(
        "memo.transport.inspect_archived_agent_runs",
        lambda *args, **kwargs: (
            [
                {
                    "session_id": "full-memo-session",
                    "capture_scope": "partial",
                    "harness": "codex",
                    "native_id": "native-session",
                    "complete_size": len(content.encode()),
                    "digest": hashlib.sha256(content.encode()).hexdigest(),
                    "state": "complete",
                }
            ],
            {"full-memo-session"},
        ),
    )
    paths = StoragePaths(tmp_path / "memo-home")

    summary = import_native_sessions(paths)

    assert summary.skipped == ["codex:native-session"]
    assert summary.imported == []


def test_import_dry_run_reports_without_writing(tmp_path: Path, monkeypatch) -> None:
    _, codex = _roots(monkeypatch, tmp_path)
    root = tmp_path / "project"
    root.mkdir()
    source = codex / "session.jsonl"
    source.write_text(_record("native-session", root, "first"))
    paths = StoragePaths(tmp_path / "memo-home")

    preview = import_native_sessions(paths, dry_run=True)

    assert preview.imported == ["native-session"]
    assert not SessionStore(paths).session_path("native-session").exists()

    import_native_sessions(paths)
    source.write_text(
        source.read_text()
        + json.dumps(
            {
                "timestamp": "2026-08-15T12:02:00Z",
                "type": "response_item",
                "payload": {"type": "message", "role": "assistant", "content": "second"},
            }
        )
        + "\n"
    )
    refresh = import_native_sessions(paths, dry_run=True)

    assert refresh.refreshed == ["native-session"]
    assert SessionStore(paths).head("native-session").step == 0


def test_import_is_idempotent_and_preserves_divergent_archive(
    tmp_path: Path,
    monkeypatch,
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
