from __future__ import annotations

import json
from pathlib import Path

from memo.agents.collector import TraceCollector
from memo.recording.paths import StoragePaths
from memo.recording.models import DirectorySession, SessionOrigin
from memo.daemon.registry import AgentLaunch, Registry
from memo.recording.store import SessionStore
from memo.agents.shim import ensure_shims, run as run_shim
from memo.agents.tracewatch import TraceCheckpoint, capture, changed, snapshot_complete


def _record(session_id: str, cwd: Path, content: str) -> str:
    return json.dumps({
        "session_id": session_id,
        "cwd": str(cwd.resolve()),
        "type": "user",
        "content": content,
    }) + "\n"


def test_checkpoint_finds_every_change_and_round_trips(tmp_path: Path) -> None:
    root = tmp_path / "traces"
    root.mkdir()
    existing = root / "existing.jsonl"
    existing.write_text("{}\n")
    checkpoint = capture((root,))
    existing.write_text("{}\n{}\n")
    created = root / "created.jsonl"
    created.write_text("{}\n")

    restored = TraceCheckpoint.from_json(checkpoint.to_json())
    assert changed((root,), restored) == [created, existing]


def test_snapshot_stops_at_observed_complete_newline(tmp_path: Path) -> None:
    source = tmp_path / "source.jsonl"
    destination = tmp_path / "copy.jsonl"
    source.write_bytes(b'{"complete":true}\n{"partial":')

    state, boundary = snapshot_complete(source, destination)

    assert state.size == source.stat().st_size
    assert boundary == len(b'{"complete":true}\n')
    assert destination.read_bytes() == b'{"complete":true}\n'


def test_collector_archives_all_matching_sessions_and_updates_resume(
    tmp_path: Path, monkeypatch
) -> None:
    trace_root = tmp_path / "native"
    trace_root.mkdir()
    project = tmp_path / "project"
    project.mkdir()
    other = tmp_path / "other"
    other.mkdir()
    paths = StoragePaths(tmp_path / "home")
    paths.ensure_storage()
    store = SessionStore(paths)
    store.create(DirectorySession(
        "memo-session", str(project.resolve()), "start", "start",
        SessionOrigin("1.0.0", "user", "host"),
    ))
    registry = Registry(paths.registry)
    registry.create(project, "start", "memo-session")
    registry.allocate_attachment("memo-session", "start", "terminal")
    registry.allocate_attachment("memo-session", "start", "terminal-two")
    monkeypatch.setenv("MEMO_TRACE_DIR", str(trace_root))
    old = trace_root / "old.jsonl"
    old.write_text(_record("old", project, "before launch"))
    checkpoint = capture((trace_root,)).to_json()
    registry.create_window("memo-session", "claude", str(project.resolve()), checkpoint)
    registry.add_launch(AgentLaunch(
        "one", "memo-session", "terminal", "claude", str(project.resolve()),
        ["claude"], "one",
    ))
    registry.add_launch(AgentLaunch(
        "two", "memo-session", "terminal-two", "claude", str(project.resolve()),
        ["claude", "--model", "other"], "one-and-a-half",
    ))
    first = trace_root / "first.jsonl"
    second = trace_root / "second.jsonl"
    unrelated = trace_root / "unrelated.jsonl"
    pending = _record("native-one", project, "pending").rstrip("\n")
    first.write_text(_record("native-one", project, "first") + pending[:-1])
    second.write_text(_record("native-two", project, "second"))
    unrelated.write_text(_record("native-other", other, "ignore"))

    collector = TraceCollector(store, registry)
    assert len(collector.collect("memo-session")) == 2
    run_files = sorted((store.session_path("memo-session") / "agents/runs").glob("*.json"))
    assert len(run_files) == 2
    assert {json.loads(path.read_text())["agent_session_id"] for path in run_files} == {
        "native-one", "native-two",
    }
    assert all("before launch" not in path.read_text() for path in (
        store.session_path("memo-session") / "agents/traces"
    ).glob("*.jsonl"))

    registry.finish_launch("one", "two", 0)
    registry.finish_launch("two", "two", 0)
    collector.collect("memo-session")
    assert registry.windows("memo-session")
    first.write_text(first.read_text() + "}\n")
    collector.collect("memo-session")
    assert registry.windows("memo-session") == []
    registry.create_window(
        "memo-session", "claude", str(project.resolve()), capture((trace_root,)).to_json()
    )
    registry.add_launch(AgentLaunch(
        "resume", "memo-session", "terminal", "claude", str(project.resolve()),
        ["claude", "--resume", "native-one"], "three",
    ))
    first.write_text(first.read_text() + _record("native-one", project, "resumed"))
    collector.collect("memo-session")
    assert len(list((store.session_path("memo-session") / "agents/runs").glob("*.json"))) == 2
    metadata = next(
        json.loads(path.read_text()) for path in run_files
        if json.loads(path.read_text())["agent_session_id"] == "native-one"
    )
    assert "resumed" in (
        store.session_path("memo-session") / "agents/traces" / metadata["trace_file"]
    ).read_text()
    registry.close()


def test_shims_are_derived_from_registered_harnesses(tmp_path: Path) -> None:
    directory = ensure_shims(StoragePaths(tmp_path / "home"))
    assert {path.name for path in directory.iterdir()} == {"claude", "codex"}
    assert all(path.stat().st_mode & 0o100 for path in directory.iterdir())


def test_shim_notifies_around_real_process_and_preserves_status(
    tmp_path: Path, monkeypatch
) -> None:
    paths = StoragePaths(tmp_path / "home")
    shim_directory = ensure_shims(paths)
    binaries = tmp_path / "bin"
    binaries.mkdir()
    real = binaries / "claude"
    real.write_text("#!/bin/sh\nexit 7\n")
    real.chmod(0o700)
    messages = []
    monkeypatch.setenv("PATH", f"{shim_directory}:{binaries}")
    monkeypatch.setenv("MEMO_SHIM_DIR", str(shim_directory))
    monkeypatch.setenv("MEMO_SESSION_ID", "session")
    monkeypatch.setenv("MEMO_TERMINAL_ID", "terminal")
    monkeypatch.setattr("memo.agents.shim.StoragePaths.discover", lambda: paths)
    monkeypatch.setattr("memo.agents.shim.ensure_daemon", lambda _: None)
    monkeypatch.setattr(
        "memo.agents.shim.request",
        lambda _socket, operation, payload, **_kwargs: messages.append((operation, payload)) or {},
    )

    assert run_shim("claude", ["--model", "test"]) == 7
    assert [operation for operation, _ in messages] == ["agent_launch", "agent_complete"]
    assert messages[0][1]["command"] == ["claude", "--model", "test"]
    assert messages[1][1]["exit_code"] == 7
