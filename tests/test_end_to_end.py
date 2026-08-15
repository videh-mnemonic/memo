from __future__ import annotations

import base64
import json
import os
import stat
from pathlib import Path

from memo.cli import main
from memo.config import Paths
from memo.protocol import request
from memo.session_store import SessionStore


def test_public_lifecycle_and_inspect_use_zero_based_steps(tmp_path: Path, monkeypatch, capsys) -> None:
    home = tmp_path / "memo-home"
    root = tmp_path / "work"
    root.mkdir()
    (root / "note.txt").write_text("recorded\n")
    monkeypatch.setenv("MEMO_HOME", str(home))
    monkeypatch.chdir(root)
    try:
        assert main(["background"]) == 0
        output = capsys.readouterr().out
        assert "started:" in output and "step=0" in output
        session_file = next(home.glob("archive/*/*/session.json"))
        session_dir = session_file.parent
        assert (session_dir / "HEAD").read_text() == "0\n"
        assert (session_dir / "steps/0.json").is_file()
        assert (session_dir / "snapshots/0/note.txt").read_text() == "recorded\n"
        assert main(["status"]) == 0
        status = capsys.readouterr().out
        assert "STEP" in status and session_dir.name in status
        assert main(["inspect", session_dir.name]) == 0
        inspected = capsys.readouterr().out
        assert f"Session: {session_dir.name}" in inspected
        assert "Step: 0" in inspected
        assert main(["end"]) == 0
        assert "step=1" in capsys.readouterr().out
        assert json.loads(session_file.read_text())["state"] == "complete"
    finally:
        socket = home / "runtime/memo.sock"
        if socket.exists():
            request(str(socket), "shutdown")


def test_background_joins_existing_directory_recording(tmp_path: Path, monkeypatch, capsys) -> None:
    home = tmp_path / "memo-home"
    root = tmp_path / "work"
    root.mkdir()
    monkeypatch.setenv("MEMO_HOME", str(home))
    try:
        assert main(["background", str(root)]) == 0
        capsys.readouterr()
        assert main(["background", str(root)]) == 0
        assert "joined:" in capsys.readouterr().out
    finally:
        socket = home / "runtime/memo.sock"
        if socket.exists():
            request(str(socket), "shutdown")


def test_public_traces_and_replay_use_step_bounded_terminal_input(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    home = tmp_path / "memo-home"
    root = tmp_path / "work"
    root.mkdir()
    (root / "note.txt").write_text("initial\n")
    monkeypatch.setenv("MEMO_HOME", str(home))
    try:
        assert main(["background", str(root)]) == 0
        capsys.readouterr()
        session_dir = next(home.glob("archive/*/*/session.json")).parent
        socket = home / "runtime/memo.sock"
        attached = request(str(socket), "attach", {"path": str(root)})
        terminal_id = attached["terminal_id"]
        request(str(socket), "events", {
            "terminal_id": terminal_id,
            "events": [{
                "sequence": 1,
                "direction": "input",
                "data": base64.b64encode(b"recorded input\n").decode(),
            }],
        })
        (root / "note.txt").write_text("updated\n")
        request(str(socket), "step", {"path": str(root)})

        assert main(["traces", session_dir.name]) == 0
        traces = json.loads(capsys.readouterr().out)
        assert traces[0]["terminal_id"] == terminal_id
        assert traces[0]["data"] == "recorded input\n"
        trace_path = tmp_path / "trace.json"
        assert main(["traces", session_dir.name, "--terminals", terminal_id,
                     "--path", str(trace_path)]) == 0
        assert json.loads(trace_path.read_text()) == traces

        initial = tmp_path / "initial"
        assert main(["replay", session_dir.name, "0", str(initial)]) == 0
        capsys.readouterr()
        assert (initial / "note.txt").read_text() == "initial\n"
        assert not (initial / ".prompts.md").exists()
        latest = tmp_path / "latest"
        assert main(["replay", session_dir.name, "-1", str(latest),
                     "--include-prompts"]) == 0
        capsys.readouterr()
        assert (latest / "note.txt").read_text() == "updated\n"
        assert "recorded input" in (latest / ".prompts.md").read_text()
    finally:
        socket = home / "runtime/memo.sock"
        if socket.exists():
            request(str(socket), "shutdown")


def test_agent_harness_records_command_and_native_trace_in_directory_session(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    home = tmp_path / "memo-home"
    root = tmp_path / "work"
    root.mkdir()
    binary = tmp_path / "bin"
    binary.mkdir()
    executable = binary / "claude"
    executable.write_text(
        "#!/usr/bin/env python3\n"
        "import json, os, pathlib\n"
        "root = pathlib.Path(os.environ['MEMO_TRACE_DIR'])\n"
        "root.mkdir(parents=True, exist_ok=True)\n"
        "record = {'session_id': 'agent-session', 'type': 'user', 'content': 'fix it'}\n"
        "reply = {'type': 'assistant', 'effort': 'high', 'message': {'role': 'assistant', 'model': 'test-model'}}\n"
        "(root / 'agent-session.jsonl').write_text(json.dumps(record) + '\\n' + json.dumps(reply) + '\\n')\n"
    )
    executable.chmod(executable.stat().st_mode | stat.S_IXUSR)
    monkeypatch.setenv("MEMO_HOME", str(home))
    monkeypatch.setenv("MEMO_TRACE_DIR", str(tmp_path / "native-traces"))
    monkeypatch.setenv("PATH", f"{binary}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.chdir(root)
    try:
        assert main(["claude", "--model", "test-model"]) == 0
        session_dir = next(home.glob("archive/*/*/session.json")).parent
        manifest = SessionStore(Paths.discover()).head(session_dir.parent.name, session_dir.name)
        assert manifest is not None and manifest.step == 1
        assert len(manifest.agent_runs) == 1
        run_id = manifest.agent_runs[0]
        metadata = json.loads((session_dir / "agents/runs" / f"{run_id}.json").read_text())
        assert metadata["command"] == ["claude", "--model", "test-model"]
        assert (metadata["harness"], metadata["model"], metadata["reasoning"]) == (
            "claude", "test-model", "high",
        )
        assert metadata["agent_session_id"] == "agent-session"
        assert metadata["exit_code"] == 0
        assert (session_dir / "agents/traces" / metadata["trace_file"]).is_file()

        assert main(["traces", session_dir.name]) == 0
        normalized = json.loads(capsys.readouterr().out)
        assert normalized[0]["provider"] == "claude"
        assert normalized[0]["event"]["content"] == "fix it"
        assert normalized[0]["native"]["record"]["session_id"] == "agent-session"
        assert main(["traces", session_dir.name, "--raw"]) == 0
        assert json.loads(capsys.readouterr().out)[0]["session_id"] == "agent-session"
    finally:
        socket = home / "runtime/memo.sock"
        if socket.exists():
            request(str(socket), "shutdown")


def test_public_push_and_pull_subcommands_route_results(tmp_path: Path, monkeypatch, capsys) -> None:
    from memo import cli, transport

    monkeypatch.setattr(cli, "push", lambda session_id: {
        "pushed": [session_id], "skipped": [], "failed": [],
    })
    assert main(["push", "session"]) == 0
    assert capsys.readouterr().out == "pushed: session\n"

    destination = Paths(tmp_path / "home").archive / "namespace/session"
    monkeypatch.setattr(
        transport, "pull_session",
        lambda session_id, force=False: destination,
    )
    assert main(["pull", "session", "--force"]) == 0
    assert capsys.readouterr().out == f"pulled: session path={destination}\n"
