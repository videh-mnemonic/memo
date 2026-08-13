from __future__ import annotations

import base64
import json
from pathlib import Path

import pytest

from memo.cli import main
from memo.protocol import request


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


def test_removed_command_forms_are_rejected() -> None:
    with pytest.raises(SystemExit):
        main(["claude"])
    with pytest.raises(SystemExit):
        main(["codex"])
    with pytest.raises(SystemExit):
        main(["--load", "session", "--inspect"])
