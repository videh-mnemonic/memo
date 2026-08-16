from __future__ import annotations

from pathlib import Path

from memo.cli import main, parser
from memo.config import Paths


def test_removed_public_commands_are_not_registered() -> None:
    choices = parser()._subparsers._group_actions[0].choices
    assert "background" not in choices
    assert "record" not in choices
    assert "claude" not in choices
    assert "codex" not in choices


def test_default_and_path_invocations_launch_generic_relay(monkeypatch, tmp_path: Path) -> None:
    calls: list[Path] = []
    monkeypatch.setattr("memo.cli.run_relay", lambda path: calls.append(path) or 7)
    monkeypatch.chdir(tmp_path)

    assert main([]) == 7
    assert main([str(tmp_path)]) == 7
    assert calls == [tmp_path, tmp_path]


def test_end_prefers_shell_session_identity(monkeypatch, tmp_path: Path, capsys) -> None:
    captured = {}
    monkeypatch.setenv("MEMO_SESSION_ID", "session")
    monkeypatch.setenv("MEMO_TERMINAL_ID", "terminal")

    def fake_end(path=None, **values):
        captured.update({"path": path, **values})
        return {"session_id": "session", "step": 2, "already_complete": False}

    monkeypatch.setattr("memo.cli.end", fake_end)
    assert main(["end"]) == 0
    assert captured["path"] is None
    assert captured["session_id"] == "session"
    assert captured["terminal_id"] == "terminal"
    assert "completed: session step=2" in capsys.readouterr().out


def test_end_decline_leaves_recording_unchanged(monkeypatch, capsys) -> None:
    calls = []
    monkeypatch.setattr("memo.cli.end", lambda *args, **kwargs: calls.append(kwargs) or {
        "confirmation_required": True, "session_id": "session",
        "revision": 4, "other_terminals": 2,
    })
    monkeypatch.setattr("builtins.input", lambda _: "n")

    assert main(["end", "."]) == 0
    assert len(calls) == 1
    assert "recording unchanged" in capsys.readouterr().out


def test_public_push_and_pull_subcommands_route_results(tmp_path: Path, monkeypatch, capsys) -> None:
    from memo import cli, transport

    monkeypatch.setattr(cli, "push", lambda session_id: {
        "pushed": [session_id], "skipped": [], "failed": [],
    })
    assert main(["push", "session"]) == 0
    assert capsys.readouterr().out == "pushed: session\n"

    destination = Paths(tmp_path / "home").archive / "session"
    monkeypatch.setattr(transport, "pull_session", lambda session_id, force=False: destination)
    assert main(["pull", "session", "--force"]) == 0
    assert capsys.readouterr().out == f"pulled: session path={destination}\n"
