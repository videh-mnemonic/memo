from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from memo.cli import main, parser
from memo.config import Paths


def test_removed_public_commands_are_not_registered() -> None:
    choices = parser()._subparsers._group_actions[0].choices
    assert "background" not in choices
    assert "record" not in choices
    assert "claude" not in choices
    assert "codex" not in choices
    assert "inspect" not in choices


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


def test_tidy_imports_pushes_then_removes_archived(monkeypatch, capsys) -> None:
    calls: list[object] = []

    def fake_import():
        calls.append("import")
        return SimpleNamespace(imported=["native"], refreshed=[], skipped=[], failed=[])

    def fake_push(session_id=None):
        calls.append(("push", session_id))
        return {
            "pushed": ["complete"],
            "skipped": ["active"],
            "failed": [("failed", "offline")],
        }

    def fake_remove(exclude):
        calls.append(("remove", exclude))
        return {
            "removed": ["complete"],
            "retained": [("active", "recording is not complete"),
                         ("failed", "push failed")],
            "failed": [],
        }

    monkeypatch.setattr("memo.agents.importer.import_native_sessions", fake_import)
    monkeypatch.setattr("memo.cli.push", fake_push)
    monkeypatch.setattr("memo.cli.remove_archived", fake_remove)

    assert main(["tidy"]) == 1
    assert calls == ["import", ("push", None), ("remove", ["failed"])]
    captured = capsys.readouterr()
    assert "removed: complete" in captured.out
    assert "retained: active: recording is not complete" in captured.out
    assert "failed: failed: offline" in captured.err


def test_read_commands_ensure_session_is_local(monkeypatch, tmp_path: Path, capsys) -> None:
    calls: list[str] = []
    monkeypatch.setattr("memo.cli._ensure_local_session", calls.append)
    monkeypatch.setattr(
        "memo.cli.render_status",
        lambda **kwargs: f"{kwargs['session_id']}\n",
    )
    monkeypatch.setattr("memo.cli.trace_json", lambda session_id, *args, **kwargs: "[]\n")
    monkeypatch.setattr("memo.cli.replay_session", lambda *args, **kwargs: tmp_path / "out")

    assert main(["status", "one"]) == 0
    assert main(["traces", "two"]) == 0
    assert main(["replay", "three", "-1", str(tmp_path / "out")]) == 0
    assert calls == ["one", "two", "three"]
    capsys.readouterr()


def test_status_options_route_to_renderer(monkeypatch, capsys) -> None:
    captured = {}
    monkeypatch.setattr(
        "memo.cli.render_status",
        lambda **kwargs: captured.update(kwargs) or "status\n",
    )

    assert main(["status", "--include-archive", "--limit", "7"]) == 0
    assert captured == {"include_archive": True, "limit": 7, "session_id": None}
    assert capsys.readouterr().out == "status\n"


def test_single_status_rejects_list_options_before_pull(monkeypatch, capsys) -> None:
    calls: list[str] = []
    monkeypatch.setattr("memo.cli._ensure_local_session", calls.append)

    assert main(["status", "session", "--limit", "1"]) == 1
    assert calls == []
    assert "single-session status" in capsys.readouterr().err


def test_traces_lists_terminal_ids(monkeypatch, capsys) -> None:
    monkeypatch.setattr("memo.cli._ensure_local_session", lambda _session_id: None)
    monkeypatch.setattr("memo.cli.terminal_ids", lambda _session_id: ["a", "z"])

    assert main(["traces", "session", "--list-terminals"]) == 0
    assert capsys.readouterr().out == "a\nz\n"


def test_terminal_listing_rejects_export_options(monkeypatch, capsys) -> None:
    monkeypatch.setattr("memo.cli._ensure_local_session", lambda _session_id: None)

    assert main(["traces", "session", "--list-terminals", "--raw"]) == 1
    assert "cannot be combined" in capsys.readouterr().err


def test_end_prompts_for_scope_when_daemon_requests_it(monkeypatch, capsys) -> None:
    calls = []

    def fake_end(*args, **kwargs):
        calls.append(kwargs)
        if len(calls) == 1:
            return {"scope_confirmation_required": True, "revision": 1,
                    "other_terminals": 0}
        return {"session_id": "session", "step": 2, "already_complete": False}

    monkeypatch.setattr("memo.cli.end", fake_end)
    monkeypatch.setattr("builtins.input", lambda _: "yes")
    monkeypatch.setattr("memo.cli.sys.stdin.isatty", lambda: True)

    assert main(["end", "."]) == 0
    assert calls[0]["prompt_scope"] is True
    assert calls[1]["capture_scope"] == "full"
    assert "completed: session" in capsys.readouterr().out
