from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from memo.cli import main, parser
from memo.recording.paths import StoragePaths


@pytest.fixture(autouse=True)
def _configured_s3(monkeypatch) -> None:
    monkeypatch.setenv("MEMO_S3_BUCKET", "test-bucket")


def test_cli_requires_s3_configuration(monkeypatch, capsys) -> None:
    monkeypatch.delenv("MEMO_S3_BUCKET", raising=False)
    monkeypatch.setattr("memo.cli.app.run_relay", lambda _path: 0)

    assert main([]) == 1
    assert "S3 transport requires MEMO_S3_BUCKET" in capsys.readouterr().err


def test_removed_public_commands_are_not_registered() -> None:
    choices = parser()._subparsers._group_actions[0].choices
    assert "background" not in choices
    assert "record" not in choices
    assert "claude" not in choices
    assert "codex" not in choices
    assert "inspect" not in choices
    assert "migrate-legacy" not in choices


def test_daemon_commands_do_not_require_s3(monkeypatch, tmp_path, capsys) -> None:
    monkeypatch.delenv("MEMO_S3_BUCKET", raising=False)
    monkeypatch.setenv("MEMO_HOME", str(tmp_path / "home"))

    assert main(["daemon", "status"]) == 0
    assert capsys.readouterr().out == "daemon: stopped\n"


def test_local_status_does_not_require_s3_but_archive_status_does(
    monkeypatch, tmp_path, capsys
) -> None:
    monkeypatch.delenv("MEMO_S3_BUCKET", raising=False)
    monkeypatch.setenv("MEMO_HOME", str(tmp_path / "home"))

    assert main(["status", "--json"]) == 0
    assert capsys.readouterr().out == "[]\n"
    assert main(["status", "--archive"]) == 1
    assert "S3 transport requires MEMO_S3_BUCKET" in capsys.readouterr().err


def test_default_and_path_invocations_launch_generic_relay(monkeypatch, tmp_path: Path) -> None:
    calls: list[Path] = []
    monkeypatch.setattr("memo.cli.app.run_relay", lambda path: calls.append(path) or 7)
    monkeypatch.chdir(tmp_path)

    assert main([]) == 7
    assert main([str(tmp_path)]) == 7
    assert calls == [tmp_path, tmp_path]


def test_end_prefers_shell_session_identity(monkeypatch, tmp_path: Path, capsys) -> None:
    captured = {}
    monkeypatch.setenv("MEMO_HOME", str(tmp_path / "memo-home"))
    monkeypatch.setenv("MEMO_SESSION_ID", "session")
    monkeypatch.setenv("MEMO_TERMINAL_ID", "terminal")
    session_path = StoragePaths.discover().archive / "session"
    session_path.mkdir(parents=True)
    (session_path / "object").write_bytes(b"x" * 1536)

    def fake_end(path=None, **values):
        captured.update({"path": path, **values})
        return {"session_id": "session", "step": 2, "already_complete": False}

    monkeypatch.setattr("memo.cli.commands.end.end", fake_end)
    assert main(["end"]) == 0
    assert captured["path"] is None
    assert captured["session_id"] == "session"
    assert captured["terminal_id"] == "terminal"
    assert capsys.readouterr().out == "completed: session step=2 size=1.5 KiB\n"


def test_end_reports_required_cloud_upload_failure(monkeypatch, capsys) -> None:
    def fake_end(path=None, **values):
        del path, values
        raise RuntimeError("cloud upload failed: session: denied")

    monkeypatch.setattr("memo.cli.commands.end.end", fake_end)

    assert main(["end", "."]) == 1
    captured_output = capsys.readouterr()
    assert "cloud upload failed: session: denied" in captured_output.err


def test_end_decline_leaves_recording_unchanged(monkeypatch, capsys) -> None:
    calls = []
    monkeypatch.setattr(
        "memo.cli.commands.end.end",
        lambda *args, **kwargs: (
            calls.append(kwargs)
            or {
                "confirmation_required": True,
                "session_id": "session",
                "revision": 4,
                "other_terminals": 2,
            }
        ),
    )
    monkeypatch.setattr("builtins.input", lambda _: "n")

    assert main(["end", "."]) == 0
    assert len(calls) == 1
    assert "recording unchanged" in capsys.readouterr().out


def test_public_push_and_pull_subcommands_route_results(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    from memo.cli.commands import pull, push

    monkeypatch.setattr(
        push,
        "push",
        lambda session_id: {
            "pushed": [session_id],
            "skipped": [],
            "failed": [],
        },
    )
    assert main(["push", "session"]) == 0
    assert capsys.readouterr().out == "pushed: session\n"

    destination = StoragePaths(tmp_path / "home").archive / "session"
    calls = []

    def fake_pull(session_id, force=False, destination=None):
        calls.append((session_id, force, destination))
        return destination or StoragePaths(tmp_path / "home").archive / session_id

    monkeypatch.setattr(pull, "pull_session", fake_pull)
    assert main(["pull", "session", "--force"]) == 0
    assert capsys.readouterr().out == f"pulled: session path={destination}\n"

    external = tmp_path / "external-recording"
    assert main(["pull", "session", "--destination", str(external)]) == 0
    assert capsys.readouterr().out == f"pulled: session path={external}\n"
    assert calls == [("session", True, None), ("session", False, external)]


def test_pull_all_reports_each_result_and_failure(monkeypatch, capsys) -> None:
    from memo.cli.commands import pull

    captured = {}

    def fake_pull_all(*, force=False):
        captured["force"] = force
        return SimpleNamespace(
            pulled=["one"],
            skipped=["two"],
            failed=[("three", "offline")],
        )

    monkeypatch.setattr(pull, "pull_all_sessions", fake_pull_all)

    assert main(["pull", "--all", "--force"]) == 1
    assert captured == {"force": True}
    output = capsys.readouterr()
    assert output.out == "pulled: one\nskipped: local exists: two\n"
    assert output.err == "failed: three: offline\n"


def test_pull_requires_exactly_one_target(capsys) -> None:
    with pytest.raises(SystemExit):
        parser().parse_args(["pull"])
    assert "one of the arguments" in capsys.readouterr().err

    with pytest.raises(SystemExit):
        parser().parse_args(["pull", "session", "--all"])
    assert "not allowed with argument" in capsys.readouterr().err


def test_pull_destination_rejects_all(monkeypatch, tmp_path, capsys) -> None:
    from memo.cli.commands import pull

    monkeypatch.setattr(
        pull,
        "pull_all_sessions",
        lambda **_kwargs: pytest.fail("bulk pull should not start"),
    )

    assert main(["pull", "--all", "--destination", str(tmp_path / "output")]) == 1
    assert "--destination cannot be used with --all" in capsys.readouterr().err


def test_import_dry_run_routes_to_importer(monkeypatch, capsys) -> None:
    captured = {}

    def fake_import(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(imported=["one"], refreshed=["two"], skipped=[], failed=[])

    monkeypatch.setattr("memo.cli.commands.import_sessions.import_native_sessions", fake_import)

    assert main(["import", "--dry-run"]) == 0
    assert captured == {"dry_run": True}
    assert "would import: one" in capsys.readouterr().out


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
            "retained": [("active", "recording is not complete"), ("failed", "push failed")],
            "failed": [],
        }

    monkeypatch.setattr("memo.cli.commands.tidy.import_native_sessions", fake_import)
    monkeypatch.setattr("memo.cli.commands.tidy.push", fake_push)
    monkeypatch.setattr("memo.cli.commands.tidy.remove_archived", fake_remove)

    assert main(["tidy"]) == 1
    assert calls == ["import", ("push", None), ("remove", ["failed"])]
    captured = capsys.readouterr()
    assert "removed: complete" in captured.out
    assert "retained: active: recording is not complete" in captured.out
    assert "failed: failed: offline" in captured.err


def test_export_commands_pull_but_status_remains_local(monkeypatch, tmp_path: Path, capsys) -> None:
    calls: list[str] = []
    monkeypatch.setattr("memo.cli.commands.traces.require_local_session", calls.append)
    monkeypatch.setattr("memo.cli.commands.replay.require_local_session", calls.append)
    monkeypatch.setattr(
        "memo.cli.commands.status.render_status",
        lambda **kwargs: f"{kwargs['session_id']}\n",
    )
    monkeypatch.setattr(
        "memo.cli.commands.traces.trace_json", lambda session_id, *args, **kwargs: "[]\n"
    )
    monkeypatch.setattr(
        "memo.cli.commands.replay.replay_session", lambda *args, **kwargs: tmp_path / "out"
    )

    assert main(["status", "one"]) == 0
    assert main(["traces", "two"]) == 0
    assert main(["replay", "three", "-1", str(tmp_path / "out")]) == 0
    assert calls == ["two", "three"]
    capsys.readouterr()


def test_status_options_route_to_renderer(monkeypatch, capsys) -> None:
    captured = {}
    monkeypatch.setattr(
        "memo.cli.commands.status.render_status",
        lambda **kwargs: captured.update(kwargs) or "status\n",
    )

    assert main(["status", "--archive", "--limit", "7", "--json"]) == 0
    assert captured == {
        "archive_only": True,
        "limit": 7,
        "session_id": None,
        "active_only": False,
        "json_output": True,
    }
    assert capsys.readouterr().out == "status\n"


def test_single_status_rejects_list_options(capsys) -> None:
    assert main(["status", "session", "--limit", "1"]) == 1
    assert "single-session status" in capsys.readouterr().err


def test_traces_lists_terminal_ids(monkeypatch, capsys) -> None:
    monkeypatch.setattr("memo.cli.commands.traces.require_local_session", lambda _session_id: None)
    monkeypatch.setattr("memo.cli.commands.traces.terminal_ids", lambda _session_id: ["a", "z"])

    assert main(["traces", "session", "--list-terminals"]) == 0
    assert capsys.readouterr().out == "a\nz\n"


def test_terminal_listing_rejects_export_options(monkeypatch, capsys) -> None:
    monkeypatch.setattr("memo.cli.commands.traces.require_local_session", lambda _session_id: None)

    assert main(["traces", "session", "--list-terminals", "--raw"]) == 1
    assert "cannot be combined" in capsys.readouterr().err


def test_end_prompts_for_scope_when_daemon_requests_it(monkeypatch, capsys) -> None:
    calls = []

    def fake_end(*args, **kwargs):
        calls.append(kwargs)
        if len(calls) == 1:
            return {"scope_confirmation_required": True, "revision": 1, "other_terminals": 0}
        return {"session_id": "session", "step": 2, "already_complete": False}

    monkeypatch.setattr("memo.cli.commands.end.end", fake_end)
    monkeypatch.setattr("builtins.input", lambda _: "yes")
    monkeypatch.setattr("memo.cli.commands.end.sys.stdin.isatty", lambda: True)

    assert main(["end", "."]) == 0
    assert calls[0]["prompt_scope"] is True
    assert calls[1]["capture_scope"] == "full"
    assert "completed: session" in capsys.readouterr().out
