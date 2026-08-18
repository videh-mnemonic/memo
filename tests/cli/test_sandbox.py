from __future__ import annotations

from pathlib import Path

from memo.agents.sandbox.config import load_root_config
from memo.cli import main
from memo.recording.paths import StoragePaths


def test_allow_show_disallow_and_reset(tmp_path: Path, monkeypatch, capsys) -> None:
    root = tmp_path / "project"
    source = tmp_path / "credential"
    destination = tmp_path / "sandbox-home" / ".aws" / "credentials"
    root.mkdir()
    source.write_text("secret")
    monkeypatch.chdir(root)
    monkeypatch.setenv("MEMO_RECORDING_ROOT", str(root))

    assert main(["sandbox", "allow", "--read", str(source), "--at", str(destination)]) == 0
    config = load_root_config(root)
    assert config.grants[0].source == str(source)
    assert config.grants[0].destination == str(destination)

    assert main(["sandbox", "show"]) == 0
    output = capsys.readouterr().out
    assert "effective mounts:" in output
    assert str(destination) in output

    assert main(["sandbox", "disallow", str(destination)]) == 0
    assert load_root_config(root).grants == ()
    assert main(["sandbox", "reset"]) == 0
    assert load_root_config(root).home_read_only_if_present == ()
    assert load_root_config(root).home_read_write_if_present == (".cache", ".triton", ".nv")


def test_sandbox_shell_requires_recorded_terminal(tmp_path: Path, monkeypatch, capsys) -> None:
    monkeypatch.chdir(tmp_path)
    assert main(["sandbox", "shell"]) == 1
    assert "active Memo recording" in capsys.readouterr().err


def test_setup_reports_backend_failure(monkeypatch, capsys) -> None:
    from memo.agents.sandbox.command import SandboxUnavailable

    monkeypatch.setattr(
        "memo.cli.commands.sandbox.self_test",
        lambda **_kwargs: (_ for _ in ()).throw(SandboxUnavailable("namespace disabled")),
    )
    assert main(["sandbox", "setup"]) == 1
    assert "namespace disabled" in capsys.readouterr().err


def test_sandbox_shell_reports_lifecycle_to_active_recording(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "project"
    root.mkdir()
    paths = StoragePaths(tmp_path / "memo-home")
    messages = []
    monkeypatch.chdir(root)
    monkeypatch.setenv("MEMO_RECORDING_ROOT", str(root))
    monkeypatch.setenv("MEMO_SESSION_ID", "session")
    monkeypatch.setenv("MEMO_TERMINAL_ID", "terminal")
    monkeypatch.setenv("SHELL", "/bin/sh")
    monkeypatch.setattr("memo.cli.commands.sandbox.StoragePaths.discover", lambda: paths)
    monkeypatch.setattr("memo.cli.commands.sandbox.self_test", lambda _paths: {})
    monkeypatch.setattr("memo.cli.commands.sandbox.ensure_daemon", lambda _paths: None)
    monkeypatch.setattr(
        "memo.cli.commands.sandbox.build_command",
        lambda _policy, _target: ["/bin/sh", "-c", "exit 3"],
    )
    monkeypatch.setattr(
        "memo.cli.commands.sandbox.request",
        lambda _socket, operation, payload, **_kwargs: messages.append((operation, payload)) or {},
    )

    assert main(["sandbox", "shell"]) == 3
    assert [item[0] for item in messages] == [
        "sandbox_shell_launch",
        "sandbox_shell_complete",
    ]
    assert messages[0][1]["policy_digest"]
    assert messages[1][1]["exit_code"] == 3
