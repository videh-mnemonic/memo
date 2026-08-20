from __future__ import annotations

from memo.daemon import install_guard
from memo.daemon.control import LiveAttachment


def test_installer_stops_idle_daemon(monkeypatch, capsys) -> None:
    monkeypatch.setattr(install_guard, "stop_daemon", lambda _paths, force=False: (True, []))

    assert install_guard.main([]) == 0
    assert capsys.readouterr().out == "stopped Memo daemon\n"


def test_installer_refuses_attached_terminal(monkeypatch, capsys) -> None:
    attachment = LiveAttachment("terminal", "session", "/work")
    monkeypatch.setattr(
        install_guard,
        "stop_daemon",
        lambda _paths, force=False: (False, [attachment]),
    )

    assert install_guard.main([]) == 2
    output = capsys.readouterr()
    assert "upgrade refused" in output.err
    assert "/work (session, terminal)" in output.err
    assert "./install --force-stop" in output.err


def test_installer_passes_force_stop(monkeypatch) -> None:
    captured = []
    monkeypatch.setattr(
        install_guard,
        "stop_daemon",
        lambda _paths, force=False: captured.append(force) or (False, []),
    )

    assert install_guard.main(["--force-stop"]) == 0
    assert captured == [True]


def test_installer_refuses_when_daemon_cannot_be_checked(monkeypatch, capsys) -> None:
    def fail(_paths, force=False):
        raise RuntimeError("daemon did not answer health checks")

    monkeypatch.setattr(install_guard, "stop_daemon", fail)

    assert install_guard.main([]) == 2
    assert capsys.readouterr().err == (
        "Memo upgrade refused: daemon did not answer health checks\n"
    )
