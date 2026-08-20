from __future__ import annotations

import sqlite3
import time
from pathlib import Path

from memo.daemon import control
from memo.recording.paths import StoragePaths


def _registry(path: Path, *, last_seen_ns: int) -> None:
    connection = sqlite3.connect(path)
    connection.execute("CREATE TABLE active_sessions (root TEXT, session_id TEXT PRIMARY KEY)")
    connection.execute(
        "CREATE TABLE attachments (terminal_id TEXT, session_id TEXT, "
        "detached_utc TEXT, last_seen_ns INTEGER)"
    )
    connection.execute("INSERT INTO active_sessions VALUES ('/work', 'session')")
    connection.execute(
        "INSERT INTO attachments VALUES ('terminal', 'session', NULL, ?)",
        (last_seen_ns,),
    )
    connection.commit()
    connection.close()


def test_live_attachments_include_current_and_legacy_rows(tmp_path: Path) -> None:
    paths = StoragePaths(tmp_path / "home")
    paths.runtime.mkdir(parents=True)
    _registry(paths.registry, last_seen_ns=0)

    assert [item.terminal_id for item in control.live_attachments(paths)] == ["terminal"]


def test_live_attachments_keep_expired_rows_until_explicitly_detached(tmp_path: Path) -> None:
    paths = StoragePaths(tmp_path / "home")
    paths.runtime.mkdir(parents=True)
    stale = 1
    _registry(paths.registry, last_seen_ns=stale)

    assert [item.terminal_id for item in control.live_attachments(paths)] == ["terminal"]


def test_stop_refuses_while_terminal_is_attached(tmp_path: Path, monkeypatch) -> None:
    paths = StoragePaths(tmp_path / "home")
    paths.runtime.mkdir(parents=True)
    _registry(paths.registry, last_seen_ns=time.time_ns())
    monkeypatch.setattr(control, "daemon_health", lambda _paths: {"status": "ok"})
    sent = []
    monkeypatch.setattr(control, "request", lambda *_args, **_kwargs: sent.append(True))

    stopped, attachments = control.stop_daemon(paths)

    assert stopped is False
    assert len(attachments) == 1
    assert sent == []


def test_force_stop_sends_shutdown(tmp_path: Path, monkeypatch) -> None:
    paths = StoragePaths(tmp_path / "home")
    paths.runtime.mkdir(parents=True)
    paths.socket.touch()
    _registry(paths.registry, last_seen_ns=time.time_ns())
    monkeypatch.setattr(control, "daemon_health", lambda _paths: {"status": "ok"})

    def request(_socket, operation, timeout):
        assert operation == "shutdown"
        assert timeout == control.STOP_TIMEOUT_SECONDS
        paths.socket.unlink()
        return {"status": "stopping"}

    monkeypatch.setattr(control, "request", request)

    assert control.stop_daemon(paths, force=True) == (True, [])
