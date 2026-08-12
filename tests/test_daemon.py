from __future__ import annotations

import threading
import time
from pathlib import Path

from memo.config import Paths
from memo.daemon import MemoDaemon
from memo.protocol import request
from memo.session_store import SessionStore


def test_background_recording_publishes_periodic_checkpoint(tmp_path: Path) -> None:
    home = tmp_path / "memo-home"
    paths = Paths(home, home / "scratch", home / "archive", tmp_path / "unpack")
    root = tmp_path / "work"
    root.mkdir()
    (root / "file.txt").write_text("first")
    daemon = MemoDaemon(paths, interval=0.15)
    thread = threading.Thread(target=daemon.serve_forever)
    thread.start()
    try:
        assert paths.socket is not None
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline and not paths.socket.exists():
            time.sleep(0.01)
        started = request(str(paths.socket), "start", {"path": str(root)})
        store = SessionStore(paths)
        first = store.head(started["archive_namespace"], started["session_id"])
        assert first is not None
        assert first.generation == 1
        assert (store.session_path(started["archive_namespace"], started["session_id"])
                / first.snapshot / "file.txt").read_text() == "first"

        (root / "file.txt").write_text("second")
        deadline = time.monotonic() + 3
        current = first
        while time.monotonic() < deadline and current.generation < 2:
            time.sleep(0.05)
            current = store.head(started["archive_namespace"], started["session_id"]) or current
        assert current.generation >= 2
        assert (store.session_path(started["archive_namespace"], started["session_id"])
                / current.snapshot / "file.txt").read_text() == "second"
        assert request(str(paths.socket), "health") == {"status": "ok"}
    finally:
        if paths.socket and paths.socket.exists():
            request(str(paths.socket), "shutdown")
        thread.join(timeout=3)
    assert not thread.is_alive()
