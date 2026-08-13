from __future__ import annotations

import base64
import json
import threading
import time
from pathlib import Path

from memo.config import Paths
from memo.daemon import MemoDaemon
from memo.protocol import request
from memo.session_store import SessionStore


def test_background_recording_publishes_periodic_step(tmp_path: Path) -> None:
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
        assert first.step == 0
        assert (store.session_path(started["archive_namespace"], started["session_id"])
                / first.snapshot / "file.txt").read_text() == "first"

        (root / "file.txt").write_text("second")
        deadline = time.monotonic() + 3
        current = first
        while time.monotonic() < deadline and current.step < 1:
            time.sleep(0.05)
            current = store.head(started["archive_namespace"], started["session_id"]) or current
        assert current.step >= 1
        assert (store.session_path(started["archive_namespace"], started["session_id"])
                / current.snapshot / "file.txt").read_text() == "second"
        assert request(str(paths.socket), "health") == {"status": "ok"}
    finally:
        if paths.socket and paths.socket.exists():
            request(str(paths.socket), "shutdown")
        thread.join(timeout=3)
    assert not thread.is_alive()


def test_concurrent_terminal_ingestion_is_sealed_in_one_step(tmp_path: Path) -> None:
    home = tmp_path / "memo-home"
    paths = Paths(home, home / "scratch", home / "archive", tmp_path / "unpack")
    root = tmp_path / "work"
    root.mkdir()
    daemon = MemoDaemon(paths, interval=10)
    thread = threading.Thread(target=daemon.serve_forever)
    thread.start()
    try:
        assert paths.socket is not None
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline and not paths.socket.exists():
            time.sleep(0.01)
        first = request(str(paths.socket), "attach", {"path": str(root)})
        second = request(str(paths.socket), "attach", {"path": str(root)})

        def send(allocation: dict[str, object], value: bytes) -> None:
            request(str(paths.socket), "events", {
                "terminal_id": allocation["terminal_id"],
                "events": [{"sequence": 1, "direction": "output",
                            "data": base64.b64encode(value).decode()}],
            })

        clients = [threading.Thread(target=send, args=(first, b"first")),
                   threading.Thread(target=send, args=(second, b"second"))]
        for client in clients:
            client.start()
        for client in clients:
            client.join()
        published = request(str(paths.socket), "step", {"path": str(root)})
        manifest = SessionStore(paths).head(first["archive_namespace"], first["session_id"])
        assert manifest is not None
        assert manifest.step == published["step"]
        assert manifest.stream_high_water == {
            first["terminal_id"]: 1,
            second["terminal_id"]: 1,
        }
        session = paths.archive / first["archive_namespace"] / first["session_id"]
        for terminal_id in manifest.stream_high_water:
            metadata = json.loads(
                (session / "streams" / "terminals" / terminal_id / "stream.json").read_text()
            )
            assert metadata["highest_sequence"] == 1
    finally:
        if paths.socket and paths.socket.exists():
            request(str(paths.socket), "shutdown")
        thread.join(timeout=3)
