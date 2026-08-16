from __future__ import annotations

import base64
import json
import threading
import time
from pathlib import Path

from memo.config import StoragePaths
from memo.daemon import MemoDaemon
from memo.protocol import request
from memo.session_store import SessionStore


def _running(tmp_path: Path, interval: float = 10) -> tuple[StoragePaths, Path, MemoDaemon, threading.Thread]:
    paths = StoragePaths(tmp_path / "memo-home")
    root = tmp_path / "work"
    root.mkdir()
    daemon = MemoDaemon(paths, interval=interval)
    thread = threading.Thread(target=daemon.serve_forever)
    thread.start()
    assert paths.socket is not None
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline and not paths.socket.exists():
        time.sleep(0.01)
    return paths, root, daemon, thread


def _stop(paths: StoragePaths, thread: threading.Thread) -> None:
    if paths.socket and paths.socket.exists():
        request(str(paths.socket), "shutdown")
    thread.join(timeout=3)
    assert not thread.is_alive()


def test_zero_terminal_recording_keeps_publishing(tmp_path: Path) -> None:
    paths, root, _, thread = _running(tmp_path, interval=0.1)
    try:
        (root / "file.txt").write_text("first")
        attached = request(str(paths.socket), "attach", {"path": str(root)})
        request(str(paths.socket), "detach", {"terminal_id": attached["terminal_id"]})
        (root / "file.txt").write_text("second")
        store = SessionStore(paths)
        deadline = time.monotonic() + 3
        manifest = store.head(attached["session_id"])
        while time.monotonic() < deadline and (manifest is None or manifest.step < 1):
            time.sleep(0.05)
            manifest = store.head(attached["session_id"])
        assert manifest is not None and manifest.step >= 1
        assert (store.session_path(attached["session_id"]) / manifest.snapshot / "file.txt").read_text() == "second"
        decision = request(str(paths.socket), "attach", {"path": str(root)})
        assert decision["decision_required"] is True
    finally:
        _stop(paths, thread)


def test_concurrent_streams_are_drained_by_end(tmp_path: Path) -> None:
    paths, root, _, thread = _running(tmp_path)
    try:
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
        warning = request(str(paths.socket), "end", {
            "session_id": first["session_id"], "terminal_id": first["terminal_id"],
        })
        assert warning["confirmation_required"] is True
        assert warning["other_terminals"] == 1
        ended = request(str(paths.socket), "end", {
            "session_id": first["session_id"], "terminal_id": first["terminal_id"],
            "confirmed": True, "expected_revision": warning["revision"],
        })
        assert ended["state"] == "complete"
        assert request(str(paths.socket), "events", {
            "terminal_id": second["terminal_id"], "events": [],
        })["recording_ended"] is True
        assert request(str(paths.socket), "end", {
            "session_id": first["session_id"],
        })["already_complete"] is True
        manifest = SessionStore(paths).head(first["session_id"])
        assert manifest is not None
        assert manifest.stream_high_water == {
            first["terminal_id"]: 1, second["terminal_id"]: 1,
        }
        session = paths.archive / first["session_id"]
        for terminal_id in manifest.stream_high_water:
            metadata = json.loads(
                (session / "streams/terminals" / terminal_id / "stream.json").read_text()
            )
            assert metadata["highest_sequence"] == 1
    finally:
        _stop(paths, thread)


def test_end_scope_is_selected_before_completion(tmp_path: Path) -> None:
    paths, root, _, thread = _running(tmp_path)
    try:
        attached = request(str(paths.socket), "attach", {"path": str(root)})
        pending = request(str(paths.socket), "end", {
            "session_id": attached["session_id"],
            "terminal_id": attached["terminal_id"],
            "prompt_scope": True,
        })
        assert pending["scope_confirmation_required"] is True

        ended = request(str(paths.socket), "end", {
            "session_id": attached["session_id"],
            "terminal_id": attached["terminal_id"],
            "capture_scope": "full",
        })

        assert ended["state"] == "complete"
        assert SessionStore(paths).load_session(str(attached["session_id"])).capture_scope == "full"
    finally:
        _stop(paths, thread)


def test_resume_replace_and_stale_decisions(tmp_path: Path) -> None:
    paths, root, _, thread = _running(tmp_path)
    try:
        first = request(str(paths.socket), "attach", {"path": str(root)})
        request(str(paths.socket), "detach", {"terminal_id": first["terminal_id"]})
        choice = request(str(paths.socket), "attach", {"path": str(root)})
        joined = request(str(paths.socket), "attach", {
            "path": str(root), "decision": "resume",
            "expected_session_id": choice["session_id"],
            "expected_revision": choice["revision"],
        })
        assert joined["session_id"] == first["session_id"]
        assert joined["terminal_id"] != first["terminal_id"]
        stale = request(str(paths.socket), "attach", {
            "path": str(root), "decision": "replace",
            "expected_session_id": choice["session_id"],
            "expected_revision": choice["revision"],
        })
        assert stale["stale"] is True
        request(str(paths.socket), "detach", {"terminal_id": joined["terminal_id"]})
        replace_choice = request(str(paths.socket), "attach", {"path": str(root)})
        replacement = request(str(paths.socket), "attach", {
            "path": str(root), "decision": "replace",
            "expected_session_id": replace_choice["session_id"],
            "expected_revision": replace_choice["revision"],
        })
        assert replacement["session_id"] != first["session_id"]
        assert SessionStore(paths).load_session(first["session_id"]).state == "complete"
    finally:
        _stop(paths, thread)


def test_end_rejects_stale_confirmation_after_attachment_change(tmp_path: Path) -> None:
    paths, root, daemon, thread = _running(tmp_path)
    try:
        first = request(str(paths.socket), "attach", {"path": str(root)})
        second = request(str(paths.socket), "attach", {"path": str(root)})
        warning = request(str(paths.socket), "end", {
            "session_id": first["session_id"], "terminal_id": first["terminal_id"],
        })
        third = request(str(paths.socket), "attach", {"path": str(root)})
        stale = request(str(paths.socket), "end", {
            "session_id": first["session_id"], "terminal_id": first["terminal_id"],
            "confirmed": True, "expected_revision": warning["revision"],
        })
        assert stale["stale"] is True
        assert stale["other_terminals"] == 2
        assert len(daemon.registry.attached(first["session_id"])) == 3
        for allocation in (first, second, third):
            request(str(paths.socket), "detach", {"terminal_id": allocation["terminal_id"]})
    finally:
        _stop(paths, thread)


def test_end_starts_final_push_after_releasing_lifecycle_locks(
    tmp_path: Path, monkeypatch,
) -> None:
    monkeypatch.setattr(
        "memo.daemon.TransportConfig.discover",
        classmethod(lambda cls, required=False: object()),
    )
    called = threading.Event()
    lock_state: dict[str, bool] = {}

    def fake_push(self: MemoDaemon, payload: dict[str, object]) -> dict[str, object]:
        session_id = str(payload["session_id"])
        session_lock = self._session_lock(session_id)
        root_lock = self._root_lock(root)
        lock_state["session"] = session_lock.acquire(blocking=False)
        if lock_state["session"]:
            session_lock.release()
        lock_state["root"] = root_lock.acquire(blocking=False)
        if lock_state["root"]:
            root_lock.release()
        lock_state["complete"] = SessionStore(paths).load_session(session_id).state == "complete"
        called.set()
        return {"pushed": [session_id], "skipped": [], "failed": []}

    monkeypatch.setattr(MemoDaemon, "_push", fake_push)
    paths, root, _, thread = _running(tmp_path)
    try:
        attached = request(str(paths.socket), "attach", {"path": str(root)})
        ended = request(str(paths.socket), "end", {
            "session_id": attached["session_id"],
            "terminal_id": attached["terminal_id"],
        })
        assert ended["state"] == "complete"
        assert ended["cloud"] == "pending"
        assert called.wait(3)
        assert lock_state == {"session": True, "root": True, "complete": True}
    finally:
        _stop(paths, thread)
