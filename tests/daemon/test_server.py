from __future__ import annotations

import base64
import json
import threading
import time
from pathlib import Path

import pytest

from memo.daemon.protocol import ProtocolError, request
from memo.daemon.server import TERMINAL_STALE_SECONDS, MemoDaemon
from memo.recording.paths import StoragePaths
from memo.recording.store import SessionStore
from memo.transport.config import S3Config


@pytest.fixture(autouse=True)
def _configured_fake_s3(monkeypatch) -> None:
    monkeypatch.setenv("MEMO_S3_BUCKET", "test-bucket")

    def push(self: MemoDaemon, payload: dict[str, object]) -> dict[str, object]:
        session_id = payload.get("session_id")
        pushed = [] if session_id is None else [str(session_id)]
        return {"pushed": pushed, "skipped": [], "failed": []}

    monkeypatch.setattr(MemoDaemon, "_push", push)


def _running(
    tmp_path: Path, interval: float = 10
) -> tuple[StoragePaths, Path, MemoDaemon, threading.Thread]:
    paths = StoragePaths(tmp_path / "memo-home")
    root = tmp_path / "work"
    root.mkdir()
    daemon = MemoDaemon(paths, interval=interval)
    thread = threading.Thread(target=daemon.serve_forever)
    thread.start()
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
        restored = tmp_path / "restored"
        store.restore_manifest(attached["session_id"], manifest, restored)
        assert (restored / "file.txt").read_text() == "second"
        decision = request(str(paths.socket), "attach", {"path": str(root)})
        assert decision["decision_required"] is True
    finally:
        _stop(paths, thread)


def test_snapshot_reads_do_not_trigger_more_steps(tmp_path: Path) -> None:
    paths, root, _, thread = _running(tmp_path, interval=60)
    try:
        (root / "untracked.txt").write_text("initial")
        attached = request(str(paths.socket), "attach", {"path": str(root)})
        session_id = str(attached["session_id"])
        (root / "untracked.txt").write_text("changed")
        store = SessionStore(paths)
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline and store.head(session_id).step < 1:
            time.sleep(0.05)

        assert store.head(session_id).step == 1
        assert (root / "untracked.txt").read_text() == "changed"
        time.sleep(0.75)
        assert store.head(session_id).step == 1

        restored = tmp_path / "restored-untracked"
        store.restore_manifest(session_id, store.head(session_id), restored)
        assert (restored / "untracked.txt").read_text() == "changed"
    finally:
        _stop(paths, thread)


def test_mutation_burst_is_coalesced_into_one_step(tmp_path: Path) -> None:
    paths, root, _, thread = _running(tmp_path, interval=60)
    try:
        attached = request(str(paths.socket), "attach", {"path": str(root)})
        session_id = str(attached["session_id"])
        for value in range(10):
            (root / "generated.txt").write_text(str(value))
            time.sleep(0.02)

        store = SessionStore(paths)
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline and store.head(session_id).step < 1:
            time.sleep(0.05)
        time.sleep(0.5)

        manifest = store.head(session_id)
        assert manifest.step == 1
        restored = tmp_path / "restored-burst"
        store.restore_manifest(session_id, manifest, restored)
        assert (restored / "generated.txt").read_text() == "9"
    finally:
        _stop(paths, thread)


def test_stale_terminal_attachment_prompts_on_next_attach(tmp_path: Path) -> None:
    paths, root, daemon, thread = _running(tmp_path)
    try:
        attached = request(str(paths.socket), "attach", {"path": str(root)})
        stale_seen = time.time_ns() - int((TERMINAL_STALE_SECONDS + 1) * 1_000_000_000)
        assert daemon.registry.attachment(attached["terminal_id"]).last_seen_ns > 0
        daemon.registry.touch_attachment(attached["terminal_id"], stale_seen)

        decision = request(str(paths.socket), "attach", {"path": str(root)})

        assert decision["decision_required"] is True
        assert decision["session_id"] == attached["session_id"]
        attachment = daemon.registry.attachment(attached["terminal_id"])
        assert attachment is not None
        assert attachment.detached_utc is not None
    finally:
        _stop(paths, thread)


def test_concurrent_streams_are_drained_by_end(tmp_path: Path) -> None:
    paths, root, _, thread = _running(tmp_path)
    try:
        first = request(str(paths.socket), "attach", {"path": str(root)})
        second = request(str(paths.socket), "attach", {"path": str(root)})

        def send(allocation: dict[str, object], value: bytes) -> None:
            request(
                str(paths.socket),
                "events",
                {
                    "terminal_id": allocation["terminal_id"],
                    "events": [
                        {
                            "sequence": 1,
                            "direction": "output",
                            "data": base64.b64encode(value).decode(),
                        }
                    ],
                },
            )

        clients = [
            threading.Thread(target=send, args=(first, b"first")),
            threading.Thread(target=send, args=(second, b"second")),
        ]
        for client in clients:
            client.start()
        for client in clients:
            client.join()
        warning = request(
            str(paths.socket),
            "end",
            {
                "session_id": first["session_id"],
                "terminal_id": first["terminal_id"],
            },
        )
        assert warning["confirmation_required"] is True
        assert warning["other_terminals"] == 1
        ended = request(
            str(paths.socket),
            "end",
            {
                "session_id": first["session_id"],
                "terminal_id": first["terminal_id"],
                "confirmed": True,
                "expected_revision": warning["revision"],
            },
        )
        assert ended["state"] == "complete"
        assert (
            request(
                str(paths.socket),
                "events",
                {
                    "terminal_id": second["terminal_id"],
                    "events": [],
                },
            )["recording_ended"]
            is True
        )
        assert (
            request(
                str(paths.socket),
                "end",
                {
                    "session_id": first["session_id"],
                },
            )["already_complete"]
            is True
        )
        manifest = SessionStore(paths).head(first["session_id"])
        assert manifest is not None
        assert manifest.stream_high_water == {
            first["terminal_id"]: 1,
            second["terminal_id"]: 1,
        }
        session = paths.archive / first["session_id"]
        for terminal_id in manifest.stream_high_water:
            metadata = json.loads(
                (session / "streams/terminals" / terminal_id / "stream.json").read_text()
            )
            assert metadata["highest_sequence"] == 1
    finally:
        _stop(paths, thread)


def test_queued_worker_cannot_publish_after_end(tmp_path: Path, monkeypatch) -> None:
    paths, root, daemon, thread = _running(tmp_path, interval=60)
    try:
        attached = request(str(paths.socket), "attach", {"path": str(root)})
        request(
            str(paths.socket),
            "events",
            {
                "terminal_id": attached["terminal_id"],
                "events": [
                    {
                        "sequence": 1,
                        "direction": "output",
                        "data": base64.b64encode(b"kept").decode(),
                    }
                ],
            },
        )

        worker = daemon._workers[str(attached["session_id"])]
        worker_paused = threading.Event()
        release_worker = threading.Event()
        worker_done = threading.Event()
        original_publish = daemon._publish

        def pause_worker_publish(session, **kwargs):
            if threading.current_thread() is worker:
                worker_paused.set()
                assert release_worker.wait(2)
            try:
                return original_publish(session, **kwargs)
            finally:
                if threading.current_thread() is worker:
                    worker_done.set()

        monkeypatch.setattr(daemon, "_publish", pause_worker_publish)
        daemon._step_requests[str(attached["session_id"])].set()
        assert worker_paused.wait(2)

        result: dict[str, object] = {}

        def end() -> None:
            result.update(
                request(
                    str(paths.socket),
                    "end",
                    {
                        "session_id": attached["session_id"],
                        "terminal_id": attached["terminal_id"],
                    },
                )
            )

        ending = threading.Thread(target=end)
        ending.start()
        time.sleep(0.05)
        release_worker.set()
        ending.join(3)
        assert not ending.is_alive()
        assert worker_done.wait(2)
        assert result["state"] == "complete"

        manifest = SessionStore(paths).head(str(attached["session_id"]))
        assert manifest is not None
        assert manifest.stream_high_water == {attached["terminal_id"]: 1}
    finally:
        _stop(paths, thread)


def test_end_scope_is_selected_before_completion(tmp_path: Path) -> None:
    paths, root, _, thread = _running(tmp_path)
    try:
        attached = request(str(paths.socket), "attach", {"path": str(root)})
        pending = request(
            str(paths.socket),
            "end",
            {
                "session_id": attached["session_id"],
                "terminal_id": attached["terminal_id"],
                "prompt_scope": True,
            },
        )
        assert pending["scope_confirmation_required"] is True

        ended = request(
            str(paths.socket),
            "end",
            {
                "session_id": attached["session_id"],
                "terminal_id": attached["terminal_id"],
                "capture_scope": "full",
            },
        )

        assert ended["state"] == "complete"
        assert SessionStore(paths).load_session(str(attached["session_id"])).capture_scope == "full"
    finally:
        _stop(paths, thread)


def test_agent_and_sandbox_shell_launch_metadata_are_archived(tmp_path: Path) -> None:
    paths, root, daemon, thread = _running(tmp_path)
    try:
        attached = request(str(paths.socket), "attach", {"path": str(root)})
        common = {
            "session_id": attached["session_id"],
            "terminal_id": attached["terminal_id"],
            "cwd": str(root),
            "started_utc": "start",
            "policy_summary": {"root": str(root), "environment_inherited": ["PATH"]},
            "policy_digest": "a" * 64,
        }
        request(
            str(paths.socket),
            "agent_launch",
            {
                **common,
                "launch_id": "agent-launch",
                "harness": "codex",
                "command": ["codex"],
                "effective_command": [
                    "codex",
                    "--dangerously-bypass-approvals-and-sandbox",
                ],
                "sandbox_mode": "sandbox",
                "sandbox_args": [],
                "guidance_digest": "b" * 64,
            },
        )
        request(
            str(paths.socket),
            "agent_complete",
            {"launch_id": "agent-launch", "ended_utc": "end", "exit_code": 0},
        )
        request(
            str(paths.socket),
            "sandbox_shell_launch",
            {**common, "launch_id": "shell-launch", "command": ["/bin/sh"]},
        )
        request(
            str(paths.socket),
            "sandbox_shell_complete",
            {"launch_id": "shell-launch", "ended_utc": "end", "exit_code": 7},
        )

        directory = (
            SessionStore(paths).session_path(str(attached["session_id"])) / "agents" / "launches"
        )
        agent = json.loads((directory / "agent-launch.json").read_text())
        shell = json.loads((directory / "shell-launch.json").read_text())
        assert agent["kind"] == "agent"
        assert agent["sandbox_mode"] == "sandbox"
        assert agent["exit_code"] == 0
        assert shell["kind"] == "sandbox-shell"
        assert shell["exit_code"] == 7
        assert daemon.registry.sandbox_shell_launch("shell-launch").exit_code == 7
    finally:
        _stop(paths, thread)


def test_resume_replace_and_stale_decisions(tmp_path: Path) -> None:
    paths, root, _, thread = _running(tmp_path)
    try:
        first = request(str(paths.socket), "attach", {"path": str(root)})
        request(str(paths.socket), "detach", {"terminal_id": first["terminal_id"]})
        choice = request(str(paths.socket), "attach", {"path": str(root)})
        joined = request(
            str(paths.socket),
            "attach",
            {
                "path": str(root),
                "decision": "resume",
                "expected_session_id": choice["session_id"],
                "expected_revision": choice["revision"],
            },
        )
        assert joined["session_id"] == first["session_id"]
        assert joined["terminal_id"] != first["terminal_id"]
        stale = request(
            str(paths.socket),
            "attach",
            {
                "path": str(root),
                "decision": "replace",
                "expected_session_id": choice["session_id"],
                "expected_revision": choice["revision"],
            },
        )
        assert stale["stale"] is True
        request(str(paths.socket), "detach", {"terminal_id": joined["terminal_id"]})
        replace_choice = request(str(paths.socket), "attach", {"path": str(root)})
        replacement = request(
            str(paths.socket),
            "attach",
            {
                "path": str(root),
                "decision": "replace",
                "expected_session_id": replace_choice["session_id"],
                "expected_revision": replace_choice["revision"],
            },
        )
        assert replacement["session_id"] != first["session_id"]
        assert SessionStore(paths).load_session(first["session_id"]).state == "complete"
    finally:
        _stop(paths, thread)


def test_end_rejects_stale_confirmation_after_attachment_change(tmp_path: Path) -> None:
    paths, root, daemon, thread = _running(tmp_path)
    try:
        first = request(str(paths.socket), "attach", {"path": str(root)})
        second = request(str(paths.socket), "attach", {"path": str(root)})
        warning = request(
            str(paths.socket),
            "end",
            {
                "session_id": first["session_id"],
                "terminal_id": first["terminal_id"],
            },
        )
        third = request(str(paths.socket), "attach", {"path": str(root)})
        stale = request(
            str(paths.socket),
            "end",
            {
                "session_id": first["session_id"],
                "terminal_id": first["terminal_id"],
                "confirmed": True,
                "expected_revision": warning["revision"],
            },
        )
        assert stale["stale"] is True
        assert stale["other_terminals"] == 2
        assert len(daemon.registry.attached(first["session_id"])) == 3
        for allocation in (first, second, third):
            request(str(paths.socket), "detach", {"terminal_id": allocation["terminal_id"]})
    finally:
        _stop(paths, thread)


def test_end_pushes_before_success_after_releasing_lifecycle_locks(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "memo.daemon.server.S3Config.discover",
        classmethod(lambda cls, required=False: S3Config("bucket")),
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
        ended = request(
            str(paths.socket),
            "end",
            {
                "session_id": attached["session_id"],
                "terminal_id": attached["terminal_id"],
            },
        )
        assert ended["state"] == "complete"
        assert ended["cloud"] == "pushed"
        assert called.is_set()
        assert lock_state == {"session": True, "root": True, "complete": True}
    finally:
        _stop(paths, thread)


def test_end_uses_caller_config_for_required_final_push(tmp_path: Path, monkeypatch) -> None:
    observed_payload: dict[str, object] = {}

    def fake_push(self: MemoDaemon, payload: dict[str, object]) -> dict[str, object]:
        observed_payload.update(payload)
        return {"pushed": [str(payload["session_id"])], "skipped": [], "failed": []}

    monkeypatch.setattr(MemoDaemon, "_push", fake_push)
    paths, root, _, thread = _running(tmp_path)
    try:
        attached = request(str(paths.socket), "attach", {"path": str(root)})
        ended = request(
            str(paths.socket),
            "end",
            {
                "session_id": attached["session_id"],
                "terminal_id": attached["terminal_id"],
                "s3": S3Config("bucket", access_key="access", secret_key="secret").to_dict(),
            },
        )
        assert ended["state"] == "complete"
        assert ended["cloud"] == "pushed"
        assert ended["push"]["pushed"] == [attached["session_id"]]
        assert observed_payload["session_id"] == attached["session_id"]
        assert observed_payload["s3"]["access_key"] == "access"
    finally:
        _stop(paths, thread)


def test_end_fails_on_upload_error_and_completed_session_can_be_retried(
    tmp_path: Path, monkeypatch
) -> None:
    attempts = 0

    def fake_push(self: MemoDaemon, payload: dict[str, object]) -> dict[str, object]:
        nonlocal attempts
        attempts += 1
        session_id = str(payload["session_id"])
        if attempts == 1:
            return {"pushed": [], "skipped": [], "failed": [(session_id, "denied")]}
        return {"pushed": [session_id], "skipped": [], "failed": []}

    monkeypatch.setattr(MemoDaemon, "_push", fake_push)
    paths, root, _, thread = _running(tmp_path)
    config = S3Config("bucket").to_dict()
    try:
        attached = request(str(paths.socket), "attach", {"path": str(root)})
        with pytest.raises(ProtocolError, match="cloud upload failed.*denied"):
            request(
                str(paths.socket),
                "end",
                {
                    "session_id": attached["session_id"],
                    "terminal_id": attached["terminal_id"],
                    "s3": config,
                },
            )
        assert SessionStore(paths).load_session(attached["session_id"]).state == "complete"

        retried = request(
            str(paths.socket),
            "end",
            {"session_id": attached["session_id"], "s3": config},
        )
        assert retried["already_complete"] is True
        assert retried["cloud"] == "pushed"
        assert attempts == 2
    finally:
        _stop(paths, thread)


def test_completing_a_recording_keeps_a_concurrent_upload_record(
    tmp_path: Path, monkeypatch
) -> None:
    paths, root, _daemon, thread = _running(tmp_path)
    try:
        attached = request(str(paths.socket), "attach", {"path": str(root)})
        store = SessionStore(paths)
        session_id = str(attached["session_id"])
        publish = MemoDaemon._publish

        def publish_then_record(self: MemoDaemon, session, *, force: bool = False):
            manifest = publish(self, session, force=force)
            # Stand in for the archive publisher landing while the recording is
            # being completed: it records the uploaded generation on its own.
            self.store.amend_session(
                session.session_id,
                last_pushed_step=manifest.step,
                last_pushed_digest="0" * 64,
                remote_object="remote",
            )
            return manifest

        monkeypatch.setattr(MemoDaemon, "_publish", publish_then_record)
        ended = request(
            str(paths.socket),
            "end",
            {
                "session_id": session_id,
                "terminal_id": attached["terminal_id"],
                "capture_scope": "full",
            },
        )
        assert ended["state"] == "complete"

        stored = store.load_session(session_id)
        assert stored.state == "complete"
        assert stored.capture_scope == "full"
        # Completing the recording must not revert what the upload recorded.
        assert stored.last_pushed_step is not None
        assert stored.remote_object == "remote"
    finally:
        _stop(paths, thread)
