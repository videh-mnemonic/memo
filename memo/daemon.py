from __future__ import annotations

import fcntl
import os
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import IO, Any

from .step import StepPublisher, utcnow
from .config import (Paths, TransportConfig, automatic_push_enabled,
                     automatic_push_interval, checkpoint_interval, recovery_enabled,
                     watcher_debounce, watcher_enabled)
from .identity import local_namespace
from .models import DirectorySession
from .protocol import (
    DisconnectedError,
    ProtocolError,
    Request,
    Response,
    receive_request,
    request,
    send_message,
)
from .registry import ActiveSession, Registry
from .session_store import SessionStore


class DaemonAlreadyRunning(RuntimeError):
    pass


class MemoDaemon:
    def __init__(self, paths: Paths | None = None, interval: float | None = None):
        self.paths = paths or Paths.discover()
        self.paths.ensure_storage()
        assert self.paths.registry is not None
        assert self.paths.socket is not None
        self.registry = Registry(self.paths.registry)
        self.store = SessionStore(self.paths)
        from .streams import StreamStore
        self.streams = StreamStore(self.paths, self.registry)
        self.publisher = StepPublisher(
            self.store,
            lambda session: self.streams.seal_session(
                session.archive_namespace, session.session_id
            ),
        )
        self.interval = checkpoint_interval() if interval is None else interval
        self.socket_path = self.paths.socket
        self._stop = threading.Event()
        self._workers: dict[str, threading.Thread] = {}
        self._step_requests: dict[str, threading.Event] = {}
        self._observers: dict[str, Any] = {}
        self._worker_lock = threading.Lock()
        self._session_locks: dict[str, threading.RLock] = {}
        self._push_thread: threading.Thread | None = None
        self._server: socket.socket | None = None
        self._lock_handle: IO[str] | None = None

    def _acquire_daemon_lock(self) -> None:
        assert self.paths.runtime is not None
        lock_path = self.paths.runtime / "daemon.lock"
        self._lock_handle = lock_path.open("a+")
        try:
            fcntl.flock(self._lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            self._lock_handle.close()
            self._lock_handle = None
            raise DaemonAlreadyRunning("memo daemon is already running") from error

    def _session_model(self, active: ActiveSession) -> DirectorySession:
        return self.store.load_session(active.archive_namespace, active.session_id)

    def _session_lock(self, session_id: str) -> threading.RLock:
        with self._worker_lock:
            return self._session_locks.setdefault(session_id, threading.RLock())

    def _publish(self, session: DirectorySession):
        with self._session_lock(session.session_id):
            return self.publisher.publish(session)

    def _ensure_worker(self, active: ActiveSession) -> None:
        with self._worker_lock:
            worker = self._workers.get(active.session_id)
            if worker and worker.is_alive():
                return
            worker = threading.Thread(target=self._step_loop, args=(active,), daemon=True)
            self._workers[active.session_id] = worker
            self._step_requests.setdefault(active.session_id, threading.Event())
            worker.start()
            self._ensure_watcher(active)

    def _ensure_watcher(self, active: ActiveSession) -> None:
        if not watcher_enabled() or active.session_id in self._observers:
            return
        from watchdog.events import FileSystemEventHandler
        from watchdog.observers import Observer

        request_event = self._step_requests[active.session_id]

        class Handler(FileSystemEventHandler):
            def on_any_event(self, event) -> None:
                if not event.is_directory or event.event_type != "opened":
                    request_event.set()

        observer = Observer()
        observer.schedule(Handler(), str(active.root), recursive=True)
        observer.start()
        self._observers[active.session_id] = observer

    def _step_loop(self, active: ActiveSession) -> None:
        request_event = self._step_requests[active.session_id]
        deadline = time.monotonic() + self.interval
        while not self._stop.is_set():
            request_event.wait(max(0.0, deadline - time.monotonic()))
            if self._stop.is_set():
                return
            current = self.registry.lookup(active.root)
            if current is None or current.session_id != active.session_id or current.state != "active":
                return
            if request_event.is_set():
                request_event.clear()
                if self._stop.wait(watcher_debounce()):
                    return
                request_event.clear()
            try:
                self._publish(self._session_model(active))
            except Exception as error:
                print(f"memo daemon: step failed for {active.session_id}: {error}", file=sys.stderr)
            deadline = time.monotonic() + self.interval

    def _start(self, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            root = Path(payload["path"])
        except (KeyError, TypeError) as error:
            raise ProtocolError("start requires a path") from error
        canonical = root.expanduser().resolve(strict=True)
        namespace = local_namespace(canonical)
        created = utcnow()
        active, is_new = self.registry.start_or_join(canonical, namespace, created)
        if is_new:
            session = DirectorySession(
                session_id=active.session_id,
                root=str(active.root),
                archive_namespace=active.archive_namespace,
                created_utc=active.created_utc,
                updated_utc=active.created_utc,
            )
            try:
                self.store.create(session)
                manifest = self._publish(session)
            except BaseException:
                self.registry.remove(active.session_id)
                raise
        else:
            session = self._session_model(active)
            manifest = self.store.head(active.archive_namespace, active.session_id)
            if manifest is None:
                manifest = self._publish(session)
        self._ensure_worker(active)
        return {
            "session_id": active.session_id,
            "root": str(active.root),
            "archive_namespace": active.archive_namespace,
            "joined": not is_new,
            "step": manifest.step,
        }

    def _lookup(self, payload: dict[str, Any]) -> dict[str, Any]:
        active = self.registry.lookup(Path(payload["path"]))
        if active is None:
            return {"session": None}
        head = self.store.head(active.archive_namespace, active.session_id)
        return {
            "session": {
                "session_id": active.session_id,
                "root": str(active.root),
                "archive_namespace": active.archive_namespace,
                "step": head.step if head else None,
                "state": active.state,
                "attachments": len([
                    item for item in self.registry.list_attachments(active.session_id)
                    if item.detached_utc is None
                ]),
            }
        }

    def _end(self, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            root = Path(payload["path"]).expanduser().resolve(strict=True)
        except (KeyError, TypeError) as error:
            raise ProtocolError("end requires a path") from error
        active = self.registry.lookup(root)
        if active is None:
            completed = [
                session for _, session in self.store.list_sessions()
                if Path(session.root) == root and session.state == "complete"
            ]
            if completed:
                session = max(completed, key=lambda item: item.updated_utc)
                manifest = self.store.head(session.archive_namespace, session.session_id)
                return {
                    "session_id": session.session_id,
                    "state": "complete",
                    "step": manifest.step if manifest else None,
                    "already_complete": True,
                }
            raise FileNotFoundError("no active recording for path")
        if active.state == "active":
            active = self.registry.transition(active.session_id, "active", "ending")
        session = self._session_model(active)
        if session.state != "ending":
            session.state = "ending"
            session.updated_utc = utcnow()
            self.store.update_session(session)
        detached_at = utcnow()
        for attachment in self.registry.list_attachments(active.session_id):
            if attachment.detached_utc is None:
                self.registry.detach(attachment.terminal_id, detached_at)
        manifest = self._publish(session)
        session.state = "complete"
        session.updated_utc = manifest.created_utc
        self.store.update_session(session)
        self.registry.transition(active.session_id, "ending", "complete")
        self.registry.remove(active.session_id)
        request_event = self._step_requests.get(active.session_id)
        if request_event:
            request_event.set()
        observer = self._observers.pop(active.session_id, None)
        if observer:
            observer.stop()
            observer.join(timeout=2)
        return {
            "session_id": active.session_id,
            "state": "complete",
            "step": manifest.step,
            "already_complete": False,
        }

    def _status(self) -> dict[str, Any]:
        sessions = []
        for active in self.registry.list_active():
            head = self.store.head(active.archive_namespace, active.session_id)
            sessions.append({
                "session_id": active.session_id,
                "root": str(active.root),
                "archive_namespace": active.archive_namespace,
                "state": active.state,
                "step": head.step if head else None,
                "latest_step_utc": head.created_utc if head else None,
                "attachments": len([
                    item for item in self.registry.list_attachments(active.session_id)
                    if item.detached_utc is None
                ]),
            })
        return {"sessions": sessions}

    def _push(self, payload: dict[str, Any]) -> dict[str, Any]:
        from .transport import PushSummary, push_session
        config = TransportConfig.discover(required=True)
        assert config is not None
        selected = payload.get("session_id")
        summary = PushSummary()
        sessions = [session for _, session in self.store.list_sessions()
                    if selected is None or session.session_id == selected]
        if selected and not sessions:
            summary.failed.append((str(selected), "directory session not found"))
        for session in sessions:
            try:
                with self._session_lock(session.session_id):
                    result = push_session(self.store, session, config)
                target = summary.skipped if result["status"] == "skipped" else summary.pushed
                target.append(session.session_id)
            except Exception as error:
                summary.failed.append((session.session_id, str(error)))
        return {"pushed": summary.pushed, "skipped": summary.skipped,
                "failed": summary.failed}

    def _automatic_push_loop(self) -> None:
        interval = automatic_push_interval()
        while not self._stop.wait(interval):
            try:
                self._push({})
            except Exception as error:
                print(f"memo daemon: automatic push failed: {error}", file=sys.stderr)

    def dispatch(self, message: Request) -> dict[str, Any]:
        if message.operation == "health":
            return {"status": "ok"}
        if message.operation == "start":
            return self._start(message.payload)
        if message.operation == "attach":
            started = self._start(message.payload)
            attachment = self.registry.allocate_attachment(started["session_id"], utcnow())
            return {**started, "terminal_id": attachment.terminal_id,
                    "accepted_sequence": attachment.accepted_sequence}
        if message.operation == "events":
            terminal_id = str(message.payload["terminal_id"])
            attachment = self.registry.attachment(terminal_id)
            if attachment is None:
                raise KeyError(f"unknown terminal attachment: {terminal_id}")
            values = message.payload.get("events")
            if not isinstance(values, list):
                raise ProtocolError("events requires an event list")
            accepted = self.streams.append(
                attachment.session_id, terminal_id, values, time.time_ns()
            )
            return {"terminal_id": terminal_id, "accepted_sequence": accepted}
        if message.operation == "detach":
            terminal_id = str(message.payload["terminal_id"])
            self.registry.detach(terminal_id, utcnow())
            return {"terminal_id": terminal_id, "detached": True}
        if message.operation == "lookup":
            return self._lookup(message.payload)
        if message.operation == "end":
            return self._end(message.payload)
        if message.operation == "status":
            return self._status()
        if message.operation == "push":
            return self._push(message.payload)
        if message.operation == "step":
            active = self.registry.lookup(Path(message.payload["path"]))
            if active is None:
                raise FileNotFoundError("no active recording for path")
            manifest = self._publish(self._session_model(active))
            return {"session_id": active.session_id, "step": manifest.step}
        if message.operation == "shutdown":
            self._stop.set()
            return {"status": "stopping"}
        raise ProtocolError(f"unknown operation: {message.operation}")

    def _handle(self, connection: socket.socket) -> None:
        with connection:
            try:
                message = receive_request(connection)
                response = Response(True, self.dispatch(message))
            except DisconnectedError:
                return
            except Exception as error:
                response = Response(False, {}, str(error))
            try:
                send_message(connection, response)
            except (BrokenPipeError, ConnectionResetError):
                pass

    def serve_forever(self) -> None:
        self._acquire_daemon_lock()
        self.registry.remove_stale(self.paths.archive)
        if recovery_enabled():
            for active in self.registry.list_active():
                self.store.check_integrity(active.archive_namespace, active.session_id)
            self.streams.recover_all()
            self.registry.expire_attachments(utcnow())
            for active in self.registry.list_active():
                if active.state == "ending":
                    self._end({"path": str(active.root)})
        self.socket_path.unlink(missing_ok=True)
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._server = server
        try:
            server.bind(str(self.socket_path))
            os.chmod(self.socket_path, 0o600)
            server.listen(32)
            server.settimeout(0.25)
            for active in self.registry.list_active():
                self._ensure_worker(active)
            if automatic_push_enabled() and TransportConfig.discover() is not None:
                self._push_thread = threading.Thread(target=self._automatic_push_loop, daemon=True)
                self._push_thread.start()
            while not self._stop.is_set():
                try:
                    connection, _ = server.accept()
                except socket.timeout:
                    continue
                threading.Thread(target=self._handle, args=(connection,), daemon=True).start()
        finally:
            for observer in self._observers.values():
                observer.stop()
            for observer in self._observers.values():
                observer.join(timeout=2)
            server.close()
            self.socket_path.unlink(missing_ok=True)
            self.registry.close()
            if self._lock_handle:
                fcntl.flock(self._lock_handle.fileno(), fcntl.LOCK_UN)
                self._lock_handle.close()


def ensure_daemon(paths: Paths | None = None, timeout: float = 5.0) -> None:
    paths = paths or Paths.discover()
    paths.ensure_storage()
    assert paths.socket is not None
    try:
        if request(str(paths.socket), "health", timeout=0.25).get("status") == "ok":
            return
    except (OSError, ProtocolError):
        pass
    subprocess.Popen(
        [sys.executable, "-m", "memo.daemon"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        env=os.environ.copy(),
    )
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if request(str(paths.socket), "health", timeout=0.25).get("status") == "ok":
                return
        except (OSError, ProtocolError):
            time.sleep(0.05)
    raise RuntimeError("memo daemon did not start")


def activate(path: Path, paths: Paths | None = None) -> dict[str, Any]:
    paths = paths or Paths.discover()
    ensure_daemon(paths)
    assert paths.socket is not None
    return request(str(paths.socket), "start", {"path": str(path)})


def attach(path: Path, paths: Paths | None = None) -> dict[str, Any]:
    paths = paths or Paths.discover()
    ensure_daemon(paths)
    assert paths.socket is not None
    return request(str(paths.socket), "attach", {"path": str(path)})


def end(path: Path, paths: Paths | None = None) -> dict[str, Any]:
    paths = paths or Paths.discover()
    ensure_daemon(paths)
    assert paths.socket is not None
    return request(str(paths.socket), "end", {"path": str(path)}, timeout=60.0)


def push(session_id: str | None = None, paths: Paths | None = None) -> dict[str, Any]:
    paths = paths or Paths.discover()
    ensure_daemon(paths)
    assert paths.socket is not None
    payload = {"session_id": session_id} if session_id else {}
    return request(str(paths.socket), "push", payload, timeout=300.0)


def main() -> int:
    MemoDaemon().serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
