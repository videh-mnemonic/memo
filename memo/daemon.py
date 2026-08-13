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

from .checkpoint import CheckpointPublisher, utcnow
from .config import Paths, checkpoint_interval
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
        self.publisher = CheckpointPublisher(
            self.store,
            lambda session: self.streams.seal_session(
                session.archive_namespace, session.session_id
            ),
        )
        self.interval = checkpoint_interval() if interval is None else interval
        self.socket_path = self.paths.socket
        self._stop = threading.Event()
        self._workers: dict[str, threading.Thread] = {}
        self._worker_lock = threading.Lock()
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

    def _ensure_worker(self, active: ActiveSession) -> None:
        with self._worker_lock:
            worker = self._workers.get(active.session_id)
            if worker and worker.is_alive():
                return
            worker = threading.Thread(target=self._checkpoint_loop, args=(active,), daemon=True)
            self._workers[active.session_id] = worker
            worker.start()

    def _checkpoint_loop(self, active: ActiveSession) -> None:
        while not self._stop.wait(self.interval):
            try:
                self.publisher.publish(self._session_model(active))
            except Exception as error:
                print(f"memo daemon: checkpoint failed for {active.session_id}: {error}", file=sys.stderr)

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
                manifest = self.publisher.publish(session)
            except BaseException:
                self.registry.remove(active.session_id)
                raise
        else:
            session = self._session_model(active)
            manifest = self.store.head(active.archive_namespace, active.session_id)
            if manifest is None:
                manifest = self.publisher.publish(session)
        self._ensure_worker(active)
        return {
            "session_id": active.session_id,
            "root": str(active.root),
            "archive_namespace": active.archive_namespace,
            "joined": not is_new,
            "generation": manifest.generation,
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
                "generation": head.generation if head else 0,
            }
        }

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
        if message.operation == "checkpoint":
            active = self.registry.lookup(Path(message.payload["path"]))
            if active is None:
                raise FileNotFoundError("no active recording for path")
            manifest = self.publisher.publish(self._session_model(active))
            return {"session_id": active.session_id, "generation": manifest.generation,
                    "checkpoint_id": manifest.checkpoint_id}
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
            while not self._stop.is_set():
                try:
                    connection, _ = server.accept()
                except socket.timeout:
                    continue
                threading.Thread(target=self._handle, args=(connection,), daemon=True).start()
        finally:
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


def main() -> int:
    MemoDaemon().serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
