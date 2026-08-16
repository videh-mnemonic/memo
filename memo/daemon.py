from __future__ import annotations

import fcntl
import os
import subprocess
import sys
import threading
import time
from multiprocessing.connection import Client, Connection, Listener
from pathlib import Path
from typing import IO, Any

from .step import StepPublisher, utcnow
from .config import (PUSH_INTERVAL_SECONDS, STEP_INTERVAL_SECONDS,
                     WATCHER_DEBOUNCE_SECONDS, StoragePaths, TransportConfig)
from .models import DirectorySession, SessionOrigin
from .protocol import (
    DisconnectedError,
    ProtocolError,
    Request,
    Response,
    receive_request,
    request,
    send_message,
)
from .registry import ActiveSession, AgentLaunch, Registry
from .session_store import SessionStore
from .agents.collector import TraceCollector
from .agents.harnesses import get_harness
from .agents.tracewatch import capture


class DaemonAlreadyRunning(RuntimeError):
    pass


class MemoDaemon:
    def __init__(self, paths: StoragePaths | None = None, interval: float | None = None):
        self.paths = paths or StoragePaths.discover()
        self.paths.ensure_storage()
        assert self.paths.registry is not None
        assert self.paths.socket is not None
        self.registry = Registry(self.paths.registry)
        self.store = SessionStore(self.paths)
        from .streams import StreamStore
        self.streams = StreamStore(self.paths, self.registry)
        self.publisher = StepPublisher(
            self.store,
            lambda session: self.streams.seal_session(session.session_id),
        )
        self.collector = TraceCollector(self.store, self.registry)
        self.interval = STEP_INTERVAL_SECONDS if interval is None else interval
        self.socket_path = self.paths.socket
        self._stop = threading.Event()
        self._workers: dict[str, threading.Thread] = {}
        self._step_requests: dict[str, threading.Event] = {}
        self._observers: dict[str, Any] = {}
        self._worker_lock = threading.Lock()
        self._session_locks: dict[str, threading.RLock] = {}
        self._root_locks: dict[str, threading.RLock] = {}
        self._push_thread: threading.Thread | None = None
        self._server: Listener | None = None
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
        return self.store.load_session(active.session_id)

    def _session_lock(self, session_id: str) -> threading.RLock:
        with self._worker_lock:
            return self._session_locks.setdefault(session_id, threading.RLock())

    def _root_lock(self, root: Path) -> threading.RLock:
        key = str(root.resolve())
        with self._worker_lock:
            return self._root_locks.setdefault(key, threading.RLock())

    def _publish(self, session: DirectorySession):
        with self._session_lock(session.session_id):
            self.collector.collect(session.session_id)
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
        if active.session_id in self._observers:
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
                if self._stop.wait(WATCHER_DEBOUNCE_SECONDS):
                    return
                request_event.clear()
            try:
                self._publish(self._session_model(active))
            except Exception as error:
                print(f"memo daemon: step failed for {active.session_id}: {error}", file=sys.stderr)
            deadline = time.monotonic() + self.interval

    def _create(self, root: Path) -> dict[str, Any]:
        canonical = root.expanduser().resolve(strict=True)
        created = utcnow()
        active = self.registry.create(canonical, created)
        session = DirectorySession(
            session_id=active.session_id,
            root=str(active.root),
            created_utc=active.created_utc,
            updated_utc=active.created_utc,
            origin=SessionOrigin.current(),
        )
        try:
            self.store.create(session)
            manifest = self._publish(session)
        except BaseException:
            self.registry.remove(active.session_id)
            raise
        self._ensure_worker(active)
        return {"session_id": active.session_id, "root": str(active.root), "step": manifest.step}

    def _open(self, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            root = Path(payload["path"])
        except (KeyError, TypeError) as error:
            raise ProtocolError("open requires a path") from error
        canonical = root.expanduser().resolve(strict=True)
        with self._root_lock(canonical):
            active = self.registry.lookup(canonical)
            if active is None:
                created = self._create(canonical)
                active = self.registry.lookup_session(str(created["session_id"]))
                assert active is not None
            else:
                attached = self.registry.attached(active.session_id)
                decision = payload.get("decision")
                if not attached and decision is None:
                    return {"decision_required": True, "session_id": active.session_id,
                            "revision": active.revision, "root": str(active.root)}
                expected = payload.get("expected_session_id")
                revision = payload.get("expected_revision")
                if decision is not None and (
                    expected != active.session_id or revision != active.revision
                ):
                    return {"stale": True, "session_id": active.session_id,
                            "revision": active.revision,
                            "attachments": len(self.registry.attached(active.session_id))}
                if decision == "replace":
                    if attached:
                        return {"stale": True, "session_id": active.session_id,
                                "revision": active.revision, "attachments": len(attached)}
                    self._finish(active)
                    created = self._create(canonical)
                    active = self.registry.lookup_session(str(created["session_id"]))
                    assert active is not None
                elif decision not in (None, "resume"):
                    raise ProtocolError(f"invalid open decision: {decision}")
            attachment = self.registry.allocate_attachment(active.session_id, utcnow())
            manifest = self.store.head(active.session_id)
            assert manifest is not None
            self._ensure_worker(active)
            return {"session_id": active.session_id, "root": str(active.root),
                    "terminal_id": attachment.terminal_id,
                    "accepted_sequence": attachment.accepted_sequence,
                    "step": manifest.step}

    def _resolve_active(self, payload: dict[str, Any]) -> ActiveSession | None:
        session_id = payload.get("session_id")
        if session_id:
            return self.registry.lookup_session(str(session_id))
        path = payload.get("path")
        if path is None:
            raise ProtocolError("operation requires a session ID or path")
        return self.registry.lookup(Path(path))

    def _end(self, payload: dict[str, Any]) -> dict[str, Any]:
        active = self._resolve_active(payload)
        if active is None:
            session_id = payload.get("session_id")
            if session_id:
                try:
                    session = self.store.load_session(str(session_id))
                except (FileNotFoundError, ValueError):
                    pass
                else:
                    if session.state == "complete":
                        head = self.store.head(session.session_id)
                        return {
                            "session_id": session.session_id,
                            "state": "complete",
                            "step": None if head is None else head.step,
                            "already_complete": True,
                        }
            raise FileNotFoundError("no active recording for path")
        with self._root_lock(active.root):
            active = self.registry.lookup_session(active.session_id)
            if active is None:
                raise FileNotFoundError("recording already completed")
            others = self.registry.attached(active.session_id)
            caller = payload.get("terminal_id")
            if caller and any(item.terminal_id == caller for item in others):
                others = [item for item in others if item.terminal_id != caller]
            expected = payload.get("expected_revision")
            if expected is not None and int(expected) != active.revision:
                return {"stale": True, "session_id": active.session_id,
                        "revision": active.revision, "other_terminals": len(others)}
            if others and (not payload.get("confirmed") or expected is None):
                return {"confirmation_required": True, "session_id": active.session_id,
                        "revision": active.revision, "other_terminals": len(others)}
            session = self.store.load_session(active.session_id)
            selected_scope = payload.get("capture_scope")
            if selected_scope is not None and selected_scope not in {"partial", "full"}:
                raise ProtocolError("end capture scope must be partial or full")
            if (session.capture_scope == "partial" and selected_scope is None
                    and payload.get("prompt_scope")):
                return {
                    "scope_confirmation_required": True,
                    "session_id": active.session_id,
                    "revision": active.revision,
                    "other_terminals": len(others),
                }
            result = self._finish(active, capture_scope=selected_scope)
        # Local completion is authoritative. Cloud publication starts only after
        # lifecycle locks have been released and cannot delay or roll it back.
        if TransportConfig.discover() is not None:
            self._schedule_push(active.session_id)
            result["cloud"] = "pending"
        return result

    def _schedule_push(self, session_id: str) -> None:
        def upload() -> None:
            try:
                result = self._push({"session_id": session_id})
                if result["failed"]:
                    print(
                        f"memo daemon: final cloud push pending for {session_id}: "
                        f"{result['failed'][0][1]}",
                        file=sys.stderr,
                    )
            except Exception as error:
                print(
                    f"memo daemon: final cloud push pending for {session_id}: {error}",
                    file=sys.stderr,
                )

        threading.Thread(target=upload, daemon=True).start()

    def _finish(self, active: ActiveSession,
                capture_scope: str | None = None) -> dict[str, Any]:
        with self._session_lock(active.session_id):
            return self._finish_locked(active, capture_scope)

    def _finish_locked(self, active: ActiveSession,
                       capture_scope: str | None = None) -> dict[str, Any]:
        session = self._session_model(active)
        if session.state == "complete":
            if active.state == "ending":
                self.registry.transition(active.session_id, "ending", "complete")
            self.registry.remove(active.session_id)
            self._cleanup_runtime(active.session_id)
            head = self.store.head(active.session_id)
            return {
                "session_id": active.session_id,
                "state": "complete",
                "step": None if head is None else head.step,
                "already_complete": True,
            }
        if active.state == "active":
            active = self.registry.transition(active.session_id, "active", "ending")
        if capture_scope is not None:
            session.capture_scope = capture_scope
        if session.state != "ending":
            session.state = "ending"
            session.updated_utc = utcnow()
            self.store.update_session(session)
        self.streams.drain_and_detach(active.session_id, utcnow())
        manifest = self._publish(session)
        session.state = "complete"
        session.updated_utc = manifest.created_utc
        self.store.update_session(session)
        self.registry.transition(active.session_id, "ending", "complete")
        self.registry.remove(active.session_id)
        self._cleanup_runtime(active.session_id)
        return {
            "session_id": active.session_id,
            "state": "complete",
            "step": manifest.step,
            "already_complete": False,
        }

    def _cleanup_runtime(self, session_id: str) -> None:
        request_event = self._step_requests.get(session_id)
        if request_event:
            request_event.set()
        observer = self._observers.pop(session_id, None)
        if observer:
            observer.stop()
            observer.join(timeout=2)

    def _agent_launch(self, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            session_id = str(payload["session_id"])
            terminal_id = str(payload["terminal_id"])
            harness_name = str(payload["harness"])
            launch_id = str(payload["launch_id"])
            cwd = Path(payload["cwd"]).expanduser().resolve(strict=True)
            command = payload["command"]
            started_utc = str(payload["started_utc"])
        except (KeyError, TypeError, OSError) as error:
            raise ProtocolError("invalid agent launch") from error
        if not cwd.is_dir() or not isinstance(command, list) or not all(
            isinstance(value, str) for value in command
        ):
            raise ProtocolError("invalid agent launch")
        harness = get_harness(harness_name)
        active = self.registry.lookup_session(session_id)
        attachment = self.registry.attachment(terminal_id)
        if active is None or active.state != "active":
            raise RuntimeError(f"recording is not active: {session_id}")
        if (attachment is None or attachment.session_id != session_id
                or attachment.detached_utc is not None):
            raise RuntimeError(f"terminal is not attached to recording: {terminal_id}")
        with self._session_lock(session_id):
            existing = [window for window in self.registry.windows(session_id)
                        if window.harness == harness_name and window.cwd == str(cwd)]
            if not existing:
                checkpoint = capture(harness.trace_roots()).to_json()
                self.registry.create_window(session_id, harness_name, str(cwd), checkpoint)
            self.registry.add_launch(AgentLaunch(
                launch_id, session_id, terminal_id, harness_name, str(cwd),
                list(command), started_utc,
            ))
        return {"launch_id": launch_id, "capture": "active"}

    def _agent_complete(self, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            launch_id = str(payload["launch_id"])
            ended_utc = str(payload["ended_utc"])
            exit_code = int(payload["exit_code"])
        except (KeyError, TypeError, ValueError) as error:
            raise ProtocolError("invalid agent completion") from error
        launch = self.registry.launch(launch_id)
        if launch is None:
            raise KeyError(f"unknown agent launch: {launch_id}")
        with self._session_lock(launch.session_id):
            completed = self.registry.finish_launch(launch_id, ended_utc, exit_code)
            active = self.registry.lookup_session(completed.session_id)
            if active is not None:
                self._publish(self._session_model(active))
        return {"launch_id": launch_id, "capture": "complete"}

    def _push(self, payload: dict[str, Any]) -> dict[str, Any]:
        from .transport import (PushSummary, prepare_generation,
                                publish_generation)
        config = TransportConfig.discover(required=True)
        assert config is not None
        selected = payload.get("session_id")
        summary = PushSummary()
        sessions = [session for _, session in self.store.list_sessions()
                    if selected is None or session.session_id == selected]
        if selected and not sessions:
            summary.failed.append((str(selected), "directory session not found"))
        for session in sessions:
            prepared = None
            try:
                with self._session_lock(session.session_id):
                    session = self.store.load_session(session.session_id)
                    manifest = self.store.head(session.session_id)
                    if manifest is None:
                        raise ValueError(
                            f"session has no published step: {session.session_id}"
                        )
                    if session.last_pushed_step == manifest.step:
                        result = {"status": "skipped"}
                    else:
                        prepared = prepare_generation(self.store, session)
                if prepared is not None:
                    result = publish_generation(
                        self.store, session, prepared, config, update_local=False
                    )
                    with self._session_lock(session.session_id):
                        current = self.store.load_session(session.session_id)
                        if (current.last_pushed_step is None
                                or current.last_pushed_step <= prepared.step):
                            current.last_pushed_step = prepared.step
                            current.last_pushed_digest = prepared.digest
                            current.remote_object = str(result["object"])
                            self.store.update_session(current)
                target = summary.skipped if result["status"] == "skipped" else summary.pushed
                target.append(session.session_id)
            except Exception as error:
                summary.failed.append((session.session_id, str(error)))
            finally:
                if prepared is not None:
                    prepared.cleanup()
        return {"pushed": summary.pushed, "skipped": summary.skipped,
                "failed": summary.failed}

    def _remove_archived(self, payload: dict[str, Any]) -> dict[str, Any]:
        excluded = payload.get("exclude", [])
        if not isinstance(excluded, list) or not all(isinstance(value, str) for value in excluded):
            raise ProtocolError("remove_archived exclude must be a list of session IDs")
        excluded_ids = set(excluded)
        removed: list[str] = []
        retained: list[tuple[str, str]] = []
        failed: list[tuple[str, str]] = []
        for _, listed in self.store.list_sessions():
            session_id = listed.session_id
            if session_id in excluded_ids:
                retained.append((session_id, "push failed"))
                continue
            with self._session_lock(session_id):
                try:
                    self.store.remove_archived(session_id)
                    removed.append(session_id)
                except ValueError as error:
                    retained.append((session_id, str(error)))
                except OSError as error:
                    failed.append((session_id, str(error)))
        return {"removed": removed, "retained": retained, "failed": failed}

    def _automatic_push_loop(self) -> None:
        interval = PUSH_INTERVAL_SECONDS
        while not self._stop.wait(interval):
            try:
                self._push({})
            except Exception as error:
                print(f"memo daemon: automatic push failed: {error}", file=sys.stderr)

    def dispatch(self, message: Request) -> dict[str, Any]:
        if message.operation == "health":
            return {"status": "ok"}
        if message.operation == "attach":
            return self._open(message.payload)
        if message.operation == "events":
            terminal_id = str(message.payload["terminal_id"])
            attachment = self.registry.attachment(terminal_id)
            if attachment is None:
                return {"terminal_id": terminal_id, "recording_ended": True}
            active = self.registry.lookup_session(attachment.session_id)
            if active is None or active.state != "active" or attachment.detached_utc:
                return {"terminal_id": terminal_id, "recording_ended": True}
            values = message.payload.get("events")
            if not isinstance(values, list):
                raise ProtocolError("events requires an event list")
            try:
                accepted = self.streams.append(
                    attachment.session_id, terminal_id, values, time.time_ns()
                )
            except (KeyError, RuntimeError):
                return {"terminal_id": terminal_id, "recording_ended": True}
            return {"terminal_id": terminal_id, "accepted_sequence": accepted}
        if message.operation == "detach":
            terminal_id = str(message.payload["terminal_id"])
            attachment = self.registry.attachment(terminal_id)
            active = None if attachment is None else self.registry.lookup_session(
                attachment.session_id
            )
            if active is not None:
                with self._root_lock(active.root):
                    self.streams.detach(terminal_id, utcnow())
            return {"terminal_id": terminal_id, "detached": True}
        if message.operation == "end":
            return self._end(message.payload)
        if message.operation == "push":
            return self._push(message.payload)
        if message.operation == "remove_archived":
            return self._remove_archived(message.payload)
        if message.operation == "agent_launch":
            return self._agent_launch(message.payload)
        if message.operation == "agent_complete":
            return self._agent_complete(message.payload)
        if message.operation == "shutdown":
            self._stop.set()
            try:
                with Client(str(self.socket_path), family="AF_UNIX"):
                    pass
            except OSError:
                pass
            return {"status": "stopping"}
        raise ProtocolError(f"unknown operation: {message.operation}")

    def _handle(self, connection: Connection) -> None:
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
        for active in self.registry.list_active():
            self.store.check_integrity(active.session_id)
        self.streams.recover_all()
        self.registry.expire_attachments(utcnow())
        for active in self.registry.list_active():
            if active.state == "ending":
                self._finish(active)
            elif active.state == "complete":
                self.registry.remove(active.session_id)
        self.socket_path.unlink(missing_ok=True)
        server = Listener(str(self.socket_path), family="AF_UNIX", backlog=32)
        self._server = server
        try:
            os.chmod(self.socket_path, 0o600)
            for active in self.registry.list_active():
                self._ensure_worker(active)
            if TransportConfig.discover() is not None:
                self._push_thread = threading.Thread(target=self._automatic_push_loop, daemon=True)
                self._push_thread.start()
            while not self._stop.is_set():
                connection = server.accept()
                if self._stop.is_set():
                    connection.close()
                    break
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


def ensure_daemon(paths: StoragePaths | None = None, timeout: float = 5.0) -> None:
    paths = paths or StoragePaths.discover()
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


def attach(path: Path, paths: StoragePaths | None = None, *, decision: str | None = None,
           expected_session_id: str | None = None,
           expected_revision: int | None = None) -> dict[str, Any]:
    paths = paths or StoragePaths.discover()
    ensure_daemon(paths)
    assert paths.socket is not None
    payload: dict[str, Any] = {"path": str(path)}
    if decision is not None:
        payload.update({"decision": decision, "expected_session_id": expected_session_id,
                        "expected_revision": expected_revision})
    return request(str(paths.socket), "attach", payload)


def end(path: Path | None = None, paths: StoragePaths | None = None, *,
        session_id: str | None = None, terminal_id: str | None = None,
        confirmed: bool = False, expected_revision: int | None = None,
        capture_scope: str | None = None, prompt_scope: bool = False) -> dict[str, Any]:
    paths = paths or StoragePaths.discover()
    ensure_daemon(paths)
    assert paths.socket is not None
    payload: dict[str, Any] = {}
    if path is not None:
        payload["path"] = str(path)
    if session_id is not None:
        payload["session_id"] = session_id
    if terminal_id is not None:
        payload["terminal_id"] = terminal_id
    if confirmed:
        payload["confirmed"] = True
    if expected_revision is not None:
        payload["expected_revision"] = expected_revision
    if capture_scope is not None:
        payload["capture_scope"] = capture_scope
    if prompt_scope:
        payload["prompt_scope"] = True
    return request(str(paths.socket), "end", payload, timeout=60.0)


def push(session_id: str | None = None, paths: StoragePaths | None = None) -> dict[str, Any]:
    paths = paths or StoragePaths.discover()
    ensure_daemon(paths)
    assert paths.socket is not None
    payload = {"session_id": session_id} if session_id else {}
    return request(str(paths.socket), "push", payload, timeout=300.0)


def remove_archived(exclude: list[str] | None = None,
                    paths: StoragePaths | None = None) -> dict[str, Any]:
    paths = paths or StoragePaths.discover()
    ensure_daemon(paths)
    assert paths.socket is not None
    return request(
        str(paths.socket), "remove_archived", {"exclude": exclude or []}, timeout=60.0
    )


def main() -> int:
    MemoDaemon().serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
