"""Persist active sessions, terminal attachments, capture windows, and agent launches."""

from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path


class OverlappingRootError(RuntimeError):
    pass


@dataclass(frozen=True)
class ActiveSession:
    session_id: str
    root: Path
    created_utc: str
    state: str = "active"
    revision: int = 0


@dataclass(frozen=True)
class Attachment:
    terminal_id: str
    session_id: str
    accepted_sequence: int
    attached_utc: str
    detached_utc: str | None = None
    last_seen_ns: int = 0


@dataclass(frozen=True)
class CaptureWindow:
    session_id: str
    harness: str
    cwd: str
    checkpoint: str


@dataclass(frozen=True)
class AgentLaunch:
    launch_id: str
    session_id: str
    terminal_id: str
    harness: str
    cwd: str
    command: list[str]
    started_utc: str
    ended_utc: str | None = None
    exit_code: int | None = None


def canonical_root(path: Path) -> Path:
    resolved = path.expanduser().resolve(strict=True)
    if not resolved.is_dir():
        raise NotADirectoryError(str(resolved))
    return resolved


def _overlaps(left: Path, right: Path) -> bool:
    return left == right or left in right.parents or right in left.parents


class Registry:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.path = path
        self.connection = sqlite3.connect(
            path, timeout=30, isolation_level=None, check_same_thread=False
        )
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS active_sessions ("
            "root TEXT PRIMARY KEY, session_id TEXT UNIQUE NOT NULL, "
            "created_utc TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'active', "
            "revision INTEGER NOT NULL DEFAULT 0)"
        )
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS attachments ("
            "terminal_id TEXT PRIMARY KEY, session_id TEXT NOT NULL, "
            "accepted_sequence INTEGER NOT NULL DEFAULT 0, attached_utc TEXT NOT NULL, "
            "detached_utc TEXT, last_seen_ns INTEGER NOT NULL DEFAULT 0, "
            "FOREIGN KEY(session_id) REFERENCES active_sessions(session_id))"
        )
        columns = {
            str(row[1]) for row in self.connection.execute("PRAGMA table_info(attachments)")
        }
        if "last_seen_ns" not in columns:
            self.connection.execute(
                "ALTER TABLE attachments ADD COLUMN last_seen_ns INTEGER NOT NULL DEFAULT 0"
            )
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS capture_windows ("
            "session_id TEXT NOT NULL, harness TEXT NOT NULL, cwd TEXT NOT NULL, "
            "checkpoint TEXT NOT NULL, PRIMARY KEY(session_id, harness, cwd), "
            "FOREIGN KEY(session_id) REFERENCES active_sessions(session_id))"
        )
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS agent_launches ("
            "launch_id TEXT PRIMARY KEY, session_id TEXT NOT NULL, terminal_id TEXT NOT NULL, "
            "harness TEXT NOT NULL, cwd TEXT NOT NULL, command_json TEXT NOT NULL, "
            "started_utc TEXT NOT NULL, ended_utc TEXT, exit_code INTEGER, "
            "FOREIGN KEY(session_id) REFERENCES active_sessions(session_id))"
        )
        self._lock = threading.RLock()

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> Registry:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    @staticmethod
    def _row(value: tuple[str, str, str, str, int]) -> ActiveSession:
        root, session_id, created, state, revision = value
        return ActiveSession(session_id, Path(root), created, state, revision)

    def list_active(self) -> list[ActiveSession]:
        with self._lock:
            rows = self.connection.execute(
                "SELECT root, session_id, created_utc, state, revision "
                "FROM active_sessions ORDER BY root"
            ).fetchall()
        return [self._row(row) for row in rows]

    def lookup(self, path: Path) -> ActiveSession | None:
        root = str(canonical_root(path))
        with self._lock:
            row = self.connection.execute(
                "SELECT root, session_id, created_utc, state, revision "
                "FROM active_sessions WHERE root = ?",
                (root,),
            ).fetchone()
        return self._row(row) if row else None

    def lookup_session(self, session_id: str) -> ActiveSession | None:
        with self._lock:
            row = self.connection.execute(
                "SELECT root, session_id, created_utc, state, revision "
                "FROM active_sessions WHERE session_id = ?",
                (session_id,),
            ).fetchone()
        return self._row(row) if row else None

    def create(self, path: Path, created_utc: str, session_id: str | None = None) -> ActiveSession:
        root = canonical_root(path)
        with self._lock:
            self.connection.execute("BEGIN IMMEDIATE")
            try:
                rows = self.connection.execute(
                    "SELECT root, session_id, created_utc, state, revision FROM active_sessions"
                ).fetchall()
                for row in rows:
                    active = self._row(row)
                    if active.root == root:
                        raise RuntimeError(f"recording already exists: {active.session_id}")
                    if _overlaps(active.root, root):
                        raise OverlappingRootError(
                            f"recording root overlaps active session {active.session_id}: {active.root}"
                        )
                active = ActiveSession(session_id or uuid.uuid4().hex, root, created_utc)
                self.connection.execute(
                    "INSERT INTO active_sessions(root, session_id, created_utc, state, revision) "
                    "VALUES (?, ?, ?, 'active', 0)",
                    (str(root), active.session_id, created_utc),
                )
                self.connection.execute("COMMIT")
                return active
            except BaseException:
                if self.connection.in_transaction:
                    self.connection.execute("ROLLBACK")
                raise

    def allocate_attachment(
        self, session_id: str, attached_utc: str, terminal_id: str | None = None
    ) -> Attachment:
        attachment = Attachment(
            terminal_id or uuid.uuid4().hex,
            session_id,
            0,
            attached_utc,
            None,
            time.time_ns(),
        )
        with self._lock:
            state = self.connection.execute(
                "SELECT state FROM active_sessions WHERE session_id = ?", (session_id,)
            ).fetchone()
            if state is None:
                raise KeyError(f"unknown active session: {session_id}")
            if state[0] != "active":
                raise RuntimeError(f"recording is {state[0]}: {session_id}")
            self.connection.execute(
                "INSERT INTO attachments(terminal_id, session_id, accepted_sequence, attached_utc, last_seen_ns) "
                "VALUES (?, ?, 0, ?, ?)",
                (attachment.terminal_id, session_id, attached_utc, attachment.last_seen_ns),
            )
            self.connection.execute(
                "UPDATE active_sessions SET revision = revision + 1 WHERE session_id = ?",
                (session_id,),
            )
        return attachment

    def attachment(self, terminal_id: str) -> Attachment | None:
        with self._lock:
            row = self.connection.execute(
                "SELECT terminal_id, session_id, accepted_sequence, attached_utc, detached_utc, last_seen_ns "
                "FROM attachments WHERE terminal_id = ?",
                (terminal_id,),
            ).fetchone()
        return Attachment(*row) if row else None

    def touch_attachment(self, terminal_id: str, seen_ns: int) -> None:
        with self._lock:
            self.connection.execute(
                "UPDATE attachments SET last_seen_ns = ? "
                "WHERE terminal_id = ? AND detached_utc IS NULL",
                (seen_ns, terminal_id),
            )

    def accept_sequence(
        self, terminal_id: str, expected: int, accepted: int, seen_ns: int
    ) -> None:
        with self._lock:
            cursor = self.connection.execute(
                "UPDATE attachments SET accepted_sequence = ?, last_seen_ns = ? "
                "WHERE terminal_id = ? AND accepted_sequence = ? AND detached_utc IS NULL",
                (accepted, seen_ns, terminal_id, expected),
            )
            if cursor.rowcount != 1:
                current = self.attachment(terminal_id)
                if current is None:
                    raise KeyError(f"unknown terminal attachment: {terminal_id}")
                if current.detached_utc:
                    raise RuntimeError(f"terminal attachment is detached: {terminal_id}")
                raise ValueError(
                    f"event sequence does not follow acknowledged sequence {current.accepted_sequence}"
                )

    def recover_sequence(self, terminal_id: str, accepted: int) -> None:
        with self._lock:
            cursor = self.connection.execute(
                "UPDATE attachments SET accepted_sequence = ? WHERE terminal_id = ?",
                (accepted, terminal_id),
            )
            if cursor.rowcount != 1:
                raise KeyError(f"unknown terminal attachment: {terminal_id}")

    def expire_attachments(self, detached_utc: str) -> list[str]:
        with self._lock:
            rows = self.connection.execute(
                "SELECT terminal_id FROM attachments WHERE detached_utc IS NULL"
            ).fetchall()
            self.connection.execute(
                "UPDATE attachments SET detached_utc = ? WHERE detached_utc IS NULL",
                (detached_utc,),
            )
        return [row[0] for row in rows]

    def expire_stale_attachments(
        self, cutoff_seen_ns: int, detached_utc: str
    ) -> list[str]:
        with self._lock:
            rows = self.connection.execute(
                "SELECT terminal_id, session_id FROM attachments "
                "WHERE detached_utc IS NULL AND last_seen_ns > 0 AND last_seen_ns < ?",
                (cutoff_seen_ns,),
            ).fetchall()
            self.connection.execute(
                "UPDATE attachments SET detached_utc = ? "
                "WHERE detached_utc IS NULL AND last_seen_ns > 0 AND last_seen_ns < ?",
                (detached_utc, cutoff_seen_ns),
            )
            for _terminal_id, session_id in rows:
                self.connection.execute(
                    "UPDATE active_sessions SET revision = revision + 1 WHERE session_id = ?",
                    (session_id,),
                )
        return [row[0] for row in rows]

    def detach(self, terminal_id: str, detached_utc: str) -> None:
        with self._lock:
            cursor = self.connection.execute(
                "UPDATE attachments SET detached_utc = ? WHERE terminal_id = ? AND detached_utc IS NULL",
                (detached_utc, terminal_id),
            )
            if cursor.rowcount:
                row = self.connection.execute(
                    "SELECT session_id FROM attachments WHERE terminal_id = ?", (terminal_id,)
                ).fetchone()
                if row:
                    self.connection.execute(
                        "UPDATE active_sessions SET revision = revision + 1 WHERE session_id = ?",
                        (row[0],),
                    )

    def list_attachments(self, session_id: str) -> list[Attachment]:
        with self._lock:
            rows = self.connection.execute(
                "SELECT terminal_id, session_id, accepted_sequence, attached_utc, detached_utc, last_seen_ns "
                "FROM attachments WHERE session_id = ? ORDER BY terminal_id",
                (session_id,),
            ).fetchall()
        return [Attachment(*row) for row in rows]

    def attached(self, session_id: str) -> list[Attachment]:
        return [item for item in self.list_attachments(session_id) if item.detached_utc is None]

    def create_window(
        self, session_id: str, harness: str, cwd: str, checkpoint: str
    ) -> CaptureWindow:
        with self._lock:
            self.connection.execute(
                "INSERT OR IGNORE INTO capture_windows(session_id, harness, cwd, checkpoint) "
                "VALUES (?, ?, ?, ?)",
                (session_id, harness, cwd, checkpoint),
            )
            row = self.connection.execute(
                "SELECT session_id, harness, cwd, checkpoint FROM capture_windows "
                "WHERE session_id = ? AND harness = ? AND cwd = ?",
                (session_id, harness, cwd),
            ).fetchone()
        assert row is not None
        return CaptureWindow(*row)

    def windows(self, session_id: str) -> list[CaptureWindow]:
        with self._lock:
            rows = self.connection.execute(
                "SELECT session_id, harness, cwd, checkpoint FROM capture_windows "
                "WHERE session_id = ? ORDER BY harness, cwd",
                (session_id,),
            ).fetchall()
        return [CaptureWindow(*row) for row in rows]

    def update_window(self, window: CaptureWindow, checkpoint: str) -> None:
        with self._lock:
            self.connection.execute(
                "UPDATE capture_windows SET checkpoint = ? "
                "WHERE session_id = ? AND harness = ? AND cwd = ?",
                (checkpoint, window.session_id, window.harness, window.cwd),
            )

    def remove_window(self, window: CaptureWindow) -> None:
        with self._lock:
            self.connection.execute(
                "DELETE FROM capture_windows WHERE session_id = ? AND harness = ? AND cwd = ?",
                (window.session_id, window.harness, window.cwd),
            )

    def add_launch(self, launch: AgentLaunch) -> None:
        with self._lock:
            self.connection.execute(
                "INSERT INTO agent_launches(launch_id, session_id, terminal_id, harness, cwd, "
                "command_json, started_utc) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    launch.launch_id,
                    launch.session_id,
                    launch.terminal_id,
                    launch.harness,
                    launch.cwd,
                    json.dumps(launch.command),
                    launch.started_utc,
                ),
            )

    @staticmethod
    def _launch(row: tuple[object, ...]) -> AgentLaunch:
        return AgentLaunch(
            str(row[0]),
            str(row[1]),
            str(row[2]),
            str(row[3]),
            str(row[4]),
            list(json.loads(str(row[5]))),
            str(row[6]),
            None if row[7] is None else str(row[7]),
            None if row[8] is None else int(row[8]),
        )

    def launch(self, launch_id: str) -> AgentLaunch | None:
        with self._lock:
            row = self.connection.execute(
                "SELECT launch_id, session_id, terminal_id, harness, cwd, command_json, "
                "started_utc, ended_utc, exit_code FROM agent_launches WHERE launch_id = ?",
                (launch_id,),
            ).fetchone()
        return self._launch(row) if row else None

    def launches(
        self, session_id: str, harness: str | None = None, cwd: str | None = None
    ) -> list[AgentLaunch]:
        query = (
            "SELECT launch_id, session_id, terminal_id, harness, cwd, command_json, "
            "started_utc, ended_utc, exit_code FROM agent_launches WHERE session_id = ?"
        )
        values: list[object] = [session_id]
        if harness is not None:
            query += " AND harness = ?"
            values.append(harness)
        if cwd is not None:
            query += " AND cwd = ?"
            values.append(cwd)
        query += " ORDER BY started_utc, launch_id"
        with self._lock:
            rows = self.connection.execute(query, values).fetchall()
        return [self._launch(row) for row in rows]

    def finish_launch(self, launch_id: str, ended_utc: str, exit_code: int) -> AgentLaunch:
        with self._lock:
            cursor = self.connection.execute(
                "UPDATE agent_launches SET ended_utc = ?, exit_code = ? "
                "WHERE launch_id = ? AND ended_utc IS NULL",
                (ended_utc, exit_code, launch_id),
            )
            if cursor.rowcount != 1:
                raise KeyError(f"unknown or completed agent launch: {launch_id}")
        launch = self.launch(launch_id)
        assert launch is not None
        return launch

    def transition(self, session_id: str, expected: str, state: str) -> ActiveSession:
        if expected not in {"active", "ending"} or state not in {"ending", "complete"}:
            raise ValueError("invalid session transition")
        with self._lock:
            cursor = self.connection.execute(
                "UPDATE active_sessions SET state = ?, revision = revision + 1 "
                "WHERE session_id = ? AND state = ?",
                (state, session_id, expected),
            )
            row = self.connection.execute(
                "SELECT root, session_id, created_utc, state, revision "
                "FROM active_sessions WHERE session_id = ?",
                (session_id,),
            ).fetchone()
        if row is None:
            raise KeyError(f"unknown active session: {session_id}")
        active = self._row(row)
        if cursor.rowcount != 1 and active.state != state:
            raise RuntimeError(
                f"cannot transition session {session_id} from {active.state} to {state}"
            )
        return active

    def remove(self, session_id: str) -> None:
        with self._lock:
            self.connection.execute(
                "DELETE FROM agent_launches WHERE session_id = ?", (session_id,)
            )
            self.connection.execute(
                "DELETE FROM capture_windows WHERE session_id = ?", (session_id,)
            )
            self.connection.execute("DELETE FROM attachments WHERE session_id = ?", (session_id,))
            self.connection.execute(
                "DELETE FROM active_sessions WHERE session_id = ?", (session_id,)
            )

    def remove_stale(self, archive_root: Path) -> list[str]:
        removed = []
        for active in self.list_active():
            session = archive_root / active.session_id / "session.json"
            if not active.root.is_dir() or not session.is_file():
                self.remove(active.session_id)
                removed.append(active.session_id)
        return removed
