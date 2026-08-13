from __future__ import annotations

import sqlite3
import threading
import uuid
from dataclasses import dataclass
from pathlib import Path


class OverlappingRootError(RuntimeError):
    pass


@dataclass(frozen=True)
class ActiveSession:
    session_id: str
    root: Path
    archive_namespace: str
    created_utc: str


@dataclass(frozen=True)
class Attachment:
    terminal_id: str
    session_id: str
    accepted_sequence: int
    attached_utc: str
    detached_utc: str | None = None


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
        self.connection = sqlite3.connect(path, timeout=30, isolation_level=None,
                                          check_same_thread=False)
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS active_sessions ("
            "root TEXT PRIMARY KEY, session_id TEXT UNIQUE NOT NULL, "
            "archive_namespace TEXT NOT NULL, created_utc TEXT NOT NULL)"
        )
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS attachments ("
            "terminal_id TEXT PRIMARY KEY, session_id TEXT NOT NULL, "
            "accepted_sequence INTEGER NOT NULL DEFAULT 0, attached_utc TEXT NOT NULL, "
            "detached_utc TEXT, FOREIGN KEY(session_id) REFERENCES active_sessions(session_id))"
        )
        self._lock = threading.RLock()

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> "Registry":
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    @staticmethod
    def _row(value: tuple[str, str, str, str]) -> ActiveSession:
        root, session_id, namespace, created = value
        return ActiveSession(session_id, Path(root), namespace, created)

    def list_active(self) -> list[ActiveSession]:
        with self._lock:
            rows = self.connection.execute(
                "SELECT root, session_id, archive_namespace, created_utc FROM active_sessions ORDER BY root"
            ).fetchall()
        return [self._row(row) for row in rows]

    def lookup(self, path: Path) -> ActiveSession | None:
        root = str(canonical_root(path))
        with self._lock:
            row = self.connection.execute(
                "SELECT root, session_id, archive_namespace, created_utc FROM active_sessions WHERE root = ?",
                (root,),
            ).fetchone()
        return self._row(row) if row else None

    def start_or_join(self, path: Path, archive_namespace: str, created_utc: str,
                      session_id: str | None = None) -> tuple[ActiveSession, bool]:
        root = canonical_root(path)
        with self._lock:
            self.connection.execute("BEGIN IMMEDIATE")
            try:
                rows = self.connection.execute(
                    "SELECT root, session_id, archive_namespace, created_utc FROM active_sessions"
                ).fetchall()
                for row in rows:
                    active = self._row(row)
                    if active.root == root:
                        self.connection.execute("COMMIT")
                        return active, False
                    if _overlaps(active.root, root):
                        raise OverlappingRootError(
                            f"recording root overlaps active session {active.session_id}: {active.root}"
                        )
                active = ActiveSession(session_id or uuid.uuid4().hex, root, archive_namespace, created_utc)
                self.connection.execute(
                    "INSERT INTO active_sessions(root, session_id, archive_namespace, created_utc) VALUES (?, ?, ?, ?)",
                    (str(root), active.session_id, archive_namespace, created_utc),
                )
                self.connection.execute("COMMIT")
                return active, True
            except BaseException:
                if self.connection.in_transaction:
                    self.connection.execute("ROLLBACK")
                raise

    def allocate_attachment(self, session_id: str, attached_utc: str,
                            terminal_id: str | None = None) -> Attachment:
        attachment = Attachment(terminal_id or uuid.uuid4().hex, session_id, 0, attached_utc)
        with self._lock:
            self.connection.execute(
                "INSERT INTO attachments(terminal_id, session_id, accepted_sequence, attached_utc) "
                "VALUES (?, ?, 0, ?)",
                (attachment.terminal_id, session_id, attached_utc),
            )
        return attachment

    def attachment(self, terminal_id: str) -> Attachment | None:
        with self._lock:
            row = self.connection.execute(
                "SELECT terminal_id, session_id, accepted_sequence, attached_utc, detached_utc "
                "FROM attachments WHERE terminal_id = ?", (terminal_id,),
            ).fetchone()
        return Attachment(*row) if row else None

    def accept_sequence(self, terminal_id: str, expected: int, accepted: int) -> None:
        with self._lock:
            cursor = self.connection.execute(
                "UPDATE attachments SET accepted_sequence = ? "
                "WHERE terminal_id = ? AND accepted_sequence = ? AND detached_utc IS NULL",
                (accepted, terminal_id, expected),
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

    def detach(self, terminal_id: str, detached_utc: str) -> None:
        with self._lock:
            self.connection.execute(
                "UPDATE attachments SET detached_utc = ? WHERE terminal_id = ? AND detached_utc IS NULL",
                (detached_utc, terminal_id),
            )

    def list_attachments(self, session_id: str) -> list[Attachment]:
        with self._lock:
            rows = self.connection.execute(
                "SELECT terminal_id, session_id, accepted_sequence, attached_utc, detached_utc "
                "FROM attachments WHERE session_id = ? ORDER BY terminal_id", (session_id,),
            ).fetchall()
        return [Attachment(*row) for row in rows]

    def remove(self, session_id: str) -> None:
        with self._lock:
            self.connection.execute("DELETE FROM active_sessions WHERE session_id = ?", (session_id,))

    def remove_stale(self, archive_root: Path) -> list[str]:
        removed = []
        for active in self.list_active():
            session = archive_root / active.archive_namespace / active.session_id / "session.json"
            if not active.root.is_dir() or not session.is_file():
                self.remove(active.session_id)
                removed.append(active.session_id)
        return removed
