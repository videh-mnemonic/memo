"""Inspect and stop the daemon without starting it or requiring S3 configuration."""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from typing import Any

from ..recording.paths import StoragePaths
from .protocol import ProtocolError, request

STOP_TIMEOUT_SECONDS = 60.0


@dataclass(frozen=True)
class LiveAttachment:
    terminal_id: str
    session_id: str
    root: str


def daemon_health(paths: StoragePaths, timeout: float = 5.0) -> dict[str, Any] | None:
    try:
        return request(str(paths.socket), "health", timeout=timeout)
    except (FileNotFoundError, ConnectionRefusedError):
        return None
    except TimeoutError as error:
        raise RuntimeError(
            f"memo daemon at {paths.socket} is running but did not answer health checks"
        ) from error
    except ProtocolError as error:
        raise RuntimeError(f"memo daemon returned an incompatible response: {error}") from error
    except OSError as error:
        if not paths.socket.exists():
            return None
        raise RuntimeError(f"cannot contact memo daemon at {paths.socket}: {error}") from error


def live_attachments(paths: StoragePaths) -> list[LiveAttachment]:
    if not paths.registry.is_file():
        return []
    uri = f"file:{paths.registry.as_posix()}?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=2)
    try:
        if not connection.execute("PRAGMA table_info(attachments)").fetchone():
            return []
        rows = connection.execute(
            "SELECT a.terminal_id, a.session_id, s.root FROM attachments a "
            "JOIN active_sessions s ON s.session_id = a.session_id "
            "WHERE a.detached_utc IS NULL"
        ).fetchall()
    finally:
        connection.close()
    return [
        LiveAttachment(str(terminal_id), str(session_id), str(root))
        for terminal_id, session_id, root in rows
    ]


def stop_daemon(
    paths: StoragePaths,
    *,
    force: bool = False,
    timeout: float = STOP_TIMEOUT_SECONDS,
) -> tuple[bool, list[LiveAttachment]]:
    health = daemon_health(paths)
    if health is None:
        return False, []
    attachments = live_attachments(paths)
    if attachments and not force:
        return False, attachments
    request(str(paths.socket), "shutdown", timeout=timeout)
    deadline = time.monotonic() + timeout
    while paths.socket.exists() and time.monotonic() < deadline:
        time.sleep(0.05)
    if paths.socket.exists():
        raise TimeoutError(f"memo daemon did not stop after {timeout} seconds")
    return True, []
