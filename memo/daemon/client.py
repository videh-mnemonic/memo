from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from ..config import StoragePaths
from .protocol import ProtocolError, request


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
