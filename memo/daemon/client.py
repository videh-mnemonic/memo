"""Start the Memo daemon and expose request helpers for its operations."""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from ..recording.paths import StoragePaths
from ..transport.config import S3Config
from .protocol import ProtocolError, request

REMOVE_ARCHIVED_TIMEOUT_SECONDS = 15 * 60
LONG_OPERATION_TIMEOUT_SECONDS = 30 * 60


def _s3_payload() -> dict[str, Any]:
    config = S3Config.discover(required=True)
    assert config is not None
    return config.to_dict()


def ensure_daemon(paths: StoragePaths | None = None, timeout: float = 5.0) -> None:
    S3Config.discover(required=True)
    paths = paths or StoragePaths.discover()
    paths.ensure_storage()
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


def attach(
    path: Path,
    paths: StoragePaths | None = None,
    *,
    decision: str | None = None,
    expected_session_id: str | None = None,
    expected_revision: int | None = None,
) -> dict[str, Any]:
    paths = paths or StoragePaths.discover()
    ensure_daemon(paths)
    payload: dict[str, Any] = {"path": str(path)}
    if decision is not None:
        payload.update(
            {
                "decision": decision,
                "expected_session_id": expected_session_id,
                "expected_revision": expected_revision,
            }
        )
    # A first attachment creates and publishes the initial directory snapshot.
    # Large working trees can legitimately take longer than the protocol's
    # short default timeout, while the daemon continues processing the request.
    return request(str(paths.socket), "attach", payload, timeout=300.0)


def end(
    path: Path | None = None,
    paths: StoragePaths | None = None,
    *,
    session_id: str | None = None,
    terminal_id: str | None = None,
    confirmed: bool = False,
    expected_revision: int | None = None,
    capture_scope: str | None = None,
    prompt_scope: bool = False,
    allow_large: bool = False,
) -> dict[str, Any]:
    paths = paths or StoragePaths.discover()
    ensure_daemon(paths)
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
    if allow_large:
        payload["allow_large"] = True
    payload["s3"] = _s3_payload()
    return request(str(paths.socket), "end", payload, timeout=LONG_OPERATION_TIMEOUT_SECONDS)


def push(
    session_id: str | None = None,
    paths: StoragePaths | None = None,
    *,
    allow_large: bool = False,
) -> dict[str, Any]:
    paths = paths or StoragePaths.discover()
    ensure_daemon(paths)
    payload = {"session_id": session_id} if session_id else {}
    payload["s3"] = _s3_payload()
    if allow_large:
        payload["allow_large"] = True
    return request(str(paths.socket), "push", payload, timeout=LONG_OPERATION_TIMEOUT_SECONDS)


def remove_archived(
    exclude: list[str] | None = None, paths: StoragePaths | None = None
) -> dict[str, Any]:
    paths = paths or StoragePaths.discover()
    ensure_daemon(paths)
    return request(
        str(paths.socket),
        "remove_archived",
        {"exclude": exclude or []},
        timeout=REMOVE_ARCHIVED_TIMEOUT_SECONDS,
    )
