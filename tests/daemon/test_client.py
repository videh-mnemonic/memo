from __future__ import annotations

from typing import Any

import pytest

from memo.daemon import client
from memo.runtime import RUNTIME_ID


def test_ensure_daemon_accepts_matching_runtime(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("MEMO_S3_BUCKET", "bucket")
    monkeypatch.setattr(
        client,
        "request",
        lambda *_args, **_kwargs: {"status": "ok", "runtime_id": RUNTIME_ID},
    )

    client.ensure_daemon(client.StoragePaths(tmp_path / "home"))


def test_ensure_daemon_rejects_legacy_or_stale_runtime(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("MEMO_S3_BUCKET", "bucket")
    monkeypatch.setattr(client, "request", lambda *_args, **_kwargs: {"status": "ok"})

    with pytest.raises(RuntimeError, match="running different code"):
        client.ensure_daemon(client.StoragePaths(tmp_path / "home"))


def test_attach_waits_for_initial_snapshot(monkeypatch, tmp_path) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(client, "ensure_daemon", lambda paths: None)
    monkeypatch.setattr(
        client,
        "request",
        lambda socket, operation, payload, timeout=10.0: (
            captured.update(
                {
                    "operation": operation,
                    "payload": payload,
                    "timeout": timeout,
                }
            )
            or {"session_id": "session"}
        ),
    )

    client.attach(tmp_path)

    assert captured["operation"] == "attach"
    assert captured["payload"] == {"path": str(tmp_path)}
    assert captured["timeout"] == 300.0


def test_push_sends_caller_s3_config(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("MEMO_HOME", str(tmp_path / "memo-home"))
    monkeypatch.setenv("MEMO_S3_BUCKET", "bucket")
    monkeypatch.setenv("MEMO_S3_REGION", "region")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "access")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")

    captured: dict[str, Any] = {}
    monkeypatch.setattr(client, "ensure_daemon", lambda paths: None)
    monkeypatch.setattr(
        client,
        "request",
        lambda socket, operation, payload, timeout=5.0: (
            captured.update(
                {
                    "socket": socket,
                    "operation": operation,
                    "payload": payload,
                    "timeout": timeout,
                }
            )
            or {"pushed": [], "skipped": [], "failed": []}
        ),
    )

    client.push("session")

    assert captured["operation"] == "push"
    assert captured["payload"]["session_id"] == "session"
    assert captured["payload"]["s3"]["bucket"] == "bucket"
    assert captured["payload"]["s3"]["region"] == "region"
    assert captured["payload"]["s3"]["access_key"] == "access"
    assert captured["payload"]["s3"]["secret_key"] == "secret"
    assert captured["timeout"] == client.LONG_OPERATION_TIMEOUT_SECONDS


def test_end_waits_for_push_and_sends_s3_config(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("MEMO_HOME", str(tmp_path / "memo-home"))
    monkeypatch.setenv("MEMO_S3_BUCKET", "bucket")

    captured: dict[str, Any] = {}
    monkeypatch.setattr(client, "ensure_daemon", lambda paths: None)
    monkeypatch.setattr(
        client,
        "request",
        lambda socket, operation, payload, timeout=5.0: (
            captured.update(
                {
                    "operation": operation,
                    "payload": payload,
                    "timeout": timeout,
                }
            )
            or {"session_id": "session", "step": 1, "already_complete": False}
        ),
    )

    client.end(session_id="session")

    assert captured["operation"] == "end"
    assert "wait_for_push" not in captured["payload"]
    assert captured["payload"]["s3"]["bucket"] == "bucket"
    assert captured["timeout"] == client.LONG_OPERATION_TIMEOUT_SECONDS
