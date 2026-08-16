from __future__ import annotations

from typing import Any

from memo.daemon import client


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
        lambda socket, operation, payload, timeout=5.0: captured.update(
            {
                "socket": socket,
                "operation": operation,
                "payload": payload,
                "timeout": timeout,
            }
        )
        or {"pushed": [], "skipped": [], "failed": []},
    )

    client.push("session")

    assert captured["operation"] == "push"
    assert captured["payload"]["session_id"] == "session"
    assert captured["payload"]["s3"]["bucket"] == "bucket"
    assert captured["payload"]["s3"]["region"] == "region"
    assert captured["payload"]["s3"]["access_key"] == "access"
    assert captured["payload"]["s3"]["secret_key"] == "secret"


def test_end_wait_for_push_extends_timeout_and_sends_s3_config(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("MEMO_HOME", str(tmp_path / "memo-home"))
    monkeypatch.setenv("MEMO_S3_BUCKET", "bucket")

    captured: dict[str, Any] = {}
    monkeypatch.setattr(client, "ensure_daemon", lambda paths: None)
    monkeypatch.setattr(
        client,
        "request",
        lambda socket, operation, payload, timeout=5.0: captured.update(
            {
                "operation": operation,
                "payload": payload,
                "timeout": timeout,
            }
        )
        or {"session_id": "session", "step": 1, "already_complete": False},
    )

    client.end(session_id="session", wait_for_push=True)

    assert captured["operation"] == "end"
    assert captured["payload"]["wait_for_push"] is True
    assert captured["payload"]["s3"]["bucket"] == "bucket"
    assert captured["timeout"] == 300.0
