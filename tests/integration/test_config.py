from pathlib import Path

from memo.daemon.server import (
    PUSH_INTERVAL_SECONDS,
    STEP_INTERVAL_SECONDS,
    WATCHER_DEBOUNCE_SECONDS,
)
from memo.recording.paths import StoragePaths
from memo.recording.snapshots import MAX_FILE_SIZE_BYTES
from memo.transport.config import S3Config


def test_paths_discovery_uses_only_memo_home(monkeypatch, tmp_path: Path) -> None:
    home = tmp_path / "memo-home"
    monkeypatch.setenv("MEMO_HOME", str(home))
    monkeypatch.setenv("TMPDIR", str(tmp_path / "ignored-temp"))

    paths = StoragePaths.discover()
    assert paths == StoragePaths(home.resolve())
    assert paths.archive == home / "archive"
    assert paths.runtime == home / "runtime"
    assert paths.spool == home / "runtime" / "sessions"


def test_paths_accept_independent_storage_overrides(tmp_path: Path) -> None:
    home = tmp_path / "home"
    archive = tmp_path / "archive"
    runtime = tmp_path / "runtime"

    paths = StoragePaths(home, archive=archive, runtime=runtime)

    assert paths.archive == archive
    assert paths.runtime == runtime
    assert paths.socket == runtime / "memo.sock"
    assert paths.registry == runtime / "registry.sqlite"
    assert paths.spool == runtime / "sessions"


def test_transport_discovery_uses_retained_environment_surface(monkeypatch) -> None:
    monkeypatch.setenv("MEMO_S3_BUCKET", "bucket")
    monkeypatch.setenv("MEMO_S3_PREFIX", "/prefix/")
    monkeypatch.setenv("MEMO_S3_ENDPOINT", "http://localhost:9000")
    monkeypatch.setenv("MEMO_S3_REGION", "region")
    monkeypatch.setenv("MEMO_S3_PROFILE", "profile")
    monkeypatch.setenv("MEMO_S3_UPLOAD_CONCURRENCY", "7")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "access")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "token")

    assert S3Config.discover() == S3Config(
        "bucket",
        "prefix",
        "http://localhost:9000",
        "region",
        "profile",
        7,
        "access",
        "secret",
        "token",
    )


def test_transport_config_round_trips_request_payload() -> None:
    config = S3Config(
        "bucket",
        "prefix",
        "http://localhost:9000",
        "region",
        "profile",
        7,
        "access",
        "secret",
        "token",
    )

    assert S3Config.from_dict(config.to_dict()) == config


def test_transport_rejects_nonpositive_upload_concurrency(monkeypatch) -> None:
    monkeypatch.setenv("MEMO_S3_BUCKET", "bucket")
    monkeypatch.setenv("MEMO_S3_UPLOAD_CONCURRENCY", "0")

    try:
        S3Config.discover()
    except ValueError as error:
        assert "must be positive" in str(error)
    else:
        raise AssertionError("expected invalid upload concurrency to fail")


def test_transport_rejects_partial_aws_credentials(monkeypatch) -> None:
    monkeypatch.setenv("MEMO_S3_BUCKET", "bucket")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "access")

    try:
        S3Config.discover()
    except ValueError as error:
        assert "must be set together" in str(error)
    else:
        raise AssertionError("expected partial AWS credentials to fail")


def test_operational_defaults_are_fixed_code_values() -> None:
    assert STEP_INTERVAL_SECONDS == 15.0
    assert MAX_FILE_SIZE_BYTES == 100 * 1024 * 1024
    assert WATCHER_DEBOUNCE_SECONDS == 0.25
    assert PUSH_INTERVAL_SECONDS == 15 * 60.0
