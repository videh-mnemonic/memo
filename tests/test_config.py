from pathlib import Path

from memo.daemon.server import (PUSH_INTERVAL_SECONDS, STEP_INTERVAL_SECONDS,
                                WATCHER_DEBOUNCE_SECONDS)
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


def test_transport_discovery_uses_retained_environment_surface(monkeypatch) -> None:
    monkeypatch.setenv("MEMO_S3_BUCKET", "bucket")
    monkeypatch.setenv("MEMO_S3_PREFIX", "/prefix/")
    monkeypatch.setenv("MEMO_S3_ENDPOINT", "http://localhost:9000")
    monkeypatch.setenv("MEMO_S3_REGION", "region")
    monkeypatch.setenv("MEMO_S3_PROFILE", "profile")

    assert S3Config.discover() == S3Config(
        "bucket", "prefix", "http://localhost:9000", "region", "profile"
    )


def test_operational_defaults_are_fixed_code_values() -> None:
    assert STEP_INTERVAL_SECONDS == 15.0
    assert MAX_FILE_SIZE_BYTES == 100 * 1024 * 1024
    assert WATCHER_DEBOUNCE_SECONDS == 0.25
    assert PUSH_INTERVAL_SECONDS == 15 * 60.0
