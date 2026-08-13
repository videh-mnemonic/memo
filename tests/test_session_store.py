import json
import tempfile
from pathlib import Path

import pytest

from memo.config import Paths
from memo.models import DirectorySession, SnapshotEntry, StepManifest
from memo.session_store import SessionStore


def _paths(tmp_path: Path) -> Paths:
    return Paths(tmp_path, tmp_path / "scratch", tmp_path / "archive", tmp_path / "unpack")


def _session(root: Path) -> DirectorySession:
    return DirectorySession("session", str(root.resolve()), "namespace", "now", "now")


def _publish(store: SessionStore, session: DirectorySession, step: int) -> StepManifest:
    temporary = Path(tempfile.mkdtemp(prefix="prepared-", dir=store.session_path("namespace", "session")))
    (temporary / "file.txt").write_text(str(step))
    return store.publish(session, StepManifest("session", step, "now", f"snapshots/{step}",
                         [SnapshotEntry("file.txt", "file", 0o644, 1)]), temporary)


def test_publishes_zero_based_steps_and_numeric_head(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    store = SessionStore(_paths(tmp_path))
    session = _session(root)
    directory = store.create(session)
    first = _publish(store, session, 0)
    second = _publish(store, session, 1)
    assert (directory / "steps/0.json").is_file()
    assert (directory / "snapshots/0/file.txt").read_text() == "0"
    assert (directory / "HEAD").read_text() == "1\n"
    assert store.step("namespace", "session", 0) == first
    assert store.step("namespace", "session", -1) == second


def test_rejects_skipped_and_invalid_selectors(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    store = SessionStore(_paths(tmp_path))
    session = _session(root)
    store.create(session)
    with pytest.raises(ValueError, match="next step"):
        _publish(store, session, 1)
    with pytest.raises(ValueError, match="invalid step selector"):
        store.step("namespace", "session", -2)
    with pytest.raises(ValueError, match="invalid step selector"):
        store.step("namespace", "session", "HEAD")


def test_unsupported_old_format_fails_explicitly(tmp_path: Path) -> None:
    store = SessionStore(_paths(tmp_path))
    path = store.session_path("namespace", "old")
    path.mkdir(parents=True)
    (path / "session.json").write_text(json.dumps({
        "session_id": "old", "root": str(tmp_path.resolve()), "archive_namespace": "namespace",
        "created_utc": "now", "updated_utc": "now", "format": "memo-directory-session",
        "format_version": 1,
    }))
    with pytest.raises(ValueError, match="unsupported directory session format"):
        store.load_session("namespace", "old")


def test_failed_head_replacement_preserves_previous_visibility(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "root"
    root.mkdir()
    store = SessionStore(_paths(tmp_path))
    session = _session(root)
    directory = store.create(session)
    first = _publish(store, session, 0)
    from memo import session_store
    original = session_store.atomic_write
    def fail_head(path: Path, data: bytes) -> None:
        if path.name == "HEAD":
            raise OSError("injected publication failure")
        original(path, data)
    monkeypatch.setattr(session_store, "atomic_write", fail_head)
    with pytest.raises(OSError, match="injected"):
        _publish(store, session, 1)
    assert (directory / "HEAD").read_text() == "0\n"
    assert store.head("namespace", "session") == first
