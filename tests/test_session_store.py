import json
import tempfile
from pathlib import Path

import pytest

from memo.config import Paths
from memo.models import DirectorySession, SessionOrigin, SnapshotEntry, StepManifest
from memo.session_store import SessionNotFoundError, SessionStore


def _paths(tmp_path: Path) -> Paths:
    return Paths(tmp_path)


def _session(root: Path) -> DirectorySession:
    return DirectorySession("session", str(root.resolve()), "now", "now",
                            SessionOrigin("1.0.0", "user", "host"))


def _publish(store: SessionStore, session: DirectorySession, step: int) -> StepManifest:
    temporary = Path(tempfile.mkdtemp(prefix="prepared-", dir=store.session_path("session")))
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
    assert store.step("session", 0) == first
    assert store.step("session", -1) == second


def test_rejects_skipped_and_invalid_selectors(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    store = SessionStore(_paths(tmp_path))
    session = _session(root)
    store.create(session)
    with pytest.raises(ValueError, match="next step"):
        _publish(store, session, 1)
    with pytest.raises(ValueError, match="invalid step selector"):
        store.step("session", -2)
    with pytest.raises(ValueError, match="invalid step selector"):
        store.step("session", "HEAD")


def test_unsupported_old_format_fails_explicitly(tmp_path: Path) -> None:
    store = SessionStore(_paths(tmp_path))
    path = store.session_path("old")
    path.mkdir(parents=True)
    (path / "session.json").write_text(json.dumps({
        "session_id": "old", "root": str(tmp_path.resolve()),
        "origin": {"memo_version_id": "1.0.0", "username": "user", "hostname": "host"},
        "created_utc": "now", "updated_utc": "now", "format": "memo-directory-session",
        "format_version": 1,
    }))
    with pytest.raises(ValueError, match="unsupported directory session format"):
        store.load_session("old")


def test_sessions_default_to_partial_and_validate_capture_scope(tmp_path: Path) -> None:
    session = _session(tmp_path)
    assert session.capture_scope == "partial"
    session.capture_scope = "unknown"
    with pytest.raises(ValueError, match="invalid capture scope"):
        session.validate()


def test_session_json_without_capture_scope_loads_as_partial(tmp_path: Path) -> None:
    value = _session(tmp_path).to_dict()
    value.pop("capture_scope")
    path = tmp_path / "session.json"
    path.write_text(json.dumps(value))

    assert DirectorySession.load(path).capture_scope == "partial"


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
    assert store.head("session") == first


def test_session_id_is_the_flat_archive_key(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    store = SessionStore(_paths(tmp_path / "home"))
    session = _session(root)
    directory = store.create(session)

    location, loaded = store.find("session")

    assert directory == store.paths.archive / "session"
    assert location == directory
    assert loaded == session
    with pytest.raises(SessionNotFoundError):
        store.find("missing")
