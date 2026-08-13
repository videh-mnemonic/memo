from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from memo.config import Paths
from memo.models import CheckpointManifest, DirectorySession, SnapshotEntry
from memo.session_store import SessionStore


def _paths(tmp_path: Path) -> Paths:
    return Paths(tmp_path, tmp_path / "scratch", tmp_path / "archive", tmp_path / "unpack")


def _session(root: Path) -> DirectorySession:
    return DirectorySession("session", str(root.resolve()), "namespace", "now", "now")


def _publish(store: SessionStore, session: DirectorySession, generation: int) -> CheckpointManifest:
    checkpoint_id = f"checkpoint-{generation}"
    temporary = Path(tempfile.mkdtemp(prefix="prepared-", dir=store.session_path("namespace", "session")))
    (temporary / "file.txt").write_text(str(generation))
    manifest = CheckpointManifest(checkpoint_id, "session", generation, "now",
                                  f"snapshots/{checkpoint_id}",
                                  [SnapshotEntry("file.txt", "file", 0o644, 1)])
    return store.publish(session, manifest, temporary)


def test_publishes_immutable_artifacts_and_monotonic_head(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    store = SessionStore(_paths(tmp_path))
    session = _session(root)
    directory = store.create(session)
    first = _publish(store, session, 1)
    second = _publish(store, session, 2)

    assert (directory / "session.json").is_file()
    assert (directory / first.snapshot / "file.txt").read_text() == "1"
    assert (directory / second.snapshot / "file.txt").read_text() == "2"
    assert store.head("namespace", "session") == second
    assert (directory / "HEAD").read_text().strip() == second.checkpoint_id


def test_rejects_skipped_generation_and_incomplete_reference(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    store = SessionStore(_paths(tmp_path))
    session = _session(root)
    directory = store.create(session)
    with pytest.raises(ValueError, match="next generation"):
        _publish(store, session, 2)

    (directory / "checkpoints" / "missing.json").write_text(json.dumps({
        "schema_version": 1, "checkpoint_id": "missing", "session_id": "session",
        "generation": 1, "created_utc": "now", "snapshot": "snapshots/missing", "entries": [],
    }))
    (directory / "HEAD").write_text("missing\n")
    with pytest.raises(ValueError, match="missing snapshot"):
        store.head("namespace", "session")


def test_failed_head_replacement_preserves_previous_visibility(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "root"
    root.mkdir()
    store = SessionStore(_paths(tmp_path))
    session = _session(root)
    directory = store.create(session)
    first = _publish(store, session, 1)

    from memo import session_store
    original = session_store.atomic_write

    def fail_head(path: Path, data: bytes) -> None:
        if path.name == "HEAD":
            raise OSError("injected publication failure")
        original(path, data)

    monkeypatch.setattr(session_store, "atomic_write", fail_head)
    with pytest.raises(OSError, match="injected"):
        _publish(store, session, 2)
    assert (directory / "HEAD").read_text().strip() == first.checkpoint_id
    assert store.head("namespace", "session") == first
