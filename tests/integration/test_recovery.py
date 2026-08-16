import base64
import json
import struct
from pathlib import Path

from memo.daemon.registry import Registry
from memo.recording.metadata import DirectorySession, SessionOrigin, SnapshotEntry, StepManifest
from memo.recording.paths import StoragePaths
from memo.recording.store import SessionStore
from memo.recording.streams import StreamStore


def _setup(tmp_path: Path):
    home = tmp_path / "home"
    paths = StoragePaths(home)
    paths.ensure_storage()
    root = tmp_path / "root"
    root.mkdir()
    registry = Registry(paths.registry)
    active = registry.create(root, "now", "session")
    registry.allocate_attachment(active.session_id, "now", "terminal")
    store = SessionStore(paths)
    store.create(
        DirectorySession(
            "session", str(root.resolve()), "now", "now", SessionOrigin("1.0.0", "user", "host")
        )
    )
    return paths, registry, store


def test_recovery_truncates_partial_frame_and_restores_ack(tmp_path: Path) -> None:
    paths, registry, _ = _setup(tmp_path)
    streams = StreamStore(paths, registry)
    event = {"sequence": 1, "direction": "output", "data": base64.b64encode(b"durable").decode()}
    streams.append("session", "terminal", [event], 10)
    spool = paths.spool / "session" / "spool" / "terminal.frames"
    valid_size = spool.stat().st_size
    with spool.open("ab") as handle:
        handle.write(struct.pack("!I", 100) + b"partial")
    registry.recover_sequence("terminal", 0)

    assert streams.recover_spool("session", "terminal") == 1
    assert spool.stat().st_size == valid_size
    assert registry.attachment("terminal").accepted_sequence == 1
    assert streams.seal_session("session") == {"terminal": 1}
    registry.close()


def test_integrity_keeps_last_head_and_ignores_unpublished_artifacts(tmp_path: Path) -> None:
    _, registry, store = _setup(tmp_path)
    session = store.load_session("session")
    session_path = store.session_path("session")
    prepared = session_path / "prepared"
    prepared.mkdir()
    (prepared / "file.txt").write_text("complete")
    manifest = StepManifest(
        "session", 0, "now", "snapshots/0", [SnapshotEntry("file.txt", "file", 0o644, 8)]
    )
    store.publish(session, manifest, prepared)
    (session_path / "snapshots" / ".abandoned").mkdir()
    (session_path / "steps" / ".abandoned.tmp").write_text("partial")

    assert store.check_integrity("session") == manifest
    assert (session_path / "HEAD").read_text().strip() == "0"
    registry.close()


def test_integrity_rejects_missing_published_stream_chunk(tmp_path: Path) -> None:
    paths, registry, store = _setup(tmp_path)
    streams = StreamStore(paths, registry)
    event = {"sequence": 1, "direction": "output", "data": base64.b64encode(b"x").decode()}
    streams.append("session", "terminal", [event], 10)
    streams.seal_session("session")
    session = store.load_session("session")
    prepared = store.session_path("session") / "prepared"
    prepared.mkdir()
    manifest = StepManifest("session", 0, "now", "snapshots/0", [], {"terminal": 1})
    store.publish(session, manifest, prepared)
    metadata_path = store.session_path("session") / "streams/terminals/terminal/stream.json"
    metadata = json.loads(metadata_path.read_text())
    (metadata_path.parent / metadata["chunks"][0]).unlink()

    try:
        store.check_integrity("session")
    except ValueError as error:
        assert "missing chunk" in str(error)
    else:
        raise AssertionError("missing stream chunk was accepted")
    registry.close()
